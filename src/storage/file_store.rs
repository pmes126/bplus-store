pub use crate::storage::file_store;

use crate::bplustree::NodeView;
use crate::bplustree::tree::BPlusTree;
use crate::storage::epoch::EpochManager;
use crate::storage::catalog::Catalog;
use crate::codec::bincode::NoopNodeViewCodec;
use crate::layout::PAGE_SIZE;
use crate::metadata::{
    MetadataPage, calculate_checksum, new_metadata_page,
    new_metadata_page_with_object,
};
use crate::storage::{Metadata, HasEpoch, MetadataStorage, NodeStorage, PageStorage, StorageError};
use crate::storage::manifest::{reader::ManifestReader, writer::ManifestWriter, ManifestRec};
use crate::api::{TreeId, TreeMeta, KeyEncodingId, KeyLimits};

use std::path::Path;
use zerocopy::AsBytes;
use std::sync::{Arc, Mutex, RwLock};


pub struct FileStore<S: PageStorage> 
{
    store: S,
    epoch_mgr: Arc<EpochManager>, // Epoch manager for transaction management
    catalog: RwLock<Catalog>,         // in-mem committed view
    manifest: Mutex<ManifestWriter>,  // single writer
}

impl<S: PageStorage> HasEpoch for FileStore<S>
where
    S: Send + Sync + 'static,
{
    fn epoch_mgr(&self) -> &Arc<EpochManager> {
        &self.epoch_mgr
    }
}

impl<S: PageStorage> FileStore<S> 
    where
        S: Send + Sync + 'static,

{
    pub fn new<P: AsRef<Path>>(storage_path: P, manifest_path: P) -> Result<Self, std::io::Error> {
        Ok(Self {
            store: S::open(storage_path)?,
            epoch_mgr: EpochManager::new_shared(),
            catalog: RwLock::new(Catalog::new()),
            manifest: Mutex::new(ManifestWriter::open(manifest_path.as_ref(), 0)?),
        })
    }

    pub fn open(path: &std::path::Path) -> anyhow::Result<Self> {
       let manifest_path = path.join("MANIFEST");
       let mut fs =  Self::new(path, manifest_path.as_path())?;

       // 1) Replay manifest -> build catalog
       let mut reader = ManifestReader::open(manifest_path.as_ref())?; // allow empty
       let mut cat = Catalog::new();
       while let Some(rec) = reader.next()? {
           cat.replay_record(&rec);
       }

       // 2) Reconcile with per-tree metadata pages (metadata page is truth)
       for meta in cat.metas.values_mut() {
           let metadata_page = fs.read_active_meta(meta.meta_a, meta.meta_b)?;
           let (root, h, sz) = (metadata_page.root_id, metadata_page.height, metadata_page.size);
           if (root, h, sz) != (meta.root_id, meta.height, meta.size) {
               meta.root_id = root; meta.height = h; meta.size = sz;
           }
       }

       let seq = cat.next_seq;
       fs.catalog = RwLock::new(cat);
       // 3) Open writer starting at cat.next_seq - 1
       fs.manifest = Mutex::new(ManifestWriter::open(&manifest_path, seq - 1)?);

       Ok(fs)
    }

    pub fn create_tree(&self, name: &str, enc: KeyEncodingId, order: usize, limits: Option<KeyLimits>) -> Result<TreeMeta, std::io::Error>     {
        let id = self.alloc_tree_id(name); // UUID/ULID/etc.
        let (meta_a, meta_b, metadata) = self.bootstrap_metadata(id, order)?; // format meta pages

        let rec = ManifestRec::CreateTree {
            seq: 0, id: id.clone(), name: name.to_string(),
            key_encoding: enc, encoding_version: 1, key_limits: limits,
            meta_a, meta_b,
            root_id: metadata.root_node_id, 
            height: metadata.height as u64,
            size: metadata.size as u64,
        };

        let mut w = self.manifest.lock().unwrap();
        let seq = w.append(rec)?;
        w.fsync()?; // can batch later
        // update catalog with the assigned seq
        let mut cat = self.catalog.write().unwrap();
        cat.replay_record(&ManifestRec::CreateTree {
            seq, id: id.clone(), name: name.to_string(),
            key_encoding: enc, encoding_version: 1, key_limits: limits,
            meta_a, meta_b,
            root_id: metadata.root_node_id,
            height: metadata.height as u64, size: metadata.size as u64,
        });

        // return meta (from catalog)
        let meta = cat.metas.get_mut(&id).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "tree not found"))?.clone();
        Ok(meta)
    }

    pub fn rename_tree(&self, id: &TreeId, new_name: &str) -> anyhow::Result<()> {
        let mut w = self.manifest.lock().unwrap();
         // check tree exists
        {
            let cat = self.catalog.read().unwrap();
            if !cat.metas.contains_key(id) {
                return Err(anyhow::anyhow!("tree not found"));
            }
        }
        let seq = w.append(ManifestRec::RenameTree { seq: 0, id: id.clone(), new_name: new_name.to_string() })?;
        w.fsync()?;
        drop(w);

        let mut cat = self.catalog.write().unwrap();
        cat.replay_record(&ManifestRec::RenameTree { seq, id: id.clone(), new_name: new_name.to_string() });
        Ok(())
    }

    pub fn drop_tree(&self, id: &TreeId) -> anyhow::Result<()> {
        let mut w = self.manifest.lock().unwrap();
         // check tree exists
        {
            let cat = self.catalog.read().unwrap();
            if !cat.metas.contains_key(id) {
                return Err(anyhow::anyhow!("tree not found"));
            }
        }
        let seq = w.append(ManifestRec::DropTree { seq: 0, id: id.clone() })?;
        w.fsync()?;
        drop(w);

        let mut cat = self.catalog.write().unwrap();
        cat.replay_record(&ManifestRec::DropTree { seq, id: id.clone() });

        // optional: enqueue the tree’s pages for GC (background walker)
        Ok(())
    }

    pub fn epoch_mgr(&self) -> &Arc<EpochManager> { &self.epoch_mgr }

    // tree factories route to per-tree wrappers
    pub fn open_tree(&self, id: &TreeId) -> anyhow::Result<BPlusTree<FileStore<S>>> {
        let meta = self.catalog.meta(id).ok_or_else(|| anyhow::anyhow!("tree not found"))?.clone();
        Ok(BPlusTree {
            store: self,
            root_id: meta.root_id,
            height: meta.height,
            size: meta.size,
            // any other per-tree cached stuff…
        })
    }

    pub fn alloc_tree_id(&self, name: &str) -> TreeId {
        // Simple example: hash of name + timestamp; replace with UUID/ULID/etc.
        use std::time::{SystemTime, UNIX_EPOCH};
        use std::hash::{Hasher, Hash};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        name.hash(&mut hasher);
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        ts.hash(&mut hasher);
        hasher.finish()
    }
}

impl<S: PageStorage> MetadataStorage for FileStore<S> {
    fn read_metadata(&self, slot: u64) -> Result<MetadataPage, std::io::Error> {
        self.store.read_page(slot as u64, &mut buf)?;

        let metadata = MetadataPage::from_bytes(&buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // Validate checksum
        let checksum = metadata.data.checksum;
        let calculated_checksum = calculate_checksum(metadata);
        if checksum != calculated_checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Metadata checksum mismatch",
            ));
        }
        Ok(*metadata)
    }

    fn write_metadata(&self, slot: u64, meta: &mut MetadataPage) -> Result<(), std::io::Error> {
        let checksum = calculate_checksum(meta);
        meta.data.checksum = checksum;
        let buf = meta.as_bytes();
        self.store.write_page_at_offset(slot as u64, buf)?;
        Ok(())
    }

    fn read_active_meta(&self, meta_a: u64, meta_b: u64) -> Result<TreeMeta, std::io::Error> {
        let meta0 = self.read_metadata(meta_a)?;
        let meta1 = self.read_metadata(meta_b)?;
        let active_meta = if meta0.data.txn_id >= meta1.data.txn_id {
            meta0
        } else {
            meta1
        };

        let meta = TreeMeta {
            id: active_meta.data.id,
            name: String::new(),
            key_encoding: KeyEncodingId::default(),
            encoding_version: 1,
            root_id: active_meta.data.root_node_id,
            height: active_meta.data.height,
            size: active_meta.data.size,
            meta_a,
            meta_b,
            last_seq: 0,
        };
        Ok(meta)
    }

    fn get_metadata(&self, meta_a: u64, meta_b: u64) -> Result<Metadata, std::io::Error> {
        let meta0 = self.read_metadata(meta_a)?;
        let meta1 = self.read_metadata(meta_b)?;
        if meta0.data.txn_id >= meta1.data.txn_id {
            Ok(meta0.data)
        } else {
            Ok(meta1.data)
        }
    }

    fn commit_metadata(
        &self,
        slot: u64,
        txn_id: u64,
        id: u64,
        root: u64,
        height: usize,
        order: usize,
        size: usize,
    ) -> Result<(), std::io::Error> {
        let mut metadata_page = new_metadata_page(root, txn_id, id, 0, height, order, size);
        self.write_metadata(slot, &mut metadata_page)?;
        Ok(())
    }

    fn commit_metadata_with_object(
        &self,
        slot: u64,
        metadata: &Metadata,
    ) -> Result<(), std::io::Error> {
        let mut metadata_page = new_metadata_page_with_object(metadata);
        self.write_metadata(slot, &mut metadata_page)?;
        Ok(())
    }

    fn bootstrap_metadata(&self, id: u64, order: usize) -> Result<(u64, u64, Metadata), std::io::Error> {
        let initial_txn_id = 1;
        let meta_a = self.store.allocate_page()?;
        let meta_b = self.store.allocate_page()?;
        let root_node_id = self.store.allocate_page()?;

        let mut metadata_page_a = new_metadata_page(root_node_id, id, initial_txn_id, 0, 1, order, 0);
        let mut metadata_page_b = new_metadata_page(root_node_id, id, initial_txn_id - 1, 0, 1, order, 0);

        self.write_metadata(meta_a, &mut metadata_page_a)?;
        self.write_metadata(meta_b, &mut metadata_page_b)?;

        Ok((meta_a, meta_b, metadata_page_a.data))
    }
}

impl<S: PageStorage> NodeStorage for FileStore<S>
where
    S: Send + Sync + 'static,
{
    fn read_node_view(&self, page_id: u64) -> Result<Option<NodeView>, StorageError> {
        let mut buf = [0u8; PAGE_SIZE];
        self.store.read_page(page_id, &mut buf)?;
        NoopNodeViewCodec::decode(&buf).map(|view| Ok(Some(view)))?
    }

    fn write_node_view(&self, node_view: &NodeView) -> Result<u64, StorageError> {
        let buf = NoopNodeViewCodec::encode(node_view)?;
        let res = self.store.write_page(buf)?;
        Ok(res)
    }

    fn flush(&self) -> Result<(), std::io::Error> {
        self.store.flush()
    }

    fn free_node(&self, id: u64) -> Result<(), std::io::Error> {
        self.store.free_page(id)
    }
}
