use crate::bplustree::NodeView;
use crate::bplustree::tree::BPlusTree;
use crate::storage::epoch::EpochManager;
use crate::storage::catalog::Catalog;
use crate::codec::bincode::NoopNodeViewCodec;
use crate::layout::PAGE_SIZE;
use crate::metadata::{
    METADATA_PAGE_1, METADATA_PAGE_2, MetadataPage, calculate_checksum, new_metadata_page,
    new_metadata_page_with_object,
};
use crate::storage::{Metadata, HasEpoch, MetadataStorage, NodeStorage, PageStorage, StorageError};
use crate::storage::manifest::{reader::ManifestReader, writer::ManifestWriter, ManifestRec};
use crate::api::{TreeId, TreeMeta, KeyEncodingId, KeyLimits};

use std::path::Path;
use zerocopy::AsBytes;
use std::sync::{Arc, Mutex, RwLock};

pub use crate::storage::file_store;

pub struct FileStore<S: PageStorage> 
{
    store: S,
    epoch_mgr: Arc<EpochManager>, // Epoch manager for transaction management
    catalog: RwLock<Catalog>,         // in-mem committed view
    manifest: Mutex<ManifestWriter>,  // single writer
    manifest_path: Path,          // path to manifest file
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
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let manifest_path = path.as_ref().join("MANIFEST");
        Ok(Self {
            store: S::open(manifest_path)?,
            epoch_mgr: EpochManager::new_shared(),
            catalog: RwLock::new(Catalog::new()),
            manifest_path: *manifest_path.as_path(),
            manifest: Mutex::new(ManifestWriter::open(&manifest_path, 0)?),
        })
    }

     pub fn open(path: &std::path::Path) -> anyhow::Result<Self> {
        let fs =  Self::new(path)?;

        // 1) Replay manifest -> build catalog
        let mut reader = ManifestReader::open(&fs.manifest_path)?; // allow empty
        let mut cat = Catalog::new();
        while let Some(rec) = reader.next()? {
            cat.replay_record(&rec);
        }

        // 2) Reconcile with per-tree metadata pages (metadata page is truth)
        for meta in cat.metas.values_mut() {
            let metadata_page = fs.read_metadata(&meta.meta_a)?;
            let (root, h, sz) = fs.read_metadata(&meta.id)?;
            if (root, h, sz) != (meta.root_id, meta.height, meta.size) {
                meta.root_id = root; meta.height = h; meta.size = sz;
            }
        }

        fs.catalog = RwLock::new(cat);
        // 3) Open writer starting at cat.next_seq - 1
        fs.manifest = ManifestWriter::open(&manifest_path, cat.next_seq - 1)?;

        Ok(fs)
    }

    pub fn create_tree(&self, name: &str, enc: KeyEncodingId, limits: Option<KeyLimits>) -> Result<TreeMeta> {
        let id = self.alloc_tree_id(name); // UUID/ULID/etc.
        let (root_id, height, size) = self.pages.bootstrap_tree(&id)?; // format meta page
        let rec = ManifestRec::CreateTree {
            seq: 0, id: id.clone(), name: name.to_string(),
            key_encoding: enc, encoding_version: 1, key_limits: limits,
            root_id, height, size
     };
        {
            let mut w = self.manifest.lock().unwrap();
            let seq = w.append(rec)?;
            w.fsync()?; // can batch later
            // update catalog with the assigned seq
            let mut cat = self.catalog.write();
            cat.replay_record(&ManifestRec::CreateTree {
                seq, id: id.clone(), name: name.to_string(),
                key_encoding: enc, encoding_version: 1, key_limits: limits,
                root_id, height, size
            });
        }
        // return meta (from catalog)
        let cat = self.catalog.read();
        let meta = cat.metas.get(&id).unwrap().clone();
        Ok(meta)
    }

    // Example: post-commit hook to log UpdateRoot and update catalog
    pub fn log_update_root(&self, id: &TreeId, root: u64, height: u16, size: u64) -> Result<()> {
        let rec = ManifestRec::UpdateRoot { seq: 0, id: id.clone(), root_id: root, height, size };
        let mut w = self.manifest.lock().unwrap();
        let seq = w.append(rec)?;
        // optional: defer fsync; call it on a timer or “checkpoint” API
        drop(w);
        let mut cat = self.catalog.write();
        cat.replay_record(&ManifestRec::UpdateRoot{ seq, id: id.clone(), root_id: root, height, size });
        Ok(())
    }

    pub fn epoch_mgr(&self) -> &Arc<EpochMgr> { &self.epoch }

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
}

impl<S: PageStorage> MetadataStorage for FileStore<S> {
    fn read_metadata(&self, slot: u8) -> Result<MetadataPage, std::io::Error> {
        if slot > METADATA_PAGE_2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid metadata slot",
            ));
        }
        let mut buf = [0u8; PAGE_SIZE];
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

    fn write_metadata(&self, slot: u8, meta: &mut MetadataPage) -> Result<(), std::io::Error> {
        let checksum = calculate_checksum(meta);
        meta.data.checksum = checksum;
        let buf = meta.as_bytes();
        self.store.write_page_at_offset(slot as u64, buf)?;
        Ok(())
    }

    fn read_current_root(&self, meta_a: u8, meta_b: u8) -> Result<u64, std::io::Error> {
        let meta0 = self.read_metadata(meta_a)?;
        let meta1 = self.read_metadata(meta_b)?;
        let root_node_id = if meta0.data.txn_id > meta1.data.txn_id {
            meta0.data.root_node_id
        } else {
            meta1.data.root_node_id
        };
        let meta = TreeMeta {
            id: TreeId::new(), // placeholder
            name: String::new(), // placeholder
            key_encoding: KeyEncodingId::default(), // placeholder
            encoding_version: 1, // placeholder
            key_limits: None, // placeholder
            root_id: root_node_id,
            height: if meta0.data.txn_id > meta1.data.txn_id { meta0.data.height } else { meta1.data.height },
            size: if meta0.data.txn_id > meta1.data.txn_id { meta0.data.size } else { meta1.data.size },
            meta_a,
            meta_b,
        };
        Ok(root_node_id)
    }

    fn get_metadata(&self) -> Result<Metadata, std::io::Error> {
        let meta0 = self.read_metadata(METADATA_PAGE_1)?;
        let meta1 = self.read_metadata(METADATA_PAGE_2)?;
        if meta0.data.txn_id >= meta1.data.txn_id {
            Ok(meta0.data)
        } else {
            Ok(meta1.data)
        }
    }

    fn commit_metadata(
        &self,
        slot: u8,
        txn_id: u64,
        root: u64,
        height: usize,
        order: usize,
        size: usize,
    ) -> Result<(), std::io::Error> {
        let mut metadata_page = new_metadata_page(root, txn_id, 0, height, order, size);
        self.write_metadata(slot, &mut metadata_page)?;
        Ok(())
    }

    fn commit_metadata_with_object(
        &self,
        slot: u8,
        metadata: &Metadata,
    ) -> Result<(), std::io::Error> {
        let mut metadata_page = new_metadata_page_with_object(metadata);
        self.write_metadata(slot, &mut metadata_page)?;
        Ok(())
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
