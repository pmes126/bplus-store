pub mod catalog;
pub mod manifest;
pub mod metadata;
pub mod superblock;

use std::sync::Arc;

use crate::bplustree::NodeView;
use crate::layout::PAGE_SIZE;
use crate::api::{TreeId, KeyEncodingId, KeyLimits};
use crate::keyfmt::KeyFormat;
use crate::storage::{Storage, StorageError};
use crate::storage::metadata_manager::MetadataManager;
use crate::store::catalog::Catalog;
use crate::store::superblock::Superblock;
use crate::store::catalog::TreeMeta;
use crate::store::manifest::{ManifestRec, ManifestLog};
use crate::store::manifest::reader::ManifestReader;
use crate::store::metadata::Metadata;

use anyhow::Result;
use std::path::Path;
use std::sync::RwLock;

const MANIFEST_FILE_NAME: &str = "MANIFEST";
const SUPERBLOCK_OFFSET: u64 = 0; // Superblock is at the beginning of the manifest file
const FREELIST_OFFSET: u64 = 4096;
const FREELIST_PAGE_ID: u64 = 1;

struct Store<S: PageStorage> where S: PageStorage {
    storage: Arc<S>,
    metadata_mgr: MetadataManager<S>,
    catalog: RwLock<Catalog>,         // in-mem committed view
    manifest_log: ManifestLog,
}

struct RecoveredState {
    catalog: Catalog,
    manifest_log: ManifestLog,
}

impl<S: PageStorage> Store<S> {
    pub fn new(storage: &S, metadata_mgr: MetadataManager<S>, catalog: Catalog, manifest_log: ManifestLog) -> Self {
        Self {
            storage: Arc::new(storage),
            metadata_mgr: MetadataManager::new(storage),
            catalog,
            manifest_log,
        }
    }

    pub fn open(path: &std::path::Path) -> Result<Self> {
       let manifest_path = path.join("MANIFEST");
       let storage = S::open(path)?;

       // Recover state: read superblock, load freelist, replay manifest to build catalog
       let recovered = Self::recover_state(storage.as_ref())?;
       let next_tree_id = recovered.catalog.max_tree_id() + 1;

       let manifest_reader = ManifestReader::open(manifest_path.as_path())?;
       for rec in manifest_reader.next() {
           recovered.catalog.replay_record(&rec);
       }
       ManifestLog::open(manifest_reader, recovered.catalog.next_seq)?;
       // Reconcile with per-tree metadata pages (metadata page is truth)
       let metadata_mgr = MetadataManager::new(storage);
       for meta in recovered.catalog.metas.values_mut() {
           let metadata_page = metadata_mgr.read_active_meta(meta.meta_a, meta.meta_b)?;
           let (root, h, sz) = (metadata_page.root_node_id, metadata_page.height, metadata_page.size);
           if (root, h, sz) != (meta.root_id, meta.height, meta.size) {
               meta.root_id = root; meta.height = h; meta.size = sz;
           }
       }
       Ok(Self {
           storage: Arc::new(storage),
           catalog: RwLock::new(recovered.catalog),
           metadata_mgr: MetadataManager::new(storage),
           manifest_log: recovered.manifest_log,
       })
   }

   fn recover_state(storage: &S) -> Result<RecoveredState> {
       let superblock = Self::load_superblock(storage)?;
       superblock.validate()?;
       let (catalog, manifest_log) = Self::replay_manifest(storage)?;
       let next_tree_id = catalog.max_tree_id() + 1;
       let freelist_info = superblock::read_freepages_snapshot(storage, FREELIST_OFFSET)?;
       storage.set_next_page_id(freelist_info.0)?;
       storage.set_freelist(freelist_info.1)?;

       Ok(RecoveredState {
           catalog,
           manifest_log,
       })
   }

   fn load_superblock(storage: &S) -> Result<Superblock> {
       let mut buf = [0u8; PAGE_SIZE];
       storage.read_page(SUPERBLOCK_OFFSET / PAGE_SIZE as u64, &mut buf)?;
       Superblock::decode(&buf).map_err(|e| StorageError::CodecError { msg: format!("Failed to decode superblock: {}", e) })
   }

   fn replay_manifest(storage: &S) -> Result<Catalog, ManifestLog> {
       let mut reader = ManifestReader::open(&storage)?;
       let mut catalog = Catalog::new();
       let mut manifest_log = ManifestLog{
           recs: Vec::new(),
       };
       while let Some(rec) = reader.next()? {
           catalog.replay_record(&rec);
           manifest_log.recs.push(rec);
       }
       Ok(catalog)
   }

    pub fn bootstap_metadata(&self, id: TreeId, order: usize) -> Result<(u64, u64, Metadata), std::io::Error> {
        // Allocate two metadata pages for redundancy (meta_a and meta_b)
        let meta_a = self.storage.allocate_page()?;
        let meta_b = self.storage.allocate_page()?;

        // Initialize metadata with default values
        let metadata = Metadata {
            root_node_id: 0, // Will be set when the first node is created
            id,
            txn_id: 0,
            height: 0,
            order,
            size: 0,
            checksum: 0,
        };

        Ok((meta_a, meta_b, metadata))
    }

   // API methods for tree management (create, rename, drop) → write manifest record → update
   // catalog → return meta
   // TODO: return tree handle instead of meta; tree handle routes to meta + storage for
   // operations
   pub fn create_tree(&self, name: &str, enc: KeyEncodingId, key_format: KeyFormat, order: usize, limits: Option<KeyLimits>) -> Result<TreeMeta, std::io::Error>{
       let id = self.alloc_tree_id(name); // UUID/ULID/etc.
       let (meta_a, meta_b, metadata) = self.bootstrap_metadata(id, order)?; // format meta pages

       let rec = ManifestRec::CreateTree {
           seq: 0, id: id.clone(), name: name.to_string(),
           key_format,
           key_encoding: enc, encoding_version: 1, key_limits: limits,
           meta_a, meta_b,
           order: order as u64,
           root_id: metadata.root_node_id, 
           height: metadata.height as u64,
           size: metadata.size as u64,
       };

       let mut w = self.manifest_log;
       w.recs.push(rec);
       // flush manifest to ensure durability of the new tree record before updating in-memory
       // catalog
       // update catalog with the assigned seq
       let mut cat = self.catalog.write().unwrap();
       cat.replay_record(&ManifestRec::CreateTree {
           seq: self., id: id.clone(), name: name.to_string(),
           key_encoding: enc, encoding_version: 1, key_limits: limits,
           key_format,
           order: order as u64,
           meta_a, meta_b,
           root_id: metadata.root_node_id,
           height: metadata.height as u64, size: metadata.size as u64,
       });

       // return meta (from catalog)
       let meta = cat.metas.get_mut(&id).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "tree not found"))?.clone();
       Ok(meta)
   }

   // rename by  id (not name); check tree exists; write
   // manifest record; update catalog
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

   // delete by id (not name); check tree exists; write manifest record; update catalog; optional:
   pub fn drop_tree(&self, id: &TreeId) -> anyhow::Result<()> {
       let mut w = self.manifest.lock().unwrap();
        // check tree exists
       {
           let cat = self.catalog.read().unwrap();
           if !cat.metas.contains_key(id) {
               return Err(anyhow::anyhow!("tree not found"));
           }
       }
       let seq = w.append(ManifestRec::DeleteTree { seq: 0, id: id.clone() })?;
       w.fsync()?;
       drop(w);

       let mut cat = self.catalog.write().unwrap();
       cat.replay_record(&ManifestRec::DeleteTree { seq, id: id.clone() });

       // optional: enqueue the tree’s pages for GC (background walker)
       Ok(())
   }

   // tree factories route to per-tree wrappers. Return TreeHandle
   pub fn open_tree(&self, id: &TreeId) -> anyhow::Result<()> {
       let cat = self.catalog.read().unwrap();
        // check tree exists 
       let meta = cat.get_by_id(id).ok_or_else(|| anyhow::anyhow!("tree not found"))?;

       Ok(())
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

/// Unified page storage interface for B+ tree logic
pub trait PageStorage {
    /// Initializes the storage, creating necessary files or structures
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error>
    where
        Self: Sized;

    /// Reads a page by ID into a fixed 4KB buffer
    fn read_page(&self, page_id: u64, target: &mut [u8; PAGE_SIZE]) -> Result<(), std::io::Error>;

    /// Writes a full 4KB page to disk and returns the offset
    fn write_page(&self, data: &[u8]) -> Result<u64, std::io::Error>;

    /// Writes a full 4KB page to disk at the given offset
    fn write_page_at_offset(&self, offset: u64, data: &[u8]) -> Result<u64, std::io::Error>;

    /// Ensures all writes are flushed to disk
    fn flush(&self) -> Result<(), std::io::Error>;

    /// Allocates a new, unused page ID
    fn allocate_page(&self) -> Result<u64, std::io::Error>;

    /// Frees a page ID for future reuse
    fn free_page(&self, page_id: u64) -> Result<(), std::io::Error>;

    /// Closes the storage, flushing any pending writes
    fn close(&self) -> Result<(), std::io::Error>;

    /// Set the next page ID (to be used for allocation
    fn set_next_page_id(&self, next_page_id: u64) -> Result<(), std::io::Error>;

    /// Set/Extend the freelist with a list of freed pages
    fn set_freelist(&self, freed_pages: Vec<u64>) -> Result<(), std::io::Error>;
}

pub trait NodeStorage: Send + Sync + 'static {
    /// Reads a node view (undecoded) from storage by its ID
    fn read_node_view(&self, id: u64) -> Result<Option<NodeView>, StorageError>;

    /// Writes a node view (encoded) to storage by its ID
    fn write_node_view(&self, node_view: &NodeView) -> Result<u64, StorageError>;

    /// Writes a node view (encoded) to storage by its ID at a specific offset
    fn write_node_view_at_offset(&self, node_view: &NodeView, offset: u64) -> Result<u64, StorageError>;

    /// Flushes any cached writes to persistent storage
    fn flush(&self) -> Result<(), std::io::Error>;

    /// Frees a node by its ID
    fn free_node(&self, id: u64) -> Result<(), std::io::Error>;
}
