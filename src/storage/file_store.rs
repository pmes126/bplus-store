pub use crate::storage::file_store;

use crate::bplustree::NodeView;
use crate::storage::epoch::EpochManager;
use crate::codec::bincode::NoopNodeViewCodec;
use crate::layout::PAGE_SIZE;
use crate::storage::{Storage, HasEpoch, NodeStorage, PageStorage, StorageError};
use crate::store::manifest::writer::ManifestWriter;

use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct FileStore<S: PageStorage> 
{
    store: S,
    epoch_mgr: Arc<EpochManager>, // Epoch manager for transaction management
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
            manifest: Mutex::new(ManifestWriter::open(manifest_path.as_ref(), 0)?),
        })
    }

}
// NodeStorage impl: read/write node views as pages; encode/decode with NodeViewCodec; free pages
// on demand.
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

    fn write_node_view_at_offset(&self, node_view: &NodeView, offset: u64) -> Result<u64, StorageError> {
        let buf = NoopNodeViewCodec::encode(node_view)?;
        let res = self.store.write_page_at_offset(offset, buf)?;
        Ok(res)
    }

    fn flush(&self) -> Result<(), std::io::Error> {
        self.store.flush()
    }

    fn free_node(&self, id: u64) -> Result<(), std::io::Error> {
        self.store.free_page(id)
    }
}
