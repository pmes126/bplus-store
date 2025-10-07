use crate::bplustree::{NodeId, NodeView};
use crate::codec::CodecError;
use crate::layout::PAGE_SIZE;
use crate::metadata::{Metadata, MetadataPage};
use crate::storage::epoch::EpochManager;
use crate::api::TreeMeta;
use anyhow::Result;
use std::path::Path;

/// Implementations
pub mod file_store;
pub mod page_store;
pub mod epoch;
pub mod catalog;
pub mod manifest;

use thiserror::Error;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("page corrupted: {msg}")]
    CodecError { msg: String },
    #[error("Storage error: {msg}")]
    StorageAny { msg: String },
    #[error("page {pid} not found")]
    NotFound { pid: NodeId },
    #[error("invariant: {0}")]
    Invariant(&'static str),
    #[error("backend error: {source}")]
    Other {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub trait HasEpoch {
    fn epoch_mgr(&self) -> &std::sync::Arc<EpochManager>;
}

/// Unified storage interface for B+ tree logic
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
}

pub trait NodeStorage: Send + Sync + 'static {
    /// Reads a node view (undecoded) from storage by its ID
    fn read_node_view(&self, id: u64) -> Result<Option<NodeView>, StorageError>;

    /// Writes a node view (encoded) to storage by its ID
    fn write_node_view(&self, node_view: &NodeView) -> Result<u64, StorageError>;

    /// Flushes any cached writes to persistent storage
    fn flush(&self) -> Result<(), std::io::Error>;

    /// Frees a node by its ID
    fn free_node(&self, id: u64) -> Result<(), std::io::Error>;
}

pub trait MetadataStorage {
    /// Reads metadata from a specific page
    fn read_metadata(&self, slot: u64) -> Result<MetadataPage, std::io::Error>;

    /// Writes metadata to a specific slot
    fn write_metadata(&self, slot: u64, meta: &mut MetadataPage) -> Result<(), std::io::Error>;

    /// Reads the current root node ID from metadata
    fn read_active_meta(&self, meta_a: u64, meta_b: u64) -> Result<TreeMeta, std::io::Error>;

    // Get the current metadata
    fn get_metadata(&self) -> Result<Metadata, std::io::Error>;

    // Commits the provided metadata to the oldest metadata slot and advances the transaction ID
    fn commit_metadata(
        &self,
        slot: u64,
        txn_id: u64,
        id: u64,
        root: u64,
        height: usize,
        order: usize,
        size: usize,
    ) -> Result<(), std::io::Error>;

    // Commit metadata with a metadata object
    fn commit_metadata_with_object(
        &self,
        slot: u64,
        metadata: &Metadata,
    ) -> Result<(), std::io::Error>;

    // bootstrap the metadata for a tree if not initialized, returns (meta_a, meta_b, Metadata)
    fn bootstrap_metadata(&self, id: u64, order: usize) -> Result<(u64, u64, Metadata), std::io::Error>;
}
