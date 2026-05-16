//! [`NodeStorage`] implementation backed by a [`PageStorage`] instance.
//!
//! This is the pluggable node encoding strategy. Different implementations
//! can use different codecs (e.g. prefix-compressed pages) while delegating
//! raw page I/O to the underlying [`PageStorage`].
//!
//! Includes a bounded in-memory read cache (CLOCK-Pro via `quick_cache`) that
//! eliminates repeated `pread` syscalls for hot pages. COW semantics guarantee
//! that a page's content never changes once written, so cache entries are always
//! valid until the page is freed and potentially reallocated.
//!
//! The cache is bounded to a configurable number of pages (default 16,384 =
//! ~64 MB for 4 KB pages). CLOCK-Pro provides scan resistance: range scans
//! over cold leaf pages will not evict frequently-accessed root/internal nodes.

use crate::bplustree::NodeView;
use crate::codec::bincode::NoopNodeViewCodec;
use crate::layout::PAGE_SIZE;
use crate::storage::epoch::EpochManager;
use crate::storage::{HasEpoch, NodeStorage, PageStorage, StorageError};

use quick_cache::sync::Cache;
use std::path::Path;
use std::sync::Arc;

/// Default maximum number of cached pages (16,384 × 4 KB ≈ 64 MB).
pub const DEFAULT_CACHE_CAPACITY: usize = 16_384;

/// A [`NodeStorage`] that encodes node views as pages and delegates I/O to a [`PageStorage`].
///
/// Maintains a bounded in-memory cache of decoded [`NodeView`]s keyed by page ID.
/// Uses CLOCK-Pro eviction (via `quick_cache`) for scan resistance — root and
/// upper internal nodes stay cached even under full range scans.
///
/// Cache correctness relies on COW: a page ID's content is immutable once
/// written. Entries are explicitly removed by [`free_node`] when the page is
/// reclaimed by epoch-based GC (and may be reallocated).
pub struct PagedNodeStorage<S: PageStorage> {
    store: Arc<S>,
    epoch_mgr: Arc<EpochManager>,
    cache: Cache<u64, NodeView>,
}

impl<S: PageStorage + Send + Sync + 'static> HasEpoch for PagedNodeStorage<S> {
    fn epoch_mgr(&self) -> &Arc<EpochManager> {
        &self.epoch_mgr
    }
}

impl<S: PageStorage + Send + Sync + 'static> PagedNodeStorage<S> {
    /// Opens (or creates) a [`PagedNodeStorage`] from the given data path.
    ///
    /// Creates its own [`EpochManager`]. Used by standalone callers (tests,
    /// benchmarks) that don't go through [`Database`][crate::database::Database].
    /// Uses [`DEFAULT_CACHE_CAPACITY`].
    pub fn new<P: AsRef<Path>>(storage_path: P, _manifest_path: P) -> Result<Self, std::io::Error> {
        Ok(Self {
            store: Arc::new(S::open(storage_path)?),
            epoch_mgr: Arc::new(EpochManager::new()),
            cache: Cache::new(DEFAULT_CACHE_CAPACITY),
        })
    }

    /// Wraps a shared [`PageStorage`] with a shared epoch manager.
    ///
    /// Uses [`DEFAULT_CACHE_CAPACITY`]. For custom capacity, use [`from_parts_with_capacity`].
    pub fn from_parts(store: Arc<S>, epoch_mgr: Arc<EpochManager>) -> Self {
        Self::from_parts_with_capacity(store, epoch_mgr, DEFAULT_CACHE_CAPACITY)
    }

    /// Wraps a shared [`PageStorage`] with a shared epoch manager and explicit cache capacity.
    ///
    /// `capacity` is the maximum number of pages held in the cache.
    /// Each page is ~4 KB, so total memory ≈ `capacity × PAGE_SIZE`.
    pub fn from_parts_with_capacity(
        store: Arc<S>,
        epoch_mgr: Arc<EpochManager>,
        capacity: usize,
    ) -> Self {
        Self {
            store,
            epoch_mgr,
            cache: Cache::new(capacity),
        }
    }

    /// Returns a reference to the underlying page storage.
    pub fn page_storage(&self) -> &S {
        &self.store
    }

    /// Returns the shared [`Arc`] handle to the underlying page storage.
    pub fn page_storage_shared(&self) -> Arc<S> {
        Arc::clone(&self.store)
    }
}

impl<S: PageStorage + Send + Sync + 'static> NodeStorage for PagedNodeStorage<S> {
    fn read_node_view(&self, page_id: u64) -> Result<Option<NodeView>, StorageError> {
        // Fast path: check cache (lock-free read via quick_cache).
        if let Some(view) = self.cache.get(&page_id) {
            return Ok(Some(view));
        }

        // Slow path: read from disk, decode, and populate cache.
        let mut buf = [0u8; PAGE_SIZE];
        self.store.read_page(page_id, &mut buf)?;
        let mut view = NoopNodeViewCodec::decode(&buf)?;
        view.set_page_id(page_id);

        self.cache.insert(page_id, view);

        Ok(Some(view))
    }

    fn write_node_view(&self, node_view: &NodeView) -> Result<u64, StorageError> {
        let buf = NoopNodeViewCodec::encode(node_view)?;
        let page_id = self.store.write_page(buf)?;

        // Populate cache with the written node (already decoded).
        let mut cached = *node_view;
        cached.set_page_id(page_id);
        self.cache.insert(page_id, cached);

        Ok(page_id)
    }

    fn write_node_view_at_offset(
        &self,
        node_view: &NodeView,
        offset: u64,
    ) -> Result<u64, StorageError> {
        let buf = NoopNodeViewCodec::encode(node_view)?;
        let page_id = self.store.write_page_at_offset(offset, buf)?;

        // Update cache entry for this page ID.
        let mut cached = *node_view;
        cached.set_page_id(page_id);
        self.cache.insert(page_id, cached);

        Ok(page_id)
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.store.flush().map_err(StorageError::Io)
    }

    fn free_node(&self, id: u64) -> Result<(), StorageError> {
        // Evict from cache — the page ID may be reallocated.
        self.cache.remove(&id);
        self.store.free_page(id).map_err(StorageError::Io)
    }
}
