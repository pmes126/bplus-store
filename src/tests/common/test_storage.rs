#![allow(dead_code)]
use crate::bplustree::{Node, NodeView};
use crate::storage::{NodeStorage, StorageError};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Default, Debug)]
pub struct StorageState {
    commits: Vec<(u8, u64, u64, usize, usize, usize)>, // (slot, txn_id, root_id, height, order, size)
    flushes: u64,
    freed: Vec<u64>,
}

/// A simple, thread-safe fake Storage with logging + failure injection.
#[derive(Clone)]
pub struct TestStorage {
    pub state: Arc<Mutex<StorageState>>,
    pub fail_commit: Arc<AtomicBool>,
    pub fail_flush: Arc<AtomicBool>,
    root_node_id: u64,
}

impl TestStorage {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(StorageState::default())),
            fail_commit: Arc::new(AtomicBool::new(false)),
            fail_flush: Arc::new(AtomicBool::new(false)),
            root_node_id: 2, // Initialize with a default root node ID
        }
    }

    // ------------ Failure injection ------------

    pub fn inject_commit_failure(&self, on: bool) {
        self.fail_commit.store(on, Ordering::Relaxed);
    }
    pub fn inject_flush_failure(&self, on: bool) {
        self.fail_flush.store(on, Ordering::Relaxed);
    }

    // ------------ Introspection / assertions ------------

    /// Returns the last (slot, txn_id, root_id, height, order, size).
    pub fn last_commit(&self) -> Option<(u8, u64, u64, usize, usize, usize)> {
        self.state.lock().unwrap().commits.last().copied()
    }

    pub fn all_commits(&self) -> Vec<(u8, u64, u64, usize, usize, usize)> {
        self.state.lock().unwrap().commits.clone()
    }

    pub fn flush_count(&self) -> u64 {
        self.state.lock().unwrap().flushes
    }

    pub fn freed_pages(&self) -> Vec<u64> {
        self.state.lock().unwrap().freed.clone()
    }
}

impl NodeStorage for TestStorage
where
{
    fn read_node_view(&self, _id: u64) -> Result<Option<Node>, StorageError> {
        // Simulate reading a node by returning None
        Ok(None)
    }

    fn write_node_view(&self, node_view: &NodeView) -> anyhow::Result<u64, StorageError> {
        // Simulate writing a node by returning a dummy ID
        Ok(0)
    }

    fn write_node_view_at_offset(&self, node_view: &NodeView, offset: u64) -> anyhow::Result<u64, StorageError> {
        // Simulate writing a node by returning a dummy ID
        Ok(0)
    }

    fn flush(&self) -> Result<(), std::io::Error> {
        if self.fail_flush.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("flush (injected failure)"));
        }
        self.state.lock().unwrap().flushes += 1;
        Ok(())
    }

    fn free_node(&self, pid: u64) -> Result<(), std::io::Error> {
        self.state.lock().unwrap().freed.push(pid);
        Ok(())
    }
}
