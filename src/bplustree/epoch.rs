use crate::bplustree::NodeId;
use std::collections::{HashMap, BTreeMap};
use std::thread::{ThreadId};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex};

pub type Epoch = u64;

/// Tracks active reader epochs and deferred_pages reclamation.
#[derive(Debug)]
pub struct EpochManager {
    global_epoch: AtomicU64,
    active_readers: Mutex<HashMap<ThreadId, Epoch>>,
    deferred_pages: Mutex<BTreeMap<u64, Vec<NodeId>>>, // (epoch, NodeId)
}

impl EpochManager {
    pub fn new() -> Self {
        Self {
            global_epoch: AtomicU64::new(1),
            active_readers: Mutex::new(HashMap::new()),
            deferred_pages: Mutex::new(BTreeMap::new()),
        }
    }

    /// Advance the epoch (typically called on commit)
    pub fn advance(&mut self) -> u64 {
        self.global_epoch.fetch_add(1, Ordering::SeqCst)
    }
    
    pub fn current(&self) -> u64 {
        self.global_epoch.load(Ordering::SeqCst)
    }

    /// Register a page for future reclamation
    pub fn add_reclaim_candidate(&mut self, epoch: u64, page_id: u64) {
        self.deferred_pages.lock().unwrap().entry(epoch).or_default().push(page_id);
        println!("Added page {} for reclamation at epoch {}", page_id, epoch);
        println!("Deferred pages: {:?}", self.deferred_pages.lock().unwrap());
    }

    /// Reader pins itself to current epoch
    pub fn pin(&mut self) -> ReaderGuard {
        let epoch = self.global_epoch.load(Ordering::Relaxed); // Get the current epoch, no need for SeqCst here
        let tid = std::thread::current().id();
        self.active_readers.lock().unwrap().insert(tid, epoch);
        ReaderGuard {
            epoch_mgr: self,
            tid,
            epoch,
        }
    }

    /// Reader unpins itself
    pub fn unpin(&self) {
        let tid = std::thread::current().id();
        self.active_readers.lock().unwrap().remove(&tid);
    }

    /// Unpin for a specific thread ID
    pub fn unpin_by_id(&self, tid: ThreadId) {
        self.active_readers.lock().unwrap().remove(&tid);
    }

    /// Return the minimum epoch still pinned
    pub fn oldest_active(&self) -> Epoch {
        let readers = self.active_readers.lock().unwrap();
        readers.values().copied().min().unwrap_or(self.global_epoch.load(Ordering::Relaxed))
    }

    /// Reclaim all pages older than or equal to a safe epoch
    pub fn reclaim(&mut self, safe_epoch: Epoch) -> Vec<NodeId> {
        let mut reclaimed = vec![];
        let to_reclaim: Vec<u64> = self.deferred_pages.lock().unwrap()
            .range(..safe_epoch) // exclude the safe_epoch itself anything older can be reclaimed
            .map(|(e, _)| *e)
            .collect();

        for epoch in to_reclaim {
            if let Some(pages) = self.deferred_pages.lock().unwrap().remove(&epoch) {
                reclaimed.extend(pages);
            }
        }
        reclaimed
    }

    /// Get the current epoch for the current thread
    pub fn get_current_thread_epoch(&self) -> Option<Epoch> {
        let tid = std::thread::current().id();
        self.active_readers.lock().unwrap().get(&tid).copied()
    }
}

pub struct ReaderGuard<'a> {
    epoch_mgr: &'a EpochManager,
    tid: ThreadId,
    epoch: Epoch,
}

impl<'a> ReaderGuard<'a> {
    fn drop(&mut self) {
        self.epoch_mgr.unpin_by_id(self.tid);
    }
    fn epoch(&self) -> Epoch {
        self.epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_reclamation_flow() {
        let mut mgr = EpochManager::new();

        let e1 = {
            let g1 = mgr.pin();
            g1.epoch()
        }; // g1 dropped here

        mgr.advance();

        let e2 = {
            let g2 = mgr.pin();
            g2.epoch()
        };

        mgr.add_reclaim_candidate(e1, 42);
        mgr.add_reclaim_candidate(e2, 43);

        mgr.advance();

        let safe = mgr.oldest_active();
        assert!(safe >= e2);

        let reclaimed = mgr.reclaim(safe);
        println!("Reclaimed pages: {:?}", reclaimed);
        assert!(reclaimed.contains(&42));
        assert!(reclaimed.contains(&43));
    }
}
