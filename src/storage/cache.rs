use lru::LruCache;
use crate::bplustree::{Node, NodeId};
use crate::storage::{KeyCodec, ValueCodec, PageStorage, NodeStorage};
use std::io;
use std::num::NonZeroUsize;

// CacheLayer is a decorator around a backend storage that caches nodes in memory.
pub struct CacheLayer<K, V, B: PageStorage> {
    backend: B,
    cache: LruCache<NodeId, Node<K, V>>,
}

// Implement the initialization for CacheLayer with a specified capacity and backend storage.
impl<K, V, B> CacheLayer<K, V, B>
where
    K: KeyCodec + Clone,
    V: KeyCodec + Clone,
    B: PageStorage,
{
    fn new(capacity: usize, backend: B) -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(capacity).expect("Invalid cache capacity")),
            backend,
        }
    }
}

// Implement the NodeStorage trait
impl<K, V, B> NodeStorage<K, V> for CacheLayer<K, V, B>
    where
    K: KeyCodec,
    V: ValueCodec,
    B: PageStorage,
{
    fn read_node(&mut self, id: u64) -> io::Result<Option<Node<K, V>>> {
        if let Some(node) = self.cache.get(&id) {
            return Ok(Some(node.clone()));
        }
        let node = self.backend.read_node(id)?;
        if let Some(n) = &node {
            self.cache.put(id, n.clone());
        } else {
            // If the node is not found in the backend, return None
            return Ok(None);
        }
        Ok(node)
    }

    fn write_node(&mut self, node: &Node<K, V>) -> io::Result<u64> {
        // Write the node to the backend storage
        let id = self.backend.write_node(node)?;
        self.cache.put(id, node.clone()).ok_or(io::Error::other(
            "Cache write failed: cache is full or node already exists", // TODO rethink this error
            // message
        ))?;
    }
    
    fn flush(&mut self) -> io::Result<()> {
        self.backend.flush()
    }
} 
