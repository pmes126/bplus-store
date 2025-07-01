pub mod cache;
pub mod flatfile;

use crate::bplustree::{Node, NodeId};
pub use flatfile::FlatFile;
pub use lru::LruCache;
pub use std::io::Result;

pub trait NodeStorage<K, V> {
    fn write_node(&mut self, id: NodeId, node: &Node<K, V, NodeId>) -> Result<()>;
    fn read_node(&mut self, id: NodeId) -> Result<Node<K, V, NodeId>>;
    fn flush(&mut self) -> Result<()>;
    fn get_root(&self) -> Result<u64>;
}
