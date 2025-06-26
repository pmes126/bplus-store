use crate::node::Node;
use std::cell::RefCell;
use std::rc::Rc;

pub trait Storage<K, V> {
    type NodeId: Clone;
    fn load(&self, id: &Self::NodeId) -> Rc<RefCell<Node<K, V, Self::NodeId>>>;
    fn store(&mut self, node: Rc<RefCell<Node<K, V, Self::NodeId>>>) -> Self::NodeId;
    fn update(&mut self, id: &Self::NodeId, node: Rc<RefCell<Node<K, V, Self::NodeId>>>);
    fn clear(&mut self);
    fn is_empty(&self) -> bool;
}

pub mod file; // File based
pub mod in_memory; // In memory
