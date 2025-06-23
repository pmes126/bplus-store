use std::cell::RefCell;
use std::rc::Rc;

pub type NodeRef<K, V> = Rc<RefCell<Node<K, V>>>;

#[derive(Debug, Clone)]
pub enum Node<K, V> {
    Internal {
        keys: Vec<K>,
        children: Vec<NodeRef<K, V>>,
    },
    Leaf {
        keys: Vec<K>,
        values: Vec<V>,
        next: Option<NodeRef<K, V>>,
    },
}

impl<K, V> Node<K, V> {
    pub fn as_leaf_mut(&mut self) -> Option<&mut Node<K, V>> {
        match self {
            Node::Leaf { .. } => Some(self),
            _ => None,
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            Node::Internal { keys, children } => keys.is_empty() && children.is_empty(),
            Node::Leaf { keys, values, next: _ } => keys.is_empty() && values.is_empty(),
        }
    }
    pub fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf { .. })
    }
    pub fn is_internal(&self) -> bool {
        matches!(self, Node::Internal { .. })
    }
}
