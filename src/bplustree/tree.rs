pub use crate::bplustree::Node;
pub use crate::storage::NodeStorage;
pub use crate::bplustree::BPlusTreeRangeIter;
use serde::{Deserialize, Serialize};
use std::io::Result;

pub type NodeId = u64; // Type for node IDs

#[derive(Debug)]
pub struct BPlusTree<K, V, S: NodeStorage<K, V>> {
    root_id: NodeId,
    next_id: NodeId,
    order: usize,
    max_keys: usize,
    min_keys: usize,
    storage: S,
}

// BPlusTree implementation
impl<K, V, S> BPlusTree<K, V, S>
where
    K: Serialize + for<'a>Deserialize<'a> + Ord + Clone,
    V: Serialize + for<'a>Deserialize<'a> + Clone,
    S: NodeStorage<K, V>,
{
    pub fn new(mut storage: S, order: usize) -> Self {
        let root_node = Node::Leaf {
            keys: vec![],
            values: vec![],
            next: None,
        };
        storage.write_node(0, &root_node);
        Self {
            root_id: 0,
            next_id: 1,
            storage,
            order,
            max_keys: order - 1,
            min_keys: (order + 1) / 2,
        }
    }

    // Reads a node from the B+ tree storage, using the cache if available.
    fn read_node(&mut self, id: NodeId) -> Result<Node<K, V, NodeId>> {
        self.storage.read_node(id)
    }

    // Writes a node to the B+ tree storage and updates the cache.
    fn write_node(&mut self, id: NodeId, node: Node<K, V, NodeId>) -> Result<()> {
        self.storage.write_node(id, &node)
    }

    // Gets the value associated with a key in the B+ tree.
    fn get(&mut self, key: &K) -> Result<Option<V>> {
        let mut id = self.root_id;
        loop {
            let node = self.read_node(id)?;
            match node {
                Node::Internal { keys, children } => {
                    let idx = match keys.binary_search(&key) {
                        Ok(i) => i + 1,
                        Err(_) => return Ok(None), // Key not found
                    };
                    id = children[idx];
                }
                Node::Leaf { keys, values, .. } => {
                    match keys.binary_search(&key) {
                        Ok(i) =>  return Ok(Some(values[i].clone())),
                        Err(_) => return Ok(None), // Key not found
                    };
                }
            }
        }
    }

    // Inserts a key-value pair into the B+ tree.
    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        let mut path = vec![];
        let mut current_id = self.root_id;

        // Find insertion point
        loop {
            let node_res = self.storage.read_node(current_id)?;
            let node =  node_res.borrow();
            let node_borrow = node..borrow();
            match &*node_borrow {
                Node::Internal { keys, children } => {
                    let i = match keys.binary_search(&key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    path.push((current_id, i));
                    current_id = children[i];
                }
                Node::Leaf { .. } => break,
            }
        }
        // We have found the leaf node, update a copy of the leaf node and insert it back with a
        // new id retaining COW semantics.
        let leaf_node = self.storage.read_node(current_id);
        let mut leaf = leaf_node.borrow_mut();
        if let Node::Leaf { keys, values, .. } = &mut *leaf {
            match keys.binary_search(&key) {
                Ok(i) => {
                    values[i] = value; // Replace existing value
                    return;
                }
                Err(i) => {
                    keys.insert(i, key.clone());
                    values.insert(i, value);
                    self.size += 1;
                }
            }
        }
        // Handle overflow
        if let Node::Leaf { keys, values, next } = &mut *leaf {
            if keys.len() > self.max_keys {
                let mid = keys.len() / 2;
                let right_keys = keys.split_off(mid);
                let right_values = values.split_off(mid);
                let new_leaf = Node::Leaf {
                    keys: right_keys,
                    values: right_values,
                    next: next.take(),
                };
                // Write the updated leaf node to storage
                self.storage.write_node(self.next_id, leaf);
                self.next_id += 1;
                // Write the new leaf node to storage
                new_leaf.next = Some(self.next_id);
                self.storage.write_node(self.next_id, new_leaf);
                self.next_id += 1;
                // Propagate the split upwards.
                self.insert_into_parent(path, key, new_leaf_id);
            }
        }
    }
    // insert into a parent node, the path is the collection of the nodes that are parent to the
    // leaf, try inserting in a lifo manner.
    fn insert_into_parent(
        &mut self,
        mut path: Vec<(u64, usize)>,
        mut key: K,
        mut new_child_id: u64,
    ) {
        while let Some((parent_id, insert_pos)) = path.pop() {
            let node = self.storage.read_node(parent_id);
            let mut node_borrow = node.borrow_mut();
            if let Node::Internal { keys, children } = &mut *node_borrow {
                keys.insert(insert_pos, key.clone());
                children.insert(insert_pos + 1, new_child_id);

                if keys.len() <= self.max_keys {
                    self.storage.write_node(self.next_id, node_borrow);
                    self.next_id += 1;
                    return;
                }

                // Node is overflowed, we need to split it
                let mid = keys.len() / 2;
                let right_keys = keys.split_off(mid + 1);
                let right_children = children.split_off(mid + 1);
                let split_key_for_parent = keys.pop().unwrap();

                let new_internal = Node::Internal {
                    keys: right_keys,
                    children: right_children,
                };
                // Write the new internal node to storage
                let new_internal_id = self.storage.write_node(self.next_id, new_internal);
                self.next_id += 1;
                // Write the split internal node to storage
                self.storage.write_node(self.next_id, node_borrow);
                self.next_id += 1;

                key = split_key_for_parent;
                new_child_id = new_internal_id;
                continue;
            }
        }

        let old_root = self.root_id;
        let new_root = Node::Internal {
            keys: vec![key],
            children: vec![old_root, new_child_id],
        };
        // Write the new root node to storage
        self.storage.write_node(self.next_id, new_root);
        self.next_id += 1;
        self.height += 1;
    }

    // Returns true if the B+ tree is empty.
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    // Returns the number of keys in the B+ tree.
    pub fn len(&self) -> usize {
        self.size
    }

    // Returns the height of the B+ tree.
    pub fn height(&self) -> usize {
        self.height
    }

    // Search for a key and return the value if exists
    pub fn search(&self, key: &K) -> Option<V> {
        let mut current_id = self.root_id;
        loop {
            let node = self.storage.load(current_id);
            let node = node.borrow();
            match &*node {
                Node::Internal { keys, children } => {
                    let i = match keys.binary_search(&key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    current_id = children[i];
                }
                Node::Leaf { keys, values, .. } => {
                    match keys.binary_search(&key) {
                        Ok(i) => return Some(values[i].clone()),
                        Err(_i) => return None, // Key not found
                    };
                }
            }
        }
    }

    // Searches for a range of keys in the B+ tree and returns an iterator over the key-value
    // pairs.
    pub fn search_range(&self, start: &K, end: &K) -> Option<impl Iterator<Item = (K, V)>> {
        if start > end {
            return None; // Invalid range
        }
        let mut current_id = self.root_id.clone();

        loop {
            let node = self.storage.load(current_id);
            let node_borrow = node.borrow();

            match &*node_borrow {
                Node::Internal { keys, children } => {
                    let i = match keys.binary_search(&start) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    current_id = children[i];
                }
                Node::Leaf { keys, .. } => {
                    // Find the index in the leaf node
                    let start_index = keys.binary_search(&start).unwrap_or(
                        keys.len(), // If not found the iterator will skip to the next leaf node
                    );

                    return Some(BPlusTreeRangeIter {
                        storage: &self.storage,
                        current_id: Some(current_id),
                        index: start_index,
                        start: start.clone(),
                        end: end.clone(),
                    });
                }
            }
        }
    }

    // Delete and handle underflow of leaf nodes
    pub fn delete(&mut self, key: &K) -> Option<V> {
        let mut current_id = self.root_id;
        // Stack to keep track of parent nodes and the index of the child in the parent
        let mut parent_stack: Vec<(u64, usize)> = vec![];

        loop {
            let node = self.storage.load(current_id);
            let node_borrow = node.borrow();
            match &*node_borrow {
                Node::Internal { keys, children } => {
                    let i = match keys.binary_search(&key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    parent_stack.push((current_id, i));
                    current_id = children[i];
                }
                Node::Leaf { .. } => {
                    break; // Found the leaf node
                }
            }
        }
        // We have found the leaf node
        // Need to borrow the node mutably and re-match to leaf to remove the key
        let node = self.storage.load(current_id);
        let mut node_mut = node.borrow_mut();
        let mut ret_val: Option<V> = None;
        if let Node::Leaf { keys, values, .. } = &mut *node_mut {
            match keys.binary_search(&key) {
                Ok(i) => {
                    keys.remove(i);
                    values.remove(i);
                    ret_val = Some(values.remove(i));
                    self.storage.delete_node(current_id);
                    self.size -= 1;
                }
                Err(_i) => {}
            }
            // Check if the leaf node is underflowed
            if keys.len() < self.min_keys && !parent_stack.is_empty() {
                self.handle_leaf_underflow(&mut parent_stack, current_id);
            }
        }
        return ret_val;
    }

    // Handle underflow of leaf nodes
    fn handle_leaf_underflow(&mut self, parent_stack: &mut Vec<(u64, usize)>, child_id: u64) {
        // If the leaf node is underflowed, we need to either merge or borrow from a sibling
        while let Some((parent_id, index_in_parent)) = parent_stack.pop_back() {
            let parent = self.storage.load(parent_id);
            let mut child = self.storage.load(child_id);
            let mut parent_mut = parent.borrow_mut();

            if let Node::Internal { keys, children } = &mut *parent_mut {
                let (sibling_index, is_left) = if index_in_parent > -1 {
                    index_in_parent - 1
                } else {
                    index_in_parent + 1
                };

                // Retrieve the sibling node
                if let Some(&sibling_id) = children.get(sibling_index) {
                    let sibling = self.storage.read_node(sibling_id);
                    let mut sibling_mut = sibling.borrow_mut();
                    let mut child_mut = child.borrow_mut();

                    match (&mut under_node, &mut sibling_node) {
                        (Node::Leaf(under_leaf), Node::Leaf(sibling_leaf)) => {
                            // Handle borrowing from sibling leaf nodes
                            if is_left && sibling_leaf.entries.len() > self.min_keys {
                                let borrowed = sibling_leaf.entries.pop().unwrap();
                                under_leaf.entries.insert(0, borrowed);
                            } else if !is_left && sibling_leaf.entries.len() > self.min_keys {
                                let borrowed = sibling_leaf.entries.remove(0);
                                under_leaf.entries.push(borrowed);
                                // merge into the siblings and update the parent node
                            } else {
                                if is_left {
                                    sibling_leaf.entries.extend(under_leaf.entries.drain(..));
                                    sibling_leaf.next = under_leaf.next;
                                    parent_node.children.remove(sibling_id);
                                    parent_node.keys.remove(sibling_id);
                                    self.storage.write_node(self.next_id, sibling_leaf);
                                    self.next_id += 1;
                                    self.storage.write_node(self.next_id, parent_node);
                                    self.next_id += 1;
                                } else {
                                    under_leaf.entries.extend(sibling_leaf.entries.drain(..));
                                    under_leaf.next = sibling_leaf.next;
                                    parent_node.children.remove(sibling_id);
                                    parent_node.keys.remove(sibling_id);
                                    self.storage.write_node(self.next_id, under_leaf);
                                    self.next_id += 1;
                                    self.storage.write_node(self.next_id, parent_node);
                                    self.next_id += 1;
                                }
                            }
                        }
                        (Node::Internal(under_internal), Node::Internal(sibling_internal)) => {
                            // Handle borrowing from sibling internal nodes
                            if is_left && sibling_internal.keys.len() > self.min_keys {
                                let borrowed_key = sibling_internal.keys.pop().unwrap();
                                under_internal.keys.insert(0, borrowed_key);
                                under_internal.children.insert(0, sibling_internal.children.pop().unwrap());
                            } else if !is_left && sibling_internal.keys.len() > self.min_keys {
                                let borrowed_key = sibling_internal.keys.remove(0);
                                under_internal.keys.push(borrowed_key);
                                under_internal.children.push(sibling_internal.children.remove(0));
                            } else {
                                // Merge into the siblings and update the parent node
                                if is_left {
                                    sibling_internal.keys.append(&mut under_internal.keys);
                                    sibling_internal.children.append(&mut under_internal.children);
                                    parent_mut.children.remove(index_in_parent);
                                    parent_mut.keys.remove(sibling_index);
                                    self.storage.write_node(sibling_id, &Node::Internal(sibling_internal.clone()));
                                } else {
                                    under_internal.keys.append(&mut sibling_internal.keys);
                                    under_internal.children.append(&mut sibling_internal.children);
                                    parent_mut.children.remove(sibling_index);
                                    parent_mut.keys.remove(index_in_parent);
                                    self.storage.write_node(parent_id, &Node::Internal(under_internal.clone()));
                                }
                            }
                        }
                        _ => break,
                    }
                } 
                // If underflow has reached the root and the root has only one child, we can discard it and update the root
                if parent_id == self.root && parent_node.children.len() == 1 {
                    self.root = parent_node.children[0];
                    break;
                }

                child_id = parent_id;
            }
        }
    }
    // Set the root of the B+ tree
    pub fn set_root(&mut self, root: NodeId) {
        self.root = root;
        self.storage.set_root(root);
    }

    pub fn create_from_storage(storage: S) -> Self {
        let root = storage.get_root();
        let next_id = storage.get_next_id();
        let order = storage.get_order();
        let max_keys = order - 1;
        let min_keys = (order + 1) / 2;

        Self {
            root,
            next_id,
            order,
            max_keys,
            min_keys,
            storage,
        }
    }
}
