use crate::bplustree::Node;
use crate::storage::NodeStorage;
use crate::bplustree::BPlusTreeRangeIter;
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
    phantom: std::marker::PhantomData<(K, V)>,
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
            phantom: std::marker::PhantomData,
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
                        Ok(i) => i,
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
            let node = self.storage.read_node(current_id)?;
            match node {
                Node::Internal { keys, children } => {
                    let i = match keys.binary_search(&key) {
                        Ok(i) => i,
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
        let leaf_node = self.storage.read_node(current_id)?;
        let mut leaf = leaf_node;
        if let Node::Leaf { ref mut keys, ref mut values, mut next} = leaf {
            match keys.binary_search(&key) {
                Ok(i) => {
                    values[i] = value; // Replace existing value
                }
                Err(i) => {
                    keys.insert(i, key.clone());
                    values.insert(i, value);
                }
            }
            if keys.len() > self.max_keys {
                let mid = keys.len() / 2;
                let right_keys = keys.split_off(mid);
                let right_values = values.split_off(mid);
                let new_leaf = Node::Leaf {
                    keys: right_keys,
                    values: right_values,
                    next: next.take(), // Retain the next pointer
                };
                // Write the new leaf node to storage
                self.storage.write_node(self.next_id, &new_leaf)?;
                self.next_id += 1;
                // Write the updated leaf node back to storage
                let new_leaf_id = self.next_id;
                self.storage.write_node(new_leaf_id, &mut leaf)?;
                self.next_id += 1;
                // Propagate the split upwards.
                self.insert_into_parent(path, key, new_leaf_id)?;
            } else {
                // Write the updated leaf node back to storage
                self.storage.write_node(self.next_id, &leaf)?;
                self.next_id += 1;
            }
        }
        Ok(())
    }
    // insert into a parent node, the path is the collection of the nodes that are parent to the
    // leaf, try inserting in a lifo manner.
    fn insert_into_parent(
        &mut self,
        mut path: Vec<(u64, usize)>,
        mut key: K,
        mut new_child_id: u64,
    ) -> Result<()> {
        while let Some((parent_id, insert_pos)) = path.pop() {
            let mut node = self.read_node(parent_id)?;
            match node {
                Node::Leaf { .. } => {
                    // We should never reach a leaf node here, as we are inserting into the parent
                    // of a leaf node.
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Reached a leaf node while trying to insert into parent",
                    ));
                }
                Node::Internal { ref mut keys, ref mut children  } => {
                    keys.insert(insert_pos, key.clone());
                    children.insert(insert_pos + 1, new_child_id);

                    if keys.len() <= self.max_keys {
                        self.storage.write_node(self.next_id, &node)?;
                        self.next_id += 1;
                        return Ok(())
                    } else {
                        // Node is overflowed, we need to split it
                        let mid = keys.len() / 2;
                        let right_keys = keys.split_off(mid + 1);
                        let right_children = children.split_off(mid + 1);
                        let split_key_for_parent = keys.pop().unwrap_or_else(|| {
                                // If the split key is None, it means we are splitting the root node
                                // and we need to create a new root.
                                key.clone()
                            });

                        let new_internal = Node::Internal {
                            keys: right_keys,
                            children: right_children,
                        };
                        // Write the new internal node to storage
                        let new_internal_id = self.next_id;
                        self.storage.write_node(new_internal_id, &new_internal);
                        self.next_id += 1;
                        // Write the split internal node to storage
                        self.storage.write_node(self.next_id, &node)?;
                        self.next_id += 1;

                        key = split_key_for_parent;
                        new_child_id = new_internal_id;
                        continue;
                    }
                }
            }
        }

        let old_root = self.root_id;
        let new_root = Node::Internal {
            keys: vec![key],
            children: vec![old_root, new_child_id],
        };
        // Write the new root node to storage
        self.storage.write_node(self.next_id, &new_root);
        self.root_id = self.next_id;
        self.next_id += 1;
        Ok(())
    }

    // Search for a key and return the value if exists
    pub fn search(&mut self, key: &K) -> Result<Option<V>> {
        let mut current_id = self.root_id;
        loop {
            let node = self.storage.read_node(current_id)?;
            match node {
                Node::Internal { keys, children } => {
                    let i = match keys.binary_search(&key) {
                        Ok(i) => i,
                        Err(_i) => return Ok(None), // Key not found
                    };
                    current_id = children[i];
                }
                Node::Leaf { keys, values, .. } => {
                    match keys.binary_search(&key) {
                        Ok(i) => return Ok(Some(values[i].clone())),
                        Err(_i) => return Ok(None), // Key not found
                    };
                }
            }
        }
    }

    // Searches for a range of keys in the B+ tree and returns an iterator over the key-value
    // pairs.
    pub fn search_range(&mut self, start: &K, end: &K) -> Result<Option<BPlusTreeRangeIter<K, V, S>>> {
        if start > end {
            return Ok(None); // Invalid range
        }
        let mut current_id = self.root_id.clone();

        loop {
            let node = self.storage.read_node(current_id)?;

            match node {
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

                    return Ok(Some(BPlusTreeRangeIter {
                        storage: self.storage,
                        current_id: Some(current_id),
                        index: start_index,
                        start: start.clone(),
                        end: end.clone(),
                        phantom: std::marker::PhantomData,
                    }));
                }
            }
        }
    }

    // Delete and handle underflow of leaf nodes
    pub fn delete(&mut self, key: &K) -> Result<Option<V>> {
        let mut current_id = self.root_id;
        // Stack to keep track of parent nodes and the index of the child in the parent
        let mut parent_stack: Vec<(u64, usize)> = vec![];

        loop {
            let node = self.storage.read_node(current_id)?;
            match node {
                Node::Internal { keys, children } => {
                    let i = match keys.binary_search(&key) {
                        Ok(i) => i,
                        Err(_) => return Ok(None), // Key not found
                    };
                    parent_stack.push((current_id, i));
                    current_id = children[i];
                }
                Node::Leaf {mut keys, mut values, .. } => {
                    match keys.binary_search(&key) {
                        Ok(i) => {
                            let ret_val = Some(values[i].clone());
                            keys.remove(i);
                            values.remove(i);
                            // Check if the leaf node is underflowed
                            if keys.len() < self.min_keys && !parent_stack.is_empty() {
                                // Handle underflow by borrowing from the parent or merging
                                //self.handle_leaf_underflow(&mut parent_stack, current_id)?;
                            }
                            return Ok(ret_val)
                        }
                        Err(_i) => {
                            return Ok(None); // Key not found
                        }
                    }
                }
            }
        }
    }

    // Set the root of the B+ tree
    pub fn set_root(&mut self, root: NodeId) {
        self.root_id = root;
        //self.storage.set_root(root);
    }

    //pub fn create_from_storage(storage: S) -> Result<Self> {
    //    let root = storage.get_root()?;

    //    //Ok(Self {
    //    //    root_id: root,
    //    //    next_id: storage.get_next_id()?,
    //    //    order: storage.get_order()?,
    //    //    max_keys : storage.get_order()? - 1,
    //    //    min_keys : (storage.get_order()? + 1) / 2,
    //    //    storage,
    //    //})
    //}
}
