use crate::node::Node;
use crate::node::NodeRef;
use std::cell::RefCell;
use std::rc::Rc;

pub mod iter;
pub use iter::BPlusTreeRangeIter;

static DEGREE: usize = 4; // B+ tree degree

#[derive(Debug)]
pub struct BPlusTree<K: Ord + Clone + Default, V: Clone> {
    root: NodeRef<K, V>,
}

// BPlusTree implementation
impl<K: Ord + Clone + Default, V: Clone> BPlusTree<K, V> {
    pub fn new() -> Self {
        BPlusTree {
            root: Rc::new(RefCell::new(Node::Leaf {
                keys: vec![],
                values: vec![],
                next: None,
            })),
        }
    }

    // Inserts a key-value pair into the B+ tree.
    pub fn insert(&mut self, key: K, value: V) {
        // If the root is empty, we need to create a new root
        if self.root.borrow().is_empty() {
            self.root = Rc::new(RefCell::new(Node::Leaf {
                keys: vec![key],
                values: vec![value],
                next: None,
            }));
            return;
        }
        // Insert into the B+ tree  
        if let Some((new_key, new_node)) = self.insert_inner(key, value) {
            // If the root was split, we need to create a new root
            let old_root = self.root.clone();
            self.root = Rc::new(RefCell::new(Node::Internal {
                keys: vec![new_key],
                children: vec![old_root, new_node],
            }));
        }
    }

    // Inserts a key-value pair into the B+ tree iteratively.
    pub fn insert_inner(&mut self, key: K, value: V) -> Option<(K, NodeRef<K, V>)> {
        let mut path = vec![];
        let mut current = self.root.clone();

        // Step 1: Traverse down and record the path
        loop {
            let node = current.clone();
            match &*node.borrow() {
                Node::Internal { keys, children } => {
                    let pos = keys.binary_search(&key).unwrap_or_else(|x| x);
                    path.push((current.clone(), pos));
                    current = children.get(pos).cloned()
                        .unwrap_or_else(|| {
                            unreachable!("B+ tree structure is invalid, no child found at position {}", pos)
                        });
                }
                Node::Leaf { .. } => break,
            }
        }

        let mut promoted: Option<(K, NodeRef<K, V>)> = {
            let mut leaf = current.borrow_mut();
            if let Node::Leaf { keys, values, next } = &mut *leaf {
                let pos = keys.binary_search(&key).unwrap_or_else(|x| x);
                keys.insert(pos, key.clone());
                values.insert(pos, value);

                if keys.len() < DEGREE * 2 {
                    None
                } else {
                    let split_off = DEGREE;
                    let new_keys = keys.split_off(split_off);
                    let new_values = values.split_off(split_off);

                    let new_leaf = Rc::new(RefCell::new(Node::Leaf {
                        keys: new_keys.clone(),
                        values: new_values,
                        next: next.take(),
                    }));
                    *next = Some(new_leaf.clone());

                    Some((new_keys[0].clone(), new_leaf))
                }
            } else {
                unreachable!()
            }
        };

        // Step 3: Propagate split up the tree
        while let Some((parent, index)) = path.pop() {
            if let Some((promote_key, new_child)) = promoted {
                let mut parent_mut = parent.borrow_mut();
                if let Node::Internal { keys, children } = &mut *parent_mut {
                    keys.insert(index, promote_key.clone());
                    children.insert(index + 1, new_child);

                    if keys.len() >= DEGREE * 2 {
                        let split_index = DEGREE;
                        let promote = keys[split_index].clone();

                        let new_keys = keys.split_off(split_index + 1);
                        let new_children = children.split_off(split_index + 1);
                        keys.pop(); // remove promoted

                        let new_internal = Rc::new(RefCell::new(Node::Internal {
                            keys: new_keys,
                            children: new_children,
                        }));

                        promoted = Some((promote, new_internal));
                        continue;
                    }
                }
                // No further split needed
                promoted = None;
            }
        }
        promoted
    }

    // Searches for a key in the B+ tree and returns the associated value if it exists.
    pub fn search(&self, key: &K) -> Option<V> {
        let mut current = self.root.clone();

        loop {
            let next = {
                let node = current.borrow();
                match &*node {
                    Node::Internal { keys, children } => {
                        let pos = keys.binary_search(key).unwrap_or_else(|x| x);
                        children.get(pos).cloned()
                    }
                    Node::Leaf { keys, values, .. } => {
                        return keys.binary_search(key).ok().map(|pos| values[pos].clone());
                    }
                }
            };
            current = next.unwrap_or_else(|| {
                // Should never happen if the tree is well-formed
                return Rc::new(RefCell::new(Node::Leaf {
                    keys: vec![],
                    values: vec![],
                    next: None,
                }));
            });
        }
    }

    // Deletes a key from the B+ tree and returns the associated value if it exists.
    pub fn delete(&mut self, key: &K) -> Option<V> {
        let result = self.delete_inner(&mut self.root.clone(), key, true);
        if let Some((Some(new_root), true, deleted)) = result {
            self.root = new_root;
            return deleted;
        }
        result.and_then(|(_, _, deleted)| deleted)
    }

    // Inner function to handle deletion logic
    fn delete_inner(
        &mut self,
        node: &mut NodeRef<K, V>,
        key: &K,
        is_root: bool,
    ) -> Option<(Option<NodeRef<K, V>>, bool, Option<V>)> {
        let mut node_ref = node.borrow_mut();

        match &mut *node_ref {
            // If the node is a leaf, we can directly remove the key and value
            // If the key is not found, we return None
            // If the node is underflowed, we return Some with underflow flag
            Node::Leaf { keys, values, .. } => {
                if let Ok(pos) = keys.binary_search(key) {
                    keys.remove(pos);
                    let val = values.remove(pos);
                    let underflow = !is_root && keys.len() < DEGREE;
                    return Some((None, underflow, Some(val)));
                }
                None
            }
            // If the node is an internal node, we need to find the child to delete from
            // and handle underflow
            // If the child is underflowed, we need to borrow from a sibling or merge
            // If the child is not underflowed, we can just delete the key from it
            // If the child is not found, we return None
            Node::Internal { keys, children } => {
                let idx = keys.binary_search(key).unwrap_or_else(|x| x);
                let result = self.delete_inner(&mut children[idx], key, false);

                if let Some((Some(new_child), true, deleted)) = result {
                    return Some((Some(new_child), true, deleted));
                }

                if let Some((None, true, deleted)) = result {
                    if idx > 0 && self.can_borrow(&children[idx - 1]) {
                        self.borrow_from_left(idx, keys, children);
                    } else if idx + 1 < children.len() && self.can_borrow(&children[idx + 1]) {
                        self.borrow_from_right(idx, keys, children);
                    } else {
                        self.merge_children(idx, keys, children);
                    }

                    if is_root && keys.is_empty() {
                        let new_root = children.remove(0);
                        return Some((Some(new_root), true, deleted));
                    }

                    return Some((None, false, deleted));
                }

                result
            }
        }
    }

    // Checks if a sibling node can be borrowed from
    fn can_borrow(&self, sibling: &NodeRef<K, V>) -> bool {
        let node = sibling.borrow();
        match &*node {
            Node::Leaf { keys, .. } => keys.len() > DEGREE,
            Node::Internal { keys, .. } => keys.len() > DEGREE,
        }
    }

    // Borrow a key and child from the left sibling
    fn borrow_from_left(&self, idx: usize, keys: &mut Vec<K>, children: &mut Vec<NodeRef<K, V>>) {
        let (left, target) = (&children[idx - 1], &children[idx]);
        let mut left_node = left.borrow_mut();
        let mut target_node = target.borrow_mut();

        match (&mut *left_node, &mut *target_node) {
            (
                Node::Leaf {
                    keys: lk,
                    values: lv,
                    ..
                },
                Node::Leaf {
                    keys: tk,
                    values: tv,
                    ..
                },
            ) => {
                tk.insert(0, lk.pop().unwrap());
                tv.insert(0, lv.pop().unwrap());
                keys[idx - 1] = tk[0].clone();
            }
            (
                Node::Internal {
                    keys: lk,
                    children: lc,
                },
                Node::Internal {
                    keys: tk,
                    children: tc,
                },
            ) => {
                tk.insert(0, keys[idx - 1].clone());
                keys[idx - 1] = lk.pop().unwrap();
                tc.insert(0, lc.pop().unwrap());
            }
            _ => {}
        }
    }

    // Borrow a key and child from the right sibling
    fn borrow_from_right(&self, idx: usize, keys: &mut Vec<K>, children: &mut Vec<NodeRef<K, V>>) {
        let (target, right) = (&children[idx], &children[idx + 1]);
        let mut target_node = target.borrow_mut();
        let mut right_node = right.borrow_mut();

        match (&mut *target_node, &mut *right_node) {
            (
                Node::Leaf {
                    keys: tk,
                    values: tv,
                    ..
                },
                Node::Leaf {
                    keys: rk,
                    values: rv,
                    ..
                },
            ) => {
                tk.push(rk.remove(0));
                tv.push(rv.remove(0));
                keys[idx] = rk[0].clone();
            }
            (
                Node::Internal {
                    keys: tk,
                    children: tc,
                },
                Node::Internal {
                    keys: rk,
                    children: rc,
                },
            ) => {
                tk.push(keys[idx].clone());
                keys[idx] = rk.remove(0);
                tc.push(rc.remove(0));
            }
            _ => {}
        }
    }

    // Merge two children nodes into one
    // This function assumes that the children at `idx` and `idx + 1` are underflowed
    // and need to be merged.
    fn merge_children(&self, idx: usize, keys: &mut Vec<K>, children: &mut Vec<NodeRef<K, V>>) {
        let (left, right) = (&children[idx], &children[idx + 1]);
        let mut left_node = left.borrow_mut();
        let mut right_node = right.borrow_mut();

        match (&mut *left_node, &mut *right_node) {
            (
                Node::Leaf {
                    keys: lk,
                    values: lv,
                    next: ln,
                },
                Node::Leaf {
                    keys: rk,
                    values: rv,
                    next: rn,
                },
            ) => {
                lk.extend(rk.drain(..));
                lv.extend(rv.drain(..));
                *ln = rn.take();
                keys.remove(idx);
                children.remove(idx + 1);
            }
            (
                Node::Internal {
                    keys: lk,
                    children: lc,
                },
                Node::Internal {
                    keys: rk,
                    children: rc,
                },
            ) => {
                lk.push(keys.remove(idx));
                lk.extend(rk.drain(..));
                lc.extend(rc.drain(..));
                children.remove(idx + 1);
            }
            _ => {}
        }
    }

    // Returns true if the B+ tree is empty.
    pub fn is_empty(&self) -> bool {
        self.root.borrow().is_empty()
    }

    // Returns the number of keys in the B+ tree.
    pub fn len(&self) -> usize {
        self.root.borrow().len()
    }

    // Returns the height of the B+ tree.
    pub fn height(&self) -> usize {
        self.root.borrow().height()
    }

    // Searches for a range of keys in the B+ tree and returns an iterator over the key-value
    // pairs.
    pub fn search_range(&self, start: &K, end: &K) -> impl Iterator<Item = (K, V)> {
        let current = self.root.clone();

        loop {
            let node = current.borrow();
            match &*node {
                Node::Internal { keys, children } => {
                    let idx = keys.binary_search(start).unwrap_or_else(|x| x);
                    if idx < keys.len() && keys[idx] == *start {
                        // Found exact match, return iterator from this leaf
                        return BPlusTreeRangeIter {
                            current_node: Some(current.clone()),
                            index: idx,
                            end_bound: end.clone(),
                        };
                    } else if idx < children.len() {
                        // Descend to the child node
                        current = children[idx].clone();
                    } else {
                        // No child found, return empty iterator
                        return BPlusTreeRangeIter {
                            current_node: None,
                            index: 0,
                            end_bound: end.clone(),
                        };
                    }
                }
                Node::Leaf { .. } => break,
            }
        }
    }

    pub fn clear(&mut self) {
        self.root = Rc::new(RefCell::new(Node::Leaf {
            keys: vec![],
            values: vec![],
            next: None,
        }));
    }

    /// Returns an iterator over the B+ tree, starting from the first leaf node.
    pub fn iter(&self) -> BPlusTreeRangeIter<K, V> {
        let mut current = self.root.clone();

        loop {
            let next = {
                let node = current.borrow();
                match &*node {
                    Node::Internal { children, .. } => {
                        children.get(0).cloned()
                    }
                    Node::Leaf { .. } => {
                        break;
                    }
                }
            };
            current = next.unwrap_or_else(|| {
                // Should never happen if the tree is well-formed
                return Rc::new(RefCell::new(Node::Leaf {
                    keys: vec![],
                    values: vec![],
                    next: None,
                }));
            });
        }

        BPlusTreeRangeIter {
            current_node: Some(current.clone()),
            index: 0,
            end_bound: K::default(), // Default value, will be set in search_range
        }
    }
}
