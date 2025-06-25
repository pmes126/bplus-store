// src/tree/iter.rs

use crate::node::{Node, NodeRef};

pub struct BPlusTreeRangeIter<K, V> {
    pub(super) current_node: Option<NodeRef<K, V>>,
    pub(super) index: usize,
    pub(super) end_bound: K,
}

impl<K: Ord + Clone, V: Clone> Iterator for BPlusTreeRangeIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = &self.current_node.clone() {

            if let Node::Leaf { keys, values, next } = node.borrow_mut().as_leaf_mut()? {
                while self.index < keys.len() {
                    let key = &keys[self.index];
                    if key >= &self.end_bound {
                        self.current_node = None;
                        return None;
                    }

                    let result = (key.clone(), values[self.index].clone());
                    self.index += 1;
                    return Some(result);
                }

                // Finished current leaf: move to next
                self.current_node = next.clone();
                self.index = 0;
            } else {
                // Should never happen, defensive exit
                return None;
            }
        }

        None
    }
}
