use std::cmp::Ordering;
use crate::storage::page::{InternalPage, LeafPage};
use anyhow::Result;

pub type NodeId = u64;

/// A view of a B+ tree node stored in a page
#[derive(Clone)]
pub enum NodeView {
    Internal {
        page: InternalPage
    },
    Leaf {
        page: LeafPage
    },
}

impl NodeView {
    #[inline]
    pub fn is_internal(&self) -> bool {
        matches!(self, NodeView::Internal { .. })
    }

    #[inline]
    pub fn is_leaf(&self) -> bool {
        matches!(self, NodeView::Leaf { .. })
    }

    #[inline]
    pub fn key_bytes_at(&self, idx: usize) -> &[u8] {
        match self {
            NodeView::Internal { page } => page.key_bytes_at(idx).unwrap(),
            NodeView::Leaf { page } => page.key_bytes_at(idx).unwrap(),
        }
    }

    #[inline]
    pub fn keys_len(&self) -> usize {
        match self {
            NodeView::Internal { page } => page.header.entry_count as usize,
            NodeView::Leaf { page } => page.header.entry_count as usize,
        }
    }

    /// Lower bound using bytewise compare
    pub fn lower_bound(&self, probe: &[u8]) -> Result<usize, usize> {
        let mut lo = 0usize;
        let mut hi = self.keys_len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.key_bytes_at(mid).cmp(probe) {
                Ordering::Less => lo = mid + 1, // move to the right
                Ordering::Equal => return Ok(mid), // found exact match
                Ordering::Greater => hi = mid,
            }
        }
        Err(lo) // return the insertion point   
    }

    /// Get the child pointer at index i
    #[inline]
    pub fn child_ptr_at(&self, i: usize) -> Result<Option<u64>> {
        match self {
            NodeView::Internal { page } => { 
                if i == 0 {
                    return Ok(Some(page.header.leftmost_child)); // No child pointer for index 0
                }
                let idx = i - 1; // Internal nodes have child pointers at i-1
                page.child_at(idx).map(Some).map_err(|e| anyhow::anyhow!(e)) 
            },
            NodeView::Leaf { .. } => Ok(None), // Leaf pages don't have children, but we return 0
        }
    }

    /// Get the value at index i
    #[inline]
    pub fn value_at(&self, i: usize) -> Result<Option<Vec<u8>>> {
        match self {
            NodeView::Internal { .. } => Ok(None), // Internal nodes do not store values
            NodeView::Leaf { page } => {
                let value = page.value_bytes_at(i)?;
                Ok(Some(value.to_vec()))
            }
        }
    }
}
