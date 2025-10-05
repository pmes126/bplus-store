pub mod reader;
pub mod writer;

use crate::api::{TreeId, KeyEncodingId, KeyLimits};

#[derive(Debug, Clone)]
pub enum ManifestRec {
    CreateTree {
        seq: u64,
        id: TreeId,
        meta_a : u64,
        meta_b : u64,
        name: String,
        key_encoding: KeyEncodingId,
        encoding_version: u16,
        key_limits: Option<KeyLimits>,
        root_id: u64,
        height: u16,
        size: u64,
    },
    UpdateRoot {
        seq: u64,
        id: TreeId,
        root_id: u64,
        height: u16,
        size: u64,
    },
    RenameTree { seq: u64, id: TreeId, new_name: String },
    DropTree   { seq: u64, id: TreeId },
    Checkpoint { seq: u64 },
}

