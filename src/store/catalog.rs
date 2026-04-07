use std::collections::HashMap;
use crate::api::{TreeId, KeyEncodingId};
use crate::keyfmt::KeyFormat;
use crate::store::manifest::{ManifestRec, ManifestRec::*};

/// Persistent catalog entry describing one logical B+ tree.
/// This struct is owned by the Store/Catalog and persisted via manifest records.
#[derive(Clone)]
pub struct TreeMeta {
    // ─── Identity ──────────────────────────────────────────────
    /// Stable opaque identifier for this tree (not reused)
    pub id: TreeId,

    /// Human-readable logical name (may change via rename)
    pub name: String,

    // ─── Encoding / layout contract ─────────────────────────────
    /// Comparator / byte ordering (e.g. Bytewise, U64Be, LexVarint)
    pub key_encoding: KeyEncodingId,

    /// On-page key layout (e.g. Raw, PrefixRestarts)
    pub keyfmt_id: KeyFormat,

    /// On-page layout version (for forward compat)
    pub format_version: u16,

    /// Optional format parameters (restart interval, prefix length, etc.)
    //pub format_params: KeyFormatParams,

    ///// Optional key length or lexicographic bounds
    //pub key_limits: Option<KeyLimits>,

    // ─── Physical placement ─────────────────────────────────────
    /// Page IDs of the two alternating metadata slots (A/B)
    pub meta_a: u64,
    pub meta_b: u64,

    // ─── Snapshot (filled from meta pages on open) ──────────────
    /// Currently committed root node page
    pub root_id: u64,

    /// Current height of the B+ tree
    pub height: usize,

    /// Approximate number of entries (copied from Metadata)
    pub size: usize,

    /// Order of the B+ tree (copied from Metadata)
    pub order: usize,

    // ─── Catalog bookkeeping ────────────────────────────────────
    /// Last manifest sequence number that touched this record
    pub last_seq: u64,
}

// The Catalog is an in-memory structure that tracks all existing trees by name and ID, and their
// metadata. It is reconstructed by replaying the manifest log on startup, and updated by applying
// new manifest records as they are written. It serves as the authoritative source for tree
// metadata, and is used to route API calls by tree name or ID to the correct metadata and storage
// locations.
pub struct Catalog {
    pub by_name: HashMap<String, TreeId>,
    pub metas:   HashMap<TreeId, TreeMeta>,
    pub next_seq: u64, // next manifest seq to use
}

impl Catalog {
    pub fn new() -> Self {
        Self { by_name: HashMap::new(), metas: HashMap::new(), next_seq: 1 }
    }

    pub fn get_by_name(&self, name: &str) -> Option<&TreeMeta> {
        self.by_name.get(name).and_then(|id| self.metas.get(id))
    }

    pub fn get_by_id(&self, id: &TreeId) -> Option<&TreeMeta> {
        self.metas.get(id)
    }

    pub fn replay_record(&mut self, rec: &ManifestRec) {
        match rec.clone() {
            CreateTree { seq, id, name, key_encoding, key_limits, key_format, encoding_version, meta_a, meta_b, root_id, order, height, size } => {
                self.by_name.insert(name.clone(), id.clone());
                self.metas.insert(id.clone(), TreeMeta {
                    id: id.clone(), name: name.clone(),
                    key_encoding, keyfmt_id: key_format,
                    meta_a, meta_b,
                    format_version: encoding_version,
                    order: order as usize,
                    root_id,
                    height: height as usize, size: size as usize,
                    last_seq: seq,
                });
                let _key_limits = key_limits; // TODO
                self.next_seq = self.next_seq.max(seq + 1);
            }
            RenameTree { seq, id, new_name } => {
                if let Some(m) = self.metas.get_mut(&id) {
                    self.by_name.remove(&m.name);
                    m.name = new_name.clone();
                    self.by_name.insert(new_name.clone(), id.clone());
                    m.last_seq = seq; self.next_seq = self.next_seq.max(seq + 1);
                }
            }
            DeleteTree { seq, id } => {
                if let Some(m) = self.metas.remove(&id) {
                    self.by_name.remove(&m.name);
                    self.next_seq = self.next_seq.max(seq + 1);
                }
            }
            Checkpoint { seq } => { self.next_seq = self.next_seq.max(seq + 1); }
        }
    }
}

