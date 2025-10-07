use std::collections::HashMap;
use crate::api::{TreeId, TreeMeta, KeyEncodingId, KeyLimits};
use crate::storage::manifest::{ManifestRec, ManifestRec::*};

pub struct Catalog {
    pub by_name: HashMap<String, TreeId>,
    pub metas:   HashMap<TreeId, TreeMeta>,
    pub next_seq: u64, // next manifest seq to use
}

impl Catalog {
    pub fn new() -> Self {
        Self { by_name: HashMap::new(), metas: HashMap::new(), next_seq: 1 }
    }

    pub fn replay_record(&mut self, rec: &ManifestRec) {
        match rec.clone() {
            CreateTree { seq, id, name, key_encoding, key_limits, encoding_version, meta_a, meta_b, root_id, height, size } => {
                self.by_name.insert(name.clone(), id.clone());
                self.metas.insert(id.clone(), TreeMeta {
                    id: id.clone(), name: name.clone(),
                    key_encoding, encoding_version,
                    meta_a, meta_b,
                    root_id,
                    height: height as usize, size: size as usize,
                    last_seq: seq,
                });
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
            DropTree { seq, id } => {
                if let Some(m) = self.metas.remove(&id) {
                    self.by_name.remove(&m.name);
                    self.next_seq = self.next_seq.max(seq + 1);
                }
            }
            Checkpoint { seq } => { self.next_seq = self.next_seq.max(seq + 1); }
        }
    }
}

