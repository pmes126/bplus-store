//! WAL replay reader.
//!
//! Follows the same CRC-framed pattern as [`ManifestReader`]: reads records
//! sequentially, validates CRC-32C, and treats truncated trailing records
//! as a clean crash boundary (returns `Ok(None)`).

use crate::database::wal::record::WalRecord;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

/// Reads [`WalRecord`] entries from a WAL file, validating CRC-32C on each.
pub struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    /// Opens the WAL file at `path` for reading.
    pub fn open(path: &Path) -> io::Result<Self> {
        let f = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(f),
        })
    }

    /// Reads the next record from the WAL.
    ///
    /// Returns:
    /// - `Ok(Some(record))` — a valid, CRC-checked record.
    /// - `Ok(None)` — end of file or truncated trailing record (crash boundary).
    /// - `Err(_)` — CRC mismatch on a complete record (corruption).
    pub fn read_next(&mut self) -> io::Result<Option<WalRecord>> {
        // Encode the record into a buffer so we can verify the CRC over
        // the raw bytes, then decode from the verified buffer.
        //
        // We read: [tag (1B)][len (4B)][payload (len B)] as the record body,
        // followed by [crc32c (4B)].

        // Read tag byte.
        let mut tag = [0u8; 1];
        match self.reader.read_exact(&mut tag) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        // Read length prefix.
        let mut len_buf = [0u8; size_of::<u32>()];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let payload_len = u32::from_le_bytes(len_buf) as usize;

        // Read payload.
        let mut payload = vec![0u8; payload_len];
        match self.reader.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        // Read CRC.
        let mut crc_buf = [0u8; size_of::<u32>()];
        match self.reader.read_exact(&mut crc_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let stored_crc = u32::from_le_bytes(crc_buf);

        // Reconstruct the record bytes (tag + len + payload) and verify CRC.
        let mut record_bytes = Vec::with_capacity(1 + 4 + payload_len);
        record_bytes.extend_from_slice(&tag);
        record_bytes.extend_from_slice(&len_buf);
        record_bytes.extend_from_slice(&payload);

        let computed_crc = crc32fast::hash(&record_bytes);
        if computed_crc != stored_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL CRC mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x}"
                ),
            ));
        }

        // Decode from the verified bytes.
        let record = WalRecord::decode(&record_bytes[..])?;
        Ok(Some(record))
    }

    /// Reads all records and returns incomplete commits (intents without
    /// a matching complete record).
    ///
    /// This is the core recovery operation. Each returned [`WalRecord::CommitIntent`]
    /// represents a commit that was in flight when the process crashed — its
    /// `allocated_pages` should be returned to the freelist.
    pub fn find_incomplete_commits(mut self) -> io::Result<Vec<WalRecord>> {
        let mut intents: HashMap<u64, WalRecord> = HashMap::new();

        while let Some(record) = self.read_next()? {
            match &record {
                WalRecord::CommitIntent { seq, .. } => {
                    intents.insert(*seq, record);
                }
                WalRecord::CommitComplete { intent_seq, .. } => {
                    intents.remove(intent_seq);
                }
            }
        }

        Ok(intents.into_values().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::wal::writer::WalWriter;
    use tempfile::TempDir;

    #[test]
    fn read_written_records() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wal.log");

        // Write two records.
        {
            let mut writer = WalWriter::open(&path, 0).unwrap();
            writer
                .append(WalRecord::CommitIntent {
                    seq: 0,
                    tree_id: 1,
                    txn_id: 1,
                    new_root_id: 10,
                    allocated_pages: vec![10, 11],
                    retired_pages: vec![5],
                })
                .unwrap();
            writer
                .append(WalRecord::CommitComplete {
                    seq: 0,
                    intent_seq: 1,
                })
                .unwrap();
        }

        // Read them back.
        let mut reader = WalReader::open(&path).unwrap();
        let r1 = reader.read_next().unwrap().expect("should have record 1");
        let r2 = reader.read_next().unwrap().expect("should have record 2");
        assert!(reader.read_next().unwrap().is_none());

        assert!(matches!(r1, WalRecord::CommitIntent { .. }));
        assert!(matches!(r2, WalRecord::CommitComplete { .. }));
    }

    #[test]
    fn find_incomplete_commits_returns_unmatched_intents() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut writer = WalWriter::open(&path, 0).unwrap();
            // Intent 1 — will be completed.
            writer
                .append(WalRecord::CommitIntent {
                    seq: 0,
                    tree_id: 1,
                    txn_id: 1,
                    new_root_id: 10,
                    allocated_pages: vec![10],
                    retired_pages: vec![],
                })
                .unwrap();
            // Intent 2 — will NOT be completed (simulates crash).
            writer
                .append(WalRecord::CommitIntent {
                    seq: 0,
                    tree_id: 1,
                    txn_id: 2,
                    new_root_id: 20,
                    allocated_pages: vec![20, 21],
                    retired_pages: vec![10],
                })
                .unwrap();
            // Complete intent 1 only.
            writer
                .append(WalRecord::CommitComplete {
                    seq: 0,
                    intent_seq: 1,
                })
                .unwrap();
        }

        let reader = WalReader::open(&path).unwrap();
        let incomplete = reader.find_incomplete_commits().unwrap();

        assert_eq!(incomplete.len(), 1);
        match &incomplete[0] {
            WalRecord::CommitIntent {
                txn_id,
                allocated_pages,
                ..
            } => {
                assert_eq!(*txn_id, 2);
                assert_eq!(allocated_pages, &[20, 21]);
            }
            _ => panic!("expected CommitIntent"),
        }
    }
}
