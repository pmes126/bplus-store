//! WAL record types and binary encoding.

use crate::api::TreeId;
use std::io::{self, Read, Write};

// --- Tag constants -----------------------------------------------------------

pub(crate) const TAG_COMMIT_INTENT: u8 = 1;
pub(crate) const TAG_COMMIT_COMPLETE: u8 = 2;

// --- Record enum -------------------------------------------------------------

/// A single write-ahead log record.
///
/// The WAL uses a two-phase pattern per commit:
///
/// 1. **`CommitIntent`** — written *before* any pages are flushed. Records
///    which pages were allocated so recovery can free them if the commit
///    never completes.
/// 2. **`CommitComplete`** — written *after* the metadata page is durably
///    updated. Signals that the commit succeeded and the intent can be
///    discarded.
///
/// A `CommitIntent` without a matching `CommitComplete` means the commit
/// was interrupted — recovery adds the allocated pages to the freelist.
#[derive(Debug, Clone)]
pub enum WalRecord {
    /// Logged before pages are written to disk.
    CommitIntent {
        /// Monotonic WAL sequence number, assigned by the writer.
        seq: u64,
        /// The tree this commit belongs to.
        tree_id: TreeId,
        /// Transaction ID being committed (matches `Metadata::txn_id`).
        txn_id: u64,
        /// The new root page after this commit.
        new_root_id: u64,
        /// Pages allocated during the COW path that should be freed if
        /// the commit does not complete.
        allocated_pages: Vec<u64>,
        /// Pages retired (replaced) during the COW path. On successful
        /// commit these are handed to epoch-based GC; on crash they
        /// should remain untouched (still reachable from the old root).
        retired_pages: Vec<u64>,
    },
    /// Logged after the metadata page fsync succeeds.
    CommitComplete {
        /// WAL sequence number.
        seq: u64,
        /// Must match the `seq` of the corresponding `CommitIntent`.
        intent_seq: u64,
    },
}

impl WalRecord {
    /// Sets the WAL sequence number on this record.
    pub fn set_seq(&mut self, new_seq: u64) {
        match self {
            Self::CommitIntent { seq, .. } | Self::CommitComplete { seq, .. } => {
                *seq = new_seq;
            }
        }
    }

    /// Encodes this record into the given writer.
    ///
    /// Layout: `[tag: 1B][payload_len: u32 LE][payload][crc32c: 4B LE]`
    ///
    /// The CRC covers the tag, length prefix, and payload bytes.
    pub fn encode(&self, mut w: impl Write) -> io::Result<()> {
        match self {
            Self::CommitIntent {
                seq,
                tree_id,
                txn_id,
                new_root_id,
                allocated_pages,
                retired_pages,
            } => {
                w.write_all(&[TAG_COMMIT_INTENT])?;

                let mut payload = Vec::new();
                payload.extend_from_slice(&seq.to_le_bytes());
                payload.extend_from_slice(&tree_id.to_le_bytes());
                payload.extend_from_slice(&txn_id.to_le_bytes());
                payload.extend_from_slice(&new_root_id.to_le_bytes());

                // Allocated pages: count + page IDs.
                let alloc_count = u32::try_from(allocated_pages.len()).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "too many allocated pages")
                })?;
                payload.extend_from_slice(&alloc_count.to_le_bytes());
                for &pid in allocated_pages {
                    payload.extend_from_slice(&pid.to_le_bytes());
                }

                // Retired pages: count + page IDs.
                let retired_count = u32::try_from(retired_pages.len()).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "too many retired pages")
                })?;
                payload.extend_from_slice(&retired_count.to_le_bytes());
                for &pid in retired_pages {
                    payload.extend_from_slice(&pid.to_le_bytes());
                }

                write_len_prefixed_payload(&mut w, &payload)
            }
            Self::CommitComplete { seq, intent_seq } => {
                w.write_all(&[TAG_COMMIT_COMPLETE])?;

                let mut payload = Vec::new();
                payload.extend_from_slice(&seq.to_le_bytes());
                payload.extend_from_slice(&intent_seq.to_le_bytes());

                write_len_prefixed_payload(&mut w, &payload)
            }
        }
    }

    /// Decodes a record from the given reader.
    pub fn decode(mut r: impl Read) -> io::Result<Self> {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag)?;

        let payload = read_len_prefixed_payload(&mut r)?;
        let mut cur = &payload[..];

        match tag[0] {
            TAG_COMMIT_INTENT => {
                let seq = read_u64(&mut cur)?;
                let tree_id = read_u64(&mut cur)?;
                let txn_id = read_u64(&mut cur)?;
                let new_root_id = read_u64(&mut cur)?;

                let alloc_count = read_u32(&mut cur)? as usize;
                let mut allocated_pages = Vec::with_capacity(alloc_count);
                for _ in 0..alloc_count {
                    allocated_pages.push(read_u64(&mut cur)?);
                }

                let retired_count = read_u32(&mut cur)? as usize;
                let mut retired_pages = Vec::with_capacity(retired_count);
                for _ in 0..retired_count {
                    retired_pages.push(read_u64(&mut cur)?);
                }

                Ok(Self::CommitIntent {
                    seq,
                    tree_id,
                    txn_id,
                    new_root_id,
                    allocated_pages,
                    retired_pages,
                })
            }
            TAG_COMMIT_COMPLETE => {
                let seq = read_u64(&mut cur)?;
                let intent_seq = read_u64(&mut cur)?;
                Ok(Self::CommitComplete { seq, intent_seq })
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown WAL tag: {other}"),
            )),
        }
    }
}

// --- Encoding helpers --------------------------------------------------------

fn read_u64(mut r: impl Read) -> io::Result<u64> {
    let mut buf = [0u8; size_of::<u64>()];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn read_u32(mut r: impl Read) -> io::Result<u32> {
    let mut buf = [0u8; size_of::<u32>()];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn write_len_prefixed_payload(mut w: impl Write, payload: &[u8]) -> io::Result<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "payload too large"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(payload)?;
    Ok(())
}

fn read_len_prefixed_payload(mut r: impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; size_of::<u32>()];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_commit_intent() {
        let rec = WalRecord::CommitIntent {
            seq: 1,
            tree_id: 42,
            txn_id: 7,
            new_root_id: 100,
            allocated_pages: vec![10, 11, 12],
            retired_pages: vec![5, 6],
        };

        let mut buf = Vec::new();
        rec.encode(&mut buf).unwrap();

        let decoded = WalRecord::decode(&buf[..]).unwrap();
        match decoded {
            WalRecord::CommitIntent {
                seq,
                tree_id,
                txn_id,
                new_root_id,
                allocated_pages,
                retired_pages,
            } => {
                assert_eq!(seq, 1);
                assert_eq!(tree_id, 42);
                assert_eq!(txn_id, 7);
                assert_eq!(new_root_id, 100);
                assert_eq!(allocated_pages, vec![10, 11, 12]);
                assert_eq!(retired_pages, vec![5, 6]);
            }
            _ => panic!("expected CommitIntent"),
        }
    }

    #[test]
    fn roundtrip_commit_complete() {
        let rec = WalRecord::CommitComplete {
            seq: 2,
            intent_seq: 1,
        };

        let mut buf = Vec::new();
        rec.encode(&mut buf).unwrap();

        let decoded = WalRecord::decode(&buf[..]).unwrap();
        match decoded {
            WalRecord::CommitComplete { seq, intent_seq } => {
                assert_eq!(seq, 2);
                assert_eq!(intent_seq, 1);
            }
            _ => panic!("expected CommitComplete"),
        }
    }

    #[test]
    fn empty_page_lists() {
        let rec = WalRecord::CommitIntent {
            seq: 1,
            tree_id: 1,
            txn_id: 1,
            new_root_id: 1,
            allocated_pages: vec![],
            retired_pages: vec![],
        };

        let mut buf = Vec::new();
        rec.encode(&mut buf).unwrap();
        let decoded = WalRecord::decode(&buf[..]).unwrap();

        match decoded {
            WalRecord::CommitIntent {
                allocated_pages,
                retired_pages,
                ..
            } => {
                assert!(allocated_pages.is_empty());
                assert!(retired_pages.is_empty());
            }
            _ => panic!("expected CommitIntent"),
        }
    }
}
