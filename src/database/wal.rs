//! Write-ahead log for crash-safe commits.
//!
//! The WAL records commit intents *before* pages are written, so recovery can
//! detect and clean up incomplete commits (leaked pages). It lives in a
//! separate file (`wal.log`) from the manifest log, because:
//!
//! - The manifest records rare catalog mutations (create/delete/rename tree).
//! - The WAL records every data commit — much higher write frequency.
//! - Each file has its own truncation/compaction lifecycle.
//!
//! ## Wire format
//!
//! Same CRC-framed layout as the manifest log:
//!
//! ```text
//! [tag: 1B][len: u32 LE][payload: len bytes][crc32c: 4B LE]
//! ```
//!
//! ## Group commit
//!
//! Multiple concurrent writers submit [`WalEntry`] values to a shared queue.
//! A dedicated WAL writer thread drains the queue, batches entries into a
//! single `write_all` + `fsync`, and notifies each writer via a oneshot
//! channel. This amortises fsync cost across concurrent commits.
//!
//! ## Recovery
//!
//! On [`Database::open`], the WAL is replayed after the manifest:
//!
//! 1. Read all complete records (truncated trailing record = crash, skip it).
//! 2. For each `CommitIntent` without a matching `CommitComplete`:
//!    - The commit was in flight when the process crashed.
//!    - The allocated pages listed in `allocated_pages` are leaked — add them
//!      to the freelist.
//! 3. Truncate the WAL file (all durable state is in the metadata pages).

pub mod record;
pub mod reader;
pub mod writer;
