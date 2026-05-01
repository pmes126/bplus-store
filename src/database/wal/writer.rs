//! WAL writer with group commit support.
//!
//! ## Direct mode (single-writer)
//!
//! [`WalWriter::append`] writes and fsyncs a single record. Simple but every
//! commit pays its own fsync latency.
//!
//! ## Group commit mode (multi-writer)
//!
//! [`WalGroupCommitter`] runs a background thread that batches WAL entries
//! from multiple concurrent writers into a single `write_all` + `fsync`.
//! Writers call [`WalGroupCommitter::submit`], which enqueues the record
//! and blocks until the batch containing their entry is durable.
//!
//! This amortises fsync cost: if 10 writers submit during one fsync window,
//! they share a single disk flush instead of 10 sequential ones.

use crate::database::wal::record::WalRecord;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

// =============================================================================
// Direct WAL writer (single-thread, synchronous)
// =============================================================================

/// Appends [`WalRecord`] entries to the WAL file with CRC-32C framing.
///
/// Each record is immediately flushed to the OS buffer. Call [`fsync`] to
/// force durability.
pub struct WalWriter {
    file: File,
    /// Next sequence number to assign.
    seq: u64,
}

impl WalWriter {
    /// Opens or creates the WAL file at `path`.
    pub fn open(path: &Path, start_seq: u64) -> io::Result<Self> {
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Self {
            file: f,
            seq: start_seq,
        })
    }

    /// Assigns a sequence number, encodes the record with a trailing CRC-32C,
    /// and appends it to the WAL file.
    ///
    /// Returns the assigned sequence number.
    pub fn append(&mut self, mut rec: WalRecord) -> io::Result<u64> {
        self.seq += 1;
        rec.set_seq(self.seq);

        let mut buf = Vec::new();
        rec.encode(&mut buf)?;
        let crc = crc32fast::hash(&buf);
        self.file.write_all(&buf)?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.flush()?;
        Ok(self.seq)
    }

    /// Flushes and syncs the WAL file to durable storage.
    pub fn fsync(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Truncates the WAL file, discarding all records.
    ///
    /// Called after all intents are confirmed complete (e.g. after recovery
    /// reclaims leaked pages, or periodically when the WAL grows large).
    pub fn truncate(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.seq = 0;
        Ok(())
    }
}

// =============================================================================
// Group commit (multi-writer, batched fsync)
// =============================================================================

/// A pending WAL entry submitted by a writer, waiting for durable flush.
struct PendingEntry {
    record: WalRecord,
    /// Notified with the assigned sequence number once the batch is durable.
    result: Arc<(Mutex<Option<io::Result<u64>>>, Condvar)>,
}

/// Batched WAL writer that amortises fsync across concurrent committers.
///
/// Writers call [`submit`] which enqueues a record and blocks until the
/// batch containing it is flushed to durable storage. A background thread
/// drains the queue in a loop:
///
/// 1. Wait for at least one entry.
/// 2. Drain all currently queued entries.
/// 3. Encode and write all records in a single `write_all`.
/// 4. `fsync` once.
/// 5. Notify all writers in the batch.
///
/// This means N concurrent commits share 1 fsync instead of N.
pub struct WalGroupCommitter {
    sender: mpsc::Sender<PendingEntry>,
    /// Join handle for the background writer thread.
    _handle: thread::JoinHandle<()>,
}

impl WalGroupCommitter {
    /// Spawns the background writer thread.
    ///
    /// `path` is the WAL file location; `start_seq` is the sequence number
    /// to resume from (obtained from WAL replay on recovery).
    pub fn start(path: &Path, start_seq: u64) -> io::Result<Self> {
        let mut writer = WalWriter::open(path, start_seq)?;
        let (sender, receiver) = mpsc::channel::<PendingEntry>();

        let handle = thread::spawn(move || {
            Self::writer_loop(&mut writer, &receiver);
        });

        Ok(Self {
            sender,
            _handle: handle,
        })
    }

    /// Submits a WAL record and blocks until it is durably written.
    ///
    /// Returns the assigned WAL sequence number, or an I/O error if the
    /// batch write or fsync failed.
    pub fn submit(&self, record: WalRecord) -> io::Result<u64> {
        let result = Arc::new((Mutex::new(None), Condvar::new()));
        let entry = PendingEntry {
            record,
            result: Arc::clone(&result),
        };

        self.sender.send(entry).map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "WAL writer thread has exited")
        })?;

        // Block until the background thread notifies us.
        let (lock, cvar) = &*result;
        let mut guard = lock.lock().unwrap();
        while guard.is_none() {
            guard = cvar.wait(guard).unwrap();
        }

        guard.take().unwrap()
    }

    /// The background writer loop.
    ///
    /// Drains pending entries, writes them as a batch, fsyncs once, then
    /// notifies all writers in the batch.
    fn writer_loop(writer: &mut WalWriter, receiver: &mpsc::Receiver<PendingEntry>) {
        loop {
            // Block waiting for the first entry.
            let first = match receiver.recv() {
                Ok(entry) => entry,
                Err(_) => return, // Channel closed, shut down.
            };

            // Drain any additional entries that arrived while we were
            // processing (non-blocking).
            let mut batch = vec![first];
            while let Ok(entry) = receiver.try_recv() {
                batch.push(entry);
            }

            // Write all records in the batch.
            let mut results: Vec<(Arc<(Mutex<Option<io::Result<u64>>>, Condvar)>, u64)> =
                Vec::with_capacity(batch.len());
            let mut write_failed = false;

            for entry in &mut batch {
                if write_failed {
                    // If a prior write in this batch failed, don't attempt
                    // more writes — just notify with the error.
                    let (lock, cvar) = &*entry.result;
                    let mut guard = lock.lock().unwrap();
                    *guard = Some(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "prior write in batch failed",
                    )));
                    cvar.notify_one();
                    continue;
                }

                match writer.append(entry.record.clone()) {
                    Ok(seq) => {
                        results.push((Arc::clone(&entry.result), seq));
                    }
                    Err(e) => {
                        write_failed = true;
                        let (lock, cvar) = &*entry.result;
                        let mut guard = lock.lock().unwrap();
                        *guard = Some(Err(e));
                        cvar.notify_one();
                    }
                }
            }

            // Single fsync for the entire batch.
            let fsync_result = writer.fsync();

            // Notify all successful writers.
            for (result, seq) in results {
                let (lock, cvar) = &*result;
                let mut guard = lock.lock().unwrap();
                *guard = Some(match &fsync_result {
                    Ok(()) => Ok(seq),
                    Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
                });
                cvar.notify_one();
            }
        }
    }
}
