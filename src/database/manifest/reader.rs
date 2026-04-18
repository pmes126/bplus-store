//! Sequential reader for the manifest log.

use crate::database::manifest::ManifestRec;
use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

/// Reads [`ManifestRec`] entries sequentially from a manifest log file.
pub struct ManifestReader {
    file: File,
}

impl ManifestReader {
    /// Opens an existing manifest log at `path` for sequential reading.
    pub fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            file: File::open(path)?,
        })
    }

    /// Reads and decodes the next CRC-framed record from the log.
    ///
    /// On-disk layout per record: `[tag: 1][len: 4][payload: len][crc32c: 4]`.
    ///
    /// Returns `Ok(None)` at a clean end of file (no more records) or if a
    /// trailing record is truncated (incomplete write from a crash).
    /// Returns `Err` if a complete record has a CRC mismatch (true corruption).
    pub fn read_next(&mut self) -> io::Result<Option<ManifestRec>> {
        // Read the tag byte. EOF here means no more records.
        let mut tag = [0u8; 1];
        match self.file.read_exact(&mut tag) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        // Read payload length. Truncation here means a partial write — treat
        // as end-of-valid-data.
        let mut len_buf = [0u8; 4];
        if let Err(e) = self.file.read_exact(&mut len_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(e);
        }
        let payload_len = u32::from_le_bytes(len_buf) as usize;

        // Read payload bytes.
        let mut payload = vec![0u8; payload_len];
        if let Err(e) = self.file.read_exact(&mut payload) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(e);
        }

        // Read and verify the trailing CRC-32C.
        let mut crc_buf = [0u8; 4];
        if let Err(e) = self.file.read_exact(&mut crc_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(e);
        }
        let stored_crc = u32::from_le_bytes(crc_buf);

        // Reconstruct the record bytes (tag + len + payload) and verify.
        let mut record_bytes = Vec::with_capacity(1 + 4 + payload_len);
        record_bytes.extend_from_slice(&tag);
        record_bytes.extend_from_slice(&len_buf);
        record_bytes.extend_from_slice(&payload);
        let computed_crc = crc32fast::hash(&record_bytes);

        if stored_crc != computed_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "manifest record CRC mismatch: stored {stored_crc:#010x}, \
                     computed {computed_crc:#010x}"
                ),
            ));
        }

        // Decode the record from the buffered bytes.
        ManifestRec::decode(&mut &record_bytes[..]).map(Some)
    }
}
