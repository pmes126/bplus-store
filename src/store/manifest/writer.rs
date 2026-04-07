use std::{fs::{File, OpenOptions}, io::{self, Write}, path::Path};
use crate::store::manifest::ManifestRec;

pub struct ManifestWriter {
    file: File,
    pub seq: u64,
}

impl ManifestWriter {
    pub fn open(path: &Path, start_seq: u64) -> io::Result<Self> {
        let f = OpenOptions::new().create(true).append(true).read(true).open(path)?;
        // optional: scan tail to detect last good seq; else trust start_seq from replay
        Ok(Self { file: f, seq: start_seq })
    }

    pub fn append(&mut self, mut rec: ManifestRec) -> io::Result<u64> {
        // assign seq
        self.seq += 1;
        set_seq(&mut rec, self.seq);

        // write record: len + type + payload, TODO: add crc32c here
        rec.encode(self.file.by_ref())?; 
        self.file.flush()?;
        Ok(self.seq)
    }

    pub fn fsync(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}

// helpers (stub):
fn set_seq(rec: &mut ManifestRec, seq: u64) {
    match rec {
        ManifestRec::CreateTree{seq: s, ..} => *s = seq,
        ManifestRec::RenameTree{seq: s, ..} => *s = seq,
        ManifestRec::DeleteTree{seq: s, ..} => *s = seq,
        ManifestRec::Checkpoint{seq: s}     => *s = seq,
    }
}
