use std::{fs::{File, OpenOptions}, io::{self, Write, Seek, SeekFrom}, path::Path, sync::Mutex};
use crc32c::crc32c;
use crate::storage::manifest::ManifestRec;

pub struct ManifestWriter {
    file: File,
    pub seq: u64,
}

impl ManifestWriter {
    pub fn open(path: &Path, start_seq: u64) -> io::Result<Self> {
        let mut f = OpenOptions::new().create(true).append(true).read(true).open(path)?;
        // optional: scan tail to detect last good seq; else trust start_seq from replay
        Ok(Self { file: f, seq: start_seq })
    }

    pub fn append(&mut self, mut rec: ManifestRec) -> io::Result<u64> {
        // assign seq
        self.seq += 1;
        set_seq(&mut rec, self.seq);

        // serialize (pick your codec; here is a placeholder bincode)
        let payload = bincode::serialize(&rec).unwrap(); // replace with robust opts
        let typ = rec_discriminant(&rec); // u8
        let len = (1 + payload.len() + 4) as u64; // type + payload + crc

        // frame: LEN(varint u64) | TYPE(u8) | PAYLOAD | CRC32C(u32)
        write_varu64(&mut self.file, len)?;
        self.file.write_all(&[typ])?;
        self.file.write_all(&payload)?;
        let crc = crc32c(std::iter::once(&[typ][..]).chain(std::iter::once(payload.as_slice())).flatten().copied().collect::<Vec<_>>().as_slice());
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.flush()?; // keep small; you can batch/fsync less often

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
        ManifestRec::UpdateRoot{seq: s, ..} => *s = seq,
        ManifestRec::RenameTree{seq: s, ..}  => *s = seq,
        ManifestRec::DropTree{seq: s, ..}    => *s = seq,
        ManifestRec::Checkpoint{seq: s}      => *s = seq,
    }
}
fn rec_discriminant(rec: &ManifestRec) -> u8 {
    match rec {
        ManifestRec::CreateTree{..} => 1,
        ManifestRec::UpdateRoot{..} => 2,
        ManifestRec::RenameTree{..} => 3,
        ManifestRec::DropTree{..}   => 4,
        ManifestRec::Checkpoint{..} => 5,
    }
}
fn write_varu64(f: &mut File, mut x: u64) -> io::Result<()> {
    let mut buf = [0u8; 10]; let mut i = 0;
    while x >= 0x80 { buf[i]=((x as u8)&0x7F)|0x80; x >>= 7; i+=1; }
    buf[i]=x as u8; f.write_all(&buf[..=i])
}

