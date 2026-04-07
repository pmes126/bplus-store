use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

const SUPERBLOCK_MAGIC: u32 = 0x5355504552; // "SUPER" in little-endian
const SUPERBLOCK_VERSION: u32 = 1;
const SUPERBLOCK_SIZE: usize = std::mem::size_of::<Superblock>();

pub const FREELIST_SNAPSHOT_MAGIC: u32 = 0x314C5346; // "FLS1" in little-endian
pub const FREELIST_SNAPSHOT_VERSION: u16 = 1;
pub const FREELIST_SNAPSHOT_HEADER_SIZE: usize = std::mem::size_of::<FreeListSnaphotHeader>();

// Helpers for superblock and freelist snapshot management. Superblock is a fixed location page
// that stores critical info like the current freelist head and manifest location. The freelist
// snapshot is a simple file that stores the current freelist page IDs and next page ID, with a
// header and CRC for validation. Both are read on startup to initialize the PageStore and validate
// integrity.j
pub fn read_superblock(path: &std::path::Path, offset: u64) -> Result<Superblock, std::io::Error> {
    let page_path = path.join("pages.data");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .open(page_path)?;
    let mut buf = [0u8; size_of::<Superblock>()];
    file.read_exact_at(&mut buf, offset)?;
    let sb = Superblock::from_bytes(&buf)?;
    // Validate checksum
    //let checksum = sb.checksum;
    //let calculated_checksum = calculate_superblock_checksum(&sb);
    //if checksum != calculated_checksum {
    //    return Err(std::io::Error::new(
    //        std::io::ErrorKind::InvalidData,
    //        "Superblock checksum mismatch",
    //    ));
    //}
    Ok(*sb)
}

// TODO: implement this as a linked list of pages - after long operation freed pages may not fit in
// single page
pub fn write_freepages_snapshot(path: &Path, version: u16, next_pid: u64, ids: &[u64]) -> Result<(), std::io::Error> {
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true).truncate(true)
        .open(path)?;
    let hdr = FreeListSnaphotHeader { magic: 0x314C5346, version, _pad: 0, next_page_id: next_pid, count: ids.len() as u32, _pad2: 0 };
    // write header
    f.write_all(&hdr.as_bytes())?;
    // write entries (u64 little-endian)
    for &pid in ids { f.write_all(&pid.to_le_bytes())?; }
    // crc over header+entries
    //let mut hasher = crc32c::Hasher::new();
    //hasher.update_file_region(&f, 0, mem::size_of::<FreeListSnaphotHeader>() + ids.len()*8)?; // or buffer and hash
    //let crc = hasher.finalize();
    //f.write_all(&crc.to_le_bytes())?;
    Ok(())
}

pub fn read_freepages_snapshot(path: &Path, offset: u64) -> Result<(u64, Vec<u64>), std::io::Error> {
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .open(path)?;
    let mut buf = [0u8; FREELIST_SNAPSHOT_HEADER_SIZE];
    f.read_exact_at(&mut buf, offset)?;
    let hdr = FreeListSnaphotHeader::from_bytes(&buf)?;
    hdr.validate()?;
    let mut ids = vec![0u64; hdr.count as usize];
    for i in 0..ids.len() {
        let mut b = [0u8;8];
        f.read(&mut b)?;
        ids[i] = u64::from_le_bytes(b);
    }
    //let mut crc_bytes = [0u8;4]; f.read_exact(&mut crc_bytes)?;
    //let crc_read = u32::from_le_bytes(crc_bytes);
    // recompute crc over header+entries
    // (if you buffered, hash the buffer; otherwise re-read or mmap)
    // If mismatch → return Err or Ok with empty vec.
    Ok((hdr.next_page_id, ids))
}
// Superblock is the header of the manifest file. It contains metadata about the manifest, such as
// the magic number, version, generation ID, page size, next page ID, and the head of the freelist.
// It is used to validate the manifest file and to locate the freelist and the next page ID for
// allocation.
#[repr(C)]
#[derive(AsBytes, FromBytes, FromZeroes, Debug, Clone, Copy)]
pub struct Superblock {
    pub magic: u32,
    pub version: u32,
    pub gen_id: u64,
    pub page_size: u64,
    pub next_page_id: u64,
    pub freelist_head: u64, // 0 = none
    pub crc32c: u32,
    _pad:  u32,
}

// FreeListSnaphotHeader is the header of a page containing a snapshot of the freelist. It is used
// to reconstruct the freelist when loading the manifest, and to track the next page id to use for
// freelist snapshots.
#[repr(C)]
#[derive(AsBytes, FromBytes, FromZeroes, Debug, Clone, Copy)]
pub struct FreeListSnaphotHeader {
    pub magic: u32,         // b"FLS1"
    pub version: u16,       // 1
    pub _pad: u16,
    pub next_page_id: u64,
    pub count: u32,
    pub _pad2: u32,
}

impl Superblock {
    pub fn from_bytes(buf: &[u8; SUPERBLOCK_SIZE]) -> Result<&Self, std::io::Error> {
        Superblock::ref_from(buf).ok_or(io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to decode Superblock",
        ))
    }
    pub fn validate(&self) -> Result<(), std::io::Error> {
        if self.magic != SUPERBLOCK_MAGIC { // b"SUPER"
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Superblock magic"));
        }
        if self.version != SUPERBLOCK_VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Unsupported manifest version"));
        }
        Ok(())
    }
}

impl FreeListSnaphotHeader {
    pub fn from_bytes(buf: &[u8; FREELIST_SNAPSHOT_HEADER_SIZE]) -> Result<&Self, std::io::Error> {
        FreeListSnaphotHeader::ref_from(buf).ok_or(io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to decode FreeListSnaphotHeader",
        ))
    }
    pub fn validate(&self) -> Result<(), std::io::Error> {
        if self.magic != FREELIST_SNAPSHOT_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Snapshot header magic"));
        }
        if self.version != FREELIST_SNAPSHOT_VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Unsupported manifest version"));
        }
        Ok(())
    }
}
