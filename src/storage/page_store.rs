use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use zerocopy::{AsBytes, FromZeroes};

use crate::layout::PAGE_SIZE;
use crate::storage::PageStorage;

pub use crate::storage::page_store;
pub use crate::storage::manifest::FreeListSnaphotHeader;

// Reserve first 16 pages for future use.
const INITIAL_PAGE_ID: u32 = 16;

const FREE_LIST_SNAPSHOT_MAGIC: u32 = 0x314C5346; // "FLS1" in little-endian

pub struct PageStore {
    file: Arc<File>,
    pub freed_pages: Mutex<Vec<u64>>,
    pub next_page_id: AtomicU64,
}

impl PageStore {
    pub fn flush(&self) -> Result<(), std::io::Error> {
        self.file.sync_data()
    }

    pub fn close(&self) -> Result<(), std::io::Error> {
        self.flush()
    }
}

impl Drop for PageStore {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            eprintln!("Error closing PageStore: {}", e);
        }
    }
}

impl PageStorage for PageStore {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error>
    where
        Self: Sized,
    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        Ok(Self {
            file: Arc::new(file),
            freed_pages: Mutex::new(Vec::new()),
            next_page_id: AtomicU64::new(INITIAL_PAGE_ID as u64),
        })
    }
    
    fn close(&self) -> Result<(), std::io::Error> {
        self.flush()
    }

    fn read_page(&self, page_id: u64, target: &mut [u8; PAGE_SIZE]) -> Result<(), std::io::Error> {
        let offset = page_id * PAGE_SIZE as u64;
        self.file.read_exact_at(target, offset)?;
        Ok(())
    }

    fn write_page(&self, data: &[u8]) -> Result<u64, std::io::Error> {
        assert_eq!(data.len(), PAGE_SIZE);
        let page_id = self.allocate_page()?;
        let offset = page_id * PAGE_SIZE as u64;
        self.file.write_all_at(data, offset)?;
        Ok(page_id)
    }

    fn write_page_at_offset(&self, offset: u64, data: &[u8]) -> Result<u64, std::io::Error> {
        assert_eq!(data.len(), PAGE_SIZE);
        let page_offset = offset * PAGE_SIZE as u64;
        self.file.write_all_at(data, page_offset)?;
        Ok(offset)
    }

    fn allocate_page(&self) -> Result<u64, std::io::Error> {
        let mut freed = self.freed_pages.lock().unwrap();
        if let Some(page_id) = freed.pop() {
            Ok(page_id)
        } else {
            Ok(self.next_page_id.fetch_add(1, Ordering::SeqCst))
        }
    }

    fn free_page(&self, page_id: u64) -> Result<(), std::io::Error> {
        if page_id < INITIAL_PAGE_ID.into() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot free initial pages",
            ));
        }
        let mut freed = self.freed_pages.lock().unwrap();
        freed.push(page_id);
        Ok(())
    }

    fn flush(&self) -> Result<(), std::io::Error> {
        self.flush()
    }

    fn set_next_page_id(&self, next_page_id: u64) -> Result<(), std::io::Error> {
        self.next_page_id.store(next_page_id, Ordering::SeqCst);
        Ok(())
    }

    fn set_freelist(&self, freed_pages: Vec<u64>) -> Result<(), std::io::Error> {
        let mut freed = self.freed_pages.lock().unwrap();
        *freed = freed_pages;
        Ok(())
    }
}


fn write_freepages_snapshot(path: &Path, version: u16, next_pid: u64, ids: &[u64]) -> Result<(), std::io::Error> {
    let f = OpenOptions::new().create(true).write(true).append(true).open(path)?;
    let hdr = FreeListSnaphotHeader { magic: FREE_LIST_SNAPSHOT_MAGIC, version, _pad: 0, next_page_id: next_pid, count: ids.len() as u32, _pad2: 0 };
    let mut w = BufWriter::new(f);
    // write header
    w.write_all(hdr.as_bytes())?;
    // write entries (u64 little-endian)
    for &pid in ids { w.write_all(&pid.to_le_bytes())?; }
    // crc over header+entries
    //let mut hasher = crc32c::Hasher::new();
    //hasher.update_file_region(&f, 0, mem::size_of::<FreeSnapHeader>() + ids.len()*8)?; // or buffer and hash
    //let crc = hasher.finalize();
    //f.write_all(&crc.to_le_bytes())?;
    Ok(())
}

fn read_freepages_snapshot(path: &Path) -> Result<(u16, u64, Vec<u64>), std::io::Error> {
    let mut f = File::open(path)?;
    let mut hdr = FreeListSnaphotHeader::new_zeroed();
    f.read_exact(hdr.as_bytes_mut())?; 

    if hdr.magic != FREE_LIST_SNAPSHOT_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Cannot free initial pages",
        ));
    }
    let mut ids = vec![0u64; hdr.count as usize];
    for i in 0..ids.len() {
        let mut b = [0u8;8]; f.read_exact(&mut b)?;
        ids[i] = u64::from_le_bytes(b);
    }
    //let mut crc_bytes = [0u8;4]; f.read_exact(&mut crc_bytes)?;
    //let crc_read = u32::from_le_bytes(crc_bytes);
    // recompute crc over header+entries
    // If mismatch → return Err or Ok with empty vec.
    Ok((hdr.version, hdr.next_page_id, ids))
    // For simplicity, skipping crc check here.
    // In production, implement crc check and return error if mismatch.
//    Ok((hdr.version, hdr.next_page_id, ids))
}
