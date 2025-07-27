use std::fs::{OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;

use crate::storage::PageStorage;
use crate::storage::metadata::INITIAL_PAGE_ID;
use crate::layout::PAGE_SIZE;

pub struct PageStore {
    file: std::fs::File,
    freed_pages: Vec<u64>,
    next_page_id: u64,
}

impl PageStore {
    pub fn flush(&mut self) -> Result<(), std::io::Error> {
        self.file.sync_data()
    }

    pub fn close(&mut self) -> Result<(), std::io::Error> {
        self.flush()?;
        Ok(())
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
    fn init<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> where
        Self: Sized {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(Self { file, freed_pages: Vec::new(), next_page_id: INITIAL_PAGE_ID as u64 })
    }

    fn read_page(&mut self, page_id: u64, target: &mut [u8; PAGE_SIZE]) -> Result<(), std::io::Error> {
        let offset = page_id * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(target)?;
        Ok(())
    }

    fn write_page(&mut self, data: &[u8]) -> Result<u64, std::io::Error> {
        assert_eq!(data.len(), PAGE_SIZE);
        let page_id = self.allocate_page()?;
        let offset = page_id * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        Ok(page_id)
    }

    fn write_page_at_offset(&mut self, offset: u64, data: &[u8]) -> Result<u64, std::io::Error> {
        assert_eq!(data.len(), PAGE_SIZE);
        let page_offset = offset * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(page_offset))?;
        self.file.write_all(data)?;
        Ok(offset / PAGE_SIZE as u64)
    }

    fn allocate_page(&mut self) -> Result<u64, std::io::Error> {
        if let Some(page_id) = self.freed_pages.pop() {
            Ok(page_id)
        } else {
            let page_id = self.next_page_id;
            self.next_page_id += 1;
            Ok(page_id)
        }
    }

    fn free_page(&mut self, page_id: u64) -> Result<(), std::io::Error> {
        if page_id < INITIAL_PAGE_ID.into() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot free initial pages",
            ));
        }
        self.freed_pages.push(page_id);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.flush()
    }
}
