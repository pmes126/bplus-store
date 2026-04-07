use zerocopy::{AsBytes};

use crate::layout::PAGE_SIZE;
use crate::storage::PageStorage;
use crate::store::metadata::{Metadata, MetadataPage, new_metadata_page, new_metadata_page_with_object, calculate_checksum};
// Manager for handling metadata pages, including reading, writing, and maintaining consistency
pub struct MetadataManager;

// MetadataStorage impl: read/write metadata pages, commit with txn_id and root/height/size,
// bootstrap new tree with initial metadata; read active meta by comparing txn_id; validate
// checksums on read; on write, calculate and store checksum.
impl MetadataManager {
    // Reads a metadata page from the given slot, validates its checksum, and returns the metadata
    fn read_metadata<S: PageStorage>(storage: &S, slot: u64) -> Result<MetadataPage, std::io::Error> {
        let mut buf = [0u8; PAGE_SIZE];
        storage.read_page(slot as u64, &mut buf)?;

        let metadata = MetadataPage::from_bytes(&buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // Validate checksum
        let checksum = metadata.data.checksum;
        let calculated_checksum = calculate_checksum(metadata);
        if checksum != calculated_checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Metadata checksum mismatch",
            ));
        }
        Ok(*metadata)
    }

    // Writes a metadata page to the given slot, calculating and setting the checksum before
    pub fn write_metadata<S: PageStorage>(storage: &S, slot: u64, meta: &mut MetadataPage) -> Result<(), std::io::Error> {
        let checksum = calculate_checksum(meta);
        meta.data.checksum = checksum;
        let buf = meta.as_bytes();
        storage.write_page_at_offset(slot as u64, buf)?;
        Ok(())
    }

    pub fn read_active_meta<S: PageStorage>(storage: &S, meta_a: u64, meta_b: u64) -> Result<Metadata, std::io::Error> {
        let meta0 = Self::read_metadata(storage, meta_a)?;
        let meta1 = Self::read_metadata(storage, meta_b)?;
        let active_meta = if meta0.data.txn_id >= meta1.data.txn_id {
            meta0
        } else {
            meta1
        };

        Ok(active_meta.data)
    }

    // Returns the most recent metadata based on the transaction ID, without validating the
    // checksum (for performance)
    pub fn get_metadata<S: PageStorage>(storage: &S, meta_a: u64, meta_b: u64) -> Result<Metadata, std::io::Error> {
        let meta0 = Self::read_metadata(storage, meta_a)?;
        let meta1 = Self::read_metadata(storage, meta_b)?;
        if meta0.data.txn_id >= meta1.data.txn_id {
            Ok(meta0.data)
        } else {
            Ok(meta1.data)
        }
    }

    // Commits new metadata by writing it to the specified slot, calculating the checksum
    pub fn commit_metadata<S: PageStorage>(
        storage: &S,
        slot: u64,
        txn_id: u64,
        id: u64,
        root: u64,
        height: usize,
        order: usize,
        size: usize,
    ) -> Result<(), std::io::Error> {
        let mut metadata_page = new_metadata_page(root, txn_id, id, 0, height, order, size);
        Self::write_metadata(storage,slot, &mut metadata_page)?;
        Ok(())
    }

    // Commits new metadata by writing a Metadata object to the specified slot, calculating the checksum
    pub fn commit_metadata_with_object<S: PageStorage>(
        storage: &S,
        slot: u64,
        metadata: &Metadata,
    ) -> Result<(), std::io::Error> {
        let mut metadata_page = new_metadata_page_with_object(metadata);
        Self::write_metadata(storage, slot, &mut metadata_page)?;
        Ok(())
    }

    // Initializes metadata for a new tree, allocating two metadata pages and writing initial
    // metadata to them
    fn bootstrap_metadata<S: PageStorage>(storage: &S, id: u64, order: usize) -> Result<(u64, u64, Metadata), std::io::Error> {
        let initial_txn_id = 1;
        let meta_a = storage.allocate_page()?;
        let meta_b = storage.allocate_page()?;
        let root_node_id = storage.allocate_page()?;

        let mut metadata_page_a = new_metadata_page(root_node_id, id, initial_txn_id, 0, 1, order, 0);
        let mut metadata_page_b = new_metadata_page(root_node_id, id, initial_txn_id - 1, 0, 1, order, 0);

        Self::write_metadata(storage, meta_a, &mut metadata_page_a)?;
        Self::write_metadata(storage, meta_b, &mut metadata_page_b)?;

        Ok((meta_a, meta_b, metadata_page_a.data))
    }
}
