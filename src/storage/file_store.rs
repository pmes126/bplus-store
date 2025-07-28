use crate::bplustree::Node;
use crate::storage::{PageStorage, NodeStorage, MetadataStorage, Metadata, codec::DefaultNodeCodec, { KeyCodec, ValueCodec, NodeCodec, metadata, metadata::{MetadataPage, METADATA_PAGE_1, METADATA_PAGE_2}}};
use crate::layout::{PAGE_SIZE};
use anyhow::Result;
use std::path::Path;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

pub struct FileStore<S: PageStorage> {
    store: S,
}

impl<S: PageStorage> FileStore<S> {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        Ok(Self {
            store: S::init(path)?
        })
    }

}

impl<S: PageStorage> MetadataStorage for FileStore<S> {
    fn read_meta(&mut self, slot: u8) -> Result<MetadataPage, std::io::Error> {
        println!("Reading metadata from slot {}", slot);
        if slot > METADATA_PAGE_2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid metadata slot",
            ));
        }
        let mut buf = [0u8; PAGE_SIZE];
        self.store.read_page(slot as u64, &mut buf)?;

        let metadata = MetadataPage::from_bytes(&buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(*metadata) // Return a COPY of the metadata page
    }

    fn write_meta(&mut self, slot: u8, meta: &MetadataPage) -> Result<(), std::io::Error> {
        let buf = meta.as_bytes();
        self.store.write_page_at_offset(slot as u64, buf)?;
        Ok(())
    }

    fn read_current_root(&mut self) -> Result<u64, std::io::Error> {
        let meta0 = self.read_meta(METADATA_PAGE_1)?;
        let meta1 = self.read_meta(METADATA_PAGE_2)?;
        let root_node_id = if meta0.data.txn_id > meta1.data.txn_id {
            meta0.data.root_node_id
        } else {
            meta1.data.root_node_id
        };
        Ok(root_node_id)
    }

    // Commits a new root page ID to the metadata.
    fn commit_root(&mut self, new_root: u64, height: usize) -> Result<(), std::io::Error> {
        // select the lower txn_id metadata page
        let meta0 = self.read_meta(METADATA_PAGE_1)?;
        let meta1 = self.read_meta(METADATA_PAGE_2)?;
        let next_slot = if meta0.data.txn_id > meta1.data.txn_id { METADATA_PAGE_2 } else { METADATA_PAGE_1 };
        let order = meta0.data.order;

        let new_meta = metadata::new_metadata_page(
                new_root,
                meta0.data.txn_id.max(meta1.data.txn_id) + 1, // max txn_id + 1
                0, // checksum placeholder, should be calculated based on the new root
                order,
                height); // order placeholder, should be set based on the tree's order

        self.write_meta(next_slot, &new_meta)?;
        self.store.flush()?;

        Ok(())
    }
    fn get_metadata(&mut self) -> Result<Metadata, std::io::Error> {
        let meta0 = self.read_meta(METADATA_PAGE_1)?;
        let meta1 = self.read_meta(METADATA_PAGE_2)?;
        //println!("Current metadata: txn_id: {}, root_id: {}. order: {}", meta0.data.txn_id, meta0.data.root_node_id, meta0.data.order);
        if meta0.data.txn_id > meta1.data.txn_id {
            Ok(meta0.data)
        } else {
            Ok(meta1.data)
        }
    }
}

impl<S: PageStorage, K, V> NodeStorage<K, V> for FileStore<S>
    where
        K: KeyCodec + Ord,
        V: ValueCodec,
{
    fn read_node(&mut self, page_id: u64) -> Result<Option<Node<K, V>>, anyhow::Error>
    where
        K: KeyCodec,
        V: ValueCodec,
    {
        let mut buf = [0u8; PAGE_SIZE];
        self.store.read_page(page_id, &mut buf)?;
        DefaultNodeCodec::decode(&buf).
            map_or(Ok(None), |node| {
                    Ok(Some(node))
                }
            )
    }

    fn write_node(&mut self, node: &Node<K, V>) -> Result<u64, anyhow::Error>
    where
        K: KeyCodec,
        V: ValueCodec,
    {
        let buf = DefaultNodeCodec::encode(node)?;
        let res = self.store.write_page(&buf)?;
        Ok(res)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.store.flush()
    }


    fn free_node(&mut self, id: u64) -> Result<(), std::io::Error> {
        self.store.free_page(id)
    }
}
