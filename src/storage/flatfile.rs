use crate::bplustree::{Node, NodeId};
use crate::storage::NodeStorage;
use bincode;
use serde::{Serialize, de::DeserializeOwned};
use std::{fs::{File, OpenOptions}, io::{Read, Write, Seek, SeekFrom, Result}, collections::HashMap};

const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
struct OffSetEntry {
    offset: u64,
    length: u64,
}

#[derive(Debug)]
pub struct FlatFile<K, V> {
    file: File,
    index: HashMap<NodeId, OffSetEntry>, // node_id -> file offset
    next_offset: u64,
    _marker: std::marker::PhantomData<(K, V)>
}

// Implement a constructor for FlatFile
impl<K, V> FlatFile<K, V> {
    fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
        // Initialize the file and read existing entries
        Ok(
            Self {
                next_offset: file.seek(SeekFrom::End(0))?,
                file,
                index: HashMap::new(),
                _marker: std::marker::PhantomData,
            }
        )
    }
}

// Implement the NodeStorage trait for FlatFile
impl<K, V> NodeStorage<K, V> for FlatFile<K, V>
where K: Serialize + DeserializeOwned + Ord + Clone,
      V: Serialize + DeserializeOwned + Clone,
{
    // Read a node from the flat file by its ID
    fn read_node(&mut self, id: NodeId) -> Result<Node<K, V, NodeId>> {
        let entry = self.index.get(&id).expect("Missing offset entry");
        self.file.seek(SeekFrom::Start(entry.offset)).unwrap();

        // Read the length of the serialized data
        let mut len_buf = [0u8; 4];
        self.file.read_exact(&mut len_buf).unwrap();
        let length = u32::from_le_bytes(len_buf);

        // Read the serialized data
        let mut buf = vec![0u8; length as usize];
        self.file.read_exact(&mut buf)?;
        let val = bincode::deserialize(&buf);
        return val.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e));
    }

    // Write a node to the flat file and update the index
    fn write_node(&mut self, id: NodeId, node: &Node<K, V, NodeId>) -> Result<()> {
        let data = bincode::serialize(node).unwrap();
        let length = data.len() as u64;
        let offset = self.next_offset;

        self.file.seek(SeekFrom::Start(offset))?;
        // Write the length of the serialized data
        self.file.write_all(&length.to_le_bytes())?;
        // Pad data to next multiple of PAGE_SIZE
        let mut padded_data = data;
        let total_len = padded_data.len() + 4; // include length prefix
        // Calculate padding length - this handles the case where total_len is already a multiple
        // of PAGE_SIZE
        let pad_len = (PAGE_SIZE - (total_len % PAGE_SIZE)) % PAGE_SIZE;
        padded_data.extend(vec![0u8; pad_len]);
        // Write the serialized data
        self.file.write_all(&padded_data)?;
        self.file.flush()?;

        self.index.insert(id, OffSetEntry { offset, length });
        self.next_offset += length + pad_len as u64; // Update the next offset
        Ok(())
    }

    // Flush the file to ensure all changes are written
    fn flush(&mut self) -> Result<()> {
        self.file.flush()
    }

    // Get the root node ID (not implemented, just a placeholder)
    fn get_root(&self) -> Result<u64> {
        Ok(0) // Placeholder, should return the actual root node ID
    }
}
