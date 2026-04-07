pub mod reader;
pub mod writer;

use crate::api::{TreeId, KeyEncodingId, KeyLimits};
use crate::keyfmt::KeyFormat;
use std::io::{self, Read, Write};

const TAG_CREATE_TREE: u8 = 1;
const TAG_DELETE_TREE: u8 = 2;
const TAG_RENAME_TREE: u8 = 3;
// ManifestRec represents a record in the manifest log. It is used to track the state of the trees
// in the store, and to reconstruct the state of the store when loading the manifest. Each record
// has a sequence number (seq) that is used to order the records in the log. The manifest log is
// append-only, and each record is immutable once written.
#[derive(Debug, Clone)]
pub enum ManifestRec {
    CreateTree {
        seq: u64,
        id: TreeId,
        meta_a : u64,
        meta_b : u64,
        name: String,
        key_encoding: KeyEncodingId,
        key_format: KeyFormat,
        encoding_version: u16,
        key_limits: Option<KeyLimits>,
        order: u64,
        root_id: u64,
        height: u64,
        size: u64,
    },
    RenameTree { seq: u64, id: TreeId, new_name: String },
    DeleteTree { seq: u64, id: TreeId },
    Checkpoint { seq: u64 },
}

pub struct ManifestLog {
    pub recs: Vec<ManifestRec>,
}

impl ManifestRec {
    pub fn encode(&self, mut w: impl Write) -> io::Result<()> {
        match self {
            ManifestRec::CreateTree {
                seq,
                id,
                name,
                meta_a,
                meta_b,
                key_encoding,
                key_format,
                encoding_version,
                key_limits,
                order,
                root_id,
                height,
                size,
            } => {
                w.write_all(&[TAG_CREATE_TREE])?;

                let mut payload = Vec::new();
                payload.extend_from_slice(&seq.to_le_bytes());
                payload.extend_from_slice(&id.to_le_bytes());
                write_string(&mut payload, name)?;
                payload.extend_from_slice(&meta_a.to_le_bytes());
                payload.extend_from_slice(&meta_b.to_le_bytes());
                payload.extend_from_slice(&(*key_encoding as u64).to_le_bytes());
                payload.extend_from_slice(&key_format.id().to_le_bytes());
                payload.extend_from_slice(&encoding_version.to_le_bytes());
                payload.extend_from_slice(&order.to_le_bytes());
                payload.extend_from_slice(&root_id.to_le_bytes());
                payload.extend_from_slice(&height.to_le_bytes());
                payload.extend_from_slice(&size.to_le_bytes());
                    if let Some(limits) = key_limits {
                        payload.push(1); // has limits
                        payload.extend_from_slice(&limits.min_len.to_le_bytes());
                        payload.extend_from_slice(&limits.max_len.to_le_bytes());
                    } else {
                        payload.push(0); // no limits
                    }

                write_len_prefixed_payload(&mut w, &payload)
            }
            ManifestRec::DeleteTree { seq, id } => {
                w.write_all(&[TAG_DELETE_TREE])?;

                let payload = id.to_le_bytes();
                write_len_prefixed_payload(&mut w, &payload)
            }
            ManifestRec::RenameTree { seq, id, new_name } => {
                w.write_all(&[TAG_RENAME_TREE])?;

                let mut payload = Vec::new();
                payload.extend_from_slice(&id.to_le_bytes());
                write_string(&mut payload, new_name)?;

                write_len_prefixed_payload(&mut w, &payload)
            }
            ManifestRec::Checkpoint { seq } => {
                w.write_all(&[0])?; // no tag for checkpoint, just a seq update
                let payload = seq.to_le_bytes();
                write_len_prefixed_payload(&mut w, &payload)
            }
        }
    }

    pub fn decode(mut r: impl Read) -> io::Result<Self> {
        let mut tag = [0u8; size_of::<u8>()];
        r.read_exact(&mut tag)?;

        let payload = read_len_prefixed_payload(&mut r)?;
        let mut cur = &payload[..];

        match tag[0] {
            TAG_CREATE_TREE => {
                let seq = read_u64(&mut cur)?; // seq is determined by the writer, not stored in the payload
                let id = read_u64(&mut cur)?;
                let name = read_string(&mut cur)?;
                let meta_a = read_u64(&mut cur)?;
                let meta_b = read_u64(&mut cur)?;
                let key_encoding = KeyEncodingId::try_from(read_u64(&mut cur)?).map_err(|_| io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid key encoding id",
                ))?;
                let key_format_id = read_u64(&mut cur)? as u16;
                let encoding_version = read_u64(&mut cur)? as u16;
                let order = read_u64(&mut cur)?;
                let root_id = read_u64(&mut cur)?;
                let height = read_u64(&mut cur)?;
                let size = read_u64(&mut cur)?;
                let has_limits = {
                    let mut b = [0u8;1];
                    cur.read_exact(&mut b)?;
                    b[0] != 0
                };
                let key_limits = if has_limits {
                    let min_len = read_u64(&mut cur)? as u32;
                    let max_len = read_u64(&mut cur)? as u32;
                    Some(KeyLimits { min_len, max_len })
                } else {
                    None
                };
                let key_format = KeyFormat::from_id(key_format_id as u8).ok_or(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown key format id: {}", key_format_id),
                ))?;


                Ok(Self::CreateTree {
                    seq,
                    id,
                    name,
                    meta_a,
                    meta_b,
                    key_encoding,
                    key_format,
                    encoding_version,
                    key_limits,
                    order,
                    root_id,
                    height,
                    size,
                })
            }
            TAG_DELETE_TREE => {
                let seq = read_u64(&mut cur)?; // seq is determined by the writer, not stored in the payload
                let id = read_u64(&mut cur)?;
                Ok(Self::DeleteTree { seq, id })
            }
            TAG_RENAME_TREE => {
                let seq = read_u64(&mut cur)?; // seq is determined by the writer, not stored in
                // the payload
                let id = read_u64(&mut cur)?;
                let new_name = read_string(&mut cur)?;
                Ok(Self::RenameTree { seq, id, new_name })
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown manifest tag: {}", other),
            )),
        }
    }
}

fn read_u64(mut r: impl Read) -> io::Result<u64> {
    let mut buf = [0u8; size_of::<u64>()];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

#[inline]
fn write_u64(mut w: impl Write, val: u64) -> io::Result<()> {
    w.write_all(&val.to_le_bytes())
}

fn write_string(mut w: impl Write, s: &str) -> io::Result<()> {
    let bytes = s.as_bytes();
    let len = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "string too long"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(bytes)?;
    Ok(())
}

fn read_string(mut r: impl Read) -> io::Result<String> {
    let mut len_buf = [0u8; size_of::<u32>()];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut str_buf = vec![0u8; len];
    r.read_exact(&mut str_buf)?;
    String::from_utf8(str_buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn write_len_prefixed_payload(mut w: impl Write, payload: &[u8]) -> io::Result<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "payload too large"))?;

    w.write_all(&len.to_le_bytes())?;
    w.write_all(payload)?;
    Ok(())
}

fn read_len_prefixed_payload(mut r: impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; size_of::<u32>()];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;
    Ok(payload)
}

