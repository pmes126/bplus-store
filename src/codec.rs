pub mod bincode;

use crate::bplustree::node::Node;
use crate::layout::PAGE_SIZE;
use thiserror::Error;

// Trait for node storage trasnformation
pub trait KeyCodec<K: ?Sized + ToOwned> {
    /// Append an order-preserving encoding of `key` to `out`.
    fn encode_key(key: &K, out: &mut [u8]) -> Result<usize, CodecError>;

    /// Decode from the exact encoded key bytes.
    fn decode_key(bytes: &[u8]) -> Result<<K as ToOwned>::Owned, CodecError>;

    /// Compare two *encoded* keys. Default: bytewise lexicographic.
    fn compare_encoded(a: &[u8], b: &[u8]) -> core::cmp::Ordering { a.cmp(b) }
    
    /// Return the length of the encoded key.
    fn encoded_len(key: &K) -> usize;
}

pub trait ValueCodec<V: ?Sized + ToOwned> {
    fn encode_value( value: &V, out: &mut [u8]) -> Result<usize, CodecError>;
    fn decode_value( bytes: &[u8]) -> Result<<V as ToOwned>::Owned, CodecError>;
    fn encoded_len(value: &V) -> usize;
}


/// Trait for encoding/decoding nodes to/from fixed-size pages
pub trait NodeCodec<K, V, KC, VC>: Send + Sync + 'static
where
    K:  ToOwned,
    V:  ToOwned,
    KC: KeyCodec<K>,
    VC: ValueCodec<V>,
{
    fn encode(node: &Node<K, V>) -> Result<[u8; PAGE_SIZE], CodecError>;
    fn decode(buf: &[u8; PAGE_SIZE]) -> Result<Node<K, V>, CodecError>;
}

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("Error decoding value: {msg}")]
    DecodeFailure { msg: String },

    #[error("Error encoding value: {msg}")]
    EncodeFailure { msg: String },

    #[error("Error converting from byte slice: {source}")]
    FromSliceError {
        #[from]
        source: std::array::TryFromSliceError,
    },

    #[error("Truncated slice")]
    Truncated{},

    #[error("IO error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },
}
