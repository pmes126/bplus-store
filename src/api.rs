pub mod transport; 

use thiserror::Error;
use std::{fmt, str::FromStr};
use bytes::Bytes;
use std::ops::Bound;

#[repr(u64)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyEncodingId {
BeU64 = 0,
ZigZagI64 = 1,
Utf8 = 2,
RawBytes = 3,
}

impl fmt::Display for KeyEncodingId {
fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
f.write_str(match self {
KeyEncodingId::BeU64 => "be_u64",
KeyEncodingId::ZigZagI64 => "zigzag_i64",
KeyEncodingId::Utf8 => "utf8",
KeyEncodingId::RawBytes => "raw",
})
}
}

impl FromStr for KeyEncodingId {
type Err = String;
fn from_str(s: &str) -> Result<Self, Self::Err> {
match s {
"be_u64" => Ok(Self::BeU64),
"zigzag_i64" => Ok(Self::ZigZagI64),
"utf8" => Ok(Self::Utf8),
"raw" => Ok(Self::RawBytes),
other => Err(format!("unknown key encoding: {}", other)),
}
}
}

impl TryFrom<u64> for KeyEncodingId {
type Error = String;
fn try_from(value: u64) -> Result<Self, Self::Error> {
match value {
0 => Ok(Self::BeU64),
1 => Ok(Self::ZigZagI64),
2 => Ok(Self::Utf8),
3 => Ok(Self::RawBytes),
other => Err(format!("unknown key encoding id: {}", other)),
}
}
}


#[derive(Debug, Clone, Copy)]
pub struct KeyConstraints {
pub fixed_key_len: bool,
pub key_len: u32,
pub max_key_len: u32,
}

impl Default for KeyConstraints {
fn default() -> Self {
Self { fixed_key_len: false, key_len: 0, max_key_len: 1 << 20 }
}
}

#[derive(Debug, Error)]
pub enum ApiError {
#[error("transport: {0}")]
Transport(#[from] tonic::transport::Error),
#[error("rpc: {0}")]
Rpc(#[from] tonic::Status),
#[error("rpc: {0}")]
UnknownEncoding(String),
#[error("key type incompatible with tree encoding {0}")]
Decode(String),
#[error("range request requires end >= start in key order")]
BadRangeBounds,
}

pub type TreeId = u64;

#[derive(Clone, Copy, Debug)]
pub struct KeyLimits { pub min_len: u32, pub max_len: u32 }

pub type ResumeToken = Bytes;

#[derive(Clone, Copy, Debug)]
pub enum Order { Fwd, Rev }

#[derive(Clone, Debug)]
pub struct KeyRange<'a> {
    pub start: Bound<&'a [u8]>,
    pub end:   Bound<&'a [u8]>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum KeyFormatId {
    Raw,
    PrefixRestarts,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub struct KeyFormatParams {
    pub restart_interval: u16, // only meaningful for PrefixRestarts
}
