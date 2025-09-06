use thiserror::Error;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("page {pid} not found")]
    NotFound { pid: u64 },

    #[error("page {pid} corrupted: {msg}")]
    Corrupted { pid: u64, msg: &'static str },

    #[error("out of space")]
    OutOfSpace,

    #[error("invariant: {0}")]
    Invariant(&'static str),
}
