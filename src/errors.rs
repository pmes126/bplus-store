use thiserror::Error;

/// Crate-level Result alias.
pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Debug, Error)]
pub enum KvError {
    #[error("key not found")]
    NotFound,

    #[error("stale base version (commit raced)")]
    StaleBase,

    /// Any engine/internal error we haven't categorized yet.
    #[error(transparent)]
    Engine(#[from] anyhow::Error),
}

// Map engine commit errors into our public error.
impl From<crate::bplustree::tree::CommitError> for KvError {
    fn from(e: crate::bplustree::tree::CommitError) -> Self {
        match e {
            crate::bplustree::tree::CommitError::StaleBase => KvError::StaleBase,
            _ => KvError::Engine(anyhow::anyhow!(e.to_string())),
        }
    }
}
