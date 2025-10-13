pub mod service;
pub use service::{KvService, RawClient, TreeMeta};

#[cfg(feature = "transport-grpc")]
pub mod grpc;
#[cfg(feature = "transport-grpc")]
pub use grpc::GrpcService;

#[cfg(feature = "transport-inproc")]
pub mod inproc;
#[cfg(feature = "transport-inproc")]
pub use inproc::InprocService;

use std::{pin::Pin, sync::Arc};
use futures::Stream;

use crate::api::encoding::{KeyConstraints, KeyEncodingId};
use crate::api::errors::ApiError;

pub type ResumeToken = Bytes;
pub type TreeId = Bytes;

#[derive(Clone, Copy, Debug)]
pub enum Order { Fwd, Rev }

#[derive(Clone, Debug)]
pub struct KeyRange<'a> {
    pub start: Bound<&'a [u8]>, // Unbounded | Included(&[u8]) | Excluded(&[u8])
    pub end:   Bound<&'a [u8]>,
}

#[async_trait::async_trait] // if you target stable without GATs, keep this; see boxed variant below
pub trait KvService: Send + Sync + 'static {
    // Associated stream type so implementors can avoid boxing
    type RangeStream<'a>: Stream<Item = Result<(Bytes, Bytes), ApiError>> + Send + 'a
    where
        Self: 'a;

    async fn create_tree(&self, name: &str,
        enc: KeyEncodingId, kc: KeyConstraints
    ) -> Result<TreeMeta, ApiError>;

    async fn describe_tree(&self, name: &str) -> Result<TreeMeta, ApiError>;

    async fn put(&self, tree: &TreeId, key: &[u8], val: &[u8]) -> Result<(), ApiError>;

    async fn get(&self, tree: &TreeId, key: &[u8]) -> Result<Option<Bytes>, ApiError>;

    async fn del(&self, tree: &TreeId, key: &[u8]) -> Result<bool, ApiError>;

  // range scan with pagination
    async fn range(
        &self,
        tree: &TreeId,
        range: KeyRange<'_>,
        order: Order,
        limit: u32,
        resume: Option<ResumeToken>,
    ) -> Result<
        (
            // streaming out rows (zero-copy `Bytes`)
            std::pin::Pin<Box<dyn Stream<Item = Result<(Bytes, Bytes), ApiError>> + Send>>,
            // next page token (None if done)
            Option<ResumeToken>
        ),
        ApiError
    >;
}
