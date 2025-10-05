// ==============================================
// FILE: src/api/raw.rs
// ==============================================
//! Thin bytes-only gRPC wrapper. Adjust `crate::pb` paths to your generated code.


use futures::{Stream, TryStreamExt};
use tonic::transport::{Channel, Endpoint};


use super::encoding::{KeyConstraints, KeyEncodingId};
use super::errors::ApiError;


// --- GENERATED FROM YOUR .proto ---
// Adjust this to whatever module `tonic-build` produced in your crate.
pub mod pb {
tonic::include_proto!("kv");
}

use pb::{CreateTreeRequest, DelRequest, DescribeTreeRequest, GetRequest, PutRequest, RangeRequest, Tree};

use crate::api::transport::service::KvService;
use crate::api::types::*;
use bytes::Bytes;
use futures_core::Stream;
use std::{pin::Pin, sync::Arc};

#[derive(Clone)]
pub struct RawClient<T: KvService> {
    svc: Arc<T>,
}
impl<T: KvService> RawClient<T> {
    pub fn new(svc: Arc<T>) -> Self { Self { svc } }
    pub async fn create_tree(&self, name: &str, enc: KeyEncodingId, limits: Option<KeyLimits>)
        -> Result<TreeMeta, ApiError> { self.svc.create_tree(name, enc, limits).await }
    pub async fn describe_tree(&self, name: &str)
        -> Result<TreeMeta, ApiError> { self.svc.describe_tree(name).await }
    pub fn handle(&self, meta: &TreeMeta) -> TreeHandle<T> {
        TreeHandle { svc: self.svc.clone(), id: meta.id.clone() }
    }
    pub async fn put(&self, tree: &TreeId, key: &[u8], val: &[u8])
        -> Result<(), ApiError> { self.svc.put(tree, key, val).await }
    pub async fn get(&self, tree: &TreeId, key: &[u8])
        -> Result<Option<Bytes>, ApiError> { self.svc.get(tree, key).await }
    pub async fn del(&self, tree: &TreeId, key: &[u8])
        -> Result<bool, ApiError> { self.svc.del(tree, key).await }
    pub async fn range(
        &self, tree: &TreeId, range: KeyRange<'_>, order: Order, limit: u32, resume: Option<ResumeToken>
    ) -> Result<(
        Pin<Box<dyn Stream<Item = Result<(Bytes, Bytes), ApiError>> + Send>>, Option<ResumeToken>
    ), ApiError> {
        self.svc.range(tree, range, order, limit, resume).await
    }
}

#[derive(Clone)]
pub struct TreeHandle<T: KvService> {
    pub(crate) svc: Arc<T>,
    pub id: TreeId,
}
impl<T: KvService> TreeHandle<T> {
    pub fn id(&self) -> &TreeId { &self.id }
    pub async fn put(&self, key: &[u8], val: &[u8]) -> Result<(), ApiError> {
        self.svc.put(&self.id, key, val).await
    }
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ApiError> {
        self.svc.get(&self.id, key).await
    }
    pub async fn del(&self, key: &[u8]) -> Result<bool, ApiError> {
        self.svc.del(&self.id, key).await
    }
    pub async fn range(
        &self, range: KeyRange<'_>, order: Order, limit: u32, resume: Option<ResumeToken>
    ) -> Result<(
        Pin<Box<dyn Stream<Item = Result<(Bytes, Bytes), ApiError>> + Send>>, Option<ResumeToken>
    ), ApiError> {
        self.svc.range(&self.id, range, order, limit, resume).await
    }
}

