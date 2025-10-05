use std::sync::Arc;
use crate::api::{
    raw::{RawClient, TreeHandle},
    transport::service::KvService,
    types::*,
};

#[derive(Clone)]
pub struct KvStore<T: KvService> {
    raw: RawClient<T>,
}

impl<T: KvService> KvStore<T> {
    pub fn new(svc: Arc<T>) -> Self {
        Self { raw: RawClient::new(svc) }
    }

    pub async fn create_tree(
        &self, name: &str, enc: KeyEncodingId, limits: Option<KeyLimits>
    ) -> Result<TreeHandle<T>, ApiError> {
        let meta = self.raw.create_tree(name, enc, limits).await?;
        Ok(self.raw.handle(&meta))
    }

    pub async fn open_tree(&self, name: &str) -> Result<TreeHandle<T>, ApiError> {
        let meta = self.raw.describe_tree(name).await?;
        Ok(self.raw.handle(&meta))
    }

    pub async fn describe_tree(&self, name: &str) -> Result<TreeMeta, ApiError> {
        self.raw.describe_tree(name).await
    }

    // Optional pass-through
    pub fn raw(&self) -> &RawClient<T> { &self.raw }
}

