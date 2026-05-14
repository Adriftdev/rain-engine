use crate::{AttachmentRef, BlobDescriptor, MultimodalPayload};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{message}")]
pub struct BlobStoreError {
    pub message: String,
}

impl BlobStoreError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put(
        &self,
        attachment_id: String,
        payload: MultimodalPayload,
    ) -> Result<AttachmentRef, BlobStoreError>;

    async fn get(&self, descriptor: &BlobDescriptor) -> Result<MultimodalPayload, BlobStoreError>;
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryBlobStore {
    values: Arc<RwLock<HashMap<String, MultimodalPayload>>>,
}

impl InMemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl BlobStore for InMemoryBlobStore {
    async fn put(
        &self,
        attachment_id: String,
        payload: MultimodalPayload,
    ) -> Result<AttachmentRef, BlobStoreError> {
        let uri = format!("memory://{}", Uuid::new_v4());
        self.values
            .write()
            .await
            .insert(uri.clone(), payload.clone());
        Ok(AttachmentRef::blob(
            attachment_id,
            payload.mime_type,
            payload.file_name,
            BlobDescriptor {
                uri,
                size_bytes: payload.data.len(),
            },
        ))
    }

    async fn get(&self, descriptor: &BlobDescriptor) -> Result<MultimodalPayload, BlobStoreError> {
        self.values
            .read()
            .await
            .get(&descriptor.uri)
            .cloned()
            .ok_or_else(|| BlobStoreError::new("blob not found"))
    }
}
