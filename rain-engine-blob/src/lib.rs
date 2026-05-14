//! Blob backends for multimodal RainEngine attachments.
//!
//! The core only stores attachment references; this crate provides concrete
//! local and in-memory storage implementations plus config wiring.

use async_trait::async_trait;
use rain_engine_core::{
    AttachmentRef, BlobDescriptor, BlobStore, BlobStoreError, InMemoryBlobStore, MultimodalPayload,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub use rain_engine_core::{BlobStore as BlobStoreTrait, InMemoryBlobStore as MemoryBlobStore};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BlobBackendConfig {
    InMemory,
    LocalDirectory {
        path: String,
    },
    S3 {
        bucket: String,
        prefix: Option<String>,
    },
    Gcs {
        bucket: String,
        prefix: Option<String>,
    },
}

#[derive(Clone)]
pub struct LocalFileBlobStore {
    root: PathBuf,
}

impl LocalFileBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, BlobStoreError> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|err| BlobStoreError::new(err.to_string()))?;
        Ok(Self { root })
    }

    pub fn uri_for_path(path: &Path) -> String {
        format!("file://{}", path.display())
    }

    pub fn path_from_uri(uri: &str) -> Result<PathBuf, BlobStoreError> {
        uri.strip_prefix("file://")
            .map(PathBuf::from)
            .ok_or_else(|| BlobStoreError::new("unsupported local blob uri"))
    }
}

#[async_trait]
impl BlobStore for LocalFileBlobStore {
    async fn put(
        &self,
        attachment_id: String,
        payload: MultimodalPayload,
    ) -> Result<AttachmentRef, BlobStoreError> {
        let mut path = self
            .root
            .join(format!("{}_{}", attachment_id, Uuid::new_v4()));
        if let Some(file_name) = &payload.file_name {
            path.set_file_name(format!(
                "{}_{}_{}",
                attachment_id,
                Uuid::new_v4(),
                file_name
            ));
        }
        fs::write(&path, &payload.data).map_err(|err| BlobStoreError::new(err.to_string()))?;
        Ok(AttachmentRef::blob(
            attachment_id,
            payload.mime_type,
            payload.file_name,
            BlobDescriptor {
                uri: Self::uri_for_path(&path),
                size_bytes: payload.data.len(),
            },
        ))
    }

    async fn get(&self, descriptor: &BlobDescriptor) -> Result<MultimodalPayload, BlobStoreError> {
        let path = Self::path_from_uri(&descriptor.uri)?;
        let data = fs::read(&path).map_err(|err| BlobStoreError::new(err.to_string()))?;
        Ok(MultimodalPayload {
            mime_type: "application/octet-stream".to_string(),
            file_name: path
                .file_name()
                .map(|name| name.to_string_lossy().to_string()),
            data,
        })
    }
}

pub fn build_blob_store(config: &BlobBackendConfig) -> Result<Box<dyn BlobStore>, BlobStoreError> {
    match config {
        BlobBackendConfig::InMemory => Ok(Box::new(InMemoryBlobStore::new())),
        BlobBackendConfig::LocalDirectory { path } => Ok(Box::new(LocalFileBlobStore::new(path)?)),
        BlobBackendConfig::S3 { .. } => Err(BlobStoreError::new(
            "S3 blob backend is not implemented in this workspace build",
        )),
        BlobBackendConfig::Gcs { .. } => Err(BlobStoreError::new(
            "GCS blob backend is not implemented in this workspace build",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_file_store_round_trips() {
        let temp_dir = std::env::temp_dir().join(format!("rain-engine-blob-{}", Uuid::new_v4()));
        let store = LocalFileBlobStore::new(&temp_dir).expect("store");
        let reference = store
            .put(
                "attachment-1".to_string(),
                MultimodalPayload {
                    mime_type: "text/plain".to_string(),
                    file_name: Some("hello.txt".to_string()),
                    data: b"hello".to_vec(),
                },
            )
            .await
            .expect("put");
        let descriptor = match reference.content {
            rain_engine_core::AttachmentContent::Blob { descriptor } => descriptor,
            other => panic!("expected blob reference, got {other:?}"),
        };
        let payload = store.get(&descriptor).await.expect("get");
        assert_eq!(payload.data, b"hello");
    }
}
