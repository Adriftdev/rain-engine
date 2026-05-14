use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{message}")]
pub struct CoordinationError {
    pub message: String,
}

impl CoordinationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoordinationClaim {
    pub claim_id: String,
    pub trigger_key: String,
    pub expires_at: SystemTime,
}

#[async_trait]
pub trait CoordinationStore: Send + Sync {
    async fn claim_trigger(
        &self,
        trigger_key: &str,
        ttl: Duration,
    ) -> Result<Option<CoordinationClaim>, CoordinationError>;

    async fn renew_claim(
        &self,
        claim_id: &str,
        ttl: Duration,
    ) -> Result<Option<CoordinationClaim>, CoordinationError>;

    async fn release_claim(&self, claim_id: &str) -> Result<(), CoordinationError>;

    async fn scratchpad_get(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<Value>, CoordinationError>;

    async fn scratchpad_set(
        &self,
        namespace: &str,
        key: &str,
        value: Value,
        ttl: Duration,
    ) -> Result<(), CoordinationError>;
}

#[derive(Clone, Default)]
pub struct InMemoryCoordinationStore {
    claims: Arc<RwLock<HashMap<String, InMemoryClaim>>>,
    scratchpad: Arc<RwLock<HashMap<(String, String), InMemoryScratchpadValue>>>,
}

#[derive(Clone)]
struct InMemoryClaim {
    claim_id: String,
    expires_at: Instant,
}

#[derive(Clone)]
struct InMemoryScratchpadValue {
    value: Value,
    expires_at: Instant,
}

impl InMemoryCoordinationStore {
    pub fn new() -> Self {
        Self::default()
    }

    async fn prune_expired(&self) {
        let now = Instant::now();
        self.claims
            .write()
            .await
            .retain(|_, claim| claim.expires_at > now);
        self.scratchpad
            .write()
            .await
            .retain(|_, entry| entry.expires_at > now);
    }
}

#[async_trait]
impl CoordinationStore for InMemoryCoordinationStore {
    async fn claim_trigger(
        &self,
        trigger_key: &str,
        ttl: Duration,
    ) -> Result<Option<CoordinationClaim>, CoordinationError> {
        self.prune_expired().await;
        let mut claims = self.claims.write().await;
        if claims.contains_key(trigger_key) {
            return Ok(None);
        }
        let claim_id = Uuid::new_v4().to_string();
        let expires_at = Instant::now() + ttl;
        claims.insert(
            trigger_key.to_string(),
            InMemoryClaim {
                claim_id: claim_id.clone(),
                expires_at,
            },
        );
        Ok(Some(CoordinationClaim {
            claim_id,
            trigger_key: trigger_key.to_string(),
            expires_at: SystemTime::now() + ttl,
        }))
    }

    async fn renew_claim(
        &self,
        claim_id: &str,
        ttl: Duration,
    ) -> Result<Option<CoordinationClaim>, CoordinationError> {
        self.prune_expired().await;
        let mut claims = self.claims.write().await;
        let Some((trigger_key, claim)) = claims
            .iter_mut()
            .find(|(_, claim)| claim.claim_id == claim_id)
        else {
            return Ok(None);
        };
        claim.expires_at = Instant::now() + ttl;
        Ok(Some(CoordinationClaim {
            claim_id: claim_id.to_string(),
            trigger_key: trigger_key.clone(),
            expires_at: SystemTime::now() + ttl,
        }))
    }

    async fn release_claim(&self, claim_id: &str) -> Result<(), CoordinationError> {
        self.prune_expired().await;
        let mut claims = self.claims.write().await;
        if let Some(key) = claims
            .iter()
            .find_map(|(key, claim)| (claim.claim_id == claim_id).then_some(key.clone()))
        {
            claims.remove(&key);
        }
        Ok(())
    }

    async fn scratchpad_get(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<Value>, CoordinationError> {
        self.prune_expired().await;
        Ok(self
            .scratchpad
            .read()
            .await
            .get(&(namespace.to_string(), key.to_string()))
            .map(|entry| entry.value.clone()))
    }

    async fn scratchpad_set(
        &self,
        namespace: &str,
        key: &str,
        value: Value,
        ttl: Duration,
    ) -> Result<(), CoordinationError> {
        self.prune_expired().await;
        self.scratchpad.write().await.insert(
            (namespace.to_string(), key.to_string()),
            InMemoryScratchpadValue {
                value,
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(())
    }
}
