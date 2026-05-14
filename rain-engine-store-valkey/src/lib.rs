//! Valkey-backed coordination and scratchpad storage for RainEngine workers.
//!
//! Valkey is used for distributed claims, leases, and short-lived key/value
//! state. Durable agent state remains in the ledger store.

use async_trait::async_trait;
use rain_engine_core::{
    CoordinationClaim, CoordinationError, CoordinationStore, InMemoryCoordinationStore,
};
use redis::{Commands, Connection, cmd};
use serde_json::Value;
use std::time::Duration;

#[derive(Clone)]
pub enum ValkeyBackend {
    Redis(redis::Client),
    InMemory(InMemoryCoordinationStore),
}

#[derive(Clone)]
pub struct ValkeyCoordinationStore {
    namespace: String,
    backend: ValkeyBackend,
}

impl ValkeyCoordinationStore {
    pub fn connect(url: &str) -> Result<Self, CoordinationError> {
        Self::connect_with_namespace(url, "rain_engine")
    }

    pub fn connect_with_namespace(
        url: &str,
        namespace: impl Into<String>,
    ) -> Result<Self, CoordinationError> {
        let namespace = namespace.into();
        let backend = if url == "memory://" {
            ValkeyBackend::InMemory(InMemoryCoordinationStore::new())
        } else {
            ValkeyBackend::Redis(
                redis::Client::open(url).map_err(|err| CoordinationError::new(err.to_string()))?,
            )
        };
        Ok(Self { namespace, backend })
    }

    fn claim_key(&self, trigger_key: &str) -> String {
        format!("{}:claim:{trigger_key}", self.namespace)
    }

    fn reverse_claim_key(&self, claim_id: &str) -> String {
        format!("{}:claim_by_id:{claim_id}", self.namespace)
    }

    fn scratchpad_key(&self, namespace: &str, key: &str) -> String {
        format!("{}:scratchpad:{namespace}:{key}", self.namespace)
    }

    async fn with_connection<T, F>(&self, operation: F) -> Result<T, CoordinationError>
    where
        T: Send + 'static,
        F: FnOnce(Connection) -> Result<T, CoordinationError> + Send + 'static,
    {
        let ValkeyBackend::Redis(client) = &self.backend else {
            return Err(CoordinationError::new("redis backend not configured"));
        };
        let client = client.clone();
        tokio::task::spawn_blocking(move || {
            let connection = client
                .get_connection()
                .map_err(|err| CoordinationError::new(err.to_string()))?;
            operation(connection)
        })
        .await
        .map_err(|err| CoordinationError::new(err.to_string()))?
    }
}

#[async_trait]
impl CoordinationStore for ValkeyCoordinationStore {
    async fn claim_trigger(
        &self,
        trigger_key: &str,
        ttl: Duration,
    ) -> Result<Option<CoordinationClaim>, CoordinationError> {
        if let ValkeyBackend::InMemory(store) = &self.backend {
            return store.claim_trigger(trigger_key, ttl).await;
        }

        let claim_key = self.claim_key(trigger_key);
        let claim_id = uuid::Uuid::new_v4().to_string();
        let reverse_key = self.reverse_claim_key(&claim_id);
        let ttl_ms = ttl.as_millis() as usize;
        let trigger_key_owned = trigger_key.to_string();

        self.with_connection(move |mut connection| {
            let acquired: Option<String> = cmd("SET")
                .arg(&claim_key)
                .arg(&claim_id)
                .arg("NX")
                .arg("PX")
                .arg(ttl_ms)
                .query(&mut connection)
                .map_err(|err| CoordinationError::new(err.to_string()))?;
            if acquired.is_none() {
                return Ok(None);
            }
            let _: () = cmd("SET")
                .arg(&reverse_key)
                .arg(&trigger_key_owned)
                .arg("PX")
                .arg(ttl_ms)
                .query(&mut connection)
                .map_err(|err| CoordinationError::new(err.to_string()))?;
            Ok(Some(CoordinationClaim {
                claim_id,
                trigger_key: trigger_key_owned,
                expires_at: std::time::SystemTime::now() + ttl,
            }))
        })
        .await
    }

    async fn renew_claim(
        &self,
        claim_id: &str,
        ttl: Duration,
    ) -> Result<Option<CoordinationClaim>, CoordinationError> {
        if let ValkeyBackend::InMemory(store) = &self.backend {
            return store.renew_claim(claim_id, ttl).await;
        }

        let reverse_key = self.reverse_claim_key(claim_id);
        let claim_id = claim_id.to_string();
        let ttl_ms = ttl.as_millis() as usize;
        let namespace = self.namespace.clone();

        self.with_connection(move |mut connection| {
            let trigger_key: Option<String> = connection
                .get(&reverse_key)
                .map_err(|err| CoordinationError::new(err.to_string()))?;
            let Some(trigger_key) = trigger_key else {
                return Ok(None);
            };
            let claim_key = format!("{namespace}:claim:{trigger_key}");
            let current_claim: Option<String> = connection
                .get(&claim_key)
                .map_err(|err| CoordinationError::new(err.to_string()))?;
            if current_claim.as_deref() != Some(claim_id.as_str()) {
                return Ok(None);
            }
            let _: bool = cmd("PEXPIRE")
                .arg(&claim_key)
                .arg(ttl_ms)
                .query(&mut connection)
                .map_err(|err| CoordinationError::new(err.to_string()))?;
            let _: bool = cmd("PEXPIRE")
                .arg(&reverse_key)
                .arg(ttl_ms)
                .query(&mut connection)
                .map_err(|err| CoordinationError::new(err.to_string()))?;
            Ok(Some(CoordinationClaim {
                claim_id,
                trigger_key,
                expires_at: std::time::SystemTime::now() + ttl,
            }))
        })
        .await
    }

    async fn release_claim(&self, claim_id: &str) -> Result<(), CoordinationError> {
        if let ValkeyBackend::InMemory(store) = &self.backend {
            return store.release_claim(claim_id).await;
        }

        let reverse_key = self.reverse_claim_key(claim_id);
        let claim_id = claim_id.to_string();
        let namespace = self.namespace.clone();
        self.with_connection(move |mut connection| {
            let trigger_key: Option<String> = connection
                .get(&reverse_key)
                .map_err(|err| CoordinationError::new(err.to_string()))?;
            if let Some(trigger_key) = trigger_key {
                let claim_key = format!("{namespace}:claim:{trigger_key}");
                let current_claim: Option<String> = connection
                    .get(&claim_key)
                    .map_err(|err| CoordinationError::new(err.to_string()))?;
                if current_claim.as_deref() == Some(claim_id.as_str()) {
                    let _: usize = connection
                        .del(&claim_key)
                        .map_err(|err| CoordinationError::new(err.to_string()))?;
                }
            }
            let _: usize = connection
                .del(&reverse_key)
                .map_err(|err| CoordinationError::new(err.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn scratchpad_get(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<Value>, CoordinationError> {
        if let ValkeyBackend::InMemory(store) = &self.backend {
            return store.scratchpad_get(namespace, key).await;
        }

        let scratchpad_key = self.scratchpad_key(namespace, key);
        self.with_connection(move |mut connection| {
            let value: Option<String> = connection
                .get(&scratchpad_key)
                .map_err(|err| CoordinationError::new(err.to_string()))?;
            value
                .map(|value| {
                    serde_json::from_str(&value)
                        .map_err(|err| CoordinationError::new(err.to_string()))
                })
                .transpose()
        })
        .await
    }

    async fn scratchpad_set(
        &self,
        namespace: &str,
        key: &str,
        value: Value,
        ttl: Duration,
    ) -> Result<(), CoordinationError> {
        if let ValkeyBackend::InMemory(store) = &self.backend {
            return store.scratchpad_set(namespace, key, value, ttl).await;
        }

        let scratchpad_key = self.scratchpad_key(namespace, key);
        let value =
            serde_json::to_string(&value).map_err(|err| CoordinationError::new(err.to_string()))?;
        let ttl_secs = ttl.as_secs().max(1);
        self.with_connection(move |mut connection| {
            let _: () = cmd("SETEX")
                .arg(&scratchpad_key)
                .arg(ttl_secs)
                .arg(value)
                .query(&mut connection)
                .map_err(|err| CoordinationError::new(err.to_string()))?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rain_engine_core::CoordinationStore;

    #[tokio::test]
    async fn in_memory_backend_supports_claims_and_scratchpad() {
        let store = ValkeyCoordinationStore::connect("memory://").expect("store");
        let claim = store
            .claim_trigger("trigger-1", Duration::from_secs(30))
            .await
            .expect("claim")
            .expect("some claim");
        assert!(
            store
                .claim_trigger("trigger-1", Duration::from_secs(30))
                .await
                .expect("second claim")
                .is_none()
        );
        store
            .scratchpad_set(
                "ns",
                "key",
                serde_json::json!({"value": 1}),
                Duration::from_secs(60),
            )
            .await
            .expect("set");
        assert_eq!(
            store.scratchpad_get("ns", "key").await.expect("get"),
            Some(serde_json::json!({"value": 1}))
        );
        store.release_claim(&claim.claim_id).await.expect("release");
    }
}
