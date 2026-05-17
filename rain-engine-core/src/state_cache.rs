use crate::{SessionSnapshot, StateProjectionCache};
use async_trait::async_trait;
use redis::AsyncCommands;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
pub struct InMemoryStateCache {
    cache: Arc<RwLock<HashMap<String, SessionSnapshot>>>,
}

impl InMemoryStateCache {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StateProjectionCache for InMemoryStateCache {
    async fn get_projection(&self, session_id: &str) -> Result<Option<SessionSnapshot>, String> {
        let lock = self.cache.read().map_err(|e| e.to_string())?;
        Ok(lock.get(session_id).cloned())
    }

    async fn set_projection(
        &self,
        session_id: &str,
        snapshot: SessionSnapshot,
    ) -> Result<(), String> {
        let mut lock = self.cache.write().map_err(|e| e.to_string())?;
        lock.insert(session_id.to_string(), snapshot);
        Ok(())
    }

    async fn invalidate(&self, session_id: &str) -> Result<(), String> {
        let mut lock = self.cache.write().map_err(|e| e.to_string())?;
        lock.remove(session_id);
        Ok(())
    }
}

pub struct ValkeyStateCache {
    client: redis::Client,
    prefix: String,
}

impl ValkeyStateCache {
    pub fn new(url: &str, prefix: &str) -> Result<Self, String> {
        let client = redis::Client::open(url).map_err(|e| e.to_string())?;
        Ok(Self {
            client,
            prefix: prefix.to_string(),
        })
    }

    fn key(&self, session_id: &str) -> String {
        format!("{}:state:{}", self.prefix, session_id)
    }
}

#[async_trait]
impl StateProjectionCache for ValkeyStateCache {
    async fn get_projection(&self, session_id: &str) -> Result<Option<SessionSnapshot>, String> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        let key = self.key(session_id);
        let val: Option<Vec<u8>> = conn.get(&key).await.map_err(|e| e.to_string())?;

        match val {
            Some(bytes) => {
                let snapshot =
                    serde_json::from_slice(&bytes).map_err(|e| format!("De-serialize error: {}", e))?;
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }

    async fn set_projection(
        &self,
        session_id: &str,
        snapshot: SessionSnapshot,
    ) -> Result<(), String> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        let key = self.key(session_id);
        let bytes = serde_json::to_vec(&snapshot).map_err(|e| e.to_string())?;

        // Expire snapshots after 24 hours to prevent cache ballooning
        let _: () = conn
            .set_ex(&key, bytes, 60 * 60 * 24)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn invalidate(&self, session_id: &str) -> Result<(), String> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        let key = self.key(session_id);
        let _: () = conn.del(&key).await.map_err(|e| e.to_string())?;
        Ok(())
    }
}
