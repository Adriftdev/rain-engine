use rain_engine_core::{
    AgentEngine, AgentTrigger, EnginePolicy, ProcessRequest, ProviderRequestConfig,
};
use rain_engine_store_valkey::ValkeyCoordinationStore;
use redis::cmd;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngressEventEnvelope {
    pub session_id: String,
    pub trigger: AgentTrigger,
    #[serde(default)]
    pub granted_scopes: BTreeSet<String>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub policy: Option<EnginePolicy>,
    #[serde(default)]
    pub provider: Option<ProviderRequestConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ValkeyStreamConfig {
    pub url: String,
    pub stream: String,
    pub group: String,
    pub consumer: String,
    pub block_ms: usize,
}

#[derive(Debug, Error)]
pub enum IngressError {
    #[error("{0}")]
    Message(String),
}

#[derive(Clone)]
pub struct ValkeyStreamIngress {
    store: ValkeyCoordinationStore,
    config: ValkeyStreamConfig,
}

impl ValkeyStreamIngress {
    pub fn new(config: ValkeyStreamConfig) -> Result<Self, IngressError> {
        let store = ValkeyCoordinationStore::connect(&config.url)
            .map_err(|err| IngressError::Message(err.message))?;
        Ok(Self { store, config })
    }

    pub async fn publish(&self, event: &IngressEventEnvelope) -> Result<String, IngressError> {
        match &self.store {
            ValkeyCoordinationStore { .. } => {}
        }
        let client = redis::Client::open(self.config.url.clone())
            .map_err(|err| IngressError::Message(err.to_string()))?;
        let stream = self.config.stream.clone();
        let payload =
            serde_json::to_string(event).map_err(|err| IngressError::Message(err.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let mut connection = client
                .get_connection()
                .map_err(|err| IngressError::Message(err.to_string()))?;
            let id: String = cmd("XADD")
                .arg(stream)
                .arg("*")
                .arg("payload")
                .arg(payload)
                .query(&mut connection)
                .map_err(|err| IngressError::Message(err.to_string()))?;
            Ok(id)
        })
        .await
        .map_err(|err| IngressError::Message(err.to_string()))?
    }

    pub async fn ensure_group(&self) -> Result<(), IngressError> {
        let client = redis::Client::open(self.config.url.clone())
            .map_err(|err| IngressError::Message(err.to_string()))?;
        let stream = self.config.stream.clone();
        let group = self.config.group.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = client
                .get_connection()
                .map_err(|err| IngressError::Message(err.to_string()))?;
            let result: Result<String, redis::RedisError> = cmd("XGROUP")
                .arg("CREATE")
                .arg(&stream)
                .arg(&group)
                .arg("0")
                .arg("MKSTREAM")
                .query(&mut connection);
            match result {
                Ok(_) => Ok(()),
                Err(err) if err.to_string().contains("BUSYGROUP") => Ok(()),
                Err(err) => Err(IngressError::Message(err.to_string())),
            }
        })
        .await
        .map_err(|err| IngressError::Message(err.to_string()))?
    }

    pub async fn run_once(&self, engine: &AgentEngine) -> Result<bool, IngressError> {
        self.ensure_group().await?;
        let client = redis::Client::open(self.config.url.clone())
            .map_err(|err| IngressError::Message(err.to_string()))?;
        let stream = self.config.stream.clone();
        let group = self.config.group.clone();
        let consumer = self.config.consumer.clone();
        let block_ms = self.config.block_ms;
        let read = tokio::task::spawn_blocking(move || {
            let mut connection = client
                .get_connection()
                .map_err(|err| IngressError::Message(err.to_string()))?;
            let value: redis::Value = cmd("XREADGROUP")
                .arg("GROUP")
                .arg(&group)
                .arg(&consumer)
                .arg("COUNT")
                .arg(1)
                .arg("BLOCK")
                .arg(block_ms)
                .arg("STREAMS")
                .arg(&stream)
                .arg(">")
                .query(&mut connection)
                .map_err(|err| IngressError::Message(err.to_string()))?;
            Ok::<_, IngressError>(value)
        })
        .await
        .map_err(|err| IngressError::Message(err.to_string()))??;

        let Some((entry_id, event)) = parse_xreadgroup_payload(read)? else {
            return Ok(false);
        };
        engine
            .process_trigger(ProcessRequest {
                session_id: event.session_id.clone(),
                trigger: event.trigger,
                granted_scopes: event.granted_scopes,
                idempotency_key: event.idempotency_key,
                policy: event.policy.unwrap_or_default(),
                provider: event.provider.unwrap_or_default(),
                cancellation: tokio_util::sync::CancellationToken::new(),
            })
            .await
            .map_err(|err| IngressError::Message(err.to_string()))?;

        let client = redis::Client::open(self.config.url.clone())
            .map_err(|err| IngressError::Message(err.to_string()))?;
        let stream = self.config.stream.clone();
        let group = self.config.group.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = client
                .get_connection()
                .map_err(|err| IngressError::Message(err.to_string()))?;
            let _: usize = cmd("XACK")
                .arg(stream)
                .arg(group)
                .arg(entry_id)
                .query(&mut connection)
                .map_err(|err| IngressError::Message(err.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|err| IngressError::Message(err.to_string()))??;

        Ok(true)
    }
}

fn parse_xreadgroup_payload(
    value: redis::Value,
) -> Result<Option<(String, IngressEventEnvelope)>, IngressError> {
    use redis::Value;
    let Value::Array(streams) = value else {
        return Ok(None);
    };
    let Some(Value::Array(stream_record)) = streams.into_iter().next() else {
        return Ok(None);
    };
    let Some(Value::Array(entries)) = stream_record.get(1).cloned() else {
        return Ok(None);
    };
    let Some(Value::Array(entry)) = entries.into_iter().next() else {
        return Ok(None);
    };
    let Some(Value::BulkString(id_bytes)) = entry.first().cloned() else {
        return Ok(None);
    };
    let Some(Value::Array(fields)) = entry.get(1).cloned() else {
        return Ok(None);
    };
    let mut payload = None::<String>;
    let mut index = 0usize;
    while index + 1 < fields.len() {
        let key = match &fields[index] {
            Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
            _ => String::new(),
        };
        if key == "payload" {
            if let Value::BulkString(bytes) = &fields[index + 1] {
                payload = Some(String::from_utf8_lossy(bytes).to_string());
            }
            break;
        }
        index += 2;
    }
    let payload =
        payload.ok_or_else(|| IngressError::Message("missing payload field".to_string()))?;
    let event =
        serde_json::from_str(&payload).map_err(|err| IngressError::Message(err.to_string()))?;
    Ok(Some((
        String::from_utf8_lossy(&id_bytes).to_string(),
        event,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_envelope_json() {
        let envelope = IngressEventEnvelope {
            session_id: "s1".to_string(),
            trigger: AgentTrigger::Message {
                user_id: "u".to_string(),
                content: "hello".to_string(),
                attachments: Vec::new(),
            },
            granted_scopes: BTreeSet::from(["tool:run".to_string()]),
            idempotency_key: Some("abc".to_string()),
            policy: Some(EnginePolicy::default()),
            provider: Some(ProviderRequestConfig::default()),
        };
        let encoded = serde_json::to_string(&envelope).expect("encode");
        let decoded: IngressEventEnvelope = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, envelope);
    }
}
