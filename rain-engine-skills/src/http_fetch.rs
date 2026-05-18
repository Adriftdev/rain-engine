//! HTTP fetch skill with host allowlist.

use crate::{AccessPolicy, SharedAccessPolicy, shared_access_policy};
use async_trait::async_trait;
use rain_engine_core::{
    NativeSkill, SkillExecutionError, SkillFailureKind, SkillInvocation, SkillManifest,
};
use serde_json::{Value, json};
use std::time::Duration;
use tracing::warn;

pub struct HttpFetchSkill {
    client: reqwest::Client,
    policy: SharedAccessPolicy,
    timeout: Duration,
}

impl HttpFetchSkill {
    /// Create with host allowlist. Empty set = deny all.
    pub fn new(allowed_hosts: std::collections::HashSet<String>, timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::new(),
            policy: shared_access_policy(allowed_hosts, false),
            timeout,
        }
    }

    pub fn permissive(timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::new(),
            policy: shared_access_policy(std::collections::HashSet::new(), true),
            timeout,
        }
    }

    pub fn with_shared_policy(policy: SharedAccessPolicy, timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::new(),
            policy,
            timeout,
        }
    }

    async fn is_allowed(&self, url: &str) -> bool {
        let policy = self.policy.read().await;
        if policy.permissive {
            return true;
        }
        reqwest::Url::parse(url)
            .ok()
            .and_then(|parsed: reqwest::Url| parsed.host_str().map(|h| h.to_string()))
            .is_some_and(|host| policy.allowlist.contains(&host))
    }

    pub async fn access_policy(&self) -> AccessPolicy {
        self.policy.read().await.clone()
    }
}

pub fn manifest() -> SkillManifest {
    crate::base_manifest(
        "http_fetch",
        "Make an HTTP request and return the response. Hosts must be on the allowlist.",
        json!({
            "type": "object",
            "properties": {
                "url": { "type": "string", "description": "The URL to fetch" },
                "method": { "type": "string", "description": "HTTP method (GET, POST, etc.)", "default": "GET" },
                "headers": { "type": "object", "description": "Optional headers" },
                "body": { "type": "string", "description": "Optional request body" }
            },
            "required": ["url"]
        }),
    )
}

#[async_trait]
impl NativeSkill for HttpFetchSkill {
    async fn execute(&self, invocation: SkillInvocation) -> Result<Value, SkillExecutionError> {
        let url = invocation.args["url"].as_str().ok_or_else(|| {
            SkillExecutionError::new(SkillFailureKind::InvalidResponse, "missing 'url' arg")
        })?;

        if !self.is_allowed(url).await {
            warn!(url = %url, "http_fetch: host not on allowlist");
            return Err(SkillExecutionError::new(
                SkillFailureKind::PermissionDenied,
                "host not allowed",
            ));
        }

        let method_str = invocation.args["method"].as_str().unwrap_or("GET");
        let method: reqwest::Method = method_str.parse().map_err(|_| {
            SkillExecutionError::new(
                SkillFailureKind::InvalidResponse,
                format!("invalid method: {method_str}"),
            )
        })?;

        let mut builder = self.client.request(method, url).timeout(self.timeout);

        if let Some(headers) = invocation.args["headers"].as_object() {
            for (key, value) in headers {
                if let Some(val) = value.as_str() {
                    builder = builder.header(key.as_str(), val);
                }
            }
        }

        if let Some(body) = invocation.args["body"].as_str() {
            builder = builder.body(body.to_string());
        }

        let response = builder.send().await.map_err(|err| {
            SkillExecutionError::new(SkillFailureKind::Internal, format!("request failed: {err}"))
        })?;

        let status = response.status().as_u16();
        let response_headers: serde_json::Map<String, Value> = response
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    Value::String(v.to_str().unwrap_or_default().to_string()),
                )
            })
            .collect();
        let body = response.text().await.map_err(|err| {
            SkillExecutionError::new(
                SkillFailureKind::Internal,
                format!("body read failed: {err}"),
            )
        })?;

        Ok(json!({
            "status": status,
            "headers": response_headers,
            "body": body,
        }))
    }

    fn executor_kind(&self) -> &'static str {
        "native:http_fetch"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rain_engine_core::{
        AgentContextSnapshot, AgentId, AgentStateSnapshot, EnginePolicy, SkillInvocation,
    };

    fn invocation(url: &str) -> SkillInvocation {
        SkillInvocation {
            call_id: "call-1".to_string(),
            manifest: manifest(),
            args: json!({ "url": url }),
            dry_run: false,
            context: AgentContextSnapshot {
                session_id: "session".to_string(),
                granted_scopes: vec!["tool:run".to_string()],
                trigger_id: "trigger".to_string(),
                idempotency_key: None,
                current_step: 0,
                max_steps: 1,
                history: Vec::new(),
                prior_tool_results: Vec::new(),
                session_cost_usd: 0.0,
                state: AgentStateSnapshot {
                    agent_id: AgentId("session".to_string()),
                    profile: None,
                    goals: Vec::new(),
                    tasks: Vec::new(),
                    observations: Vec::new(),
                    artifacts: Vec::new(),
                    resources: Vec::new(),
                    relationships: Vec::new(),
                    pending_wake: None,
                },
                policy: EnginePolicy::default(),
                active_execution_plan: None,
            },
        }
    }

    #[tokio::test]
    async fn empty_allowlist_denies_by_default() {
        let skill = HttpFetchSkill::new(std::collections::HashSet::new(), Duration::from_secs(1));
        let err = skill
            .execute(invocation("https://example.com"))
            .await
            .expect_err("empty allowlist denies");
        assert_eq!(err.kind, SkillFailureKind::PermissionDenied);
    }
}
