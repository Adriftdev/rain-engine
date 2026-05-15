//! HTTP fetch skill with host allowlist.

use async_trait::async_trait;
use rain_engine_core::{
    NativeSkill, SkillExecutionError, SkillFailureKind, SkillInvocation, SkillManifest,
};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::time::Duration;
use tracing::warn;

pub struct HttpFetchSkill {
    client: reqwest::Client,
    allowed_hosts: HashSet<String>,
    timeout: Duration,
}

impl HttpFetchSkill {
    /// Create with host allowlist. Empty set = allow all (dev mode).
    pub fn new(allowed_hosts: HashSet<String>, timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::new(),
            allowed_hosts,
            timeout,
        }
    }

    fn is_allowed(&self, url: &str) -> bool {
        if self.allowed_hosts.is_empty() {
            return true; // permissive mode
        }
        reqwest::Url::parse(url)
            .ok()
            .and_then(|parsed: reqwest::Url| parsed.host_str().map(|h| h.to_string()))
            .is_some_and(|host| self.allowed_hosts.contains(&host))
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

        if !self.is_allowed(url) {
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
