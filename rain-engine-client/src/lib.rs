use rain_engine_core::ApprovalDecision;
use rain_engine_runtime::{
    ApprovalIngressRequest, DelegationResultIngressRequest, EventIngressRequest,
    HumanInputIngressRequest, RuntimeRunResult, ScheduledWakeIngressRequest, WebhookIngressRequest,
};
use reqwest::{Client, Url};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error: {status} - {message}")]
    Api { status: u16, message: String },
    #[error("URL parsing error: {0}")]
    Url(#[from] url::ParseError),
}

/// A client for the RainEngine Gateway.
///
/// Provides strongly-typed async methods for every ingress route exposed by
/// the runtime HTTP server.
#[derive(Debug, Clone)]
pub struct RainEngineClient {
    base_url: Url,
    http: Client,
}

impl RainEngineClient {
    /// Creates a new client connecting to the given base URL.
    pub fn new(base_url: &str) -> Result<Self, ClientError> {
        let mut base_url = Url::parse(base_url)?;
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }

        Ok(Self {
            base_url,
            http: Client::new(),
        })
    }

    /// Creates a new client with custom request headers (e.g. for future auth).
    pub fn with_headers(
        base_url: &str,
        headers: reqwest::header::HeaderMap,
    ) -> Result<Self, ClientError> {
        let mut base_url = Url::parse(base_url)?;
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }

        let http = Client::builder()
            .default_headers(headers)
            .build()
            .map_err(ClientError::Http)?;

        Ok(Self { base_url, http })
    }

    // ── Trigger: Human Input ────────────────────────────────────────────

    /// Sends human input to a specific actor.
    /// Route: `POST /triggers/human/{actor_id}`
    pub async fn send_human_input(
        &self,
        actor_id: &str,
        session_id: &str,
        content: &str,
    ) -> Result<RuntimeRunResult, ClientError> {
        let url = self
            .base_url
            .join(&format!("triggers/human/{}", actor_id))?;
        let request = HumanInputIngressRequest {
            session_id: session_id.to_string(),
            content: content.to_string(),
            attachments: vec![],
            granted_scopes: Default::default(),
            idempotency_key: None,
            provider: None,
            policy_override: None,
        };

        self.post(url, &request).await
    }

    /// Sends a fully-specified human input request.
    /// Route: `POST /triggers/human/{actor_id}`
    pub async fn send_human_input_request(
        &self,
        actor_id: &str,
        request: &HumanInputIngressRequest,
    ) -> Result<RuntimeRunResult, ClientError> {
        let url = self
            .base_url
            .join(&format!("triggers/human/{}", actor_id))?;
        self.post(url, request).await
    }

    // ── Trigger: Approval ───────────────────────────────────────────────

    /// Submits an approval decision.
    /// Route: `POST /triggers/approval`
    pub async fn submit_approval(
        &self,
        session_id: &str,
        resume_token: &str,
        decision: ApprovalDecision,
        metadata: Value,
    ) -> Result<RuntimeRunResult, ClientError> {
        let url = self.base_url.join("triggers/approval")?;
        let request = ApprovalIngressRequest {
            session_id: session_id.to_string(),
            resume_token: resume_token.to_string(),
            decision,
            metadata,
            granted_scopes: Default::default(),
            provider: None,
            policy_override: None,
        };

        self.post(url, &request).await
    }

    /// Submits a fully-specified approval request.
    /// Route: `POST /triggers/approval`
    pub async fn submit_approval_request(
        &self,
        request: &ApprovalIngressRequest,
    ) -> Result<RuntimeRunResult, ClientError> {
        let url = self.base_url.join("triggers/approval")?;
        self.post(url, request).await
    }

    // ── Trigger: Webhook ────────────────────────────────────────────────

    /// Sends a webhook event from the given source.
    /// Route: `POST /triggers/webhook/{source}`
    pub async fn send_webhook(
        &self,
        source: &str,
        request: &WebhookIngressRequest,
    ) -> Result<RuntimeRunResult, ClientError> {
        let url = self
            .base_url
            .join(&format!("triggers/webhook/{}", source))?;
        self.post(url, request).await
    }

    // ── Trigger: External Event ─────────────────────────────────────────

    /// Sends an external event from the given source.
    /// Route: `POST /triggers/external/{source}`
    pub async fn send_external_event(
        &self,
        source: &str,
        request: &EventIngressRequest,
    ) -> Result<RuntimeRunResult, ClientError> {
        let url = self
            .base_url
            .join(&format!("triggers/external/{}", source))?;
        self.post(url, request).await
    }

    // ── Trigger: System Observation ─────────────────────────────────────

    /// Sends a system observation from the given source.
    /// Route: `POST /triggers/system/{source}`
    pub async fn send_system_observation(
        &self,
        source: &str,
        request: &EventIngressRequest,
    ) -> Result<RuntimeRunResult, ClientError> {
        let url = self.base_url.join(&format!("triggers/system/{}", source))?;
        self.post(url, request).await
    }

    // ── Trigger: Scheduled Wake ─────────────────────────────────────────

    /// Sends a scheduled wake event with simple args.
    /// Route: `POST /triggers/wake`
    pub async fn send_scheduled_wake(
        &self,
        session_id: &str,
        wake_id: &str,
        due_at: i64,
        reason: &str,
    ) -> Result<RuntimeRunResult, ClientError> {
        let url = self.base_url.join("triggers/wake")?;
        let due_at = std::time::UNIX_EPOCH + std::time::Duration::from_millis(due_at as u64);
        let request = ScheduledWakeIngressRequest {
            session_id: session_id.to_string(),
            wake_id: wake_id.to_string(),
            due_at,
            reason: reason.to_string(),
            granted_scopes: Default::default(),
            provider: None,
            policy_override: None,
        };
        self.post(url, &request).await
    }

    /// Sends a fully-specified scheduled wake request.
    /// Route: `POST /triggers/wake`
    pub async fn send_scheduled_wake_request(
        &self,
        request: &ScheduledWakeIngressRequest,
    ) -> Result<RuntimeRunResult, ClientError> {
        let url = self.base_url.join("triggers/wake")?;
        self.post(url, request).await
    }

    // ── Trigger: Delegation Result ──────────────────────────────────────

    /// Sends a delegation result back to the engine.
    /// Route: `POST /triggers/delegation-result`
    pub async fn send_delegation_result(
        &self,
        request: &DelegationResultIngressRequest,
    ) -> Result<RuntimeRunResult, ClientError> {
        let url = self.base_url.join("triggers/delegation-result")?;
        self.post(url, request).await
    }

    // ── Read: Health ────────────────────────────────────────────────────

    /// Check the gateway health.
    /// Route: `GET /health`
    pub async fn health(&self) -> Result<Value, ClientError> {
        let url = self.base_url.join("health")?;
        self.get(url).await
    }

    // ── Read: Sessions ──────────────────────────────────────────────────

    /// List all sessions.
    /// Route: `GET /sessions`
    pub async fn list_sessions(&self) -> Result<Value, ClientError> {
        let url = self.base_url.join("sessions")?;
        self.get(url).await
    }

    /// Get a session snapshot by ID.
    /// Route: `GET /sessions/{session_id}`
    pub async fn get_session(&self, session_id: &str) -> Result<Value, ClientError> {
        let url = self.base_url.join(&format!("sessions/{}", session_id))?;
        self.get(url).await
    }

    /// List records in a session with pagination.
    /// Route: `GET /sessions/{session_id}/records`
    pub async fn list_records(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Value, ClientError> {
        let url = self.base_url.join(&format!(
            "sessions/{}/records?offset={}&limit={}",
            session_id, offset, limit
        ))?;
        self.get(url).await
    }

    // ── Internal ────────────────────────────────────────────────────────

    async fn post<T: serde::Serialize>(
        &self,
        url: Url,
        payload: &T,
    ) -> Result<RuntimeRunResult, ClientError> {
        let response = self.http.post(url).json(payload).send().await?;
        if response.status().is_success() {
            let result = response.json::<RuntimeRunResult>().await?;
            Ok(result)
        } else {
            Err(ClientError::Api {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            })
        }
    }

    async fn get(&self, url: Url) -> Result<Value, ClientError> {
        let response = self.http.get(url).send().await?;
        if response.status().is_success() {
            let result = response.json::<Value>().await?;
            Ok(result)
        } else {
            Err(ClientError::Api {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            })
        }
    }
}
