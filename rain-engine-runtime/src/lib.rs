//! Reference HTTP runtime for RainEngine.
//!
//! The runtime owns request parsing and repeated calls to `AgentEngine::advance`
//! until a terminal, suspended, delegated, or policy-stopped outcome is reached.

use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header::CONTENT_TYPE},
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
};
use futures_util::stream;
use rain_engine_blob::{BlobBackendConfig, build_blob_store};
use rain_engine_core::{
    AdvanceRequest, AgentEngine, AgentStateSnapshot, AgentTrigger, ApprovalDecision, AttachmentRef,
    BlobStore, ContinueRequest, CorrelationId, EngineError, EngineOutcome, EnginePolicy,
    InMemoryMemoryStore, LlmProvider, MemoryStore, MockLlmProvider, MultimodalPayload,
    PendingApprovalRecord, ProcessRequest, ProviderRequestConfig, RecordPageQuery,
    SessionListQuery, SessionRecord, SessionSnapshot, SkillDefinition, StopReason, ToolCallRecord,
    ToolResultRecord, WakeId, unix_time_ms,
};
use rain_engine_openai::{OpenAiCompatibleConfig, OpenAiCompatibleProvider};
use rain_engine_provider_gemini::{GeminiAuth, GeminiConfig, GeminiProvider};
use rain_engine_store_pg::PgMemoryStore;
use rain_engine_store_sqlite::SqliteMemoryStore;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

const MAX_INGRESS_BODY_BYTES: usize = 64 * 1024 * 1024;

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WebhookIngressRequest {
    pub session_id: String,
    pub payload: Value,
    #[serde(default)]
    pub attachments: Vec<MultimodalPayload>,
    #[serde(default)]
    pub granted_scopes: BTreeSet<String>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub provider: Option<ProviderRequestConfig>,
    #[serde(default)]
    pub policy_override: Option<EnginePolicy>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApprovalIngressRequest {
    pub session_id: String,
    pub resume_token: String,
    pub decision: ApprovalDecision,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub granted_scopes: BTreeSet<String>,
    #[serde(default)]
    pub provider: Option<ProviderRequestConfig>,
    #[serde(default)]
    pub policy_override: Option<EnginePolicy>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventIngressRequest {
    pub session_id: String,
    pub payload: Value,
    #[serde(default)]
    pub attachments: Vec<MultimodalPayload>,
    #[serde(default)]
    pub granted_scopes: BTreeSet<String>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub provider: Option<ProviderRequestConfig>,
    #[serde(default)]
    pub policy_override: Option<EnginePolicy>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HumanInputIngressRequest {
    pub session_id: String,
    pub content: String,
    #[serde(default)]
    pub attachments: Vec<MultimodalPayload>,
    #[serde(default)]
    pub granted_scopes: BTreeSet<String>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub provider: Option<ProviderRequestConfig>,
    #[serde(default)]
    pub policy_override: Option<EnginePolicy>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduledWakeIngressRequest {
    pub session_id: String,
    pub wake_id: String,
    pub due_at: std::time::SystemTime,
    pub reason: String,
    #[serde(default)]
    pub granted_scopes: BTreeSet<String>,
    #[serde(default)]
    pub provider: Option<ProviderRequestConfig>,
    #[serde(default)]
    pub policy_override: Option<EnginePolicy>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DelegationResultIngressRequest {
    pub session_id: String,
    pub correlation_id: String,
    pub payload: Value,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub granted_scopes: BTreeSet<String>,
    #[serde(default)]
    pub provider: Option<ProviderRequestConfig>,
    #[serde(default)]
    pub policy_override: Option<EnginePolicy>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeServerConfig {
    pub bind_address: SocketAddr,
    pub request_timeout_ms: u64,
    pub default_policy: EnginePolicy,
    pub allow_policy_overrides: bool,
    pub allow_provider_overrides: bool,
    pub default_provider: ProviderRequestConfig,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StoreBootstrapConfig {
    InMemory,
    Sqlite { database_url: String },
    Postgres { database_url: String },
}

pub type BlobBootstrapConfig = BlobBackendConfig;

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum GeminiAuthMode {
    ApiKey,
    BearerToken,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderBootstrapConfig {
    Mock {
        response: String,
    },
    OpenAiCompatible {
        base_url: String,
        api_key: String,
        model: String,
        #[serde(default)]
        temperature: Option<f32>,
        #[serde(default)]
        max_tokens: Option<u32>,
        #[serde(default = "default_system_prompt")]
        system_prompt: String,
    },
    Gemini {
        base_url: String,
        auth_mode: GeminiAuthMode,
        credential: String,
        model: String,
        #[serde(default)]
        temperature: Option<f32>,
        #[serde(default)]
        max_tokens: Option<u32>,
        #[serde(default = "default_gemini_system_instruction")]
        system_instruction: String,
        #[serde(default = "default_gemini_provider_name")]
        provider_name: String,
    },
}

fn default_system_prompt() -> String {
    "You are a server-side automation agent. Prefer tool calls when available. When replying directly, return plain text or JSON with type=yield.".to_string()
}

fn default_gemini_system_instruction() -> String {
    "You are a multimodal server-side automation agent. Use tools when they can complete the task precisely.".to_string()
}

fn default_gemini_provider_name() -> String {
    "gemini".to_string()
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeBootstrapConfig {
    pub server: RuntimeServerConfig,
    pub store: StoreBootstrapConfig,
    pub blob: BlobBootstrapConfig,
    pub provider: ProviderBootstrapConfig,
}

#[derive(Clone)]
pub struct RuntimeState {
    engine: AgentEngine,
    memory: Arc<dyn MemoryStore>,
    blob_store: Arc<dyn BlobStore>,
    config: RuntimeServerConfig,
    provider_kind: String,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeRunResult {
    pub advances: Vec<rain_engine_core::AdvanceResult>,
    pub outcome: EngineOutcome,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Empty,
    Running,
    Completed,
    Suspended,
    Delegated,
    Stopped,
    Failed,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApprovalView {
    pub resume_token: String,
    pub created_at_ms: i64,
    pub trigger_id: String,
    pub step: usize,
    pub reason: String,
    pub pending_calls: Vec<rain_engine_core::PlannedSkillCall>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolTimelineItem {
    pub call_id: String,
    pub skill_name: String,
    pub step: usize,
    pub called_at_ms: i64,
    pub finished_at_ms: Option<i64>,
    pub backend_kind: String,
    pub args: Value,
    pub success: Option<bool>,
    pub output_preview: Option<String>,
    pub failure_kind: Option<String>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SelfImprovementView {
    pub active_overlay: Option<rain_engine_core::PolicyOverlay>,
    pub reflections: Vec<rain_engine_core::ReflectionRecord>,
    pub policy_tunings: Vec<rain_engine_core::PolicyTuningRecord>,
    pub strategy_preferences: Vec<rain_engine_core::StrategyPreferenceRecord>,
    pub tool_performance: Vec<rain_engine_core::ToolPerformanceRecord>,
    pub profile_patches: Vec<rain_engine_core::ProfilePatchRecord>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionGraphView {
    pub active_graph: Option<rain_engine_core::ToolExecutionGraph>,
    pub graphs: Vec<rain_engine_core::ToolExecutionGraph>,
    pub checkpoints: Vec<rain_engine_core::ToolNodeCheckpointRecord>,
    pub validations: Vec<rain_engine_core::SkillInputValidationRecord>,
    pub blocked_call_ids: Vec<String>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "payload")]
pub enum TimelineItem {
    HumanInput {
        actor_id: String,
        content: String,
        occurred_at_ms: i64,
    },
    AssistantResponse {
        content: String,
        stop_reason: StopReason,
        occurred_at_ms: i64,
    },
    ToolCall {
        call_id: String,
        skill_name: String,
        formatted_call: String,
        occurred_at_ms: i64,
    },
    ToolResult {
        call_id: String,
        skill_name: String,
        success: bool,
        preview: String,
        occurred_at_ms: i64,
    },
    ApprovalRequested {
        resume_token: String,
        pending_calls: Vec<rain_engine_core::PlannedSkillCall>,
        occurred_at_ms: i64,
    },
    ApprovalResolved {
        resume_token: String,
        decision: ApprovalDecision,
        occurred_at_ms: i64,
    },
    Plan {
        summary: String,
        candidate_actions: Vec<String>,
        confidence: f64,
        outcome: rain_engine_core::DeliberationOutcome,
        occurred_at_ms: i64,
    },
    ToolCheckpoint {
        call_id: String,
        skill_name: String,
        status: rain_engine_core::ToolNodeStatus,
        attempt: usize,
        detail: Option<String>,
        occurred_at_ms: i64,
    },
    ValidationFailure {
        call_id: String,
        skill_name: String,
        errors: Vec<String>,
        occurred_at_ms: i64,
    },
    Learning {
        label: String,
        detail: String,
        confidence: f64,
        occurred_at_ms: i64,
    },
    System {
        label: String,
        detail: String,
        occurred_at_ms: i64,
    },
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionView {
    pub session_id: String,
    pub status: SessionStatus,
    pub last_sequence_no: Option<i64>,
    pub latest_outcome: Option<rain_engine_core::OutcomeRecord>,
    pub pending_approval: Option<ApprovalView>,
    pub state: AgentStateSnapshot,
    pub timeline: Vec<TimelineItem>,
    pub tool_timeline: Vec<ToolTimelineItem>,
    pub self_improvement: SelfImprovementView,
    pub execution_graph: ExecutionGraphView,
    pub record_count: usize,
    pub total_estimated_cost_usd: f64,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeCapabilities {
    pub version: String,
    pub provider_kind: String,
    pub default_model: Option<String>,
    pub streaming: bool,
    pub approvals: bool,
    pub multipart_uploads: bool,
    pub default_scopes: Vec<String>,
    pub default_policy: EnginePolicy,
    pub skills: Vec<SkillDefinition>,
}

impl RuntimeState {
    pub fn new(
        engine: AgentEngine,
        memory: Arc<dyn MemoryStore>,
        blob_store: Arc<dyn BlobStore>,
        config: RuntimeServerConfig,
    ) -> Self {
        Self {
            engine,
            memory,
            blob_store,
            config,
            provider_kind: "custom".to_string(),
        }
    }

    pub fn with_provider_kind(mut self, provider_kind: impl Into<String>) -> Self {
        self.provider_kind = provider_kind.into();
        self
    }

    pub fn engine(&self) -> &AgentEngine {
        &self.engine
    }

    pub fn memory(&self) -> Arc<dyn MemoryStore> {
        self.memory.clone()
    }

    pub fn blob_store(&self) -> Arc<dyn BlobStore> {
        self.blob_store.clone()
    }
}

#[derive(Debug, Error)]
pub enum RuntimeConfigError {
    #[error("{0}")]
    Invalid(String),
}

#[derive(Debug, Error)]
enum IngressError {
    #[error("unsupported content type")]
    UnsupportedContentType,
    #[error("malformed request: {0}")]
    Malformed(String),
    #[error("blob storage failed: {0}")]
    Blob(String),
}

pub async fn build_runtime_state(
    config: RuntimeBootstrapConfig,
) -> Result<RuntimeState, RuntimeConfigError> {
    let provider_kind = match &config.provider {
        ProviderBootstrapConfig::Mock { .. } => "mock",
        ProviderBootstrapConfig::OpenAiCompatible { .. } => "openai_compatible",
        ProviderBootstrapConfig::Gemini { .. } => "gemini",
    }
    .to_string();

    let memory: Arc<dyn MemoryStore> = match &config.store {
        StoreBootstrapConfig::InMemory => Arc::new(InMemoryMemoryStore::new()),
        StoreBootstrapConfig::Sqlite { database_url } => {
            if database_url.trim().is_empty() {
                return Err(RuntimeConfigError::Invalid(
                    "sqlite database_url must not be empty".to_string(),
                ));
            }
            Arc::new(
                SqliteMemoryStore::connect(database_url)
                    .await
                    .map_err(|err| RuntimeConfigError::Invalid(err.message))?,
            )
        }
        StoreBootstrapConfig::Postgres { database_url } => {
            if database_url.trim().is_empty() {
                return Err(RuntimeConfigError::Invalid(
                    "postgres database_url must not be empty".to_string(),
                ));
            }
            Arc::new(
                PgMemoryStore::connect_lazy(database_url)
                    .map_err(|err| RuntimeConfigError::Invalid(err.message))?,
            )
        }
    };

    let blob_store: Arc<dyn BlobStore> = Arc::from(
        build_blob_store(&config.blob).map_err(|err| RuntimeConfigError::Invalid(err.message))?,
    );

    let llm: Arc<dyn LlmProvider> = match &config.provider {
        ProviderBootstrapConfig::Mock { response } => Arc::new(MockLlmProvider::scripted(vec![
            rain_engine_core::AgentAction::Respond {
                content: response.clone(),
            },
        ])),
        ProviderBootstrapConfig::OpenAiCompatible {
            base_url,
            api_key,
            model,
            temperature,
            max_tokens,
            system_prompt,
        } => Arc::new(
            OpenAiCompatibleProvider::new(OpenAiCompatibleConfig {
                base_url: base_url.clone(),
                api_key: api_key.clone(),
                default_request: ProviderRequestConfig {
                    model: Some(model.clone()),
                    temperature: *temperature,
                    max_tokens: *max_tokens,
                },
                system_prompt: system_prompt.clone(),
            })
            .map_err(|err| RuntimeConfigError::Invalid(err.to_string()))?,
        ),
        ProviderBootstrapConfig::Gemini {
            base_url,
            auth_mode,
            credential,
            model,
            temperature,
            max_tokens,
            system_instruction,
            provider_name,
        } => Arc::new(
            GeminiProvider::new(GeminiConfig {
                base_url: base_url.clone(),
                auth: match auth_mode {
                    GeminiAuthMode::ApiKey => GeminiAuth::ApiKey(credential.clone()),
                    GeminiAuthMode::BearerToken => GeminiAuth::BearerToken(credential.clone()),
                },
                default_request: ProviderRequestConfig {
                    model: Some(model.clone()),
                    temperature: *temperature,
                    max_tokens: *max_tokens,
                },
                system_instruction: system_instruction.clone(),
                provider_name: provider_name.clone(),
            })
            .map_err(|err| RuntimeConfigError::Invalid(err.to_string()))?,
        ),
    };

    Ok(RuntimeState::new(
        AgentEngine::new(llm, memory.clone()),
        memory,
        blob_store,
        config.server,
    )
    .with_provider_kind(provider_kind))
}

pub fn app(state: RuntimeState) -> Router {
    Router::new()
        // Trigger (write) routes
        .route("/triggers/webhook/{source}", post(handle_webhook))
        .route("/triggers/external/{source}", post(handle_external_event))
        .route("/triggers/human/{actor_id}", post(handle_human_input))
        .route("/triggers/system/{source}", post(handle_system_observation))
        .route("/triggers/wake", post(handle_scheduled_wake))
        .route("/triggers/approval", post(handle_approval))
        .route(
            "/triggers/delegation-result",
            post(handle_delegation_result),
        )
        // Read routes
        .route("/health", get(handle_health))
        .route("/capabilities", get(handle_capabilities))
        .route("/sessions", get(handle_list_sessions))
        .route("/sessions/{session_id}", get(handle_get_session))
        .route("/sessions/{session_id}/view", get(handle_get_session_view))
        .route(
            "/sessions/{session_id}/execution-graph",
            get(handle_get_execution_graph),
        )
        .route("/sessions/{session_id}/records", get(handle_list_records))
        // SSE streaming
        .route("/sessions/{session_id}/stream", get(handle_sse_stream))
        .with_state(state)
}

pub async fn serve(addr: SocketAddr, state: RuntimeState) -> Result<(), std::io::Error> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app(state)).await
}

async fn handle_external_event(
    State(state): State<RuntimeState>,
    Path(source): Path<String>,
    Json(request): Json<EventIngressRequest>,
) -> Result<Json<RuntimeRunResult>, (StatusCode, String)> {
    let policy = effective_policy(&state.config, request.policy_override);
    let provider = effective_provider(&state.config, request.provider);
    let attachments = materialize_attachments(
        &state,
        policy.max_inline_attachment_bytes,
        request.attachments,
    )
    .await
    .map_err(map_ingress_error)?;
    run_process_request(
        &state,
        ProcessRequest {
            session_id: request.session_id,
            trigger: AgentTrigger::ExternalEvent {
                source,
                payload: request.payload,
                attachments,
            },
            granted_scopes: request.granted_scopes,
            idempotency_key: request.idempotency_key,
            policy,
            provider,
            cancellation: tokio_util::sync::CancellationToken::new(),
        },
    )
    .await
}

async fn handle_human_input(
    State(state): State<RuntimeState>,
    Path(actor_id): Path<String>,
    Json(request): Json<HumanInputIngressRequest>,
) -> Result<Json<RuntimeRunResult>, (StatusCode, String)> {
    let policy = effective_policy(&state.config, request.policy_override);
    let provider = effective_provider(&state.config, request.provider);
    let attachments = materialize_attachments(
        &state,
        policy.max_inline_attachment_bytes,
        request.attachments,
    )
    .await
    .map_err(map_ingress_error)?;
    run_process_request(
        &state,
        ProcessRequest {
            session_id: request.session_id,
            trigger: AgentTrigger::HumanInput {
                actor_id,
                content: request.content,
                attachments,
            },
            granted_scopes: request.granted_scopes,
            idempotency_key: request.idempotency_key,
            policy,
            provider,
            cancellation: tokio_util::sync::CancellationToken::new(),
        },
    )
    .await
}

async fn handle_system_observation(
    State(state): State<RuntimeState>,
    Path(source): Path<String>,
    Json(request): Json<EventIngressRequest>,
) -> Result<Json<RuntimeRunResult>, (StatusCode, String)> {
    let policy = effective_policy(&state.config, request.policy_override);
    let provider = effective_provider(&state.config, request.provider);
    let attachments = materialize_attachments(
        &state,
        policy.max_inline_attachment_bytes,
        request.attachments,
    )
    .await
    .map_err(map_ingress_error)?;
    run_process_request(
        &state,
        ProcessRequest {
            session_id: request.session_id,
            trigger: AgentTrigger::SystemObservation {
                source,
                observation: request.payload,
                attachments,
            },
            granted_scopes: request.granted_scopes,
            idempotency_key: request.idempotency_key,
            policy,
            provider,
            cancellation: tokio_util::sync::CancellationToken::new(),
        },
    )
    .await
}

async fn handle_scheduled_wake(
    State(state): State<RuntimeState>,
    Json(request): Json<ScheduledWakeIngressRequest>,
) -> Result<Json<RuntimeRunResult>, (StatusCode, String)> {
    let policy = effective_policy(&state.config, request.policy_override);
    let provider = effective_provider(&state.config, request.provider);
    run_process_request(
        &state,
        ProcessRequest {
            session_id: request.session_id,
            trigger: AgentTrigger::ScheduledWake {
                wake_id: WakeId(request.wake_id),
                due_at: request.due_at,
                reason: request.reason,
            },
            granted_scopes: request.granted_scopes,
            idempotency_key: None,
            policy,
            provider,
            cancellation: tokio_util::sync::CancellationToken::new(),
        },
    )
    .await
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt::try_init();
}

async fn handle_delegation_result(
    State(state): State<RuntimeState>,
    Json(request): Json<DelegationResultIngressRequest>,
) -> Result<Json<RuntimeRunResult>, (StatusCode, String)> {
    let policy = effective_policy(&state.config, request.policy_override);
    let provider = effective_provider(&state.config, request.provider);
    run_process_request(
        &state,
        ProcessRequest {
            session_id: request.session_id,
            trigger: AgentTrigger::DelegationResult {
                correlation_id: CorrelationId(request.correlation_id),
                payload: request.payload,
                metadata: request.metadata,
            },
            granted_scopes: request.granted_scopes,
            idempotency_key: None,
            policy,
            provider,
            cancellation: tokio_util::sync::CancellationToken::new(),
        },
    )
    .await
}

async fn handle_webhook(
    State(state): State<RuntimeState>,
    Path(source): Path<String>,
    request: Request,
) -> Result<Json<RuntimeRunResult>, (StatusCode, String)> {
    let envelope = parse_webhook_envelope(request)
        .await
        .map_err(map_ingress_error)?;
    let policy = effective_policy(&state.config, envelope.policy_override);
    let provider = effective_provider(&state.config, envelope.provider);
    let attachments = materialize_attachments(
        &state,
        policy.max_inline_attachment_bytes,
        envelope.attachments,
    )
    .await
    .map_err(map_ingress_error)?;

    run_process_request(
        &state,
        ProcessRequest {
            session_id: envelope.session_id,
            trigger: AgentTrigger::Webhook {
                source,
                payload: envelope.payload,
                attachments,
            },
            granted_scopes: envelope.granted_scopes,
            idempotency_key: envelope.idempotency_key,
            policy,
            provider,
            cancellation: tokio_util::sync::CancellationToken::new(),
        },
    )
    .await
}

async fn handle_approval(
    State(state): State<RuntimeState>,
    Json(request): Json<ApprovalIngressRequest>,
) -> Result<Json<RuntimeRunResult>, (StatusCode, String)> {
    let policy = effective_policy(&state.config, request.policy_override);
    let provider = effective_provider(&state.config, request.provider);
    run_process_request(
        &state,
        ProcessRequest {
            session_id: request.session_id,
            trigger: AgentTrigger::Approval {
                resume_token: rain_engine_core::ResumeToken(request.resume_token),
                decision: request.decision,
                metadata: request.metadata,
            },
            granted_scopes: request.granted_scopes,
            idempotency_key: None,
            policy,
            provider,
            cancellation: tokio_util::sync::CancellationToken::new(),
        },
    )
    .await
}

async fn parse_webhook_envelope(request: Request) -> Result<WebhookIngressRequest, IngressError> {
    let content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    let body = to_bytes(request.into_body(), MAX_INGRESS_BODY_BYTES)
        .await
        .map_err(|err| IngressError::Malformed(err.to_string()))?;

    if content_type.starts_with("application/json") {
        return serde_json::from_slice(&body)
            .map_err(|err| IngressError::Malformed(err.to_string()));
    }

    if content_type.starts_with("multipart/form-data") {
        return parse_multipart_webhook(&content_type, body).await;
    }

    Err(IngressError::UnsupportedContentType)
}

async fn parse_multipart_webhook(
    content_type: &str,
    body: axum::body::Bytes,
) -> Result<WebhookIngressRequest, IngressError> {
    let boundary = multer::parse_boundary(content_type)
        .map_err(|err| IngressError::Malformed(err.to_string()))?;
    let stream = stream::once(async move { Ok::<_, std::convert::Infallible>(body) });
    let mut multipart = multer::Multipart::new(stream, boundary);

    let mut session_id = None::<String>;
    let mut payload = None::<Value>;
    let mut attachments = Vec::new();
    let mut granted_scopes = BTreeSet::new();
    let mut idempotency_key = None::<String>;
    let mut provider = None::<ProviderRequestConfig>;
    let mut policy_override = None::<EnginePolicy>;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|err| IngressError::Malformed(err.to_string()))?
    {
        let name = field.name().unwrap_or_default().to_string();
        let file_name = field.file_name().map(str::to_string);
        let mime_type = field
            .content_type()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "application/octet-stream".to_string());

        if file_name.is_some() {
            attachments.push(MultimodalPayload {
                mime_type,
                file_name,
                data: field
                    .bytes()
                    .await
                    .map_err(|err| IngressError::Malformed(err.to_string()))?
                    .to_vec(),
            });
            continue;
        }

        let text = field
            .text()
            .await
            .map_err(|err| IngressError::Malformed(err.to_string()))?;
        match name.as_str() {
            "session_id" => session_id = Some(text),
            "payload" => {
                payload = Some(parse_json_value(&text, "payload")?);
            }
            "granted_scope" => {
                if !text.trim().is_empty() {
                    granted_scopes.insert(text);
                }
            }
            "granted_scopes" => {
                let scopes = parse_json_value::<Vec<String>>(&text, "granted_scopes")?;
                granted_scopes.extend(scopes);
            }
            "idempotency_key" => {
                if !text.trim().is_empty() {
                    idempotency_key = Some(text);
                }
            }
            "provider" => provider = Some(parse_json_value(&text, "provider")?),
            "policy_override" => {
                policy_override = Some(parse_json_value(&text, "policy_override")?)
            }
            _ => {}
        }
    }

    Ok(WebhookIngressRequest {
        session_id: session_id
            .ok_or_else(|| IngressError::Malformed("missing session_id".to_string()))?,
        payload: payload.unwrap_or(Value::Null),
        attachments,
        granted_scopes,
        idempotency_key,
        provider,
        policy_override,
    })
}

fn parse_json_value<T: DeserializeOwned>(text: &str, field_name: &str) -> Result<T, IngressError> {
    serde_json::from_str(text)
        .map_err(|err| IngressError::Malformed(format!("invalid {field_name}: {err}")))
}

async fn materialize_attachments(
    state: &RuntimeState,
    max_inline_attachment_bytes: usize,
    payloads: Vec<MultimodalPayload>,
) -> Result<Vec<AttachmentRef>, IngressError> {
    let mut attachments = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let attachment_id = Uuid::new_v4().to_string();
        if payload.data.len() <= max_inline_attachment_bytes {
            attachments.push(AttachmentRef::inline(
                attachment_id,
                payload.mime_type,
                payload.file_name,
                payload.data,
            ));
        } else {
            attachments.push(
                state
                    .blob_store
                    .put(attachment_id, payload)
                    .await
                    .map_err(|err| IngressError::Blob(err.message))?,
            );
        }
    }
    Ok(attachments)
}

fn effective_policy(
    config: &RuntimeServerConfig,
    override_policy: Option<EnginePolicy>,
) -> EnginePolicy {
    if config.allow_policy_overrides {
        override_policy.unwrap_or_else(|| config.default_policy.clone())
    } else {
        config.default_policy.clone()
    }
}

fn effective_provider(
    config: &RuntimeServerConfig,
    override_provider: Option<ProviderRequestConfig>,
) -> ProviderRequestConfig {
    if config.allow_provider_overrides {
        override_provider.unwrap_or_else(|| config.default_provider.clone())
    } else {
        config.default_provider.clone()
    }
}

async fn run_process_request(
    state: &RuntimeState,
    mut request: ProcessRequest,
) -> Result<Json<RuntimeRunResult>, (StatusCode, String)> {
    // Default scopes for simple ingress if none provided
    if request.granted_scopes.is_empty() {
        request.granted_scopes.insert("tool:run".to_string());
    }

    let timeout = Duration::from_millis(state.config.request_timeout_ms.max(1));
    match tokio::time::timeout(timeout, run_until_terminal_trace(&state.engine, request)).await {
        Ok(Ok(result)) => Ok(Json(result)),
        Ok(Err(err)) => Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
        Err(_) => Err((StatusCode::REQUEST_TIMEOUT, "request timed out".to_string())),
    }
}

pub async fn run_until_terminal_trace(
    engine: &AgentEngine,
    request: ProcessRequest,
) -> Result<RuntimeRunResult, EngineError> {
    let mut advances = Vec::new();
    let mut next = AdvanceRequest::Trigger(request.clone());
    loop {
        let result = engine.advance(next).await?;
        if let Some(outcome) = result.outcome.clone() {
            advances.push(result);
            return Ok(RuntimeRunResult { advances, outcome });
        }
        advances.push(result);
        next = AdvanceRequest::Continue(ContinueRequest {
            session_id: request.session_id.clone(),
            granted_scopes: request.granted_scopes.clone(),
            policy: request.policy.clone(),
            provider: request.provider.clone(),
            cancellation: request.cancellation.clone(),
        });
    }
}

pub async fn run_until_terminal(
    engine: &AgentEngine,
    request: ProcessRequest,
) -> Result<EngineOutcome, EngineError> {
    Ok(run_until_terminal_trace(engine, request).await?.outcome)
}

fn map_ingress_error(error: IngressError) -> (StatusCode, String) {
    match error {
        IngressError::UnsupportedContentType => {
            (StatusCode::UNSUPPORTED_MEDIA_TYPE, error.to_string())
        }
        IngressError::Malformed(_) => (StatusCode::BAD_REQUEST, error.to_string()),
        IngressError::Blob(_) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Read handlers
// ---------------------------------------------------------------------------

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

async fn handle_health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

async fn handle_capabilities(State(state): State<RuntimeState>) -> Json<RuntimeCapabilities> {
    Json(RuntimeCapabilities {
        version: env!("CARGO_PKG_VERSION").to_string(),
        provider_kind: state.provider_kind.clone(),
        default_model: state.config.default_provider.model.clone(),
        streaming: true,
        approvals: true,
        multipart_uploads: true,
        default_scopes: vec!["tool:run".to_string()],
        default_policy: state.config.default_policy.clone(),
        skills: state.engine.skill_definitions().await,
    })
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionListParams {
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub since_ms: Option<i64>,
    #[serde(default)]
    pub until_ms: Option<i64>,
}

async fn handle_list_sessions(
    State(state): State<RuntimeState>,
    Query(params): Query<SessionListParams>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let query = SessionListQuery {
        offset: params.offset.unwrap_or(0),
        limit: params.limit.unwrap_or(100),
        since_ms: params.since_ms,
        until_ms: params.until_ms,
    };
    let sessions = state
        .memory
        .list_sessions(query)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.message))?;
    Ok(Json(serde_json::to_value(sessions).unwrap_or_default()))
}

async fn handle_get_session(
    State(state): State<RuntimeState>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let snapshot = state
        .memory
        .load_session(&session_id)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.message))?;
    Ok(Json(serde_json::to_value(snapshot).unwrap_or_default()))
}

async fn handle_get_session_view(
    State(state): State<RuntimeState>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionView>, (StatusCode, String)> {
    let snapshot = state
        .memory
        .load_session(&session_id)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.message))?;
    Ok(Json(build_session_view(snapshot)))
}

async fn handle_get_execution_graph(
    State(state): State<RuntimeState>,
    Path(session_id): Path<String>,
) -> Result<Json<ExecutionGraphView>, (StatusCode, String)> {
    let snapshot = state
        .memory
        .load_session(&session_id)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.message))?;
    Ok(Json(build_execution_graph_view(&snapshot)))
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecordListParams {
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub since_ms: Option<i64>,
    #[serde(default)]
    pub until_ms: Option<i64>,
}

async fn handle_list_records(
    State(state): State<RuntimeState>,
    Path(session_id): Path<String>,
    Query(params): Query<RecordListParams>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let query = RecordPageQuery {
        session_id,
        offset: params.offset.unwrap_or(0),
        limit: params.limit.unwrap_or(100),
        since_ms: params.since_ms,
        until_ms: params.until_ms,
    };
    let page = state
        .memory
        .list_records(query)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.message))?;
    Ok(Json(serde_json::to_value(page).unwrap_or_default()))
}

fn build_session_view(snapshot: SessionSnapshot) -> SessionView {
    let state = snapshot.agent_state();
    let pending_approval = latest_pending_approval(&snapshot);
    let timeline = build_timeline(&snapshot.records);
    let tool_timeline = build_tool_timeline(&snapshot.records);
    let status = derive_session_status(&snapshot, pending_approval.as_ref());
    let total_estimated_cost_usd = snapshot.total_estimated_cost_usd();
    let self_improvement = build_self_improvement_view(&snapshot);
    let execution_graph = build_execution_graph_view(&snapshot);

    SessionView {
        session_id: snapshot.session_id,
        status,
        last_sequence_no: snapshot.last_sequence_no,
        latest_outcome: snapshot.latest_outcome,
        pending_approval,
        state,
        timeline,
        tool_timeline,
        self_improvement,
        execution_graph,
        record_count: snapshot.records.len(),
        total_estimated_cost_usd,
    }
}

fn build_execution_graph_view(snapshot: &SessionSnapshot) -> ExecutionGraphView {
    let active_graph = snapshot.active_tool_execution_graph();
    let graphs = snapshot.tool_execution_graphs();
    let checkpoints = snapshot.tool_node_checkpoints();
    let validations = snapshot.skill_input_validations();
    let mut latest = BTreeMap::<String, rain_engine_core::ToolNodeStatus>::new();
    for checkpoint in checkpoints.iter().filter(|checkpoint| {
        active_graph
            .as_ref()
            .map(|graph| graph.graph_id == checkpoint.graph_id)
            .unwrap_or(false)
    }) {
        latest.insert(checkpoint.call_id.clone(), checkpoint.status.clone());
    }
    let blocked_call_ids = active_graph
        .as_ref()
        .map(|graph| {
            graph
                .nodes
                .iter()
                .filter(|node| {
                    node.dependencies.iter().any(|dependency| {
                        matches!(
                            latest.get(&dependency.call_id),
                            Some(rain_engine_core::ToolNodeStatus::Failed)
                                | Some(rain_engine_core::ToolNodeStatus::Skipped)
                                | Some(rain_engine_core::ToolNodeStatus::TimedOut)
                        )
                    })
                })
                .map(|node| node.call_id.clone())
                .collect()
        })
        .unwrap_or_default();

    ExecutionGraphView {
        active_graph,
        graphs,
        checkpoints,
        validations,
        blocked_call_ids,
    }
}

fn build_self_improvement_view(snapshot: &SessionSnapshot) -> SelfImprovementView {
    SelfImprovementView {
        active_overlay: snapshot.active_policy_overlay(),
        reflections: snapshot.reflections(),
        policy_tunings: snapshot.policy_tunings(),
        strategy_preferences: snapshot.strategy_preferences(),
        tool_performance: snapshot.tool_performance_records(),
        profile_patches: snapshot
            .records
            .iter()
            .filter_map(|record| match record {
                SessionRecord::ProfilePatch(patch) => Some(patch.clone()),
                _ => None,
            })
            .collect(),
    }
}

fn derive_session_status(
    snapshot: &SessionSnapshot,
    pending_approval: Option<&ApprovalView>,
) -> SessionStatus {
    if snapshot.records.is_empty() {
        return SessionStatus::Empty;
    }

    if pending_approval.is_some() {
        return SessionStatus::Suspended;
    }

    match snapshot
        .latest_outcome
        .as_ref()
        .map(|outcome| &outcome.stop_reason)
    {
        None => SessionStatus::Running,
        Some(StopReason::Responded | StopReason::Yielded) => SessionStatus::Completed,
        Some(StopReason::Suspended) => SessionStatus::Suspended,
        Some(StopReason::Delegated) => SessionStatus::Delegated,
        Some(
            StopReason::ProviderFailure
            | StopReason::StorageFailure
            | StopReason::PolicyAborted
            | StopReason::DeadlineExceeded
            | StopReason::Cancelled,
        ) => SessionStatus::Failed,
        Some(StopReason::MaxStepsReached) => SessionStatus::Stopped,
    }
}

fn latest_pending_approval(snapshot: &SessionSnapshot) -> Option<ApprovalView> {
    let resolved = snapshot
        .records
        .iter()
        .filter_map(|record| match record {
            SessionRecord::ApprovalResolution(resolution) => {
                Some(resolution.resume_token.0.clone())
            }
            _ => None,
        })
        .collect::<BTreeSet<_>>();

    let target_token = snapshot
        .latest_outcome
        .as_ref()
        .filter(|outcome| outcome.stop_reason == StopReason::Suspended)
        .and_then(|outcome| outcome.resume_token.as_ref())
        .map(|token| token.0.as_str());

    snapshot
        .records
        .iter()
        .rev()
        .find_map(|record| match record {
            SessionRecord::PendingApproval(approval)
                if !resolved.contains(&approval.resume_token.0)
                    && target_token
                        .map(|token| token == approval.resume_token.0.as_str())
                        .unwrap_or(true) =>
            {
                Some(approval_view(approval))
            }
            _ => None,
        })
}

fn approval_view(record: &PendingApprovalRecord) -> ApprovalView {
    ApprovalView {
        resume_token: record.resume_token.0.clone(),
        created_at_ms: unix_time_ms(record.created_at),
        trigger_id: record.trigger_id.clone(),
        step: record.step,
        reason: match &record.reason {
            rain_engine_core::SuspendReason::HumanApprovalRequired { skill_names } => {
                format!("Human approval required for {}", skill_names.join(", "))
            }
            rain_engine_core::SuspendReason::ProviderRequested { message } => message.clone(),
        },
        pending_calls: record.pending_calls.clone(),
    }
}

fn build_timeline(records: &[SessionRecord]) -> Vec<TimelineItem> {
    records
        .iter()
        .filter_map(|record| match record {
            SessionRecord::Trigger(trigger) => match &trigger.trigger {
                AgentTrigger::HumanInput {
                    actor_id, content, ..
                } => Some(TimelineItem::HumanInput {
                    actor_id: actor_id.clone(),
                    content: content.clone(),
                    occurred_at_ms: unix_time_ms(trigger.recorded_at),
                }),
                AgentTrigger::ExternalEvent { source, .. }
                | AgentTrigger::Webhook { source, .. }
                | AgentTrigger::SystemObservation { source, .. } => Some(TimelineItem::System {
                    label: "event received".to_string(),
                    detail: source.clone(),
                    occurred_at_ms: unix_time_ms(trigger.recorded_at),
                }),
                AgentTrigger::ScheduledWake { reason, .. } => Some(TimelineItem::System {
                    label: "scheduled wake".to_string(),
                    detail: reason.clone(),
                    occurred_at_ms: unix_time_ms(trigger.recorded_at),
                }),
                AgentTrigger::Approval { decision, .. } => Some(TimelineItem::System {
                    label: "approval submitted".to_string(),
                    detail: format!("{decision:?}"),
                    occurred_at_ms: unix_time_ms(trigger.recorded_at),
                }),
                AgentTrigger::DelegationResult { correlation_id, .. } => {
                    Some(TimelineItem::System {
                        label: "delegation result".to_string(),
                        detail: correlation_id.0.clone(),
                        occurred_at_ms: unix_time_ms(trigger.recorded_at),
                    })
                }
                AgentTrigger::RuleTrigger { rule_id, .. } => Some(TimelineItem::System {
                    label: "rule trigger".to_string(),
                    detail: rule_id.clone(),
                    occurred_at_ms: unix_time_ms(trigger.recorded_at),
                }),
                AgentTrigger::ProactiveHeartbeat { .. } => Some(TimelineItem::System {
                    label: "heartbeat".to_string(),
                    detail: "runtime wake".to_string(),
                    occurred_at_ms: unix_time_ms(trigger.recorded_at),
                }),
                AgentTrigger::Message {
                    user_id, content, ..
                } => Some(TimelineItem::HumanInput {
                    actor_id: user_id.clone(),
                    content: content.clone(),
                    occurred_at_ms: unix_time_ms(trigger.recorded_at),
                }),
            },
            SessionRecord::Outcome(outcome) => outcome
                .response
                .clone()
                .or_else(|| outcome.detail.clone())
                .map(|content| TimelineItem::AssistantResponse {
                    content,
                    stop_reason: outcome.stop_reason.clone(),
                    occurred_at_ms: unix_time_ms(outcome.finished_at),
                }),
            SessionRecord::ToolCall(call) => Some(TimelineItem::ToolCall {
                call_id: call.call_id.clone(),
                skill_name: call.skill_name.clone(),
                formatted_call: format_tool_call(&call.skill_name, &call.args),
                occurred_at_ms: unix_time_ms(call.called_at),
            }),
            SessionRecord::ToolResult(result) => {
                let (success, preview) = tool_result_preview(result);
                Some(TimelineItem::ToolResult {
                    call_id: result.call_id.clone(),
                    skill_name: result.skill_name.clone(),
                    success,
                    preview,
                    occurred_at_ms: unix_time_ms(result.finished_at),
                })
            }
            SessionRecord::PendingApproval(approval) => Some(TimelineItem::ApprovalRequested {
                resume_token: approval.resume_token.0.clone(),
                pending_calls: approval.pending_calls.clone(),
                occurred_at_ms: unix_time_ms(approval.created_at),
            }),
            SessionRecord::ApprovalResolution(resolution) => Some(TimelineItem::ApprovalResolved {
                resume_token: resolution.resume_token.0.clone(),
                decision: resolution.decision.clone(),
                occurred_at_ms: unix_time_ms(resolution.resolved_at),
            }),
            SessionRecord::Deliberation(deliberation) => Some(TimelineItem::Plan {
                summary: deliberation.summary.clone(),
                candidate_actions: deliberation.candidate_actions.clone(),
                confidence: deliberation.confidence,
                outcome: deliberation.outcome.clone(),
                occurred_at_ms: unix_time_ms(deliberation.created_at),
            }),
            SessionRecord::ToolNodeCheckpoint(checkpoint) => Some(TimelineItem::ToolCheckpoint {
                call_id: checkpoint.call_id.clone(),
                skill_name: checkpoint.skill_name.clone(),
                status: checkpoint.status.clone(),
                attempt: checkpoint.attempt,
                detail: checkpoint.detail.clone(),
                occurred_at_ms: unix_time_ms(checkpoint.occurred_at),
            }),
            SessionRecord::SkillInputValidation(validation) if !validation.valid => {
                Some(TimelineItem::ValidationFailure {
                    call_id: validation.call_id.clone(),
                    skill_name: validation.skill_name.clone(),
                    errors: validation.errors.clone(),
                    occurred_at_ms: unix_time_ms(validation.validated_at),
                })
            }
            SessionRecord::Reflection(reflection) => Some(TimelineItem::Learning {
                label: "reflection".to_string(),
                detail: reflection.summary.clone(),
                confidence: reflection.confidence,
                occurred_at_ms: unix_time_ms(reflection.created_at),
            }),
            SessionRecord::PolicyTuning(tuning) => Some(TimelineItem::Learning {
                label: format!("policy {:?}", tuning.action).to_ascii_lowercase(),
                detail: tuning.overlay.reason.clone(),
                confidence: tuning.overlay.confidence,
                occurred_at_ms: unix_time_ms(tuning.created_at),
            }),
            SessionRecord::StrategyPreference(preference) => Some(TimelineItem::Learning {
                label: "strategy preference".to_string(),
                detail: preference.reason.clone(),
                confidence: preference.confidence,
                occurred_at_ms: unix_time_ms(preference.created_at),
            }),
            _ => None,
        })
        .collect()
}

fn build_tool_timeline(records: &[SessionRecord]) -> Vec<ToolTimelineItem> {
    let results = records
        .iter()
        .filter_map(|record| match record {
            SessionRecord::ToolResult(result) => Some((result.call_id.clone(), result)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();

    records
        .iter()
        .filter_map(|record| match record {
            SessionRecord::ToolCall(call) => Some(tool_timeline_item(
                call,
                results.get(&call.call_id).copied(),
            )),
            _ => None,
        })
        .collect()
}

fn tool_timeline_item(
    call: &ToolCallRecord,
    result: Option<&ToolResultRecord>,
) -> ToolTimelineItem {
    let (success, output_preview, failure_kind, finished_at_ms) = match result {
        Some(result) => {
            let (success, preview) = tool_result_preview(result);
            let failure_kind = match &result.output {
                Ok(_) => None,
                Err(error) => Some(format!("{:?}", error.kind)),
            };
            (
                Some(success),
                Some(preview),
                failure_kind,
                Some(unix_time_ms(result.finished_at)),
            )
        }
        None => (None, None, None, None),
    };

    ToolTimelineItem {
        call_id: call.call_id.clone(),
        skill_name: call.skill_name.clone(),
        step: call.step,
        called_at_ms: unix_time_ms(call.called_at),
        finished_at_ms,
        backend_kind: format!("{:?}", call.backend_kind),
        args: call.args.clone(),
        success,
        output_preview,
        failure_kind,
    }
}

fn tool_result_preview(result: &ToolResultRecord) -> (bool, String) {
    match &result.output {
        Ok(value) => (true, preview_value(value)),
        Err(error) => (false, error.message.clone()),
    }
}

fn preview_value(value: &Value) -> String {
    if let Some(stdout) = value.get("stdout").and_then(Value::as_str) {
        return truncate_preview(stdout);
    }
    if let Some(content) = value.get("content").and_then(Value::as_str) {
        return truncate_preview(content);
    }
    if let Some(text) = value.as_str() {
        return truncate_preview(text);
    }
    truncate_preview(&serde_json::to_string(value).unwrap_or_else(|_| "<unprintable>".to_string()))
}

fn truncate_preview(value: &str) -> String {
    const MAX_PREVIEW_CHARS: usize = 240;
    let mut preview = value
        .trim()
        .chars()
        .take(MAX_PREVIEW_CHARS)
        .collect::<String>();
    if value.trim().chars().count() > MAX_PREVIEW_CHARS {
        preview.push('…');
    }
    preview
}

fn format_tool_call(name: &str, args: &Value) -> String {
    match args {
        Value::Object(map) if map.is_empty() => format!("{name}()"),
        Value::Object(map) => {
            let rendered = map
                .iter()
                .map(|(key, value)| format!("{key}: {}", preview_value(value)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{name}({rendered})")
        }
        other => format!("{name}({})", preview_value(other)),
    }
}

// ---------------------------------------------------------------------------
// SSE streaming
// ---------------------------------------------------------------------------

async fn handle_sse_stream(
    State(state): State<RuntimeState>,
    Path(session_id): Path<String>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let memory = state.memory.clone();
    let mut last_seq: Option<i64> = None;

    let stream = async_stream::stream! {
        loop {
            let snapshot = match memory.load_session(&session_id).await {
                Ok(snap) => snap,
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            let current_seq = snapshot.last_sequence_no;
            if current_seq != last_seq {
                last_seq = current_seq;
                if let Ok(json) = serde_json::to_string(&build_session_view(snapshot.clone())) {
                    yield Ok(Event::default().event("session_view").data(json));
                }
                if let Ok(json) = serde_json::to_string(&snapshot.records) {
                    yield Ok(Event::default().event("records").data(json));
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::Request;
    use rain_engine_blob::LocalFileBlobStore;
    use rain_engine_core::{
        AgentAction, NativeSkill, PlannedSkillCall, SessionRecord, SkillExecutionError,
        SkillInvocation, SkillManifest, StopReason,
    };
    use serde_json::json;
    use tower::ServiceExt;

    #[derive(Clone)]
    struct ApprovalNativeSkill;

    #[async_trait]
    impl NativeSkill for ApprovalNativeSkill {
        async fn execute(
            &self,
            invocation: SkillInvocation,
        ) -> Result<serde_json::Value, SkillExecutionError> {
            Ok(json!({"approved": invocation.args}))
        }

        fn requires_human_approval(&self) -> bool {
            true
        }
    }

    fn server_config() -> RuntimeServerConfig {
        RuntimeServerConfig {
            bind_address: "127.0.0.1:0".parse().expect("addr"),
            request_timeout_ms: 1_000,
            default_policy: EnginePolicy::default(),
            allow_policy_overrides: true,
            allow_provider_overrides: true,
            default_provider: ProviderRequestConfig::default(),
        }
    }

    fn runtime_state_with_mock(response: &str) -> RuntimeState {
        let memory: Arc<dyn MemoryStore> = Arc::new(InMemoryMemoryStore::new());
        let blob_store: Arc<dyn BlobStore> =
            Arc::from(build_blob_store(&BlobBootstrapConfig::InMemory).expect("blob store"));
        let llm: Arc<dyn LlmProvider> =
            Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
                content: response.to_string(),
            }]));
        RuntimeState::new(
            AgentEngine::new(llm, memory.clone()),
            memory,
            blob_store,
            server_config(),
        )
    }

    #[tokio::test]
    async fn webhook_route_converts_json_request_into_agent_trigger() {
        let state = runtime_state_with_mock("processed");

        let response = app(state)
            .oneshot(
                Request::post("/triggers/webhook/github")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&WebhookIngressRequest {
                            session_id: "runtime-session".to_string(),
                            payload: serde_json::json!({"action": "opened"}),
                            attachments: Vec::new(),
                            granted_scopes: BTreeSet::new(),
                            idempotency_key: Some("abc".to_string()),
                            provider: None,
                            policy_override: None,
                        })
                        .expect("request json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let run: RuntimeRunResult = serde_json::from_slice(&bytes).expect("run result");
        assert_eq!(run.outcome.stop_reason, StopReason::Responded);
        assert_eq!(run.outcome.response.as_deref(), Some("processed"));
        assert!(!run.advances.is_empty());
    }

    #[tokio::test]
    async fn capabilities_route_exposes_runtime_surface() {
        let state = runtime_state_with_mock("processed");

        let response = app(state)
            .oneshot(
                Request::get("/capabilities")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let capabilities: RuntimeCapabilities =
            serde_json::from_slice(&bytes).expect("capabilities");
        assert!(capabilities.streaming);
        assert!(capabilities.approvals);
        assert!(
            capabilities
                .default_scopes
                .contains(&"tool:run".to_string())
        );
    }

    #[tokio::test]
    async fn session_view_projects_records_for_control_room() {
        let state = runtime_state_with_mock("processed");
        let router = app(state.clone());

        let response = router
            .oneshot(
                Request::post("/triggers/webhook/github")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&WebhookIngressRequest {
                            session_id: "view-session".to_string(),
                            payload: serde_json::json!({"action": "opened"}),
                            attachments: Vec::new(),
                            granted_scopes: BTreeSet::new(),
                            idempotency_key: None,
                            provider: None,
                            policy_override: None,
                        })
                        .expect("request json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let view_response = app(state)
            .oneshot(
                Request::get("/sessions/view-session/view")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(view_response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(view_response.into_body(), usize::MAX)
            .await
            .expect("body");
        let view: SessionView = serde_json::from_slice(&bytes).expect("session view");
        assert_eq!(view.status, SessionStatus::Completed);
        assert!(view.record_count >= 3);
        assert!(
            view.timeline
                .iter()
                .any(|item| matches!(item, TimelineItem::AssistantResponse { .. }))
        );
        assert!(!view.self_improvement.reflections.is_empty());
    }

    #[tokio::test]
    async fn multipart_webhook_persists_blob_attachment_references() {
        let memory: Arc<dyn MemoryStore> = Arc::new(InMemoryMemoryStore::new());
        let blob_store: Arc<dyn BlobStore> =
            Arc::from(build_blob_store(&BlobBootstrapConfig::InMemory).expect("blob store"));
        let llm: Arc<dyn LlmProvider> =
            Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
                content: "processed".to_string(),
            }]));
        let mut config = server_config();
        config.default_policy.max_inline_attachment_bytes = 4;
        let state = RuntimeState::new(
            AgentEngine::new(llm, memory.clone()),
            memory.clone(),
            blob_store,
            config,
        );

        let boundary = "rain-engine-boundary";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"session_id\"\r\n\r\nmulti-session\r\n\
--{boundary}\r\nContent-Disposition: form-data; name=\"payload\"\r\n\r\n{{\"event\":\"upload\"}}\r\n\
--{boundary}\r\nContent-Disposition: form-data; name=\"attachment\"; filename=\"schema.png\"\r\nContent-Type: image/png\r\n\r\n123456789\r\n\
--{boundary}--\r\n"
        );

        let response = app(state.clone())
            .oneshot(
                Request::post("/triggers/webhook/files")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(body.into_bytes()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let snapshot = state
            .memory()
            .load_session("multi-session")
            .await
            .expect("session");
        let trigger = snapshot
            .records
            .into_iter()
            .find_map(|record| match record {
                SessionRecord::Trigger(record) => Some(record.trigger),
                _ => None,
            })
            .expect("trigger");
        match trigger {
            AgentTrigger::Webhook { attachments, .. } => {
                assert_eq!(attachments.len(), 1);
                assert!(matches!(
                    attachments[0].content,
                    rain_engine_core::AttachmentContent::Blob { .. }
                ));
            }
            other => panic!("unexpected trigger: {other:?}"),
        }
    }

    #[tokio::test]
    async fn approval_route_resumes_suspended_native_skill() {
        let memory: Arc<dyn MemoryStore> = Arc::new(InMemoryMemoryStore::new());
        let blob_store: Arc<dyn BlobStore> =
            Arc::from(build_blob_store(&BlobBootstrapConfig::InMemory).expect("blob store"));
        let llm: Arc<dyn LlmProvider> = Arc::new(MockLlmProvider::scripted(vec![
            AgentAction::CallSkills(vec![PlannedSkillCall {
                call_id: "native-call".to_string(),
                name: "dangerous_native".to_string(),
                args: json!({"apply": true}),
                priority: 0,
                depends_on: Vec::new(),
                retry_policy: Default::default(),
                dry_run: false,
            }]),
            AgentAction::Respond {
                content: "completed".to_string(),
            },
        ]));
        let state = RuntimeState::new(
            AgentEngine::new(llm, memory.clone()),
            memory,
            blob_store,
            server_config(),
        );
        state
            .engine()
            .register_native_skill(
                SkillManifest {
                    name: "dangerous_native".to_string(),
                    description: "Requires approval".to_string(),
                    input_schema: json!({"type":"object"}),
                    required_scopes: vec!["tool:run".to_string()],
                    capability_grants: vec![],
                    resource_policy: rain_engine_core::ResourcePolicy::default_for_tools(),
                    approval_required: true,
                },
                Arc::new(ApprovalNativeSkill),
            )
            .await;

        let start = app(state.clone())
            .oneshot(
                Request::post("/triggers/webhook/github")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&WebhookIngressRequest {
                            session_id: "approval-session".to_string(),
                            payload: json!({"action": "deploy"}),
                            attachments: Vec::new(),
                            granted_scopes: BTreeSet::from(["tool:run".to_string()]),
                            idempotency_key: None,
                            provider: None,
                            policy_override: None,
                        })
                        .expect("request json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        let start_bytes = axum::body::to_bytes(start.into_body(), usize::MAX)
            .await
            .expect("body");
        let suspended: RuntimeRunResult = serde_json::from_slice(&start_bytes).expect("outcome");
        assert_eq!(suspended.outcome.stop_reason, StopReason::Suspended);
        let resume_token = suspended.outcome.resume_token.expect("resume token").0;

        let resume = app(state)
            .oneshot(
                Request::post("/triggers/approval")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&ApprovalIngressRequest {
                            session_id: "approval-session".to_string(),
                            resume_token,
                            decision: ApprovalDecision::Approved,
                            metadata: json!({"approved_by": "tester"}),
                            granted_scopes: BTreeSet::from(["tool:run".to_string()]),
                            provider: None,
                            policy_override: None,
                        })
                        .expect("request json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        let resume_bytes = axum::body::to_bytes(resume.into_body(), usize::MAX)
            .await
            .expect("body");
        let resumed: RuntimeRunResult = serde_json::from_slice(&resume_bytes).expect("outcome");
        assert_eq!(resumed.outcome.stop_reason, StopReason::Responded);
        assert_eq!(resumed.outcome.response.as_deref(), Some("completed"));
    }

    #[tokio::test]
    async fn invalid_runtime_config_fails_fast() {
        let result = build_runtime_state(RuntimeBootstrapConfig {
            server: server_config(),
            store: StoreBootstrapConfig::Sqlite {
                database_url: "".to_string(),
            },
            blob: BlobBootstrapConfig::InMemory,
            provider: ProviderBootstrapConfig::Mock {
                response: "processed".to_string(),
            },
        })
        .await;

        match result {
            Ok(_) => panic!("expected config error"),
            Err(error) => assert!(error.to_string().contains("must not be empty")),
        }
    }

    #[test]
    fn local_directory_blob_store_rejects_invalid_uri() {
        let error = LocalFileBlobStore::path_from_uri("memory://abc").expect_err("error");
        assert_eq!(error.message, "unsupported local blob uri");
    }
}
