//! Reference HTTP runtime for RainEngine.
//!
//! The runtime owns request parsing and repeated calls to `AgentEngine::advance`
//! until a terminal, suspended, delegated, or policy-stopped outcome is reached.

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header::CONTENT_TYPE},
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures_util::stream;
use rain_engine_blob::{BlobBackendConfig, build_blob_store};
use rain_engine_core::{
    AdvanceRequest, AgentEngine, AgentStateSnapshot, AgentTrigger, ApprovalDecision, AttachmentRef,
    BlobStore, ContinueRequest, CorrelationId, EngineError, EngineOutcome, EnginePolicy,
    InMemoryMemoryStore, LlmProvider, MemoryStore, MockLlmProvider, MultimodalPayload, NativeSkill,
    PendingApprovalRecord, ProcessRequest, ProviderRequestConfig, RecordPageQuery, ResourcePolicy,
    SessionListQuery, SessionRecord, SessionSnapshot, SkillCapability, SkillDefinition,
    SkillExecutionError, SkillFailureKind, SkillInvocation, SkillManifest, SkillStore, StopReason,
    ToolCallRecord, ToolResultRecord, WakeId, unix_time_ms,
};
use rain_engine_openai::{OpenAiCompatibleConfig, OpenAiCompatibleProvider};
use rain_engine_provider_gemini::{GeminiAuth, GeminiConfig, GeminiProvider};
use rain_engine_store_pg::PgMemoryStore;
use rain_engine_store_sqlite::SqliteMemoryStore;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{error, info};
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
pub struct InstallSkillRequest {
    pub manifest: rain_engine_core::SkillManifest,
    pub wasm_url: Option<String>,
    pub wasm_base64: Option<String>,
    pub file_path: Option<String>,
}

pub struct SkillInstallerSkill {
    engine: AgentEngine,
}

impl SkillInstallerSkill {
    pub fn new(engine: AgentEngine) -> Self {
        Self { engine }
    }
}

pub fn install_skill_manifest() -> SkillManifest {
    SkillManifest {
        name: "install_skill".to_string(),
        description: "Installs a new WASM-based skill from a URL.".to_string(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "description": { "type": "string" },
                "wasm_url": { "type": "string" },
                "input_schema": { "type": "object" }
            },
            "required": ["name", "description", "wasm_url", "input_schema"]
        }),
        required_scopes: vec!["operator:skills".to_string()],
        capability_grants: vec![],
        resource_policy: ResourcePolicy::default_for_tools(),
        approval_required: true,
        circuit_breaker_threshold: 0.5,
    }
}

#[async_trait]
impl NativeSkill for SkillInstallerSkill {
    async fn execute(&self, invocation: SkillInvocation) -> Result<Value, SkillExecutionError> {
        let name = invocation
            .args
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SkillExecutionError::new(SkillFailureKind::InvalidArguments, "missing name")
            })?;
        let description = invocation
            .args
            .get("description")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SkillExecutionError::new(SkillFailureKind::InvalidArguments, "missing description")
            })?;
        let wasm_url = invocation.args.get("wasm_url").and_then(|v| v.as_str());
        let wasm_base64 = invocation.args.get("wasm_base64").and_then(|v| v.as_str());
        let file_path = invocation.args.get("file_path").and_then(|v| v.as_str());

        if wasm_url.is_none() && wasm_base64.is_none() && file_path.is_none() {
            return Err(SkillExecutionError::new(
                SkillFailureKind::InvalidArguments,
                "missing wasm_url, wasm_base64, or file_path",
            ));
        }

        let input_schema = invocation
            .args
            .get("input_schema")
            .cloned()
            .ok_or_else(|| {
                SkillExecutionError::new(SkillFailureKind::InvalidArguments, "missing input_schema")
            })?;

        let manifest = SkillManifest {
            name: name.to_string(),
            description: description.to_string(),
            input_schema,
            required_scopes: vec!["tool:run".to_string()],
            capability_grants: vec![
                SkillCapability::HttpOutbound {
                    allow_hosts: vec![],
                },
                SkillCapability::StructuredLog,
            ],
            resource_policy: ResourcePolicy::default_for_tools(),
            approval_required: false,
            circuit_breaker_threshold: 0.5,
        };

        // Reuse the handler logic or just do it here
        let wasm_bytes = if let Some(url) = wasm_url {
            let client = reqwest::Client::new();
            client
                .get(url)
                .send()
                .await
                .map_err(|err| {
                    SkillExecutionError::new(
                        SkillFailureKind::Internal,
                        format!("Download failed: {}", err),
                    )
                })?
                .bytes()
                .await
                .map_err(|err| {
                    SkillExecutionError::new(
                        SkillFailureKind::Internal,
                        format!("Read failed: {}", err),
                    )
                })?
                .to_vec()
        } else if let Some(b64) = wasm_base64 {
            use base64::{Engine as _, engine::general_purpose};
            general_purpose::STANDARD.decode(b64).map_err(|err| {
                SkillExecutionError::new(
                    SkillFailureKind::InvalidArguments,
                    format!("Base64 decode failed: {}", err),
                )
            })?
        } else if let Some(path) = file_path {
            tokio::fs::read(path).await.map_err(|err| {
                SkillExecutionError::new(
                    SkillFailureKind::InvalidArguments,
                    format!("File read failed: {}", err),
                )
            })?
        } else {
            return Err(SkillExecutionError::new(
                SkillFailureKind::InvalidArguments,
                "missing wasm_url, wasm_base64, or file_path",
            ));
        };

        let config = rain_engine_wasm::WasmSkillConfig {
            manifest: manifest.clone(),
            wasm_bytes: Arc::new(wasm_bytes.to_vec()),
            capabilities: Arc::new(
                rain_engine_wasm::InMemoryCapabilityHost::new().with_http_client(),
            ),
        };

        let executor = rain_engine_wasm::WasmSkillExecutor::new(config).map_err(|err| {
            SkillExecutionError::new(SkillFailureKind::Internal, format!("Init failed: {}", err))
        })?;

        self.engine
            .register_wasm_skill_persistent(manifest, Arc::new(executor), wasm_bytes.to_vec())
            .await
            .map_err(|err| {
                SkillExecutionError::new(
                    SkillFailureKind::Internal,
                    format!("Storage failed: {err}"),
                )
            })?;

        Ok(serde_json::json!({
            "status": "success",
            "message": format!("Skill {} installed successfully", name)
        }))
    }
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
    #[serde(default)]
    pub async_ingress: bool,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StoreBootstrapConfig {
    InMemory,
    Sqlite { database_url: String },
    Postgres { database_url: String },
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CacheBootstrapConfig {
    pub valkey_url: Option<String>,
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
        #[serde(default = "default_gemini_embedding_model")]
        embedding_model: String,
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

fn default_gemini_embedding_model() -> String {
    "text-embedding-004".to_string()
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeBootstrapConfig {
    pub server: RuntimeServerConfig,
    pub store: StoreBootstrapConfig,
    #[serde(default)]
    pub cache: Option<CacheBootstrapConfig>,
    pub blob: BlobBootstrapConfig,
    pub provider: ProviderBootstrapConfig,
    #[serde(default)]
    pub enable_research_planner: bool,
}

#[derive(Clone)]
pub struct RuntimeState {
    engine: AgentEngine,
    memory: Arc<dyn MemoryStore>,
    blob_store: Arc<dyn BlobStore>,
    config: RuntimeServerConfig,
    provider_kind: String,
    default_scopes: Vec<String>,
    channel_statuses: Vec<RuntimeChannelView>,
    settings: RuntimeMutableSettings,
    ingress: Option<Arc<rain_engine_ingress::ValkeyStreamIngress>>,
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
#[serde(rename_all = "snake_case")]
pub enum SessionActivityState {
    Idle,
    Reasoning,
    RunningTools,
    WaitingHuman,
    WaitingExternal,
    Scheduled,
    Delegated,
    Errored,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WakeView {
    pub wake_id: String,
    pub reason: String,
    pub status: String,
    pub occurred_at_ms: i64,
    pub due_at_ms: Option<i64>,
    pub task_id: Option<String>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HeartbeatStatusView {
    pub wake_id: String,
    pub reason: String,
    pub occurred_at_ms: i64,
    pub outcome_summary: Option<String>,
    pub stop_reason: Option<StopReason>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionListItemView {
    pub session_id: String,
    pub status: SessionStatus,
    pub activity_state: SessionActivityState,
    pub current_focus: Option<String>,
    pub latest_provider: Option<String>,
    pub last_activity_at_ms: i64,
    pub pending_approval: bool,
    pub pending_wake: bool,
    pub unread_event_count: usize,
    pub active_channel_ids: Vec<String>,
    pub record_count: usize,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeChannelStatus {
    Connected,
    Degraded,
    Disabled,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeChannelView {
    pub channel_id: String,
    pub label: String,
    pub transport: String,
    pub status: RuntimeChannelStatus,
    pub detail: Option<String>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApprovalMetadataSupport {
    pub structured_json: bool,
    pub recommended_fields: Vec<String>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UploadSupport {
    pub multipart: bool,
    pub max_request_bytes: usize,
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
    pub activity_state: SessionActivityState,
    pub current_focus: Option<String>,
    pub current_task_id: Option<String>,
    pub current_task_title: Option<String>,
    pub next_wake_at_ms: Option<i64>,
    pub blocked_reason: Option<String>,
    pub last_human_input_at_ms: Option<i64>,
    pub last_assistant_activity_at_ms: Option<i64>,
    pub active_channel_ids: Vec<String>,
    pub pending_wake: Option<WakeView>,
    pub wake_history: Vec<WakeView>,
    pub last_heartbeat: Option<HeartbeatStatusView>,
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
    pub channels: Vec<RuntimeChannelView>,
    pub approval_metadata: ApprovalMetadataSupport,
    pub wake_support: bool,
    pub delegation_support: bool,
    pub learning_support: bool,
    pub upload_limits: UploadSupport,
    pub skills: Vec<SkillDefinition>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeSettingsView {
    pub shell_exec: MutableSkillPolicyView,
    pub http_fetch: MutableSkillPolicyView,
    pub web_reader: MutableSkillPolicyView,
    pub engine_policy: rain_engine_core::EnginePolicy,
    pub provider_config: rain_engine_core::ProviderRequestConfig,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MutableSkillPolicyView {
    pub permissive: bool,
    pub allowlist: Vec<String>,
    pub timeout_secs: u64,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct RuntimeSettingsUpdateRequest {
    #[serde(default)]
    pub shell_exec: Option<MutableSkillPolicyUpdate>,
    #[serde(default)]
    pub http_fetch: Option<MutableSkillPolicyUpdate>,
    #[serde(default)]
    pub web_reader: Option<MutableSkillPolicyUpdate>,
    #[serde(default)]
    pub engine_policy: Option<rain_engine_core::EnginePolicy>,
    #[serde(default)]
    pub provider_config: Option<rain_engine_core::ProviderRequestConfig>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct MutableSkillPolicyUpdate {
    #[serde(default)]
    pub permissive: Option<bool>,
    #[serde(default)]
    pub allowlist: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct ManagedSkillPolicy {
    pub access: rain_engine_skills::SharedAccessPolicy,
    pub timeout: Duration,
}

impl ManagedSkillPolicy {
    pub fn new(access: rain_engine_skills::SharedAccessPolicy, timeout: Duration) -> Self {
        Self { access, timeout }
    }

    async fn to_view(&self) -> MutableSkillPolicyView {
        let policy = self.access.read().await;
        let mut allowlist = policy.allowlist.iter().cloned().collect::<Vec<_>>();
        allowlist.sort();
        MutableSkillPolicyView {
            permissive: policy.permissive,
            allowlist,
            timeout_secs: self.timeout.as_secs(),
        }
    }

    async fn apply(&self, update: MutableSkillPolicyUpdate) {
        let mut policy = self.access.write().await;
        if let Some(permissive) = update.permissive {
            policy.permissive = permissive;
        }
        if let Some(allowlist) = update.allowlist {
            policy.allowlist = normalize_allowlist(allowlist);
        }
    }
}

#[derive(Clone)]
pub struct RuntimeMutableSettings {
    pub shell_exec: ManagedSkillPolicy,
    pub http_fetch: ManagedSkillPolicy,
    pub web_reader: ManagedSkillPolicy,
    pub engine_policy: std::sync::Arc<tokio::sync::RwLock<rain_engine_core::EnginePolicy>>,
    pub provider_config:
        std::sync::Arc<tokio::sync::RwLock<rain_engine_core::ProviderRequestConfig>>,
}

impl RuntimeMutableSettings {
    pub fn defaults(
        engine_policy: rain_engine_core::EnginePolicy,
        provider_config: rain_engine_core::ProviderRequestConfig,
    ) -> Self {
        let timeout = Duration::from_secs(30);
        Self {
            shell_exec: ManagedSkillPolicy::new(
                rain_engine_skills::shared_access_policy(HashSet::new(), false),
                timeout,
            ),
            http_fetch: ManagedSkillPolicy::new(
                rain_engine_skills::shared_access_policy(HashSet::new(), false),
                timeout,
            ),
            web_reader: ManagedSkillPolicy::new(
                rain_engine_skills::shared_access_policy(HashSet::new(), false),
                timeout,
            ),
            engine_policy: std::sync::Arc::new(tokio::sync::RwLock::new(engine_policy)),
            provider_config: std::sync::Arc::new(tokio::sync::RwLock::new(provider_config)),
        }
    }

    async fn to_view(&self) -> RuntimeSettingsView {
        let engine_policy = self.engine_policy.read().await.clone();
        let provider_config = self.provider_config.read().await.clone();
        RuntimeSettingsView {
            shell_exec: self.shell_exec.to_view().await,
            http_fetch: self.http_fetch.to_view().await,
            web_reader: self.web_reader.to_view().await,
            engine_policy,
            provider_config,
        }
    }

    async fn apply(&self, update: RuntimeSettingsUpdateRequest) {
        if let Some(shell_exec) = update.shell_exec {
            self.shell_exec.apply(shell_exec).await;
        }
        if let Some(http_fetch) = update.http_fetch {
            self.http_fetch.apply(http_fetch).await;
        }
        if let Some(web_reader) = update.web_reader {
            self.web_reader.apply(web_reader).await;
        }
        if let Some(engine_policy) = update.engine_policy {
            *self.engine_policy.write().await = engine_policy;
        }
        if let Some(provider_config) = update.provider_config {
            *self.provider_config.write().await = provider_config;
        }
    }
}

impl RuntimeState {
    pub fn new(
        engine: AgentEngine,
        memory: Arc<dyn MemoryStore>,
        blob_store: Arc<dyn BlobStore>,
        config: RuntimeServerConfig,
        settings: RuntimeMutableSettings,
    ) -> Self {
        Self {
            engine,
            memory,
            blob_store,
            config,
            provider_kind: "custom".to_string(),
            default_scopes: vec!["tool:run".to_string()],
            channel_statuses: Vec::new(),
            settings,
            ingress: None,
        }
    }

    pub fn with_provider_kind(mut self, provider_kind: impl Into<String>) -> Self {
        self.provider_kind = provider_kind.into();
        self
    }

    pub fn with_default_scopes(mut self, default_scopes: Vec<String>) -> Self {
        self.default_scopes = default_scopes;
        self
    }

    pub fn with_channel_statuses(mut self, channel_statuses: Vec<RuntimeChannelView>) -> Self {
        self.channel_statuses = channel_statuses;
        self
    }

    pub fn with_settings(mut self, settings: RuntimeMutableSettings) -> Self {
        self.settings = settings;
        self
    }

    pub fn with_ingress(mut self, ingress: rain_engine_ingress::ValkeyStreamIngress) -> Self {
        self.ingress = Some(Arc::new(ingress));
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

    pub fn settings(&self) -> RuntimeMutableSettings {
        self.settings.clone()
    }
}

fn normalize_allowlist(values: Vec<String>) -> HashSet<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect()
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

    let (memory, skill_store): (Arc<dyn MemoryStore>, Option<Arc<dyn SkillStore>>) =
        match &config.store {
            StoreBootstrapConfig::InMemory => (
                Arc::new(InMemoryMemoryStore::new()),
                Some(Arc::new(rain_engine_core::InMemorySkillStore::new()) as Arc<dyn SkillStore>),
            ),
            StoreBootstrapConfig::Sqlite { database_url } => {
                if database_url.trim().is_empty() {
                    return Err(RuntimeConfigError::Invalid(
                        "sqlite database_url must not be empty".to_string(),
                    ));
                }
                let store = Arc::new(
                    SqliteMemoryStore::connect(database_url)
                        .await
                        .map_err(|err| RuntimeConfigError::Invalid(err.message))?,
                );
                (
                    store.clone() as Arc<dyn MemoryStore>,
                    Some(store as Arc<dyn SkillStore>),
                )
            }
            StoreBootstrapConfig::Postgres { database_url } => {
                if database_url.trim().is_empty() {
                    return Err(RuntimeConfigError::Invalid(
                        "postgres database_url must not be empty".to_string(),
                    ));
                }
                (
                    Arc::new(
                        PgMemoryStore::connect_lazy(database_url)
                            .map_err(|err| RuntimeConfigError::Invalid(err.message))?,
                    ),
                    None,
                )
            }
        };

    let blob_store: Arc<dyn BlobStore> = Arc::from(
        build_blob_store(&config.blob).map_err(|err| RuntimeConfigError::Invalid(err.message))?,
    );

    let llm: Arc<dyn LlmProvider> = match &config.provider {
        ProviderBootstrapConfig::Mock { response } => {
            let response_str = response.clone();
            Arc::new(MockLlmProvider::dynamic(move |_| {
                Ok(rain_engine_core::ProviderDecision {
                    action: rain_engine_core::AgentAction::Respond {
                        content: response_str.clone(),
                    },
                    usage: None,
                    cache: None,
                })
            }))
        }
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
            embedding_model,
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
                embedding_model: embedding_model.clone(),
            })
            .map_err(|err| RuntimeConfigError::Invalid(err.to_string()))?,
        ),
    };

    let mut engine = AgentEngine::new(llm.clone(), memory.clone());

    if let Some(cache_config) = &config.cache
        && let Some(valkey_url) = &cache_config.valkey_url
    {
        let valkey_cache = rain_engine_core::ValkeyStateCache::new(valkey_url, "rain")
            .map_err(|err| RuntimeConfigError::Invalid(format!("Valkey cache error: {}", err)))?;
        engine = engine.with_state_cache(Arc::new(valkey_cache));
        tracing::info!("Configured Valkey state projection cache");
    }

    if let Some(store) = &skill_store {
        engine = engine.with_skill_store(store.clone());

        // Reload persisted WASM skills
        if let Ok(persisted_skills) = store.list_skills().await {
            info!("Reloading {} persisted skills...", persisted_skills.len());
            for (manifest, wasm_bytes) in persisted_skills {
                let config = rain_engine_wasm::WasmSkillConfig {
                    manifest: manifest.clone(),
                    wasm_bytes: Arc::new(wasm_bytes),
                    capabilities: Arc::new(
                        rain_engine_wasm::InMemoryCapabilityHost::new().with_http_client(),
                    ),
                };
                match rain_engine_wasm::WasmSkillExecutor::new(config) {
                    Ok(executor) => {
                        engine.register_wasm_skill(manifest, Arc::new(executor));
                    }
                    Err(err) => {
                        error!("Failed to reload skill {}: {}", manifest.name, err);
                    }
                }
            }
        }
    }

    engine.register_native_skill(
        install_skill_manifest(),
        Arc::new(SkillInstallerSkill::new(engine.clone())),
    );

    let settings = RuntimeMutableSettings::defaults(
        rain_engine_core::EnginePolicy::default(),
        rain_engine_core::ProviderRequestConfig::default(),
    );
    engine.register_native_skill(
        rain_engine_skills::web_reader::manifest(),
        Arc::new(
            rain_engine_skills::web_reader::WebReaderSkill::with_shared_policy(
                settings.web_reader.access.clone(),
                settings.web_reader.timeout,
            ),
        ),
    );

    if config.enable_research_planner {
        engine = engine.with_planner(Arc::new(rain_engine_cognition::ResearchPlanner::new(llm)));
    }

    Ok(
        RuntimeState::new(engine, memory, blob_store, config.server, settings)
            .with_provider_kind(provider_kind),
    )
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
        .route(
            "/settings",
            get(handle_get_settings).put(handle_update_settings),
        )
        .route("/sessions", get(handle_list_sessions))
        .route("/sessions/views", get(handle_list_session_views))
        .route("/sessions/{session_id}", get(handle_get_session))
        .route("/sessions/{session_id}/view", get(handle_get_session_view))
        .route(
            "/sessions/{session_id}/execution-graph",
            get(handle_get_execution_graph),
        )
        .route("/sessions/{session_id}/records", get(handle_list_records))
        // Capabilities routes
        .route("/capabilities/skills", post(handle_install_skill))
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
) -> Result<axum::response::Response, (StatusCode, String)> {
    let policy = effective_policy(&state, request.policy_override).await;
    let provider = effective_provider(&state, request.provider).await;
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
) -> Result<axum::response::Response, (StatusCode, String)> {
    let policy = effective_policy(&state, request.policy_override).await;
    let provider = effective_provider(&state, request.provider).await;
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
) -> Result<axum::response::Response, (StatusCode, String)> {
    let policy = effective_policy(&state, request.policy_override).await;
    let provider = effective_provider(&state, request.provider).await;
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
) -> Result<axum::response::Response, (StatusCode, String)> {
    let policy = effective_policy(&state, request.policy_override).await;
    let provider = effective_provider(&state, request.provider).await;
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
) -> Result<axum::response::Response, (StatusCode, String)> {
    let policy = effective_policy(&state, request.policy_override).await;
    let provider = effective_provider(&state, request.provider).await;
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
) -> Result<axum::response::Response, (StatusCode, String)> {
    let envelope = parse_webhook_envelope(request)
        .await
        .map_err(map_ingress_error)?;
    let policy = effective_policy(&state, envelope.policy_override).await;
    let provider = effective_provider(&state, envelope.provider).await;
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
) -> Result<axum::response::Response, (StatusCode, String)> {
    let policy = effective_policy(&state, request.policy_override).await;
    let provider = effective_provider(&state, request.provider).await;
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

async fn effective_policy(
    state: &RuntimeState,
    override_policy: Option<EnginePolicy>,
) -> EnginePolicy {
    if state.config.allow_policy_overrides
        && let Some(p) = override_policy
    {
        return p;
    }
    state.settings.engine_policy.read().await.clone()
}

async fn effective_provider(
    state: &RuntimeState,
    override_provider: Option<ProviderRequestConfig>,
) -> ProviderRequestConfig {
    if state.config.allow_provider_overrides
        && let Some(p) = override_provider
    {
        return p;
    }
    state.settings.provider_config.read().await.clone()
}

async fn run_process_request(
    state: &RuntimeState,
    mut request: ProcessRequest,
) -> Result<axum::response::Response, (StatusCode, String)> {
    // Default scopes for simple ingress if none provided
    if request.granted_scopes.is_empty() {
        request
            .granted_scopes
            .extend(state.default_scopes.iter().cloned());
    }

    if state.config.async_ingress
        && let Some(ingress) = &state.ingress
    {
        let envelope = rain_engine_ingress::IngressEventEnvelope {
            session_id: request.session_id.clone(),
            trigger: request.trigger,
            granted_scopes: request.granted_scopes,
            idempotency_key: request.idempotency_key,
            policy: Some(request.policy),
            provider: Some(request.provider),
        };
        ingress
            .publish(&envelope)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

        let response_body = serde_json::json!({
            "session_id": request.session_id,
            "status": "accepted"
        });
        return Ok((StatusCode::ACCEPTED, axum::Json(response_body)).into_response());
    }

    let timeout = Duration::from_millis(state.config.request_timeout_ms.max(1));
    match tokio::time::timeout(timeout, run_until_terminal_trace(&state.engine, request)).await {
        Ok(Ok(result)) => Ok(axum::Json(result).into_response()),
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
        default_scopes: state.default_scopes.clone(),
        default_policy: state.config.default_policy.clone(),
        channels: state.channel_statuses.clone(),
        approval_metadata: ApprovalMetadataSupport {
            structured_json: true,
            recommended_fields: vec![
                "actor_id".to_string(),
                "client".to_string(),
                "reason".to_string(),
                "decided_at_ms".to_string(),
            ],
        },
        wake_support: true,
        delegation_support: true,
        learning_support: true,
        upload_limits: UploadSupport {
            multipart: true,
            max_request_bytes: MAX_INGRESS_BODY_BYTES,
        },
        skills: state.engine.skill_definitions().await,
    })
}

async fn handle_get_settings(State(state): State<RuntimeState>) -> Json<RuntimeSettingsView> {
    Json(state.settings.to_view().await)
}

async fn handle_update_settings(
    State(state): State<RuntimeState>,
    Json(request): Json<RuntimeSettingsUpdateRequest>,
) -> Json<RuntimeSettingsView> {
    state.settings.apply(request).await;
    Json(state.settings.to_view().await)
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

async fn handle_list_session_views(
    State(state): State<RuntimeState>,
    Query(params): Query<SessionListParams>,
) -> Result<Json<Vec<SessionListItemView>>, (StatusCode, String)> {
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

    let mut views = Vec::with_capacity(sessions.len());
    for session in sessions {
        let snapshot = if let Ok(Some(cached)) = state
            .engine
            .state_cache()
            .get_projection(&session.session_id)
            .await
        {
            cached
        } else {
            state
                .memory
                .load_session(&session.session_id)
                .await
                .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.message))?
        };
        views.push(build_session_list_item(snapshot));
    }

    views.sort_by(|left, right| right.last_activity_at_ms.cmp(&left.last_activity_at_ms));
    Ok(Json(views))
}

async fn handle_get_session(
    State(state): State<RuntimeState>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let snapshot =
        if let Ok(Some(cached)) = state.engine.state_cache().get_projection(&session_id).await {
            cached
        } else {
            state
                .memory
                .load_session(&session_id)
                .await
                .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.message))?
        };
    Ok(Json(serde_json::to_value(snapshot).unwrap_or_default()))
}

async fn handle_get_session_view(
    State(state): State<RuntimeState>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionView>, (StatusCode, String)> {
    let snapshot =
        if let Ok(Some(cached)) = state.engine.state_cache().get_projection(&session_id).await {
            cached
        } else {
            state
                .memory
                .load_session(&session_id)
                .await
                .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.message))?
        };
    Ok(Json(build_session_view(snapshot)))
}

async fn handle_get_execution_graph(
    State(state): State<RuntimeState>,
    Path(session_id): Path<String>,
) -> Result<Json<ExecutionGraphView>, (StatusCode, String)> {
    let snapshot =
        if let Ok(Some(cached)) = state.engine.state_cache().get_projection(&session_id).await {
            cached
        } else {
            state
                .memory
                .load_session(&session_id)
                .await
                .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.message))?
        };
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

async fn handle_install_skill(
    State(state): State<RuntimeState>,
    Json(request): Json<InstallSkillRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let direct_install_enabled = std::env::var("RAIN_ENABLE_DIRECT_SKILL_INSTALL")
        .map(|value| value == "true" || value == "1")
        .unwrap_or(false);
    if !direct_install_enabled {
        return Err((
            StatusCode::FORBIDDEN,
            "direct skill install is disabled; use operator tooling or set RAIN_ENABLE_DIRECT_SKILL_INSTALL=true"
                .to_string(),
        ));
    }

    info!("Attempting to install skill: {}", request.manifest.name);

    let wasm_bytes = if let Some(url) = &request.wasm_url {
        let client = reqwest::Client::new();
        client
            .get(url)
            .send()
            .await
            .map_err(|err| (StatusCode::BAD_GATEWAY, format!("Download failed: {}", err)))?
            .bytes()
            .await
            .map_err(|err| (StatusCode::BAD_GATEWAY, format!("Read failed: {}", err)))?
            .to_vec()
    } else if let Some(b64) = &request.wasm_base64 {
        use base64::{Engine as _, engine::general_purpose};
        general_purpose::STANDARD.decode(b64).map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                format!("Base64 decode failed: {}", err),
            )
        })?
    } else if let Some(path) = &request.file_path {
        tokio::fs::read(path).await.map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                format!("File read failed: {}", err),
            )
        })?
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            "Must provide wasm_url, wasm_base64, or file_path".to_string(),
        ));
    };

    let config = rain_engine_wasm::WasmSkillConfig {
        manifest: request.manifest.clone(),
        wasm_bytes: Arc::new(wasm_bytes.clone()),
        capabilities: Arc::new(rain_engine_wasm::InMemoryCapabilityHost::new().with_http_client()),
    };

    let executor = rain_engine_wasm::WasmSkillExecutor::new(config).map_err(|err| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Init failed: {}", err),
        )
    })?;
    let executor = Arc::new(executor);
    state
        .engine
        .register_wasm_skill_persistent(request.manifest, executor.clone(), wasm_bytes)
        .await
        .map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Storage failed: {err}"),
            )
        })?;

    info!(
        "Skill successfully registered: {}",
        executor.manifest().name
    );
    Ok(StatusCode::CREATED)
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
    let activity_state = derive_activity_state(&snapshot, pending_approval.as_ref());
    let total_estimated_cost_usd = snapshot.total_estimated_cost_usd();
    let self_improvement = build_self_improvement_view(&snapshot);
    let execution_graph = build_execution_graph_view(&snapshot);
    let current_task = current_task(&state);
    let active_channel_ids = derive_active_channel_ids(&snapshot.records);
    let pending_wake = state.pending_wake.as_ref().map(pending_wake_view);
    let current_focus = derive_current_focus(
        &snapshot,
        pending_approval.as_ref(),
        activity_state.clone(),
        current_task.as_ref(),
        pending_wake.as_ref(),
    );
    let blocked_reason = derive_blocked_reason(
        pending_approval.as_ref(),
        current_task.as_ref(),
        pending_wake.as_ref(),
    );

    SessionView {
        session_id: snapshot.session_id,
        status,
        activity_state,
        current_focus,
        current_task_id: current_task.as_ref().map(|task| task.task_id.0.clone()),
        current_task_title: current_task.as_ref().map(|task| task.title.clone()),
        next_wake_at_ms: pending_wake.as_ref().and_then(|wake| wake.due_at_ms),
        blocked_reason,
        last_human_input_at_ms: last_human_input_at_ms(&snapshot.records),
        last_assistant_activity_at_ms: last_assistant_activity_at_ms(&snapshot.records),
        active_channel_ids,
        pending_wake,
        wake_history: build_wake_history(&snapshot.records),
        last_heartbeat: build_last_heartbeat(&snapshot.records),
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

fn build_session_list_item(snapshot: SessionSnapshot) -> SessionListItemView {
    let state = snapshot.agent_state();
    let pending_approval = latest_pending_approval(&snapshot);
    let status = derive_session_status(&snapshot, pending_approval.as_ref());
    let activity_state = derive_activity_state(&snapshot, pending_approval.as_ref());
    let pending_wake = state.pending_wake.as_ref().map(pending_wake_view);
    let current_task = current_task(&state);
    let current_focus = derive_current_focus(
        &snapshot,
        pending_approval.as_ref(),
        activity_state.clone(),
        current_task.as_ref(),
        pending_wake.as_ref(),
    );

    SessionListItemView {
        session_id: snapshot.session_id.clone(),
        status,
        activity_state: activity_state.clone(),
        current_focus,
        latest_provider: latest_provider_name(&snapshot.records),
        last_activity_at_ms: last_activity_at_ms(&snapshot.records),
        pending_approval: pending_approval.is_some(),
        pending_wake: pending_wake.is_some(),
        unread_event_count: derive_unread_event_count(&snapshot.records),
        active_channel_ids: derive_active_channel_ids(&snapshot.records),
        record_count: snapshot.records.len(),
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

fn derive_activity_state(
    snapshot: &SessionSnapshot,
    pending_approval: Option<&ApprovalView>,
) -> SessionActivityState {
    if pending_approval.is_some() {
        return SessionActivityState::WaitingHuman;
    }

    if snapshot.active_tool_execution_graph().is_some() {
        return SessionActivityState::RunningTools;
    }

    if snapshot
        .records
        .iter()
        .rev()
        .find_map(|record| match record {
            SessionRecord::Deliberation(_) => Some(SessionActivityState::Reasoning),
            SessionRecord::Delegation(_) => Some(SessionActivityState::Delegated),
            SessionRecord::KernelEvent(event) => match &event.event {
                rain_engine_core::KernelEvent::TaskBlocked { .. } => {
                    Some(SessionActivityState::WaitingExternal)
                }
                rain_engine_core::KernelEvent::WakeRequested(_)
                | rain_engine_core::KernelEvent::WakeScheduled(_) => {
                    Some(SessionActivityState::Scheduled)
                }
                _ => None,
            },
            SessionRecord::Outcome(outcome) => match outcome.stop_reason {
                StopReason::Delegated => Some(SessionActivityState::Delegated),
                StopReason::ProviderFailure
                | StopReason::StorageFailure
                | StopReason::PolicyAborted
                | StopReason::DeadlineExceeded
                | StopReason::Cancelled => Some(SessionActivityState::Errored),
                _ => None,
            },
            _ => None,
        })
        .is_some()
    {
        return snapshot
            .records
            .iter()
            .rev()
            .find_map(|record| match record {
                SessionRecord::Deliberation(_) => Some(SessionActivityState::Reasoning),
                SessionRecord::Delegation(_) => Some(SessionActivityState::Delegated),
                SessionRecord::KernelEvent(event) => match &event.event {
                    rain_engine_core::KernelEvent::TaskBlocked { .. } => {
                        Some(SessionActivityState::WaitingExternal)
                    }
                    rain_engine_core::KernelEvent::WakeRequested(_)
                    | rain_engine_core::KernelEvent::WakeScheduled(_) => {
                        Some(SessionActivityState::Scheduled)
                    }
                    _ => None,
                },
                SessionRecord::Outcome(outcome) => match outcome.stop_reason {
                    StopReason::Delegated => Some(SessionActivityState::Delegated),
                    StopReason::ProviderFailure
                    | StopReason::StorageFailure
                    | StopReason::PolicyAborted
                    | StopReason::DeadlineExceeded
                    | StopReason::Cancelled => Some(SessionActivityState::Errored),
                    _ => None,
                },
                _ => None,
            })
            .unwrap_or(SessionActivityState::Idle);
    }

    match snapshot
        .latest_outcome
        .as_ref()
        .map(|outcome| &outcome.stop_reason)
    {
        Some(StopReason::Delegated) => SessionActivityState::Delegated,
        Some(
            StopReason::ProviderFailure
            | StopReason::StorageFailure
            | StopReason::PolicyAborted
            | StopReason::DeadlineExceeded
            | StopReason::Cancelled,
        ) => SessionActivityState::Errored,
        _ if snapshot.agent_state().pending_wake.is_some() => SessionActivityState::Scheduled,
        _ => SessionActivityState::Idle,
    }
}

fn current_task(state: &AgentStateSnapshot) -> Option<rain_engine_core::TaskRecord> {
    let priority_order = |status: &rain_engine_core::TaskStatus| match status {
        rain_engine_core::TaskStatus::Running => 0,
        rain_engine_core::TaskStatus::Ready => 1,
        rain_engine_core::TaskStatus::WaitingHuman => 2,
        rain_engine_core::TaskStatus::Blocked => 3,
        rain_engine_core::TaskStatus::Pending => 4,
        rain_engine_core::TaskStatus::Failed => 5,
        rain_engine_core::TaskStatus::Done => 6,
        rain_engine_core::TaskStatus::Abandoned => 7,
    };

    state
        .tasks
        .iter()
        .filter(|task| {
            !matches!(
                task.status,
                rain_engine_core::TaskStatus::Done | rain_engine_core::TaskStatus::Abandoned
            )
        })
        .min_by_key(|task| (priority_order(&task.status), unix_time_ms(task.created_at)))
        .cloned()
}

fn derive_current_focus(
    snapshot: &SessionSnapshot,
    pending_approval: Option<&ApprovalView>,
    activity_state: SessionActivityState,
    current_task: Option<&rain_engine_core::TaskRecord>,
    pending_wake: Option<&WakeView>,
) -> Option<String> {
    if let Some(approval) = pending_approval {
        return Some(approval.reason.clone());
    }

    if let Some(task) = current_task {
        return Some(match task.status {
            rain_engine_core::TaskStatus::Running => format!("Working on {}", task.title),
            rain_engine_core::TaskStatus::Ready => format!("Ready to start {}", task.title),
            rain_engine_core::TaskStatus::WaitingHuman => {
                format!("Awaiting approval for {}", task.title)
            }
            rain_engine_core::TaskStatus::Blocked => format!("Blocked on {}", task.title),
            rain_engine_core::TaskStatus::Pending => format!("Queued {}", task.title),
            rain_engine_core::TaskStatus::Failed => format!("Recovering {}", task.title),
            rain_engine_core::TaskStatus::Done | rain_engine_core::TaskStatus::Abandoned => {
                task.title.clone()
            }
        });
    }

    if let Some(wake) = pending_wake {
        return Some(format!("Scheduled wake: {}", wake.reason));
    }

    if let Some(deliberation) = snapshot
        .records
        .iter()
        .rev()
        .find_map(|record| match record {
            SessionRecord::Deliberation(deliberation) => Some(deliberation.summary.clone()),
            _ => None,
        })
    {
        return Some(deliberation);
    }

    if let Some(outcome) = snapshot.latest_outcome.as_ref() {
        return outcome
            .response
            .clone()
            .or_else(|| outcome.detail.clone())
            .map(|text| truncate_preview(&text));
    }

    Some(match activity_state {
        SessionActivityState::RunningTools => "Executing tools".to_string(),
        SessionActivityState::Reasoning => "Evaluating next action".to_string(),
        SessionActivityState::WaitingExternal => "Waiting on external state".to_string(),
        SessionActivityState::Scheduled => "Waiting for scheduled wake".to_string(),
        SessionActivityState::Delegated => "Waiting on delegated work".to_string(),
        SessionActivityState::Errored => "Investigating a failed step".to_string(),
        SessionActivityState::WaitingHuman => "Waiting on a human decision".to_string(),
        SessionActivityState::Idle => "Idle until the next event".to_string(),
    })
}

fn derive_blocked_reason(
    pending_approval: Option<&ApprovalView>,
    current_task: Option<&rain_engine_core::TaskRecord>,
    pending_wake: Option<&WakeView>,
) -> Option<String> {
    if let Some(approval) = pending_approval {
        return Some(approval.reason.clone());
    }
    if let Some(task) = current_task
        && matches!(
            task.status,
            rain_engine_core::TaskStatus::Blocked | rain_engine_core::TaskStatus::WaitingHuman
        )
    {
        return task.detail.clone().or_else(|| {
            if task.blocked_by.is_empty() {
                None
            } else {
                Some(format!("blocked by {}", task.blocked_by.len()))
            }
        });
    }
    pending_wake.map(|wake| wake.reason.clone())
}

fn derive_active_channel_ids(records: &[SessionRecord]) -> Vec<String> {
    let mut channels = BTreeSet::new();
    for record in records {
        if let SessionRecord::Trigger(trigger) = record {
            match &trigger.trigger {
                AgentTrigger::HumanInput { actor_id, .. }
                | AgentTrigger::Message {
                    user_id: actor_id, ..
                } => {
                    if let Some((channel, _)) = actor_id.split_once(':') {
                        channels.insert(channel.to_string());
                    }
                }
                _ => {}
            }
        }
    }
    channels.into_iter().collect()
}

fn last_human_input_at_ms(records: &[SessionRecord]) -> Option<i64> {
    records.iter().rev().find_map(|record| match record {
        SessionRecord::Trigger(trigger)
            if matches!(
                trigger.trigger,
                AgentTrigger::HumanInput { .. } | AgentTrigger::Message { .. }
            ) =>
        {
            Some(unix_time_ms(trigger.recorded_at))
        }
        _ => None,
    })
}

fn last_assistant_activity_at_ms(records: &[SessionRecord]) -> Option<i64> {
    records.iter().rev().find_map(|record| match record {
        SessionRecord::Outcome(outcome) => Some(unix_time_ms(outcome.finished_at)),
        SessionRecord::ToolResult(result) => Some(unix_time_ms(result.finished_at)),
        SessionRecord::Deliberation(deliberation) => Some(unix_time_ms(deliberation.created_at)),
        _ => None,
    })
}

fn last_activity_at_ms(records: &[SessionRecord]) -> i64 {
    records
        .iter()
        .rev()
        .find_map(record_occurred_at_ms)
        .unwrap_or_default()
}

fn record_occurred_at_ms(record: &SessionRecord) -> Option<i64> {
    match record {
        SessionRecord::Trigger(trigger) => Some(unix_time_ms(trigger.recorded_at)),
        SessionRecord::TriggerIntent(intent) => Some(unix_time_ms(intent.classified_at)),
        SessionRecord::KernelEvent(event) => Some(unix_time_ms(event.occurred_at)),
        SessionRecord::ModelDecision(decision) => Some(unix_time_ms(decision.decided_at)),
        SessionRecord::Deliberation(deliberation) => Some(unix_time_ms(deliberation.created_at)),
        SessionRecord::ToolExecutionGraph(graph) => Some(unix_time_ms(graph.created_at)),
        SessionRecord::ToolNodeCheckpoint(checkpoint) => Some(unix_time_ms(checkpoint.occurred_at)),
        SessionRecord::SkillInputValidation(validation) => {
            Some(unix_time_ms(validation.validated_at))
        }
        SessionRecord::ToolCall(call) => Some(unix_time_ms(call.called_at)),
        SessionRecord::ToolResult(result) => Some(unix_time_ms(result.finished_at)),
        SessionRecord::PendingApproval(approval) => Some(unix_time_ms(approval.created_at)),
        SessionRecord::ApprovalResolution(resolution) => Some(unix_time_ms(resolution.resolved_at)),
        SessionRecord::Delegation(delegation) => Some(unix_time_ms(delegation.created_at)),
        SessionRecord::CoordinationClaim(claim) => Some(unix_time_ms(claim.claimed_at)),
        SessionRecord::ProviderUsage(usage) => Some(unix_time_ms(usage.recorded_at)),
        SessionRecord::ProviderCache(cache) => Some(unix_time_ms(cache.cached_at)),
        SessionRecord::Reflection(reflection) => Some(unix_time_ms(reflection.created_at)),
        SessionRecord::PolicyTuning(tuning) => Some(unix_time_ms(tuning.created_at)),
        SessionRecord::StrategyPreference(preference) => Some(unix_time_ms(preference.created_at)),
        SessionRecord::ToolPerformance(perf) => Some(unix_time_ms(perf.created_at)),
        SessionRecord::ProfilePatch(patch) => Some(unix_time_ms(patch.created_at)),
        SessionRecord::ExecutionPlan(plan) => Some(unix_time_ms(plan.created_at)),
        SessionRecord::Summary(summary) => Some(unix_time_ms(summary.created_at)),
        SessionRecord::Outcome(outcome) => Some(unix_time_ms(outcome.finished_at)),
    }
}

fn latest_provider_name(records: &[SessionRecord]) -> Option<String> {
    records.iter().rev().find_map(|record| match record {
        SessionRecord::ProviderUsage(usage) => Some(usage.provider_name.clone()),
        _ => None,
    })
}

fn derive_unread_event_count(records: &[SessionRecord]) -> usize {
    let last_human_at = last_human_input_at_ms(records).unwrap_or_default();
    records
        .iter()
        .filter(|record| {
            record_occurred_at_ms(record).is_some_and(|at| {
                at >= last_human_at
                    && !matches!(
                        record,
                        SessionRecord::Trigger(trigger)
                            if matches!(
                                trigger.trigger,
                                AgentTrigger::HumanInput { .. } | AgentTrigger::Message { .. }
                            )
                    )
            })
        })
        .count()
}

fn pending_wake_view(wake: &rain_engine_core::WakeRequestRecord) -> WakeView {
    WakeView {
        wake_id: wake.wake_id.0.clone(),
        reason: wake.reason.clone(),
        status: "scheduled".to_string(),
        occurred_at_ms: unix_time_ms(wake.requested_at),
        due_at_ms: Some(unix_time_ms(wake.due_at)),
        task_id: wake.task_id.as_ref().map(|task_id| task_id.0.clone()),
    }
}

fn build_wake_history(records: &[SessionRecord]) -> Vec<WakeView> {
    let mut wakes = Vec::new();
    for record in records.iter().rev() {
        match record {
            SessionRecord::KernelEvent(event) => match &event.event {
                rain_engine_core::KernelEvent::WakeRequested(wake)
                | rain_engine_core::KernelEvent::WakeScheduled(wake) => wakes.push(WakeView {
                    wake_id: wake.wake_id.0.clone(),
                    reason: wake.reason.clone(),
                    status: "scheduled".to_string(),
                    occurred_at_ms: unix_time_ms(event.occurred_at),
                    due_at_ms: Some(unix_time_ms(wake.due_at)),
                    task_id: wake.task_id.as_ref().map(|task_id| task_id.0.clone()),
                }),
                rain_engine_core::KernelEvent::WakeCompleted {
                    wake_id, reason, ..
                } => {
                    wakes.push(WakeView {
                        wake_id: wake_id.0.clone(),
                        reason: reason.clone(),
                        status: "completed".to_string(),
                        occurred_at_ms: unix_time_ms(event.occurred_at),
                        due_at_ms: None,
                        task_id: None,
                    });
                }
                _ => {}
            },
            SessionRecord::Trigger(trigger)
                if matches!(trigger.trigger, AgentTrigger::ScheduledWake { .. }) =>
            {
                if let AgentTrigger::ScheduledWake {
                    wake_id,
                    due_at,
                    reason,
                } = &trigger.trigger
                {
                    wakes.push(WakeView {
                        wake_id: wake_id.0.clone(),
                        reason: reason.clone(),
                        status: "fired".to_string(),
                        occurred_at_ms: unix_time_ms(trigger.recorded_at),
                        due_at_ms: Some(unix_time_ms(*due_at)),
                        task_id: None,
                    });
                }
            }
            _ => {}
        }
        if wakes.len() >= 12 {
            break;
        }
    }
    wakes
}

fn build_last_heartbeat(records: &[SessionRecord]) -> Option<HeartbeatStatusView> {
    let mut latest_wake: Option<(String, String, i64)> = None;
    for record in records.iter().rev() {
        if let SessionRecord::Trigger(trigger) = record
            && let AgentTrigger::ScheduledWake {
                wake_id, reason, ..
            } = &trigger.trigger
            && reason.to_ascii_lowercase().contains("heartbeat")
        {
            latest_wake = Some((
                wake_id.0.clone(),
                reason.clone(),
                unix_time_ms(trigger.recorded_at),
            ));
            break;
        }
    }

    let (wake_id, reason, occurred_at_ms) = latest_wake?;
    let outcome = records.iter().rev().find_map(|record| match record {
        SessionRecord::Outcome(outcome) if unix_time_ms(outcome.finished_at) >= occurred_at_ms => {
            Some(outcome)
        }
        _ => None,
    });

    Some(HeartbeatStatusView {
        wake_id,
        reason,
        occurred_at_ms,
        outcome_summary: outcome
            .and_then(|outcome| outcome.response.clone().or_else(|| outcome.detail.clone()))
            .map(|text| truncate_preview(&text)),
        stop_reason: outcome.map(|outcome| outcome.stop_reason.clone()),
    })
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
            SessionRecord::TriggerIntent(intent) => Some(TimelineItem::System {
                label: "intent classified".to_string(),
                detail: intent.intent.clone(),
                occurred_at_ms: unix_time_ms(intent.classified_at),
            }),
            SessionRecord::KernelEvent(event) => match &event.event {
                rain_engine_core::KernelEvent::WakeRequested(wake)
                | rain_engine_core::KernelEvent::WakeScheduled(wake) => {
                    Some(TimelineItem::System {
                        label: "wake scheduled".to_string(),
                        detail: format!("{} · {}", wake.wake_id.0, wake.reason),
                        occurred_at_ms: unix_time_ms(event.occurred_at),
                    })
                }
                rain_engine_core::KernelEvent::WakeCompleted {
                    wake_id, reason, ..
                } => Some(TimelineItem::System {
                    label: "wake completed".to_string(),
                    detail: format!("{}: {}", wake_id.0, reason),
                    occurred_at_ms: unix_time_ms(event.occurred_at),
                }),
                _ => None,
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
                let session_view = build_session_view(snapshot.clone());
                if let Ok(json) = serde_json::to_string(&session_view) {
                    yield Ok(Event::default().event("session_view").data(json));
                }
                if let Ok(json) = serde_json::to_string(&session_view.execution_graph) {
                    yield Ok(Event::default().event("execution_graph").data(json));
                }
                if let Ok(json) = serde_json::to_string(&session_view.self_improvement) {
                    yield Ok(Event::default().event("learning").data(json));
                }
                if let Ok(json) = serde_json::to_string(&session_view.pending_approval) {
                    yield Ok(Event::default().event("approval").data(json));
                }
                if let Ok(json) = serde_json::to_string(&session_view.wake_history) {
                    yield Ok(Event::default().event("wake").data(json));
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
            async_ingress: false,
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
        let config = server_config();
        let settings = RuntimeMutableSettings::defaults(
            config.default_policy.clone(),
            config.default_provider.clone(),
        );
        RuntimeState::new(
            AgentEngine::new(llm, memory.clone()),
            memory,
            blob_store,
            config,
            settings,
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
        assert!(capabilities.wake_support);
        assert!(capabilities.learning_support);
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
        assert_eq!(view.activity_state, SessionActivityState::Idle);
        assert!(view.record_count >= 3);
        assert!(view.current_focus.is_some());
        assert!(
            view.timeline
                .iter()
                .any(|item| matches!(item, TimelineItem::AssistantResponse { .. }))
        );
        assert!(!view.self_improvement.reflections.is_empty());
    }

    #[tokio::test]
    async fn session_list_views_include_presence_metadata() {
        let state = runtime_state_with_mock("processed");
        let router = app(state.clone());

        let response = router
            .oneshot(
                Request::post("/triggers/human/telegram:42")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&HumanInputIngressRequest {
                            session_id: "presence-session".to_string(),
                            content: "check status".to_string(),
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

        let list_response = app(state)
            .oneshot(
                Request::get("/sessions/views")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(list_response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(list_response.into_body(), usize::MAX)
            .await
            .expect("body");
        let views: Vec<SessionListItemView> =
            serde_json::from_slice(&bytes).expect("session list views");
        let view = views
            .iter()
            .find(|session| session.session_id == "presence-session")
            .expect("presence session view");
        assert_eq!(view.status, SessionStatus::Completed);
        assert_eq!(view.activity_state, SessionActivityState::Idle);
        assert!(view.current_focus.is_some());
        assert_eq!(view.active_channel_ids, vec!["telegram".to_string()]);
        assert!(view.record_count >= 3);
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
        let settings = RuntimeMutableSettings::defaults(
            config.default_policy.clone(),
            config.default_provider.clone(),
        );
        let state = RuntimeState::new(
            AgentEngine::new(llm, memory.clone()),
            memory.clone(),
            blob_store,
            config,
            settings,
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
        let config = server_config();
        let settings = RuntimeMutableSettings::defaults(
            config.default_policy.clone(),
            config.default_provider.clone(),
        );
        let state = RuntimeState::new(
            AgentEngine::new(llm, memory.clone()),
            memory,
            blob_store,
            config,
            settings,
        );
        state.engine().register_native_skill(
            SkillManifest {
                name: "dangerous_native".to_string(),
                description: "Requires approval".to_string(),
                input_schema: json!({"type":"object"}),
                required_scopes: vec!["tool:run".to_string()],
                capability_grants: vec![],
                resource_policy: rain_engine_core::ResourcePolicy::default_for_tools(),
                approval_required: true,
                circuit_breaker_threshold: 0.5,
            },
            Arc::new(ApprovalNativeSkill),
        );

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
            cache: None,
            blob: BlobBootstrapConfig::InMemory,
            provider: ProviderBootstrapConfig::Mock {
                response: "processed".to_string(),
            },
            enable_research_planner: false,
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
