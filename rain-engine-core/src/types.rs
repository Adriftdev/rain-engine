use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultimodalPayload {
    pub mime_type: String,
    pub file_name: Option<String>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobDescriptor {
    pub uri: String,
    pub size_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AttachmentContent {
    Inline { data: Vec<u8> },
    Blob { descriptor: BlobDescriptor },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttachmentRef {
    pub attachment_id: String,
    pub mime_type: String,
    pub file_name: Option<String>,
    pub size_bytes: usize,
    pub content: AttachmentContent,
}

impl AttachmentRef {
    pub fn inline(
        attachment_id: impl Into<String>,
        mime_type: impl Into<String>,
        file_name: Option<String>,
        data: Vec<u8>,
    ) -> Self {
        Self {
            attachment_id: attachment_id.into(),
            mime_type: mime_type.into(),
            file_name,
            size_bytes: data.len(),
            content: AttachmentContent::Inline { data },
        }
    }

    pub fn blob(
        attachment_id: impl Into<String>,
        mime_type: impl Into<String>,
        file_name: Option<String>,
        descriptor: BlobDescriptor,
    ) -> Self {
        Self {
            attachment_id: attachment_id.into(),
            mime_type: mime_type.into(),
            file_name,
            size_bytes: descriptor.size_bytes,
            content: AttachmentContent::Blob { descriptor },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ApprovalDecision {
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResumeToken(pub String);

impl ResumeToken {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorrelationId(pub String);

impl CorrelationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationTarget {
    pub stream: String,
    pub worker: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DelegationTask {
    pub task_type: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AgentTrigger {
    Webhook {
        source: String,
        payload: Value,
        attachments: Vec<AttachmentRef>,
    },
    RuleTrigger {
        rule_id: String,
        context: Value,
        attachments: Vec<AttachmentRef>,
    },
    ProactiveHeartbeat {
        timestamp: u64,
        attachments: Vec<AttachmentRef>,
    },
    Message {
        user_id: String,
        content: String,
        attachments: Vec<AttachmentRef>,
    },
    Approval {
        resume_token: ResumeToken,
        decision: ApprovalDecision,
        metadata: Value,
    },
    DelegationResult {
        correlation_id: CorrelationId,
        payload: Value,
        metadata: Value,
    },
}

impl AgentTrigger {
    pub fn attachments(&self) -> &[AttachmentRef] {
        match self {
            AgentTrigger::Webhook { attachments, .. }
            | AgentTrigger::RuleTrigger { attachments, .. }
            | AgentTrigger::ProactiveHeartbeat { attachments, .. }
            | AgentTrigger::Message { attachments, .. } => attachments,
            AgentTrigger::Approval { .. } | AgentTrigger::DelegationResult { .. } => &[],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriggerRecord {
    pub trigger_id: String,
    pub session_id: String,
    pub idempotency_key: Option<String>,
    pub recorded_at: SystemTime,
    pub trigger: AgentTrigger,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ContinueReason {
    ToolResultAppended,
    ModelRequested,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlannedSkillCall {
    pub call_id: String,
    pub name: String,
    pub args: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SuspendReason {
    HumanApprovalRequired { skill_names: Vec<String> },
    ProviderRequested { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AgentAction {
    Respond {
        content: String,
    },
    CallSkills(Vec<PlannedSkillCall>),
    Continue {
        reason: ContinueReason,
    },
    Yield {
        reason: Option<String>,
    },
    Suspend {
        reason: SuspendReason,
        pending_calls: Vec<PlannedSkillCall>,
        resume_token: ResumeToken,
    },
    Delegate {
        target: DelegationTarget,
        task: DelegationTask,
        correlation_id: CorrelationId,
        resume_token: ResumeToken,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelDecisionRecord {
    pub step: usize,
    pub decided_at: SystemTime,
    pub action: AgentAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourcePolicy {
    pub timeout_ms: u64,
    pub max_memory_bytes: usize,
    pub max_fuel: Option<u64>,
}

impl ResourcePolicy {
    pub fn default_for_tools() -> Self {
        Self {
            timeout_ms: 5_000,
            max_memory_bytes: 8 * 1024 * 1024,
            max_fuel: Some(10_000_000),
        }
    }

    pub fn validated(&self) -> Self {
        Self {
            timeout_ms: self.timeout_ms.max(1),
            max_memory_bytes: self.max_memory_bytes.max(64 * 1024),
            max_fuel: self.max_fuel,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SkillCapability {
    KeyValueRead { namespaces: Vec<String> },
    HttpOutbound { allow_hosts: Vec<String> },
    StructuredLog,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillBackendKind {
    Wasm,
    Native,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillManifest {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub required_scopes: Vec<String>,
    pub capability_grants: Vec<SkillCapability>,
    pub resource_policy: ResourcePolicy,
    pub approval_required: bool,
}

impl SkillManifest {
    pub fn effective_resource_policy(&self, engine_policy: &EnginePolicy) -> ResourcePolicy {
        let mut policy = self.resource_policy.validated();
        policy.timeout_ms = policy
            .timeout_ms
            .min(engine_policy.max_tool_timeout_ms.max(1));
        policy
    }
}

pub trait SkillManifestDescriptor {
    fn skill_manifest() -> SkillManifest;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillDefinition {
    pub manifest: SkillManifest,
    pub executor_kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCallRecord {
    pub call_id: String,
    pub step: usize,
    pub called_at: SystemTime,
    pub skill_name: String,
    pub args: Value,
    pub backend_kind: SkillBackendKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SkillFailureKind {
    PermissionDenied,
    CapabilityDenied,
    Timeout,
    MemoryLimitExceeded,
    Trap,
    InvalidResponse,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillFailure {
    pub kind: SkillFailureKind,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolResultRecord {
    pub call_id: String,
    pub finished_at: SystemTime,
    pub skill_name: String,
    pub output: Result<Value, SkillFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PendingApprovalRecord {
    pub resume_token: ResumeToken,
    pub created_at: SystemTime,
    pub trigger_id: String,
    pub step: usize,
    pub reason: SuspendReason,
    pub pending_calls: Vec<PlannedSkillCall>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApprovalResolutionRecord {
    pub resume_token: ResumeToken,
    pub resolved_at: SystemTime,
    pub decision: ApprovalDecision,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DelegationRecord {
    pub correlation_id: CorrelationId,
    pub created_at: SystemTime,
    pub trigger_id: String,
    pub target: DelegationTarget,
    pub task: DelegationTask,
    pub resume_token: ResumeToken,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoordinationClaimRecord {
    pub claim_id: String,
    pub trigger_key: String,
    pub claimed_at: SystemTime,
    pub expires_at: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderUsageRecord {
    pub provider_name: String,
    pub recorded_at: SystemTime,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub estimated_cost_usd: f64,
    pub cached_content_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderCacheRecord {
    pub provider_name: String,
    pub cached_content_id: String,
    pub token_count: usize,
    pub cached_at: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StopReason {
    Responded,
    Yielded,
    MaxStepsReached,
    DeadlineExceeded,
    Cancelled,
    ProviderFailure,
    StorageFailure,
    PolicyAborted,
    Suspended,
    Delegated,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OutcomeRecord {
    pub trigger_id: String,
    pub idempotency_key: Option<String>,
    pub finished_at: SystemTime,
    pub stop_reason: StopReason,
    pub response: Option<String>,
    pub detail: Option<String>,
    pub steps_executed: usize,
    pub resume_token: Option<ResumeToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SessionRecord {
    Trigger(TriggerRecord),
    ModelDecision(ModelDecisionRecord),
    ToolCall(ToolCallRecord),
    ToolResult(ToolResultRecord),
    PendingApproval(PendingApprovalRecord),
    ApprovalResolution(ApprovalResolutionRecord),
    Delegation(DelegationRecord),
    CoordinationClaim(CoordinationClaimRecord),
    ProviderUsage(ProviderUsageRecord),
    ProviderCache(ProviderCacheRecord),
    Outcome(OutcomeRecord),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionRecordKind {
    Trigger,
    ModelDecision,
    ToolCall,
    ToolResult,
    PendingApproval,
    ApprovalResolution,
    Delegation,
    CoordinationClaim,
    ProviderUsage,
    ProviderCache,
    Outcome,
}

impl SessionRecordKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionRecordKind::Trigger => "trigger",
            SessionRecordKind::ModelDecision => "model_decision",
            SessionRecordKind::ToolCall => "tool_call",
            SessionRecordKind::ToolResult => "tool_result",
            SessionRecordKind::PendingApproval => "pending_approval",
            SessionRecordKind::ApprovalResolution => "approval_resolution",
            SessionRecordKind::Delegation => "delegation",
            SessionRecordKind::CoordinationClaim => "coordination_claim",
            SessionRecordKind::ProviderUsage => "provider_usage",
            SessionRecordKind::ProviderCache => "provider_cache",
            SessionRecordKind::Outcome => "outcome",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "trigger" => Some(SessionRecordKind::Trigger),
            "model_decision" => Some(SessionRecordKind::ModelDecision),
            "tool_call" => Some(SessionRecordKind::ToolCall),
            "tool_result" => Some(SessionRecordKind::ToolResult),
            "pending_approval" => Some(SessionRecordKind::PendingApproval),
            "approval_resolution" => Some(SessionRecordKind::ApprovalResolution),
            "delegation" => Some(SessionRecordKind::Delegation),
            "coordination_claim" => Some(SessionRecordKind::CoordinationClaim),
            "provider_usage" => Some(SessionRecordKind::ProviderUsage),
            "provider_cache" => Some(SessionRecordKind::ProviderCache),
            "outcome" => Some(SessionRecordKind::Outcome),
            _ => None,
        }
    }
}

impl SessionRecord {
    pub fn kind(&self) -> SessionRecordKind {
        match self {
            SessionRecord::Trigger(_) => SessionRecordKind::Trigger,
            SessionRecord::ModelDecision(_) => SessionRecordKind::ModelDecision,
            SessionRecord::ToolCall(_) => SessionRecordKind::ToolCall,
            SessionRecord::ToolResult(_) => SessionRecordKind::ToolResult,
            SessionRecord::PendingApproval(_) => SessionRecordKind::PendingApproval,
            SessionRecord::ApprovalResolution(_) => SessionRecordKind::ApprovalResolution,
            SessionRecord::Delegation(_) => SessionRecordKind::Delegation,
            SessionRecord::CoordinationClaim(_) => SessionRecordKind::CoordinationClaim,
            SessionRecord::ProviderUsage(_) => SessionRecordKind::ProviderUsage,
            SessionRecord::ProviderCache(_) => SessionRecordKind::ProviderCache,
            SessionRecord::Outcome(_) => SessionRecordKind::Outcome,
        }
    }

    pub fn occurred_at(&self) -> SystemTime {
        match self {
            SessionRecord::Trigger(record) => record.recorded_at,
            SessionRecord::ModelDecision(record) => record.decided_at,
            SessionRecord::ToolCall(record) => record.called_at,
            SessionRecord::ToolResult(record) => record.finished_at,
            SessionRecord::PendingApproval(record) => record.created_at,
            SessionRecord::ApprovalResolution(record) => record.resolved_at,
            SessionRecord::Delegation(record) => record.created_at,
            SessionRecord::CoordinationClaim(record) => record.claimed_at,
            SessionRecord::ProviderUsage(record) => record.recorded_at,
            SessionRecord::ProviderCache(record) => record.cached_at,
            SessionRecord::Outcome(record) => record.finished_at,
        }
    }

    pub fn trigger_id(&self) -> Option<&str> {
        match self {
            SessionRecord::Trigger(record) => Some(&record.trigger_id),
            SessionRecord::PendingApproval(record) => Some(&record.trigger_id),
            SessionRecord::Delegation(record) => Some(&record.trigger_id),
            SessionRecord::Outcome(record) => Some(&record.trigger_id),
            _ => None,
        }
    }

    pub fn idempotency_key(&self) -> Option<&str> {
        match self {
            SessionRecord::Trigger(record) => record.idempotency_key.as_deref(),
            SessionRecord::Outcome(record) => record.idempotency_key.as_deref(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredSessionRecord {
    pub session_id: String,
    pub sequence_no: i64,
    pub occurred_at_ms: i64,
    pub record_kind: SessionRecordKind,
    pub trigger_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub record: SessionRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewSessionRecord {
    pub session_id: String,
    pub occurred_at_ms: i64,
    pub record_kind: SessionRecordKind,
    pub trigger_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub record: SessionRecord,
}

impl NewSessionRecord {
    pub fn from_record(session_id: impl Into<String>, record: SessionRecord) -> Self {
        let occurred_at_ms = unix_time_ms(record.occurred_at());
        let trigger_id = record.trigger_id().map(str::to_string);
        let idempotency_key = record.idempotency_key().map(str::to_string);
        Self {
            session_id: session_id.into(),
            occurred_at_ms,
            record_kind: record.kind(),
            trigger_id,
            idempotency_key,
            record,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionSnapshot {
    pub session_id: String,
    pub records: Vec<SessionRecord>,
    pub last_sequence_no: Option<i64>,
    pub latest_outcome: Option<OutcomeRecord>,
}

impl SessionSnapshot {
    pub fn tool_results(&self) -> Vec<ToolResultRecord> {
        self.records
            .iter()
            .filter_map(|record| match record {
                SessionRecord::ToolResult(result) => Some(result.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn total_estimated_cost_usd(&self) -> f64 {
        self.records
            .iter()
            .filter_map(|record| match record {
                SessionRecord::ProviderUsage(usage) => Some(usage.estimated_cost_usd),
                _ => None,
            })
            .sum()
    }

    pub fn latest_cache_record(&self, provider_name: &str) -> Option<ProviderCacheRecord> {
        self.records.iter().rev().find_map(|record| match record {
            SessionRecord::ProviderCache(cache) if cache.provider_name == provider_name => {
                Some(cache.clone())
            }
            _ => None,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionSummary {
    pub session_id: String,
    pub first_recorded_at_ms: i64,
    pub last_recorded_at_ms: i64,
    pub record_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionListQuery {
    pub offset: usize,
    pub limit: usize,
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
}

impl Default for SessionListQuery {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 50,
            since_ms: None,
            until_ms: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordPageQuery {
    pub session_id: String,
    pub offset: usize,
    pub limit: usize,
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
}

impl RecordPageQuery {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            offset: 0,
            limit: 100,
            since_ms: None,
            until_ms: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecordPage {
    pub session_id: String,
    pub records: Vec<StoredSessionRecord>,
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EnginePolicy {
    pub max_steps: usize,
    pub max_execution_time_ms: u64,
    pub provider_timeout_ms: u64,
    pub max_tool_timeout_ms: u64,
    pub max_consecutive_tool_failures: usize,
    pub cache_threshold_tokens: usize,
    pub max_cost_per_session: f64,
    pub max_parallel_skill_calls: usize,
    pub max_inline_attachment_bytes: usize,
    pub allow_native_skills: bool,
}

impl Default for EnginePolicy {
    fn default() -> Self {
        Self {
            max_steps: 16,
            max_execution_time_ms: 30_000,
            provider_timeout_ms: 15_000,
            max_tool_timeout_ms: 5_000,
            max_consecutive_tool_failures: 3,
            cache_threshold_tokens: 32_000,
            max_cost_per_session: 5.0,
            max_parallel_skill_calls: 4,
            max_inline_attachment_bytes: 512 * 1024,
            allow_native_skills: true,
        }
    }
}

impl EnginePolicy {
    pub fn max_execution_time(&self) -> Duration {
        Duration::from_millis(self.max_execution_time_ms.max(1))
    }

    pub fn provider_timeout(&self) -> Duration {
        Duration::from_millis(self.provider_timeout_ms.max(1))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ProviderRequestConfig {
    pub model: Option<String>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProviderRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProviderContentPart {
    Text(String),
    Json(Value),
    InlineData(MultimodalPayload),
    Attachment(AttachmentRef),
    ToolResult(ToolResultRecord),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderMessage {
    pub role: ProviderRole,
    pub parts: Vec<ProviderContentPart>,
}

#[derive(Debug, Clone)]
pub struct ExecutionMetadata {
    pub trigger_id: String,
    pub idempotency_key: Option<String>,
    pub started_at: SystemTime,
    pub deadline: Instant,
    pub policy: EnginePolicy,
    pub provider: ProviderRequestConfig,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct AgentContext {
    pub session_id: String,
    pub records: Vec<SessionRecord>,
    pub prior_tool_results: Vec<ToolResultRecord>,
    pub granted_scopes: BTreeSet<String>,
    pub metadata: ExecutionMetadata,
}

impl AgentContext {
    pub fn to_snapshot(&self, current_step: usize) -> AgentContextSnapshot {
        AgentContextSnapshot {
            session_id: self.session_id.clone(),
            granted_scopes: self.granted_scopes.iter().cloned().collect(),
            trigger_id: self.metadata.trigger_id.clone(),
            idempotency_key: self.metadata.idempotency_key.clone(),
            current_step,
            max_steps: self.metadata.policy.max_steps,
            history: self.records.clone(),
            prior_tool_results: self.prior_tool_results.clone(),
            session_cost_usd: self
                .records
                .iter()
                .filter_map(|record| match record {
                    SessionRecord::ProviderUsage(usage) => Some(usage.estimated_cost_usd),
                    _ => None,
                })
                .sum(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentContextSnapshot {
    pub session_id: String,
    pub granted_scopes: Vec<String>,
    pub trigger_id: String,
    pub idempotency_key: Option<String>,
    pub current_step: usize,
    pub max_steps: usize,
    pub history: Vec<SessionRecord>,
    pub prior_tool_results: Vec<ToolResultRecord>,
    pub session_cost_usd: f64,
}

#[derive(Debug, Clone)]
pub struct ProcessRequest {
    pub session_id: String,
    pub trigger: AgentTrigger,
    pub granted_scopes: BTreeSet<String>,
    pub idempotency_key: Option<String>,
    pub policy: EnginePolicy,
    pub provider: ProviderRequestConfig,
    pub cancellation: CancellationToken,
}

impl ProcessRequest {
    pub fn new(session_id: impl Into<String>, trigger: AgentTrigger) -> Self {
        Self {
            session_id: session_id.into(),
            trigger,
            granted_scopes: BTreeSet::new(),
            idempotency_key: None,
            policy: EnginePolicy::default(),
            provider: ProviderRequestConfig::default(),
            cancellation: CancellationToken::new(),
        }
    }

    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.granted_scopes.insert(scope.into());
        self
    }

    pub fn with_policy(mut self, policy: EnginePolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn with_idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.idempotency_key = Some(idempotency_key.into());
        self
    }

    pub fn with_provider_model(mut self, model: impl Into<String>) -> Self {
        self.provider.model = Some(model.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderRequest {
    pub trigger: AgentTrigger,
    pub context: AgentContextSnapshot,
    pub available_skills: Vec<SkillDefinition>,
    pub config: ProviderRequestConfig,
    pub policy: EnginePolicy,
    pub contents: Vec<ProviderMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderDecision {
    pub action: AgentAction,
    pub usage: Option<ProviderUsageRecord>,
    pub cache: Option<ProviderCacheRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillInvocation {
    pub call_id: String,
    pub manifest: SkillManifest,
    pub args: Value,
    pub context: AgentContextSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EngineOutcome {
    pub trigger_id: String,
    pub stop_reason: StopReason,
    pub response: Option<String>,
    pub detail: Option<String>,
    pub steps_executed: usize,
    pub idempotent_replay: bool,
    pub resume_token: Option<ResumeToken>,
}

impl EngineOutcome {
    pub fn from_record(record: OutcomeRecord) -> Self {
        Self {
            trigger_id: record.trigger_id,
            stop_reason: record.stop_reason,
            response: record.response,
            detail: record.detail,
            steps_executed: record.steps_executed,
            idempotent_replay: false,
            resume_token: record.resume_token,
        }
    }
}

pub fn unix_time_ms(value: SystemTime) -> i64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
