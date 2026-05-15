use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultimodalPayload {
    pub mime_type: String,
    pub file_name: Option<String>,
    pub data: Vec<u8>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobDescriptor {
    pub uri: String,
    pub size_bytes: usize,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "payload")]
pub enum AttachmentContent {
    Inline { data: Vec<u8> },
    Blob { descriptor: BlobDescriptor },
}

#[typeshare::typeshare]
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

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ApprovalDecision {
    Approved,
    Rejected,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResumeToken(pub String);

impl ResumeToken {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorrelationId(pub String);

impl CorrelationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentId(pub String);

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GoalId(pub String);

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(pub String);

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArtifactId(pub String);

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObservationId(pub String);

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WakeId(pub String);

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceRef {
    pub resource_id: String,
    pub resource_type: String,
    pub label: String,
    pub external_ref: Option<String>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelationshipEdge {
    pub from_resource_id: String,
    pub to_resource_id: String,
    pub relation: String,
    pub observed_at: SystemTime,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum GoalStatus {
    Active,
    Blocked,
    Completed,
    Cancelled,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Ready,
    Running,
    Blocked,
    WaitingHuman,
    Done,
    Failed,
    Abandoned,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryPolicy {
    pub max_recent_items: usize,
    pub semantic_retrieval_limit: usize,
    pub graph_hops: usize,
}

impl Default for MemoryPolicy {
    fn default() -> Self {
        Self {
            max_recent_items: 32,
            semantic_retrieval_limit: 8,
            graph_hops: 6,
        }
    }
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EscalationPolicy {
    pub require_human_approval_scopes: Vec<String>,
    pub max_auto_delegations: usize,
}

impl Default for EscalationPolicy {
    fn default() -> Self {
        Self {
            require_human_approval_scopes: vec!["scope:human_approval".to_string()],
            max_auto_delegations: 50,
        }
    }
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WakePolicy {
    pub allow_scheduled_wakes: bool,
    pub default_recheck_ms: u64,
    pub max_pending_wakes: usize,
}

impl Default for WakePolicy {
    fn default() -> Self {
        Self {
            allow_scheduled_wakes: true,
            default_recheck_ms: 30 * 60 * 1000,
            max_pending_wakes: 8,
        }
    }
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReviewPolicy {
    pub require_review_for_native_skills: bool,
    pub require_review_for_delegation: bool,
    pub reviewer_scopes: Vec<String>,
}

impl Default for ReviewPolicy {
    fn default() -> Self {
        Self {
            require_review_for_native_skills: false,
            require_review_for_delegation: true,
            reviewer_scopes: vec!["scope:human_approval".to_string()],
        }
    }
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentProfile {
    pub agent_id: AgentId,
    pub role: String,
    pub default_scopes: Vec<String>,
    pub allowed_skill_names: Vec<String>,
    pub goal_ids: Vec<GoalId>,
    pub memory_policy: MemoryPolicy,
    pub wake_policy: WakePolicy,
    pub review_policy: ReviewPolicy,
    pub escalation_policy: EscalationPolicy,
    pub config_artifact: Option<ArtifactId>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GoalRecord {
    pub goal_id: GoalId,
    pub created_at: SystemTime,
    pub title: String,
    pub detail: Option<String>,
    pub status: GoalStatus,
    pub parent_goal_id: Option<GoalId>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskRecord {
    pub task_id: TaskId,
    pub goal_id: Option<GoalId>,
    pub parent_task_id: Option<TaskId>,
    pub created_at: SystemTime,
    pub title: String,
    pub detail: Option<String>,
    pub status: TaskStatus,
    pub assignee: Option<String>,
    pub blocked_by: Vec<TaskId>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObservationRecord {
    pub observation_id: ObservationId,
    pub recorded_at: SystemTime,
    pub source: String,
    pub content: Value,
    pub attachment_ids: Vec<String>,
    pub related_resources: Vec<ResourceRef>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArtifactRecord {
    pub artifact_id: ArtifactId,
    pub created_at: SystemTime,
    pub name: String,
    pub mime_type: String,
    pub descriptor: Option<BlobDescriptor>,
    pub metadata: Value,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WakeRequestRecord {
    pub wake_id: WakeId,
    pub requested_at: SystemTime,
    pub due_at: SystemTime,
    pub reason: String,
    pub task_id: Option<TaskId>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentStateSnapshot {
    pub agent_id: AgentId,
    pub profile: Option<AgentProfile>,
    pub goals: Vec<GoalRecord>,
    pub tasks: Vec<TaskRecord>,
    pub observations: Vec<ObservationRecord>,
    pub artifacts: Vec<ArtifactRecord>,
    pub resources: Vec<ResourceRef>,
    pub relationships: Vec<RelationshipEdge>,
    pub pending_wake: Option<WakeRequestRecord>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct AgentStateDelta {
    pub created_goal_ids: Vec<GoalId>,
    pub updated_task_ids: Vec<TaskId>,
    pub observation_ids: Vec<ObservationId>,
    pub artifact_ids: Vec<ArtifactId>,
    pub delegation_correlation_ids: Vec<CorrelationId>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "payload")]
pub enum KernelEvent {
    GoalCreated(GoalRecord),
    TaskPlanned(TaskRecord),
    TaskClaimed {
        task_id: TaskId,
        claimed_at: SystemTime,
        assignee: Option<String>,
    },
    TaskBlocked {
        task_id: TaskId,
        blocked_at: SystemTime,
        reason: String,
    },
    TaskCompleted {
        task_id: TaskId,
        completed_at: SystemTime,
        artifact_ids: Vec<ArtifactId>,
    },
    TaskFailed {
        task_id: TaskId,
        failed_at: SystemTime,
        reason: String,
    },
    TaskAbandoned {
        task_id: TaskId,
        abandoned_at: SystemTime,
        reason: String,
    },
    HumanInputRequested {
        task_id: Option<TaskId>,
        requested_at: SystemTime,
        prompt: String,
        resume_token: ResumeToken,
    },
    ObservationAppended(ObservationRecord),
    ArtifactProduced(ArtifactRecord),
    WakeRequested(WakeRequestRecord),
    WakeScheduled(WakeRequestRecord),
    DelegationRequested(DelegationRecord),
    DelegationResolved {
        correlation_id: CorrelationId,
        resolved_at: SystemTime,
        payload: Value,
        metadata: Value,
    },
    ResourceRegistered(ResourceRef),
    RelationshipObserved(RelationshipEdge),
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KernelEventRecord {
    pub event_id: String,
    pub occurred_at: SystemTime,
    pub event: KernelEvent,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationTarget {
    pub stream: String,
    pub worker: Option<String>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DelegationTask {
    pub task_type: String,
    pub payload: Value,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "payload")]
pub enum AgentTrigger {
    ExternalEvent {
        source: String,
        payload: Value,
        attachments: Vec<AttachmentRef>,
    },
    ScheduledWake {
        wake_id: WakeId,
        due_at: SystemTime,
        reason: String,
    },
    HumanInput {
        actor_id: String,
        content: String,
        attachments: Vec<AttachmentRef>,
    },
    SystemObservation {
        source: String,
        observation: Value,
        attachments: Vec<AttachmentRef>,
    },
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
            AgentTrigger::ExternalEvent { attachments, .. }
            | AgentTrigger::HumanInput { attachments, .. }
            | AgentTrigger::SystemObservation { attachments, .. }
            | AgentTrigger::Webhook { attachments, .. }
            | AgentTrigger::RuleTrigger { attachments, .. }
            | AgentTrigger::ProactiveHeartbeat { attachments, .. }
            | AgentTrigger::Message { attachments, .. } => attachments,
            AgentTrigger::ScheduledWake { .. }
            | AgentTrigger::Approval { .. }
            | AgentTrigger::DelegationResult { .. } => &[],
        }
    }
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriggerRecord {
    pub trigger_id: String,
    pub session_id: String,
    pub idempotency_key: Option<String>,
    pub recorded_at: SystemTime,
    pub trigger: AgentTrigger,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ContinueReason {
    ToolResultAppended,
    ModelRequested,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlannedSkillCall {
    pub call_id: String,
    pub name: String,
    pub args: Value,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "payload")]
pub enum SuspendReason {
    HumanApprovalRequired { skill_names: Vec<String> },
    ProviderRequested { message: String },
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "payload")]
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

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelDecisionRecord {
    pub step: usize,
    pub decided_at: SystemTime,
    pub action: AgentAction,
}

#[typeshare::typeshare]
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

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "payload")]
pub enum SkillCapability {
    KeyValueRead { namespaces: Vec<String> },
    HttpOutbound { allow_hosts: Vec<String> },
    StructuredLog,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillBackendKind {
    Wasm,
    Native,
}

#[typeshare::typeshare]
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

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillDefinition {
    pub manifest: SkillManifest,
    pub executor_kind: String,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCallRecord {
    pub call_id: String,
    pub step: usize,
    pub called_at: SystemTime,
    pub skill_name: String,
    pub args: Value,
    pub backend_kind: SkillBackendKind,
}

#[typeshare::typeshare]
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

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillFailure {
    pub kind: SkillFailureKind,
    pub message: String,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolResultRecord {
    pub call_id: String,
    pub finished_at: SystemTime,
    pub skill_name: String,
    pub output: Result<Value, SkillFailure>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PendingApprovalRecord {
    pub resume_token: ResumeToken,
    pub created_at: SystemTime,
    pub trigger_id: String,
    pub step: usize,
    pub reason: SuspendReason,
    pub pending_calls: Vec<PlannedSkillCall>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApprovalResolutionRecord {
    pub resume_token: ResumeToken,
    pub resolved_at: SystemTime,
    pub decision: ApprovalDecision,
    pub metadata: Value,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DelegationRecord {
    pub correlation_id: CorrelationId,
    pub created_at: SystemTime,
    pub trigger_id: String,
    pub target: DelegationTarget,
    pub task: DelegationTask,
    pub resume_token: ResumeToken,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoordinationClaimRecord {
    pub claim_id: String,
    pub trigger_key: String,
    pub claimed_at: SystemTime,
    pub expires_at: SystemTime,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderUsageRecord {
    pub provider_name: String,
    pub recorded_at: SystemTime,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub estimated_cost_usd: f64,
    pub cached_content_id: Option<String>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderCacheRecord {
    pub provider_name: String,
    pub cached_content_id: String,
    pub token_count: usize,
    pub cached_at: SystemTime,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SelfImprovementMode {
    Advisory,
    AutoWithGuardrails,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SelfImprovementPolicy {
    pub enabled: bool,
    pub mode: SelfImprovementMode,
    pub reflection_interval_records: usize,
    pub min_observations_before_tuning: usize,
    pub max_policy_delta_percent: f64,
    pub require_approval_for_scope_expansion: bool,
    pub rollback_on_regression: bool,
}

impl Default for SelfImprovementPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: SelfImprovementMode::AutoWithGuardrails,
            reflection_interval_records: 8,
            min_observations_before_tuning: 2,
            max_policy_delta_percent: 25.0,
            require_approval_for_scope_expansion: true,
            rollback_on_regression: true,
        }
    }
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct PolicyOverlayPatch {
    pub max_steps: Option<usize>,
    pub max_execution_time_ms: Option<u64>,
    pub provider_timeout_ms: Option<u64>,
    pub max_tool_timeout_ms: Option<u64>,
    pub max_parallel_skill_calls: Option<usize>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PolicyOverlayStatus {
    Proposed,
    Applied,
    RolledBack,
    Rejected,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PolicyOverlay {
    pub overlay_id: String,
    pub created_at: SystemTime,
    pub status: PolicyOverlayStatus,
    pub reason: String,
    pub evidence_window_records: usize,
    pub patch: PolicyOverlayPatch,
    pub confidence: f64,
    pub rollback_condition: String,
}

impl PolicyOverlay {
    pub fn apply_to(&self, policy: &mut EnginePolicy) {
        if self.status != PolicyOverlayStatus::Applied {
            return;
        }
        if let Some(value) = self.patch.max_steps {
            policy.max_steps = value.max(1);
        }
        if let Some(value) = self.patch.max_execution_time_ms {
            policy.max_execution_time_ms = value.max(1);
        }
        if let Some(value) = self.patch.provider_timeout_ms {
            policy.provider_timeout_ms = value.max(1);
        }
        if let Some(value) = self.patch.max_tool_timeout_ms {
            policy.max_tool_timeout_ms = value.max(1);
        }
        if let Some(value) = self.patch.max_parallel_skill_calls {
            policy.max_parallel_skill_calls = value.max(1);
        }
    }
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PolicyTuningAction {
    Proposed,
    Applied,
    RolledBack,
    RejectedUnsafe,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReflectionRecord {
    pub reflection_id: String,
    pub created_at: SystemTime,
    pub trigger_id: String,
    pub summary: String,
    pub observations: Vec<String>,
    pub confidence: f64,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PolicyTuningRecord {
    pub tuning_id: String,
    pub created_at: SystemTime,
    pub overlay: PolicyOverlay,
    pub action: PolicyTuningAction,
    pub prior_policy: EnginePolicy,
    pub projected_policy: EnginePolicy,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyPreferenceRecord {
    pub preference_id: String,
    pub created_at: SystemTime,
    pub skill_name: Option<String>,
    pub preference: String,
    pub reason: String,
    pub confidence: f64,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolPerformanceRecord {
    pub performance_id: String,
    pub created_at: SystemTime,
    pub skill_name: String,
    pub backend_kind: String,
    pub calls: usize,
    pub successes: usize,
    pub failures: usize,
    pub failure_rate: f64,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProfilePatchRecord {
    pub patch_id: String,
    pub created_at: SystemTime,
    pub description: String,
    pub patch: Value,
    pub requires_approval: bool,
    pub applied: bool,
}

#[derive(Debug, Clone)]
pub struct SelfImprovementInput {
    pub session_id: String,
    pub records: Vec<SessionRecord>,
    pub latest_outcome: OutcomeRecord,
    pub current_policy: EnginePolicy,
    pub active_overlay: Option<PolicyOverlay>,
}

#[derive(Debug, Clone, Default)]
pub struct SelfImprovementAdvice {
    pub reflections: Vec<ReflectionRecord>,
    pub policy_tunings: Vec<PolicyTuningRecord>,
    pub strategy_preferences: Vec<StrategyPreferenceRecord>,
    pub tool_performance: Vec<ToolPerformanceRecord>,
    pub profile_patches: Vec<ProfilePatchRecord>,
}

#[typeshare::typeshare]
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

#[typeshare::typeshare]
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

#[typeshare::typeshare]
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "payload")]
pub enum SessionRecord {
    Trigger(TriggerRecord),
    KernelEvent(KernelEventRecord),
    ModelDecision(ModelDecisionRecord),
    ToolCall(ToolCallRecord),
    ToolResult(ToolResultRecord),
    PendingApproval(PendingApprovalRecord),
    ApprovalResolution(ApprovalResolutionRecord),
    Delegation(DelegationRecord),
    CoordinationClaim(CoordinationClaimRecord),
    ProviderUsage(ProviderUsageRecord),
    ProviderCache(ProviderCacheRecord),
    Reflection(ReflectionRecord),
    PolicyTuning(PolicyTuningRecord),
    StrategyPreference(StrategyPreferenceRecord),
    ToolPerformance(ToolPerformanceRecord),
    ProfilePatch(ProfilePatchRecord),
    Outcome(OutcomeRecord),
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionRecordKind {
    Trigger,
    KernelEvent,
    ModelDecision,
    ToolCall,
    ToolResult,
    PendingApproval,
    ApprovalResolution,
    Delegation,
    CoordinationClaim,
    ProviderUsage,
    ProviderCache,
    Reflection,
    PolicyTuning,
    StrategyPreference,
    ToolPerformance,
    ProfilePatch,
    Outcome,
}

impl SessionRecordKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionRecordKind::Trigger => "trigger",
            SessionRecordKind::KernelEvent => "kernel_event",
            SessionRecordKind::ModelDecision => "model_decision",
            SessionRecordKind::ToolCall => "tool_call",
            SessionRecordKind::ToolResult => "tool_result",
            SessionRecordKind::PendingApproval => "pending_approval",
            SessionRecordKind::ApprovalResolution => "approval_resolution",
            SessionRecordKind::Delegation => "delegation",
            SessionRecordKind::CoordinationClaim => "coordination_claim",
            SessionRecordKind::ProviderUsage => "provider_usage",
            SessionRecordKind::ProviderCache => "provider_cache",
            SessionRecordKind::Reflection => "reflection",
            SessionRecordKind::PolicyTuning => "policy_tuning",
            SessionRecordKind::StrategyPreference => "strategy_preference",
            SessionRecordKind::ToolPerformance => "tool_performance",
            SessionRecordKind::ProfilePatch => "profile_patch",
            SessionRecordKind::Outcome => "outcome",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "trigger" => Some(SessionRecordKind::Trigger),
            "kernel_event" => Some(SessionRecordKind::KernelEvent),
            "model_decision" => Some(SessionRecordKind::ModelDecision),
            "tool_call" => Some(SessionRecordKind::ToolCall),
            "tool_result" => Some(SessionRecordKind::ToolResult),
            "pending_approval" => Some(SessionRecordKind::PendingApproval),
            "approval_resolution" => Some(SessionRecordKind::ApprovalResolution),
            "delegation" => Some(SessionRecordKind::Delegation),
            "coordination_claim" => Some(SessionRecordKind::CoordinationClaim),
            "provider_usage" => Some(SessionRecordKind::ProviderUsage),
            "provider_cache" => Some(SessionRecordKind::ProviderCache),
            "reflection" => Some(SessionRecordKind::Reflection),
            "policy_tuning" => Some(SessionRecordKind::PolicyTuning),
            "strategy_preference" => Some(SessionRecordKind::StrategyPreference),
            "tool_performance" => Some(SessionRecordKind::ToolPerformance),
            "profile_patch" => Some(SessionRecordKind::ProfilePatch),
            "outcome" => Some(SessionRecordKind::Outcome),
            _ => None,
        }
    }
}

impl SessionRecord {
    pub fn kind(&self) -> SessionRecordKind {
        match self {
            SessionRecord::Trigger(_) => SessionRecordKind::Trigger,
            SessionRecord::KernelEvent(_) => SessionRecordKind::KernelEvent,
            SessionRecord::ModelDecision(_) => SessionRecordKind::ModelDecision,
            SessionRecord::ToolCall(_) => SessionRecordKind::ToolCall,
            SessionRecord::ToolResult(_) => SessionRecordKind::ToolResult,
            SessionRecord::PendingApproval(_) => SessionRecordKind::PendingApproval,
            SessionRecord::ApprovalResolution(_) => SessionRecordKind::ApprovalResolution,
            SessionRecord::Delegation(_) => SessionRecordKind::Delegation,
            SessionRecord::CoordinationClaim(_) => SessionRecordKind::CoordinationClaim,
            SessionRecord::ProviderUsage(_) => SessionRecordKind::ProviderUsage,
            SessionRecord::ProviderCache(_) => SessionRecordKind::ProviderCache,
            SessionRecord::Reflection(_) => SessionRecordKind::Reflection,
            SessionRecord::PolicyTuning(_) => SessionRecordKind::PolicyTuning,
            SessionRecord::StrategyPreference(_) => SessionRecordKind::StrategyPreference,
            SessionRecord::ToolPerformance(_) => SessionRecordKind::ToolPerformance,
            SessionRecord::ProfilePatch(_) => SessionRecordKind::ProfilePatch,
            SessionRecord::Outcome(_) => SessionRecordKind::Outcome,
        }
    }

    pub fn occurred_at(&self) -> SystemTime {
        match self {
            SessionRecord::Trigger(record) => record.recorded_at,
            SessionRecord::KernelEvent(record) => record.occurred_at,
            SessionRecord::ModelDecision(record) => record.decided_at,
            SessionRecord::ToolCall(record) => record.called_at,
            SessionRecord::ToolResult(record) => record.finished_at,
            SessionRecord::PendingApproval(record) => record.created_at,
            SessionRecord::ApprovalResolution(record) => record.resolved_at,
            SessionRecord::Delegation(record) => record.created_at,
            SessionRecord::CoordinationClaim(record) => record.claimed_at,
            SessionRecord::ProviderUsage(record) => record.recorded_at,
            SessionRecord::ProviderCache(record) => record.cached_at,
            SessionRecord::Reflection(record) => record.created_at,
            SessionRecord::PolicyTuning(record) => record.created_at,
            SessionRecord::StrategyPreference(record) => record.created_at,
            SessionRecord::ToolPerformance(record) => record.created_at,
            SessionRecord::ProfilePatch(record) => record.created_at,
            SessionRecord::Outcome(record) => record.finished_at,
        }
    }

    pub fn trigger_id(&self) -> Option<&str> {
        match self {
            SessionRecord::Trigger(record) => Some(&record.trigger_id),
            SessionRecord::PendingApproval(record) => Some(&record.trigger_id),
            SessionRecord::Delegation(record) => Some(&record.trigger_id),
            SessionRecord::Reflection(record) => Some(&record.trigger_id),
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

#[typeshare::typeshare]
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

#[typeshare::typeshare]
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

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionSnapshot {
    pub session_id: String,
    pub records: Vec<SessionRecord>,
    pub last_sequence_no: Option<i64>,
    pub latest_outcome: Option<OutcomeRecord>,
}

impl SessionSnapshot {
    pub fn kernel_events(&self) -> Vec<KernelEventRecord> {
        self.records
            .iter()
            .filter_map(|record| match record {
                SessionRecord::KernelEvent(event) => Some(event.clone()),
                _ => None,
            })
            .collect()
    }

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

    pub fn active_policy_overlay(&self) -> Option<PolicyOverlay> {
        let rolled_back = self
            .records
            .iter()
            .filter_map(|record| match record {
                SessionRecord::PolicyTuning(tuning)
                    if tuning.action == PolicyTuningAction::RolledBack =>
                {
                    Some(tuning.overlay.overlay_id.clone())
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>();

        self.records.iter().rev().find_map(|record| match record {
            SessionRecord::PolicyTuning(tuning)
                if tuning.action == PolicyTuningAction::Applied
                    && tuning.overlay.status == PolicyOverlayStatus::Applied
                    && !rolled_back.contains(&tuning.overlay.overlay_id) =>
            {
                Some(tuning.overlay.clone())
            }
            _ => None,
        })
    }

    pub fn reflections(&self) -> Vec<ReflectionRecord> {
        self.records
            .iter()
            .filter_map(|record| match record {
                SessionRecord::Reflection(reflection) => Some(reflection.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn policy_tunings(&self) -> Vec<PolicyTuningRecord> {
        self.records
            .iter()
            .filter_map(|record| match record {
                SessionRecord::PolicyTuning(tuning) => Some(tuning.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn strategy_preferences(&self) -> Vec<StrategyPreferenceRecord> {
        self.records
            .iter()
            .filter_map(|record| match record {
                SessionRecord::StrategyPreference(preference) => Some(preference.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn tool_performance_records(&self) -> Vec<ToolPerformanceRecord> {
        self.records
            .iter()
            .filter_map(|record| match record {
                SessionRecord::ToolPerformance(performance) => Some(performance.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn active_trigger(&self) -> Option<TriggerRecord> {
        let outcome_index = self
            .records
            .iter()
            .rposition(|record| matches!(record, SessionRecord::Outcome(_)));
        let slice = match outcome_index {
            Some(index) => &self.records[index + 1..],
            None => &self.records[..],
        };
        slice.iter().find_map(|record| match record {
            SessionRecord::Trigger(trigger) => Some(trigger.clone()),
            _ => None,
        })
    }

    pub fn current_step_count(&self) -> usize {
        let outcome_index = self
            .records
            .iter()
            .rposition(|record| matches!(record, SessionRecord::Outcome(_)));
        let slice = match outcome_index {
            Some(index) => &self.records[index + 1..],
            None => &self.records[..],
        };
        slice
            .iter()
            .filter(|record| matches!(record, SessionRecord::ModelDecision(_)))
            .count()
    }

    pub fn current_consecutive_tool_failure_steps(&self) -> usize {
        let outcome_index = self
            .records
            .iter()
            .rposition(|record| matches!(record, SessionRecord::Outcome(_)));
        let slice = match outcome_index {
            Some(index) => &self.records[index + 1..],
            None => &self.records[..],
        };

        let mut windows = Vec::<(AgentAction, Vec<ToolResultRecord>)>::new();
        let mut current_action: Option<AgentAction> = None;
        let mut current_results = Vec::<ToolResultRecord>::new();

        for record in slice {
            match record {
                SessionRecord::ModelDecision(decision) => {
                    if let Some(action) = current_action.replace(decision.action.clone()) {
                        windows.push((action, current_results));
                        current_results = Vec::new();
                    }
                }
                SessionRecord::ToolResult(result) => current_results.push(result.clone()),
                _ => {}
            }
        }

        if let Some(action) = current_action {
            windows.push((action, current_results));
        }

        let mut trailing_failures = 0usize;
        for (action, results) in windows.into_iter().rev() {
            match action {
                AgentAction::CallSkills(calls)
                    if !calls.is_empty()
                        && results.len() == calls.len()
                        && results.iter().all(|result| result.output.is_err()) =>
                {
                    trailing_failures += 1;
                }
                _ => break,
            }
        }

        trailing_failures
    }

    pub fn agent_state(&self) -> AgentStateSnapshot {
        let mut goals = Vec::<GoalRecord>::new();
        let mut tasks = Vec::<TaskRecord>::new();
        let mut observations = Vec::<ObservationRecord>::new();
        let mut artifacts = Vec::<ArtifactRecord>::new();
        let mut resources = Vec::<ResourceRef>::new();
        let mut relationships = Vec::<RelationshipEdge>::new();
        let mut pending_wake = None;

        for event_record in self.kernel_events() {
            match event_record.event {
                KernelEvent::GoalCreated(goal) => {
                    upsert_by_key(&mut goals, goal, |goal| goal.goal_id.clone())
                }
                KernelEvent::TaskPlanned(task) => {
                    upsert_by_key(&mut tasks, task, |task| task.task_id.clone())
                }
                KernelEvent::TaskClaimed {
                    task_id,
                    claimed_at: _,
                    assignee,
                } => {
                    if let Some(task) = tasks.iter_mut().find(|task| task.task_id == task_id) {
                        task.status = TaskStatus::Running;
                        task.assignee = assignee;
                    }
                }
                KernelEvent::TaskBlocked { task_id, .. } => {
                    if let Some(task) = tasks.iter_mut().find(|task| task.task_id == task_id) {
                        task.status = TaskStatus::Blocked;
                    }
                }
                KernelEvent::TaskCompleted { task_id, .. } => {
                    if let Some(task) = tasks.iter_mut().find(|task| task.task_id == task_id) {
                        task.status = TaskStatus::Done;
                    }
                }
                KernelEvent::TaskFailed { task_id, .. } => {
                    if let Some(task) = tasks.iter_mut().find(|task| task.task_id == task_id) {
                        task.status = TaskStatus::Failed;
                    }
                }
                KernelEvent::TaskAbandoned { task_id, .. } => {
                    if let Some(task) = tasks.iter_mut().find(|task| task.task_id == task_id) {
                        task.status = TaskStatus::Abandoned;
                    }
                }
                KernelEvent::HumanInputRequested { task_id, .. } => {
                    if let Some(task_id) = task_id
                        && let Some(task) = tasks.iter_mut().find(|task| task.task_id == task_id)
                    {
                        task.status = TaskStatus::WaitingHuman;
                    }
                }
                KernelEvent::ObservationAppended(observation) => observations.push(observation),
                KernelEvent::ArtifactProduced(artifact) => {
                    upsert_by_key(&mut artifacts, artifact, |artifact| {
                        artifact.artifact_id.clone()
                    })
                }
                KernelEvent::WakeRequested(wake) | KernelEvent::WakeScheduled(wake) => {
                    pending_wake = Some(wake)
                }
                KernelEvent::DelegationRequested(record) => {
                    let resource = ResourceRef {
                        resource_id: record.correlation_id.0.clone(),
                        resource_type: "delegation".to_string(),
                        label: record.task.task_type.clone(),
                        external_ref: Some(record.target.stream.clone()),
                    };
                    upsert_by_key(&mut resources, resource, |resource| {
                        resource.resource_id.clone()
                    });
                }
                KernelEvent::DelegationResolved { .. } => {}
                KernelEvent::ResourceRegistered(resource) => {
                    upsert_by_key(&mut resources, resource, |resource| {
                        resource.resource_id.clone()
                    })
                }
                KernelEvent::RelationshipObserved(edge) => relationships.push(edge),
            }
        }

        AgentStateSnapshot {
            agent_id: AgentId(self.session_id.clone()),
            profile: Some(AgentProfile {
                agent_id: AgentId(self.session_id.clone()),
                role: "event-agent".to_string(),
                default_scopes: Vec::new(),
                allowed_skill_names: Vec::new(),
                goal_ids: goals.iter().map(|goal| goal.goal_id.clone()).collect(),
                memory_policy: MemoryPolicy::default(),
                wake_policy: WakePolicy::default(),
                review_policy: ReviewPolicy::default(),
                escalation_policy: EscalationPolicy::default(),
                config_artifact: None,
            }),
            goals,
            tasks,
            observations,
            artifacts,
            resources,
            relationships,
            pending_wake,
        }
    }
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionSummary {
    pub session_id: String,
    pub first_recorded_at_ms: i64,
    pub last_recorded_at_ms: i64,
    pub record_count: usize,
}

#[typeshare::typeshare]
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

#[typeshare::typeshare]
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

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecordPage {
    pub session_id: String,
    pub records: Vec<StoredSessionRecord>,
    pub next_offset: Option<usize>,
}

#[typeshare::typeshare]
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
    #[serde(default)]
    pub self_improvement: SelfImprovementPolicy,
}

impl Default for EnginePolicy {
    fn default() -> Self {
        Self {
            max_steps: 16,
            max_execution_time_ms: 120_000,
            provider_timeout_ms: 45_000,
            max_tool_timeout_ms: 10_000,
            max_consecutive_tool_failures: 20,
            cache_threshold_tokens: 32_000,
            max_cost_per_session: 5.0,
            max_parallel_skill_calls: 4,
            max_inline_attachment_bytes: 512 * 1024,
            allow_native_skills: true,
            self_improvement: SelfImprovementPolicy::default(),
        }
    }
}

impl EnginePolicy {
    pub fn with_overlay(mut self, overlay: Option<PolicyOverlay>) -> Self {
        if let Some(overlay) = overlay {
            overlay.apply_to(&mut self);
        }
        self
    }

    pub fn max_execution_time(&self) -> Duration {
        Duration::from_millis(self.max_execution_time_ms.max(1))
    }

    pub fn provider_timeout(&self) -> Duration {
        Duration::from_millis(self.provider_timeout_ms.max(1))
    }
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ProviderRequestConfig {
    pub model: Option<String>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProviderRole {
    System,
    User,
    Assistant,
    Tool,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "payload")]
pub enum ProviderContentPart {
    Text(String),
    Json(Value),
    InlineData(MultimodalPayload),
    Attachment(AttachmentRef),
    ToolResult(ToolResultRecord),
}

#[typeshare::typeshare]
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
        let snapshot = SessionSnapshot {
            session_id: self.session_id.clone(),
            records: self.records.clone(),
            last_sequence_no: None,
            latest_outcome: None,
        };
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
            state: snapshot.agent_state(),
        }
    }
}

#[typeshare::typeshare]
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
    pub state: AgentStateSnapshot,
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

#[derive(Debug, Clone)]
pub struct ContinueRequest {
    pub session_id: String,
    pub granted_scopes: BTreeSet<String>,
    pub policy: EnginePolicy,
    pub provider: ProviderRequestConfig,
    pub cancellation: CancellationToken,
}

impl ContinueRequest {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            granted_scopes: BTreeSet::new(),
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
}

#[derive(Debug, Clone)]
pub enum AdvanceRequest {
    Trigger(ProcessRequest),
    Continue(ContinueRequest),
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdvanceResult {
    pub outcome: Option<EngineOutcome>,
    pub emitted_events: Vec<KernelEventRecord>,
    pub state_delta: AgentStateDelta,
    pub wake_request: Option<WakeRequestRecord>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderRequest {
    pub trigger: AgentTrigger,
    pub context: AgentContextSnapshot,
    pub available_skills: Vec<SkillDefinition>,
    pub config: ProviderRequestConfig,
    pub policy: EnginePolicy,
    pub contents: Vec<ProviderMessage>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderDecision {
    pub action: AgentAction,
    pub usage: Option<ProviderUsageRecord>,
    pub cache: Option<ProviderCacheRecord>,
}

#[typeshare::typeshare]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillInvocation {
    pub call_id: String,
    pub manifest: SkillManifest,
    pub args: Value,
    pub context: AgentContextSnapshot,
}

#[typeshare::typeshare]
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

fn upsert_by_key<T, K, F>(items: &mut Vec<T>, value: T, key_fn: F)
where
    K: PartialEq,
    F: Fn(&T) -> K,
{
    let key = key_fn(&value);
    if let Some(existing) = items.iter_mut().find(|item| key_fn(item) == key) {
        *existing = value;
    } else {
        items.push(value);
    }
}
