use crate::{
    AgentAction, AgentContext, AgentTrigger, ApprovalDecision, ApprovalResolutionRecord,
    DelegationRecord, EngineOutcome, ExecutionMetadata, LlmProvider, MemoryError, MemoryStore,
    MemoryStoreExt, ModelDecisionRecord, OutcomeRecord, PendingApprovalRecord, PlannedSkillCall,
    ProcessRequest, ProviderContentPart, ProviderDecision, ProviderMessage, ProviderRequest,
    ProviderRole, ResumeToken, SessionRecord, SkillBackendKind, SkillDefinition, SkillFailure,
    SkillFailureKind, SkillInvocation, SkillManifest, StopReason, SuspendReason, ToolCallRecord,
    ToolResultRecord, TriggerRecord,
};
use async_trait::async_trait;
use metrics::{counter, histogram};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("memory error: {0}")]
    Memory(#[from] MemoryError),
    #[error("blob error: {0}")]
    Blob(String),
}

#[derive(Debug, Error, Clone, PartialEq)]
#[error("{kind:?}: {message}")]
pub struct SkillExecutionError {
    pub kind: SkillFailureKind,
    pub message: String,
}

impl SkillExecutionError {
    pub fn new(kind: SkillFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

#[async_trait]
pub trait SkillExecutor: Send + Sync {
    async fn execute(
        &self,
        invocation: SkillInvocation,
    ) -> Result<serde_json::Value, SkillExecutionError>;

    fn executor_kind(&self) -> &'static str;
}

pub trait WasmSkillExecutor: SkillExecutor {}

impl<T> WasmSkillExecutor for T where T: SkillExecutor + ?Sized {}

#[async_trait]
pub trait NativeSkill: Send + Sync {
    async fn execute(
        &self,
        invocation: SkillInvocation,
    ) -> Result<serde_json::Value, SkillExecutionError>;

    fn requires_human_approval(&self) -> bool {
        false
    }

    fn executor_kind(&self) -> &'static str {
        "native"
    }
}

#[derive(Clone)]
enum RegisteredSkillBackend {
    Wasm(Arc<dyn SkillExecutor>),
    Native(Arc<dyn NativeSkill>),
}

impl RegisteredSkillBackend {
    fn kind(&self) -> SkillBackendKind {
        match self {
            RegisteredSkillBackend::Wasm(_) => SkillBackendKind::Wasm,
            RegisteredSkillBackend::Native(_) => SkillBackendKind::Native,
        }
    }

    fn executor_kind(&self) -> &'static str {
        match self {
            RegisteredSkillBackend::Wasm(executor) => executor.executor_kind(),
            RegisteredSkillBackend::Native(executor) => executor.executor_kind(),
        }
    }

    fn requires_human_approval(&self) -> bool {
        match self {
            RegisteredSkillBackend::Wasm(_) => false,
            RegisteredSkillBackend::Native(executor) => executor.requires_human_approval(),
        }
    }
}

#[derive(Clone)]
struct RegisteredSkill {
    manifest: SkillManifest,
    backend: RegisteredSkillBackend,
}

impl RegisteredSkill {
    fn definition(&self) -> SkillDefinition {
        SkillDefinition {
            manifest: self.manifest.clone(),
            executor_kind: self.backend.executor_kind().to_string(),
        }
    }
}

#[derive(Clone)]
pub struct AgentEngine {
    llm: Arc<dyn LlmProvider>,
    memory: Arc<dyn MemoryStore>,
    skills: Arc<RwLock<HashMap<String, RegisteredSkill>>>,
}

impl AgentEngine {
    pub fn new(llm: Arc<dyn LlmProvider>, memory: Arc<dyn MemoryStore>) -> Self {
        Self {
            llm,
            memory,
            skills: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn advance(&self, request: ProcessRequest) -> Result<EngineOutcome, EngineError> {
        self.process_trigger(request).await
    }

    pub async fn register_skill(&self, manifest: SkillManifest, executor: Arc<dyn SkillExecutor>) {
        self.register_wasm_skill(manifest, executor).await;
    }

    pub async fn register_wasm_skill(
        &self,
        manifest: SkillManifest,
        executor: Arc<dyn SkillExecutor>,
    ) {
        self.skills.write().await.insert(
            manifest.name.clone(),
            RegisteredSkill {
                manifest,
                backend: RegisteredSkillBackend::Wasm(executor),
            },
        );
    }

    pub async fn register_native_skill(
        &self,
        manifest: SkillManifest,
        executor: Arc<dyn NativeSkill>,
    ) {
        self.skills.write().await.insert(
            manifest.name.clone(),
            RegisteredSkill {
                manifest,
                backend: RegisteredSkillBackend::Native(executor),
            },
        );
    }

    pub async fn process_trigger(
        &self,
        request: ProcessRequest,
    ) -> Result<EngineOutcome, EngineError> {
        let started_at = SystemTime::now();
        let trigger_id = Uuid::new_v4().to_string();
        let deadline = Instant::now() + request.policy.max_execution_time();

        if let Some(idempotency_key) = request.idempotency_key.as_deref() {
            if let Ok(Some(mut prior_outcome)) = self
                .memory
                .find_outcome_by_idempotency_key(&request.session_id, idempotency_key)
                .await
            {
                prior_outcome.idempotent_replay = true;
                counter!("rain_engine.idempotent_replay_total").increment(1);
                return Ok(prior_outcome);
            }
        }

        let trigger_record = TriggerRecord {
            trigger_id: trigger_id.clone(),
            session_id: request.session_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            recorded_at: started_at,
            trigger: request.trigger.clone(),
        };
        if let Err(err) = self.memory.append_trigger(trigger_record).await {
            return Ok(storage_failure_outcome(trigger_id, 0, err.message));
        }

        let snapshot = match self.memory.load_session(&request.session_id).await {
            Ok(snapshot) => snapshot,
            Err(err) => return Ok(storage_failure_outcome(trigger_id, 0, err.message)),
        };
        let mut context = AgentContext {
            session_id: request.session_id.clone(),
            records: snapshot.records.clone(),
            prior_tool_results: snapshot.tool_results(),
            granted_scopes: request.granted_scopes.clone(),
            metadata: ExecutionMetadata {
                trigger_id: trigger_id.clone(),
                idempotency_key: request.idempotency_key.clone(),
                started_at,
                deadline,
                policy: request.policy.clone(),
                provider: request.provider.clone(),
                cancellation: request.cancellation.clone(),
            },
        };

        counter!("rain_engine.triggers_total").increment(1);
        info!(session_id = %context.session_id, trigger_id = %trigger_id, "processing trigger");

        let mut steps_executed = 0usize;
        let mut consecutive_tool_failure_steps = 0usize;

        if let AgentTrigger::Approval {
            resume_token,
            decision,
            metadata,
        } = &request.trigger
        {
            let pending = match self
                .memory
                .find_pending_approval_by_resume_token(&context.session_id, resume_token.as_str())
                .await?
            {
                Some(pending) => pending,
                None => {
                    return self
                        .finish(
                            &mut context,
                            StopReason::PolicyAborted,
                            None,
                            Some("resume token not found".to_string()),
                            0,
                            None,
                        )
                        .await;
                }
            };

            self.memory
                .append_approval_resolution(
                    &context.session_id,
                    ApprovalResolutionRecord {
                        resume_token: pending.resume_token.clone(),
                        resolved_at: SystemTime::now(),
                        decision: decision.clone(),
                        metadata: metadata.clone(),
                    },
                )
                .await?;

            let resumed = match decision {
                ApprovalDecision::Approved => match self
                    .execute_planned_calls(
                        &context,
                        pending.step,
                        pending.pending_calls.clone(),
                        true,
                    )
                    .await?
                {
                    BatchExecution::Executed(batch) => batch,
                    BatchExecution::Suspended { .. } => {
                        return self
                            .finish(
                                &mut context,
                                StopReason::PolicyAborted,
                                None,
                                Some("approval resume unexpectedly suspended".to_string()),
                                pending.step,
                                None,
                            )
                            .await;
                    }
                },
                ApprovalDecision::Rejected => ExecutedBatch {
                    results: pending
                        .pending_calls
                        .into_iter()
                        .map(|call| ToolResultRecord {
                            call_id: call.call_id,
                            finished_at: SystemTime::now(),
                            skill_name: call.name,
                            output: Err(SkillFailure {
                                kind: SkillFailureKind::PermissionDenied,
                                message: "human approval rejected".to_string(),
                            }),
                        })
                        .collect(),
                    all_failed: true,
                },
            };

            for result in resumed.results {
                self.memory
                    .append_tool_result(&context.session_id, result.clone())
                    .await?;
                context.prior_tool_results.push(result.clone());
                context.records.push(SessionRecord::ToolResult(result));
            }
            steps_executed = pending.step + 1;
            if resumed.all_failed {
                consecutive_tool_failure_steps = 1;
            }
        }

        loop {
            if context.metadata.cancellation.is_cancelled() {
                return self
                    .finish(
                        &mut context,
                        StopReason::Cancelled,
                        None,
                        Some("execution cancelled".to_string()),
                        steps_executed,
                        None,
                    )
                    .await;
            }

            if Instant::now() >= context.metadata.deadline {
                return self
                    .finish(
                        &mut context,
                        StopReason::DeadlineExceeded,
                        None,
                        Some("engine execution deadline exceeded".to_string()),
                        steps_executed,
                        None,
                    )
                    .await;
            }

            if steps_executed >= context.metadata.policy.max_steps {
                return self
                    .finish(
                        &mut context,
                        StopReason::MaxStepsReached,
                        None,
                        Some("max steps reached".to_string()),
                        steps_executed,
                        None,
                    )
                    .await;
            }

            if consecutive_tool_failure_steps
                >= context.metadata.policy.max_consecutive_tool_failures
            {
                return self
                    .finish(
                        &mut context,
                        StopReason::PolicyAborted,
                        None,
                        Some("max consecutive tool failure steps reached".to_string()),
                        steps_executed,
                        None,
                    )
                    .await;
            }

            let cost_so_far = context
                .records
                .iter()
                .filter_map(|record| match record {
                    SessionRecord::ProviderUsage(usage) => Some(usage.estimated_cost_usd),
                    _ => None,
                })
                .sum::<f64>();
            if cost_so_far >= context.metadata.policy.max_cost_per_session {
                return self
                    .finish(
                        &mut context,
                        StopReason::PolicyAborted,
                        None,
                        Some("session cost limit reached".to_string()),
                        steps_executed,
                        None,
                    )
                    .await;
            }

            let available_skills = self
                .skills
                .read()
                .await
                .values()
                .filter(|skill| {
                    skill
                        .manifest
                        .required_scopes
                        .iter()
                        .all(|scope| context.granted_scopes.contains(scope))
                })
                .map(RegisteredSkill::definition)
                .collect::<Vec<_>>();

            let provider_request = ProviderRequest {
                trigger: request.trigger.clone(),
                context: context.to_snapshot(steps_executed),
                available_skills,
                config: context.metadata.provider.clone(),
                policy: context.metadata.policy.clone(),
                contents: build_provider_contents(&request.trigger),
            };
            let provider_started = Instant::now();
            let decision = match tokio::time::timeout(
                context.metadata.policy.provider_timeout(),
                self.llm.generate_action(provider_request),
            )
            .await
            {
                Ok(Ok(decision)) => decision,
                Ok(Err(err)) => {
                    warn!(session_id = %context.session_id, "provider failed: {}", err.message);
                    return self
                        .finish(
                            &mut context,
                            StopReason::ProviderFailure,
                            None,
                            Some(format!("provider failure: {}", err.message)),
                            steps_executed,
                            None,
                        )
                        .await;
                }
                Err(_) => {
                    warn!(session_id = %context.session_id, "provider timed out");
                    return self
                        .finish(
                            &mut context,
                            StopReason::ProviderFailure,
                            None,
                            Some("provider timeout exceeded".to_string()),
                            steps_executed,
                            None,
                        )
                        .await;
                }
            };
            histogram!("rain_engine.provider_latency_seconds")
                .record(provider_started.elapsed().as_secs_f64());

            self.persist_provider_metadata(&mut context, &decision)
                .await?;

            let decision_record = ModelDecisionRecord {
                step: steps_executed,
                decided_at: SystemTime::now(),
                action: decision.action.clone(),
            };
            if let Err(err) = self
                .memory
                .append_model_decision(&context.session_id, decision_record.clone())
                .await
            {
                return Ok(storage_failure_outcome(
                    context.metadata.trigger_id.clone(),
                    steps_executed,
                    err.message,
                ));
            }
            context
                .records
                .push(SessionRecord::ModelDecision(decision_record));

            match decision.action {
                AgentAction::Respond { content } => {
                    return self
                        .finish(
                            &mut context,
                            StopReason::Responded,
                            Some(content),
                            None,
                            steps_executed + 1,
                            None,
                        )
                        .await;
                }
                AgentAction::Yield { reason } => {
                    return self
                        .finish(
                            &mut context,
                            StopReason::Yielded,
                            None,
                            reason,
                            steps_executed + 1,
                            None,
                        )
                        .await;
                }
                AgentAction::Continue { .. } => {
                    steps_executed += 1;
                }
                AgentAction::Suspend {
                    reason,
                    pending_calls,
                    resume_token,
                } => {
                    return self
                        .suspend(
                            &mut context,
                            steps_executed,
                            reason,
                            pending_calls,
                            resume_token,
                        )
                        .await;
                }
                AgentAction::CallSkills(calls) => {
                    steps_executed += 1;
                    match self
                        .execute_planned_calls(&context, steps_executed - 1, calls, false)
                        .await?
                    {
                        BatchExecution::Executed(ExecutedBatch {
                            results,
                            all_failed,
                        }) => {
                            for result in results {
                                if let Err(err) = self
                                    .memory
                                    .append_tool_result(&context.session_id, result.clone())
                                    .await
                                {
                                    return Ok(storage_failure_outcome(
                                        context.metadata.trigger_id.clone(),
                                        steps_executed,
                                        err.message,
                                    ));
                                }
                                context.prior_tool_results.push(result.clone());
                                context.records.push(SessionRecord::ToolResult(result));
                            }
                            consecutive_tool_failure_steps = if all_failed {
                                consecutive_tool_failure_steps + 1
                            } else {
                                0
                            };
                        }
                        BatchExecution::Suspended {
                            reason,
                            pending_calls,
                            resume_token,
                        } => {
                            return self
                                .suspend(
                                    &mut context,
                                    steps_executed - 1,
                                    reason,
                                    pending_calls,
                                    resume_token,
                                )
                                .await;
                        }
                    }
                }
                AgentAction::Delegate {
                    target,
                    task,
                    correlation_id,
                    resume_token,
                } => {
                    let record = DelegationRecord {
                        correlation_id,
                        created_at: SystemTime::now(),
                        trigger_id: context.metadata.trigger_id.clone(),
                        target,
                        task,
                        resume_token: resume_token.clone(),
                    };
                    self.memory
                        .append_delegation(&context.session_id, record.clone())
                        .await?;
                    context.records.push(SessionRecord::Delegation(record));
                    return self
                        .finish(
                            &mut context,
                            StopReason::Delegated,
                            None,
                            Some("delegated to downstream worker".to_string()),
                            steps_executed + 1,
                            Some(resume_token),
                        )
                        .await;
                }
            }
        }
    }

    async fn persist_provider_metadata(
        &self,
        context: &mut AgentContext,
        decision: &ProviderDecision,
    ) -> Result<(), EngineError> {
        if let Some(usage) = &decision.usage {
            self.memory
                .append_provider_usage(&context.session_id, usage.clone())
                .await?;
            context
                .records
                .push(SessionRecord::ProviderUsage(usage.clone()));
        }
        if let Some(cache) = &decision.cache {
            self.memory
                .append_provider_cache(&context.session_id, cache.clone())
                .await?;
            context
                .records
                .push(SessionRecord::ProviderCache(cache.clone()));
        }
        Ok(())
    }

    async fn suspend(
        &self,
        context: &mut AgentContext,
        step: usize,
        reason: SuspendReason,
        pending_calls: Vec<PlannedSkillCall>,
        resume_token: ResumeToken,
    ) -> Result<EngineOutcome, EngineError> {
        let pending = PendingApprovalRecord {
            resume_token: resume_token.clone(),
            created_at: SystemTime::now(),
            trigger_id: context.metadata.trigger_id.clone(),
            step,
            reason: reason.clone(),
            pending_calls,
        };
        self.memory
            .append_pending_approval(&context.session_id, pending.clone())
            .await?;
        context
            .records
            .push(SessionRecord::PendingApproval(pending.clone()));
        self.finish(
            context,
            StopReason::Suspended,
            None,
            Some(match reason {
                SuspendReason::HumanApprovalRequired { .. } => {
                    "human approval required".to_string()
                }
                SuspendReason::ProviderRequested { message } => message,
            }),
            step,
            Some(resume_token),
        )
        .await
    }

    async fn execute_planned_calls(
        &self,
        context: &AgentContext,
        step: usize,
        calls: Vec<PlannedSkillCall>,
        approval_override: bool,
    ) -> Result<BatchExecution, EngineError> {
        let registry = self.skills.read().await;
        if !approval_override {
            let approval_calls = calls
                .iter()
                .filter_map(|call| {
                    let skill = registry.get(&call.name)?;
                    skill
                        .backend
                        .requires_human_approval()
                        .then_some(call.name.clone())
                })
                .collect::<Vec<_>>();
            if !approval_calls.is_empty() {
                return Ok(BatchExecution::Suspended {
                    reason: SuspendReason::HumanApprovalRequired {
                        skill_names: approval_calls,
                    },
                    pending_calls: calls,
                    resume_token: ResumeToken(Uuid::new_v4().to_string()),
                });
            }
        }

        let mut immediate_results = Vec::<ToolResultRecord>::new();
        let mut executable = VecDeque::<PreparedCall>::new();

        for call in calls.iter() {
            let Some(skill) = registry.get(&call.name).cloned() else {
                immediate_results.push(error_result(
                    call.call_id.clone(),
                    call.name.clone(),
                    SkillFailureKind::Internal,
                    format!("skill `{}` is not registered", call.name),
                ));
                continue;
            };

            if !skill
                .manifest
                .required_scopes
                .iter()
                .all(|scope| context.granted_scopes.contains(scope))
            {
                counter!("rain_engine.permission_denials_total").increment(1);
                immediate_results.push(error_result(
                    call.call_id.clone(),
                    call.name.clone(),
                    SkillFailureKind::PermissionDenied,
                    format!("missing required scopes for skill `{}`", call.name),
                ));
                continue;
            }

            if matches!(skill.backend, RegisteredSkillBackend::Native(_))
                && !context.metadata.policy.allow_native_skills
            {
                immediate_results.push(error_result(
                    call.call_id.clone(),
                    call.name.clone(),
                    SkillFailureKind::PermissionDenied,
                    "native skills are disabled by policy".to_string(),
                ));
                continue;
            }

            let call_record = ToolCallRecord {
                call_id: call.call_id.clone(),
                step,
                called_at: SystemTime::now(),
                skill_name: skill.manifest.name.clone(),
                args: call.args.clone(),
                backend_kind: skill.backend.kind(),
            };
            self.memory
                .append_tool_call(&context.session_id, call_record.clone())
                .await?;

            let mut manifest = skill.manifest.clone();
            manifest.resource_policy = manifest.effective_resource_policy(&context.metadata.policy);
            executable.push_back(PreparedCall {
                call_id: call.call_id.clone(),
                name: call.name.clone(),
                args: call.args.clone(),
                manifest,
                backend: skill.backend.clone(),
                context_snapshot: context.to_snapshot(step),
            });
        }
        drop(registry);

        let max_parallel = context.metadata.policy.max_parallel_skill_calls.max(1);
        let mut join_set = JoinSet::new();
        let mut pending = executable;
        let mut results_by_id = HashMap::<String, ToolResultRecord>::new();

        while !pending.is_empty() || !join_set.is_empty() {
            while join_set.len() < max_parallel && !pending.is_empty() {
                let prepared = pending.pop_front().expect("pending call exists");
                join_set.spawn(run_prepared_call(prepared));
            }

            if let Some(joined) = join_set.join_next().await {
                let result = joined.map_err(|err| EngineError::Blob(err.to_string()))??;
                results_by_id.insert(result.call_id.clone(), result);
            }
        }

        let mut ordered = Vec::with_capacity(calls.len());
        let mut any_success = false;
        for call in calls {
            if let Some(result) = results_by_id.remove(&call.call_id) {
                if result.output.is_ok() {
                    any_success = true;
                }
                ordered.push(result);
            } else if let Some(index) = immediate_results
                .iter()
                .position(|result| result.call_id == call.call_id)
            {
                let result = immediate_results.remove(index);
                if result.output.is_ok() {
                    any_success = true;
                }
                ordered.push(result);
            }
        }

        Ok(BatchExecution::Executed(ExecutedBatch {
            results: ordered,
            all_failed: !any_success,
        }))
    }

    async fn finish(
        &self,
        context: &mut AgentContext,
        stop_reason: StopReason,
        response: Option<String>,
        detail: Option<String>,
        steps_executed: usize,
        resume_token: Option<ResumeToken>,
    ) -> Result<EngineOutcome, EngineError> {
        let outcome = OutcomeRecord {
            trigger_id: context.metadata.trigger_id.clone(),
            idempotency_key: context.metadata.idempotency_key.clone(),
            finished_at: SystemTime::now(),
            stop_reason: stop_reason.clone(),
            response: response.clone(),
            detail: detail.clone(),
            steps_executed,
            resume_token: resume_token.clone(),
        };
        if let Err(err) = self
            .memory
            .append_outcome(&context.session_id, outcome.clone())
            .await
        {
            error!("failed to persist outcome: {}", err.message);
            return Ok(storage_failure_outcome(
                context.metadata.trigger_id.clone(),
                steps_executed,
                err.message,
            ));
        }
        context
            .records
            .push(SessionRecord::Outcome(outcome.clone()));
        Ok(EngineOutcome {
            trigger_id: outcome.trigger_id,
            stop_reason,
            response,
            detail,
            steps_executed,
            idempotent_replay: false,
            resume_token,
        })
    }
}

struct PreparedCall {
    call_id: String,
    name: String,
    args: serde_json::Value,
    manifest: SkillManifest,
    backend: RegisteredSkillBackend,
    context_snapshot: crate::AgentContextSnapshot,
}

#[derive(Debug)]
struct ExecutedBatch {
    results: Vec<ToolResultRecord>,
    all_failed: bool,
}

#[derive(Debug)]
enum BatchExecution {
    Executed(ExecutedBatch),
    Suspended {
        reason: SuspendReason,
        pending_calls: Vec<PlannedSkillCall>,
        resume_token: ResumeToken,
    },
}

async fn run_prepared_call(prepared: PreparedCall) -> Result<ToolResultRecord, EngineError> {
    let invocation = SkillInvocation {
        call_id: prepared.call_id.clone(),
        manifest: prepared.manifest.clone(),
        args: prepared.args,
        context: prepared.context_snapshot,
    };

    let started = Instant::now();
    let output = match &prepared.backend {
        RegisteredSkillBackend::Wasm(executor) => executor.execute(invocation).await,
        RegisteredSkillBackend::Native(executor) => executor.execute(invocation).await,
    };
    histogram!("rain_engine.tool_latency_seconds").record(started.elapsed().as_secs_f64());

    let output = match output {
        Ok(value) => Ok(value),
        Err(err) => {
            match err.kind {
                SkillFailureKind::PermissionDenied | SkillFailureKind::CapabilityDenied => {
                    counter!("rain_engine.permission_denials_total").increment(1);
                }
                SkillFailureKind::Trap | SkillFailureKind::MemoryLimitExceeded => {
                    counter!("rain_engine.wasm_traps_total").increment(1);
                }
                SkillFailureKind::Timeout => {
                    counter!("rain_engine.tool_timeouts_total").increment(1);
                }
                SkillFailureKind::InvalidResponse | SkillFailureKind::Internal => {}
            }
            Err(SkillFailure {
                kind: err.kind,
                message: err.message,
            })
        }
    };

    Ok(ToolResultRecord {
        call_id: prepared.call_id,
        finished_at: SystemTime::now(),
        skill_name: prepared.name,
        output,
    })
}

fn error_result(
    call_id: String,
    skill_name: String,
    kind: SkillFailureKind,
    message: String,
) -> ToolResultRecord {
    ToolResultRecord {
        call_id,
        finished_at: SystemTime::now(),
        skill_name,
        output: Err(SkillFailure { kind, message }),
    }
}

fn build_provider_contents(trigger: &AgentTrigger) -> Vec<ProviderMessage> {
    let mut parts = Vec::new();
    match trigger {
        AgentTrigger::Webhook {
            source,
            payload,
            attachments,
        } => {
            parts.push(ProviderContentPart::Text(format!(
                "webhook source: {source}"
            )));
            parts.push(ProviderContentPart::Json(payload.clone()));
            parts.extend(
                attachments
                    .iter()
                    .cloned()
                    .map(ProviderContentPart::Attachment),
            );
        }
        AgentTrigger::RuleTrigger {
            rule_id,
            context,
            attachments,
        } => {
            parts.push(ProviderContentPart::Text(format!(
                "rule trigger: {rule_id}"
            )));
            parts.push(ProviderContentPart::Json(context.clone()));
            parts.extend(
                attachments
                    .iter()
                    .cloned()
                    .map(ProviderContentPart::Attachment),
            );
        }
        AgentTrigger::ProactiveHeartbeat {
            timestamp,
            attachments,
        } => {
            parts.push(ProviderContentPart::Text(format!(
                "heartbeat timestamp: {timestamp}"
            )));
            parts.extend(
                attachments
                    .iter()
                    .cloned()
                    .map(ProviderContentPart::Attachment),
            );
        }
        AgentTrigger::Message {
            user_id,
            content,
            attachments,
        } => {
            parts.push(ProviderContentPart::Text(format!("user_id: {user_id}")));
            parts.push(ProviderContentPart::Text(content.clone()));
            parts.extend(
                attachments
                    .iter()
                    .cloned()
                    .map(ProviderContentPart::Attachment),
            );
        }
        AgentTrigger::Approval {
            resume_token,
            decision,
            metadata,
        } => {
            parts.push(ProviderContentPart::Text(format!(
                "approval for resume token {}: {:?}",
                resume_token.as_str(),
                decision
            )));
            parts.push(ProviderContentPart::Json(metadata.clone()));
        }
        AgentTrigger::DelegationResult {
            correlation_id,
            payload,
            metadata,
        } => {
            parts.push(ProviderContentPart::Text(format!(
                "delegation result for correlation {}",
                correlation_id.as_str()
            )));
            parts.push(ProviderContentPart::Json(payload.clone()));
            parts.push(ProviderContentPart::Json(metadata.clone()));
        }
    }
    vec![ProviderMessage {
        role: ProviderRole::User,
        parts,
    }]
}

fn storage_failure_outcome(
    trigger_id: String,
    steps_executed: usize,
    detail: String,
) -> EngineOutcome {
    counter!("rain_engine.storage_failures_total").increment(1);
    EngineOutcome {
        trigger_id,
        stop_reason: StopReason::StorageFailure,
        response: None,
        detail: Some(detail),
        steps_executed,
        idempotent_replay: false,
        resume_token: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AttachmentRef, InMemoryMemoryStore, MockLlmProvider, ProviderCacheRecord, ProviderDecision,
        ProviderError, ProviderErrorKind, ProviderUsageRecord, RecordPageQuery, ResourcePolicy,
        SessionListQuery, SessionSnapshot, SkillCapability, StopReason,
    };
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Clone)]
    struct StubSkillExecutor {
        name: &'static str,
        responder: Arc<
            dyn Fn(SkillInvocation) -> Result<serde_json::Value, SkillExecutionError> + Send + Sync,
        >,
    }

    #[async_trait]
    impl SkillExecutor for StubSkillExecutor {
        async fn execute(
            &self,
            invocation: SkillInvocation,
        ) -> Result<serde_json::Value, SkillExecutionError> {
            (self.responder)(invocation)
        }

        fn executor_kind(&self) -> &'static str {
            self.name
        }
    }

    #[derive(Clone)]
    struct StubNativeSkill {
        requires_approval: bool,
        responder: Arc<
            dyn Fn(SkillInvocation) -> Result<serde_json::Value, SkillExecutionError> + Send + Sync,
        >,
    }

    #[async_trait]
    impl NativeSkill for StubNativeSkill {
        async fn execute(
            &self,
            invocation: SkillInvocation,
        ) -> Result<serde_json::Value, SkillExecutionError> {
            (self.responder)(invocation)
        }

        fn requires_human_approval(&self) -> bool {
            self.requires_approval
        }
    }

    fn manifest(name: &str, scopes: &[&str]) -> SkillManifest {
        SkillManifest {
            name: name.to_string(),
            description: format!("{name} description"),
            input_schema: json!({"type": "object"}),
            required_scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
            capability_grants: vec![SkillCapability::StructuredLog],
            resource_policy: ResourcePolicy::default_for_tools(),
            approval_required: false,
        }
    }

    fn message_trigger(content: &str) -> AgentTrigger {
        AgentTrigger::Message {
            user_id: "u1".to_string(),
            content: content.to_string(),
            attachments: Vec::new(),
        }
    }

    async fn session(store: &Arc<InMemoryMemoryStore>, session_id: &str) -> SessionSnapshot {
        store
            .load_session(session_id)
            .await
            .expect("session snapshot")
    }

    #[tokio::test]
    async fn webhook_trigger_with_attachment_responds() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
            content: "done".to_string(),
        }]));
        let engine = AgentEngine::new(llm, store.clone());

        let outcome = engine
            .process_trigger(ProcessRequest::new(
                "session-1",
                AgentTrigger::Webhook {
                    source: "github".to_string(),
                    payload: json!({"issue": 42}),
                    attachments: vec![AttachmentRef::inline(
                        "att-1",
                        "image/png",
                        Some("schema.png".to_string()),
                        vec![1, 2, 3],
                    )],
                },
            ))
            .await
            .expect("outcome");

        assert_eq!(outcome.stop_reason, StopReason::Responded);
        let snapshot = session(&store, "session-1").await;
        assert!(matches!(
            snapshot.records.first(),
            Some(SessionRecord::Trigger(_))
        ));
    }

    #[tokio::test]
    async fn duplicate_idempotency_key_reuses_prior_outcome() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
            content: "first".to_string(),
        }]));
        let engine = AgentEngine::new(llm.clone(), store.clone());
        let request = ProcessRequest::new(
            "idempotent-session",
            AgentTrigger::Webhook {
                source: "github".to_string(),
                payload: json!({"action": "sync"}),
                attachments: Vec::new(),
            },
        )
        .with_idempotency_key("abc");
        let first = engine
            .process_trigger(request.clone())
            .await
            .expect("first");
        let second = engine.process_trigger(request).await.expect("second");
        assert_eq!(first.response, second.response);
        assert!(second.idempotent_replay);
        assert_eq!(llm.observed_inputs().len(), 1);
    }

    #[tokio::test]
    async fn parallel_tool_calls_execute_and_aggregate() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::dynamic(|input| {
            if input.context.prior_tool_results.is_empty() {
                Ok(ProviderDecision {
                    action: AgentAction::CallSkills(vec![
                        PlannedSkillCall {
                            call_id: "call-1".to_string(),
                            name: "first".to_string(),
                            args: json!({"value": 1}),
                        },
                        PlannedSkillCall {
                            call_id: "call-2".to_string(),
                            name: "second".to_string(),
                            args: json!({"value": 2}),
                        },
                    ]),
                    usage: None,
                    cache: None,
                })
            } else {
                Ok(ProviderDecision {
                    action: AgentAction::Respond {
                        content: "complete".to_string(),
                    },
                    usage: None,
                    cache: None,
                })
            }
        }));
        let engine = AgentEngine::new(llm, store.clone());
        let order = Arc::new(Mutex::new(Vec::<String>::new()));

        for skill_name in ["first", "second"] {
            let local = order.clone();
            engine
                .register_wasm_skill(
                    manifest(skill_name, &["tool:run"]),
                    Arc::new(StubSkillExecutor {
                        name: "stub",
                        responder: Arc::new(move |invocation| {
                            local
                                .lock()
                                .expect("order lock")
                                .push(invocation.call_id.clone());
                            Ok(json!({"echo": invocation.args}))
                        }),
                    }),
                )
                .await;
        }

        let outcome = engine
            .process_trigger(
                ProcessRequest::new("session-2", message_trigger("run")).with_scope("tool:run"),
            )
            .await
            .expect("outcome");

        assert_eq!(outcome.stop_reason, StopReason::Responded);
        let snapshot = session(&store, "session-2").await;
        let tool_results = snapshot.tool_results();
        assert_eq!(tool_results.len(), 2);
        assert_eq!(order.lock().expect("order lock").len(), 2);
    }

    #[tokio::test]
    async fn provider_metadata_records_are_persisted() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::dynamic(|_| {
            Ok(ProviderDecision {
                action: AgentAction::Yield {
                    reason: Some("done".to_string()),
                },
                usage: Some(ProviderUsageRecord {
                    provider_name: "gemini".to_string(),
                    recorded_at: SystemTime::now(),
                    input_tokens: 100,
                    output_tokens: 20,
                    estimated_cost_usd: 0.25,
                    cached_content_id: Some("cache-1".to_string()),
                }),
                cache: Some(ProviderCacheRecord {
                    provider_name: "gemini".to_string(),
                    cached_content_id: "cache-1".to_string(),
                    token_count: 45_000,
                    cached_at: SystemTime::now(),
                }),
            })
        }));
        let engine = AgentEngine::new(llm, store.clone());

        let outcome = engine
            .process_trigger(ProcessRequest::new("session-usage", message_trigger("hi")))
            .await
            .expect("outcome");
        assert_eq!(outcome.stop_reason, StopReason::Yielded);

        let snapshot = session(&store, "session-usage").await;
        assert!(
            snapshot
                .records
                .iter()
                .any(|record| matches!(record, SessionRecord::ProviderUsage(_)))
        );
        assert!(
            snapshot
                .records
                .iter()
                .any(|record| matches!(record, SessionRecord::ProviderCache(_)))
        );
    }

    #[tokio::test]
    async fn approval_trigger_resumes_native_skill() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::dynamic(|input| {
            if input
                .context
                .history
                .iter()
                .any(|record| matches!(record, SessionRecord::ToolResult(_)))
            {
                Ok(ProviderDecision {
                    action: AgentAction::Respond {
                        content: "approved-run".to_string(),
                    },
                    usage: None,
                    cache: None,
                })
            } else {
                Ok(ProviderDecision {
                    action: AgentAction::CallSkills(vec![PlannedSkillCall {
                        call_id: "native-1".to_string(),
                        name: "db_fix".to_string(),
                        args: json!({"apply": true}),
                    }]),
                    usage: None,
                    cache: None,
                })
            }
        }));
        let engine = AgentEngine::new(llm, store.clone());
        engine
            .register_native_skill(
                SkillManifest {
                    approval_required: true,
                    ..manifest("db_fix", &["db:write"])
                },
                Arc::new(StubNativeSkill {
                    requires_approval: true,
                    responder: Arc::new(|_| Ok(json!({"status": "fixed"}))),
                }),
            )
            .await;

        let suspended = engine
            .process_trigger(
                ProcessRequest::new("approval-session", message_trigger("fix"))
                    .with_scope("db:write"),
            )
            .await
            .expect("suspended");
        assert_eq!(suspended.stop_reason, StopReason::Suspended);
        let token = suspended.resume_token.expect("resume token");

        let resumed = engine
            .process_trigger(ProcessRequest::new(
                "approval-session",
                AgentTrigger::Approval {
                    resume_token: token,
                    decision: ApprovalDecision::Approved,
                    metadata: json!({"approved_by": "human"}),
                },
            ))
            .await
            .expect("resumed");
        assert_eq!(resumed.stop_reason, StopReason::Responded);
        assert_eq!(resumed.response.as_deref(), Some("approved-run"));
    }

    #[tokio::test]
    async fn provider_failure_stops_with_explicit_reason() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::dynamic(|_| {
            Err(ProviderError::new(
                ProviderErrorKind::Transport,
                "upstream unavailable",
                true,
            ))
        }));
        let engine = AgentEngine::new(llm, store);

        let outcome = engine
            .process_trigger(ProcessRequest::new(
                "provider-failure",
                message_trigger("hello"),
            ))
            .await
            .expect("outcome");

        assert_eq!(outcome.stop_reason, StopReason::ProviderFailure);
    }

    #[tokio::test]
    async fn list_sessions_and_record_pages_work() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
            content: "ok".to_string(),
        }]));
        let engine = AgentEngine::new(llm, store.clone());
        for session_id in ["a", "b"] {
            engine
                .process_trigger(ProcessRequest::new(session_id, message_trigger("x")))
                .await
                .expect("outcome");
        }

        let sessions = store
            .list_sessions(SessionListQuery::default())
            .await
            .expect("sessions");
        assert_eq!(sessions.len(), 2);
        let page = store
            .list_records(RecordPageQuery::new("a"))
            .await
            .expect("page");
        assert!(!page.records.is_empty());
    }
}
