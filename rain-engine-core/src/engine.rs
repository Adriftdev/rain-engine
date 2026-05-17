use crate::{
    AdvanceRequest, AdvanceResult, AgentAction, AgentContext, AgentContextSnapshot,
    AgentStateDelta, AgentTrigger, ApprovalDecision, ApprovalResolutionRecord, ContinueRequest,
    DelegationRecord, DeliberationOutcome, DeliberationRecord, EngineOutcome, EnginePolicy,
    ExecutionMetadata, ExecutionPlanRecord, KernelEvent, KernelEventRecord, LlmProvider,
    MemoryError, MemoryStore, MemoryStoreExt, ModelDecisionRecord, OutcomeRecord,
    PendingApprovalRecord, PlannedSkillCall, Planner, PolicyOverlay, PolicyOverlayPatch,
    PolicyOverlayStatus, PolicyTuningAction, PolicyTuningRecord, ProcessRequest,
    ProfilePatchRecord, ProviderContentPart, ProviderDecision, ProviderMessage, ProviderRequest,
    ProviderRequestConfig, ProviderRole, ReflectionRecord, ResumeToken, RetryPolicy,
    SelfImprovementMode, SessionRecord, SessionSnapshot, SkillBackendKind, SkillDefinition,
    SkillFailure, SkillFailureKind, SkillInputValidationRecord, SkillInvocation, SkillManifest,
    SkillStore, StopReason, StrategyPreferenceRecord, SummaryRecord, SuspendReason, ToolCallRecord,
    ToolDependency, ToolExecutionGraph, ToolNode, ToolNodeCheckpointRecord, ToolNodeStatus,
    ToolPerformanceRecord, ToolResultRecord, TriggerIntentRecord, TriggerRecord, WakeRequestRecord,
};
use async_trait::async_trait;
use dashmap::DashMap;
use metrics::{counter, gauge, histogram};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use thiserror::Error;
use tokio::task::JoinSet;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("memory error: {0}")]
    Memory(#[from] MemoryError),
    #[error("blob error: {0}")]
    Blob(String),
    #[error("provider error: {0}")]
    Provider(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineErrorKind {
    Storage,
    Blob,
    Join,
    Provider,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineErrorSeverity {
    Recoverable,
    Fatal,
}

impl EngineError {
    pub fn kind(&self) -> EngineErrorKind {
        match self {
            EngineError::Memory(_) => EngineErrorKind::Storage,
            EngineError::Blob(_) => EngineErrorKind::Blob,
            EngineError::Provider(_) => EngineErrorKind::Provider,
        }
    }

    pub fn severity(&self) -> EngineErrorSeverity {
        match self {
            EngineError::Memory(_) => EngineErrorSeverity::Fatal,
            EngineError::Blob(_) => EngineErrorSeverity::Recoverable,
            EngineError::Provider(_) => EngineErrorSeverity::Recoverable,
        }
    }

    pub fn is_recoverable(&self) -> bool {
        self.severity() == EngineErrorSeverity::Recoverable
    }
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
pub(crate) enum RegisteredSkillBackend {
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
pub(crate) struct RegisteredSkill {
    pub(crate) manifest: SkillManifest,
    pub(crate) backend: RegisteredSkillBackend,
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
    skill_store: Option<Arc<dyn SkillStore>>,
    skills: Arc<DashMap<String, RegisteredSkill>>,
    planner: Option<Arc<dyn Planner>>,
}

impl AgentEngine {
    pub fn new(llm: Arc<dyn LlmProvider>, memory: Arc<dyn MemoryStore>) -> Self {
        Self {
            llm,
            memory,
            skill_store: None,
            skills: Arc::new(DashMap::new()),
            planner: None,
        }
    }

    pub fn with_skill_store(mut self, skill_store: Arc<dyn SkillStore>) -> Self {
        self.skill_store = Some(skill_store);
        self
    }

    pub fn with_planner(mut self, planner: Arc<dyn Planner>) -> Self {
        self.planner = Some(planner);
        self
    }

    pub fn register_native_skill(&self, manifest: SkillManifest, skill: Arc<dyn NativeSkill>) {
        self.skills.insert(
            manifest.name.clone(),
            RegisteredSkill {
                manifest,
                backend: RegisteredSkillBackend::Native(skill),
            },
        );
    }

    pub fn register_wasm_skill(&self, manifest: SkillManifest, executor: Arc<dyn SkillExecutor>) {
        self.skills.insert(
            manifest.name.clone(),
            RegisteredSkill {
                manifest,
                backend: RegisteredSkillBackend::Wasm(executor),
            },
        );
    }

    pub async fn register_wasm_skill_persistent(
        &self,
        manifest: SkillManifest,
        executor: Arc<dyn SkillExecutor>,
        wasm_bytes: Vec<u8>,
    ) -> Result<(), String> {
        if let Some(store) = &self.skill_store {
            store.store_skill(manifest.clone(), wasm_bytes).await?;
        }
        self.register_wasm_skill(manifest, executor);
        Ok(())
    }

    pub async fn advance(&self, request: AdvanceRequest) -> Result<AdvanceResult, EngineError> {
        match request {
            AdvanceRequest::Trigger(request) => self.advance_trigger(request).await,
            AdvanceRequest::Continue(request) => self.advance_continue(request).await,
        }
    }

    pub async fn skill_definitions(&self) -> Vec<SkillDefinition> {
        let mut definitions = self
            .skills
            .iter()
            .map(|entry| entry.value().definition())
            .collect::<Vec<_>>();
        definitions.sort_by(|left, right| left.manifest.name.cmp(&right.manifest.name));
        definitions
    }

    async fn advance_trigger(&self, request: ProcessRequest) -> Result<AdvanceResult, EngineError> {
        let started_at = SystemTime::now();
        let trigger_id = Uuid::new_v4().to_string();

        if let Some(idempotency_key) = request.idempotency_key.as_deref()
            && let Ok(Some(mut prior_outcome)) = self
                .memory
                .find_outcome_by_idempotency_key(&request.session_id, idempotency_key)
                .await
        {
            prior_outcome.idempotent_replay = true;
            counter!("rain_engine.idempotent_replay_total").increment(1);
            return Ok(AdvanceResult {
                outcome: Some(prior_outcome),
                emitted_events: Vec::new(),
                state_delta: AgentStateDelta::default(),
                wake_request: None,
            });
        }

        let trigger_record = TriggerRecord {
            trigger_id: trigger_id.clone(),
            session_id: request.session_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            recorded_at: started_at,
            trigger: request.trigger.clone(),
            intent: None,
        };
        if let Err(err) = self.memory.append_trigger(trigger_record).await {
            return Ok(AdvanceResult {
                outcome: Some(storage_failure_outcome(trigger_id, 0, err.message)),
                emitted_events: Vec::new(),
                state_delta: AgentStateDelta::default(),
                wake_request: None,
            });
        }

        let mut snapshot = match self.memory.load_session(&request.session_id).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                return Ok(AdvanceResult {
                    outcome: Some(storage_failure_outcome(trigger_id, 0, err.message)),
                    emitted_events: Vec::new(),
                    state_delta: AgentStateDelta::default(),
                    wake_request: None,
                });
            }
        };
        counter!("rain_engine.triggers_total").increment(1);

        // Intent is classified deterministically so one advance step still asks
        // the provider at most once.
        let _ = self
            .memory
            .append_trigger_intent(
                &request.session_id,
                TriggerIntentRecord {
                    trigger_id: trigger_id.clone(),
                    classified_at: SystemTime::now(),
                    intent: classify_trigger_intent(&request.trigger),
                },
            )
            .await;
        if let Ok(refreshed) = self.memory.load_session(&request.session_id).await {
            snapshot = refreshed;
        }

        // Invoke planner if available to handle goal decomposition or task adjustment
        if let Some(planner) = &self.planner {
            let output = planner
                .plan(&snapshot.agent_state(), &request.trigger)
                .await;
            let mut changed = false;
            if !output.events.is_empty() {
                for event in output.events {
                    let _ = self
                        .memory
                        .append_kernel_event(
                            &request.session_id,
                            KernelEventRecord {
                                event_id: Uuid::new_v4().to_string(),
                                occurred_at: SystemTime::now(),
                                event,
                            },
                        )
                        .await;
                }
                changed = true;
            }
            if let Some(plan) = output.proposed_plan {
                let _ = self
                    .memory
                    .append_execution_plan(
                        &request.session_id,
                        ExecutionPlanRecord {
                            plan_id: format!("plan-{}", Uuid::new_v4()),
                            created_at: SystemTime::now(),
                            objective: plan.objective,
                            steps: plan.steps,
                            current_step_index: 0,
                            completed_at: None,
                        },
                    )
                    .await;
                changed = true;
            }
            if changed {
                // Refresh snapshot if planning occurred
                if let Ok(refreshed) = self.memory.load_session(&request.session_id).await {
                    snapshot = refreshed;
                }
            }

            // Trigger history summarization if history is getting long
            if snapshot.records.len() > 10
                && !snapshot
                    .records
                    .iter()
                    .any(|r| matches!(r, SessionRecord::Summary(_)))
                && let Ok(summary) = self
                    .summarize_history(&snapshot, &request.policy, &request.trigger)
                    .await
            {
                let _ = self
                    .memory
                    .append_summary(&request.session_id, summary)
                    .await;
                // Refresh snapshot again to include summary
                if let Ok(refreshed) = self.memory.load_session(&request.session_id).await {
                    snapshot = refreshed;
                }
            }
        }
        let effective_policy = request
            .policy
            .clone()
            .with_overlay(snapshot.active_policy_overlay());
        let deadline = Instant::now() + effective_policy.max_execution_time();
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
                policy: effective_policy,
                provider: request.provider.clone(),
                cancellation: request.cancellation.clone(),
            },
        };

        counter!("rain_engine.triggers_total").increment(1);
        info!(session_id = %context.session_id, trigger_id = %trigger_id, "processing trigger");

        let emitted_events =
            derive_trigger_kernel_events(&context.metadata.trigger_id, &request.trigger);
        self.persist_kernel_events(&mut context, &emitted_events)
            .await?;

        let mut steps_executed = snapshot.current_step_count();
        let mut consecutive_tool_failure_steps = snapshot.current_consecutive_tool_failure_steps();

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
                    let outcome = self
                        .finish(
                            &mut context,
                            StopReason::PolicyAborted,
                            None,
                            Some("resume token not found".to_string()),
                            0,
                            None,
                        )
                        .await?;
                    return Ok(build_advance_result(outcome, emitted_events));
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
                        let outcome = self
                            .finish(
                                &mut context,
                                StopReason::PolicyAborted,
                                None,
                                Some("approval resume unexpectedly suspended".to_string()),
                                pending.step,
                                None,
                            )
                            .await?;
                        return Ok(build_advance_result(outcome, emitted_events));
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
                consecutive_tool_failure_steps += 1;
            }
        }

        self.perform_single_step(
            context,
            request.trigger,
            steps_executed,
            consecutive_tool_failure_steps,
            emitted_events,
        )
        .await
    }

    async fn advance_continue(
        &self,
        request: ContinueRequest,
    ) -> Result<AdvanceResult, EngineError> {
        let snapshot = match self.memory.load_session(&request.session_id).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                return Ok(AdvanceResult {
                    outcome: Some(storage_failure_outcome(
                        request.session_id.clone(),
                        0,
                        err.message,
                    )),
                    emitted_events: Vec::new(),
                    state_delta: AgentStateDelta::default(),
                    wake_request: None,
                });
            }
        };

        let Some(active_trigger) = snapshot.active_trigger() else {
            return Ok(AdvanceResult {
                outcome: Some(EngineOutcome {
                    trigger_id: Uuid::new_v4().to_string(),
                    stop_reason: StopReason::Yielded,
                    response: None,
                    detail: Some("no active trigger to continue".to_string()),
                    steps_executed: 0,
                    idempotent_replay: false,
                    resume_token: None,
                }),
                emitted_events: Vec::new(),
                state_delta: AgentStateDelta::default(),
                wake_request: None,
            });
        };

        let trigger_id = active_trigger.trigger_id.clone();
        let started_at = SystemTime::now();
        let effective_policy = request
            .policy
            .clone()
            .with_overlay(snapshot.active_policy_overlay());
        let deadline = Instant::now() + effective_policy.max_execution_time();
        let mut context = AgentContext {
            session_id: request.session_id.clone(),
            records: snapshot.records.clone(),
            prior_tool_results: snapshot.tool_results(),
            granted_scopes: request.granted_scopes.clone(),
            metadata: ExecutionMetadata {
                trigger_id,
                idempotency_key: active_trigger.idempotency_key.clone(),
                started_at,
                deadline,
                policy: effective_policy,
                provider: request.provider.clone(),
                cancellation: request.cancellation.clone(),
            },
        };

        if let Some(graph) = snapshot.active_tool_execution_graph() {
            let calls = graph
                .nodes
                .iter()
                .map(|node| PlannedSkillCall {
                    call_id: node.call_id.clone(),
                    name: node.skill_name.clone(),
                    args: node.args.clone(),
                    priority: node.priority,
                    depends_on: node
                        .dependencies
                        .iter()
                        .map(|dependency| dependency.call_id.clone())
                        .collect(),
                    retry_policy: node.retry_policy.clone(),
                    dry_run: node.dry_run,
                })
                .collect::<Vec<_>>();
            match self
                .execute_planned_calls(&context, graph.step, calls, true)
                .await?
            {
                BatchExecution::Executed(batch) => {
                    for result in batch.results {
                        self.memory
                            .append_tool_result(&context.session_id, result.clone())
                            .await?;
                        context.prior_tool_results.push(result.clone());
                        context.records.push(SessionRecord::ToolResult(result));
                    }
                    return Ok(AdvanceResult {
                        outcome: None,
                        emitted_events: Vec::new(),
                        state_delta: AgentStateDelta::default(),
                        wake_request: None,
                    });
                }
                BatchExecution::Suspended { .. } => {
                    let outcome = self
                        .finish(
                            &mut context,
                            StopReason::PolicyAborted,
                            None,
                            Some("checkpointed graph unexpectedly suspended".to_string()),
                            graph.step,
                            None,
                        )
                        .await?;
                    return Ok(build_advance_result(outcome, Vec::new()));
                }
            }
        }

        self.perform_single_step(
            context,
            active_trigger.trigger,
            snapshot.current_step_count(),
            snapshot.current_consecutive_tool_failure_steps(),
            Vec::new(),
        )
        .await
    }

    #[instrument(
        skip(self, context, trigger, emitted_events),
        fields(
            session_id = %context.session_id,
            trigger_id = %context.metadata.trigger_id,
            step = steps_executed
        )
    )]
    async fn perform_single_step(
        &self,
        mut context: AgentContext,
        trigger: AgentTrigger,
        steps_executed: usize,
        consecutive_tool_failure_steps: usize,
        emitted_events: Vec<KernelEventRecord>,
    ) -> Result<AdvanceResult, EngineError> {
        if let Some(mut plan) = context.active_execution_plan()
            && plan.current_step_index < plan.steps.len()
        {
            let action = plan.steps[plan.current_step_index].clone();
            plan.current_step_index += 1;
            if plan.current_step_index >= plan.steps.len() {
                plan.completed_at = Some(SystemTime::now());
            }
            let _ = self
                .memory
                .append_execution_plan(&context.session_id, plan)
                .await;

            return self
                .execute_action(
                    context,
                    action,
                    steps_executed,
                    consecutive_tool_failure_steps,
                    emitted_events,
                )
                .await;
        }

        if let Some(outcome) = self
            .policy_outcome(&mut context, steps_executed, consecutive_tool_failure_steps)
            .await?
        {
            return Ok(build_advance_result(outcome, emitted_events));
        }

        let available_skills = self
            .skills
            .iter()
            .filter(|skill| {
                skill
                    .value()
                    .manifest
                    .required_scopes
                    .iter()
                    .all(|scope| context.granted_scopes.contains(scope))
            })
            .map(|skill| skill.value().definition())
            .collect::<Vec<_>>();

        let provider_request = ProviderRequest {
            trigger: trigger.clone(),
            context: context.to_snapshot(steps_executed),
            available_skills,
            config: context.metadata.provider.clone(),
            policy: context.metadata.policy.clone(),
            contents: build_provider_contents(&trigger, &context.records),
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
                let outcome = self
                    .finish(
                        &mut context,
                        StopReason::ProviderFailure,
                        None,
                        Some(format!("provider failure: {}", err.message)),
                        steps_executed,
                        None,
                    )
                    .await?;
                return Ok(build_advance_result(outcome, emitted_events));
            }
            Err(_) => {
                warn!(session_id = %context.session_id, "provider timed out");
                let outcome = self
                    .finish(
                        &mut context,
                        StopReason::ProviderFailure,
                        None,
                        Some("provider timeout exceeded".to_string()),
                        steps_executed,
                        None,
                    )
                    .await?;
                return Ok(build_advance_result(outcome, emitted_events));
            }
        };
        histogram!("rain_engine.provider_latency_seconds")
            .record(provider_started.elapsed().as_secs_f64());
        counter!("rain_engine.model_decisions_total", "action" => action_metric_label(&decision.action)).increment(1);

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
            return Ok(AdvanceResult {
                outcome: Some(storage_failure_outcome(
                    context.metadata.trigger_id.clone(),
                    steps_executed,
                    err.message,
                )),
                emitted_events,
                state_delta: AgentStateDelta::default(),
                wake_request: None,
            });
        }
        context
            .records
            .push(SessionRecord::ModelDecision(decision_record));

        self.execute_action(
            context,
            decision.action,
            steps_executed,
            consecutive_tool_failure_steps,
            emitted_events,
        )
        .await
    }

    async fn execute_action(
        &self,
        mut context: AgentContext,
        action: AgentAction,
        steps_executed: usize,
        _consecutive_tool_failure_steps: usize,
        mut emitted_events: Vec<KernelEventRecord>,
    ) -> Result<AdvanceResult, EngineError> {
        match action {
            AgentAction::Plan {
                summary,
                candidate_actions,
                confidence,
            } => {
                let record = DeliberationRecord {
                    deliberation_id: Uuid::new_v4().to_string(),
                    trigger_id: context.metadata.trigger_id.clone(),
                    step: steps_executed,
                    created_at: SystemTime::now(),
                    summary,
                    candidate_actions,
                    confidence,
                    outcome: if confidence >= 0.7 {
                        DeliberationOutcome::ReadyToAct
                    } else {
                        DeliberationOutcome::NeedsRefinement
                    },
                };
                self.memory
                    .append_deliberation(&context.session_id, record.clone())
                    .await?;
                context.records.push(SessionRecord::Deliberation(record));
                Ok(AdvanceResult {
                    outcome: None,
                    emitted_events: emitted_events.clone(),
                    state_delta: derive_state_delta(&emitted_events),
                    wake_request: emitted_events.iter().find_map(extract_wake_request),
                })
            }
            AgentAction::Respond { content } => {
                let outcome = self
                    .finish(
                        &mut context,
                        StopReason::Responded,
                        Some(content),
                        None,
                        steps_executed + 1,
                        None,
                    )
                    .await?;
                Ok(build_advance_result(outcome, emitted_events))
            }
            AgentAction::Yield { reason } => {
                let outcome = self
                    .finish(
                        &mut context,
                        StopReason::Yielded,
                        None,
                        reason,
                        steps_executed + 1,
                        None,
                    )
                    .await?;
                Ok(build_advance_result(outcome, emitted_events))
            }
            AgentAction::Continue { .. } => Ok(AdvanceResult {
                outcome: None,
                emitted_events: emitted_events.clone(),
                state_delta: derive_state_delta(&emitted_events),
                wake_request: emitted_events.iter().find_map(extract_wake_request),
            }),
            AgentAction::Suspend {
                reason,
                pending_calls,
                resume_token,
            } => {
                let outcome = self
                    .suspend(
                        &mut context,
                        steps_executed,
                        reason,
                        pending_calls,
                        resume_token,
                    )
                    .await?;
                Ok(build_advance_result(outcome, emitted_events))
            }
            AgentAction::CallSkills(calls) => match self
                .execute_planned_calls(&context, steps_executed, calls, false)
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
                            return Ok(AdvanceResult {
                                outcome: Some(storage_failure_outcome(
                                    context.metadata.trigger_id.clone(),
                                    steps_executed + 1,
                                    err.message,
                                )),
                                emitted_events,
                                state_delta: AgentStateDelta::default(),
                                wake_request: None,
                            });
                        }
                        context.prior_tool_results.push(result.clone());
                        context.records.push(SessionRecord::ToolResult(result));
                    }
                    let _ = all_failed;
                    Ok(AdvanceResult {
                        outcome: None,
                        emitted_events: emitted_events.clone(),
                        state_delta: derive_state_delta(&emitted_events),
                        wake_request: emitted_events.iter().find_map(extract_wake_request),
                    })
                }
                BatchExecution::Suspended {
                    reason,
                    pending_calls,
                    resume_token,
                } => {
                    let outcome = self
                        .suspend(
                            &mut context,
                            steps_executed,
                            reason,
                            pending_calls,
                            resume_token,
                        )
                        .await?;
                    Ok(build_advance_result(outcome, emitted_events))
                }
            },
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
                context
                    .records
                    .push(SessionRecord::Delegation(record.clone()));
                let event = KernelEventRecord {
                    event_id: format!("delegation-{}", record.correlation_id.as_str()),
                    occurred_at: record.created_at,
                    event: KernelEvent::DelegationRequested(record),
                };
                self.memory
                    .append_kernel_event(&context.session_id, event.clone())
                    .await?;
                context
                    .records
                    .push(SessionRecord::KernelEvent(event.clone()));
                emitted_events.push(event);
                let outcome = self
                    .finish(
                        &mut context,
                        StopReason::Delegated,
                        None,
                        Some("delegated to downstream worker".to_string()),
                        steps_executed + 1,
                        Some(resume_token),
                    )
                    .await?;
                Ok(build_advance_result(outcome, emitted_events))
            }
        }
    }

    async fn policy_outcome(
        &self,
        context: &mut AgentContext,
        steps_executed: usize,
        consecutive_tool_failure_steps: usize,
    ) -> Result<Option<EngineOutcome>, EngineError> {
        if context.metadata.cancellation.is_cancelled() {
            return self
                .finish(
                    context,
                    StopReason::Cancelled,
                    None,
                    Some("execution cancelled".to_string()),
                    steps_executed,
                    None,
                )
                .await
                .map(Some);
        }

        if Instant::now() >= context.metadata.deadline {
            return self
                .finish(
                    context,
                    StopReason::DeadlineExceeded,
                    None,
                    Some("engine execution deadline exceeded".to_string()),
                    steps_executed,
                    None,
                )
                .await
                .map(Some);
        }

        if steps_executed >= context.metadata.policy.max_steps {
            return self
                .finish(
                    context,
                    StopReason::MaxStepsReached,
                    None,
                    Some("max steps reached".to_string()),
                    steps_executed,
                    None,
                )
                .await
                .map(Some);
        }

        if consecutive_tool_failure_steps >= context.metadata.policy.max_consecutive_tool_failures {
            return self
                .finish(
                    context,
                    StopReason::PolicyAborted,
                    None,
                    Some("max consecutive tool failure steps reached".to_string()),
                    steps_executed,
                    None,
                )
                .await
                .map(Some);
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
                    context,
                    StopReason::PolicyAborted,
                    None,
                    Some("session cost limit reached".to_string()),
                    steps_executed,
                    None,
                )
                .await
                .map(Some);
        }

        Ok(None)
    }

    async fn persist_kernel_events(
        &self,
        context: &mut AgentContext,
        events: &[KernelEventRecord],
    ) -> Result<(), EngineError> {
        for event in events {
            self.memory
                .append_kernel_event(&context.session_id, event.clone())
                .await?;
            context
                .records
                .push(SessionRecord::KernelEvent(event.clone()));
        }
        Ok(())
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

    #[instrument(
        skip(self, context, calls),
        fields(
            session_id = %context.session_id,
            trigger_id = %context.metadata.trigger_id,
            step,
            call_count = calls.len(),
            max_parallel = context.metadata.policy.max_parallel_skill_calls
        )
    )]
    async fn execute_planned_calls(
        &self,
        context: &AgentContext,
        step: usize,
        calls: Vec<PlannedSkillCall>,
        approval_override: bool,
    ) -> Result<BatchExecution, EngineError> {
        if !approval_override {
            let approval_calls = calls
                .iter()
                .filter_map(|call| {
                    let skill = self.skills.get(&call.name)?;
                    skill
                        .backend
                        .requires_human_approval()
                        .then_some(call.name.clone())
                })
                .collect::<Vec<_>>();
            if !approval_calls.is_empty() {
                counter!("rain_engine.approval_suspensions_total").increment(1);
                return Ok(BatchExecution::Suspended {
                    reason: SuspendReason::HumanApprovalRequired {
                        skill_names: approval_calls,
                    },
                    pending_calls: calls,
                    resume_token: ResumeToken(Uuid::new_v4().to_string()),
                });
            }
        }

        let graph = existing_or_new_graph(context, step, &calls);
        if !context.records.iter().any(|record| {
            matches!(
                record,
                SessionRecord::ToolExecutionGraph(existing) if existing.graph_id == graph.graph_id
            )
        }) {
            self.memory
                .append_tool_execution_graph(&context.session_id, graph.clone())
                .await?;
            for node in &graph.nodes {
                self.append_tool_checkpoint(context, &graph, node, ToolNodeStatus::Queued, 0, None)
                    .await?;
            }
        }

        let mut status_by_call = latest_tool_statuses(context, &graph.graph_id);
        let mut attempts_by_call = started_attempt_counts(context, &graph.graph_id);
        let mut results_by_call = context
            .records
            .iter()
            .filter_map(|record| match record {
                SessionRecord::ToolResult(result) => Some((result.call_id.clone(), result.clone())),
                _ => None,
            })
            .collect::<HashMap<_, _>>();
        let mut new_results = Vec::<ToolResultRecord>::new();
        let max_parallel = context
            .metadata
            .policy
            .max_parallel_skill_calls
            .max(1)
            .min(context.metadata.policy.max_ready_tool_nodes.max(1));
        gauge!("rain_engine.registered_skills").set(self.skills.len() as f64);

        loop {
            let skipped = self
                .skip_blocked_nodes(
                    context,
                    &graph,
                    &mut status_by_call,
                    &mut results_by_call,
                    &mut new_results,
                    step,
                )
                .await?;
            let ready = ready_nodes(&graph, &status_by_call)
                .into_iter()
                .take(context.metadata.policy.max_ready_tool_nodes.max(1))
                .collect::<Vec<_>>();
            if ready.is_empty() {
                if !skipped {
                    break;
                }
                continue;
            }

            let mut join_set = JoinSet::new();
            for node in ready.into_iter().take(max_parallel) {
                let prepared = self
                    .prepare_node(context, &graph, &node, step, &mut attempts_by_call)
                    .await?;
                match prepared {
                    PreparedNode::Executable(prepared) => {
                        join_set.spawn(run_prepared_call(*prepared));
                    }
                    PreparedNode::Immediate(result) => {
                        let status = final_status_for_result(&result);
                        status_by_call.insert(node.call_id.clone(), status);
                        results_by_call.insert(result.call_id.clone(), result.clone());
                        new_results.push(result);
                    }
                }
            }

            while let Some(joined) = join_set.join_next().await {
                let result = joined.map_err(|err| EngineError::Blob(err.to_string()))??;
                let Some(node) = graph
                    .nodes
                    .iter()
                    .find(|node| node.call_id == result.call_id)
                else {
                    continue;
                };
                let status = final_status_for_result(&result);
                self.append_tool_checkpoint(
                    context,
                    &graph,
                    node,
                    status.clone(),
                    *attempts_by_call.get(&node.call_id).unwrap_or(&1),
                    result_detail(&result),
                )
                .await?;
                status_by_call.insert(node.call_id.clone(), status);
                results_by_call.insert(result.call_id.clone(), result.clone());
                new_results.push(result);
            }
        }

        let ordered = graph
            .nodes
            .iter()
            .filter_map(|node| {
                new_results
                    .iter()
                    .find(|result| result.call_id == node.call_id)
                    .cloned()
            })
            .collect::<Vec<_>>();
        let any_success = ordered.iter().any(|result| result.output.is_ok());
        Ok(BatchExecution::Executed(ExecutedBatch {
            results: ordered,
            all_failed: !any_success,
        }))
    }

    async fn append_tool_checkpoint(
        &self,
        context: &AgentContext,
        graph: &ToolExecutionGraph,
        node: &ToolNode,
        status: ToolNodeStatus,
        attempt: usize,
        detail: Option<String>,
    ) -> Result<(), EngineError> {
        let record = ToolNodeCheckpointRecord {
            checkpoint_id: Uuid::new_v4().to_string(),
            graph_id: graph.graph_id.clone(),
            call_id: node.call_id.clone(),
            skill_name: node.skill_name.clone(),
            step: graph.step,
            status,
            attempt,
            occurred_at: SystemTime::now(),
            detail,
        };
        self.memory
            .append_tool_node_checkpoint(&context.session_id, record)
            .await?;
        Ok(())
    }

    async fn skip_blocked_nodes(
        &self,
        context: &AgentContext,
        graph: &ToolExecutionGraph,
        status_by_call: &mut HashMap<String, ToolNodeStatus>,
        results_by_call: &mut HashMap<String, ToolResultRecord>,
        new_results: &mut Vec<ToolResultRecord>,
        step: usize,
    ) -> Result<bool, EngineError> {
        let mut changed = false;
        for node in &graph.nodes {
            if is_terminal_status(status_by_call.get(&node.call_id)) {
                continue;
            }
            let blocked_by = node.dependencies.iter().find(|dependency| {
                matches!(
                    status_by_call.get(&dependency.call_id),
                    Some(ToolNodeStatus::Failed)
                        | Some(ToolNodeStatus::Skipped)
                        | Some(ToolNodeStatus::TimedOut)
                )
            });
            if let Some(blocked_by) = blocked_by {
                let message = format!("dependency `{}` did not succeed", blocked_by.call_id);
                let result = self.error_result(
                    node.call_id.clone(),
                    node.skill_name.clone(),
                    SkillFailureKind::Internal,
                    message.clone(),
                );
                self.append_tool_checkpoint(
                    context,
                    graph,
                    node,
                    ToolNodeStatus::Skipped,
                    0,
                    Some(message),
                )
                .await?;
                status_by_call.insert(node.call_id.clone(), ToolNodeStatus::Skipped);
                results_by_call.insert(node.call_id.clone(), result.clone());
                new_results.push(result);
                let _ = step;
                changed = true;
            }
        }
        Ok(changed)
    }

    async fn prepare_node(
        &self,
        context: &AgentContext,
        graph: &ToolExecutionGraph,
        node: &ToolNode,
        step: usize,
        attempts_by_call: &mut HashMap<String, usize>,
    ) -> Result<PreparedNode, EngineError> {
        let Some(skill) = self
            .skills
            .get(&node.skill_name)
            .map(|entry| entry.value().clone())
        else {
            self.append_validation(
                context,
                graph,
                node,
                false,
                vec![format!("skill `{}` is not registered", node.skill_name)],
            )
            .await?;
            self.append_tool_checkpoint(
                context,
                graph,
                node,
                ToolNodeStatus::Failed,
                0,
                Some(format!("skill `{}` is not registered", node.skill_name)),
            )
            .await?;
            return Ok(PreparedNode::Immediate(self.error_result(
                node.call_id.clone(),
                node.skill_name.clone(),
                SkillFailureKind::Internal,
                format!("skill `{}` is not registered", node.skill_name),
            )));
        };

        if context.metadata.policy.validate_tool_args {
            let errors = validate_against_schema(&node.args, &skill.manifest.input_schema);
            self.append_validation(context, graph, node, errors.is_empty(), errors.clone())
                .await?;
            if !errors.is_empty() {
                let message = errors.join("; ");
                self.append_tool_checkpoint(
                    context,
                    graph,
                    node,
                    ToolNodeStatus::Failed,
                    0,
                    Some(message.clone()),
                )
                .await?;
                return Ok(PreparedNode::Immediate(self.error_result(
                    node.call_id.clone(),
                    node.skill_name.clone(),
                    SkillFailureKind::InvalidArguments,
                    message,
                )));
            }
        }
        self.append_tool_checkpoint(context, graph, node, ToolNodeStatus::Validated, 0, None)
            .await?;

        if !skill
            .manifest
            .required_scopes
            .iter()
            .all(|scope| context.granted_scopes.contains(scope))
        {
            counter!("rain_engine.permission_denials_total").increment(1);
            self.append_tool_checkpoint(
                context,
                graph,
                node,
                ToolNodeStatus::Failed,
                0,
                Some(format!(
                    "missing required scopes for skill `{}`",
                    node.skill_name
                )),
            )
            .await?;
            return Ok(PreparedNode::Immediate(self.error_result(
                node.call_id.clone(),
                node.skill_name.clone(),
                SkillFailureKind::PermissionDenied,
                format!("missing required scopes for skill `{}`", node.skill_name),
            )));
        }

        if matches!(skill.backend, RegisteredSkillBackend::Native(_))
            && !context.metadata.policy.allow_native_skills
        {
            self.append_tool_checkpoint(
                context,
                graph,
                node,
                ToolNodeStatus::Failed,
                0,
                Some("native skills are disabled by policy".to_string()),
            )
            .await?;
            return Ok(PreparedNode::Immediate(self.error_result(
                node.call_id.clone(),
                node.skill_name.clone(),
                SkillFailureKind::PermissionDenied,
                "native skills are disabled by policy".to_string(),
            )));
        }

        let mut manifest = skill.manifest.clone();
        manifest.resource_policy = manifest.effective_resource_policy(&context.metadata.policy);
        if node.dry_run
            && (!context.metadata.policy.enable_tool_dry_run
                || !manifest.resource_policy.dry_run_supported)
        {
            self.append_tool_checkpoint(
                context,
                graph,
                node,
                ToolNodeStatus::Failed,
                0,
                Some("dry-run execution is not enabled for this skill".to_string()),
            )
            .await?;
            return Ok(PreparedNode::Immediate(self.error_result(
                node.call_id.clone(),
                node.skill_name.clone(),
                SkillFailureKind::CapabilityDenied,
                "dry-run execution is not enabled for this skill".to_string(),
            )));
        }

        if self.is_skill_circuit_broken(&node.skill_name, context) {
            counter!("rain_engine.circuit_breaker_trips_total", "skill" => node.skill_name.clone())
                .increment(1);
            self.append_tool_checkpoint(
                context,
                graph,
                node,
                ToolNodeStatus::Failed,
                0,
                Some(format!(
                    "circuit breaker tripped for skill `{}`",
                    node.skill_name
                )),
            )
            .await?;
            return Ok(PreparedNode::Immediate(self.error_result(
                node.call_id.clone(),
                node.skill_name.clone(),
                SkillFailureKind::CapabilityDenied,
                format!("circuit breaker tripped for skill `{}`", node.skill_name),
            )));
        }

        let attempt = attempts_by_call.entry(node.call_id.clone()).or_insert(0);
        *attempt += 1;
        self.append_tool_checkpoint(
            context,
            graph,
            node,
            ToolNodeStatus::Started,
            *attempt,
            None,
        )
        .await?;

        let call_record = ToolCallRecord {
            call_id: node.call_id.clone(),
            step,
            called_at: SystemTime::now(),
            skill_name: skill.manifest.name.clone(),
            args: node.args.clone(),
            backend_kind: skill.backend.kind(),
        };
        self.memory
            .append_tool_call(&context.session_id, call_record)
            .await?;
        counter!(
            "rain_engine.tool_calls_total",
            "skill" => skill.manifest.name.clone(),
            "backend" => format!("{:?}", skill.backend.kind())
        )
        .increment(1);

        let mut retry_policy = node.retry_policy.policy.clone();
        retry_policy.max_attempts = retry_policy
            .max_attempts
            .min(manifest.resource_policy.retry_policy.max_attempts)
            .min(context.metadata.policy.max_tool_retries_per_step);

        Ok(PreparedNode::Executable(Box::new(PreparedCall {
            call_id: node.call_id.clone(),
            name: node.skill_name.clone(),
            args: node.args.clone(),
            manifest,
            backend: skill.backend.clone(),
            context_snapshot: context.to_snapshot(step),
            dry_run: node.dry_run,
            retry_policy,
        })))
    }

    async fn append_validation(
        &self,
        context: &AgentContext,
        graph: &ToolExecutionGraph,
        node: &ToolNode,
        valid: bool,
        errors: Vec<String>,
    ) -> Result<(), EngineError> {
        let record = SkillInputValidationRecord {
            validation_id: Uuid::new_v4().to_string(),
            graph_id: graph.graph_id.clone(),
            call_id: node.call_id.clone(),
            skill_name: node.skill_name.clone(),
            validated_at: SystemTime::now(),
            valid,
            errors,
        };
        self.memory
            .append_skill_input_validation(&context.session_id, record)
            .await?;
        Ok(())
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
        let session_id = context.session_id.clone();
        let snapshot = context.to_snapshot(steps_executed);
        let outcome_clone = outcome.clone();
        self.run_self_improvement(session_id, snapshot, outcome_clone)
            .await?;
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

    #[instrument(
        skip(self, snapshot, outcome),
        fields(
            session_id = %session_id,
            trigger_id = %snapshot.trigger_id,
            stop_reason = ?outcome.stop_reason
        )
    )]
    async fn run_self_improvement(
        &self,
        session_id: String,
        snapshot: AgentContextSnapshot,
        outcome: OutcomeRecord,
    ) -> Result<(), EngineError> {
        let policy = snapshot.policy.self_improvement.clone();
        if !policy.enabled {
            return Ok(());
        }

        counter!("rain_engine.self_improvement_reflections_total").increment(1);

        let observations = reflection_observations(&snapshot, &outcome);
        let reflection = ReflectionRecord {
            reflection_id: format!("reflection-{}", Uuid::new_v4()),
            created_at: SystemTime::now(),
            trigger_id: snapshot.trigger_id.clone(),
            summary: format!(
                "Observed {:?} after {} step(s); evaluating future policy and strategy.",
                outcome.stop_reason, outcome.steps_executed
            ),
            observations,
            confidence: 0.72,
        };
        self.memory
            .append_reflection(&session_id, reflection.clone())
            .await?;

        for performance in summarize_tool_performance(&snapshot.history) {
            self.memory
                .append_tool_performance(&session_id, performance.clone())
                .await?;
            if performance.calls > 0 {
                counter!(
                    "rain_engine.tool_performance_summaries_total",
                    "skill" => performance.skill_name.clone()
                )
                .increment(1);
            }
            if performance.failure_rate > 0.5 {
                let preference = StrategyPreferenceRecord {
                    preference_id: format!("strategy-{}", Uuid::new_v4()),
                    created_at: SystemTime::now(),
                    skill_name: Some(performance.skill_name.clone()),
                    preference: "avoid_when_alternatives_exist".to_string(),
                    reason: format!(
                        "{} failed in {:.0}% of recent calls",
                        performance.skill_name,
                        performance.failure_rate * 100.0
                    ),
                    confidence: 0.68,
                };
                self.memory
                    .append_strategy_preference(&session_id, preference)
                    .await?;
            }
        }

        if terminal_observation_count(&snapshot.history) < policy.min_observations_before_tuning {
            return Ok(());
        }

        if let Some(rollback) = maybe_rollback_regression(&snapshot, &outcome) {
            self.memory
                .append_policy_tuning(&session_id, rollback)
                .await?;
            counter!("rain_engine.self_improvement_rollbacks_total").increment(1);
            return Ok(());
        }

        let Some(tuning) = propose_policy_tuning(&snapshot, &outcome) else {
            return Ok(());
        };
        match tuning.action {
            PolicyTuningAction::Applied => {
                counter!("rain_engine.self_improvement_overlays_applied_total").increment(1)
            }
            PolicyTuningAction::RejectedUnsafe => {
                counter!("rain_engine.self_improvement_rejected_unsafe_total").increment(1)
            }
            PolicyTuningAction::Proposed | PolicyTuningAction::RolledBack => {}
        }
        self.memory
            .append_policy_tuning(&session_id, tuning)
            .await?;

        let profile_patch = ProfilePatchRecord {
            patch_id: format!("profile-patch-{}", Uuid::new_v4()),
            created_at: SystemTime::now(),
            description: "No capability or scope expansion was applied automatically.".to_string(),
            patch: serde_json::json!({"guardrail": "privilege_expansion_requires_approval"}),
            requires_approval: false,
            applied: true,
        };
        self.memory
            .append_profile_patch(&session_id, profile_patch)
            .await?;

        Ok(())
    }

    fn is_skill_circuit_broken(&self, skill_name: &str, context: &AgentContext) -> bool {
        let performance = summarize_tool_performance(&context.records);
        if let Some(perf) = performance.into_iter().find(|p| p.skill_name == skill_name)
            && perf.calls >= 3
        {
            let threshold = self
                .skills
                .get(skill_name)
                .map(|s| s.value().definition().manifest.circuit_breaker_threshold)
                .unwrap_or(0.5);
            return perf.failure_rate >= threshold;
        }
        false
    }

    fn error_result(
        &self,
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

    async fn summarize_history(
        &self,
        snapshot: &SessionSnapshot,
        policy: &EnginePolicy,
        trigger: &AgentTrigger,
    ) -> Result<SummaryRecord, EngineError> {
        let history_text = snapshot
            .records
            .iter()
            .map(|r| format!("{:?}", r))
            .collect::<Vec<_>>()
            .join("\n");
        let prompt = format!(
            "Summarize the following conversation history concisely while preserving all key decisions, facts, and outcomes:\n\n{}",
            history_text
        );

        let context = AgentContextSnapshot {
            session_id: snapshot.session_id.clone(),
            granted_scopes: Vec::new(),
            trigger_id: "internal".to_string(),
            idempotency_key: None,
            current_step: 0,
            max_steps: 0,
            history: snapshot.records.clone(),
            prior_tool_results: snapshot.tool_results(),
            session_cost_usd: 0.0,
            state: snapshot.agent_state(),
            policy: policy.clone(),
            active_execution_plan: snapshot.active_execution_plan(),
        };

        let request = ProviderRequest {
            trigger: trigger.clone(),
            context,
            available_skills: self.skill_definitions().await,
            config: ProviderRequestConfig {
                model: None,
                temperature: Some(0.0),
                max_tokens: Some(500),
            },
            policy: policy.clone(),
            contents: vec![ProviderMessage {
                role: ProviderRole::User,
                parts: vec![ProviderContentPart::Text(prompt)],
            }],
        };

        let decision = self
            .llm
            .generate_action(request)
            .await
            .map_err(|e| EngineError::Provider(e.to_string()))?;

        if let AgentAction::Respond { content } = decision.action {
            Ok(SummaryRecord {
                summary_id: format!("summary-{}", Uuid::new_v4()),
                created_at: SystemTime::now(),
                content,
                original_sequence_range: (0, snapshot.records.len()),
            })
        } else {
            Err(EngineError::Provider(
                "Failed to generate summary".to_string(),
            ))
        }
    }
}

fn classify_trigger_intent(trigger: &AgentTrigger) -> String {
    match trigger {
        AgentTrigger::ExternalEvent { source, .. } => format!("external_event:{source}"),
        AgentTrigger::ScheduledWake { .. } => "scheduled_wake".to_string(),
        AgentTrigger::HumanInput { content, .. } | AgentTrigger::Message { content, .. } => {
            let lowered = content.to_lowercase();
            if lowered.contains("approve") || lowered.contains("permission") {
                "approval_or_permission".to_string()
            } else if lowered.contains("fix")
                || lowered.contains("change")
                || lowered.contains("write")
            {
                "task_execution".to_string()
            } else if lowered.contains("what") || lowered.contains("why") || lowered.contains("how")
            {
                "question_answering".to_string()
            } else {
                "conversation".to_string()
            }
        }
        AgentTrigger::SystemObservation { source, .. } => format!("system_observation:{source}"),
        AgentTrigger::Webhook { source, .. } => format!("webhook:{source}"),
        AgentTrigger::RuleTrigger { rule_id, .. } => format!("rule:{rule_id}"),
        AgentTrigger::ProactiveHeartbeat { .. } => "heartbeat".to_string(),
        AgentTrigger::Approval { decision, .. } => format!("approval:{decision:?}"),
        AgentTrigger::DelegationResult { .. } => "delegation_result".to_string(),
    }
}

fn action_metric_label(action: &AgentAction) -> &'static str {
    match action {
        AgentAction::Plan { .. } => "plan",
        AgentAction::Respond { .. } => "respond",
        AgentAction::CallSkills(_) => "call_skills",
        AgentAction::Continue { .. } => "continue",
        AgentAction::Yield { .. } => "yield",
        AgentAction::Suspend { .. } => "suspend",
        AgentAction::Delegate { .. } => "delegate",
    }
}

fn reflection_observations(
    snapshot: &AgentContextSnapshot,
    outcome: &OutcomeRecord,
) -> Vec<String> {
    let tool_results = snapshot
        .history
        .iter()
        .filter(|record| matches!(record, SessionRecord::ToolResult(_)))
        .count();
    let failed_tools = snapshot
        .history
        .iter()
        .filter(|record| match record {
            SessionRecord::ToolResult(result) => result.output.is_err(),
            _ => false,
        })
        .count();
    let provider_cost = snapshot.session_cost_usd;

    vec![
        format!("terminal_stop_reason={:?}", outcome.stop_reason),
        format!("steps_executed={}", outcome.steps_executed),
        format!("tool_results={tool_results}"),
        format!("failed_tool_results={failed_tools}"),
        format!("estimated_session_cost_usd={provider_cost:.6}"),
    ]
}

fn summarize_tool_performance(records: &[SessionRecord]) -> Vec<ToolPerformanceRecord> {
    let calls = records
        .iter()
        .filter_map(|record| match record {
            SessionRecord::ToolCall(call) => Some((call.call_id.clone(), call)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let mut grouped = HashMap::<String, (String, usize, usize, usize)>::new();

    for record in records {
        let SessionRecord::ToolResult(result) = record else {
            continue;
        };
        let backend = calls
            .get(&result.call_id)
            .map(|call| format!("{:?}", call.backend_kind))
            .unwrap_or_else(|| "unknown".to_string());
        let entry = grouped
            .entry(result.skill_name.clone())
            .or_insert((backend, 0, 0, 0));
        entry.1 += 1;
        if result.output.is_ok() {
            entry.2 += 1;
        } else {
            entry.3 += 1;
        }
    }

    grouped
        .into_iter()
        .map(
            |(skill_name, (backend_kind, calls, successes, failures))| ToolPerformanceRecord {
                performance_id: format!("tool-performance-{}", Uuid::new_v4()),
                created_at: SystemTime::now(),
                skill_name,
                backend_kind,
                calls,
                successes,
                failures,
                failure_rate: if calls == 0 {
                    0.0
                } else {
                    failures as f64 / calls as f64
                },
            },
        )
        .collect()
}

fn terminal_observation_count(records: &[SessionRecord]) -> usize {
    records
        .iter()
        .filter(|record| matches!(record, SessionRecord::Outcome(_)))
        .count()
}

fn maybe_rollback_regression(
    snapshot: &AgentContextSnapshot,
    outcome: &OutcomeRecord,
) -> Option<PolicyTuningRecord> {
    if !snapshot.policy.self_improvement.rollback_on_regression {
        return None;
    }
    if !matches!(
        outcome.stop_reason,
        StopReason::ProviderFailure
            | StopReason::DeadlineExceeded
            | StopReason::PolicyAborted
            | StopReason::MaxStepsReached
    ) {
        return None;
    }
    let active = SessionSnapshot {
        session_id: snapshot.session_id.clone(),
        records: snapshot.history.clone(),
        last_sequence_no: None,
        latest_outcome: Some(outcome.clone()),
    }
    .active_policy_overlay()?;

    let mut projected_policy = snapshot.policy.clone();
    projected_policy.self_improvement = snapshot.policy.self_improvement.clone();
    Some(PolicyTuningRecord {
        tuning_id: format!("tuning-{}", Uuid::new_v4()),
        created_at: SystemTime::now(),
        overlay: PolicyOverlay {
            status: PolicyOverlayStatus::RolledBack,
            reason: format!(
                "Regression detected after overlay {}; rolling back for future advances.",
                active.overlay_id
            ),
            ..active
        },
        action: PolicyTuningAction::RolledBack,
        prior_policy: snapshot.policy.clone(),
        projected_policy,
    })
}

fn propose_policy_tuning(
    snapshot: &AgentContextSnapshot,
    outcome: &OutcomeRecord,
) -> Option<PolicyTuningRecord> {
    let improvement = &snapshot.policy.self_improvement;
    let mut patch = PolicyOverlayPatch::default();
    let mut reason = None::<String>;
    let delta = improvement.max_policy_delta_percent.clamp(1.0, 100.0);

    match outcome.stop_reason {
        StopReason::ProviderFailure
            if outcome
                .detail
                .as_deref()
                .map(|detail| detail.to_ascii_lowercase().contains("timeout"))
                .unwrap_or(false) =>
        {
            patch.provider_timeout_ms = Some(increase_by_percent(
                snapshot.policy.provider_timeout_ms,
                delta,
            ));
            reason = Some(
                "Provider timed out; increasing provider timeout within guardrails.".to_string(),
            );
        }
        StopReason::MaxStepsReached => {
            patch.max_steps = Some(increase_usize_by_percent(snapshot.policy.max_steps, delta));
            reason = Some(
                "Session hit max steps; increasing future step budget within guardrails."
                    .to_string(),
            );
        }
        StopReason::DeadlineExceeded => {
            patch.max_execution_time_ms = Some(increase_by_percent(
                snapshot.policy.max_execution_time_ms,
                delta,
            ));
            reason = Some("Execution deadline was reached; increasing future wall-clock budget within guardrails.".to_string());
        }
        StopReason::PolicyAborted
            if outcome
                .detail
                .as_deref()
                .map(|detail| detail.contains("cost"))
                .unwrap_or(false) =>
        {
            reason = Some(
                "Cost limit was reached; automatic cost-limit increases are blocked.".to_string(),
            );
        }
        _ => {}
    }

    let reason = reason?;
    let mut overlay = PolicyOverlay {
        overlay_id: format!("overlay-{}", Uuid::new_v4()),
        created_at: SystemTime::now(),
        status: match improvement.mode {
            SelfImprovementMode::Advisory => PolicyOverlayStatus::Proposed,
            SelfImprovementMode::AutoWithGuardrails => PolicyOverlayStatus::Applied,
        },
        reason,
        evidence_window_records: snapshot.history.len(),
        patch,
        confidence: 0.74,
        rollback_condition:
            "Rollback if the next terminal outcome regresses to a policy/provider failure."
                .to_string(),
    };

    let action = if outcome
        .detail
        .as_deref()
        .map(|detail| detail.contains("cost"))
        .unwrap_or(false)
    {
        overlay.status = PolicyOverlayStatus::Rejected;
        PolicyTuningAction::RejectedUnsafe
    } else {
        match improvement.mode {
            SelfImprovementMode::Advisory => PolicyTuningAction::Proposed,
            SelfImprovementMode::AutoWithGuardrails => PolicyTuningAction::Applied,
        }
    };

    let mut projected_policy = snapshot.policy.clone();
    overlay.apply_to(&mut projected_policy);

    Some(PolicyTuningRecord {
        tuning_id: format!("tuning-{}", Uuid::new_v4()),
        created_at: SystemTime::now(),
        overlay,
        action,
        prior_policy: snapshot.policy.clone(),
        projected_policy,
    })
}

fn increase_by_percent(value: u64, percent: f64) -> u64 {
    ((value.max(1) as f64) * (1.0 + percent / 100.0)).ceil() as u64
}

fn increase_usize_by_percent(value: usize, percent: f64) -> usize {
    ((value.max(1) as f64) * (1.0 + percent / 100.0)).ceil() as usize
}

fn existing_or_new_graph(
    context: &AgentContext,
    step: usize,
    calls: &[PlannedSkillCall],
) -> ToolExecutionGraph {
    let call_ids = calls
        .iter()
        .map(|call| call.call_id.as_str())
        .collect::<BTreeSet<_>>();
    if let Some(graph) = context
        .records
        .iter()
        .rev()
        .find_map(|record| match record {
            SessionRecord::ToolExecutionGraph(graph)
                if graph.step == step
                    && graph
                        .nodes
                        .iter()
                        .map(|node| node.call_id.as_str())
                        .collect::<BTreeSet<_>>()
                        == call_ids =>
            {
                Some(graph.clone())
            }
            _ => None,
        })
    {
        return graph;
    }

    ToolExecutionGraph {
        graph_id: format!("{}:{step}", context.metadata.trigger_id),
        trigger_id: context.metadata.trigger_id.clone(),
        step,
        created_at: SystemTime::now(),
        nodes: calls
            .iter()
            .enumerate()
            .map(|(provider_order, call)| ToolNode {
                call_id: call.call_id.clone(),
                skill_name: call.name.clone(),
                args: call.args.clone(),
                priority: call.priority,
                dependencies: call
                    .depends_on
                    .iter()
                    .map(|call_id| ToolDependency {
                        call_id: call_id.clone(),
                    })
                    .collect(),
                retry_policy: call.retry_policy.clone(),
                dry_run: call.dry_run,
                provider_order,
            })
            .collect(),
    }
}

fn latest_tool_statuses(context: &AgentContext, graph_id: &str) -> HashMap<String, ToolNodeStatus> {
    let mut statuses = HashMap::new();
    for record in &context.records {
        if let SessionRecord::ToolNodeCheckpoint(checkpoint) = record
            && checkpoint.graph_id == graph_id
        {
            statuses.insert(checkpoint.call_id.clone(), checkpoint.status.clone());
        }
    }
    statuses
}

fn started_attempt_counts(context: &AgentContext, graph_id: &str) -> HashMap<String, usize> {
    let mut attempts = HashMap::<String, usize>::new();
    for record in &context.records {
        if let SessionRecord::ToolNodeCheckpoint(checkpoint) = record
            && checkpoint.graph_id == graph_id
            && checkpoint.status == ToolNodeStatus::Started
        {
            let current = attempts.entry(checkpoint.call_id.clone()).or_default();
            *current = (*current).max(checkpoint.attempt);
        }
    }
    attempts
}

fn ready_nodes(
    graph: &ToolExecutionGraph,
    status_by_call: &HashMap<String, ToolNodeStatus>,
) -> Vec<ToolNode> {
    let mut nodes = graph
        .nodes
        .iter()
        .filter(|node| !is_terminal_status(status_by_call.get(&node.call_id)))
        .filter(|node| {
            node.dependencies.iter().all(|dependency| {
                matches!(
                    status_by_call.get(&dependency.call_id),
                    Some(ToolNodeStatus::Succeeded)
                )
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then(left.provider_order.cmp(&right.provider_order))
            .then(left.call_id.cmp(&right.call_id))
    });
    nodes
}

fn is_terminal_status(status: Option<&ToolNodeStatus>) -> bool {
    matches!(
        status,
        Some(
            ToolNodeStatus::Succeeded
                | ToolNodeStatus::Failed
                | ToolNodeStatus::Skipped
                | ToolNodeStatus::TimedOut
        )
    )
}

fn final_status_for_result(result: &ToolResultRecord) -> ToolNodeStatus {
    match &result.output {
        Ok(_) => ToolNodeStatus::Succeeded,
        Err(error) if error.kind == SkillFailureKind::Timeout => ToolNodeStatus::TimedOut,
        Err(_) => ToolNodeStatus::Failed,
    }
}

fn result_detail(result: &ToolResultRecord) -> Option<String> {
    match &result.output {
        Ok(_) => None,
        Err(error) => Some(error.message.clone()),
    }
}

fn validate_against_schema(value: &serde_json::Value, schema: &serde_json::Value) -> Vec<String> {
    let schema_type = schema.get("type").and_then(serde_json::Value::as_str);
    let mut errors = Vec::new();
    if let Some(schema_type) = schema_type
        && !json_type_matches(value, schema_type)
    {
        errors.push(format!("expected root type `{schema_type}`"));
        return errors;
    }

    if schema_type == Some("object") {
        let Some(object) = value.as_object() else {
            return vec!["expected root object".to_string()];
        };
        if let Some(required) = schema.get("required").and_then(serde_json::Value::as_array) {
            for required_key in required.iter().filter_map(serde_json::Value::as_str) {
                if !object.contains_key(required_key) {
                    errors.push(format!("missing required property `{required_key}`"));
                }
            }
        }
        if let Some(properties) = schema
            .get("properties")
            .and_then(serde_json::Value::as_object)
        {
            for (key, property_schema) in properties {
                let Some(property_value) = object.get(key) else {
                    continue;
                };
                if let Some(property_type) = property_schema
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    && !json_type_matches(property_value, property_type)
                {
                    errors.push(format!("property `{key}` expected type `{property_type}`"));
                }
            }
        }
    }
    errors
}

fn json_type_matches(value: &serde_json::Value, expected: &str) -> bool {
    match expected {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => true,
    }
}

struct PreparedCall {
    call_id: String,
    name: String,
    args: serde_json::Value,
    manifest: SkillManifest,
    backend: RegisteredSkillBackend,
    context_snapshot: crate::AgentContextSnapshot,
    dry_run: bool,
    retry_policy: RetryPolicy,
}

enum PreparedNode {
    Executable(Box<PreparedCall>),
    Immediate(ToolResultRecord),
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
    let started = Instant::now();
    let mut output = Err(SkillExecutionError::new(
        SkillFailureKind::Internal,
        "tool was not attempted",
    ));
    let mut current_interval_ms = prepared.retry_policy.initial_interval_ms;
    for attempt in 0..prepared.retry_policy.max_attempts {
        let invocation = SkillInvocation {
            call_id: prepared.call_id.clone(),
            manifest: prepared.manifest.clone(),
            args: prepared.args.clone(),
            context: prepared.context_snapshot.clone(),
            dry_run: prepared.dry_run,
        };
        output = match &prepared.backend {
            RegisteredSkillBackend::Wasm(executor) => executor.execute(invocation).await,
            RegisteredSkillBackend::Native(executor) => executor.execute(invocation).await,
        };
        if output.is_ok()
            || !is_retryable_skill_error(&output)
            || attempt + 1 >= prepared.retry_policy.max_attempts
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(current_interval_ms)).await;
        current_interval_ms =
            ((current_interval_ms as f64) * prepared.retry_policy.backoff_multiplier) as u64;
        current_interval_ms = current_interval_ms.min(prepared.retry_policy.max_interval_ms);
    }
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
                SkillFailureKind::InvalidArguments
                | SkillFailureKind::InvalidResponse
                | SkillFailureKind::Internal => {}
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

fn is_retryable_skill_error(output: &Result<serde_json::Value, SkillExecutionError>) -> bool {
    matches!(
        output,
        Err(SkillExecutionError {
            kind: SkillFailureKind::Timeout | SkillFailureKind::Internal | SkillFailureKind::Trap,
            ..
        })
    )
}

fn build_advance_result(
    outcome: EngineOutcome,
    emitted_events: Vec<KernelEventRecord>,
) -> AdvanceResult {
    let wake_request = emitted_events.iter().find_map(extract_wake_request);
    let state_delta = derive_state_delta(&emitted_events);
    AdvanceResult {
        outcome: Some(outcome),
        emitted_events,
        state_delta,
        wake_request,
    }
}

fn extract_wake_request(event: &KernelEventRecord) -> Option<WakeRequestRecord> {
    match &event.event {
        KernelEvent::WakeRequested(wake) | KernelEvent::WakeScheduled(wake) => Some(wake.clone()),
        _ => None,
    }
}

fn derive_state_delta(events: &[KernelEventRecord]) -> AgentStateDelta {
    let mut delta = AgentStateDelta::default();
    for event in events {
        match &event.event {
            KernelEvent::GoalCreated(goal) => delta.created_goal_ids.push(goal.goal_id.clone()),
            KernelEvent::TaskPlanned(task) => delta.updated_task_ids.push(task.task_id.clone()),
            KernelEvent::TaskClaimed { task_id, .. } => {
                delta.updated_task_ids.push(task_id.clone())
            }
            KernelEvent::TaskBlocked { task_id, .. }
            | KernelEvent::TaskCompleted { task_id, .. }
            | KernelEvent::TaskFailed { task_id, .. }
            | KernelEvent::TaskAbandoned { task_id, .. } => {
                delta.updated_task_ids.push(task_id.clone())
            }
            KernelEvent::HumanInputRequested { task_id, .. } => {
                if let Some(task_id) = task_id {
                    delta.updated_task_ids.push(task_id.clone());
                }
            }
            KernelEvent::ObservationAppended(observation) => delta
                .observation_ids
                .push(observation.observation_id.clone()),
            KernelEvent::ArtifactProduced(artifact) => {
                delta.artifact_ids.push(artifact.artifact_id.clone())
            }
            KernelEvent::DelegationRequested(record) => delta
                .delegation_correlation_ids
                .push(record.correlation_id.clone()),
            KernelEvent::DelegationResolved { correlation_id, .. } => delta
                .delegation_correlation_ids
                .push(correlation_id.clone()),
            KernelEvent::WakeRequested(_)
            | KernelEvent::WakeScheduled(_)
            | KernelEvent::WakeCompleted { .. }
            | KernelEvent::ResourceRegistered(_)
            | KernelEvent::RelationshipObserved(_) => {}
        }
    }
    delta
}

fn derive_trigger_kernel_events(
    trigger_id: &str,
    trigger: &AgentTrigger,
) -> Vec<KernelEventRecord> {
    let mut events = Vec::new();
    let observed_at = SystemTime::now();
    let mut push_observation = |source: String,
                                content: serde_json::Value,
                                attachments: Vec<String>| {
        events.push(KernelEventRecord {
            event_id: format!("observation-{trigger_id}-{}", events.len()),
            occurred_at: observed_at,
            event: KernelEvent::ObservationAppended(crate::ObservationRecord {
                observation_id: crate::ObservationId(format!("{trigger_id}-obs-{}", events.len())),
                recorded_at: observed_at,
                source,
                content,
                attachment_ids: attachments,
                related_resources: Vec::new(),
            }),
        });
    };

    match trigger {
        AgentTrigger::ExternalEvent {
            source,
            payload,
            attachments,
        } => push_observation(
            format!("external:{source}"),
            payload.clone(),
            attachments
                .iter()
                .map(|attachment| attachment.attachment_id.clone())
                .collect(),
        ),
        AgentTrigger::HumanInput {
            actor_id,
            content,
            attachments,
        } => push_observation(
            format!("human:{actor_id}"),
            serde_json::json!({ "content": content }),
            attachments
                .iter()
                .map(|attachment| attachment.attachment_id.clone())
                .collect(),
        ),
        AgentTrigger::SystemObservation {
            source,
            observation,
            attachments,
        } => push_observation(
            format!("system:{source}"),
            observation.clone(),
            attachments
                .iter()
                .map(|attachment| attachment.attachment_id.clone())
                .collect(),
        ),
        AgentTrigger::Webhook {
            source,
            payload,
            attachments,
        } => push_observation(
            format!("webhook:{source}"),
            payload.clone(),
            attachments
                .iter()
                .map(|attachment| attachment.attachment_id.clone())
                .collect(),
        ),
        AgentTrigger::RuleTrigger {
            rule_id,
            context,
            attachments,
        } => push_observation(
            format!("rule:{rule_id}"),
            context.clone(),
            attachments
                .iter()
                .map(|attachment| attachment.attachment_id.clone())
                .collect(),
        ),
        AgentTrigger::ProactiveHeartbeat { timestamp, .. } => push_observation(
            "heartbeat".to_string(),
            serde_json::json!({ "timestamp": timestamp }),
            Vec::new(),
        ),
        AgentTrigger::ScheduledWake {
            wake_id, reason, ..
        } => events.push(KernelEventRecord {
            event_id: format!("wake-completed-{trigger_id}"),
            occurred_at: observed_at,
            event: KernelEvent::WakeCompleted {
                wake_id: wake_id.clone(),
                reason: reason.clone(),
                completed_at: observed_at,
            },
        }),
        AgentTrigger::Message {
            user_id,
            content,
            attachments,
        } => push_observation(
            format!("message:{user_id}"),
            serde_json::json!({ "content": content }),
            attachments
                .iter()
                .map(|attachment| attachment.attachment_id.clone())
                .collect(),
        ),
        AgentTrigger::DelegationResult {
            correlation_id,
            payload,
            metadata,
        } => events.push(KernelEventRecord {
            event_id: format!("delegation-resolved-{trigger_id}"),
            occurred_at: observed_at,
            event: KernelEvent::DelegationResolved {
                correlation_id: correlation_id.clone(),
                resolved_at: observed_at,
                payload: payload.clone(),
                metadata: metadata.clone(),
            },
        }),
        AgentTrigger::Approval { .. } => {}
    }

    events
}

fn build_provider_contents(
    trigger: &AgentTrigger,
    history: &[SessionRecord],
) -> Vec<ProviderMessage> {
    let mut messages = Vec::new();
    let mut intent_by_trigger = HashMap::<String, String>::new();
    for record in history {
        match record {
            SessionRecord::Trigger(trigger) => {
                if let Some(intent) = &trigger.intent {
                    intent_by_trigger.insert(trigger.trigger_id.clone(), intent.clone());
                }
            }
            SessionRecord::TriggerIntent(intent) => {
                intent_by_trigger.insert(intent.trigger_id.clone(), intent.intent.clone());
            }
            _ => {}
        }
    }

    // Map history to messages to provide multi-turn memory
    for record in history {
        match record {
            SessionRecord::Trigger(t) => {
                let mut parts = build_trigger_parts(&t.trigger);
                if let Some(intent) = t
                    .intent
                    .as_ref()
                    .or_else(|| intent_by_trigger.get(&t.trigger_id))
                {
                    parts.push(ProviderContentPart::Text(format!(
                        "Classified intent: {intent}"
                    )));
                }
                messages.push(ProviderMessage {
                    role: ProviderRole::User,
                    parts,
                });
            }
            SessionRecord::Summary(s) => {
                messages.push(ProviderMessage {
                    role: ProviderRole::Assistant,
                    parts: vec![ProviderContentPart::Text(format!(
                        "Summary of prior history: {}",
                        s.content
                    ))],
                });
            }
            SessionRecord::ModelDecision(d) => match &d.action {
                AgentAction::Respond { content } => {
                    messages.push(ProviderMessage {
                        role: ProviderRole::Assistant,
                        parts: vec![ProviderContentPart::Text(content.clone())],
                    });
                }
                AgentAction::CallSkills(calls) => {
                    // Represent tool calls in history
                    messages.push(ProviderMessage {
                        role: ProviderRole::Assistant,
                        parts: vec![ProviderContentPart::Json(
                            serde_json::to_value(calls).unwrap_or_default(),
                        )],
                    });
                }
                _ => {}
            },
            SessionRecord::ToolResult(r) => {
                messages.push(ProviderMessage {
                    role: ProviderRole::Tool,
                    parts: vec![ProviderContentPart::ToolResult(r.clone())],
                });
            }
            _ => {}
        }
    }

    // Add the current trigger that kicked off this step
    messages.push(ProviderMessage {
        role: ProviderRole::User,
        parts: build_trigger_parts(trigger),
    });

    messages
}

fn build_trigger_parts(trigger: &AgentTrigger) -> Vec<ProviderContentPart> {
    let mut parts = Vec::new();
    match trigger {
        AgentTrigger::ExternalEvent {
            source,
            payload,
            attachments,
        } => {
            parts.push(ProviderContentPart::Text(format!(
                "external event source: {source}"
            )));
            parts.push(ProviderContentPart::Json(payload.clone()));
            parts.extend(
                attachments
                    .iter()
                    .cloned()
                    .map(ProviderContentPart::Attachment),
            );
        }
        AgentTrigger::ScheduledWake {
            wake_id,
            due_at,
            reason,
        } => {
            parts.push(ProviderContentPart::Text(format!(
                "scheduled wake {} due at {:?}: {reason}",
                wake_id.0, due_at
            )));
        }
        AgentTrigger::HumanInput {
            actor_id,
            content,
            attachments,
        } => {
            parts.push(ProviderContentPart::Text(format!(
                "human actor: {actor_id}"
            )));
            parts.push(ProviderContentPart::Text(content.clone()));
            parts.extend(
                attachments
                    .iter()
                    .cloned()
                    .map(ProviderContentPart::Attachment),
            );
        }
        AgentTrigger::SystemObservation {
            source,
            observation,
            attachments,
        } => {
            parts.push(ProviderContentPart::Text(format!(
                "system observation source: {source}"
            )));
            parts.push(ProviderContentPart::Json(observation.clone()));
            parts.extend(
                attachments
                    .iter()
                    .cloned()
                    .map(ProviderContentPart::Attachment),
            );
        }
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
    parts
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
        AttachmentRef, EnginePolicy, InMemoryMemoryStore, MockLlmProvider, ProviderCacheRecord,
        ProviderDecision, ProviderError, ProviderErrorKind, ProviderUsageRecord, RecordPageQuery,
        ResourcePolicy, SessionListQuery, SessionSnapshot, SkillCapability, StopReason, TaskId,
        TaskRecord, TaskStatus, WakeId, WakeRequestRecord,
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
            circuit_breaker_threshold: 0.5,
        }
    }

    fn planned(call_id: &str, name: &str, args: serde_json::Value) -> PlannedSkillCall {
        PlannedSkillCall {
            call_id: call_id.to_string(),
            name: name.to_string(),
            args,
            priority: 0,
            depends_on: Vec::new(),
            retry_policy: Default::default(),
            dry_run: false,
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

    async fn run_until_terminal(
        engine: &AgentEngine,
        request: ProcessRequest,
    ) -> Result<EngineOutcome, EngineError> {
        let mut next = AdvanceRequest::Trigger(request.clone());
        loop {
            let result = engine.advance(next).await?;
            if let Some(outcome) = result.outcome {
                return Ok(outcome);
            }
            next = AdvanceRequest::Continue(ContinueRequest {
                session_id: request.session_id.clone(),
                granted_scopes: request.granted_scopes.clone(),
                policy: request.policy.clone(),
                provider: request.provider.clone(),
                cancellation: request.cancellation.clone(),
            });
        }
    }

    #[tokio::test]
    async fn webhook_trigger_with_attachment_responds() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
            content: "done".to_string(),
        }]));
        let engine = AgentEngine::new(llm, store.clone());

        let outcome = run_until_terminal(
            &engine,
            ProcessRequest::new(
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
            ),
        )
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
    async fn advance_executes_one_progression_step() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::scripted(vec![
            AgentAction::CallSkills(vec![PlannedSkillCall {
                call_id: "call-1".to_string(),
                name: "echo".to_string(),
                args: json!({"value": 1}),
                priority: 0,
                depends_on: Vec::new(),
                retry_policy: Default::default(),
                dry_run: false,
            }]),
            AgentAction::Respond {
                content: "done".to_string(),
            },
        ]));
        let engine = AgentEngine::new(llm, store.clone());
        engine.register_wasm_skill(
            manifest("echo", &["tool:run"]),
            Arc::new(StubSkillExecutor {
                name: "stub",
                responder: Arc::new(|invocation| Ok(json!({"echo": invocation.args}))),
            }),
        );
        let request =
            ProcessRequest::new("step-session", message_trigger("run")).with_scope("tool:run");

        let first = engine
            .advance(AdvanceRequest::Trigger(request.clone()))
            .await
            .expect("first advance");
        assert!(first.outcome.is_none());
        assert_eq!(first.emitted_events.len(), 1);

        let second = engine
            .advance(AdvanceRequest::Continue(ContinueRequest {
                session_id: request.session_id.clone(),
                granted_scopes: request.granted_scopes.clone(),
                policy: request.policy.clone(),
                provider: request.provider.clone(),
                cancellation: request.cancellation.clone(),
            }))
            .await
            .expect("second advance");
        assert_eq!(
            second.outcome.expect("terminal").stop_reason,
            StopReason::Responded
        );
        assert_eq!(
            store
                .load_session("step-session")
                .await
                .unwrap()
                .tool_results()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn replay_projects_task_transitions() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let now = SystemTime::now();
        let task_id = TaskId("task-1".to_string());
        store
            .append_kernel_event(
                "projection-session",
                KernelEventRecord {
                    event_id: "task-planned".to_string(),
                    occurred_at: now,
                    event: KernelEvent::TaskPlanned(TaskRecord {
                        task_id: task_id.clone(),
                        goal_id: None,
                        parent_task_id: None,
                        created_at: now,
                        title: "triage".to_string(),
                        detail: None,
                        status: TaskStatus::Ready,
                        assignee: None,
                        blocked_by: Vec::new(),
                    }),
                },
            )
            .await
            .expect("planned");
        store
            .append_kernel_event(
                "projection-session",
                KernelEventRecord {
                    event_id: "task-done".to_string(),
                    occurred_at: now,
                    event: KernelEvent::TaskCompleted {
                        task_id: task_id.clone(),
                        completed_at: now,
                        artifact_ids: Vec::new(),
                    },
                },
            )
            .await
            .expect("completed");

        let state = store
            .load_session("projection-session")
            .await
            .expect("snapshot")
            .agent_state();
        assert_eq!(state.tasks[0].status, TaskStatus::Done);
    }

    #[tokio::test]
    async fn scheduled_wake_trigger_completes_pending_wake() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let now = SystemTime::now();
        let wake_id = WakeId("wake-1".to_string());
        store
            .append_kernel_event(
                "wake-session",
                KernelEventRecord {
                    event_id: "wake-scheduled".to_string(),
                    occurred_at: now,
                    event: KernelEvent::WakeScheduled(WakeRequestRecord {
                        wake_id: wake_id.clone(),
                        requested_at: now,
                        due_at: now,
                        reason: "check later".to_string(),
                        task_id: None,
                    }),
                },
            )
            .await
            .expect("scheduled");
        assert!(
            store
                .load_session("wake-session")
                .await
                .expect("snapshot")
                .agent_state()
                .pending_wake
                .is_some()
        );

        let llm = Arc::new(MockLlmProvider::scripted(vec![AgentAction::Yield {
            reason: Some("wake handled".to_string()),
        }]));
        let engine = AgentEngine::new(llm, store.clone());
        let outcome = run_until_terminal(
            &engine,
            ProcessRequest::new(
                "wake-session",
                AgentTrigger::ScheduledWake {
                    wake_id: wake_id.clone(),
                    due_at: now,
                    reason: "check later".to_string(),
                },
            ),
        )
        .await
        .expect("outcome");
        assert_eq!(outcome.stop_reason, StopReason::Yielded);
        let snapshot = store.load_session("wake-session").await.expect("snapshot");
        assert!(snapshot.agent_state().pending_wake.is_none());
        assert!(snapshot.records.iter().any(|record| matches!(
            record,
            SessionRecord::KernelEvent(KernelEventRecord {
                event: KernelEvent::WakeCompleted { wake_id: completed, .. },
                ..
            }) if completed == &wake_id
        )));
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
        let first = run_until_terminal(&engine, request.clone())
            .await
            .expect("first");
        let second = run_until_terminal(&engine, request).await.expect("second");
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
                            priority: 0,
                            depends_on: Vec::new(),
                            retry_policy: Default::default(),
                            dry_run: false,
                        },
                        PlannedSkillCall {
                            call_id: "call-2".to_string(),
                            name: "second".to_string(),
                            args: json!({"value": 2}),
                            priority: 0,
                            depends_on: Vec::new(),
                            retry_policy: Default::default(),
                            dry_run: false,
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
            engine.register_wasm_skill(
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
            );
        }

        let outcome = run_until_terminal(
            &engine,
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

        let outcome = run_until_terminal(
            &engine,
            ProcessRequest::new("session-usage", message_trigger("hi")),
        )
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
                        priority: 0,
                        depends_on: Vec::new(),
                        retry_policy: Default::default(),
                        dry_run: false,
                    }]),
                    usage: None,
                    cache: None,
                })
            }
        }));
        let engine = AgentEngine::new(llm, store.clone());
        engine.register_native_skill(
            SkillManifest {
                approval_required: true,
                ..manifest("db_fix", &["db:write"])
            },
            Arc::new(StubNativeSkill {
                requires_approval: true,
                responder: Arc::new(|_| Ok(json!({"status": "fixed"}))),
            }),
        );

        let suspended = run_until_terminal(
            &engine,
            ProcessRequest::new("approval-session", message_trigger("fix")).with_scope("db:write"),
        )
        .await
        .expect("suspended");
        assert_eq!(suspended.stop_reason, StopReason::Suspended);
        let token = suspended.resume_token.expect("resume token");

        let resumed = run_until_terminal(
            &engine,
            ProcessRequest::new(
                "approval-session",
                AgentTrigger::Approval {
                    resume_token: token,
                    decision: ApprovalDecision::Approved,
                    metadata: json!({"approved_by": "human"}),
                },
            ),
        )
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

        let outcome = run_until_terminal(
            &engine,
            ProcessRequest::new("provider-failure", message_trigger("hello")),
        )
        .await
        .expect("outcome");

        assert_eq!(outcome.stop_reason, StopReason::ProviderFailure);
    }

    #[tokio::test]
    async fn self_improvement_applies_bounded_max_step_overlay() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
            content: "unused".to_string(),
        }]));
        let engine = AgentEngine::new(llm, store.clone());
        let mut policy = EnginePolicy {
            max_steps: 0,
            ..EnginePolicy::default()
        };
        policy.self_improvement.min_observations_before_tuning = 1;
        policy.self_improvement.max_policy_delta_percent = 50.0;

        let outcome = run_until_terminal(
            &engine,
            ProcessRequest::new("learning-max-steps", message_trigger("continue"))
                .with_policy(policy),
        )
        .await
        .expect("outcome");

        assert_eq!(outcome.stop_reason, StopReason::MaxStepsReached);
        let snapshot = session(&store, "learning-max-steps").await;
        let overlay = snapshot.active_policy_overlay().expect("active overlay");
        assert_eq!(overlay.patch.max_steps, Some(2));
        assert!(
            snapshot
                .records
                .iter()
                .any(|record| matches!(record, SessionRecord::Reflection(_)))
        );
        assert!(
            snapshot
                .records
                .iter()
                .any(|record| matches!(record, SessionRecord::PolicyTuning(_)))
        );
    }

    #[tokio::test]
    async fn self_improvement_rolls_back_overlay_after_regression() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::dynamic(|_| {
            Err(ProviderError::new(
                ProviderErrorKind::Transport,
                "upstream unavailable",
                true,
            ))
        }));
        let engine = AgentEngine::new(llm, store.clone());
        let mut policy = EnginePolicy {
            max_steps: 0,
            ..EnginePolicy::default()
        };
        policy.self_improvement.min_observations_before_tuning = 1;

        let first = run_until_terminal(
            &engine,
            ProcessRequest::new("learning-rollback", message_trigger("first"))
                .with_policy(policy.clone()),
        )
        .await
        .expect("first");
        assert_eq!(first.stop_reason, StopReason::MaxStepsReached);
        assert!(
            session(&store, "learning-rollback")
                .await
                .active_policy_overlay()
                .is_some()
        );

        let second = run_until_terminal(
            &engine,
            ProcessRequest::new("learning-rollback", message_trigger("second")).with_policy(policy),
        )
        .await
        .expect("second");
        assert_eq!(second.stop_reason, StopReason::ProviderFailure);

        let snapshot = session(&store, "learning-rollback").await;
        assert!(snapshot.active_policy_overlay().is_none());
        assert!(snapshot.records.iter().any(|record| matches!(
            record,
            SessionRecord::PolicyTuning(tuning)
                if tuning.action == PolicyTuningAction::RolledBack
        )));
    }

    #[tokio::test]
    async fn self_improvement_records_tool_performance_and_strategy_preferences() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::dynamic(|input| {
            if input.context.prior_tool_results.is_empty() {
                Ok(ProviderDecision {
                    action: AgentAction::CallSkills(vec![PlannedSkillCall {
                        call_id: "unstable-call".to_string(),
                        name: "unstable".to_string(),
                        args: json!({}),
                        priority: 0,
                        depends_on: Vec::new(),
                        retry_policy: Default::default(),
                        dry_run: false,
                    }]),
                    usage: None,
                    cache: None,
                })
            } else {
                Ok(ProviderDecision {
                    action: AgentAction::Respond {
                        content: "finished".to_string(),
                    },
                    usage: None,
                    cache: None,
                })
            }
        }));
        let engine = AgentEngine::new(llm, store.clone());
        engine.register_wasm_skill(
            manifest("unstable", &["tool:run"]),
            Arc::new(StubSkillExecutor {
                name: "unstable",
                responder: Arc::new(|_| {
                    Err(SkillExecutionError::new(
                        SkillFailureKind::Internal,
                        "simulated failure",
                    ))
                }),
            }),
        );

        let mut policy = EnginePolicy::default();
        policy.self_improvement.min_observations_before_tuning = 1;

        let outcome = run_until_terminal(
            &engine,
            ProcessRequest::new("learning-tools", message_trigger("run"))
                .with_scope("tool:run")
                .with_policy(policy),
        )
        .await
        .expect("outcome");
        assert_eq!(outcome.stop_reason, StopReason::Responded);

        let snapshot = session(&store, "learning-tools").await;
        assert!(snapshot.records.iter().any(|record| matches!(
            record,
            SessionRecord::ToolPerformance(performance)
                if performance.skill_name == "unstable" && performance.failures >= 1
        )));
        assert!(snapshot.records.iter().any(|record| matches!(
            record,
            SessionRecord::StrategyPreference(preference)
                if preference.skill_name.as_deref() == Some("unstable")
        )));
    }

    #[tokio::test]
    async fn list_sessions_and_record_pages_work() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
            content: "ok".to_string(),
        }]));
        let engine = AgentEngine::new(llm, store.clone());
        for session_id in ["a", "b"] {
            run_until_terminal(
                &engine,
                ProcessRequest::new(session_id, message_trigger("x")),
            )
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

    #[tokio::test]
    async fn invalid_tool_arguments_are_persisted_without_executor_call() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::dynamic(|input| {
            if input.context.prior_tool_results.is_empty() {
                Ok(ProviderDecision {
                    action: AgentAction::CallSkills(vec![planned(
                        "bad-args",
                        "typed_tool",
                        json!({"value": 1}),
                    )]),
                    usage: None,
                    cache: None,
                })
            } else {
                Ok(ProviderDecision {
                    action: AgentAction::Respond {
                        content: "validated".to_string(),
                    },
                    usage: None,
                    cache: None,
                })
            }
        }));
        let engine = AgentEngine::new(llm, store.clone());
        let calls = Arc::new(Mutex::new(0usize));
        let calls_for_executor = calls.clone();
        engine.register_wasm_skill(
            SkillManifest {
                input_schema: json!({
                    "type": "object",
                    "required": ["value"],
                    "properties": {"value": {"type": "string"}}
                }),
                ..manifest("typed_tool", &["tool:run"])
            },
            Arc::new(StubSkillExecutor {
                name: "typed",
                responder: Arc::new(move |_| {
                    *calls_for_executor.lock().expect("lock") += 1;
                    Ok(json!({}))
                }),
            }),
        );

        let outcome = run_until_terminal(
            &engine,
            ProcessRequest::new("schema-session", message_trigger("run")).with_scope("tool:run"),
        )
        .await
        .expect("outcome");

        assert_eq!(outcome.stop_reason, StopReason::Responded);
        assert_eq!(*calls.lock().expect("lock"), 0);
        let snapshot = session(&store, "schema-session").await;
        assert!(snapshot.records.iter().any(|record| matches!(
            record,
            SessionRecord::SkillInputValidation(validation)
                if !validation.valid && validation.call_id == "bad-args"
        )));
        assert!(snapshot.records.iter().any(|record| matches!(
            record,
            SessionRecord::ToolResult(result)
                if result.call_id == "bad-args"
                    && matches!(
                        result.output,
                        Err(SkillFailure {
                            kind: SkillFailureKind::InvalidArguments,
                            ..
                        })
                    )
        )));
    }

    #[tokio::test]
    async fn checkpointed_graph_resume_executes_only_unfinished_nodes() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
            content: "done".to_string(),
        }]));
        let engine = AgentEngine::new(llm, store.clone());
        let c1_calls = Arc::new(Mutex::new(0usize));
        let c2_calls = Arc::new(Mutex::new(0usize));
        let c1_counter = c1_calls.clone();
        let c2_counter = c2_calls.clone();
        engine.register_wasm_skill(
            manifest("first", &["tool:run"]),
            Arc::new(StubSkillExecutor {
                name: "first",
                responder: Arc::new(move |_| {
                    *c1_counter.lock().expect("lock") += 1;
                    Ok(json!({"first": true}))
                }),
            }),
        );
        engine.register_wasm_skill(
            manifest("second", &["tool:run"]),
            Arc::new(StubSkillExecutor {
                name: "second",
                responder: Arc::new(move |_| {
                    *c2_counter.lock().expect("lock") += 1;
                    Ok(json!({"second": true}))
                }),
            }),
        );

        let trigger_id = "trigger-checkpoint".to_string();
        let session_id = "checkpoint-session";
        let trigger = TriggerRecord {
            trigger_id: trigger_id.clone(),
            session_id: session_id.to_string(),
            idempotency_key: None,
            recorded_at: SystemTime::now(),
            trigger: message_trigger("resume"),
            intent: None,
        };
        store.append_trigger(trigger).await.expect("trigger");
        let calls = vec![
            planned("call-1", "first", json!({})),
            planned("call-2", "second", json!({})),
        ];
        store
            .append_model_decision(
                session_id,
                ModelDecisionRecord {
                    step: 0,
                    decided_at: SystemTime::now(),
                    action: AgentAction::CallSkills(calls.clone()),
                },
            )
            .await
            .expect("decision");
        let graph = existing_or_new_graph(
            &AgentContext {
                session_id: session_id.to_string(),
                records: Vec::new(),
                prior_tool_results: Vec::new(),
                granted_scopes: ["tool:run".to_string()].into_iter().collect(),
                metadata: ExecutionMetadata {
                    trigger_id: trigger_id.clone(),
                    idempotency_key: None,
                    started_at: SystemTime::now(),
                    deadline: Instant::now() + EnginePolicy::default().max_execution_time(),
                    policy: EnginePolicy::default(),
                    provider: Default::default(),
                    cancellation: Default::default(),
                },
            },
            0,
            &calls,
        );
        store
            .append_tool_execution_graph(session_id, graph.clone())
            .await
            .expect("graph");
        let first_node = graph.nodes.first().expect("first node");
        store
            .append_tool_node_checkpoint(
                session_id,
                ToolNodeCheckpointRecord {
                    checkpoint_id: "cp-start".to_string(),
                    graph_id: graph.graph_id.clone(),
                    call_id: first_node.call_id.clone(),
                    skill_name: first_node.skill_name.clone(),
                    step: graph.step,
                    status: ToolNodeStatus::Started,
                    attempt: 1,
                    occurred_at: SystemTime::now(),
                    detail: None,
                },
            )
            .await
            .expect("started");
        store
            .append_tool_node_checkpoint(
                session_id,
                ToolNodeCheckpointRecord {
                    checkpoint_id: "cp-ok".to_string(),
                    graph_id: graph.graph_id.clone(),
                    call_id: first_node.call_id.clone(),
                    skill_name: first_node.skill_name.clone(),
                    step: graph.step,
                    status: ToolNodeStatus::Succeeded,
                    attempt: 1,
                    occurred_at: SystemTime::now(),
                    detail: None,
                },
            )
            .await
            .expect("succeeded");
        store
            .append_tool_result(
                session_id,
                ToolResultRecord {
                    call_id: "call-1".to_string(),
                    finished_at: SystemTime::now(),
                    skill_name: "first".to_string(),
                    output: Ok(json!({"already": "done"})),
                },
            )
            .await
            .expect("result");

        let result = engine
            .advance(AdvanceRequest::Continue(
                ContinueRequest::new(session_id).with_scope("tool:run"),
            ))
            .await
            .expect("advance");

        assert!(result.outcome.is_none());
        assert_eq!(*c1_calls.lock().expect("lock"), 0);
        assert_eq!(*c2_calls.lock().expect("lock"), 1);
        let snapshot = session(&store, session_id).await;
        assert!(snapshot.records.iter().any(|record| matches!(
            record,
            SessionRecord::ToolResult(result) if result.call_id == "call-2"
        )));
    }

    #[tokio::test]
    async fn priority_ordering_runs_high_priority_first_when_serialized() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::dynamic(|input| {
            if input.context.prior_tool_results.is_empty() {
                let mut low = planned("low", "low", json!({}));
                low.priority = 0;
                let mut high = planned("high", "high", json!({}));
                high.priority = 10;
                Ok(ProviderDecision {
                    action: AgentAction::CallSkills(vec![low, high]),
                    usage: None,
                    cache: None,
                })
            } else {
                Ok(ProviderDecision {
                    action: AgentAction::Respond {
                        content: "ordered".to_string(),
                    },
                    usage: None,
                    cache: None,
                })
            }
        }));
        let engine = AgentEngine::new(llm, store.clone());
        let order = Arc::new(Mutex::new(Vec::<String>::new()));
        for skill_name in ["low", "high"] {
            let order = order.clone();
            engine.register_wasm_skill(
                manifest(skill_name, &["tool:run"]),
                Arc::new(StubSkillExecutor {
                    name: "ordered",
                    responder: Arc::new(move |invocation| {
                        order.lock().expect("lock").push(invocation.call_id);
                        Ok(json!({}))
                    }),
                }),
            );
        }

        let mut policy = EnginePolicy {
            max_parallel_skill_calls: 1,
            ..EnginePolicy::default()
        };
        policy.self_improvement.enabled = false;
        run_until_terminal(
            &engine,
            ProcessRequest::new("priority-session", message_trigger("run"))
                .with_scope("tool:run")
                .with_policy(policy),
        )
        .await
        .expect("outcome");

        assert_eq!(&*order.lock().expect("lock"), &["high", "low"]);
    }

    #[tokio::test]
    async fn plan_action_persists_deliberation_record() {
        let store = Arc::new(InMemoryMemoryStore::new());
        let llm = Arc::new(MockLlmProvider::scripted(vec![
            AgentAction::Plan {
                summary: "inspect then act".to_string(),
                candidate_actions: vec!["search memory".to_string(), "call tool".to_string()],
                confidence: 0.82,
            },
            AgentAction::Respond {
                content: "planned".to_string(),
            },
        ]));
        let engine = AgentEngine::new(llm, store.clone());

        let outcome = run_until_terminal(
            &engine,
            ProcessRequest::new("plan-session", message_trigger("think")),
        )
        .await
        .expect("outcome");

        assert_eq!(outcome.stop_reason, StopReason::Responded);
        let snapshot = session(&store, "plan-session").await;
        assert!(snapshot.records.iter().any(|record| matches!(
            record,
            SessionRecord::Deliberation(deliberation)
                if deliberation.summary == "inspect then act"
                    && deliberation.outcome == DeliberationOutcome::ReadyToAct
        )));
    }
}
