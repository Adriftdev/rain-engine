//! Optional planning and task-graph orchestration for RainEngine.
//!
//! This crate composes over `rain-engine-core`; the kernel remains useful
//! without it.

use async_trait::async_trait;
use rain_engine_core::{
    AgentAction, AgentStateSnapshot, AgentTrigger, GoalId, GoalRecord, GoalStatus, KernelEvent,
    ResumeToken, TaskId, TaskRecord, TaskStatus, WakeId, WakeRequestRecord,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorPolicy {
    pub max_active_tasks: usize,
}

impl Default for ExecutorPolicy {
    fn default() -> Self {
        Self {
            max_active_tasks: 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewPolicy {
    pub require_review_for_delegation: bool,
    pub approval_scope: String,
}

impl Default for ReviewPolicy {
    fn default() -> Self {
        Self {
            require_review_for_delegation: true,
            approval_scope: "scope:human_approval".to_string(),
        }
    }
}

impl ReviewPolicy {
    pub fn requires_human_review(&self, required_scopes: &[String]) -> bool {
        required_scopes
            .iter()
            .any(|scope| scope == &self.approval_scope)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakePolicy {
    pub schedule_follow_up: bool,
    pub follow_up_ms: u64,
}

impl Default for WakePolicy {
    fn default() -> Self {
        Self {
            schedule_follow_up: true,
            follow_up_ms: 30 * 60 * 1000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReflectionPolicy {
    pub completed_tasks_before_review: usize,
    pub failed_tasks_before_replan: usize,
}

impl Default for ReflectionPolicy {
    fn default() -> Self {
        Self {
            completed_tasks_before_review: 3,
            failed_tasks_before_replan: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRoute {
    pub task_id: TaskId,
    pub lane: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentKernelProfile {
    pub planning_cadence: String,
    pub max_active_tasks: usize,
    pub reflection_threshold: usize,
    pub wake_policy: String,
    pub human_approval_policy: String,
}

impl Default for AgentKernelProfile {
    fn default() -> Self {
        Self {
            planning_cadence: "event".to_string(),
            max_active_tasks: 4,
            reflection_threshold: 2,
            wake_policy: "external".to_string(),
            human_approval_policy: "scoped".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannerOutput {
    pub events: Vec<KernelEvent>,
    pub suggested_action: Option<AgentAction>,
}

#[async_trait]
pub trait Planner: Send + Sync {
    async fn plan(&self, state: &AgentStateSnapshot, trigger: &AgentTrigger) -> PlannerOutput;
}

#[async_trait]
pub trait TaskRouter: Send + Sync {
    async fn route(&self, task: &TaskRecord) -> TaskRoute;
}

pub trait ReviewPolicyDecider: Send + Sync {
    fn review_policy(&self) -> ReviewPolicy;
}

#[derive(Debug, Clone, Default)]
pub struct MinimalTaskGraphPlanner;

#[async_trait]
impl Planner for MinimalTaskGraphPlanner {
    async fn plan(&self, state: &AgentStateSnapshot, trigger: &AgentTrigger) -> PlannerOutput {
        let has_active_tasks = state.tasks.iter().any(|task| {
            matches!(
                task.status,
                TaskStatus::Pending
                    | TaskStatus::Ready
                    | TaskStatus::Running
                    | TaskStatus::Blocked
                    | TaskStatus::WaitingHuman
            )
        });

        match trigger {
            AgentTrigger::HumanInput { content, .. } | AgentTrigger::Message { content, .. }
                if !has_active_tasks =>
            {
                let goal = GoalRecord {
                    goal_id: GoalId(format!("goal-{}", state.goals.len() + 1)),
                    created_at: std::time::SystemTime::now(),
                    title: content.clone(),
                    detail: Some("created from user input".to_string()),
                    status: GoalStatus::Active,
                    parent_goal_id: None,
                };
                let task = TaskRecord {
                    task_id: TaskId(format!("task-{}", state.tasks.len() + 1)),
                    goal_id: Some(goal.goal_id.clone()),
                    parent_task_id: None,
                    created_at: std::time::SystemTime::now(),
                    title: format!("Investigate: {}", goal.title),
                    detail: Some("planned from new observation".to_string()),
                    status: TaskStatus::Ready,
                    assignee: None,
                    blocked_by: Vec::new(),
                };
                let wake = follow_up_wake(Some(task.task_id.clone()));
                PlannerOutput {
                    events: vec![
                        KernelEvent::GoalCreated(goal),
                        KernelEvent::TaskPlanned(task),
                        KernelEvent::WakeScheduled(wake),
                    ],
                    suggested_action: Some(AgentAction::Continue {
                        reason: rain_engine_core::ContinueReason::ModelRequested,
                    }),
                }
            }
            AgentTrigger::SystemObservation { source, .. }
            | AgentTrigger::ExternalEvent { source, .. }
                if !has_active_tasks =>
            {
                let goal = GoalRecord {
                    goal_id: GoalId(format!("goal-{}", state.goals.len() + 1)),
                    created_at: std::time::SystemTime::now(),
                    title: format!("Respond to {source}"),
                    detail: Some("created from external observation".to_string()),
                    status: GoalStatus::Active,
                    parent_goal_id: None,
                };
                let task = TaskRecord {
                    task_id: TaskId(format!("task-{}", state.tasks.len() + 1)),
                    goal_id: Some(goal.goal_id.clone()),
                    parent_task_id: None,
                    created_at: std::time::SystemTime::now(),
                    title: format!("Triage {source}"),
                    detail: Some("planned from system observation".to_string()),
                    status: TaskStatus::Ready,
                    assignee: None,
                    blocked_by: Vec::new(),
                };
                let wake = follow_up_wake(Some(task.task_id.clone()));
                PlannerOutput {
                    events: vec![
                        KernelEvent::GoalCreated(goal),
                        KernelEvent::TaskPlanned(task),
                        KernelEvent::WakeScheduled(wake),
                    ],
                    suggested_action: Some(AgentAction::Continue {
                        reason: rain_engine_core::ContinueReason::ModelRequested,
                    }),
                }
            }
            AgentTrigger::ScheduledWake { .. } => {
                if let Some(task) = state.tasks.iter().find(|task| {
                    matches!(
                        task.status,
                        TaskStatus::Ready | TaskStatus::Blocked | TaskStatus::WaitingHuman
                    )
                }) {
                    PlannerOutput {
                        events: vec![KernelEvent::TaskClaimed {
                            task_id: task.task_id.clone(),
                            claimed_at: std::time::SystemTime::now(),
                            assignee: Some("scheduler".to_string()),
                        }],
                        suggested_action: Some(AgentAction::Continue {
                            reason: rain_engine_core::ContinueReason::ModelRequested,
                        }),
                    }
                } else {
                    PlannerOutput {
                        events: Vec::new(),
                        suggested_action: Some(AgentAction::Yield {
                            reason: Some("no waiting tasks to resume".to_string()),
                        }),
                    }
                }
            }
            _ => PlannerOutput {
                events: Vec::new(),
                suggested_action: None,
            },
        }
    }
}

pub fn human_review_event(
    task_id: Option<TaskId>,
    prompt: impl Into<String>,
    resume_token: ResumeToken,
) -> KernelEvent {
    KernelEvent::HumanInputRequested {
        task_id,
        requested_at: std::time::SystemTime::now(),
        prompt: prompt.into(),
        resume_token,
    }
}

fn follow_up_wake(task_id: Option<TaskId>) -> WakeRequestRecord {
    let requested_at = std::time::SystemTime::now();
    WakeRequestRecord {
        wake_id: WakeId(format!(
            "wake-{}",
            requested_at
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        )),
        requested_at,
        due_at: requested_at + std::time::Duration::from_millis(WakePolicy::default().follow_up_ms),
        reason: "follow up on active task".to_string(),
        task_id,
    }
}

#[derive(Debug, Clone, Default)]
pub struct RoundRobinTaskRouter;

#[async_trait]
impl TaskRouter for RoundRobinTaskRouter {
    async fn route(&self, task: &TaskRecord) -> TaskRoute {
        TaskRoute {
            task_id: task.task_id.clone(),
            lane: if task.goal_id.is_some() {
                "goal-backed".to_string()
            } else {
                "default".to_string()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rain_engine_core::{AgentId, AgentStateSnapshot};

    #[tokio::test]
    async fn planner_creates_goal_and_task_for_new_input() {
        let planner = MinimalTaskGraphPlanner;
        let state = AgentStateSnapshot {
            agent_id: AgentId("agent-1".to_string()),
            profile: None,
            goals: Vec::new(),
            tasks: Vec::new(),
            observations: Vec::new(),
            artifacts: Vec::new(),
            resources: Vec::new(),
            relationships: Vec::new(),
            pending_wake: None,
        };
        let output = planner
            .plan(
                &state,
                &AgentTrigger::HumanInput {
                    actor_id: "user".to_string(),
                    content: "Investigate outage".to_string(),
                    attachments: Vec::new(),
                },
            )
            .await;

        assert_eq!(output.events.len(), 3);
        assert!(matches!(output.events[0], KernelEvent::GoalCreated(_)));
        assert!(matches!(output.events[1], KernelEvent::TaskPlanned(_)));
        assert!(matches!(output.events[2], KernelEvent::WakeScheduled(_)));
    }
}
