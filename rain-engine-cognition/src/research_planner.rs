use async_trait::async_trait;
use rain_engine_core::{
    AgentAction, AgentStateSnapshot, AgentTrigger, GoalId, GoalRecord, GoalStatus, KernelEvent,
    LlmProvider, Planner, PlannerOutput, ProviderContentPart, ProviderMessage, ProviderRequest,
    ProviderRole, TaskId, TaskRecord, TaskStatus,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

pub struct ResearchPlanner {
    llm: Arc<dyn LlmProvider>,
}

impl ResearchPlanner {
    pub fn new(llm: Arc<dyn LlmProvider>) -> Self {
        Self { llm }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ResearchPlan {
    goal_title: String,
    goal_detail: String,
    tasks: Vec<PlannedTask>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PlannedTask {
    id: String,
    title: String,
    detail: String,
    depends_on: Vec<String>,
}

#[async_trait]
impl Planner for ResearchPlanner {
    async fn plan(&self, state: &AgentStateSnapshot, trigger: &AgentTrigger) -> PlannerOutput {
        // Only plan if we have no active tasks and this is a new message/input
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

        if has_active_tasks {
            return PlannerOutput::default();
        }

        let content = match trigger {
            AgentTrigger::HumanInput { content, .. } | AgentTrigger::Message { content, .. } => {
                content
            }
            _ => return PlannerOutput::default(),
        };

        let system_prompt = r#"You are a Research Architect. Your job is to decompose a high-level research goal into a directed acyclic graph (DAG) of specific tasks.
Output your plan as a single JSON object with this structure:
{
  "goal_title": "Short title",
  "goal_detail": "Detailed explanation",
  "tasks": [
    {
      "id": "task-1",
      "title": "Task title",
      "detail": "What to do",
      "depends_on": []
    },
    ...
  ]
}
Each task should be atomic. Use "depends_on" to specify which task IDs must be completed before a task can start."#;

        let request = ProviderRequest {
            trigger: trigger.clone(),
            context: rain_engine_core::AgentContextSnapshot {
                session_id: state.agent_id.0.clone(),
                granted_scopes: vec!["scope:research".to_string()],
                trigger_id: "planning-trigger".to_string(),
                idempotency_key: None,
                current_step: 0,
                max_steps: 1,
                history: Vec::new(),
                prior_tool_results: Vec::new(),
                session_cost_usd: 0.0,
                state: state.clone(),
                policy: Default::default(),
                active_execution_plan: None,
            },
            available_skills: Vec::new(),
            config: Default::default(),
            policy: Default::default(),
            contents: vec![
                ProviderMessage {
                    role: ProviderRole::System,
                    parts: vec![ProviderContentPart::Text(system_prompt.to_string())],
                },
                ProviderMessage {
                    role: ProviderRole::User,
                    parts: vec![ProviderContentPart::Text(format!("Goal: {}", content))],
                },
            ],
        };

        let decision = match self.llm.generate_action(request).await {
            Ok(d) => d,
            Err(_) => return PlannerOutput::default(),
        };

        let response_text = match decision.action {
            AgentAction::Respond { content } => content,
            _ => return PlannerOutput::default(),
        };

        // Attempt to parse the JSON plan
        let plan: ResearchPlan = match serde_json::from_str(&extract_json(&response_text)) {
            Ok(p) => p,
            Err(_) => return PlannerOutput::default(),
        };

        let mut events = Vec::new();
        let goal_id = GoalId(format!("goal-{}", state.goals.len() + 1));

        events.push(KernelEvent::GoalCreated(GoalRecord {
            goal_id: goal_id.clone(),
            created_at: SystemTime::now(),
            title: plan.goal_title,
            detail: Some(plan.goal_detail),
            status: GoalStatus::Active,
            parent_goal_id: None,
        }));

        let mut id_map = HashMap::new();
        for (i, planned_task) in plan.tasks.into_iter().enumerate() {
            let task_id = TaskId(format!("task-{}", state.tasks.len() + i + 1));
            id_map.insert(planned_task.id, task_id.clone());

            let mut blocked_by = Vec::new();
            for dep in planned_task.depends_on {
                if let Some(dep_id) = id_map.get(&dep) {
                    blocked_by.push(dep_id.clone());
                }
            }

            events.push(KernelEvent::TaskPlanned(TaskRecord {
                task_id,
                goal_id: Some(goal_id.clone()),
                parent_task_id: None,
                created_at: SystemTime::now(),
                title: planned_task.title,
                detail: Some(planned_task.detail),
                status: if blocked_by.is_empty() {
                    TaskStatus::Ready
                } else {
                    TaskStatus::Blocked
                },
                assignee: None,
                blocked_by,
            }));
        }

        PlannerOutput {
            events,
            proposed_plan: None,
        }
    }
}

fn extract_json(text: &str) -> String {
    let text = text.trim();
    if let Some(start) = text.find('{')
        && let Some(end) = text.rfind('}')
    {
        return text[start..=end].to_string();
    }
    text.to_string()
}
