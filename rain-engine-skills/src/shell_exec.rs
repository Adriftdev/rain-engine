//! Shell command execution skill with default-deny allowlist.

use crate::{AccessPolicy, SharedAccessPolicy, shared_access_policy};
use async_trait::async_trait;
use rain_engine_core::{
    NativeSkill, SkillExecutionError, SkillFailureKind, SkillInvocation, SkillManifest,
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::process::Command;
use tracing::warn;

pub struct ShellExecSkill {
    policy: SharedAccessPolicy,
    timeout: Duration,
}

impl ShellExecSkill {
    /// Create with explicit allowlist. Empty set = deny all.
    pub fn new(allowed_commands: std::collections::HashSet<String>, timeout: Duration) -> Self {
        Self {
            policy: shared_access_policy(allowed_commands, false),
            timeout,
        }
    }

    /// Permissive mode — allows any command (use only in dev).
    pub fn permissive(timeout: Duration) -> Self {
        Self {
            policy: shared_access_policy(std::collections::HashSet::new(), true),
            timeout,
        }
    }

    pub fn with_shared_policy(policy: SharedAccessPolicy, timeout: Duration) -> Self {
        Self { policy, timeout }
    }

    async fn is_allowed(&self, command: &str) -> bool {
        let policy = self.policy.read().await;
        if policy.permissive {
            return true;
        }
        let executable = command.split_whitespace().next().unwrap_or("");
        policy.allowlist.contains(executable)
    }

    pub async fn access_policy(&self) -> AccessPolicy {
        self.policy.read().await.clone()
    }
}

pub fn manifest() -> SkillManifest {
    crate::base_manifest(
        "shell_exec",
        "Execute a shell command and return stdout/stderr. Commands must be on the allowlist.",
        json!({
            "type": "object",
            "properties": {
                "command": { "type": "string", "description": "The shell command to execute" },
                "working_dir": { "type": "string", "description": "Optional working directory" }
            },
            "required": ["command"]
        }),
    )
}

#[async_trait]
impl NativeSkill for ShellExecSkill {
    async fn execute(&self, invocation: SkillInvocation) -> Result<Value, SkillExecutionError> {
        let command = invocation.args["command"].as_str().ok_or_else(|| {
            SkillExecutionError::new(SkillFailureKind::InvalidResponse, "missing 'command' arg")
        })?;

        if !self.is_allowed(command).await {
            warn!(command = %command, "shell_exec: command not on allowlist");
            return Err(SkillExecutionError::new(
                SkillFailureKind::PermissionDenied,
                format!(
                    "command not allowed: {}",
                    command.split_whitespace().next().unwrap_or("")
                ),
            ));
        }

        let working_dir = invocation.args["working_dir"]
            .as_str()
            .map(|s| s.to_string());

        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(command);
        if let Some(dir) = &working_dir {
            cmd.current_dir(dir);
        }

        let output = match tokio::time::timeout(self.timeout, cmd.output()).await {
            Ok(Ok(output)) => output,
            Ok(Err(err)) => {
                return Err(SkillExecutionError::new(
                    SkillFailureKind::Internal,
                    err.to_string(),
                ));
            }
            Err(_) => {
                return Err(SkillExecutionError::new(
                    SkillFailureKind::Timeout,
                    "shell command timed out",
                ));
            }
        };

        Ok(json!({
            "exit_code": output.status.code(),
            "stdout": String::from_utf8_lossy(&output.stdout),
            "stderr": String::from_utf8_lossy(&output.stderr),
        }))
    }

    fn requires_human_approval(&self) -> bool {
        true
    }

    fn executor_kind(&self) -> &'static str {
        "native:shell_exec"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rain_engine_core::{
        AgentContextSnapshot, AgentId, AgentStateSnapshot, EnginePolicy, SkillInvocation,
    };

    fn invocation(command: &str) -> SkillInvocation {
        SkillInvocation {
            call_id: "call-1".to_string(),
            manifest: manifest(),
            args: json!({ "command": command }),
            dry_run: false,
            context: AgentContextSnapshot {
                session_id: "session".to_string(),
                granted_scopes: vec!["tool:run".to_string()],
                trigger_id: "trigger".to_string(),
                idempotency_key: None,
                current_step: 0,
                max_steps: 1,
                history: Vec::new(),
                prior_tool_results: Vec::new(),
                session_cost_usd: 0.0,
                state: AgentStateSnapshot {
                    agent_id: AgentId("session".to_string()),
                    profile: None,
                    goals: Vec::new(),
                    tasks: Vec::new(),
                    observations: Vec::new(),
                    artifacts: Vec::new(),
                    resources: Vec::new(),
                    relationships: Vec::new(),
                    pending_wake: None,
                },
                policy: EnginePolicy::default(),
                active_execution_plan: None,
            },
        }
    }

    #[tokio::test]
    async fn empty_allowlist_denies_by_default() {
        let skill = ShellExecSkill::new(std::collections::HashSet::new(), Duration::from_secs(1));
        let err = skill
            .execute(invocation("echo denied"))
            .await
            .expect_err("empty allowlist denies");
        assert_eq!(err.kind, SkillFailureKind::PermissionDenied);
    }

    #[tokio::test]
    async fn explicit_permissive_mode_allows_commands() {
        let skill = ShellExecSkill::permissive(Duration::from_secs(1));
        let output = skill
            .execute(invocation("printf allowed"))
            .await
            .expect("permissive command");
        assert_eq!(output["stdout"], json!("allowed"));
    }
}
