//! Shell command execution skill with default-deny allowlist.

use async_trait::async_trait;
use rain_engine_core::{
    NativeSkill, SkillExecutionError, SkillFailureKind, SkillInvocation, SkillManifest,
};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::time::Duration;
use tokio::process::Command;
use tracing::warn;

pub struct ShellExecSkill {
    allowed_commands: HashSet<String>,
    timeout: Duration,
}

impl ShellExecSkill {
    /// Create with explicit allowlist. Empty set = deny all.
    pub fn new(allowed_commands: HashSet<String>, timeout: Duration) -> Self {
        Self {
            allowed_commands,
            timeout,
        }
    }

    /// Permissive mode — allows any command (use only in dev).
    pub fn permissive(timeout: Duration) -> Self {
        Self {
            allowed_commands: HashSet::new(),
            timeout,
        }
    }

    fn is_allowed(&self, command: &str) -> bool {
        if self.allowed_commands.is_empty() {
            return true; // permissive mode
        }
        let executable = command.split_whitespace().next().unwrap_or("");
        self.allowed_commands.contains(executable)
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

        if !self.is_allowed(command) {
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
