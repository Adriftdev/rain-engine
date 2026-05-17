//! File read/write skills scoped to a workspace directory.

use async_trait::async_trait;
use rain_engine_core::{
    NativeSkill, SkillExecutionError, SkillFailureKind, SkillInvocation, SkillManifest,
};
use serde_json::{Value, json};
use std::path::{Component, Path, PathBuf};
use tracing::warn;

// ---------------------------------------------------------------------------
// FileReadSkill
// ---------------------------------------------------------------------------

pub struct FileReadSkill {
    workspace_root: PathBuf,
}

impl FileReadSkill {
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        let workspace_root = canonical_workspace_root(workspace_root.into());
        Self { workspace_root }
    }

    fn resolve_path(&self, relative: &str) -> Result<PathBuf, SkillExecutionError> {
        let resolved = self.workspace_root.join(safe_relative_path(relative)?);
        let canonical = resolved.canonicalize().map_err(|err| {
            SkillExecutionError::new(SkillFailureKind::Internal, format!("path error: {err}"))
        })?;
        if !canonical.starts_with(&self.workspace_root) {
            warn!(path = %relative, "file_read: path traversal blocked");
            return Err(SkillExecutionError::new(
                SkillFailureKind::PermissionDenied,
                "path traversal outside workspace",
            ));
        }
        Ok(canonical)
    }
}

pub fn read_manifest() -> SkillManifest {
    crate::base_manifest(
        "file_read",
        "Read the contents of a file within the agent workspace.",
        json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "Relative path within the workspace" }
            },
            "required": ["path"]
        }),
    )
}

#[async_trait]
impl NativeSkill for FileReadSkill {
    async fn execute(&self, invocation: SkillInvocation) -> Result<Value, SkillExecutionError> {
        let relative = invocation.args["path"].as_str().ok_or_else(|| {
            SkillExecutionError::new(SkillFailureKind::InvalidResponse, "missing 'path' arg")
        })?;

        let path = self.resolve_path(relative)?;
        let content = std::fs::read_to_string(&path).map_err(|err| {
            SkillExecutionError::new(SkillFailureKind::Internal, format!("read failed: {err}"))
        })?;

        Ok(json!({
            "path": path.display().to_string(),
            "content": content,
            "size_bytes": content.len(),
        }))
    }

    fn executor_kind(&self) -> &'static str {
        "native:file_read"
    }
}

// ---------------------------------------------------------------------------
// FileWriteSkill
// ---------------------------------------------------------------------------

pub struct FileWriteSkill {
    workspace_root: PathBuf,
}

impl FileWriteSkill {
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        let workspace_root = canonical_workspace_root(workspace_root.into());
        Self { workspace_root }
    }

    fn resolve_path(&self, relative: &str) -> Result<PathBuf, SkillExecutionError> {
        let resolved = self.workspace_root.join(safe_relative_path(relative)?);
        let existing = nearest_existing_ancestor(&resolved)
            .ok_or_else(|| SkillExecutionError::new(SkillFailureKind::Internal, "invalid path"))?;
        let canonical_existing = existing.canonicalize().map_err(|err| {
            SkillExecutionError::new(SkillFailureKind::Internal, format!("path error: {err}"))
        })?;
        if !canonical_existing.starts_with(&self.workspace_root) {
            warn!(path = %relative, "file_write: path traversal blocked");
            return Err(SkillExecutionError::new(
                SkillFailureKind::PermissionDenied,
                "path traversal outside workspace",
            ));
        }
        Ok(resolved)
    }
}

pub fn write_manifest() -> SkillManifest {
    crate::base_manifest(
        "file_write",
        "Write content to a file within the agent workspace. Creates parent directories.",
        json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "Relative path within the workspace" },
                "content": { "type": "string", "description": "Content to write" }
            },
            "required": ["path", "content"]
        }),
    )
}

#[async_trait]
impl NativeSkill for FileWriteSkill {
    async fn execute(&self, invocation: SkillInvocation) -> Result<Value, SkillExecutionError> {
        let relative = invocation.args["path"].as_str().ok_or_else(|| {
            SkillExecutionError::new(SkillFailureKind::InvalidResponse, "missing 'path' arg")
        })?;
        let content = invocation.args["content"].as_str().ok_or_else(|| {
            SkillExecutionError::new(SkillFailureKind::InvalidResponse, "missing 'content' arg")
        })?;

        let path = self.resolve_path(relative)?;

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                SkillExecutionError::new(SkillFailureKind::Internal, format!("mkdir failed: {err}"))
            })?;
        }

        std::fs::write(&path, content).map_err(|err| {
            SkillExecutionError::new(SkillFailureKind::Internal, format!("write failed: {err}"))
        })?;

        Ok(json!({
            "path": path.display().to_string(),
            "bytes_written": content.len(),
        }))
    }

    fn requires_human_approval(&self) -> bool {
        true
    }

    fn executor_kind(&self) -> &'static str {
        "native:file_write"
    }
}

fn canonical_workspace_root(root: PathBuf) -> PathBuf {
    root.canonicalize().unwrap_or(root)
}

fn safe_relative_path(relative: &str) -> Result<PathBuf, SkillExecutionError> {
    let path = Path::new(relative);
    let mut cleaned = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => cleaned.push(part),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(SkillExecutionError::new(
                    SkillFailureKind::PermissionDenied,
                    "path traversal outside workspace",
                ));
            }
        }
    }
    if cleaned.as_os_str().is_empty() {
        return Err(SkillExecutionError::new(
            SkillFailureKind::InvalidResponse,
            "path must not be empty",
        ));
    }
    Ok(cleaned)
}

fn nearest_existing_ancestor(path: &Path) -> Option<PathBuf> {
    let mut current = path.to_path_buf();
    loop {
        if current.exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rain_engine_core::{
        AgentContextSnapshot, AgentId, AgentStateSnapshot, EnginePolicy, SkillInvocation,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_workspace() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("rain-engine-skills-{suffix}"));
        std::fs::create_dir_all(&root).expect("workspace");
        root
    }

    fn invocation(manifest: SkillManifest, args: Value) -> SkillInvocation {
        SkillInvocation {
            call_id: "call-1".to_string(),
            manifest,
            args,
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
    async fn file_read_blocks_parent_traversal() {
        let skill = FileReadSkill::new(temp_workspace());
        let err = skill
            .execute(invocation(
                read_manifest(),
                json!({ "path": "../secret.txt" }),
            ))
            .await
            .expect_err("traversal blocked");
        assert_eq!(err.kind, SkillFailureKind::PermissionDenied);
    }

    #[tokio::test]
    async fn file_write_blocks_traversal_even_when_parent_is_missing() {
        let skill = FileWriteSkill::new(temp_workspace());
        let err = skill
            .execute(invocation(
                write_manifest(),
                json!({ "path": "missing/../../escape.txt", "content": "nope" }),
            ))
            .await
            .expect_err("traversal blocked");
        assert_eq!(err.kind, SkillFailureKind::PermissionDenied);
    }
}
