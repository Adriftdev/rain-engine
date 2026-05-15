//! File read/write skills scoped to a workspace directory.

use async_trait::async_trait;
use rain_engine_core::{
    NativeSkill, SkillExecutionError, SkillFailureKind, SkillInvocation, SkillManifest,
};
use serde_json::{Value, json};
use std::path::PathBuf;
use tracing::warn;

// ---------------------------------------------------------------------------
// FileReadSkill
// ---------------------------------------------------------------------------

pub struct FileReadSkill {
    workspace_root: PathBuf,
}

impl FileReadSkill {
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            workspace_root: workspace_root.into(),
        }
    }

    fn resolve_path(&self, relative: &str) -> Result<PathBuf, SkillExecutionError> {
        let resolved = self.workspace_root.join(relative);
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
        let content = tokio::fs::read_to_string(&path).await.map_err(|err| {
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
        Self {
            workspace_root: workspace_root.into(),
        }
    }

    fn resolve_path(&self, relative: &str) -> Result<PathBuf, SkillExecutionError> {
        let resolved = self.workspace_root.join(relative);
        // For writes, we can't canonicalize if the file doesn't exist yet.
        // Check that the parent exists and is within workspace.
        let parent = resolved
            .parent()
            .ok_or_else(|| SkillExecutionError::new(SkillFailureKind::Internal, "invalid path"))?;
        if parent.exists() {
            let canonical_parent = parent.canonicalize().map_err(|err| {
                SkillExecutionError::new(SkillFailureKind::Internal, format!("path error: {err}"))
            })?;
            if !canonical_parent.starts_with(&self.workspace_root) {
                warn!(path = %relative, "file_write: path traversal blocked");
                return Err(SkillExecutionError::new(
                    SkillFailureKind::PermissionDenied,
                    "path traversal outside workspace",
                ));
            }
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
            tokio::fs::create_dir_all(parent).await.map_err(|err| {
                SkillExecutionError::new(SkillFailureKind::Internal, format!("mkdir failed: {err}"))
            })?;
        }

        tokio::fs::write(&path, content).await.map_err(|err| {
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
