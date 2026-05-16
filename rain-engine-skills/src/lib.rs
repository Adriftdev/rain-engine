//! Built-in native skills for RainEngine.
//!
//! These skills give the agent "hands" — the ability to interact with
//! the local filesystem, execute commands, make HTTP requests, and
//! search session memory.

pub mod file_ops;
pub mod http_fetch;
pub mod memory_search;
pub mod shell_exec;

use rain_engine_core::SkillManifest;

/// Helper to build a SkillManifest with common defaults.
fn base_manifest(name: &str, description: &str, input_schema: serde_json::Value) -> SkillManifest {
    SkillManifest {
        name: name.to_string(),
        description: description.to_string(),
        input_schema,
        required_scopes: vec!["tool:run".to_string()],
        capability_grants: vec![],
        resource_policy: rain_engine_core::ResourcePolicy {
            timeout_ms: 30_000,
            max_memory_bytes: 16 * 1024 * 1024,
            max_fuel: None,
            priority_class: 0,
            max_retries: 0,
            retry_backoff_ms: 250,
            dry_run_supported: false,
        },
        approval_required: false,
    }
}
