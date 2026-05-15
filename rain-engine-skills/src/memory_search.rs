//! Memory search skill — wraps the retrieval store for semantic search.

use async_trait::async_trait;
use rain_engine_core::{
    MemoryStore, NativeSkill, RetrievalStore, SkillExecutionError, SkillFailureKind,
    SkillInvocation, SkillManifest,
};
use rain_engine_memory::SessionRetrievalStore;
use serde_json::{Value, json};
use std::sync::Arc;

pub struct MemorySearchSkill {
    retrieval: SessionRetrievalStore,
}

impl MemorySearchSkill {
    pub fn new(memory: Arc<dyn MemoryStore>) -> Self {
        Self {
            retrieval: SessionRetrievalStore::new(memory),
        }
    }
}

pub fn manifest() -> SkillManifest {
    crate::base_manifest(
        "memory_search",
        "Search session memory for observations, tasks, and goals matching a query.",
        json!({
            "type": "object",
            "properties": {
                "query": { "type": "string", "description": "Search query" },
                "session_id": { "type": "string", "description": "Session to search (defaults to current)" },
                "limit": { "type": "integer", "description": "Max results", "default": 10 }
            },
            "required": ["query"]
        }),
    )
}

#[async_trait]
impl NativeSkill for MemorySearchSkill {
    async fn execute(&self, invocation: SkillInvocation) -> Result<Value, SkillExecutionError> {
        let query = invocation.args["query"].as_str().ok_or_else(|| {
            SkillExecutionError::new(SkillFailureKind::InvalidResponse, "missing 'query' arg")
        })?;

        let session_id = invocation.args["session_id"]
            .as_str()
            .unwrap_or(&invocation.context.session_id);

        let limit = invocation.args["limit"].as_u64().unwrap_or(10) as usize;

        let results = self
            .retrieval
            .semantic_search(session_id, query, limit)
            .await
            .map_err(|err| SkillExecutionError::new(SkillFailureKind::Internal, err.to_string()))?;

        Ok(json!({
            "query": query,
            "session_id": session_id,
            "results": results.iter().map(|item| json!({
                "kind": format!("{:?}", item.kind),
                "key": item.key,
                "score": item.score,
                "snippet": item.snippet,
            })).collect::<Vec<_>>(),
        }))
    }

    fn executor_kind(&self) -> &'static str {
        "native:memory_search"
    }
}
