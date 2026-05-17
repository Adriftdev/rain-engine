#![allow(unused_imports)]

use crate::{AgentStateSnapshot, AgentTrigger, KernelEvent};
use async_trait::async_trait;

pub use crate::blob::{BlobStore, BlobStoreError, InMemoryBlobStore};
pub use crate::coordination::{
    CoordinationClaim, CoordinationError, CoordinationStore, InMemoryCoordinationStore,
};
pub use crate::engine::{NativeSkill, SkillExecutionError, SkillExecutor, WasmSkillExecutor};
pub use crate::llm::{LlmProvider, MockLlmProvider, ProviderError, ProviderErrorKind};
pub use crate::memory::{InMemoryMemoryStore, MemoryError, MemoryStore, MemoryStoreExt};
pub use crate::retrieval::{
    RetrievalError, RetrievalStore, RetrievedItem, RetrievedItemKind, WorkingSet,
};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct PlannerOutput {
    pub events: Vec<KernelEvent>,
    pub proposed_plan: Option<crate::ExecutionPlan>,
}

#[async_trait]
pub trait Planner: Send + Sync {
    async fn plan(&self, state: &AgentStateSnapshot, trigger: &AgentTrigger) -> PlannerOutput;
}

#[async_trait]
pub trait SkillStore: Send + Sync {
    async fn store_skill(
        &self,
        manifest: crate::SkillManifest,
        wasm_bytes: Vec<u8>,
    ) -> Result<(), String>;
    async fn list_skills(&self) -> Result<Vec<(crate::SkillManifest, Vec<u8>)>, String>;
    async fn remove_skill(&self, name: &str) -> Result<(), String>;
}

#[async_trait]
pub trait StateProjectionCache: Send + Sync {
    async fn get_projection(
        &self,
        session_id: &str,
    ) -> Result<Option<crate::SessionSnapshot>, String>;
    async fn set_projection(
        &self,
        session_id: &str,
        snapshot: crate::SessionSnapshot,
    ) -> Result<(), String>;
    async fn invalidate(&self, session_id: &str) -> Result<(), String>;
}
