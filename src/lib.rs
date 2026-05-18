//! # RainEngine
//!
//! `rain-engine` is the single public entrypoint for the RainEngine workspace.
//! The implementation remains split into focused crates, but consumers can
//! depend on this crate and opt into integrations with feature flags.
//!
//! ## Feature guide
//!
//! - core kernel: always available
//! - retrieval utilities: `memory`
//! - blob backends: `blob`
//! - cognition helpers: `cognition`
//! - WASM skill execution: `wasm`
//! - runtime HTTP surface: `runtime`
//! - runtime HTTP client: `client`
//! - channel adapters: `channels`
//! - Gemini provider: `provider-gemini`
//! - OpenAI-compatible provider: `provider-openai`
//! - SQLite store: `store-sqlite`
//! - Postgres store: `store-pg`
//! - Valkey coordination store: `store-valkey`
//!
//! The default feature set stays lightweight and enables `memory` plus `blob`.

#[cfg(feature = "runtime")]
mod server;

/// Provider-neutral kernel types, traits, and execution primitives.
pub mod kernel {
    pub use rain_engine_core::*;
}

/// Retrieval helpers built on top of the kernel ledger.
#[cfg(feature = "memory")]
pub mod memory {
    pub use rain_engine_memory::*;
}

/// Blob backends for multimodal attachment storage.
#[cfg(feature = "blob")]
pub mod blob {
    pub use rain_engine_blob::*;
}

/// Optional cognition and planning helpers layered over the kernel.
#[cfg(feature = "cognition")]
pub mod cognition {
    pub use rain_engine_cognition::*;
}

/// WASM skill execution and capability hosts.
#[cfg(feature = "wasm")]
pub mod wasm {
    pub use rain_engine_wasm::*;
}

/// Runtime helpers, bootstrap config, and HTTP integration surface.
#[cfg(feature = "runtime")]
pub mod runtime {
    pub use crate::server::ServerBuilder;
    pub use rain_engine_runtime::*;
}

/// Runtime HTTP client for embedding or testing runtime-backed deployments.
#[cfg(feature = "client")]
pub mod client {
    pub use rain_engine_client::*;
}

/// Provider integrations.
pub mod providers {
    #[cfg(feature = "provider-openai")]
    pub use rain_engine_openai::*;
    #[cfg(feature = "provider-gemini")]
    pub use rain_engine_provider_gemini::*;
}

/// Durable and coordination store integrations.
pub mod stores {
    #[cfg(feature = "store-pg")]
    pub use rain_engine_store_pg::*;
    #[cfg(feature = "store-sqlite")]
    pub use rain_engine_store_sqlite::*;
    #[cfg(feature = "store-valkey")]
    pub use rain_engine_store_valkey::*;
}

/// Channel adapters that translate external messages into runtime events.
#[cfg(feature = "channels")]
pub mod channels {
    pub use rain_engine_channels::*;
}

/// Common top-level kernel re-exports for quickstarts.
pub use rain_engine_core::{
    AdvanceRequest, AdvanceResult, AgentAction, AgentEngine, AgentTrigger, ApprovalDecision,
    AttachmentRef, CorrelationId, EngineOutcome, MultimodalPayload, ProcessRequest, WakeId,
};

#[cfg(feature = "client")]
pub use rain_engine_client::RainEngineClient as Client;

#[cfg(feature = "runtime")]
pub use server::ServerBuilder;
