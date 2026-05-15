//! # RainEngine
//!
//! An event-sourced Rust kernel for building durable AI agent systems.
//! This crate provides a high-level server and client API for event-driven agent systems.

#[cfg(feature = "server")]
pub mod server;

#[cfg(feature = "server")]
pub use server::ServerBuilder;

#[cfg(feature = "client")]
pub use rain_engine_client::RainEngineClient as Client;

// Re-exports of common types for convenience
pub use rain_engine_core::{
    AdvanceRequest, AdvanceResult, AgentAction, AgentEngine, AgentTrigger, ApprovalDecision,
    AttachmentRef, CorrelationId, EngineOutcome, MultimodalPayload, ProcessRequest, WakeId,
};

#[cfg(feature = "server")]
pub use rain_engine_runtime::RuntimeRunResult;
