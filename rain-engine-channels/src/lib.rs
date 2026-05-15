//! Multi-channel adapters for RainEngine.
//!
//! Each adapter connects to a messaging platform, normalizes inbound
//! messages into `HumanInputIngressRequest` triggers, and sends agent
//! responses back to the platform.

pub mod discord;
pub mod slack;
pub mod telegram;

use async_trait::async_trait;

/// Configuration for a channel adapter.
#[derive(Debug, Clone)]
pub struct ChannelConfig {
    /// Base URL of the RainEngine gateway.
    pub gateway_url: String,
    /// Session ID to use (or derive from platform user ID).
    pub default_session_prefix: String,
}

/// Trait implemented by each channel adapter.
#[async_trait]
pub trait ChannelAdapter: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Start the adapter. Runs until cancelled.
    async fn run(&self, cancel: tokio_util::sync::CancellationToken);
}
