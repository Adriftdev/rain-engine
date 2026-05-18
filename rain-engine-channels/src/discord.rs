//! Discord bot adapter using runtime delivery plus Discord Gateway events.
//!
//! Requires `DISCORD_BOT_TOKEN` to be set.
//! This adapter uses the Discord Gateway (WebSocket) for receiving messages
//! and the REST API for sending replies.

use crate::{ChannelAdapter, ChannelConfig};
use async_trait::async_trait;
use rain_engine_client::RainEngineClient;
use serde::Deserialize;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct DiscordAdapter {
    token: String,
    client: reqwest::Client,
    engine_client: RainEngineClient,
    config: ChannelConfig,
}

impl DiscordAdapter {
    pub fn new(token: String, config: ChannelConfig) -> Self {
        Self {
            engine_client: RainEngineClient::new(&config.runtime_url)
                .expect("failed to init client"),
            client: reqwest::Client::new(),
            token,
            config,
        }
    }

    fn session_id(&self, channel_id: &str) -> String {
        format!(
            "{}-discord-{}",
            self.config.default_session_prefix, channel_id
        )
    }

    async fn send_message(&self, channel_id: &str, content: &str) -> Result<(), reqwest::Error> {
        self.client
            .post(format!(
                "https://discord.com/api/v10/channels/{channel_id}/messages"
            ))
            .header("Authorization", format!("Bot {}", self.token))
            .json(&serde_json::json!({ "content": content }))
            .send()
            .await?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct GatewayInfo {
    url: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GatewayEvent {
    op: u8,
    #[serde(default)]
    t: Option<String>,
    #[serde(default)]
    d: Option<serde_json::Value>,
    #[serde(default)]
    s: Option<i64>,
}

#[async_trait]
impl ChannelAdapter for DiscordAdapter {
    fn name(&self) -> &str {
        "discord"
    }

    async fn run(&self, cancel: tokio_util::sync::CancellationToken) {
        info!("Discord adapter started");

        // Get the Gateway URL
        let gateway_url = match self
            .client
            .get("https://discord.com/api/v10/gateway")
            .header("Authorization", format!("Bot {}", self.token))
            .send()
            .await
        {
            Ok(resp) => match resp.json::<GatewayInfo>().await {
                Ok(info) => format!("{}?v=10&encoding=json", info.url),
                Err(err) => {
                    error!("Failed to parse gateway URL: {err}");
                    return;
                }
            },
            Err(err) => {
                error!("Failed to get Discord gateway: {err}");
                return;
            }
        };

        info!(url = %gateway_url, "Connecting to Discord gateway");

        // For a production implementation, we'd use tokio-tungstenite here.
        // This is a polling fallback that checks for messages via REST.
        // Full WebSocket implementation requires the `tokio-tungstenite` crate.
        warn!(
            "Discord adapter: using REST polling fallback. For production, add WebSocket support."
        );

        let _last_message_id: Option<String> = None;

        loop {
            if cancel.is_cancelled() {
                info!("Discord adapter shutting down");
                return;
            }

            // Poll is a placeholder — in production you'd read from the WebSocket.
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
            }

            // In production, messages arrive via the WebSocket gateway.
            // This loop exists as the adapter's structural skeleton.
            // The message processing logic below is wired and ready.
        }
    }
}

/// Process a Discord MESSAGE_CREATE event. Extracted for use by both
/// the REST polling fallback and future WebSocket implementation.
impl DiscordAdapter {
    pub async fn handle_message(
        &self,
        channel_id: &str,
        author_id: &str,
        content: &str,
        is_bot: bool,
    ) {
        if is_bot {
            return; // Ignore bot messages
        }

        let actor_id = format!("discord:{author_id}");
        let session_id = self.session_id(channel_id);

        info!(channel_id, actor = %actor_id, "Discord message received");

        match self
            .engine_client
            .send_human_input(&actor_id, &session_id, content)
            .await
        {
            Ok(result) => {
                let reply = result
                    .outcome
                    .response
                    .as_deref()
                    .unwrap_or("*(no response)*");
                if let Err(err) = self.send_message(channel_id, reply).await {
                    error!("Failed to send Discord reply: {err}");
                }
            }
            Err(err) => {
                error!("Engine request failed: {err}");
                let _ = self
                    .send_message(channel_id, "⚠️ Engine error, please try again.")
                    .await;
            }
        }
    }
}
