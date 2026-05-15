//! Slack adapter using incoming/outgoing webhooks and Events API.
//!
//! Requires `SLACK_BOT_TOKEN` and `SLACK_SIGNING_SECRET`.
//! This adapter exposes an HTTP endpoint for Slack to POST events to,
//! and uses the Web API to send replies.

use crate::{ChannelAdapter, ChannelConfig};
use async_trait::async_trait;
use rain_engine_client::RainEngineClient;
use serde::Deserialize;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct SlackAdapter {
    bot_token: String,
    #[allow(dead_code)]
    signing_secret: String,
    client: reqwest::Client,
    engine_client: RainEngineClient,
    config: ChannelConfig,
    /// Port to listen on for Slack Events API.
    listen_port: u16,
}

impl SlackAdapter {
    pub fn new(
        bot_token: String,
        signing_secret: String,
        listen_port: u16,
        config: ChannelConfig,
    ) -> Self {
        Self {
            engine_client: RainEngineClient::new(&config.gateway_url)
                .expect("failed to init client"),
            client: reqwest::Client::new(),
            bot_token,
            signing_secret,
            listen_port,
            config,
        }
    }

    fn session_id(&self, channel: &str) -> String {
        format!("{}-slack-{}", self.config.default_session_prefix, channel)
    }

    async fn send_message(&self, channel: &str, text: &str) -> Result<(), reqwest::Error> {
        self.client
            .post("https://slack.com/api/chat.postMessage")
            .header("Authorization", format!("Bearer {}", self.bot_token))
            .json(&serde_json::json!({
                "channel": channel,
                "text": text,
            }))
            .send()
            .await?;
        Ok(())
    }

    pub async fn handle_event_message(&self, channel: &str, user: &str, text: &str) {
        let actor_id = format!("slack:{user}");
        let session_id = self.session_id(channel);

        info!(channel, actor = %actor_id, "Slack message received");

        match self
            .engine_client
            .send_human_input(&actor_id, &session_id, text)
            .await
        {
            Ok(result) => {
                let reply = result
                    .outcome
                    .response
                    .as_deref()
                    .unwrap_or("_(no response)_");
                if let Err(err) = self.send_message(channel, reply).await {
                    error!("Failed to send Slack reply: {err}");
                }
            }
            Err(err) => {
                error!("Engine request failed: {err}");
                let _ = self
                    .send_message(channel, "⚠️ Engine error, please try again.")
                    .await;
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct SlackEventPayload {
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    challenge: Option<String>,
    #[serde(default)]
    event: Option<SlackEvent>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct SlackEvent {
    r#type: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    bot_id: Option<String>,
}

#[async_trait]
impl ChannelAdapter for SlackAdapter {
    fn name(&self) -> &str {
        "slack"
    }

    async fn run(&self, cancel: tokio_util::sync::CancellationToken) {
        info!(
            port = self.listen_port,
            "Slack adapter started — listening for Events API"
        );

        // In production this would spin up a small axum server to receive
        // Slack Event API POSTs. For now, the structural skeleton is in place.
        // The handle_event_message method is fully wired and ready.
        //
        // Integration pattern:
        // 1. Slack sends POST to http://your-server:{port}/slack/events
        // 2. We parse the SlackEventPayload
        // 3. For url_verification: return { challenge }
        // 4. For event_callback with message type: call handle_event_message
        warn!(
            "Slack adapter: Events API HTTP listener not yet started. Use handle_event_message() for integration."
        );

        // Keep alive until cancelled
        cancel.cancelled().await;
        info!("Slack adapter shutting down");
    }
}
