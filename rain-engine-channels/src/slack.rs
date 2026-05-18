//! Slack adapter using incoming/outgoing webhooks and Events API.
//!
//! Requires `SLACK_BOT_TOKEN` and `SLACK_SIGNING_SECRET`.
//! This adapter exposes an HTTP endpoint for Slack to POST events to,
//! and uses the Web API to send replies.

use crate::{ChannelAdapter, ChannelConfig};
use async_trait::async_trait;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use rain_engine_client::RainEngineClient;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
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
            engine_client: RainEngineClient::new(&config.runtime_url)
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
        warn!("Slack request signature verification is not enforced yet");

        let app = Router::new()
            .route("/slack/events", post(handle_events))
            .with_state(Arc::new(self.clone()));

        let listener = match tokio::net::TcpListener::bind(("0.0.0.0", self.listen_port)).await {
            Ok(listener) => listener,
            Err(err) => {
                error!("Slack adapter failed to bind: {err}");
                return;
            }
        };

        if let Err(err) = axum::serve(listener, app)
            .with_graceful_shutdown(async move { cancel.cancelled().await })
            .await
        {
            error!("Slack adapter listener error: {err}");
        }

        info!("Slack adapter shutting down");
    }
}

async fn handle_events(
    State(adapter): State<Arc<SlackAdapter>>,
    Json(payload): Json<SlackEventPayload>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match payload.r#type.as_str() {
        "url_verification" => {
            let challenge = payload.challenge.unwrap_or_default();
            Ok(Json(json!({ "challenge": challenge })))
        }
        "event_callback" => {
            if let Some(event) = payload.event {
                let is_supported_message =
                    event.r#type == "message" || event.r#type == "app_mention";
                if is_supported_message
                    && let (Some(channel), Some(user), Some(text)) = (
                        event.channel.as_deref(),
                        event.user.as_deref(),
                        event.text.as_deref(),
                    )
                    && event.bot_id.is_none()
                {
                    adapter.handle_event_message(channel, user, text).await;
                }
            }
            Ok(Json(json!({ "ok": true })))
        }
        _ => Ok(Json(json!({ "ok": true }))),
    }
}
