//! Telegram Bot API adapter using long-poll getUpdates.
//!
//! Requires `TELEGRAM_BOT_TOKEN` to be set.

use crate::{ChannelAdapter, ChannelConfig};
use async_trait::async_trait;
use rain_engine_client::RainEngineClient;
use serde::Deserialize;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct TelegramAdapter {
    token: String,
    client: reqwest::Client,
    engine_client: RainEngineClient,
    config: ChannelConfig,
}

impl TelegramAdapter {
    pub fn new(token: String, config: ChannelConfig) -> Self {
        Self {
            engine_client: RainEngineClient::new(&config.gateway_url)
                .expect("failed to init client"),
            client: reqwest::Client::new(),
            token,
            config,
        }
    }

    fn api_url(&self, method: &str) -> String {
        format!("https://api.telegram.org/bot{}/{}", self.token, method)
    }

    fn session_id(&self, chat_id: i64) -> String {
        format!(
            "{}-telegram-{}",
            self.config.default_session_prefix, chat_id
        )
    }

    async fn send_message(&self, chat_id: i64, text: &str) -> Result<(), reqwest::Error> {
        self.client
            .post(self.api_url("sendMessage"))
            .json(&serde_json::json!({
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown"
            }))
            .send()
            .await?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct TelegramResponse {
    ok: bool,
    result: Option<Vec<TelegramUpdate>>,
}

#[derive(Debug, Deserialize)]
struct TelegramUpdate {
    update_id: i64,
    message: Option<TelegramMessage>,
}

#[derive(Debug, Deserialize)]
struct TelegramMessage {
    chat: TelegramChat,
    from: Option<TelegramUser>,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramChat {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct TelegramUser {
    id: i64,
    #[allow(dead_code)]
    first_name: String,
}

#[async_trait]
impl ChannelAdapter for TelegramAdapter {
    fn name(&self) -> &str {
        "telegram"
    }

    async fn run(&self, cancel: tokio_util::sync::CancellationToken) {
        info!("Telegram adapter started");
        let mut offset: Option<i64> = None;

        loop {
            if cancel.is_cancelled() {
                info!("Telegram adapter shutting down");
                return;
            }

            let mut url = self.api_url("getUpdates");
            url.push_str("?timeout=30");
            if let Some(off) = offset {
                url.push_str(&format!("&offset={off}"));
            }

            let response = tokio::select! {
                _ = cancel.cancelled() => return,
                result = self.client.get(&url).send() => {
                    match result {
                        Ok(resp) => resp,
                        Err(err) => {
                            warn!("Telegram poll error: {err}");
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            continue;
                        }
                    }
                }
            };

            let updates: TelegramResponse = match response.json().await {
                Ok(parsed) => parsed,
                Err(err) => {
                    warn!("Telegram parse error: {err}");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            if !updates.ok {
                warn!("Telegram API returned ok=false");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }

            let Some(results) = updates.result else {
                continue;
            };

            for update in results {
                offset = Some(update.update_id + 1);

                let Some(message) = update.message else {
                    continue;
                };
                let Some(text) = message.text else {
                    continue;
                };

                let actor_id = message
                    .from
                    .as_ref()
                    .map(|u| format!("telegram:{}", u.id))
                    .unwrap_or_else(|| format!("telegram:{}", message.chat.id));
                let session_id = self.session_id(message.chat.id);

                info!(
                    chat_id = message.chat.id,
                    actor = %actor_id,
                    "Telegram message received"
                );

                match self
                    .engine_client
                    .send_human_input(&actor_id, &session_id, &text)
                    .await
                {
                    Ok(result) => {
                        let reply = result
                            .outcome
                            .response
                            .as_deref()
                            .unwrap_or("*(no response)*");
                        if let Err(err) = self.send_message(message.chat.id, reply).await {
                            error!("Failed to send Telegram reply: {err}");
                        }
                    }
                    Err(err) => {
                        error!("Engine request failed: {err}");
                        let _ = self
                            .send_message(message.chat.id, "⚠️ Engine error, please try again.")
                            .await;
                    }
                }
            }
        }
    }
}
