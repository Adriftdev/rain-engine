//! Heartbeat scheduler for runtime-driven wake events.
//!
//! Respects active hours to avoid wake-ups during off-hours.

use rain_engine_client::RainEngineClient;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub struct HeartbeatScheduler {
    client: RainEngineClient,
    session_id: String,
    interval: Duration,
    active_hours: (u8, u8), // (start_hour, end_hour) in local time
}

impl HeartbeatScheduler {
    pub fn new(
        gateway_url: &str,
        session_id: String,
        interval: Duration,
        active_hours: (u8, u8),
    ) -> Self {
        Self {
            client: RainEngineClient::new(gateway_url).expect("failed to init client"),
            session_id,
            interval,
            active_hours,
        }
    }

    fn is_active_hour(&self) -> bool {
        // Use wall-clock hour. In production, make this timezone-aware.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let hour_of_day = ((now % 86400) / 3600) as u8;
        hour_of_day >= self.active_hours.0 && hour_of_day < self.active_hours.1
    }

    pub async fn run(&self, cancel: CancellationToken) {
        info!(
            interval_secs = self.interval.as_secs(),
            session = %self.session_id,
            "Heartbeat scheduler running"
        );

        let mut interval = tokio::time::interval(self.interval);
        // Skip the first immediate tick
        interval.tick().await;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("Heartbeat scheduler stopped");
                    return;
                }
                _ = interval.tick() => {
                    if !self.is_active_hour() {
                        continue;
                    }

                    info!("Heartbeat firing");
                    match self
                        .client
                        .send_human_input(
                            "heartbeat",
                            &self.session_id,
                            "HEARTBEAT: Review pending tasks, check for notifications, and perform any scheduled maintenance.",
                        )
                        .await
                    {
                        Ok(result) => {
                            if let Some(response) = &result.outcome.response {
                                if response.contains("HEARTBEAT_OK") {
                                    // Agent has nothing to do — suppress
                                } else {
                                    info!(response = %response, "Heartbeat produced output");
                                }
                            }
                        }
                        Err(err) => {
                            warn!("Heartbeat failed: {err}");
                        }
                    }
                }
            }
        }
    }
}
