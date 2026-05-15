//! Wake executor — polls for due WakeRequestRecords and fires ScheduledWake triggers.
//!
//! This is the cron-equivalent for RainEngine. Tasks and the cognition
//! planner emit WakeScheduled kernel events; this executor picks them
//! up when they become due.

use rain_engine_client::RainEngineClient;
use rain_engine_core::MemoryStore;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub struct WakeExecutor {
    client: RainEngineClient,
    memory: Arc<dyn MemoryStore>,
    poll_interval: Duration,
}

impl WakeExecutor {
    pub fn new(gateway_url: &str, memory: Arc<dyn MemoryStore>, poll_interval: Duration) -> Self {
        Self {
            client: RainEngineClient::new(gateway_url).expect("failed to init client"),
            memory,
            poll_interval,
        }
    }

    pub async fn run(&self, cancel: CancellationToken) {
        info!("Wake executor running");

        let mut interval = tokio::time::interval(self.poll_interval);
        interval.tick().await; // skip immediate

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("Wake executor stopped");
                    return;
                }
                _ = interval.tick() => {
                    self.check_and_fire().await;
                }
            }
        }
    }

    async fn check_and_fire(&self) {
        // Scan all sessions for pending wake requests that are due
        let sessions = match self
            .memory
            .list_sessions(rain_engine_core::SessionListQuery {
                offset: 0,
                limit: 1000,
                since_ms: None,
                until_ms: None,
            })
            .await
        {
            Ok(sessions) => sessions,
            Err(err) => {
                warn!("Wake executor: failed to list sessions: {}", err.message);
                return;
            }
        };

        let now = SystemTime::now();

        for session in sessions {
            let snapshot = match self.memory.load_session(&session.session_id).await {
                Ok(snap) => snap,
                Err(_) => continue,
            };

            let state = snapshot.agent_state();
            if let Some(wake) = state.pending_wake
                && wake.due_at <= now
            {
                info!(
                    session_id = %session.session_id,
                    wake_id = %wake.wake_id.0,
                    "Firing scheduled wake"
                );

                let due_at_ms = wake
                    .due_at
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);

                match self
                    .client
                    .send_scheduled_wake(
                        &session.session_id,
                        &wake.wake_id.0,
                        due_at_ms,
                        &wake.reason,
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(err) => {
                        warn!(
                            session_id = %session.session_id,
                            "Wake trigger failed: {err}"
                        );
                    }
                }
            }
        }
    }
}
