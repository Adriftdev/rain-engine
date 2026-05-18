use crate::{AccessPolicy, SharedAccessPolicy, shared_access_policy};
use async_trait::async_trait;
use headless_chrome::{Browser, LaunchOptions};
use rain_engine_core::{
    NativeSkill, SkillExecutionError, SkillFailureKind, SkillInvocation, SkillManifest,
};
use serde_json::{Value, json};
use std::time::Duration;
use tracing::{info, warn};

pub struct WebReaderSkill {
    policy: SharedAccessPolicy,
    timeout: Duration,
}

impl WebReaderSkill {
    pub fn new(allowed_hosts: std::collections::HashSet<String>, timeout: Duration) -> Self {
        Self {
            policy: shared_access_policy(allowed_hosts, false),
            timeout,
        }
    }

    pub fn permissive(timeout: Duration) -> Self {
        Self {
            policy: shared_access_policy(std::collections::HashSet::new(), true),
            timeout,
        }
    }

    pub fn with_shared_policy(policy: SharedAccessPolicy, timeout: Duration) -> Self {
        Self { policy, timeout }
    }

    async fn is_allowed(&self, url: &str) -> bool {
        let policy = self.policy.read().await;
        if policy.permissive {
            return true;
        }
        reqwest::Url::parse(url)
            .ok()
            .and_then(|parsed| parsed.host_str().map(|host| host.to_string()))
            .is_some_and(|host| policy.allowlist.contains(&host))
    }

    pub async fn access_policy(&self) -> AccessPolicy {
        self.policy.read().await.clone()
    }
}

pub fn manifest() -> SkillManifest {
    crate::base_manifest(
        "web_reader",
        "Browse a URL using a headless browser to handle dynamic (JavaScript-rendered) content. Returns the page HTML.",
        json!({
            "type": "object",
            "properties": {
                "url": { "type": "string", "description": "The URL to browse" },
                "wait_for_selector": { "type": "string", "description": "Optional CSS selector to wait for (e.g. '.content-ready') before returning" }
            },
            "required": ["url"]
        }),
    )
}

#[async_trait]
impl NativeSkill for WebReaderSkill {
    async fn execute(&self, invocation: SkillInvocation) -> Result<Value, SkillExecutionError> {
        let url = invocation.args["url"]
            .as_str()
            .ok_or_else(|| {
                SkillExecutionError::new(SkillFailureKind::InvalidResponse, "missing 'url' arg")
            })?
            .to_string();

        if !self.is_allowed(&url).await {
            warn!(url = %url, "web_reader: host not on allowlist");
            return Err(SkillExecutionError::new(
                SkillFailureKind::PermissionDenied,
                "host not allowed",
            ));
        }

        let wait_for_selector = invocation.args["wait_for_selector"]
            .as_str()
            .map(|s| s.to_string());

        info!(url = %url, "Launching headless browser for web_reader...");

        // headless_chrome is synchronous, so we move it to a blocking thread to avoid stalling the executor
        let task = tokio::task::spawn_blocking(move || {
            let browser = Browser::new(LaunchOptions::default())
                .map_err(|err| format!("Failed to launch browser: {err}"))?;

            let tab = browser
                .new_tab()
                .map_err(|err| format!("Failed to open tab: {err}"))?;

            tab.navigate_to(&url)
                .map_err(|err| format!("Navigation failed: {err}"))?;

            if let Some(selector) = wait_for_selector {
                tab.wait_for_element(&selector)
                    .map_err(|err| format!("Wait for selector '{selector}' failed: {err}"))?;
            } else {
                tab.wait_until_navigated()
                    .map_err(|err| format!("Wait for navigation failed: {err}"))?;
            }

            let content = tab
                .get_content()
                .map_err(|err| format!("Failed to get content: {err}"))?;

            Ok::<Value, String>(json!({
                "url": url,
                "content": content,
            }))
        });
        let result = tokio::time::timeout(self.timeout, task)
            .await
            .map_err(|_| {
                SkillExecutionError::new(SkillFailureKind::Timeout, "web_reader timed out")
            })?
            .map_err(|err| {
                SkillExecutionError::new(
                    SkillFailureKind::Internal,
                    format!("Task panicked: {err}"),
                )
            })?
            .map_err(|err| SkillExecutionError::new(SkillFailureKind::Internal, err))?;

        Ok(result)
    }

    fn executor_kind(&self) -> &'static str {
        "native:web_reader"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rain_engine_core::{
        AgentContextSnapshot, AgentId, AgentStateSnapshot, EnginePolicy, SkillInvocation,
    };

    fn invocation(url: &str) -> SkillInvocation {
        SkillInvocation {
            call_id: "call-1".to_string(),
            manifest: manifest(),
            args: json!({ "url": url }),
            dry_run: false,
            context: AgentContextSnapshot {
                session_id: "session".to_string(),
                granted_scopes: vec!["tool:run".to_string()],
                trigger_id: "trigger".to_string(),
                idempotency_key: None,
                current_step: 0,
                max_steps: 1,
                history: Vec::new(),
                prior_tool_results: Vec::new(),
                session_cost_usd: 0.0,
                state: AgentStateSnapshot {
                    agent_id: AgentId("session".to_string()),
                    profile: None,
                    goals: Vec::new(),
                    tasks: Vec::new(),
                    observations: Vec::new(),
                    artifacts: Vec::new(),
                    resources: Vec::new(),
                    relationships: Vec::new(),
                    pending_wake: None,
                },
                policy: EnginePolicy::default(),
                active_execution_plan: None,
            },
        }
    }

    #[tokio::test]
    async fn empty_allowlist_denies_by_default() {
        let skill = WebReaderSkill::new(std::collections::HashSet::new(), Duration::from_secs(1));
        let err = skill
            .execute(invocation("https://example.com"))
            .await
            .expect_err("empty allowlist denies");
        assert_eq!(err.kind, SkillFailureKind::PermissionDenied);
    }
}
