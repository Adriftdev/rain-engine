//! OpenAI-compatible provider adapter for RainEngine.
//!
//! This baseline provider maps provider-neutral requests into chat completion
//! style tool calls.

use async_trait::async_trait;
use rain_engine_core::{
    AgentAction, LlmProvider, PlannedSkillCall, ProviderDecision, ProviderError, ProviderErrorKind,
    ProviderRequest, ProviderRequestConfig,
};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct OpenAiCompatibleConfig {
    pub base_url: String,
    pub api_key: String,
    pub default_request: ProviderRequestConfig,
    pub system_prompt: String,
}

impl OpenAiCompatibleConfig {
    pub fn validated(&self) -> Result<(), OpenAiConfigError> {
        if self.base_url.trim().is_empty() {
            return Err(OpenAiConfigError::Invalid(
                "base_url must not be empty".to_string(),
            ));
        }
        if self.api_key.trim().is_empty() {
            return Err(OpenAiConfigError::Invalid(
                "api_key must not be empty".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum OpenAiConfigError {
    #[error("{0}")]
    Invalid(String),
}

#[derive(Clone)]
pub struct OpenAiCompatibleProvider {
    client: Client,
    config: OpenAiCompatibleConfig,
}

impl OpenAiCompatibleProvider {
    pub fn new(config: OpenAiCompatibleConfig) -> Result<Self, OpenAiConfigError> {
        config.validated()?;
        Ok(Self {
            client: Client::new(),
            config,
        })
    }
}

#[async_trait]
impl LlmProvider for OpenAiCompatibleProvider {
    async fn generate_action(
        &self,
        input: ProviderRequest,
    ) -> Result<ProviderDecision, ProviderError> {
        let model = input
            .config
            .model
            .clone()
            .or_else(|| self.config.default_request.model.clone())
            .ok_or_else(|| {
                ProviderError::new(
                    ProviderErrorKind::Configuration,
                    "no model configured for OpenAI-compatible provider",
                    false,
                )
            })?;

        let request = ChatCompletionRequest {
            model,
            temperature: input
                .config
                .temperature
                .or(self.config.default_request.temperature),
            max_tokens: input
                .config
                .max_tokens
                .or(self.config.default_request.max_tokens),
            messages: map_to_chat_messages(&input, self.config.system_prompt.clone())?,
            tools: input
                .available_skills
                .iter()
                .map(|skill| ToolDefinition {
                    kind: "function".to_string(),
                    function: ToolFunction {
                        name: skill.manifest.name.clone(),
                        description: skill.manifest.description.clone(),
                        parameters: skill.manifest.input_schema.clone(),
                    },
                })
                .collect(),
            tool_choice: Some(json!("auto")),
        };

        let response = self
            .client
            .post(format!(
                "{}/chat/completions",
                self.config.base_url.trim_end_matches('/')
            ))
            .bearer_auth(&self.config.api_key)
            .json(&request)
            .send()
            .await
            .map_err(|err| {
                ProviderError::new(ProviderErrorKind::Transport, err.to_string(), true)
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(classify_status(status, body));
        }

        let raw_text = response.text().await.map_err(|err| {
            ProviderError::new(ProviderErrorKind::Transport, err.to_string(), true)
        })?;

        let body: ChatCompletionResponse = serde_json::from_str(&raw_text).map_err(|err| {
            tracing::error!("OpenAI response deserialization failed: {err}\nRaw body: {raw_text}");
            ProviderError::new(
                ProviderErrorKind::InvalidResponse,
                format!("error decoding response body: {err}"),
                false,
            )
        })?;

        let choice = body.choices.into_iter().next().ok_or_else(|| {
            ProviderError::new(
                ProviderErrorKind::InvalidResponse,
                "provider returned no choices",
                false,
            )
        })?;

        if let Some(tool_calls) = choice.message.tool_calls
            && !tool_calls.is_empty()
        {
            let mut planned = Vec::with_capacity(tool_calls.len());
            for (index, tool_call) in tool_calls.into_iter().enumerate() {
                let args = serde_json::from_str::<Value>(&tool_call.function.arguments).map_err(
                    |err| {
                        ProviderError::new(
                            ProviderErrorKind::InvalidResponse,
                            format!("invalid tool call arguments: {err}"),
                            false,
                        )
                    },
                )?;
                planned.push(PlannedSkillCall {
                    call_id: tool_call
                        .id
                        .unwrap_or_else(|| format!("openai-call-{index}")),
                    name: tool_call.function.name,
                    args,
                    priority: 0,
                    depends_on: Vec::new(),
                    retry_policy: Default::default(),
                    dry_run: false,
                });
            }
            return Ok(ProviderDecision {
                action: AgentAction::CallSkills(planned),
                usage: None,
                cache: None,
            });
        }

        let content = choice.message.content.unwrap_or_default();
        if let Ok(structured) = serde_json::from_str::<StructuredAction>(&content) {
            return Ok(ProviderDecision {
                action: match structured.kind.as_str() {
                    "yield" => AgentAction::Yield {
                        reason: structured.content,
                    },
                    _ => AgentAction::Respond {
                        content: structured.content.unwrap_or_default(),
                    },
                },
                usage: None,
                cache: None,
            });
        }

        Ok(ProviderDecision {
            action: if content.trim().is_empty() {
                AgentAction::Yield { reason: None }
            } else {
                AgentAction::Respond { content }
            },
            usage: None,
            cache: None,
        })
    }
}

fn map_to_chat_messages(
    input: &ProviderRequest,
    system_prompt: String,
) -> Result<Vec<ChatMessage>, ProviderError> {
    let mut messages = vec![ChatMessage::system(system_prompt)];
    for msg in &input.contents {
        let role = match msg.role {
            rain_engine_core::ProviderRole::System => "system",
            rain_engine_core::ProviderRole::User => "user",
            rain_engine_core::ProviderRole::Assistant => "assistant",
            rain_engine_core::ProviderRole::Tool => "tool",
        };

        let mut content = String::new();
        let mut tool_calls = None;
        let mut tool_call_id = None;

        for part in &msg.parts {
            match part {
                rain_engine_core::ProviderContentPart::Text(t) => {
                    if !content.is_empty() {
                        content.push('\n');
                    }
                    content.push_str(t);
                }
                rain_engine_core::ProviderContentPart::Json(j) => {
                    // Try to parse as tool calls if it's an assistant message
                    if msg.role == rain_engine_core::ProviderRole::Assistant {
                        if let Ok(calls) =
                            serde_json::from_value::<Vec<PlannedSkillCall>>(j.clone())
                        {
                            tool_calls = Some(
                                calls
                                    .into_iter()
                                    .map(|c| ToolCallRequest {
                                        id: c.call_id,
                                        kind: "function".to_string(),
                                        function: ToolFunctionCall {
                                            name: c.name,
                                            arguments: c.args.to_string(),
                                        },
                                    })
                                    .collect(),
                            );
                        } else {
                            if !content.is_empty() {
                                content.push('\n');
                            }
                            content.push_str(&j.to_string());
                        }
                    } else {
                        if !content.is_empty() {
                            content.push('\n');
                        }
                        content.push_str(&j.to_string());
                    }
                }
                rain_engine_core::ProviderContentPart::ToolResult(r) => {
                    content.push_str(&serde_json::to_string(&r.output).unwrap_or_default());
                    tool_call_id = Some(r.call_id.clone());
                }
                _ => {}
            }
        }

        messages.push(ChatMessage {
            role: role.to_string(),
            content: if content.is_empty() && tool_calls.is_some() {
                None
            } else {
                Some(content)
            },
            tool_calls,
            tool_call_id,
        });
    }
    Ok(messages)
}

fn classify_status(status: StatusCode, body: String) -> ProviderError {
    match status {
        StatusCode::TOO_MANY_REQUESTS => {
            ProviderError::new(ProviderErrorKind::RateLimited, body, true)
        }
        StatusCode::BAD_REQUEST => {
            ProviderError::new(ProviderErrorKind::InvalidResponse, body, false)
        }
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            ProviderError::new(ProviderErrorKind::Configuration, body, false)
        }
        _ if status.is_server_error() => {
            ProviderError::new(ProviderErrorKind::Transport, body, true)
        }
        _ => ProviderError::new(ProviderErrorKind::Internal, body, false),
    }
}

#[derive(Debug, Serialize)]
struct ChatCompletionRequest {
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    messages: Vec<ChatMessage>,
    tools: Vec<ToolDefinition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<Value>,
}

#[derive(Debug, Serialize)]
struct ChatMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ToolCallRequest>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct ToolCallRequest {
    #[serde(rename = "type")]
    kind: String,
    id: String,
    function: ToolFunctionCall,
}

#[derive(Debug, Serialize)]
struct ToolFunctionCall {
    name: String,
    arguments: String,
}

impl ChatMessage {
    fn system(content: String) -> Self {
        Self {
            role: "system".to_string(),
            content: Some(content),
            tool_calls: None,
            tool_call_id: None,
        }
    }
}

#[derive(Debug, Serialize)]
struct ToolDefinition {
    #[serde(rename = "type")]
    kind: String,
    function: ToolFunction,
}

#[derive(Debug, Serialize)]
struct ToolFunction {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<Choice>,
}

#[derive(Debug, Deserialize)]
struct Choice {
    message: ChoiceMessage,
}

#[derive(Debug, Deserialize)]
struct ChoiceMessage {
    content: Option<String>,
    tool_calls: Option<Vec<ToolCall>>,
}

#[derive(Debug, Deserialize)]
struct ToolCall {
    id: Option<String>,
    function: ToolCallFunction,
}

#[derive(Debug, Deserialize)]
struct ToolCallFunction {
    name: String,
    arguments: String,
}

#[derive(Debug, Deserialize)]
struct StructuredAction {
    #[serde(rename = "type")]
    kind: String,
    content: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::post};
    use rain_engine_core::{
        AgentContextSnapshot, AgentId, AgentStateSnapshot, AgentTrigger, EnginePolicy,
        ProviderContentPart, SkillDefinition, SkillManifest,
    };
    use serde_json::json;

    fn provider_request() -> ProviderRequest {
        ProviderRequest {
            trigger: AgentTrigger::Message {
                user_id: "u".to_string(),
                content: "hello".to_string(),
                attachments: Vec::new(),
            },
            context: AgentContextSnapshot {
                session_id: "s".to_string(),
                granted_scopes: vec!["tool:run".to_string()],
                trigger_id: "t".to_string(),
                idempotency_key: None,
                current_step: 0,
                max_steps: 8,
                history: Vec::new(),
                prior_tool_results: Vec::new(),
                session_cost_usd: 0.0,
                state: AgentStateSnapshot {
                    agent_id: AgentId("s".to_string()),
                    profile: None,
                    goals: Vec::new(),
                    tasks: Vec::new(),
                    observations: Vec::new(),
                    artifacts: Vec::new(),
                    resources: Vec::new(),
                    relationships: Vec::new(),
                    pending_wake: None,
                },
            },
            available_skills: vec![SkillDefinition {
                manifest: SkillManifest {
                    name: "echo".to_string(),
                    description: "Echo".to_string(),
                    input_schema: json!({"type":"object"}),
                    required_scopes: vec!["tool:run".to_string()],
                    capability_grants: vec![],
                    resource_policy: rain_engine_core::ResourcePolicy::default_for_tools(),
                    approval_required: false,
                },
                executor_kind: "wasm".to_string(),
            }],
            config: ProviderRequestConfig {
                model: Some("test-model".to_string()),
                temperature: Some(0.1),
                max_tokens: Some(32),
            },
            policy: EnginePolicy::default(),
            contents: vec![rain_engine_core::ProviderMessage {
                role: rain_engine_core::ProviderRole::User,
                parts: vec![ProviderContentPart::Text("hello".to_string())],
            }],
        }
    }

    async fn spawn_test_server(response_body: Value) -> String {
        let app = Router::new().route(
            "/chat/completions",
            post(move || {
                let response_body = response_body.clone();
                async move { Json(response_body) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });
        format!("http://{}", addr)
    }

    #[tokio::test]
    async fn parses_parallel_tool_call_response() {
        let base_url = spawn_test_server(json!({
            "choices": [{
                "message": {
                    "content": null,
                    "tool_calls": [{
                        "id": "call-1",
                        "function": {
                            "name": "echo",
                            "arguments": "{\"value\":1}"
                        }
                    }, {
                        "id": "call-2",
                        "function": {
                            "name": "echo",
                            "arguments": "{\"value\":2}"
                        }
                    }]
                }
            }]
        }))
        .await;

        let provider = OpenAiCompatibleProvider::new(OpenAiCompatibleConfig {
            base_url,
            api_key: "token".to_string(),
            default_request: ProviderRequestConfig::default(),
            system_prompt: "You are helpful".to_string(),
        })
        .expect("provider");

        let decision = provider
            .generate_action(provider_request())
            .await
            .expect("decision");
        assert_eq!(
            decision.action,
            AgentAction::CallSkills(vec![
                PlannedSkillCall {
                    call_id: "call-1".to_string(),
                    name: "echo".to_string(),
                    args: json!({"value": 1}),
                    priority: 0,
                    depends_on: Vec::new(),
                    retry_policy: Default::default(),
                    dry_run: false,
                },
                PlannedSkillCall {
                    call_id: "call-2".to_string(),
                    name: "echo".to_string(),
                    args: json!({"value": 2}),
                    priority: 0,
                    depends_on: Vec::new(),
                    retry_policy: Default::default(),
                    dry_run: false,
                },
            ])
        );
    }

    #[tokio::test]
    async fn invalid_tool_call_arguments_are_classified() {
        let base_url = spawn_test_server(json!({
            "choices": [{
                "message": {
                    "content": null,
                    "tool_calls": [{
                        "function": {
                            "name": "echo",
                            "arguments": "{"
                        }
                    }]
                }
            }]
        }))
        .await;

        let provider = OpenAiCompatibleProvider::new(OpenAiCompatibleConfig {
            base_url,
            api_key: "token".to_string(),
            default_request: ProviderRequestConfig::default(),
            system_prompt: "You are helpful".to_string(),
        })
        .expect("provider");

        let error = provider
            .generate_action(provider_request())
            .await
            .expect_err("error");
        assert_eq!(error.kind, ProviderErrorKind::InvalidResponse);
    }
}
