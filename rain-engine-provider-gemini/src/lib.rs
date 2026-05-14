//! Gemini provider adapter for RainEngine.
//!
//! This crate maps provider-neutral content, tool declarations, parallel tool
//! calls, and cache metadata into Gemini REST requests.

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use rain_engine_core::{
    AgentAction, AttachmentContent, AttachmentRef, LlmProvider, PlannedSkillCall,
    ProviderCacheRecord, ProviderContentPart, ProviderDecision, ProviderError, ProviderErrorKind,
    ProviderRequest, ProviderRequestConfig, ProviderUsageRecord, SessionRecord,
};
use reqwest::{Client, RequestBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

#[derive(Debug, Clone)]
pub enum GeminiAuth {
    ApiKey(String),
    BearerToken(String),
}

#[derive(Debug, Clone)]
pub struct GeminiConfig {
    pub base_url: String,
    pub auth: GeminiAuth,
    pub default_request: ProviderRequestConfig,
    pub system_instruction: String,
    pub provider_name: String,
}

impl GeminiConfig {
    pub fn validated(&self) -> Result<(), GeminiConfigError> {
        if self.base_url.trim().is_empty() {
            return Err(GeminiConfigError::Invalid(
                "base_url must not be empty".to_string(),
            ));
        }
        match &self.auth {
            GeminiAuth::ApiKey(value) | GeminiAuth::BearerToken(value)
                if value.trim().is_empty() =>
            {
                return Err(GeminiConfigError::Invalid(
                    "auth credential must not be empty".to_string(),
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum GeminiConfigError {
    #[error("{0}")]
    Invalid(String),
}

#[derive(Clone)]
pub struct GeminiProvider {
    client: Client,
    config: GeminiConfig,
}

impl GeminiProvider {
    pub fn new(config: GeminiConfig) -> Result<Self, GeminiConfigError> {
        config.validated()?;
        Ok(Self {
            client: Client::new(),
            config,
        })
    }

    fn latest_cached_content_id(&self, request: &ProviderRequest) -> Option<String> {
        request
            .context
            .history
            .iter()
            .rev()
            .find_map(|record| match record {
                SessionRecord::ProviderCache(cache)
                    if cache.provider_name == self.config.provider_name =>
                {
                    Some(cache.cached_content_id.clone())
                }
                _ => None,
            })
    }

    async fn count_tokens(
        &self,
        model: &str,
        contents: &[GeminiContent],
    ) -> Result<usize, ProviderError> {
        let response = self
            .authorized(self.client.post(format!(
                "{}/models/{}:countTokens",
                self.config.base_url.trim_end_matches('/'),
                model
            )))
            .json(&json!({
                "contents": contents,
            }))
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
        let body: CountTokensResponse = response.json().await.map_err(|err| {
            ProviderError::new(ProviderErrorKind::InvalidResponse, err.to_string(), false)
        })?;
        Ok(body.total_tokens)
    }

    async fn create_cached_content(
        &self,
        model: &str,
        tool_definitions: &[GeminiToolDefinition],
        stable_contents: &[GeminiContent],
        token_count: usize,
    ) -> Result<ProviderCacheRecord, ProviderError> {
        let response = self
            .authorized(self.client.post(format!(
                "{}/cachedContents",
                self.config.base_url.trim_end_matches('/')
            )))
            .json(&json!({
                "model": format!("models/{model}"),
                "systemInstruction": {
                    "parts": [{ "text": self.config.system_instruction }]
                },
                "tools": tool_definitions,
                "contents": stable_contents,
            }))
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
        let body: CreateCachedContentResponse = response.json().await.map_err(|err| {
            ProviderError::new(ProviderErrorKind::InvalidResponse, err.to_string(), false)
        })?;
        Ok(ProviderCacheRecord {
            provider_name: self.config.provider_name.clone(),
            cached_content_id: body.name,
            token_count,
            cached_at: std::time::SystemTime::now(),
        })
    }

    fn authorized(&self, builder: RequestBuilder) -> RequestBuilder {
        match &self.config.auth {
            GeminiAuth::ApiKey(key) => builder.query(&[("key", key)]),
            GeminiAuth::BearerToken(token) => builder.bearer_auth(token),
        }
    }
}

#[async_trait]
impl LlmProvider for GeminiProvider {
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
                    "no model configured for Gemini provider",
                    false,
                )
            })?;

        let tools = vec![GeminiToolEnvelope {
            function_declarations: input
                .available_skills
                .iter()
                .map(|skill| GeminiToolDefinition {
                    name: skill.manifest.name.clone(),
                    description: skill.manifest.description.clone(),
                    parameters: skill.manifest.input_schema.clone(),
                })
                .collect(),
        }];
        let active_contents = map_provider_contents(&input.contents);
        let mut cache_record = None;
        let cached_content = if let Some(existing) = self.latest_cached_content_id(&input) {
            Some(existing)
        } else {
            let token_count = self.count_tokens(&model, &active_contents).await?;
            if token_count > input.policy.cache_threshold_tokens {
                let stable_contents = collect_stable_contents(&input);
                let created = self
                    .create_cached_content(
                        &model,
                        &tools[0].function_declarations,
                        &stable_contents,
                        token_count,
                    )
                    .await?;
                let id = created.cached_content_id.clone();
                cache_record = Some(created);
                Some(id)
            } else {
                None
            }
        };

        let request_body = if let Some(cached_content) = &cached_content {
            json!({
                "cachedContent": cached_content,
                "contents": active_contents,
                "tools": tools,
            })
        } else {
            json!({
                "systemInstruction": {
                    "parts": [{ "text": self.config.system_instruction }]
                },
                "contents": active_contents,
                "tools": tools,
            })
        };

        let response = self
            .authorized(self.client.post(format!(
                "{}/models/{}:generateContent",
                self.config.base_url.trim_end_matches('/'),
                model
            )))
            .json(&request_body)
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

        let body: GenerateContentResponse = response.json().await.map_err(|err| {
            ProviderError::new(ProviderErrorKind::InvalidResponse, err.to_string(), false)
        })?;
        let candidate = body.candidates.into_iter().next().ok_or_else(|| {
            ProviderError::new(
                ProviderErrorKind::InvalidResponse,
                "provider returned no candidates",
                false,
            )
        })?;

        let mut calls = Vec::new();
        let mut text_parts = Vec::new();
        for (index, part) in candidate.content.parts.into_iter().enumerate() {
            if let Some(function_call) = part.function_call {
                calls.push(PlannedSkillCall {
                    call_id: function_call
                        .id
                        .unwrap_or_else(|| format!("gemini-call-{index}")),
                    name: function_call.name,
                    args: function_call.args.unwrap_or_else(|| json!({})),
                });
            } else if let Some(text) = part.text {
                text_parts.push(text);
            }
        }

        let usage = body.usage_metadata.map(|usage| ProviderUsageRecord {
            provider_name: self.config.provider_name.clone(),
            recorded_at: std::time::SystemTime::now(),
            input_tokens: usage.prompt_token_count,
            output_tokens: usage.candidates_token_count,
            estimated_cost_usd: ((usage.prompt_token_count + usage.candidates_token_count) as f64)
                / 1_000_000.0,
            cached_content_id: cached_content,
        });

        let action = if !calls.is_empty() {
            AgentAction::CallSkills(calls)
        } else {
            let joined = text_parts.join("\n");
            if joined.trim().is_empty() {
                AgentAction::Yield { reason: None }
            } else if let Ok(structured) = serde_json::from_str::<StructuredAction>(&joined) {
                match structured.kind.as_str() {
                    "yield" => AgentAction::Yield {
                        reason: structured.content,
                    },
                    _ => AgentAction::Respond {
                        content: structured.content.unwrap_or_default(),
                    },
                }
            } else {
                AgentAction::Respond { content: joined }
            }
        };

        Ok(ProviderDecision {
            action,
            usage,
            cache: cache_record,
        })
    }
}

fn map_provider_contents(contents: &[rain_engine_core::ProviderMessage]) -> Vec<GeminiContent> {
    contents
        .iter()
        .map(|message| GeminiContent {
            role: match message.role {
                rain_engine_core::ProviderRole::System => "user".to_string(),
                rain_engine_core::ProviderRole::User => "user".to_string(),
                rain_engine_core::ProviderRole::Assistant => "model".to_string(),
                rain_engine_core::ProviderRole::Tool => "user".to_string(),
            },
            parts: message
                .parts
                .iter()
                .flat_map(map_provider_part)
                .collect::<Vec<_>>(),
        })
        .collect()
}

fn map_provider_part(part: &ProviderContentPart) -> Vec<GeminiPart> {
    match part {
        ProviderContentPart::Text(text) => vec![GeminiPart {
            text: Some(text.clone()),
            inline_data: None,
            file_data: None,
            function_call: None,
            function_response: None,
        }],
        ProviderContentPart::Json(value) => vec![GeminiPart {
            text: Some(value.to_string()),
            inline_data: None,
            file_data: None,
            function_call: None,
            function_response: None,
        }],
        ProviderContentPart::InlineData(payload) => vec![GeminiPart {
            text: None,
            inline_data: Some(InlineData {
                mime_type: payload.mime_type.clone(),
                data: STANDARD.encode(&payload.data),
            }),
            file_data: None,
            function_call: None,
            function_response: None,
        }],
        ProviderContentPart::Attachment(attachment) => vec![map_attachment_part(attachment)],
        ProviderContentPart::ToolResult(result) => vec![GeminiPart {
            text: None,
            inline_data: None,
            file_data: None,
            function_call: None,
            function_response: Some(FunctionResponse {
                name: result.skill_name.clone(),
                response: json!({
                    "call_id": result.call_id,
                    "output": result.output,
                }),
            }),
        }],
    }
}

fn map_attachment_part(attachment: &AttachmentRef) -> GeminiPart {
    match &attachment.content {
        AttachmentContent::Inline { data } => GeminiPart {
            text: None,
            inline_data: Some(InlineData {
                mime_type: attachment.mime_type.clone(),
                data: STANDARD.encode(data),
            }),
            file_data: None,
            function_call: None,
            function_response: None,
        },
        AttachmentContent::Blob { descriptor } => GeminiPart {
            text: None,
            inline_data: None,
            file_data: Some(FileData {
                mime_type: attachment.mime_type.clone(),
                file_uri: descriptor.uri.clone(),
            }),
            function_call: None,
            function_response: None,
        },
    }
}

fn collect_stable_contents(input: &ProviderRequest) -> Vec<GeminiContent> {
    let mut parts = Vec::new();
    for record in &input.context.history {
        if let SessionRecord::ToolResult(result) = record {
            parts.push(GeminiPart {
                text: None,
                inline_data: None,
                file_data: None,
                function_call: None,
                function_response: Some(FunctionResponse {
                    name: result.skill_name.clone(),
                    response: json!({
                        "call_id": result.call_id,
                        "output": result.output,
                    }),
                }),
            });
        }
    }
    if parts.is_empty() {
        map_provider_contents(&input.contents)
    } else {
        vec![GeminiContent {
            role: "user".to_string(),
            parts,
        }]
    }
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

#[derive(Debug, Serialize, Clone)]
struct GeminiContent {
    role: String,
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Serialize, Clone)]
struct GeminiPart {
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(rename = "inlineData", skip_serializing_if = "Option::is_none")]
    inline_data: Option<InlineData>,
    #[serde(rename = "fileData", skip_serializing_if = "Option::is_none")]
    file_data: Option<FileData>,
    #[serde(rename = "functionCall", skip_serializing_if = "Option::is_none")]
    function_call: Option<FunctionCall>,
    #[serde(rename = "functionResponse", skip_serializing_if = "Option::is_none")]
    function_response: Option<FunctionResponse>,
}

#[derive(Debug, Serialize, Clone)]
struct InlineData {
    #[serde(rename = "mimeType")]
    mime_type: String,
    data: String,
}

#[derive(Debug, Serialize, Clone)]
struct FileData {
    #[serde(rename = "mimeType")]
    mime_type: String,
    #[serde(rename = "fileUri")]
    file_uri: String,
}

#[derive(Debug, Serialize, Clone)]
struct FunctionResponse {
    name: String,
    response: Value,
}

#[derive(Debug, Serialize, Clone)]
struct GeminiToolEnvelope {
    #[serde(rename = "functionDeclarations")]
    function_declarations: Vec<GeminiToolDefinition>,
}

#[derive(Debug, Serialize, Clone)]
struct GeminiToolDefinition {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Debug, Deserialize)]
struct CountTokensResponse {
    #[serde(rename = "totalTokens")]
    total_tokens: usize,
}

#[derive(Debug, Deserialize)]
struct CreateCachedContentResponse {
    name: String,
}

#[derive(Debug, Deserialize)]
struct GenerateContentResponse {
    candidates: Vec<GenerateCandidate>,
    #[serde(rename = "usageMetadata")]
    usage_metadata: Option<UsageMetadata>,
}

#[derive(Debug, Deserialize)]
struct GenerateCandidate {
    content: GenerateContent,
}

#[derive(Debug, Deserialize)]
struct GenerateContent {
    parts: Vec<GeneratePart>,
}

#[derive(Debug, Deserialize)]
struct GeneratePart {
    text: Option<String>,
    #[serde(rename = "functionCall")]
    function_call: Option<FunctionCall>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct FunctionCall {
    id: Option<String>,
    name: String,
    args: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct UsageMetadata {
    #[serde(rename = "promptTokenCount")]
    prompt_token_count: u64,
    #[serde(rename = "candidatesTokenCount")]
    candidates_token_count: u64,
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
    use axum::{Json, Router, extract::State, routing::post};
    use rain_engine_core::{
        AgentContextSnapshot, AgentId, AgentStateSnapshot, AgentTrigger, AttachmentRef,
        EnginePolicy, ProviderMessage, ProviderRole, ResourcePolicy, SkillDefinition,
        SkillManifest,
    };
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct TestState {
        requests: Arc<Mutex<Vec<Value>>>,
    }

    fn provider_request(with_attachment: bool) -> ProviderRequest {
        let contents = vec![ProviderMessage {
            role: ProviderRole::User,
            parts: if with_attachment {
                vec![ProviderContentPart::Attachment(AttachmentRef::inline(
                    "a1",
                    "image/png",
                    Some("diagram.png".to_string()),
                    vec![1, 2, 3, 4],
                ))]
            } else {
                vec![ProviderContentPart::Text("hello".to_string())]
            },
        }];
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
                    name: "db_fix".to_string(),
                    description: "Fix DB".to_string(),
                    input_schema: json!({"type":"object"}),
                    required_scopes: vec!["tool:run".to_string()],
                    capability_grants: vec![],
                    resource_policy: ResourcePolicy::default_for_tools(),
                    approval_required: false,
                },
                executor_kind: "native".to_string(),
            }],
            config: ProviderRequestConfig {
                model: Some("gemini-1.5-pro".to_string()),
                temperature: None,
                max_tokens: None,
            },
            policy: EnginePolicy {
                cache_threshold_tokens: 10,
                ..EnginePolicy::default()
            },
            contents,
        }
    }

    async fn spawn_test_server() -> (String, TestState) {
        let state = TestState::default();
        let app = Router::new()
            .route(
                "/models/gemini-1.5-pro:countTokens",
                post(
                    |State(state): State<TestState>, Json(body): Json<Value>| async move {
                        state.requests.lock().expect("requests lock").push(body);
                        Json(json!({"totalTokens": 50}))
                    },
                ),
            )
            .route(
                "/cachedContents",
                post(
                    |State(state): State<TestState>, Json(body): Json<Value>| async move {
                        state.requests.lock().expect("requests lock").push(body);
                        Json(json!({"name": "cachedContents/cache-1"}))
                    },
                ),
            )
            .route(
                "/models/gemini-1.5-pro:generateContent",
                post(
                    |State(state): State<TestState>, Json(body): Json<Value>| async move {
                        state.requests.lock().expect("requests lock").push(body);
                        Json(json!({
                            "candidates": [{
                                "content": {
                                    "parts": [{
                                        "functionCall": {
                                            "id": "fc-1",
                                            "name": "db_fix",
                                            "args": {"apply": true}
                                        }
                                    }, {
                                        "functionCall": {
                                            "id": "fc-2",
                                            "name": "db_fix",
                                            "args": {"apply": false}
                                        }
                                    }]
                                }
                            }],
                            "usageMetadata": {
                                "promptTokenCount": 123,
                                "candidatesTokenCount": 45
                            }
                        }))
                    },
                ),
            )
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });
        (format!("http://{}", addr), state)
    }

    #[tokio::test]
    async fn maps_inline_attachment_and_parallel_calls() {
        let (base_url, state) = spawn_test_server().await;
        let provider = GeminiProvider::new(GeminiConfig {
            base_url,
            auth: GeminiAuth::ApiKey("token".to_string()),
            default_request: ProviderRequestConfig::default(),
            system_instruction: "You are helpful".to_string(),
            provider_name: "gemini".to_string(),
        })
        .expect("provider");

        let decision = provider
            .generate_action(provider_request(true))
            .await
            .expect("decision");
        match decision.action {
            AgentAction::CallSkills(calls) => assert_eq!(calls.len(), 2),
            other => panic!("expected parallel calls, got {other:?}"),
        }
        assert!(decision.cache.is_some());
        assert!(decision.usage.is_some());

        let requests = state.requests.lock().expect("requests");
        let generate = requests.last().expect("generate request");
        let body = generate.to_string();
        assert!(body.contains("inlineData"));
        assert!(body.contains("cachedContents/cache-1"));
    }

    #[tokio::test]
    async fn reuses_existing_cache_without_recreating() {
        let (base_url, state) = spawn_test_server().await;
        let provider = GeminiProvider::new(GeminiConfig {
            base_url,
            auth: GeminiAuth::ApiKey("token".to_string()),
            default_request: ProviderRequestConfig::default(),
            system_instruction: "You are helpful".to_string(),
            provider_name: "gemini".to_string(),
        })
        .expect("provider");

        let mut request = provider_request(false);
        request
            .context
            .history
            .push(SessionRecord::ProviderCache(ProviderCacheRecord {
                provider_name: "gemini".to_string(),
                cached_content_id: "cachedContents/existing".to_string(),
                token_count: 99_999,
                cached_at: std::time::SystemTime::now(),
            }));
        let _ = provider.generate_action(request).await.expect("decision");
        let requests = state.requests.lock().expect("requests");
        let body = requests.last().expect("generate request").to_string();
        assert!(body.contains("cachedContents/existing"));
    }
}
