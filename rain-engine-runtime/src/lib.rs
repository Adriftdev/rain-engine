use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::{StatusCode, header::CONTENT_TYPE},
    routing::post,
};
use futures_util::stream;
use rain_engine_blob::{BlobBackendConfig, build_blob_store};
use rain_engine_core::{
    AgentEngine, AgentTrigger, ApprovalDecision, AttachmentRef, BlobStore, EngineOutcome,
    EnginePolicy, InMemoryMemoryStore, LlmProvider, MemoryStore, MockLlmProvider,
    MultimodalPayload, ProcessRequest, ProviderRequestConfig,
};
use rain_engine_openai::{OpenAiCompatibleConfig, OpenAiCompatibleProvider};
use rain_engine_provider_gemini::{GeminiAuth, GeminiConfig, GeminiProvider};
use rain_engine_store_pg::PgMemoryStore;
use rain_engine_store_sqlite::SqliteMemoryStore;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

const MAX_INGRESS_BODY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WebhookIngressRequest {
    pub session_id: String,
    pub payload: Value,
    #[serde(default)]
    pub attachments: Vec<MultimodalPayload>,
    #[serde(default)]
    pub granted_scopes: BTreeSet<String>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub provider: Option<ProviderRequestConfig>,
    #[serde(default)]
    pub policy_override: Option<EnginePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApprovalIngressRequest {
    pub session_id: String,
    pub resume_token: String,
    pub decision: ApprovalDecision,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub granted_scopes: BTreeSet<String>,
    #[serde(default)]
    pub provider: Option<ProviderRequestConfig>,
    #[serde(default)]
    pub policy_override: Option<EnginePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeServerConfig {
    pub bind_address: SocketAddr,
    pub request_timeout_ms: u64,
    pub default_policy: EnginePolicy,
    pub allow_policy_overrides: bool,
    pub allow_provider_overrides: bool,
    pub default_provider: ProviderRequestConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StoreBootstrapConfig {
    InMemory,
    Sqlite { database_url: String },
    Postgres { database_url: String },
}

pub type BlobBootstrapConfig = BlobBackendConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum GeminiAuthMode {
    ApiKey,
    BearerToken,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderBootstrapConfig {
    Mock {
        response: String,
    },
    OpenAiCompatible {
        base_url: String,
        api_key: String,
        model: String,
        #[serde(default)]
        temperature: Option<f32>,
        #[serde(default)]
        max_tokens: Option<u32>,
        #[serde(default = "default_system_prompt")]
        system_prompt: String,
    },
    Gemini {
        base_url: String,
        auth_mode: GeminiAuthMode,
        credential: String,
        model: String,
        #[serde(default)]
        temperature: Option<f32>,
        #[serde(default)]
        max_tokens: Option<u32>,
        #[serde(default = "default_gemini_system_instruction")]
        system_instruction: String,
        #[serde(default = "default_gemini_provider_name")]
        provider_name: String,
    },
}

fn default_system_prompt() -> String {
    "You are a server-side automation agent. Prefer tool calls when available. When replying directly, return plain text or JSON with type=yield.".to_string()
}

fn default_gemini_system_instruction() -> String {
    "You are a multimodal server-side automation agent. Use tools when they can complete the task precisely.".to_string()
}

fn default_gemini_provider_name() -> String {
    "gemini".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeBootstrapConfig {
    pub server: RuntimeServerConfig,
    pub store: StoreBootstrapConfig,
    pub blob: BlobBootstrapConfig,
    pub provider: ProviderBootstrapConfig,
}

#[derive(Clone)]
pub struct RuntimeState {
    engine: AgentEngine,
    memory: Arc<dyn MemoryStore>,
    blob_store: Arc<dyn BlobStore>,
    config: RuntimeServerConfig,
}

impl RuntimeState {
    pub fn new(
        engine: AgentEngine,
        memory: Arc<dyn MemoryStore>,
        blob_store: Arc<dyn BlobStore>,
        config: RuntimeServerConfig,
    ) -> Self {
        Self {
            engine,
            memory,
            blob_store,
            config,
        }
    }

    pub fn engine(&self) -> &AgentEngine {
        &self.engine
    }

    pub fn memory(&self) -> Arc<dyn MemoryStore> {
        self.memory.clone()
    }

    pub fn blob_store(&self) -> Arc<dyn BlobStore> {
        self.blob_store.clone()
    }
}

#[derive(Debug, Error)]
pub enum RuntimeConfigError {
    #[error("{0}")]
    Invalid(String),
}

#[derive(Debug, Error)]
enum IngressError {
    #[error("unsupported content type")]
    UnsupportedContentType,
    #[error("malformed request: {0}")]
    Malformed(String),
    #[error("blob storage failed: {0}")]
    Blob(String),
}

pub async fn build_runtime_state(
    config: RuntimeBootstrapConfig,
) -> Result<RuntimeState, RuntimeConfigError> {
    let memory: Arc<dyn MemoryStore> = match &config.store {
        StoreBootstrapConfig::InMemory => Arc::new(InMemoryMemoryStore::new()),
        StoreBootstrapConfig::Sqlite { database_url } => {
            if database_url.trim().is_empty() {
                return Err(RuntimeConfigError::Invalid(
                    "sqlite database_url must not be empty".to_string(),
                ));
            }
            Arc::new(
                SqliteMemoryStore::connect(database_url)
                    .await
                    .map_err(|err| RuntimeConfigError::Invalid(err.message))?,
            )
        }
        StoreBootstrapConfig::Postgres { database_url } => {
            if database_url.trim().is_empty() {
                return Err(RuntimeConfigError::Invalid(
                    "postgres database_url must not be empty".to_string(),
                ));
            }
            Arc::new(
                PgMemoryStore::connect_lazy(database_url)
                    .map_err(|err| RuntimeConfigError::Invalid(err.message))?,
            )
        }
    };

    let blob_store: Arc<dyn BlobStore> = Arc::from(
        build_blob_store(&config.blob).map_err(|err| RuntimeConfigError::Invalid(err.message))?,
    );

    let llm: Arc<dyn LlmProvider> = match &config.provider {
        ProviderBootstrapConfig::Mock { response } => Arc::new(MockLlmProvider::scripted(vec![
            rain_engine_core::AgentAction::Respond {
                content: response.clone(),
            },
        ])),
        ProviderBootstrapConfig::OpenAiCompatible {
            base_url,
            api_key,
            model,
            temperature,
            max_tokens,
            system_prompt,
        } => Arc::new(
            OpenAiCompatibleProvider::new(OpenAiCompatibleConfig {
                base_url: base_url.clone(),
                api_key: api_key.clone(),
                default_request: ProviderRequestConfig {
                    model: Some(model.clone()),
                    temperature: *temperature,
                    max_tokens: *max_tokens,
                },
                system_prompt: system_prompt.clone(),
            })
            .map_err(|err| RuntimeConfigError::Invalid(err.to_string()))?,
        ),
        ProviderBootstrapConfig::Gemini {
            base_url,
            auth_mode,
            credential,
            model,
            temperature,
            max_tokens,
            system_instruction,
            provider_name,
        } => Arc::new(
            GeminiProvider::new(GeminiConfig {
                base_url: base_url.clone(),
                auth: match auth_mode {
                    GeminiAuthMode::ApiKey => GeminiAuth::ApiKey(credential.clone()),
                    GeminiAuthMode::BearerToken => GeminiAuth::BearerToken(credential.clone()),
                },
                default_request: ProviderRequestConfig {
                    model: Some(model.clone()),
                    temperature: *temperature,
                    max_tokens: *max_tokens,
                },
                system_instruction: system_instruction.clone(),
                provider_name: provider_name.clone(),
            })
            .map_err(|err| RuntimeConfigError::Invalid(err.to_string()))?,
        ),
    };

    Ok(RuntimeState::new(
        AgentEngine::new(llm, memory.clone()),
        memory,
        blob_store,
        config.server,
    ))
}

pub fn app(state: RuntimeState) -> Router {
    Router::new()
        .route("/triggers/webhook/{source}", post(handle_webhook))
        .route("/triggers/approval", post(handle_approval))
        .with_state(state)
}

pub async fn serve(addr: SocketAddr, state: RuntimeState) -> Result<(), std::io::Error> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app(state)).await
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt::try_init();
}

async fn handle_webhook(
    State(state): State<RuntimeState>,
    Path(source): Path<String>,
    request: Request,
) -> Result<Json<EngineOutcome>, (StatusCode, String)> {
    let envelope = parse_webhook_envelope(request)
        .await
        .map_err(map_ingress_error)?;
    let policy = effective_policy(&state.config, envelope.policy_override);
    let provider = effective_provider(&state.config, envelope.provider);
    let attachments = materialize_attachments(
        &state,
        policy.max_inline_attachment_bytes,
        envelope.attachments,
    )
    .await
    .map_err(map_ingress_error)?;

    run_process_request(
        &state,
        ProcessRequest {
            session_id: envelope.session_id,
            trigger: AgentTrigger::Webhook {
                source,
                payload: envelope.payload,
                attachments,
            },
            granted_scopes: envelope.granted_scopes,
            idempotency_key: envelope.idempotency_key,
            policy,
            provider,
            cancellation: tokio_util::sync::CancellationToken::new(),
        },
    )
    .await
}

async fn handle_approval(
    State(state): State<RuntimeState>,
    Json(request): Json<ApprovalIngressRequest>,
) -> Result<Json<EngineOutcome>, (StatusCode, String)> {
    let policy = effective_policy(&state.config, request.policy_override);
    let provider = effective_provider(&state.config, request.provider);
    run_process_request(
        &state,
        ProcessRequest {
            session_id: request.session_id,
            trigger: AgentTrigger::Approval {
                resume_token: rain_engine_core::ResumeToken(request.resume_token),
                decision: request.decision,
                metadata: request.metadata,
            },
            granted_scopes: request.granted_scopes,
            idempotency_key: None,
            policy,
            provider,
            cancellation: tokio_util::sync::CancellationToken::new(),
        },
    )
    .await
}

async fn parse_webhook_envelope(request: Request) -> Result<WebhookIngressRequest, IngressError> {
    let content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    let body = to_bytes(request.into_body(), MAX_INGRESS_BODY_BYTES)
        .await
        .map_err(|err| IngressError::Malformed(err.to_string()))?;

    if content_type.starts_with("application/json") {
        return serde_json::from_slice(&body)
            .map_err(|err| IngressError::Malformed(err.to_string()));
    }

    if content_type.starts_with("multipart/form-data") {
        return parse_multipart_webhook(&content_type, body).await;
    }

    Err(IngressError::UnsupportedContentType)
}

async fn parse_multipart_webhook(
    content_type: &str,
    body: axum::body::Bytes,
) -> Result<WebhookIngressRequest, IngressError> {
    let boundary = multer::parse_boundary(content_type)
        .map_err(|err| IngressError::Malformed(err.to_string()))?;
    let stream = stream::once(async move { Ok::<_, std::convert::Infallible>(body) });
    let mut multipart = multer::Multipart::new(stream, boundary);

    let mut session_id = None::<String>;
    let mut payload = None::<Value>;
    let mut attachments = Vec::new();
    let mut granted_scopes = BTreeSet::new();
    let mut idempotency_key = None::<String>;
    let mut provider = None::<ProviderRequestConfig>;
    let mut policy_override = None::<EnginePolicy>;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|err| IngressError::Malformed(err.to_string()))?
    {
        let name = field.name().unwrap_or_default().to_string();
        let file_name = field.file_name().map(str::to_string);
        let mime_type = field
            .content_type()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "application/octet-stream".to_string());

        if file_name.is_some() {
            attachments.push(MultimodalPayload {
                mime_type,
                file_name,
                data: field
                    .bytes()
                    .await
                    .map_err(|err| IngressError::Malformed(err.to_string()))?
                    .to_vec(),
            });
            continue;
        }

        let text = field
            .text()
            .await
            .map_err(|err| IngressError::Malformed(err.to_string()))?;
        match name.as_str() {
            "session_id" => session_id = Some(text),
            "payload" => {
                payload = Some(parse_json_value(&text, "payload")?);
            }
            "granted_scope" => {
                if !text.trim().is_empty() {
                    granted_scopes.insert(text);
                }
            }
            "granted_scopes" => {
                let scopes = parse_json_value::<Vec<String>>(&text, "granted_scopes")?;
                granted_scopes.extend(scopes);
            }
            "idempotency_key" => {
                if !text.trim().is_empty() {
                    idempotency_key = Some(text);
                }
            }
            "provider" => provider = Some(parse_json_value(&text, "provider")?),
            "policy_override" => {
                policy_override = Some(parse_json_value(&text, "policy_override")?)
            }
            _ => {}
        }
    }

    Ok(WebhookIngressRequest {
        session_id: session_id
            .ok_or_else(|| IngressError::Malformed("missing session_id".to_string()))?,
        payload: payload.unwrap_or(Value::Null),
        attachments,
        granted_scopes,
        idempotency_key,
        provider,
        policy_override,
    })
}

fn parse_json_value<T: DeserializeOwned>(text: &str, field_name: &str) -> Result<T, IngressError> {
    serde_json::from_str(text)
        .map_err(|err| IngressError::Malformed(format!("invalid {field_name}: {err}")))
}

async fn materialize_attachments(
    state: &RuntimeState,
    max_inline_attachment_bytes: usize,
    payloads: Vec<MultimodalPayload>,
) -> Result<Vec<AttachmentRef>, IngressError> {
    let mut attachments = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let attachment_id = Uuid::new_v4().to_string();
        if payload.data.len() <= max_inline_attachment_bytes {
            attachments.push(AttachmentRef::inline(
                attachment_id,
                payload.mime_type,
                payload.file_name,
                payload.data,
            ));
        } else {
            attachments.push(
                state
                    .blob_store
                    .put(attachment_id, payload)
                    .await
                    .map_err(|err| IngressError::Blob(err.message))?,
            );
        }
    }
    Ok(attachments)
}

fn effective_policy(
    config: &RuntimeServerConfig,
    override_policy: Option<EnginePolicy>,
) -> EnginePolicy {
    if config.allow_policy_overrides {
        override_policy.unwrap_or_else(|| config.default_policy.clone())
    } else {
        config.default_policy.clone()
    }
}

fn effective_provider(
    config: &RuntimeServerConfig,
    override_provider: Option<ProviderRequestConfig>,
) -> ProviderRequestConfig {
    if config.allow_provider_overrides {
        override_provider.unwrap_or_else(|| config.default_provider.clone())
    } else {
        config.default_provider.clone()
    }
}

async fn run_process_request(
    state: &RuntimeState,
    request: ProcessRequest,
) -> Result<Json<EngineOutcome>, (StatusCode, String)> {
    let timeout = Duration::from_millis(state.config.request_timeout_ms.max(1));
    match tokio::time::timeout(timeout, state.engine.process_trigger(request)).await {
        Ok(Ok(outcome)) => Ok(Json(outcome)),
        Ok(Err(err)) => Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
        Err(_) => Err((StatusCode::REQUEST_TIMEOUT, "request timed out".to_string())),
    }
}

fn map_ingress_error(error: IngressError) -> (StatusCode, String) {
    match error {
        IngressError::UnsupportedContentType => {
            (StatusCode::UNSUPPORTED_MEDIA_TYPE, error.to_string())
        }
        IngressError::Malformed(_) => (StatusCode::BAD_REQUEST, error.to_string()),
        IngressError::Blob(_) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::Request;
    use rain_engine_blob::LocalFileBlobStore;
    use rain_engine_core::{
        AgentAction, NativeSkill, PlannedSkillCall, SessionRecord, SkillExecutionError,
        SkillInvocation, SkillManifest, StopReason,
    };
    use serde_json::json;
    use tower::ServiceExt;

    #[derive(Clone)]
    struct ApprovalNativeSkill;

    #[async_trait]
    impl NativeSkill for ApprovalNativeSkill {
        async fn execute(
            &self,
            invocation: SkillInvocation,
        ) -> Result<serde_json::Value, SkillExecutionError> {
            Ok(json!({"approved": invocation.args}))
        }

        fn requires_human_approval(&self) -> bool {
            true
        }
    }

    fn server_config() -> RuntimeServerConfig {
        RuntimeServerConfig {
            bind_address: "127.0.0.1:0".parse().expect("addr"),
            request_timeout_ms: 1_000,
            default_policy: EnginePolicy::default(),
            allow_policy_overrides: true,
            allow_provider_overrides: true,
            default_provider: ProviderRequestConfig::default(),
        }
    }

    fn runtime_state_with_mock(response: &str) -> RuntimeState {
        let memory: Arc<dyn MemoryStore> = Arc::new(InMemoryMemoryStore::new());
        let blob_store: Arc<dyn BlobStore> =
            Arc::from(build_blob_store(&BlobBootstrapConfig::InMemory).expect("blob store"));
        let llm: Arc<dyn LlmProvider> =
            Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
                content: response.to_string(),
            }]));
        RuntimeState::new(
            AgentEngine::new(llm, memory.clone()),
            memory,
            blob_store,
            server_config(),
        )
    }

    #[tokio::test]
    async fn webhook_route_converts_json_request_into_agent_trigger() {
        let state = runtime_state_with_mock("processed");

        let response = app(state)
            .oneshot(
                Request::post("/triggers/webhook/github")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&WebhookIngressRequest {
                            session_id: "runtime-session".to_string(),
                            payload: serde_json::json!({"action": "opened"}),
                            attachments: Vec::new(),
                            granted_scopes: BTreeSet::new(),
                            idempotency_key: Some("abc".to_string()),
                            provider: None,
                            policy_override: None,
                        })
                        .expect("request json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let outcome: EngineOutcome = serde_json::from_slice(&bytes).expect("outcome");
        assert_eq!(outcome.stop_reason, StopReason::Responded);
        assert_eq!(outcome.response.as_deref(), Some("processed"));
    }

    #[tokio::test]
    async fn multipart_webhook_persists_blob_attachment_references() {
        let memory: Arc<dyn MemoryStore> = Arc::new(InMemoryMemoryStore::new());
        let blob_store: Arc<dyn BlobStore> =
            Arc::from(build_blob_store(&BlobBootstrapConfig::InMemory).expect("blob store"));
        let llm: Arc<dyn LlmProvider> =
            Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
                content: "processed".to_string(),
            }]));
        let mut config = server_config();
        config.default_policy.max_inline_attachment_bytes = 4;
        let state = RuntimeState::new(
            AgentEngine::new(llm, memory.clone()),
            memory.clone(),
            blob_store,
            config,
        );

        let boundary = "rain-engine-boundary";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"session_id\"\r\n\r\nmulti-session\r\n\
--{boundary}\r\nContent-Disposition: form-data; name=\"payload\"\r\n\r\n{{\"event\":\"upload\"}}\r\n\
--{boundary}\r\nContent-Disposition: form-data; name=\"attachment\"; filename=\"schema.png\"\r\nContent-Type: image/png\r\n\r\n123456789\r\n\
--{boundary}--\r\n"
        );

        let response = app(state.clone())
            .oneshot(
                Request::post("/triggers/webhook/files")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(body.into_bytes()))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let snapshot = state
            .memory()
            .load_session("multi-session")
            .await
            .expect("session");
        let trigger = snapshot
            .records
            .into_iter()
            .find_map(|record| match record {
                SessionRecord::Trigger(record) => Some(record.trigger),
                _ => None,
            })
            .expect("trigger");
        match trigger {
            AgentTrigger::Webhook { attachments, .. } => {
                assert_eq!(attachments.len(), 1);
                assert!(matches!(
                    attachments[0].content,
                    rain_engine_core::AttachmentContent::Blob { .. }
                ));
            }
            other => panic!("unexpected trigger: {other:?}"),
        }
    }

    #[tokio::test]
    async fn approval_route_resumes_suspended_native_skill() {
        let memory: Arc<dyn MemoryStore> = Arc::new(InMemoryMemoryStore::new());
        let blob_store: Arc<dyn BlobStore> =
            Arc::from(build_blob_store(&BlobBootstrapConfig::InMemory).expect("blob store"));
        let llm: Arc<dyn LlmProvider> = Arc::new(MockLlmProvider::scripted(vec![
            AgentAction::CallSkills(vec![PlannedSkillCall {
                call_id: "native-call".to_string(),
                name: "dangerous_native".to_string(),
                args: json!({"apply": true}),
            }]),
            AgentAction::Respond {
                content: "completed".to_string(),
            },
        ]));
        let state = RuntimeState::new(
            AgentEngine::new(llm, memory.clone()),
            memory,
            blob_store,
            server_config(),
        );
        state
            .engine()
            .register_native_skill(
                SkillManifest {
                    name: "dangerous_native".to_string(),
                    description: "Requires approval".to_string(),
                    input_schema: json!({"type":"object"}),
                    required_scopes: vec!["tool:run".to_string()],
                    capability_grants: vec![],
                    resource_policy: rain_engine_core::ResourcePolicy::default_for_tools(),
                    approval_required: true,
                },
                Arc::new(ApprovalNativeSkill),
            )
            .await;

        let start = app(state.clone())
            .oneshot(
                Request::post("/triggers/webhook/github")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&WebhookIngressRequest {
                            session_id: "approval-session".to_string(),
                            payload: json!({"action": "deploy"}),
                            attachments: Vec::new(),
                            granted_scopes: BTreeSet::from(["tool:run".to_string()]),
                            idempotency_key: None,
                            provider: None,
                            policy_override: None,
                        })
                        .expect("request json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        let start_bytes = axum::body::to_bytes(start.into_body(), usize::MAX)
            .await
            .expect("body");
        let suspended: EngineOutcome = serde_json::from_slice(&start_bytes).expect("outcome");
        assert_eq!(suspended.stop_reason, StopReason::Suspended);
        let resume_token = suspended.resume_token.expect("resume token").0;

        let resume = app(state)
            .oneshot(
                Request::post("/triggers/approval")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&ApprovalIngressRequest {
                            session_id: "approval-session".to_string(),
                            resume_token,
                            decision: ApprovalDecision::Approved,
                            metadata: json!({"approved_by": "tester"}),
                            granted_scopes: BTreeSet::from(["tool:run".to_string()]),
                            provider: None,
                            policy_override: None,
                        })
                        .expect("request json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        let resume_bytes = axum::body::to_bytes(resume.into_body(), usize::MAX)
            .await
            .expect("body");
        let resumed: EngineOutcome = serde_json::from_slice(&resume_bytes).expect("outcome");
        assert_eq!(resumed.stop_reason, StopReason::Responded);
        assert_eq!(resumed.response.as_deref(), Some("completed"));
    }

    #[tokio::test]
    async fn invalid_runtime_config_fails_fast() {
        let result = build_runtime_state(RuntimeBootstrapConfig {
            server: server_config(),
            store: StoreBootstrapConfig::Sqlite {
                database_url: "".to_string(),
            },
            blob: BlobBootstrapConfig::InMemory,
            provider: ProviderBootstrapConfig::Mock {
                response: "processed".to_string(),
            },
        })
        .await;

        match result {
            Ok(_) => panic!("expected config error"),
            Err(error) => assert!(error.to_string().contains("must not be empty")),
        }
    }

    #[test]
    fn local_directory_blob_store_rejects_invalid_uri() {
        let error = LocalFileBlobStore::path_from_uri("memory://abc").expect_err("error");
        assert_eq!(error.message, "unsupported local blob uri");
    }
}
