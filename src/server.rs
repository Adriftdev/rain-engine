use rain_engine_blob::BlobBackendConfig;
use rain_engine_core::{EnginePolicy, ProviderRequestConfig};
use rain_engine_runtime::{
    GeminiAuthMode, ProviderBootstrapConfig, RuntimeBootstrapConfig, RuntimeServerConfig,
    StoreBootstrapConfig, build_runtime_state, serve,
};
use std::net::SocketAddr;

pub struct ServerBuilder {
    config: RuntimeBootstrapConfig,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            config: RuntimeBootstrapConfig {
                server: RuntimeServerConfig {
                    bind_address: "127.0.0.1:8080".parse().unwrap(),
                    request_timeout_ms: 30000,
                    default_policy: EnginePolicy::default(),
                    allow_policy_overrides: false,
                    allow_provider_overrides: false,
                    default_provider: ProviderRequestConfig {
                        model: None,
                        temperature: None,
                        max_tokens: None,
                    },
                },
                store: StoreBootstrapConfig::InMemory, // Portable by default
                cache: None,
                blob: BlobBackendConfig::InMemory,     // Portable by default
                provider: ProviderBootstrapConfig::Mock {
                    response: "Mock Response".to_string(),
                },
                enable_research_planner: false,
            },
        }
    }
}

impl ServerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(mut self, config: RuntimeBootstrapConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_bind_address(mut self, addr: SocketAddr) -> Self {
        self.config.server.bind_address = addr;
        self
    }

    pub fn with_sqlite(mut self, database_url: impl Into<String>) -> Self {
        self.config.store = StoreBootstrapConfig::Sqlite {
            database_url: database_url.into(),
        };
        self
    }

    pub fn with_postgres(mut self, database_url: impl Into<String>) -> Self {
        self.config.store = StoreBootstrapConfig::Postgres {
            database_url: database_url.into(),
        };
        self
    }

    pub fn with_in_memory_store(mut self) -> Self {
        self.config.store = StoreBootstrapConfig::InMemory;
        self
    }

    pub fn with_gemini(
        mut self,
        base_url: impl Into<String>,
        credential: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        self.config.provider = ProviderBootstrapConfig::Gemini {
            base_url: base_url.into(),
            auth_mode: GeminiAuthMode::ApiKey,
            credential: credential.into(),
            model: model.into(),
            temperature: None,
            max_tokens: None,
            system_instruction: "You are a multimodal server-side automation agent. Use tools when they can complete the task precisely.".to_string(),
            provider_name: "gemini".to_string(),
            embedding_model: "text-embedding-004".to_string(),
        };
        self
    }

    pub fn with_openai(
        mut self,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        self.config.provider = ProviderBootstrapConfig::OpenAiCompatible {
            base_url: base_url.into(),
            api_key: api_key.into(),
            model: model.into(),
            temperature: None,
            max_tokens: None,
            system_prompt: "You are a server-side automation agent. Prefer tool calls when available. When replying directly, return plain text or JSON with type=yield.".to_string(),
        };
        self
    }

    pub async fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = self.config.server.bind_address;
        let state = build_runtime_state(self.config).await?;
        serve(addr, state).await?;
        Ok(())
    }
}
