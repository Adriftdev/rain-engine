use rain_engine_core::{EnginePolicy, ProviderRequestConfig};
use rain_engine_runtime::{
    BlobBootstrapConfig, ProviderBootstrapConfig, RuntimeBootstrapConfig, RuntimeServerConfig,
    StoreBootstrapConfig,
};
use std::net::SocketAddr;

fn main() {
    let config = RuntimeBootstrapConfig {
        server: RuntimeServerConfig {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8080)),
            request_timeout_ms: 15_000,
            default_policy: EnginePolicy::default(),
            allow_policy_overrides: false,
            allow_provider_overrides: false,
            default_provider: ProviderRequestConfig {
                model: Some("gpt-4o-mini".to_string()),
                temperature: Some(0.1),
                max_tokens: Some(512),
            },
            async_ingress: false,
        },
        store: StoreBootstrapConfig::Postgres {
            database_url: "postgres://postgres:postgres@localhost/rain_engine".to_string(),
        },
        cache: None,
        blob: BlobBootstrapConfig::InMemory,
        provider: ProviderBootstrapConfig::OpenAiCompatible {
            base_url: "https://api.openai.com/v1".to_string(),
            api_key: "set-me-via-env".to_string(),
            model: "gpt-4o-mini".to_string(),
            temperature: Some(0.1),
            max_tokens: Some(512),
            system_prompt: "You are a production automation agent.".to_string(),
        },
        enable_research_planner: false,
    };

    println!("runtime_postgres bootstrap config: {:?}", config);
}
