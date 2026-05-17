use rain_engine_blob::BlobBackendConfig;
use rain_engine_core::{EnginePolicy, ProviderRequestConfig};
use rain_engine_ingress::ValkeyStreamConfig;
use rain_engine_runtime::{
    BlobBootstrapConfig, GeminiAuthMode, ProviderBootstrapConfig, RuntimeBootstrapConfig,
    RuntimeServerConfig, StoreBootstrapConfig,
};
use std::net::SocketAddr;

fn main() {
    let config = RuntimeBootstrapConfig {
        server: RuntimeServerConfig {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8081)),
            request_timeout_ms: 30_000,
            default_policy: EnginePolicy {
                cache_threshold_tokens: 16_000,
                max_parallel_skill_calls: 8,
                ..EnginePolicy::default()
            },
            allow_policy_overrides: false,
            allow_provider_overrides: false,
            default_provider: ProviderRequestConfig {
                model: Some("gemini-1.5-pro".to_string()),
                temperature: Some(0.1),
                max_tokens: Some(1_024),
            },
        },
        store: StoreBootstrapConfig::Postgres {
            database_url: "postgres://postgres:postgres@localhost/rain_engine".to_string(),
        },
        blob: BlobBootstrapConfig::LocalDirectory {
            path: "./.rain-engine/blobs".to_string(),
        },
        provider: ProviderBootstrapConfig::Gemini {
            base_url: "https://generativelanguage.googleapis.com/v1beta".to_string(),
            auth_mode: GeminiAuthMode::ApiKey,
            credential: "${GEMINI_API_KEY}".to_string(),
            model: "gemini-1.5-pro".to_string(),
            temperature: Some(0.1),
            max_tokens: Some(1_024),
            system_instruction:
                "You are a customer support automation agent with access to trusted backend tools."
                    .to_string(),
            provider_name: "gemini".to_string(),
            embedding_model: "text-embedding-004".to_string(),
        },
        enable_research_planner: true,
    };

    let ingress = ValkeyStreamConfig {
        url: "redis://127.0.0.1/".to_string(),
        stream: "rain-engine.support.events".to_string(),
        group: "support-workers".to_string(),
        consumer: "worker-1".to_string(),
        block_ms: 5_000,
    };

    let _blob_backend = BlobBackendConfig::LocalDirectory {
        path: "./.rain-engine/blobs".to_string(),
    };

    println!("customer_support_agent runtime: {:?}", config);
    println!("customer_support_agent ingress: {:?}", ingress);
}
