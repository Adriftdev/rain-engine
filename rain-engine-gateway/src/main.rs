//! RainEngine Gateway — full-featured daemon with:
//! - HTTP API server with permissive CORS
//! - Heartbeat scheduler (proactive agent)
//! - Wake executor (scheduled task triggers)
//! - Built-in skill registration
//! - Multi-channel adapter spawning
//! - Graceful shutdown via SIGINT/SIGTERM

mod heartbeat;
mod wake;

use rain_engine_blob::BlobBackendConfig;
use rain_engine_channels::{ChannelAdapter, ChannelConfig};
use rain_engine_core::{EnginePolicy, ProviderRequestConfig};
use rain_engine_runtime::{
    GeminiAuthMode, ProviderBootstrapConfig, RuntimeBootstrapConfig, RuntimeServerConfig,
    StoreBootstrapConfig, app, build_runtime_state, init_tracing,
};
use std::collections::HashSet;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    init_tracing();

    let config = resolve_config()?;
    let addr = config.server.bind_address;

    tracing::info!("Building runtime state...");
    let state = build_runtime_state(config).await?;

    // ---------------------------------------------------------------------------
    // Register built-in native skills
    // ---------------------------------------------------------------------------
    let workspace_root = env::var("RAIN_WORKSPACE_ROOT").unwrap_or_else(|_| ".".into());
    let shell_timeout = Duration::from_secs(
        env::var("RAIN_SKILL_SHELL_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30),
    );
    let http_timeout = Duration::from_secs(
        env::var("RAIN_SKILL_HTTP_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30),
    );

    // Shell exec — default-deny allowlist from env
    let allowed_commands: HashSet<String> = env::var("RAIN_SKILL_SHELL_ALLOWLIST")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .collect();
    state
        .engine()
        .register_native_skill(
            rain_engine_skills::shell_exec::manifest(),
            Arc::new(rain_engine_skills::shell_exec::ShellExecSkill::new(
                allowed_commands,
                shell_timeout,
            )),
        )
        .await;

    // File read/write
    state
        .engine()
        .register_native_skill(
            rain_engine_skills::file_ops::read_manifest(),
            Arc::new(rain_engine_skills::file_ops::FileReadSkill::new(
                &workspace_root,
            )),
        )
        .await;
    state
        .engine()
        .register_native_skill(
            rain_engine_skills::file_ops::write_manifest(),
            Arc::new(rain_engine_skills::file_ops::FileWriteSkill::new(
                &workspace_root,
            )),
        )
        .await;

    // HTTP fetch
    let allowed_hosts: HashSet<String> = env::var("RAIN_SKILL_HTTP_ALLOWLIST")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .collect();
    state
        .engine()
        .register_native_skill(
            rain_engine_skills::http_fetch::manifest(),
            Arc::new(rain_engine_skills::http_fetch::HttpFetchSkill::new(
                allowed_hosts,
                http_timeout,
            )),
        )
        .await;

    // Memory search
    state
        .engine()
        .register_native_skill(
            rain_engine_skills::memory_search::manifest(),
            Arc::new(rain_engine_skills::memory_search::MemorySearchSkill::new(
                state.memory(),
            )),
        )
        .await;

    tracing::info!("Registered 5 built-in skills");

    // ---------------------------------------------------------------------------
    // Build the HTTP router with CORS
    // ---------------------------------------------------------------------------
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    let router = app(state.clone()).layer(cors);

    // ---------------------------------------------------------------------------
    // Graceful shutdown signal
    // ---------------------------------------------------------------------------
    let cancel = CancellationToken::new();

    // ---------------------------------------------------------------------------
    // Heartbeat scheduler
    // ---------------------------------------------------------------------------
    let heartbeat_enabled = env::var("RAIN_HEARTBEAT_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if heartbeat_enabled {
        let heartbeat_interval = Duration::from_secs(
            env::var("RAIN_HEARTBEAT_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1800),
        );
        let active_start: u8 = env::var("RAIN_HEARTBEAT_ACTIVE_START")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(7);
        let active_end: u8 = env::var("RAIN_HEARTBEAT_ACTIVE_END")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(23);
        let heartbeat_session =
            env::var("RAIN_HEARTBEAT_SESSION").unwrap_or_else(|_| "heartbeat-main".into());

        let gateway_url = format!("http://{addr}");
        let scheduler = heartbeat::HeartbeatScheduler::new(
            &gateway_url,
            heartbeat_session,
            heartbeat_interval,
            (active_start, active_end),
        );
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            scheduler.run(cancel_clone).await;
        });
        tracing::info!(
            interval_secs = heartbeat_interval.as_secs(),
            "Heartbeat scheduler started"
        );
    }

    // ---------------------------------------------------------------------------
    // Wake executor
    // ---------------------------------------------------------------------------
    let wake_enabled = env::var("RAIN_WAKE_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if wake_enabled {
        let gateway_url = format!("http://{addr}");
        let poll_interval = Duration::from_secs(
            env::var("RAIN_WAKE_POLL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(15),
        );
        let executor = wake::WakeExecutor::new(&gateway_url, state.memory(), poll_interval);
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            executor.run(cancel_clone).await;
        });
        tracing::info!("Wake executor started");
    }

    // ---------------------------------------------------------------------------
    // Channel adapters
    // ---------------------------------------------------------------------------
    let channel_config = ChannelConfig {
        gateway_url: format!("http://{addr}"),
        default_session_prefix: env::var("RAIN_SESSION_PREFIX").unwrap_or_else(|_| "rain".into()),
    };

    // Telegram
    if let Ok(token) = env::var("TELEGRAM_BOT_TOKEN") {
        let adapter =
            rain_engine_channels::telegram::TelegramAdapter::new(token, channel_config.clone());
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            adapter.run(cancel_clone).await;
        });
        tracing::info!("Telegram channel adapter started");
    }

    // Discord
    if let Ok(token) = env::var("DISCORD_BOT_TOKEN") {
        let adapter =
            rain_engine_channels::discord::DiscordAdapter::new(token, channel_config.clone());
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            adapter.run(cancel_clone).await;
        });
        tracing::info!("Discord channel adapter started");
    }

    // Slack
    if let (Ok(bot_token), Ok(signing_secret)) = (
        env::var("SLACK_BOT_TOKEN"),
        env::var("SLACK_SIGNING_SECRET"),
    ) {
        let listen_port: u16 = env::var("SLACK_LISTEN_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3100);
        let adapter = rain_engine_channels::slack::SlackAdapter::new(
            bot_token,
            signing_secret,
            listen_port,
            channel_config,
        );
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            adapter.run(cancel_clone).await;
        });
        tracing::info!("Slack channel adapter started");
    }

    // ---------------------------------------------------------------------------
    // Start the HTTP server with graceful shutdown
    // ---------------------------------------------------------------------------
    tracing::info!(%addr, "RainEngine Gateway listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;

    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Received shutdown signal");
        cancel_clone.cancel();
    });

    axum::serve(listener, router)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;

    tracing::info!("RainEngine Gateway stopped");
    Ok(())
}

/// Resolve configuration from env vars, YAML file, or defaults.
fn resolve_config() -> Result<RuntimeBootstrapConfig, Box<dyn std::error::Error>> {
    // Try YAML config file first
    let config_path = env::var("RAIN_CONFIG_PATH").unwrap_or_else(|_| "rain-engine.yaml".into());
    if let Ok(text) = std::fs::read_to_string(&config_path) {
        tracing::info!(path = %config_path, "Loading config from file");
        let config: RuntimeBootstrapConfig = serde_yaml::from_str(&text)?;
        return Ok(config);
    }

    tracing::info!("No config file found, resolving from environment / defaults");

    let bind_addr: SocketAddr = env::var("RAIN_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()?;

    let request_timeout_ms: u64 = env::var("RAIN_REQUEST_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30_000);

    let store = match env::var("RAIN_STORE_KIND").as_deref() {
        Ok("sqlite") => StoreBootstrapConfig::Sqlite {
            database_url: env::var("RAIN_STORE_URL")
                .unwrap_or_else(|_| "sqlite://rain-engine.db?mode=rwc".into()),
        },
        Ok("postgres") => StoreBootstrapConfig::Postgres {
            database_url: env::var("RAIN_STORE_URL")
                .map_err(|_| "RAIN_STORE_URL required for postgres store")?,
        },
        _ => StoreBootstrapConfig::InMemory,
    };

    let blob = match env::var("RAIN_BLOB_KIND").as_deref() {
        Ok("local") => BlobBackendConfig::LocalDirectory {
            path: env::var("RAIN_BLOB_PATH").unwrap_or_else(|_| "./blobs".into()),
        },
        _ => BlobBackendConfig::InMemory,
    };

    let provider_kind = env::var("RAIN_PROVIDER_KIND");
    tracing::info!("RAIN_PROVIDER_KIND = {:?}", provider_kind);

    let system_prompt = env::var("RAIN_SYSTEM_PROMPT").unwrap_or_else(|_| {
        "You are RainEngine, a highly capable server-side automation agent. \
         Your goal is to fulfill user requests by autonomously executing skills. \
         Available Skills:\
         - shell_exec(command: string): Execute a shell command.\
         - file_read(path: string): Read a file relative to workspace.\
         - file_write(path: string, content: string): Write a file relative to workspace.\
         - http_fetch(url: string): Make HTTP GET/POST requests.\
         - memory_search(query: string): Search session memory.\
         Rules of Engagement:\
         1. EXPLORE: Use 'shell_exec' with 'ls' or 'pwd' to discover the environment.\
         2. PLAN: Break complex tasks into smaller tool calls. Use exact skill names.\
         3. PERSIST: If a tool fails (e.g. permission denied), try an alternative or explain why.\
         4. VERIFY: Confirm your actions worked by reading files or checking command output.\
         5. CONCISE: Keep text responses professional and technical.\
         Always prefer tool calls over text when an action is possible."
            .into()
    });

    let provider = match provider_kind.as_deref() {
        Ok("gemini") => ProviderBootstrapConfig::Gemini {
            base_url: env::var("RAIN_PROVIDER_BASE_URL")
                .unwrap_or_else(|_| "https://generativelanguage.googleapis.com/v1beta".into()),
            auth_mode: GeminiAuthMode::ApiKey,
            credential: env::var("RAIN_PROVIDER_API_KEY")
                .map_err(|_| "RAIN_PROVIDER_API_KEY required for gemini provider")?,
            model: env::var("RAIN_PROVIDER_MODEL").unwrap_or_else(|_| "gemini-2.0-flash".into()),
            temperature: env::var("RAIN_PROVIDER_TEMPERATURE")
                .ok()
                .and_then(|v| v.parse().ok()),
            max_tokens: env::var("RAIN_PROVIDER_MAX_TOKENS")
                .ok()
                .and_then(|v| v.parse().ok()),
            system_instruction: system_prompt.clone(),
            provider_name: "gemini".into(),
        },
        Ok("openai") => ProviderBootstrapConfig::OpenAiCompatible {
            base_url: env::var("RAIN_PROVIDER_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".into()),
            api_key: env::var("RAIN_PROVIDER_API_KEY")
                .map_err(|_| "RAIN_PROVIDER_API_KEY required for openai provider")?,
            model: env::var("RAIN_PROVIDER_MODEL").unwrap_or_else(|_| "gpt-4o".into()),
            temperature: env::var("RAIN_PROVIDER_TEMPERATURE")
                .ok()
                .and_then(|v| v.parse().ok()),
            max_tokens: env::var("RAIN_PROVIDER_MAX_TOKENS")
                .ok()
                .and_then(|v| v.parse().ok()),
            system_prompt: system_prompt.clone(),
        },
        Ok("ollama") => ProviderBootstrapConfig::OpenAiCompatible {
            base_url: env::var("RAIN_PROVIDER_BASE_URL")
                .unwrap_or_else(|_| "http://localhost:11434/v1".into()),
            api_key: env::var("RAIN_PROVIDER_API_KEY").unwrap_or_else(|_| "ollama".into()),
            model: env::var("RAIN_PROVIDER_MODEL").unwrap_or_else(|_| "gemma3".into()),
            temperature: env::var("RAIN_PROVIDER_TEMPERATURE")
                .ok()
                .and_then(|v| v.parse().ok()),
            max_tokens: env::var("RAIN_PROVIDER_MAX_TOKENS")
                .ok()
                .and_then(|v| v.parse().ok()),
            system_prompt: system_prompt.clone(),
        },
        _ => {
            tracing::warn!("No provider configured, using mock provider");
            ProviderBootstrapConfig::Mock {
                response: "Mock Response".into(),
            }
        }
    };

    Ok(RuntimeBootstrapConfig {
        server: RuntimeServerConfig {
            bind_address: bind_addr,
            request_timeout_ms,
            default_policy: EnginePolicy::default(),
            allow_policy_overrides: env::var("RAIN_ALLOW_POLICY_OVERRIDES")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            allow_provider_overrides: env::var("RAIN_ALLOW_PROVIDER_OVERRIDES")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            default_provider: ProviderRequestConfig::default(),
        },
        store,
        blob,
        provider,
    })
}
