//! Command-line entrypoint for validating and running declarative RainEngine
//! deployments.

use rain_engine_blob::BlobBackendConfig;
use rain_engine_ingress::ValkeyStreamConfig;
use rain_engine_runtime::{
    ProviderBootstrapConfig, RuntimeBootstrapConfig, StoreBootstrapConfig, build_runtime_state,
    serve,
};
use rain_engine_store_valkey::ValkeyCoordinationStore;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct AgentFile {
    #[serde(default)]
    profile: AgentDeploymentProfile,
    runtime: RuntimeBootstrapConfig,
    #[serde(default)]
    retrieval: RetrievalConfig,
    coordination_url: Option<String>,
    ingress: Option<ValkeyStreamConfig>,
    #[serde(default)]
    skill_packs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct AgentDeploymentProfile {
    agent_id: String,
    role: String,
    default_scopes: Vec<String>,
    allowed_skill_names: Vec<String>,
    planning_cadence: String,
    max_active_tasks: usize,
    wake_policy: String,
    review_policy: String,
}

impl Default for AgentDeploymentProfile {
    fn default() -> Self {
        Self {
            agent_id: "rain-agent".to_string(),
            role: "event automation agent".to_string(),
            default_scopes: Vec::new(),
            allowed_skill_names: Vec::new(),
            planning_cadence: "event".to_string(),
            max_active_tasks: 4,
            wake_policy: "external_scheduler".to_string(),
            review_policy: "approval_scope".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RetrievalConfig {
    exact_replay: bool,
    recent_working_set_limit: usize,
    semantic_limit: usize,
    graph_hops: usize,
}

impl Default for RetrievalConfig {
    fn default() -> Self {
        Self {
            exact_replay: true,
            recent_working_set_limit: 32,
            semantic_limit: 8,
            graph_hops: 2,
        }
    }
}

#[derive(Debug, Error)]
enum CliError {
    #[error("{0}")]
    Message(String),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        print_usage();
        return Ok(());
    };
    match command.as_str() {
        "validate" => {
            let path = required_path(args.next(), "validate")?;
            let config = read_agent_file(&path)?;
            validate_agent_file(&config)?;
            println!("valid: {}", path.display());
        }
        "print-config" => {
            let path = required_path(args.next(), "print-config")?;
            let config = read_agent_file(&path)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&config)
                    .map_err(|err| CliError::Message(err.to_string()))?
            );
        }
        "run" => {
            let path = required_path(args.next(), "run")?;
            let config = read_agent_file(&path)?;
            validate_agent_file(&config)?;
            if let Some(coordination_url) = &config.coordination_url {
                let _ = ValkeyCoordinationStore::connect(coordination_url)
                    .map_err(|err| CliError::Message(err.message))?;
            }
            let state = build_runtime_state(config.runtime.clone())
                .await
                .map_err(|err| CliError::Message(err.to_string()))?;
            serve(config.runtime.server.bind_address, state).await?;
        }
        "pull-pack" => {
            let pack_ref = args
                .next()
                .ok_or_else(|| CliError::Message("missing pack reference".to_string()))?;
            let output_dir = required_path(args.next(), "pull-pack")?;
            pull_pack(&pack_ref, &output_dir)?;
            println!("pulled: {pack_ref} -> {}", output_dir.display());
        }
        _ => {
            print_usage();
        }
    }
    Ok(())
}

fn print_usage() {
    eprintln!("usage: rain-engine-cli <validate|print-config|run|pull-pack> ...");
}

fn required_path(value: Option<String>, command: &str) -> Result<PathBuf, CliError> {
    value
        .map(PathBuf::from)
        .ok_or_else(|| CliError::Message(format!("missing path for `{command}`")))
}

fn read_agent_file(path: &Path) -> Result<AgentFile, CliError> {
    let text = fs::read_to_string(path).map_err(|err| CliError::Message(err.to_string()))?;
    serde_yaml::from_str(&text).map_err(|err| CliError::Message(err.to_string()))
}

fn validate_agent_file(agent: &AgentFile) -> Result<(), CliError> {
    if agent.profile.agent_id.trim().is_empty() || agent.profile.role.trim().is_empty() {
        return Err(CliError::Message(
            "profile.agent_id and profile.role must not be empty".to_string(),
        ));
    }
    if agent.profile.max_active_tasks == 0 {
        return Err(CliError::Message(
            "profile.max_active_tasks must be greater than zero".to_string(),
        ));
    }
    if agent.retrieval.recent_working_set_limit == 0 || agent.retrieval.semantic_limit == 0 {
        return Err(CliError::Message(
            "retrieval limits must be greater than zero".to_string(),
        ));
    }
    if let StoreBootstrapConfig::Postgres { database_url } = &agent.runtime.store {
        if database_url.trim().is_empty() {
            return Err(CliError::Message(
                "runtime.store.postgres.database_url must not be empty".to_string(),
            ));
        }
    }
    match &agent.runtime.blob {
        BlobBackendConfig::LocalDirectory { path } if path.trim().is_empty() => {
            return Err(CliError::Message(
                "runtime.blob.local_directory.path must not be empty".to_string(),
            ));
        }
        _ => {}
    }
    match &agent.runtime.provider {
        ProviderBootstrapConfig::Gemini {
            base_url,
            credential,
            model,
            ..
        }
        | ProviderBootstrapConfig::OpenAiCompatible {
            base_url,
            api_key: credential,
            model,
            ..
        } => {
            if base_url.trim().is_empty() || credential.trim().is_empty() || model.trim().is_empty()
            {
                return Err(CliError::Message(
                    "provider configuration must include base_url, credential, and model"
                        .to_string(),
                ));
            }
        }
        ProviderBootstrapConfig::Mock { .. } => {}
    }
    Ok(())
}

fn pull_pack(pack_ref: &str, output_dir: &Path) -> Result<(), CliError> {
    fs::create_dir_all(output_dir).map_err(|err| CliError::Message(err.to_string()))?;
    let source = if let Some(path) = pack_ref.strip_prefix("file://") {
        PathBuf::from(path)
    } else if pack_ref.starts_with("oci://") {
        return Err(CliError::Message(
            "OCI RainPack pulling is not available in this local build; use a file:// path"
                .to_string(),
        ));
    } else {
        PathBuf::from(pack_ref)
    };
    let file_name = source
        .file_name()
        .ok_or_else(|| CliError::Message("invalid pack source".to_string()))?;
    fs::copy(&source, output_dir.join(file_name))
        .map_err(|err| CliError::Message(err.to_string()))?;
    Ok(())
}
