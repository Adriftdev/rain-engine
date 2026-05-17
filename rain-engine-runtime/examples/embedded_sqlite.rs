use async_trait::async_trait;
use rain_engine_core::{
    AgentAction, AgentEngine, AgentTrigger, MockLlmProvider, PlannedSkillCall, ProcessRequest,
    SkillExecutor, SkillInvocation, SkillManifestDescriptor,
};
use rain_engine_runtime::run_until_terminal;
use rain_engine_store_sqlite::SqliteMemoryStore;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, rain_engine_macros::SkillManifest)]
#[skill(
    name = "echo_input",
    description = "Echo the provided payload back to the caller.",
    scopes("tool:run"),
    capabilities("log")
)]
struct EchoInput {
    value: String,
}

struct EchoExecutor;

#[async_trait]
impl SkillExecutor for EchoExecutor {
    async fn execute(
        &self,
        invocation: SkillInvocation,
    ) -> Result<serde_json::Value, rain_engine_core::SkillExecutionError> {
        Ok(json!({
            "echoed": invocation.args,
        }))
    }

    fn executor_kind(&self) -> &'static str {
        "wasm"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(SqliteMemoryStore::connect("sqlite::memory:").await?);
    let llm = Arc::new(MockLlmProvider::scripted(vec![
        AgentAction::CallSkills(vec![PlannedSkillCall {
            call_id: "call-1".to_string(),
            name: EchoInput::skill_manifest().name,
            args: json!({"value": "hello"}),
            priority: 0,
            depends_on: Vec::new(),
            retry_policy: Default::default(),
            dry_run: false,
        }]),
        AgentAction::Respond {
            content: "completed".to_string(),
        },
    ]));
    let engine = AgentEngine::new(llm, store);
    engine.register_wasm_skill(EchoInput::skill_manifest(), Arc::new(EchoExecutor));

    let outcome = run_until_terminal(
        &engine,
        ProcessRequest::new(
            "example-session",
            AgentTrigger::Message {
                user_id: "u1".to_string(),
                content: "Run the skill".to_string(),
                attachments: Vec::new(),
            },
        )
        .with_scope("tool:run"),
    )
    .await?;

    println!("embedded_sqlite outcome: {:?}", outcome);
    Ok(())
}
