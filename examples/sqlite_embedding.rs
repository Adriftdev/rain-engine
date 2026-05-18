use rain_engine::kernel::{
    AgentAction, AgentEngine, AgentTrigger, MockLlmProvider, ProcessRequest,
};
use rain_engine::runtime::run_until_terminal;
use rain_engine::stores::SqliteMemoryStore;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(SqliteMemoryStore::connect("sqlite::memory:").await?);
    let provider = Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
        content: "sqlite-backed response".to_string(),
    }]));
    let engine = AgentEngine::new(provider, store);

    let outcome = run_until_terminal(
        &engine,
        ProcessRequest::new(
            "sqlite-example",
            AgentTrigger::Message {
                user_id: "user-1".to_string(),
                content: "Run with SQLite".to_string(),
                attachments: Vec::new(),
            },
        ),
    )
    .await?;

    println!("terminal outcome: {:?}", outcome.stop_reason);
    Ok(())
}
