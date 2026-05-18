use rain_engine::kernel::{
    AdvanceRequest, AgentAction, AgentEngine, AgentTrigger, InMemoryMemoryStore, MockLlmProvider,
    ProcessRequest,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let memory = Arc::new(InMemoryMemoryStore::new());
    let provider = Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
        content: "hello from rain-engine".to_string(),
    }]));

    let engine = AgentEngine::new(provider, memory);
    let request = ProcessRequest::new(
        "example-session",
        AgentTrigger::Message {
            user_id: "user-1".to_string(),
            content: "Say hello".to_string(),
            attachments: Vec::new(),
        },
    );

    let advance = engine.advance(AdvanceRequest::Trigger(request)).await?;
    println!("advance outcome: {:?}", advance.outcome);
    Ok(())
}
