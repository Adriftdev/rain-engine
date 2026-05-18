use rain_engine::kernel::ProviderRequestConfig;
use rain_engine::providers::{OpenAiCompatibleConfig, OpenAiCompatibleProvider};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = OpenAiCompatibleConfig {
        base_url: "https://api.openai.com/v1".to_string(),
        api_key: "replace-me".to_string(),
        default_request: ProviderRequestConfig {
            model: Some("gpt-4o-mini".to_string()),
            temperature: Some(0.1),
            max_tokens: Some(512),
        },
        system_prompt: "You are a precise automation assistant.".to_string(),
    };

    let _provider = OpenAiCompatibleProvider::new(config)?;
    println!("constructed OpenAI-compatible provider from the root crate");
    Ok(())
}
