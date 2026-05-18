use rain_engine::kernel::ProviderRequestConfig;
use rain_engine::providers::{GeminiAuth, GeminiConfig, GeminiProvider};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = GeminiConfig {
        base_url: "https://generativelanguage.googleapis.com/v1beta".to_string(),
        auth: GeminiAuth::ApiKey("replace-me".to_string()),
        default_request: ProviderRequestConfig {
            model: Some("gemini-2.5-flash".to_string()),
            temperature: Some(0.1),
            max_tokens: Some(512),
        },
        system_instruction: "You are a precise automation assistant.".to_string(),
        provider_name: "gemini".to_string(),
        embedding_model: "text-embedding-004".to_string(),
    };

    let _provider = GeminiProvider::new(config)?;
    println!("constructed Gemini provider from the root crate");
    Ok(())
}
