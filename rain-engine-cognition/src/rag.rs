use rain_engine_core::{EmbeddingProvider, ProviderError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentChunk {
    pub chunk_id: String,
    pub text: String,
    pub embedding: Vec<f32>,
    pub metadata: serde_json::Value,
}

pub struct CognitiveStore {
    embedding_provider: Arc<dyn EmbeddingProvider>,
    chunks: RwLock<Vec<DocumentChunk>>,
}

impl CognitiveStore {
    pub fn new(embedding_provider: Arc<dyn EmbeddingProvider>) -> Self {
        Self {
            embedding_provider,
            chunks: RwLock::new(Vec::new()),
        }
    }

    pub async fn ingest(
        &self,
        text: String,
        metadata: serde_json::Value,
    ) -> Result<(), ProviderError> {
        let chunks = chunk_text(&text, 1000);
        let embeddings = self
            .embedding_provider
            .generate_embeddings(chunks.clone())
            .await?;

        let mut store = self.chunks.write().await;
        for (chunk_text, embedding) in chunks.into_iter().zip(embeddings) {
            store.push(DocumentChunk {
                chunk_id: uuid::Uuid::new_v4().to_string(),
                text: chunk_text,
                embedding,
                metadata: metadata.clone(),
            });
        }
        Ok(())
    }

    pub async fn search(
        &self,
        query: String,
        limit: usize,
    ) -> Result<Vec<(DocumentChunk, f32)>, ProviderError> {
        let query_embedding = self
            .embedding_provider
            .generate_embeddings(vec![query])
            .await?
            .pop()
            .ok_or_else(|| ProviderError::internal("no embedding generated for query"))?;

        let chunks = self.chunks.read().await;
        let mut results: Vec<(DocumentChunk, f32)> = chunks
            .iter()
            .map(|chunk| {
                let score = cosine_similarity(&query_embedding, &chunk.embedding);
                (chunk.clone(), score)
            })
            .collect();

        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);

        Ok(results)
    }
}

fn chunk_text(text: &str, size: usize) -> Vec<String> {
    text.chars()
        .collect::<Vec<_>>()
        .chunks(size)
        .map(|c| c.iter().collect::<String>())
        .collect()
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a * norm_b)
}
