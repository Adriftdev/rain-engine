use crate::{GoalRecord, ObservationRecord, RelationshipEdge, SessionRecord, TaskRecord};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{message}")]
pub struct RetrievalError {
    pub message: String,
}

impl RetrievalError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RetrievedItemKind {
    Observation,
    Task,
    Goal,
    SessionRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetrievedItem {
    pub kind: RetrievedItemKind,
    pub key: String,
    pub score: f32,
    pub snippet: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct WorkingSet {
    pub observations: Vec<ObservationRecord>,
    pub tasks: Vec<TaskRecord>,
    pub goals: Vec<GoalRecord>,
}

#[async_trait]
pub trait RetrievalStore: Send + Sync {
    async fn exact_replay(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionRecord>, RetrievalError>;

    async fn semantic_search(
        &self,
        session_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<RetrievedItem>, RetrievalError>;

    async fn graph_neighbors(
        &self,
        session_id: &str,
        resource_id: &str,
        max_hops: usize,
    ) -> Result<Vec<RelationshipEdge>, RetrievalError>;

    async fn recent_working_set(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<WorkingSet, RetrievalError>;
}
