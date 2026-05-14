//! Ledger-backed retrieval utilities for RainEngine.
//!
//! This crate provides exact replay, recent working sets, graph-neighbor
//! traversal, and simple semantic search over projected state.

use async_trait::async_trait;
use rain_engine_core::{
    MemoryStore, RelationshipEdge, RetrievalError, RetrievalStore, RetrievedItem,
    RetrievedItemKind, SessionRecord, WorkingSet,
};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

#[derive(Clone)]
pub struct SessionRetrievalStore {
    memory: Arc<dyn MemoryStore>,
}

impl SessionRetrievalStore {
    pub fn new(memory: Arc<dyn MemoryStore>) -> Self {
        Self { memory }
    }
}

#[async_trait]
impl RetrievalStore for SessionRetrievalStore {
    async fn exact_replay(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionRecord>, RetrievalError> {
        let snapshot = self
            .memory
            .load_session(session_id)
            .await
            .map_err(|err| RetrievalError::new(err.message))?;
        let take = limit.max(1);
        let len = snapshot.records.len();
        Ok(snapshot
            .records
            .into_iter()
            .skip(len.saturating_sub(take))
            .collect())
    }

    async fn semantic_search(
        &self,
        session_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<RetrievedItem>, RetrievalError> {
        let snapshot = self
            .memory
            .load_session(session_id)
            .await
            .map_err(|err| RetrievalError::new(err.message))?;
        let query = query.to_lowercase();
        let state = snapshot.agent_state();
        let mut hits = Vec::new();

        for observation in state.observations {
            let serialized = serde_json::to_string(&observation.content).unwrap_or_default();
            if serialized.to_lowercase().contains(&query)
                || observation.source.to_lowercase().contains(&query)
            {
                hits.push(RetrievedItem {
                    kind: RetrievedItemKind::Observation,
                    key: observation.observation_id.0,
                    score: 1.0,
                    snippet: serialized,
                });
            }
        }
        for task in state.tasks {
            if text_match(&query, &task.title, task.detail.as_deref()) {
                hits.push(RetrievedItem {
                    kind: RetrievedItemKind::Task,
                    key: task.task_id.0,
                    score: 0.9,
                    snippet: task.detail.unwrap_or(task.title),
                });
            }
        }
        for goal in state.goals {
            if text_match(&query, &goal.title, goal.detail.as_deref()) {
                hits.push(RetrievedItem {
                    kind: RetrievedItemKind::Goal,
                    key: goal.goal_id.0,
                    score: 0.8,
                    snippet: goal.detail.unwrap_or(goal.title),
                });
            }
        }

        hits.truncate(limit.max(1));
        Ok(hits)
    }

    async fn graph_neighbors(
        &self,
        session_id: &str,
        resource_id: &str,
        max_hops: usize,
    ) -> Result<Vec<RelationshipEdge>, RetrievalError> {
        let snapshot = self
            .memory
            .load_session(session_id)
            .await
            .map_err(|err| RetrievalError::new(err.message))?;
        let state = snapshot.agent_state();
        let mut frontier = VecDeque::from([(resource_id.to_string(), 0usize)]);
        let mut seen = HashSet::from([resource_id.to_string()]);
        let mut seen_edges = HashSet::<(String, String, String)>::new();
        let mut edges = Vec::new();

        while let Some((node, depth)) = frontier.pop_front() {
            if depth >= max_hops {
                continue;
            }
            for edge in &state.relationships {
                if edge.from_resource_id == node || edge.to_resource_id == node {
                    let edge_key = (
                        edge.from_resource_id.clone(),
                        edge.to_resource_id.clone(),
                        edge.relation.clone(),
                    );
                    if seen_edges.insert(edge_key) {
                        edges.push(edge.clone());
                    }
                    for next in [&edge.from_resource_id, &edge.to_resource_id] {
                        if seen.insert(next.clone()) {
                            frontier.push_back((next.clone(), depth + 1));
                        }
                    }
                }
            }
        }

        Ok(edges)
    }

    async fn recent_working_set(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<WorkingSet, RetrievalError> {
        let snapshot = self
            .memory
            .load_session(session_id)
            .await
            .map_err(|err| RetrievalError::new(err.message))?;
        let state = snapshot.agent_state();
        Ok(WorkingSet {
            observations: take_tail(state.observations, limit),
            tasks: take_tail(state.tasks, limit),
            goals: take_tail(state.goals, limit),
        })
    }
}

fn text_match(query: &str, title: &str, detail: Option<&str>) -> bool {
    title.to_lowercase().contains(query)
        || detail
            .map(|detail| detail.to_lowercase().contains(query))
            .unwrap_or(false)
}

fn take_tail<T>(items: Vec<T>, limit: usize) -> Vec<T> {
    let take = limit.max(1);
    let len = items.len();
    items.into_iter().skip(len.saturating_sub(take)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rain_engine_core::{
        AgentTrigger, InMemoryMemoryStore, KernelEvent, KernelEventRecord, MemoryStoreExt,
        ObservationId, ObservationRecord, RelationshipEdge, ResourceRef,
    };

    #[tokio::test]
    async fn semantic_search_finds_observations() {
        let memory = Arc::new(InMemoryMemoryStore::new());
        memory
            .append_trigger(rain_engine_core::TriggerRecord {
                trigger_id: "t1".to_string(),
                session_id: "s1".to_string(),
                idempotency_key: None,
                recorded_at: std::time::SystemTime::now(),
                trigger: AgentTrigger::Message {
                    user_id: "u1".to_string(),
                    content: "hello".to_string(),
                    attachments: Vec::new(),
                },
            })
            .await
            .expect("trigger");
        memory
            .append_kernel_event(
                "s1",
                KernelEventRecord {
                    event_id: "e1".to_string(),
                    occurred_at: std::time::SystemTime::now(),
                    event: KernelEvent::ObservationAppended(ObservationRecord {
                        observation_id: ObservationId("obs-1".to_string()),
                        recorded_at: std::time::SystemTime::now(),
                        source: "webhook".to_string(),
                        content: serde_json::json!({"text": "database schema mismatch"}),
                        attachment_ids: Vec::new(),
                        related_resources: Vec::new(),
                    }),
                },
            )
            .await
            .expect("event");

        let store = SessionRetrievalStore::new(memory);
        let hits = store
            .semantic_search("s1", "schema", 5)
            .await
            .expect("hits");
        assert_eq!(hits.len(), 1);
    }

    #[tokio::test]
    async fn graph_neighbors_follow_relationships() {
        let memory = Arc::new(InMemoryMemoryStore::new());
        memory
            .append_kernel_event(
                "s2",
                KernelEventRecord {
                    event_id: "e2".to_string(),
                    occurred_at: std::time::SystemTime::now(),
                    event: KernelEvent::ResourceRegistered(ResourceRef {
                        resource_id: "repo".to_string(),
                        resource_type: "repo".to_string(),
                        label: "repo".to_string(),
                        external_ref: None,
                    }),
                },
            )
            .await
            .expect("resource");
        memory
            .append_kernel_event(
                "s2",
                KernelEventRecord {
                    event_id: "e3".to_string(),
                    occurred_at: std::time::SystemTime::now(),
                    event: KernelEvent::RelationshipObserved(RelationshipEdge {
                        from_resource_id: "repo".to_string(),
                        to_resource_id: "ticket".to_string(),
                        relation: "tracks".to_string(),
                        observed_at: std::time::SystemTime::now(),
                    }),
                },
            )
            .await
            .expect("relationship");

        let store = SessionRetrievalStore::new(memory);
        let edges = store.graph_neighbors("s2", "repo", 2).await.expect("edges");
        assert_eq!(edges.len(), 1);
    }
}
