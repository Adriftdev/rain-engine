use crate::{
    ApprovalResolutionRecord, CoordinationClaimRecord, DelegationRecord, EngineOutcome,
    ModelDecisionRecord, NewSessionRecord, OutcomeRecord, PendingApprovalRecord,
    ProviderCacheRecord, ProviderUsageRecord, RecordPage, RecordPageQuery, SessionListQuery,
    SessionRecord, SessionSnapshot, SessionSummary, StoredSessionRecord, ToolCallRecord,
    ToolResultRecord, TriggerRecord,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{message}")]
pub struct MemoryError {
    pub message: String,
}

impl MemoryError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
pub trait MemoryStore: Send + Sync {
    async fn append_record(
        &self,
        record: NewSessionRecord,
    ) -> Result<StoredSessionRecord, MemoryError>;

    async fn load_session(&self, session_id: &str) -> Result<SessionSnapshot, MemoryError>;

    async fn list_sessions(
        &self,
        query: SessionListQuery,
    ) -> Result<Vec<SessionSummary>, MemoryError>;

    async fn list_records(&self, query: RecordPageQuery) -> Result<RecordPage, MemoryError>;

    async fn find_outcome_by_idempotency_key(
        &self,
        session_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<EngineOutcome>, MemoryError>;

    async fn find_pending_approval_by_resume_token(
        &self,
        session_id: &str,
        resume_token: &str,
    ) -> Result<Option<PendingApprovalRecord>, MemoryError>;
}

#[async_trait]
pub trait MemoryStoreExt: MemoryStore {
    async fn append_trigger(
        &self,
        record: TriggerRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        self.append_record(NewSessionRecord::from_record(
            record.session_id.clone(),
            SessionRecord::Trigger(record),
        ))
        .await
    }

    async fn append_model_decision(
        &self,
        session_id: &str,
        record: ModelDecisionRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        self.append_record(NewSessionRecord::from_record(
            session_id.to_string(),
            SessionRecord::ModelDecision(record),
        ))
        .await
    }

    async fn append_tool_call(
        &self,
        session_id: &str,
        record: ToolCallRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        self.append_record(NewSessionRecord::from_record(
            session_id.to_string(),
            SessionRecord::ToolCall(record),
        ))
        .await
    }

    async fn append_tool_result(
        &self,
        session_id: &str,
        record: ToolResultRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        self.append_record(NewSessionRecord::from_record(
            session_id.to_string(),
            SessionRecord::ToolResult(record),
        ))
        .await
    }

    async fn append_pending_approval(
        &self,
        session_id: &str,
        record: PendingApprovalRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        self.append_record(NewSessionRecord::from_record(
            session_id.to_string(),
            SessionRecord::PendingApproval(record),
        ))
        .await
    }

    async fn append_approval_resolution(
        &self,
        session_id: &str,
        record: ApprovalResolutionRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        self.append_record(NewSessionRecord::from_record(
            session_id.to_string(),
            SessionRecord::ApprovalResolution(record),
        ))
        .await
    }

    async fn append_delegation(
        &self,
        session_id: &str,
        record: DelegationRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        self.append_record(NewSessionRecord::from_record(
            session_id.to_string(),
            SessionRecord::Delegation(record),
        ))
        .await
    }

    async fn append_coordination_claim(
        &self,
        session_id: &str,
        record: CoordinationClaimRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        self.append_record(NewSessionRecord::from_record(
            session_id.to_string(),
            SessionRecord::CoordinationClaim(record),
        ))
        .await
    }

    async fn append_provider_usage(
        &self,
        session_id: &str,
        record: ProviderUsageRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        self.append_record(NewSessionRecord::from_record(
            session_id.to_string(),
            SessionRecord::ProviderUsage(record),
        ))
        .await
    }

    async fn append_provider_cache(
        &self,
        session_id: &str,
        record: ProviderCacheRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        self.append_record(NewSessionRecord::from_record(
            session_id.to_string(),
            SessionRecord::ProviderCache(record),
        ))
        .await
    }

    async fn append_outcome(
        &self,
        session_id: &str,
        record: OutcomeRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        self.append_record(NewSessionRecord::from_record(
            session_id.to_string(),
            SessionRecord::Outcome(record),
        ))
        .await
    }
}

impl<T> MemoryStoreExt for T where T: MemoryStore + ?Sized {}

#[derive(Debug, Default, Clone)]
pub struct InMemoryMemoryStore {
    inner: Arc<RwLock<HashMap<String, Vec<StoredSessionRecord>>>>,
    next_sequence_no: Arc<RwLock<i64>>,
}

impl InMemoryMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl MemoryStore for InMemoryMemoryStore {
    async fn append_record(
        &self,
        record: NewSessionRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        let mut sequence_guard = self.next_sequence_no.write().await;
        *sequence_guard += 1;
        let stored = StoredSessionRecord {
            session_id: record.session_id.clone(),
            sequence_no: *sequence_guard,
            occurred_at_ms: record.occurred_at_ms,
            record_kind: record.record_kind,
            trigger_id: record.trigger_id,
            idempotency_key: record.idempotency_key,
            record: record.record,
        };
        drop(sequence_guard);

        let mut guard = self.inner.write().await;
        guard
            .entry(stored.session_id.clone())
            .or_default()
            .push(stored.clone());
        Ok(stored)
    }

    async fn load_session(&self, session_id: &str) -> Result<SessionSnapshot, MemoryError> {
        let guard = self.inner.read().await;
        let records = guard.get(session_id).cloned().unwrap_or_default();
        let latest_outcome = records
            .iter()
            .rev()
            .find_map(|stored| match &stored.record {
                SessionRecord::Outcome(outcome) => Some(outcome.clone()),
                _ => None,
            });

        Ok(SessionSnapshot {
            session_id: session_id.to_string(),
            last_sequence_no: records.last().map(|record| record.sequence_no),
            latest_outcome,
            records: records.into_iter().map(|record| record.record).collect(),
        })
    }

    async fn list_sessions(
        &self,
        query: SessionListQuery,
    ) -> Result<Vec<SessionSummary>, MemoryError> {
        let guard = self.inner.read().await;
        let mut sessions = guard
            .iter()
            .filter_map(|(session_id, records)| {
                let mut filtered = records.iter().filter(|record| {
                    query
                        .since_ms
                        .is_none_or(|since_ms| record.occurred_at_ms >= since_ms)
                        && query
                            .until_ms
                            .is_none_or(|until_ms| record.occurred_at_ms <= until_ms)
                });
                let first = filtered.next()?;
                let mut last = first;
                let mut count = 1usize;
                for record in filtered {
                    last = record;
                    count += 1;
                }
                Some(SessionSummary {
                    session_id: session_id.clone(),
                    first_recorded_at_ms: first.occurred_at_ms,
                    last_recorded_at_ms: last.occurred_at_ms,
                    record_count: count,
                })
            })
            .collect::<Vec<_>>();
        sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        Ok(sessions
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect())
    }

    async fn list_records(&self, query: RecordPageQuery) -> Result<RecordPage, MemoryError> {
        let guard = self.inner.read().await;
        let all = guard.get(&query.session_id).cloned().unwrap_or_default();
        let filtered = all
            .into_iter()
            .filter(|record| {
                query
                    .since_ms
                    .is_none_or(|since_ms| record.occurred_at_ms >= since_ms)
                    && query
                        .until_ms
                        .is_none_or(|until_ms| record.occurred_at_ms <= until_ms)
            })
            .collect::<Vec<_>>();
        let total = filtered.len();
        let records = filtered
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect::<Vec<_>>();

        Ok(RecordPage {
            session_id: query.session_id,
            next_offset: (query.offset + records.len() < total)
                .then_some(query.offset + records.len()),
            records,
        })
    }

    async fn find_outcome_by_idempotency_key(
        &self,
        session_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<EngineOutcome>, MemoryError> {
        let guard = self.inner.read().await;
        Ok(guard
            .get(session_id)
            .into_iter()
            .flat_map(|records| records.iter().rev())
            .find_map(|stored| match &stored.record {
                SessionRecord::Outcome(outcome)
                    if outcome.idempotency_key.as_deref() == Some(idempotency_key) =>
                {
                    Some(EngineOutcome::from_record(outcome.clone()))
                }
                _ => None,
            }))
    }

    async fn find_pending_approval_by_resume_token(
        &self,
        session_id: &str,
        resume_token: &str,
    ) -> Result<Option<PendingApprovalRecord>, MemoryError> {
        let guard = self.inner.read().await;
        let records = guard.get(session_id).cloned().unwrap_or_default();
        let mut pending = None::<PendingApprovalRecord>;
        for stored in records {
            match stored.record {
                SessionRecord::PendingApproval(record)
                    if record.resume_token.as_str() == resume_token =>
                {
                    pending = Some(record);
                }
                SessionRecord::ApprovalResolution(record)
                    if record.resume_token.as_str() == resume_token =>
                {
                    pending = None;
                }
                _ => {}
            }
        }
        Ok(pending)
    }
}
