//! SQLite ledger store for local RainEngine development and tests.

use async_trait::async_trait;
use rain_engine_core::{
    EngineOutcome, MemoryError, MemoryStore, NewSessionRecord, PendingApprovalRecord, RecordPage,
    RecordPageQuery, SessionListQuery, SessionRecord, SessionRecordKind, SessionSnapshot,
    SessionSummary, StoredSessionRecord,
};
use serde_json::from_str;
use sqlx::{Row, SqlitePool};

#[derive(Clone)]
pub struct SqliteMemoryStore {
    pool: SqlitePool,
}

impl SqliteMemoryStore {
    pub async fn connect(database_url: &str) -> Result<Self, MemoryError> {
        let pool = SqlitePool::connect(database_url)
            .await
            .map_err(|err| MemoryError::new(err.to_string()))?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS session_records (
                sequence_no INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                occurred_at_ms INTEGER NOT NULL,
                record_kind TEXT NOT NULL,
                trigger_id TEXT,
                idempotency_key TEXT,
                payload_json TEXT NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await
        .map_err(|err| MemoryError::new(err.to_string()))?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_session_records_session_id ON session_records(session_id)",
        )
        .execute(&pool)
        .await
        .map_err(|err| MemoryError::new(err.to_string()))?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[async_trait]
impl MemoryStore for SqliteMemoryStore {
    async fn append_record(
        &self,
        record: NewSessionRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        let payload_json = serde_json::to_string(&record.record)
            .map_err(|err| MemoryError::new(err.to_string()))?;
        let sequence_no: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO session_records (session_id, occurred_at_ms, record_kind, trigger_id, idempotency_key, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            RETURNING sequence_no
            "#,
        )
        .bind(&record.session_id)
        .bind(record.occurred_at_ms)
        .bind(record.record_kind.as_str())
        .bind(&record.trigger_id)
        .bind(&record.idempotency_key)
        .bind(payload_json)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| MemoryError::new(err.to_string()))?;

        Ok(StoredSessionRecord {
            session_id: record.session_id,
            sequence_no,
            occurred_at_ms: record.occurred_at_ms,
            record_kind: record.record_kind,
            trigger_id: record.trigger_id,
            idempotency_key: record.idempotency_key,
            record: record.record,
        })
    }

    async fn load_session(&self, session_id: &str) -> Result<SessionSnapshot, MemoryError> {
        let rows = sqlx::query(
            r#"
            SELECT sequence_no, payload_json
            FROM session_records
            WHERE session_id = ?
            ORDER BY sequence_no ASC
            "#,
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| MemoryError::new(err.to_string()))?;

        let mut records = Vec::with_capacity(rows.len());
        let mut last_sequence_no = None;
        let mut latest_outcome = None;
        for row in rows {
            last_sequence_no = Some(row.get::<i64, _>("sequence_no"));
            let record: SessionRecord = from_str(row.get::<&str, _>("payload_json"))
                .map_err(|err| MemoryError::new(err.to_string()))?;
            if let SessionRecord::Outcome(outcome) = &record {
                latest_outcome = Some(outcome.clone());
            }
            records.push(record);
        }
        Ok(SessionSnapshot {
            session_id: session_id.to_string(),
            records,
            last_sequence_no,
            latest_outcome,
        })
    }

    async fn list_sessions(
        &self,
        query: SessionListQuery,
    ) -> Result<Vec<SessionSummary>, MemoryError> {
        let rows = sqlx::query(
            r#"
            SELECT session_id, MIN(occurred_at_ms) AS first_recorded_at_ms, MAX(occurred_at_ms) AS last_recorded_at_ms, COUNT(*) AS record_count
            FROM session_records
            WHERE (? IS NULL OR occurred_at_ms >= ?)
              AND (? IS NULL OR occurred_at_ms <= ?)
            GROUP BY session_id
            ORDER BY session_id
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(query.since_ms)
        .bind(query.since_ms)
        .bind(query.until_ms)
        .bind(query.until_ms)
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| MemoryError::new(err.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| SessionSummary {
                session_id: row.get("session_id"),
                first_recorded_at_ms: row.get("first_recorded_at_ms"),
                last_recorded_at_ms: row.get("last_recorded_at_ms"),
                record_count: row.get::<i64, _>("record_count") as usize,
            })
            .collect())
    }

    async fn list_records(&self, query: RecordPageQuery) -> Result<RecordPage, MemoryError> {
        let rows = sqlx::query(
            r#"
            SELECT sequence_no, occurred_at_ms, record_kind, trigger_id, idempotency_key, payload_json
            FROM session_records
            WHERE session_id = ?
              AND (? IS NULL OR occurred_at_ms >= ?)
              AND (? IS NULL OR occurred_at_ms <= ?)
            ORDER BY sequence_no
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(&query.session_id)
        .bind(query.since_ms)
        .bind(query.since_ms)
        .bind(query.until_ms)
        .bind(query.until_ms)
        .bind(query.limit as i64)
        .bind(query.offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| MemoryError::new(err.to_string()))?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let record: SessionRecord = from_str(row.get::<&str, _>("payload_json"))
                .map_err(|err| MemoryError::new(err.to_string()))?;
            records.push(StoredSessionRecord {
                session_id: query.session_id.clone(),
                sequence_no: row.get("sequence_no"),
                occurred_at_ms: row.get("occurred_at_ms"),
                record_kind: SessionRecordKind::parse(row.get::<&str, _>("record_kind"))
                    .ok_or_else(|| MemoryError::new("unknown record kind"))?,
                trigger_id: row.get("trigger_id"),
                idempotency_key: row.get("idempotency_key"),
                record,
            });
        }
        let next_offset = (records.len() == query.limit).then_some(query.offset + records.len());
        Ok(RecordPage {
            session_id: query.session_id,
            records,
            next_offset,
        })
    }

    async fn find_outcome_by_idempotency_key(
        &self,
        session_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<EngineOutcome>, MemoryError> {
        let row = sqlx::query(
            r#"
            SELECT payload_json
            FROM session_records
            WHERE session_id = ?
              AND idempotency_key = ?
              AND record_kind = ?
            ORDER BY sequence_no DESC
            LIMIT 1
            "#,
        )
        .bind(session_id)
        .bind(idempotency_key)
        .bind(SessionRecordKind::Outcome.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| MemoryError::new(err.to_string()))?;

        match row {
            Some(row) => {
                let record: SessionRecord = from_str(row.get::<&str, _>("payload_json"))
                    .map_err(|err| MemoryError::new(err.to_string()))?;
                match record {
                    SessionRecord::Outcome(outcome) => {
                        Ok(Some(EngineOutcome::from_record(outcome)))
                    }
                    _ => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    async fn find_pending_approval_by_resume_token(
        &self,
        session_id: &str,
        resume_token: &str,
    ) -> Result<Option<PendingApprovalRecord>, MemoryError> {
        let snapshot = <Self as MemoryStore>::load_session(self, session_id).await?;
        let mut pending = None::<PendingApprovalRecord>;
        for record in snapshot.records {
            match record {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rain_engine_core::{
        AdvanceRequest, AgentAction, AgentEngine, AgentTrigger, ContinueRequest, EngineOutcome,
        MockLlmProvider, ProcessRequest,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn sqlite_store_replays_in_order() {
        let store = Arc::new(
            SqliteMemoryStore::connect("sqlite::memory:")
                .await
                .expect("sqlite store"),
        );
        let llm = Arc::new(MockLlmProvider::scripted(vec![AgentAction::Respond {
            content: "ok".to_string(),
        }]));
        let engine = AgentEngine::new(llm, store.clone());

        run_until_terminal(
            &engine,
            ProcessRequest::new(
                "sqlite-session",
                AgentTrigger::Message {
                    user_id: "u".to_string(),
                    content: "hello".to_string(),
                    attachments: Vec::new(),
                },
            ),
        )
        .await
        .expect("outcome");

        let snapshot = store
            .load_session("sqlite-session")
            .await
            .expect("snapshot");
        assert!(matches!(
            snapshot.records.first(),
            Some(SessionRecord::Trigger(_))
        ));
        assert!(matches!(
            snapshot.records.last(),
            Some(SessionRecord::Outcome(_))
        ));
    }

    async fn run_until_terminal(
        engine: &AgentEngine,
        request: ProcessRequest,
    ) -> Result<EngineOutcome, rain_engine_core::EngineError> {
        let mut next = AdvanceRequest::Trigger(request.clone());
        loop {
            let result = engine.advance(next).await?;
            if let Some(outcome) = result.outcome {
                return Ok(outcome);
            }
            next = AdvanceRequest::Continue(ContinueRequest {
                session_id: request.session_id.clone(),
                granted_scopes: request.granted_scopes.clone(),
                policy: request.policy.clone(),
                provider: request.provider.clone(),
                cancellation: request.cancellation.clone(),
            });
        }
    }
}
