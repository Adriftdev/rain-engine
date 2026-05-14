use async_trait::async_trait;
use rain_engine_core::{
    EngineOutcome, MemoryError, MemoryStore, NewSessionRecord, PendingApprovalRecord, RecordPage,
    RecordPageQuery, SessionListQuery, SessionRecord, SessionRecordKind, SessionSnapshot,
    SessionSummary, StoredSessionRecord,
};
use sqlx::{PgPool, Row};

#[derive(Clone)]
pub struct PgMemoryStore {
    pool: PgPool,
}

impl PgMemoryStore {
    pub async fn connect(database_url: &str) -> Result<Self, MemoryError> {
        let pool = PgPool::connect(database_url)
            .await
            .map_err(|err| MemoryError::new(err.to_string()))?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS session_records (
                sequence_no BIGSERIAL PRIMARY KEY,
                session_id TEXT NOT NULL,
                occurred_at_ms BIGINT NOT NULL,
                record_kind TEXT NOT NULL,
                trigger_id TEXT,
                idempotency_key TEXT,
                payload_json JSONB NOT NULL
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

    pub fn connect_lazy(database_url: &str) -> Result<Self, MemoryError> {
        let pool =
            PgPool::connect_lazy(database_url).map_err(|err| MemoryError::new(err.to_string()))?;
        Ok(Self { pool })
    }
}

pub type PostgresMemoryStore = PgMemoryStore;

#[async_trait]
impl MemoryStore for PgMemoryStore {
    async fn append_record(
        &self,
        record: NewSessionRecord,
    ) -> Result<StoredSessionRecord, MemoryError> {
        let payload_json = serde_json::to_value(&record.record)
            .map_err(|err| MemoryError::new(err.to_string()))?;
        let sequence_no: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO session_records (session_id, occurred_at_ms, record_kind, trigger_id, idempotency_key, payload_json)
            VALUES ($1, $2, $3, $4, $5, $6)
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
            WHERE session_id = $1
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
            let value: serde_json::Value = row.get("payload_json");
            let record: SessionRecord =
                serde_json::from_value(value).map_err(|err| MemoryError::new(err.to_string()))?;
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
            WHERE ($1::BIGINT IS NULL OR occurred_at_ms >= $1)
              AND ($2::BIGINT IS NULL OR occurred_at_ms <= $2)
            GROUP BY session_id
            ORDER BY session_id
            OFFSET $3 LIMIT $4
            "#,
        )
        .bind(query.since_ms)
        .bind(query.until_ms)
        .bind(query.offset as i64)
        .bind(query.limit as i64)
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
            WHERE session_id = $1
              AND ($2::BIGINT IS NULL OR occurred_at_ms >= $2)
              AND ($3::BIGINT IS NULL OR occurred_at_ms <= $3)
            ORDER BY sequence_no
            OFFSET $4 LIMIT $5
            "#,
        )
        .bind(&query.session_id)
        .bind(query.since_ms)
        .bind(query.until_ms)
        .bind(query.offset as i64)
        .bind(query.limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| MemoryError::new(err.to_string()))?;
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let value: serde_json::Value = row.get("payload_json");
            let record: SessionRecord =
                serde_json::from_value(value).map_err(|err| MemoryError::new(err.to_string()))?;
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
            WHERE session_id = $1
              AND idempotency_key = $2
              AND record_kind = $3
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
                let value: serde_json::Value = row.get("payload_json");
                let record: SessionRecord = serde_json::from_value(value)
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

    #[tokio::test]
    async fn lazy_connection_validates_configuration_shape() {
        let store = PgMemoryStore::connect_lazy("postgres://postgres:postgres@localhost/test")
            .expect("lazy pool");
        let _ = store;
    }
}
