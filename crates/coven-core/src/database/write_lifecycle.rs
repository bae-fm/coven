use super::*;

impl Database {
    pub(crate) fn set_write_status_on(
        conn: &Connection,
        write_id: &WriteId,
        status: &WriteStatus,
    ) -> Result<(), DbError> {
        let status = serde_json::to_string(status)
            .map_err(|error| DbError::Message(format!("serialize write status: {error}")))?;
        let updated = conn
            .execute(
                "UPDATE store_writes SET status = ?2 WHERE write_id = ?1",
                rusqlite::params![write_id.as_str(), status],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(format!("write {write_id} does not exist")));
        }
        Ok(())
    }

    pub(crate) fn notify_write_status(&self, write_id: WriteId, status: WriteStatus) {
        let senders = self
            .state
            .write_statuses
            .lock()
            .expect("write status mutex poisoned");
        if let Some(sender) = senders.get(&write_id) {
            sender.send_replace(status);
        }
    }

    pub async fn write_status(&self, write_id: &WriteId) -> Result<WriteStatus, DbError> {
        let write_id = write_id.clone();
        self.call(move |conn| {
            let raw: String = conn
                .query_row(
                    "SELECT status FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            serde_json::from_str(&raw)
                .map_err(|error| DbError::Message(format!("write {write_id} status: {error}")))
        })
        .await
    }

    pub async fn pending_writes(&self) -> Result<Vec<PendingWrite>, DbError> {
        self.call(move |conn| {
            let mut statement = conn
                .prepare(
                    "SELECT write_id, status, affected_rows FROM store_writes
                     WHERE status IN ('\"pending\"', '\"publishing\"')
                        OR json_extract(status, '$.blocked') IS NOT NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(DbError::from)?;
            rows.map(|row| {
                let (write_id, status, affected_rows) = row.map_err(DbError::from)?;
                Ok(PendingWrite {
                    write_id: WriteId::from_generated(write_id),
                    status: serde_json::from_str(&status).map_err(|error| {
                        DbError::Message(format!("pending write status: {error}"))
                    })?,
                    affected_rows: serde_json::from_str(&affected_rows).map_err(|error| {
                        DbError::Message(format!("pending affected rows: {error}"))
                    })?,
                })
            })
            .collect()
        })
        .await
    }

    /// Writes whose semantic publication fault requires an explicit host action.
    pub async fn blocked_writes(&self) -> Result<Vec<PendingWrite>, DbError> {
        Ok(self
            .pending_writes()
            .await?
            .into_iter()
            .filter(|write| matches!(write.status, WriteStatus::Blocked(_)))
            .collect())
    }

    pub async fn subscribe_write_status(
        &self,
        write_id: &WriteId,
    ) -> Result<tokio::sync::watch::Receiver<WriteStatus>, DbError> {
        let write_id = write_id.clone();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let raw: String = conn
                .query_row(
                    "SELECT status FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let current: WriteStatus = serde_json::from_str(&raw)
                .map_err(|error| DbError::Message(format!("write {write_id} status: {error}")))?;
            let mut senders = statuses.lock().expect("write status mutex poisoned");
            let sender = senders
                .entry(write_id)
                .or_insert_with(|| tokio::sync::watch::channel(current.clone()).0);
            sender.send_replace(current);
            Ok(sender.subscribe())
        })
        .await
    }
}
