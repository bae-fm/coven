use super::*;

impl Database {
    pub(crate) async fn load_provider_probe_journal(
        &self,
        probe_id: crate::sync::provider::ProviderProbeId,
    ) -> Result<Option<crate::sync::provider::ProviderProbeJournalRecord>, DbError> {
        let key = format!("provider_probe/{}", hex::encode(probe_id.as_bytes()));
        self.call(move |conn| {
            let value = conn
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [key],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(DbError::from)?;
            value
                .map(|value| {
                    serde_json::from_str(&value).map_err(|error| {
                        DbError::Message(format!("parse provider probe journal: {error}"))
                    })
                })
                .transpose()
        })
        .await
    }

    pub(crate) async fn begin_provider_probe_journal(
        &self,
        prepared: crate::sync::provider::ProviderProbeJournalRecord,
    ) -> Result<crate::sync::provider::ProviderProbeJournalRecord, DbError> {
        prepared.validate_begin().map_err(|error| {
            DbError::Message(format!("invalid provider probe journal beginning: {error}"))
        })?;
        let key = format!(
            "provider_probe/{}",
            hex::encode(prepared.probe_id().as_bytes())
        );
        let value = serde_json::to_string(&prepared).map_err(|error| {
            DbError::Message(format!("serialize provider probe journal: {error}"))
        })?;
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            tx.execute(
                "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                (&key, &value),
            )
            .map_err(DbError::from)?;
            let actual: String = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [&key],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            tx.commit().map_err(DbError::from)?;
            serde_json::from_str(&actual)
                .map_err(|error| DbError::Message(format!("parse provider probe journal: {error}")))
        })
        .await
    }

    pub(crate) async fn advance_provider_probe_journal(
        &self,
        previous: crate::sync::provider::ProviderProbeJournalRecord,
        next: crate::sync::provider::ProviderProbeJournalRecord,
    ) -> Result<(), DbError> {
        previous.validate_transition(&next).map_err(|error| {
            DbError::Message(format!("invalid provider probe journal advance: {error}"))
        })?;
        let key = format!(
            "provider_probe/{}",
            hex::encode(previous.probe_id().as_bytes())
        );
        let previous = serde_json::to_string(&previous).map_err(|error| {
            DbError::Message(format!("serialize provider probe journal: {error}"))
        })?;
        let next = serde_json::to_string(&next).map_err(|error| {
            DbError::Message(format!("serialize provider probe journal: {error}"))
        })?;
        self.call(move |conn| {
            let changed = conn
                .execute(
                    "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                    (&next, &key, &previous),
                )
                .map_err(DbError::from)?;
            if changed != 1 {
                return Err(DbError::Message(
                    "provider probe journal advance lost its exact predecessor".to_string(),
                ));
            }
            Ok(())
        })
        .await
    }
}
