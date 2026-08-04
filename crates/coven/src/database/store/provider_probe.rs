use super::*;

use async_trait::async_trait;

use crate::protocol::objects::StorageError;
use crate::protocol::provider::{
    ProviderProbeId, ProviderProbeJournal, ProviderProbeJournalRecord,
};

#[async_trait]
impl ProviderProbeJournal for StoreDatabase {
    async fn load(
        &self,
        probe_id: ProviderProbeId,
    ) -> Result<Option<ProviderProbeJournalRecord>, StorageError> {
        let key = format!("provider_probe/{}", hex::encode(probe_id.as_bytes()));
        self.connection
            .call(move |connection| {
                let value = crate::database::get_protocol_state_on(connection, &key)?;
                value
                    .map(|value| {
                        serde_json::from_str(&value).map_err(|error| {
                            DbError::Message(format!("parse provider probe journal: {error}"))
                        })
                    })
                    .transpose()
            })
            .await
            .map_err(|error| StorageError::Storage(error.to_string()))
    }

    async fn begin(
        &self,
        prepared: ProviderProbeJournalRecord,
    ) -> Result<ProviderProbeJournalRecord, StorageError> {
        prepared.validate_begin().map_err(|error| {
            StorageError::Storage(format!("invalid provider probe journal beginning: {error}"))
        })?;
        let key = format!(
            "provider_probe/{}",
            hex::encode(prepared.probe_id().as_bytes())
        );
        let value = serde_json::to_string(&prepared).map_err(|error| {
            StorageError::Storage(format!("serialize provider probe journal: {error}"))
        })?;
        self.connection
            .call(move |connection| {
                let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
                transaction
                    .execute(
                        "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                        (&key, &value),
                    )
                    .map_err(DbError::from)?;
                let actual = crate::database::required_protocol_state_on(&transaction, &key)?;
                transaction.commit().map_err(DbError::from)?;
                serde_json::from_str(&actual).map_err(|error| {
                    DbError::Message(format!("parse provider probe journal: {error}"))
                })
            })
            .await
            .map_err(|error| StorageError::Storage(error.to_string()))
    }

    async fn advance(
        &self,
        previous: &ProviderProbeJournalRecord,
        next: ProviderProbeJournalRecord,
    ) -> Result<(), StorageError> {
        previous.validate_transition(&next).map_err(|error| {
            StorageError::Storage(format!("invalid provider probe journal advance: {error}"))
        })?;
        let key = format!(
            "provider_probe/{}",
            hex::encode(previous.probe_id().as_bytes())
        );
        let previous = serde_json::to_string(previous).map_err(|error| {
            StorageError::Storage(format!("serialize provider probe journal: {error}"))
        })?;
        let next = serde_json::to_string(&next).map_err(|error| {
            StorageError::Storage(format!("serialize provider probe journal: {error}"))
        })?;
        self.connection
            .call(move |connection| {
                let changed = connection
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
            .map_err(|error| StorageError::Storage(error.to_string()))
    }
}
