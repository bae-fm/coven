use crate::database::StoreDatabase;
use crate::protocol::store_commit::{DeviceJoinAttemptId, ObjectHash};
use crate::sync::{
    DeviceJoinAction, DeviceJoinError, DeviceJoinJournalRecord, DeviceJoinRole, DeviceJoinStatus,
};

pub(crate) struct StoreJoinJournal<'database> {
    database: &'database StoreDatabase,
}

impl<'database> StoreJoinJournal<'database> {
    pub(super) fn new(database: &'database StoreDatabase) -> Self {
        Self { database }
    }

    pub(crate) fn new_attempt_id(&self) -> DeviceJoinAttemptId {
        DeviceJoinAttemptId::from_hash(ObjectHash::digest(
            self.database.new_store_write_id().as_str().as_bytes(),
        ))
    }

    pub(crate) async fn begin(
        &self,
        record: DeviceJoinJournalRecord,
    ) -> Result<DeviceJoinJournalRecord, DeviceJoinError> {
        record.require_initial()?;
        let key = record.store_key();
        let value = serde_json::to_string(&record)?;
        self.database
            .connection
            .call(move |connection| {
                connection
                    .execute(
                        "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                        (&key, &value),
                    )
                    .map_err(crate::database::DbError::from)?;
                let actual = crate::database::required_protocol_state_on(connection, &key)?;
                serde_json::from_str(&actual)
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .map_err(|error| DeviceJoinError::Store(error.into_message()))
    }

    pub(crate) async fn load(
        &self,
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    ) -> Result<Option<DeviceJoinJournalRecord>, DeviceJoinError> {
        let key = DeviceJoinJournalRecord::store_key_for(attempt_id, role);
        let value = self
            .database
            .get_protocol_state(&key)
            .await
            .map_err(|error| DeviceJoinError::Store(error.into_message()))?;
        value
            .map(|value| serde_json::from_str(&value).map_err(DeviceJoinError::from))
            .transpose()
    }

    pub(crate) async fn advance(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        previous.validate_successor(&next)?;
        let key = previous.store_key();
        let previous = serde_json::to_string(previous)?;
        let next = serde_json::to_string(&next)?;
        let changed = self
            .database
            .connection
            .call(move |connection| {
                connection
                    .execute(
                        "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                        (&next, &key, &previous),
                    )
                    .map_err(crate::database::DbError::from)
            })
            .await
            .map_err(|error| DeviceJoinError::Store(error.into_message()))?;
        if changed == 1 {
            Ok(())
        } else {
            Err(DeviceJoinError::JournalConflict)
        }
    }

    pub(crate) async fn begin_replacement_terminal(
        &self,
        record: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        record.require_replacement_terminal()?;
        let key = record.store_key();
        let value = serde_json::to_string(&record)?;
        let durable = self
            .database
            .connection
            .call(move |connection| {
                connection
                    .execute(
                        "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                        (&key, &value),
                    )
                    .map_err(crate::database::DbError::from)?;
                crate::database::required_protocol_state_on(connection, &key)
            })
            .await
            .map_err(|error| DeviceJoinError::Store(error.into_message()))?;
        if durable != serde_json::to_string(&record)? {
            return Err(DeviceJoinError::JournalConflict);
        }
        Ok(())
    }

    async fn records(&self) -> Result<Vec<DeviceJoinJournalRecord>, DeviceJoinError> {
        let rows = self
            .database
            .connection
            .call(|connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT key, value FROM protocol_state
                         WHERE key GLOB 'device_join/*' ORDER BY key",
                    )
                    .map_err(crate::database::DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(crate::database::DbError::from)?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(crate::database::DbError::from)
            })
            .await
            .map_err(|error| DeviceJoinError::Store(error.into_message()))?;
        let mut records = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let record: DeviceJoinJournalRecord = serde_json::from_str(&value)?;
            if record.store_key() != key {
                return Err(DeviceJoinError::JournalConflict);
            }
            records.push(record);
        }
        records.sort_by_key(DeviceJoinJournalRecord::sort_key);
        Ok(records)
    }

    #[cfg(test)]
    pub(crate) async fn forget_for_test(
        &self,
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    ) -> Result<(), DeviceJoinError> {
        let key = DeviceJoinJournalRecord::store_key_for(attempt_id, role);
        self.database
            .connection
            .call(move |connection| {
                connection
                    .execute("DELETE FROM protocol_state WHERE key = ?1", [&key])
                    .map(|_| ())
                    .map_err(crate::database::DbError::from)
            })
            .await
            .map_err(|error| DeviceJoinError::Store(error.into_message()))
    }

    #[cfg(test)]
    pub(crate) async fn forget_provider_administrator_journals_for_test(
        &self,
    ) -> Result<(), DeviceJoinError> {
        self.database
            .connection
            .call(|connection| {
                connection
                    .execute(
                        "DELETE FROM protocol_state
                         WHERE key GLOB 'device_join/*/provider_administrator'",
                        [],
                    )
                    .map(|_| ())
                    .map_err(crate::database::DbError::from)
            })
            .await
            .map_err(|error| DeviceJoinError::Store(error.into_message()))
    }
}

impl StoreDatabase {
    pub(crate) async fn device_join_status(
        &self,
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    ) -> Result<Option<DeviceJoinStatus>, DeviceJoinError> {
        self.device_join_journal()
            .load(attempt_id, role)
            .await
            .map(|record| record.as_ref().map(DeviceJoinJournalRecord::status))
    }

    pub(crate) async fn device_join_actions(
        &self,
    ) -> Result<Vec<DeviceJoinAction>, DeviceJoinError> {
        Ok(self
            .device_join_journal()
            .records()
            .await?
            .iter()
            .filter_map(DeviceJoinJournalRecord::action)
            .collect())
    }

    pub(crate) async fn complete_device_join(
        &self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        current: &DeviceJoinJournalRecord,
        activated: &DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        if current.sort_key() != activated.sort_key() {
            return Err(DeviceJoinError::JournalConflict);
        }
        let pending_path = pending.path().to_string_lossy().into_owned();
        let pending_attempt = current.attempt_key();
        let expected_pending = serde_json::to_string(current)?;
        let store_key = activated.store_key();
        let store_payload = serde_json::to_string(activated)?;
        self.connection
            .call(move |connection| {
                connection
                    .execute("ATTACH DATABASE ?1 AS pending_join_source", [&pending_path])
                    .map_err(crate::database::DbError::from)?;
                let operation = (|| {
                    let transaction = connection
                        .unchecked_transaction()
                        .map_err(crate::database::DbError::from)?;
                    let actual: String = transaction
                        .query_row(
                            "SELECT payload FROM pending_join_source.device_join_journals
                             WHERE attempt_id = ?1 AND role = 'joiner'",
                            [&pending_attempt],
                            |row| row.get(0),
                        )
                        .map_err(crate::database::DbError::from)?;
                    if actual != expected_pending {
                        return Err(crate::database::DbError::Message(
                            "pending join journal changed before activation".to_string(),
                        ));
                    }
                    transaction
                        .execute(
                            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)
                             ON CONFLICT(key) DO UPDATE SET value = excluded.value
                             WHERE value = excluded.value",
                            (&store_key, &store_payload),
                        )
                        .map_err(crate::database::DbError::from)?;
                    transaction
                        .execute(
                            "DELETE FROM pending_join_source.device_join_journals
                             WHERE attempt_id = ?1 AND role = 'joiner' AND payload = ?2",
                            (&pending_attempt, &expected_pending),
                        )
                        .map_err(crate::database::DbError::from)?;
                    transaction.commit().map_err(crate::database::DbError::from)
                })();
                let detached = connection
                    .execute_batch("DETACH DATABASE pending_join_source")
                    .map_err(crate::database::DbError::from);
                match (operation, detached) {
                    (Ok(()), Ok(())) => Ok(()),
                    (Err(operation), Ok(())) => Err(operation),
                    (Ok(()), Err(detach)) => Err(detach),
                    (Err(operation), Err(detach)) => {
                        Err(crate::database::DbError::Message(format!(
                            "{operation}; detaching pending join journal also failed: {detach}"
                        )))
                    }
                }
            })
            .await
            .map_err(|error| DeviceJoinError::Store(error.into_message()))
    }
}
