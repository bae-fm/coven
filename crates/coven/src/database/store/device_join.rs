use crate::database::StoreDatabase;
use crate::protocol::store_commit::{DeviceJoinAttemptId, ObjectHash};
use crate::sync::{
    DeviceJoinAction, DeviceJoinError, DeviceJoinJournalRecord, DeviceJoinRole, DeviceJoinStatus,
};

#[derive(Clone, Debug)]
pub(crate) struct DeviceJoinJournalStore {
    path: std::path::PathBuf,
}

impl DeviceJoinJournalStore {
    pub(crate) fn open(
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, crate::database::DbError> {
        let path = path.as_ref().to_path_buf();
        if let Some(directory) = path.parent() {
            std::fs::create_dir_all(directory)
                .map_err(|error| crate::database::DbError::Message(error.to_string()))?;
        }
        let connection =
            rusqlite::Connection::open(&path).map_err(crate::database::DbError::from)?;
        connection
            .execute_batch(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE IF NOT EXISTS device_join_journals (
                     attempt_id TEXT NOT NULL,
                     role TEXT NOT NULL,
                     payload TEXT NOT NULL,
                     PRIMARY KEY (attempt_id, role)
                 ) STRICT, WITHOUT ROWID;",
            )
            .map_err(crate::database::DbError::from)?;
        Ok(Self { path })
    }

    pub(crate) fn insert_or_load(
        &self,
        attempt_id: &str,
        role: &str,
        payload: &str,
    ) -> Result<String, crate::database::DbError> {
        let connection =
            rusqlite::Connection::open(&self.path).map_err(crate::database::DbError::from)?;
        let transaction = connection
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO device_join_journals (attempt_id, role, payload)
                 VALUES (?1, ?2, ?3)",
                (attempt_id, role, payload),
            )
            .map_err(crate::database::DbError::from)?;
        let actual = transaction
            .query_row(
                "SELECT payload FROM device_join_journals WHERE attempt_id = ?1 AND role = ?2",
                (attempt_id, role),
                |row| row.get(0),
            )
            .map_err(crate::database::DbError::from)?;
        transaction
            .commit()
            .map_err(crate::database::DbError::from)?;
        Ok(actual)
    }

    pub(crate) fn load(
        &self,
        attempt_id: &str,
        role: &str,
    ) -> Result<Option<String>, crate::database::DbError> {
        use rusqlite::OptionalExtension;

        let connection =
            rusqlite::Connection::open(&self.path).map_err(crate::database::DbError::from)?;
        connection
            .query_row(
                "SELECT payload FROM device_join_journals WHERE attempt_id = ?1 AND role = ?2",
                (attempt_id, role),
                |row| row.get(0),
            )
            .optional()
            .map_err(crate::database::DbError::from)
    }

    pub(crate) fn records(
        &self,
    ) -> Result<Vec<(String, String, String)>, crate::database::DbError> {
        let connection =
            rusqlite::Connection::open(&self.path).map_err(crate::database::DbError::from)?;
        let mut statement = connection
            .prepare(
                "SELECT attempt_id, role, payload FROM device_join_journals
                 ORDER BY attempt_id, role",
            )
            .map_err(crate::database::DbError::from)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(crate::database::DbError::from)?;
        rows.map(|row| row.map_err(crate::database::DbError::from))
            .collect()
    }

    pub(crate) fn compare_and_swap(
        &self,
        attempt_id: &str,
        role: &str,
        previous_payload: &str,
        next_payload: &str,
    ) -> Result<bool, crate::database::DbError> {
        let connection =
            rusqlite::Connection::open(&self.path).map_err(crate::database::DbError::from)?;
        let changed = connection
            .execute(
                "UPDATE device_join_journals SET payload = ?1
                 WHERE attempt_id = ?2 AND role = ?3 AND payload = ?4",
                (next_payload, attempt_id, role, previous_payload),
            )
            .map_err(crate::database::DbError::from)?;
        Ok(changed == 1)
    }

    pub(crate) async fn complete_into(
        &self,
        database: &StoreDatabase,
        current: &DeviceJoinJournalRecord,
        activated: &DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        if current.sort_key() != activated.sort_key() {
            return Err(DeviceJoinError::JournalConflict);
        }
        let pending_attempt = current.attempt_key();
        let expected_pending = serde_json::to_string(current)?;
        let store_key = activated.store_key();
        let store_payload = serde_json::to_string(activated)?;
        self.complete_payload_into(
            database,
            pending_attempt,
            expected_pending,
            store_key,
            store_payload,
        )
        .await
        .map_err(|error| DeviceJoinError::Store(error.into_message()))
    }

    async fn complete_payload_into(
        &self,
        database: &StoreDatabase,
        pending_attempt: String,
        expected_pending: String,
        store_key: String,
        store_payload: String,
    ) -> Result<(), crate::database::DbError> {
        let pending_path = self.path.to_string_lossy().into_owned();
        database
            .connection
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
                    let installed = transaction
                        .execute(
                            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)
                             ON CONFLICT(key) DO UPDATE SET value = excluded.value
                             WHERE value = excluded.value",
                            (&store_key, &store_payload),
                        )
                        .map_err(crate::database::DbError::from)?;
                    if installed != 1 {
                        return Err(crate::database::DbError::Message(
                            "activated join conflicts with the durable Store journal".to_string(),
                        ));
                    }
                    let removed = transaction
                        .execute(
                            "DELETE FROM pending_join_source.device_join_journals
                             WHERE attempt_id = ?1 AND role = 'joiner' AND payload = ?2",
                            (&pending_attempt, &expected_pending),
                        )
                        .map_err(crate::database::DbError::from)?;
                    if removed != 1 {
                        return Err(crate::database::DbError::Message(
                            "pending join journal changed during activation".to_string(),
                        ));
                    }
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
    }
}

impl StoreDatabase {
    pub(crate) fn new_device_join_attempt_id(&self) -> DeviceJoinAttemptId {
        DeviceJoinAttemptId::from_hash(ObjectHash::digest(
            self.new_store_write_id().as_str().as_bytes(),
        ))
    }

    pub(crate) async fn begin_device_join(
        &self,
        record: DeviceJoinJournalRecord,
    ) -> Result<DeviceJoinJournalRecord, DeviceJoinError> {
        record.require_initial()?;
        let key = record.store_key();
        let value = serde_json::to_string(&record)?;
        self.connection
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

    pub(crate) async fn load_device_join(
        &self,
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    ) -> Result<Option<DeviceJoinJournalRecord>, DeviceJoinError> {
        let key = DeviceJoinJournalRecord::store_key_for(attempt_id, role);
        let value = self
            .get_protocol_state(&key)
            .await
            .map_err(|error| DeviceJoinError::Store(error.into_message()))?;
        value
            .map(|value| serde_json::from_str(&value).map_err(DeviceJoinError::from))
            .transpose()
    }

    pub(crate) async fn advance_device_join(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        previous.validate_successor(&next)?;
        let key = previous.store_key();
        let previous = serde_json::to_string(previous)?;
        let next = serde_json::to_string(&next)?;
        let changed = self
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

    pub(crate) async fn begin_device_join_replacement_terminal(
        &self,
        record: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        record.require_replacement_terminal()?;
        let key = record.store_key();
        let value = serde_json::to_string(&record)?;
        let durable = self
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

    async fn device_join_records(&self) -> Result<Vec<DeviceJoinJournalRecord>, DeviceJoinError> {
        let rows = self
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
        self.connection
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
        self.connection
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
        self.load_device_join(attempt_id, role)
            .await
            .map(|record| record.as_ref().map(DeviceJoinJournalRecord::status))
    }

    pub(crate) async fn device_join_actions(
        &self,
    ) -> Result<Vec<DeviceJoinAction>, DeviceJoinError> {
        Ok(self
            .device_join_records()
            .await?
            .iter()
            .filter_map(DeviceJoinJournalRecord::action)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn conflicting_store_journal_keeps_the_pending_join() {
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending = DeviceJoinJournalStore::open(pending_dir.path().join("pending.sqlite"))
            .expect("open pending join journal");
        pending
            .insert_or_load("attempt", "joiner", "pending")
            .expect("insert pending join");
        let database = crate::sync::test_helpers::open_test_db();
        let store = StoreDatabase::new(&database);
        let store_key = "device_join/attempt/joiner";
        store
            .set_protocol_state(store_key, "conflicting")
            .await
            .expect("insert conflicting Store journal");

        let result = pending
            .complete_payload_into(
                &store,
                "attempt".to_string(),
                "pending".to_string(),
                store_key.to_string(),
                "activated".to_string(),
            )
            .await;

        assert!(result.is_err(), "a conflicting Store journal must fail");
        assert_eq!(
            pending
                .load("attempt", "joiner")
                .expect("load pending join"),
            Some("pending".to_string()),
        );
        assert_eq!(
            store
                .get_protocol_state(store_key)
                .await
                .expect("load Store journal"),
            Some("conflicting".to_string()),
        );
    }
}
