use crate::query_mapped_rows;
use crate::store::device_join_journal::{
    require_initial, require_replacement_terminal, validate_successor, DeviceJoinJournalError,
};
use crate::StoreDatabase;
use coven_protocol::store_commit::device_join_journal::{
    DeviceJoinAction, DeviceJoinJournalRecord, DeviceJoinRole, DeviceJoinStatus,
};
use coven_protocol::store_commit::{DeviceJoinAttemptId, ObjectHash};

#[derive(Clone, Debug)]
pub struct DeviceJoinJournalStore {
    path: std::path::PathBuf,
    connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
}

impl DeviceJoinJournalStore {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, crate::DbError> {
        Self::open_with_durability(path, crate::connection_io::ConnectionDurability::Full)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn open_for_test(path: impl AsRef<std::path::Path>) -> Result<Self, crate::DbError> {
        Self::open_with_durability(path, crate::connection_io::ConnectionDurability::Disabled)
    }

    fn open_with_durability(
        path: impl AsRef<std::path::Path>,
        durability: crate::connection_io::ConnectionDurability,
    ) -> Result<Self, crate::DbError> {
        let path = path.as_ref().to_path_buf();
        if let Some(directory) = path.parent() {
            std::fs::create_dir_all(directory)
                .map_err(|error| crate::DbError::Message(error.to_string()))?;
        }
        let connection = rusqlite::Connection::open(&path).map_err(crate::DbError::from)?;
        crate::connection_io::configure_connection_durability(&connection, durability)?;
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
            .map_err(crate::DbError::from)?;
        Ok(Self {
            path,
            connection: std::sync::Arc::new(std::sync::Mutex::new(connection)),
        })
    }

    pub fn insert_or_load(
        &self,
        attempt_id: &str,
        role: &str,
        payload: &str,
    ) -> Result<String, crate::DbError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| pending_join_connection_poisoned())?;
        let transaction = connection
            .unchecked_transaction()
            .map_err(crate::DbError::from)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO device_join_journals (attempt_id, role, payload)
                 VALUES (?1, ?2, ?3)",
                (attempt_id, role, payload),
            )
            .map_err(crate::DbError::from)?;
        let actual = transaction
            .query_row(
                "SELECT payload FROM device_join_journals WHERE attempt_id = ?1 AND role = ?2",
                (attempt_id, role),
                |row| row.get(0),
            )
            .map_err(crate::DbError::from)?;
        transaction.commit().map_err(crate::DbError::from)?;
        Ok(actual)
    }

    pub fn load(&self, attempt_id: &str, role: &str) -> Result<Option<String>, crate::DbError> {
        use rusqlite::OptionalExtension;

        let connection = self
            .connection
            .lock()
            .map_err(|_| pending_join_connection_poisoned())?;
        connection
            .query_row(
                "SELECT payload FROM device_join_journals WHERE attempt_id = ?1 AND role = ?2",
                (attempt_id, role),
                |row| row.get(0),
            )
            .optional()
            .map_err(crate::DbError::from)
    }

    pub fn records(&self) -> Result<Vec<(String, String, String)>, crate::DbError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| pending_join_connection_poisoned())?;
        query_mapped_rows(
            &connection,
            "SELECT attempt_id, role, payload FROM device_join_journals
                 ORDER BY attempt_id, role",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .map_err(crate::DbError::from)
    }

    pub fn compare_and_swap(
        &self,
        attempt_id: &str,
        role: &str,
        previous_payload: &str,
        next_payload: &str,
    ) -> Result<bool, crate::DbError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| pending_join_connection_poisoned())?;
        let changed = connection
            .execute(
                "UPDATE device_join_journals SET payload = ?1
                 WHERE attempt_id = ?2 AND role = ?3 AND payload = ?4",
                (next_payload, attempt_id, role, previous_payload),
            )
            .map_err(crate::DbError::from)?;
        Ok(changed == 1)
    }

    pub async fn complete_into(
        &self,
        database: &StoreDatabase,
        current: &DeviceJoinJournalRecord,
        activated: &DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinJournalError> {
        if current.sort_key() != activated.sort_key() {
            return Err(DeviceJoinJournalError::JournalConflict);
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
        .map_err(DeviceJoinJournalError::Database)
    }

    async fn complete_payload_into(
        &self,
        database: &StoreDatabase,
        pending_attempt: String,
        expected_pending: String,
        store_key: String,
        store_payload: String,
    ) -> Result<(), crate::DbError> {
        let pending_path = self.path.to_string_lossy().into_owned();
        database
            .call_database(move |session| {
                session.complete_device_join_from_pending(
                    &pending_path,
                    &pending_attempt,
                    &expected_pending,
                    &store_key,
                    &store_payload,
                )
            })
            .await
    }

    #[cfg(test)]
    fn synchronous_for_test(&self) -> Result<i64, crate::DbError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| pending_join_connection_poisoned())?;
        connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .map_err(crate::DbError::from)
    }
}

fn pending_join_connection_poisoned() -> crate::DbError {
    crate::DbError::Message("pending device-join journal connection lock was poisoned".to_string())
}

pub(crate) fn complete_device_join_from_pending_on(
    conn: &rusqlite::Connection,
    durability: crate::connection_io::ConnectionDurability,
    pending_path: &str,
    pending_attempt: &str,
    expected_pending: &str,
    store_key: &str,
    store_payload: &str,
) -> Result<(), crate::DbError> {
    conn.execute("ATTACH DATABASE ?1 AS pending_join_source", [pending_path])
        .map_err(crate::DbError::from)?;
    let operation = (|| {
        crate::connection_io::configure_connection_schema_durability(
            conn,
            Some("pending_join_source"),
            durability,
        )?;
        let transaction = conn.unchecked_transaction().map_err(crate::DbError::from)?;
        let actual: String = transaction
            .query_row(
                "SELECT payload FROM pending_join_source.device_join_journals
                     WHERE attempt_id = ?1 AND role = 'joiner'",
                [pending_attempt],
                |row| row.get(0),
            )
            .map_err(crate::DbError::from)?;
        if actual != expected_pending {
            return Err(crate::DbError::Message(
                "pending join journal changed before activation".to_string(),
            ));
        }
        let installed = transaction
            .execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value
                     WHERE value = excluded.value",
                (store_key, store_payload),
            )
            .map_err(crate::DbError::from)?;
        if installed != 1 {
            return Err(crate::DbError::Message(
                "activated join conflicts with the durable Store journal".to_string(),
            ));
        }
        let removed = transaction
            .execute(
                "DELETE FROM pending_join_source.device_join_journals
                     WHERE attempt_id = ?1 AND role = 'joiner' AND payload = ?2",
                (pending_attempt, expected_pending),
            )
            .map_err(crate::DbError::from)?;
        if removed != 1 {
            return Err(crate::DbError::Message(
                "pending join journal changed during activation".to_string(),
            ));
        }
        transaction.commit().map_err(crate::DbError::from)
    })();
    let detached = conn
        .execute_batch("DETACH DATABASE pending_join_source")
        .map_err(crate::DbError::from);
    match (operation, detached) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(operation), Ok(())) => Err(operation),
        (Ok(()), Err(detach)) => Err(detach),
        (Err(operation), Err(detach)) => Err(crate::DbError::Message(format!(
            "{operation}; detaching pending join journal also failed: {detach}"
        ))),
    }
}

pub(crate) fn begin_device_join_on(
    conn: &rusqlite::Connection,
    key: &str,
    value: &str,
) -> Result<DeviceJoinJournalRecord, crate::DbError> {
    conn.execute(
        "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
        (key, value),
    )
    .map_err(crate::DbError::from)?;
    let actual = crate::required_protocol_state_on(conn, key)?;
    serde_json::from_str(&actual).map_err(|error| crate::DbError::Message(error.to_string()))
}

pub(crate) fn advance_device_join_on(
    conn: &rusqlite::Connection,
    key: &str,
    previous: &str,
    next: &str,
) -> Result<usize, crate::DbError> {
    conn.execute(
        "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
        (next, key, previous),
    )
    .map_err(crate::DbError::from)
}

pub(crate) fn begin_device_join_replacement_terminal_on(
    conn: &rusqlite::Connection,
    key: &str,
    value: &str,
) -> Result<String, crate::DbError> {
    conn.execute(
        "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
        (key, value),
    )
    .map_err(crate::DbError::from)?;
    crate::required_protocol_state_on(conn, key)
}

pub(crate) fn device_join_records_on(
    conn: &rusqlite::Connection,
) -> Result<Vec<(String, String)>, crate::DbError> {
    let mut statement = conn
        .prepare(
            "SELECT key, value FROM protocol_state
                 WHERE key GLOB 'device_join/*' ORDER BY key",
        )
        .map_err(crate::DbError::from)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(crate::DbError::from)?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(crate::DbError::from)
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn forget_device_join_on(
    conn: &rusqlite::Connection,
    key: &str,
) -> Result<(), crate::DbError> {
    conn.execute("DELETE FROM protocol_state WHERE key = ?1", [key])
        .map(|_| ())
        .map_err(crate::DbError::from)
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn forget_provider_administrator_device_joins_on(
    conn: &rusqlite::Connection,
) -> Result<(), crate::DbError> {
    conn.execute(
        "DELETE FROM protocol_state
                 WHERE key GLOB 'device_join/*/provider_administrator'",
        [],
    )
    .map(|_| ())
    .map_err(crate::DbError::from)
}

impl StoreDatabase {
    pub fn new_device_join_attempt_id(&self) -> DeviceJoinAttemptId {
        DeviceJoinAttemptId::from_hash(ObjectHash::digest(
            self.new_store_write_id().as_str().as_bytes(),
        ))
    }

    pub async fn begin_device_join(
        &self,
        record: DeviceJoinJournalRecord,
    ) -> Result<DeviceJoinJournalRecord, DeviceJoinJournalError> {
        require_initial(&record)?;
        let key = record.store_key();
        let value = serde_json::to_string(&record)?;
        self.call_database(move |session| session.begin_device_join(&key, &value))
            .await
            .map_err(DeviceJoinJournalError::Database)
    }

    pub async fn load_device_join(
        &self,
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    ) -> Result<Option<DeviceJoinJournalRecord>, DeviceJoinJournalError> {
        let key = DeviceJoinJournalRecord::store_key_for(attempt_id, role);
        let value = self
            .get_protocol_state(&key)
            .await
            .map_err(DeviceJoinJournalError::Database)?;
        value
            .map(|value| {
                serde_json::from_str(&value).map_err(DeviceJoinJournalError::Serialization)
            })
            .transpose()
    }

    pub async fn advance_device_join(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinJournalError> {
        validate_successor(previous, &next)?;
        let key = previous.store_key();
        let previous = serde_json::to_string(previous)?;
        let next = serde_json::to_string(&next)?;
        let changed = self
            .call_database(move |session| session.advance_device_join(&key, &previous, &next))
            .await
            .map_err(DeviceJoinJournalError::Database)?;
        if changed == 1 {
            Ok(())
        } else {
            Err(DeviceJoinJournalError::JournalConflict)
        }
    }

    pub async fn begin_device_join_replacement_terminal(
        &self,
        record: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinJournalError> {
        require_replacement_terminal(&record)?;
        let key = record.store_key();
        let value = serde_json::to_string(&record)?;
        let durable = self
            .call_database(move |session| {
                session.begin_device_join_replacement_terminal(&key, &value)
            })
            .await
            .map_err(DeviceJoinJournalError::Database)?;
        if durable != serde_json::to_string(&record)? {
            return Err(DeviceJoinJournalError::JournalConflict);
        }
        Ok(())
    }

    async fn device_join_records(
        &self,
    ) -> Result<Vec<DeviceJoinJournalRecord>, DeviceJoinJournalError> {
        let rows = self
            .call_database(|session| session.device_join_records())
            .await
            .map_err(DeviceJoinJournalError::Database)?;
        let mut records = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let record: DeviceJoinJournalRecord = serde_json::from_str(&value)?;
            if record.store_key() != key {
                return Err(DeviceJoinJournalError::JournalConflict);
            }
            records.push(record);
        }
        records.sort_by_key(DeviceJoinJournalRecord::sort_key);
        Ok(records)
    }

    pub async fn device_join_status(
        &self,
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    ) -> Result<Option<DeviceJoinStatus>, DeviceJoinJournalError> {
        self.load_device_join(attempt_id, role)
            .await
            .map(|record| record.as_ref().map(DeviceJoinJournalRecord::status))
    }

    pub async fn device_join_actions(
        &self,
    ) -> Result<Vec<DeviceJoinAction>, DeviceJoinJournalError> {
        Ok(self
            .device_join_records()
            .await?
            .iter()
            .filter_map(DeviceJoinJournalRecord::action)
            .collect())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn forget_for_test(
        &self,
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    ) -> Result<(), DeviceJoinJournalError> {
        let key = DeviceJoinJournalRecord::store_key_for(attempt_id, role);
        self.call_database(move |session| session.forget_device_join(&key))
            .await
            .map_err(DeviceJoinJournalError::Database)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn forget_provider_administrator_journals_for_test(
        &self,
    ) -> Result<(), DeviceJoinJournalError> {
        self.call_database(|session| session.forget_provider_administrator_device_joins())
            .await
            .map_err(DeviceJoinJournalError::Database)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_join_test_store_disables_commit_durability() {
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending =
            DeviceJoinJournalStore::open_for_test(pending_dir.path().join("pending.sqlite"))
                .expect("open pending join journal");

        let synchronous = pending
            .synchronous_for_test()
            .expect("read synchronous setting");

        assert_eq!(synchronous, 0);
    }

    #[tokio::test]
    async fn conflicting_store_journal_keeps_the_pending_join() {
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let pending =
            DeviceJoinJournalStore::open_for_test(pending_dir.path().join("pending.sqlite"))
                .expect("open pending join journal");
        pending
            .insert_or_load("attempt", "joiner", "pending")
            .expect("insert pending join");
        let database = crate::synthetic_store::open_test_db();
        let store = StoreDatabase::new(database.database());
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
