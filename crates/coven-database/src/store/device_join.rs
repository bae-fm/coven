use crate::query_mapped_rows;
use crate::store::device_join_journal::{
    require_initial, validate_successor, DeviceJoinJournalError,
};
use crate::StoreDatabase;
use coven_protocol::store_commit::device_join_journal::{
    DeviceJoinAction, DeviceJoinJournalRecord, DeviceJoinRole, DeviceJoinStatus,
};
use coven_protocol::store_commit::{DeviceJoinAttemptId, ObjectHash};

/// The joining device's own journal of in-flight join attempts, kept in its own
/// SQLite file under the stores root because the Store database it is joining
/// does not exist yet while the attempt runs.
///
/// The file lives exactly as long as the attempts in it. Retiring the last row
/// closes the connection and deletes the file with its WAL sidecars, so a
/// device that has joined a store does not leave a dead journal behind forever;
/// the next attempt reopens the path and gets a new file.
#[derive(Clone, Debug)]
pub struct DeviceJoinJournalStore {
    path: std::path::PathBuf,
    durability: crate::connection_io::ConnectionDurability,
    /// `None` between the delete of an emptied journal and the next operation
    /// that reopens it. The connection is closed before the file is unlinked so
    /// no later write can land in a file nothing can be read back from.
    connection: std::sync::Arc<std::sync::Mutex<Option<rusqlite::Connection>>>,
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
        let store = Self {
            path: path.as_ref().to_path_buf(),
            durability,
            connection: std::sync::Arc::new(std::sync::Mutex::new(None)),
        };
        // Open here rather than on the first operation: a journal file that
        // cannot be opened is a failure of whoever asked for the journal, and
        // they find out at the call that asked.
        store.with_connection(|_| Ok(()))?;
        Ok(store)
    }

    /// Put a live connection in `held`, creating the journal file and its one
    /// table. Called at construction and again by the first operation after a
    /// completed attempt deleted the file.
    fn open_into(&self, held: &mut Option<rusqlite::Connection>) -> Result<(), crate::DbError> {
        if let Some(directory) = self.path.parent() {
            std::fs::create_dir_all(directory).map_err(crate::DbError::from)?;
        }
        let opened = rusqlite::Connection::open(&self.path).map_err(crate::DbError::from)?;
        crate::connection_io::configure_connection_durability(&opened, self.durability)?;
        opened
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
        *held = Some(opened);
        Ok(())
    }

    /// Run `operation` on the journal's connection, opening the file first when
    /// a completed attempt deleted it.
    fn with_connection<R>(
        &self,
        operation: impl FnOnce(&rusqlite::Connection) -> Result<R, crate::DbError>,
    ) -> Result<R, crate::DbError> {
        let mut held = self
            .connection
            .lock()
            .map_err(|_| pending_join_connection_poisoned())?;
        if held.is_none() {
            self.open_into(&mut held)?;
        }
        let connection = held.as_ref().ok_or_else(|| {
            crate::DbError::Message(
                "pending device-join journal connection is absent after opening it".to_string(),
            )
        })?;
        operation(connection)
    }

    pub fn insert_or_load(
        &self,
        attempt_id: &str,
        role: &str,
        payload: &str,
    ) -> Result<String, crate::DbError> {
        self.with_connection(|connection| {
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
        })
    }

    pub fn load(&self, attempt_id: &str, role: &str) -> Result<Option<String>, crate::DbError> {
        use rusqlite::OptionalExtension;

        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT payload FROM device_join_journals WHERE attempt_id = ?1 AND role = ?2",
                    (attempt_id, role),
                    |row| row.get(0),
                )
                .optional()
                .map_err(crate::DbError::from)
        })
    }

    pub fn records(&self) -> Result<Vec<(String, String, String)>, crate::DbError> {
        self.with_connection(|connection| {
            query_mapped_rows(
                connection,
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
        })
    }

    pub fn compare_and_swap(
        &self,
        attempt_id: &str,
        role: &str,
        previous_payload: &str,
        next_payload: &str,
    ) -> Result<bool, crate::DbError> {
        self.with_connection(|connection| {
            let changed = connection
                .execute(
                    "UPDATE device_join_journals SET payload = ?1
                     WHERE attempt_id = ?2 AND role = ?3 AND payload = ?4",
                    (next_payload, attempt_id, role, previous_payload),
                )
                .map_err(crate::DbError::from)?;
            Ok(changed == 1)
        })
    }

    /// Drop one attempt's joiner row, but only while it still holds exactly the
    /// payload the caller last read, and delete the journal file when that row
    /// was the last one in it.
    ///
    /// This is how a joining device finishes: the row is its working notes on
    /// an exchange that is over, and what says the join happened is the
    /// library's own config file, written before this runs. An emptied journal
    /// answers nothing either, so the file goes with the row rather than
    /// accumulating one dead SQLite database per store the device ever joined.
    ///
    /// The row delete, the emptiness check, and the unlink all happen under the
    /// one connection lock, so a row begun by another attempt cannot land in a
    /// file that is about to be deleted.
    pub fn compare_and_forget(
        &self,
        attempt_id: &str,
        role: &str,
        expected_payload: &str,
    ) -> Result<bool, crate::DbError> {
        let mut held = self
            .connection
            .lock()
            .map_err(|_| pending_join_connection_poisoned())?;
        if held.is_none() {
            self.open_into(&mut held)?;
        }
        let connection = held.as_ref().ok_or_else(|| {
            crate::DbError::Message(
                "pending device-join journal connection is absent after opening it".to_string(),
            )
        })?;
        let removed = connection
            .execute(
                "DELETE FROM device_join_journals
                 WHERE attempt_id = ?1 AND role = ?2 AND payload = ?3",
                (attempt_id, role, expected_payload),
            )
            .map_err(crate::DbError::from)?;
        if removed != 1 {
            return Ok(false);
        }
        let remaining: i64 = connection
            .query_row("SELECT COUNT(*) FROM device_join_journals", [], |row| {
                row.get(0)
            })
            .map_err(crate::DbError::from)?;
        if remaining == 0 {
            if let Some(connection) = held.take() {
                connection.close().map_err(|(_, error)| error)?;
            }
            remove_pending_join_files(&self.path)?;
        }
        Ok(true)
    }

    #[cfg(test)]
    fn synchronous_for_test(&self) -> Result<i64, crate::DbError> {
        self.with_connection(|connection| {
            connection
                .query_row("PRAGMA synchronous", [], |row| row.get(0))
                .map_err(crate::DbError::from)
        })
    }
}

/// Delete an emptied journal and the WAL sidecars its durable mode writes beside
/// it. Closing the connection already checkpoints and removes those in the
/// ordinary case; naming them here is what covers a file left by an earlier
/// process that did not close.
fn remove_pending_join_files(path: &std::path::Path) -> Result<(), crate::DbError> {
    for candidate in [
        path.to_path_buf(),
        std::path::PathBuf::from(format!("{}-wal", path.display())),
        std::path::PathBuf::from(format!("{}-shm", path.display())),
    ] {
        match std::fs::remove_file(&candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(crate::DbError::from(error)),
        }
    }
    Ok(())
}

fn pending_join_connection_poisoned() -> crate::DbError {
    crate::DbError::Message("pending device-join journal connection lock was poisoned".to_string())
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
    serde_json::from_str(&actual).map_err(crate::DbError::from)
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

pub(crate) fn forget_device_join_on(
    conn: &rusqlite::Connection,
    key: &str,
) -> Result<(), crate::DbError> {
    conn.execute("DELETE FROM protocol_state WHERE key = ?1", [key])
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

    /// Every attempt's journal record, for the sweeps that look across attempts.
    ///
    /// A row this binary cannot read is reported and skipped rather than failing
    /// the sweep. The rows are one per attempt and role, and an attempt being
    /// driven reads its own row by key through
    /// [`load_device_join`](Self::load_device_join), which still refuses
    /// anything it cannot parse. Aborting here instead meant one abandoned
    /// attempt's record — left by an older binary, since the journal shape is
    /// not carried across changes — stopped every later pairing on the device,
    /// with nothing short of editing the database to recover.
    async fn device_join_records(
        &self,
    ) -> Result<Vec<DeviceJoinJournalRecord>, DeviceJoinJournalError> {
        let rows = self
            .call_database(|session| session.device_join_records())
            .await
            .map_err(DeviceJoinJournalError::Database)?;
        let mut records = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let record: DeviceJoinJournalRecord = match serde_json::from_str(&value) {
                Ok(record) => record,
                Err(error) => {
                    tracing::warn!(
                        journal_key = %key,
                        %error,
                        "Skipping a device join journal record this binary cannot read"
                    );
                    continue;
                }
            };
            if record.store_key() != key {
                tracing::warn!(
                    journal_key = %key,
                    record_key = %record.store_key(),
                    "Skipping a device join journal record stored under another attempt's key"
                );
                continue;
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

    /// Every owner journal row standing at a published activation, with the
    /// registration of the device it activated.
    ///
    /// The owner's half of a join ends here and the row is never advanced past
    /// it, so this is the whole set of attempts that could be finished.
    pub async fn owner_device_joins_awaiting_arrival(
        &self,
    ) -> Result<
        Vec<(
            DeviceJoinAttemptId,
            coven_protocol::store_commit::StoreDeviceRegistrationRef,
        )>,
        DeviceJoinJournalError,
    > {
        use coven_protocol::store_commit::device_join_journal::{
            DeviceJoinRoleProgress, OwnerJoinProgress,
        };

        Ok(self
            .device_join_records()
            .await?
            .into_iter()
            .filter_map(|record| match &*record.progress {
                // Both of the owner's ends: the cross-principal join hands the
                // activation over, and the same-principal join hands the whole
                // installation over. The second is the larger row by far — a
                // snapshot's metadata and the bootstrap closure ride inside it.
                DeviceJoinRoleProgress::Owner(
                    OwnerJoinProgress::ActivationPrepared { registration, .. }
                    | OwnerJoinProgress::SamePrincipalCompleted { registration, .. },
                ) => Some((record.attempt_id, registration.clone())),
                _ => None,
            })
            .collect())
    }

    /// Drop one attempt's journal row for one role.
    ///
    /// The row is this device's working notes on an exchange, not a record
    /// anything later reads: what the join durably produced is the activation
    /// commit and the outcome object it named, both of which live in history
    /// and are what every other device verifies the join against.
    pub async fn retire_device_join(
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

    #[test]
    fn retiring_the_last_attempt_deletes_the_journal_file() {
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let path = pending_dir.path().join("pending.sqlite");
        let pending = DeviceJoinJournalStore::open(&path).expect("open pending join journal");
        pending
            .insert_or_load("attempt-one", "joiner", "first")
            .expect("begin the first attempt");
        pending
            .insert_or_load("attempt-two", "joiner", "second")
            .expect("begin the second attempt");

        assert!(pending
            .compare_and_forget("attempt-one", "joiner", "first")
            .expect("retire the first attempt"));
        assert!(
            path.exists(),
            "a journal still holding an attempt keeps its file"
        );

        assert!(pending
            .compare_and_forget("attempt-two", "joiner", "second")
            .expect("retire the last attempt"));
        assert!(!path.exists(), "an emptied journal deletes its file");
        for sidecar in ["pending.sqlite-wal", "pending.sqlite-shm"] {
            assert!(
                !pending_dir.path().join(sidecar).exists(),
                "an emptied journal deletes its {sidecar} sidecar"
            );
        }
    }

    #[test]
    fn a_journal_used_after_its_file_was_deleted_writes_a_new_one() {
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let path = pending_dir.path().join("pending.sqlite");
        let pending = DeviceJoinJournalStore::open(&path).expect("open pending join journal");
        pending
            .insert_or_load("attempt-one", "joiner", "first")
            .expect("begin the first attempt");
        pending
            .compare_and_forget("attempt-one", "joiner", "first")
            .expect("retire the first attempt");

        pending
            .insert_or_load("attempt-two", "joiner", "second")
            .expect("a later attempt reopens the deleted journal");

        assert!(path.exists());
        assert_eq!(
            DeviceJoinJournalStore::open(&path)
                .expect("reopen the journal")
                .records()
                .expect("read the reopened journal"),
            vec![(
                "attempt-two".to_string(),
                "joiner".to_string(),
                "second".to_string()
            )],
            "the later attempt is durable in the new file, not an unlinked one"
        );
    }

    #[test]
    fn a_failed_retire_leaves_the_journal_file_alone() {
        let pending_dir = tempfile::tempdir().expect("create pending join directory");
        let path = pending_dir.path().join("pending.sqlite");
        let pending = DeviceJoinJournalStore::open(&path).expect("open pending join journal");
        pending
            .insert_or_load("attempt-one", "joiner", "first")
            .expect("begin the attempt");

        assert!(!pending
            .compare_and_forget("attempt-one", "joiner", "stale")
            .expect("refuse to retire a row whose payload moved on"));

        assert!(path.exists());
        assert_eq!(
            pending
                .load("attempt-one", "joiner")
                .expect("read the attempt"),
            Some("first".to_string())
        );
    }
}
