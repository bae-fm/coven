use crate::database::Database;
use crate::sync::store::owner::device_join::{
    DeviceJoinError, DeviceJoinJournalRecord, DeviceJoinRole,
};
use crate::sync::store_commit::{DeviceJoinAttemptId, ObjectHash};

pub(crate) struct StoreJoinJournal<'database> {
    database: &'database Database,
}

impl<'database> StoreJoinJournal<'database> {
    pub(super) fn new(database: &'database Database) -> Self {
        Self { database }
    }

    pub(crate) fn new_attempt_id(&self) -> DeviceJoinAttemptId {
        DeviceJoinAttemptId::from_hash(ObjectHash::digest(
            self.database.new_write_id().as_str().as_bytes(),
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
}
