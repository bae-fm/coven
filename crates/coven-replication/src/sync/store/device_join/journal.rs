use coven_protocol::store_commit::DeviceJoinAttemptId;

use super::DeviceJoinError;
use coven_database::StoreDatabase;
use coven_protocol::store_commit::device_join_exchange::{
    DeviceJoinActivation, DeviceJoinReadiness,
};

pub(crate) use coven_database::device_join_journal::validate_initial_progress;
use coven_database::device_join_journal::validate_successor;
pub(crate) use coven_protocol::store_commit::device_join_journal::attempt_key;
pub(crate) use coven_protocol::store_commit::device_join_journal::{
    device_join_action, DeviceJoinRoleProgress, DeviceJoinRoleProgressKind, JoinerJoinProgress,
    OwnerJoinProgress, PreparedDeviceJoinObject,
};
pub use coven_protocol::store_commit::device_join_journal::{
    DeviceJoinAction, DeviceJoinJournalRecord, DeviceJoinRole, DeviceJoinStatus,
};

/// One attempt's role journal in the Store database. An operation reads and
/// advances a single attempt under a single role, so the handle carries both and
/// each call names only the progress it moves to.
pub(super) struct StoreJoinJournal<Progress> {
    database: StoreDatabase,
    attempt_id: DeviceJoinAttemptId,
    progress: std::marker::PhantomData<Progress>,
}

impl<Progress: DeviceJoinRoleProgressKind> StoreJoinJournal<Progress> {
    pub(super) fn new(database: &StoreDatabase, attempt_id: DeviceJoinAttemptId) -> Self {
        Self {
            database: database.clone(),
            attempt_id,
            progress: std::marker::PhantomData,
        }
    }

    /// The durable record for this attempt and role, or `None` when this role has
    /// never written one.
    pub(super) async fn load(&self) -> Result<Option<DeviceJoinJournalRecord>, DeviceJoinError> {
        Ok(self
            .database
            .load_device_join(self.attempt_id, Progress::ROLE)
            .await?)
    }

    /// The durable record for this attempt and role. A role holding no record has
    /// nothing to advance from, so its absence is a journal conflict.
    pub(super) async fn current(&self) -> Result<DeviceJoinJournalRecord, DeviceJoinError> {
        self.load().await?.ok_or(DeviceJoinError::JournalConflict)
    }

    pub(super) fn record(&self, progress: Progress) -> DeviceJoinJournalRecord {
        DeviceJoinJournalRecord {
            attempt_id: self.attempt_id,
            progress: Box::new(progress.into()),
        }
    }

    /// Advance from `previous`, returning the record now durable so the next step
    /// advances from it.
    pub(super) async fn advance(
        &self,
        previous: &DeviceJoinJournalRecord,
        progress: Progress,
    ) -> Result<DeviceJoinJournalRecord, DeviceJoinError> {
        let next = self.record(progress);
        self.advance_to(previous, &next).await?;
        Ok(next)
    }

    /// Advance from `previous` to a record the caller already built — the shape a
    /// caller needs when several durable predecessors advance to one successor.
    pub(super) async fn advance_to(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: &DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        Ok(self
            .database
            .advance_device_join(previous, next.clone())
            .await?)
    }
}

/// Durable role journal. Each row stores a closed progress value; SQLite's
/// compare-and-swap update rejects stale or skipped transitions.
#[derive(Clone, Debug)]
pub struct DeviceJoinJournalDatabase {
    store: coven_database::DeviceJoinJournalStore,
}

impl DeviceJoinJournalDatabase {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, DeviceJoinError> {
        Self::from_store(coven_database::DeviceJoinJournalStore::open(path))
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn open_for_test(path: impl AsRef<std::path::Path>) -> Result<Self, DeviceJoinError> {
        Self::from_store(coven_database::DeviceJoinJournalStore::open_for_test(path))
    }

    fn from_store(
        store: Result<coven_database::DeviceJoinJournalStore, coven_database::DbError>,
    ) -> Result<Self, DeviceJoinError> {
        Ok(Self {
            store: store.map_err(database_error)?,
        })
    }

    pub fn begin(
        &self,
        record: DeviceJoinJournalRecord,
    ) -> Result<DeviceJoinJournalRecord, DeviceJoinError> {
        validate_initial_progress(&record.progress)?;
        let attempt_id = attempt_key(record.attempt_id);
        let role = record.progress.role_name();
        let payload = serde_json::to_string(&record)?;
        let actual = self
            .store
            .insert_or_load(&attempt_id, role, &payload)
            .map_err(database_error)?;
        let actual = serde_json::from_str::<DeviceJoinJournalRecord>(&actual)?;
        if actual != record {
            return Err(DeviceJoinError::JournalConflict);
        }
        Ok(actual)
    }

    pub fn load(
        &self,
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    ) -> Result<Option<DeviceJoinJournalRecord>, DeviceJoinError> {
        let raw = self
            .store
            .load(&attempt_key(attempt_id), role.as_str())
            .map_err(database_error)?;
        let record = raw
            .map(|value| serde_json::from_str::<DeviceJoinJournalRecord>(&value))
            .transpose()?;
        if record
            .as_ref()
            .is_some_and(|record| record.attempt_id != attempt_id || record.progress.role() != role)
        {
            return Err(DeviceJoinError::JournalConflict);
        }
        Ok(record)
    }

    pub fn records(&self) -> Result<Vec<DeviceJoinJournalRecord>, DeviceJoinError> {
        let mut records = Vec::new();
        for (attempt_id, role, payload) in self.store.records().map_err(database_error)? {
            let record: DeviceJoinJournalRecord = serde_json::from_str(&payload)?;
            if attempt_key(record.attempt_id) != attempt_id || record.progress.role_name() != role {
                return Err(DeviceJoinError::JournalConflict);
            }
            records.push(record);
        }
        records.sort_by_key(|record| (record.attempt_id, record.progress.role()));
        Ok(records)
    }

    pub fn actions(&self) -> Result<Vec<DeviceJoinAction>, DeviceJoinError> {
        Ok(self
            .records()?
            .iter()
            .filter_map(device_join_action)
            .collect())
    }

    pub fn status(
        &self,
        attempt_id: DeviceJoinAttemptId,
    ) -> Result<Option<DeviceJoinStatus>, DeviceJoinError> {
        self.load(attempt_id, DeviceJoinRole::Joiner)
            .map(|record| record.as_ref().map(DeviceJoinJournalRecord::status))
    }

    pub fn completed_joiner_readiness(
        &self,
        attempt_id: DeviceJoinAttemptId,
    ) -> Result<Option<DeviceJoinReadiness>, DeviceJoinError> {
        let Some(record) = self.load(attempt_id, DeviceJoinRole::Joiner)? else {
            return Ok(None);
        };
        match &*record.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
                if readiness.proof.attempt_id != attempt_id {
                    return Err(DeviceJoinError::JournalConflict);
                }
                Ok(Some(readiness.clone()))
            }
            _ => Ok(None),
        }
    }

    pub fn observe_joiner_activation_if_pending(
        &self,
        activation: &DeviceJoinActivation,
    ) -> Result<Option<DeviceJoinReadiness>, DeviceJoinError> {
        let attempt_id = activation.attempt_id;
        let Some(current) = self.load(attempt_id, DeviceJoinRole::Joiner)? else {
            return Ok(None);
        };
        match &*current.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
                let observed = DeviceJoinJournalRecord {
                    attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Joiner(
                        JoinerJoinProgress::ActivationObserved {
                            readiness: readiness.clone(),
                            activation: activation.clone(),
                        },
                    )),
                };
                self.advance(&current, observed)?;
                Ok(Some(readiness.clone()))
            }
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ActivationObserved {
                readiness,
                activation: existing,
            }) if existing == activation => Ok(Some(readiness.clone())),
            _ => Err(DeviceJoinError::JournalConflict),
        }
    }

    pub fn advance(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        validate_successor(previous, &next)?;
        self.swap(previous, &next)
    }

    /// Replace `previous` with `next` only if the stored row still holds
    /// `previous` exactly.
    fn swap(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: &DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        let previous_payload = serde_json::to_string(previous)?;
        let next_payload = serde_json::to_string(next)?;
        if !self
            .store
            .compare_and_swap(
                &attempt_key(previous.attempt_id),
                previous.progress.role_name(),
                &previous_payload,
                &next_payload,
            )
            .map_err(database_error)?
        {
            return Err(DeviceJoinError::JournalConflict);
        }
        Ok(())
    }

    pub(super) async fn complete_into(
        &self,
        database: &StoreDatabase,
        current: &DeviceJoinJournalRecord,
        activated: &DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        self.store
            .complete_into(database, current, activated)
            .await
            .map_err(DeviceJoinError::from)
    }
}

pub(super) fn database_error(error: coven_database::DbError) -> DeviceJoinError {
    DeviceJoinError::Database(error)
}
