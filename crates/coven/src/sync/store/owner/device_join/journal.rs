use crate::protocol::store_commit::DeviceJoinAttemptId;

use super::DeviceJoinError;
use crate::database::StoreDatabase;
use crate::protocol::store_commit::device_join_exchange::{
    DeviceJoinActivation, DeviceJoinCleanupActivation, DeviceJoinReadiness,
};
use crate::protocol::store_commit::DeviceJoinAttemptRef;
use crate::protocol::store_commit::DeviceJoinOutcomeRef;

pub(crate) use crate::database::device_join_journal::validate_initial_progress;
use crate::database::device_join_journal::validate_successor;
pub(crate) use crate::protocol::store_commit::device_join_journal::attempt_key;
pub(crate) use crate::protocol::store_commit::device_join_journal::{
    device_join_action, DeviceJoinRoleProgress, DeviceJoinRoleProgressKind, JoinerJoinProgress,
    OwnerJoinProgress, PreparedDeviceJoinObject, ProviderAdminJoinProgress,
};
pub use crate::protocol::store_commit::device_join_journal::{
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

    /// Install `initial` as this role's first record, returning whatever the
    /// journal durably holds — an attempt that already advanced past the initial
    /// progress returns that later record for the caller to resume from.
    pub(super) async fn begin(
        &self,
        initial: &DeviceJoinJournalRecord,
    ) -> Result<DeviceJoinJournalRecord, DeviceJoinError> {
        Ok(self.database.begin_device_join(initial.clone()).await?)
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

    /// Install a terminal record for a role that never opened a journal, so a
    /// replacement of an attempt this device never ran still records how it ended.
    pub(super) async fn begin_replacement_terminal(
        &self,
        progress: Progress,
    ) -> Result<(), DeviceJoinError> {
        Ok(self
            .database
            .begin_device_join_replacement_terminal(self.record(progress))
            .await?)
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
    store: crate::database::DeviceJoinJournalStore,
}

impl DeviceJoinJournalDatabase {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, DeviceJoinError> {
        Ok(Self {
            store: crate::database::DeviceJoinJournalStore::open(path).map_err(database_error)?,
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
        attempt: &DeviceJoinAttemptRef,
    ) -> Result<Option<DeviceJoinReadiness>, DeviceJoinError> {
        let Some(record) = self.load(attempt.attempt_id, DeviceJoinRole::Joiner)? else {
            return Ok(None);
        };
        match &*record.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
                if &readiness.proof.attempt != attempt {
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
        let attempt_id = activation.outcome.attempt().attempt_id;
        if !matches!(activation.outcome, DeviceJoinOutcomeRef::Activated { .. }) {
            return Err(DeviceJoinError::AttemptMismatch);
        }
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

    pub(super) fn advance_joiner_cleanup_from_replacement(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        if previous.attempt_id != next.attempt_id
            || !matches!(
                &*previous.progress,
                DeviceJoinRoleProgress::Joiner(progress) if progress.holds_staged_work()
            )
            || !matches!(
                &*next.progress,
                DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(_))
            )
        {
            return Err(DeviceJoinError::JournalConflict);
        }
        self.swap(previous, &next)
    }

    pub fn complete_joiner_cleanup(
        &self,
        activation: DeviceJoinCleanupActivation,
    ) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
        let attempt_id = activation.receipt.attempt_id;
        let current = self
            .load(attempt_id, DeviceJoinRole::Joiner)?
            .ok_or(DeviceJoinError::JournalConflict)?;
        if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CancelledComplete(existing)) =
            &*current.progress
        {
            if existing == &activation {
                return Ok(existing.clone());
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(durable)) =
            &*current.progress
        else {
            return Err(DeviceJoinError::JournalConflict);
        };
        if durable != &activation {
            return Err(DeviceJoinError::JournalConflict);
        }
        self.advance(
            &current,
            DeviceJoinJournalRecord {
                attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::CancelledComplete(activation.clone()),
                )),
            },
        )?;
        Ok(activation)
    }
}

pub(super) fn database_error(error: crate::database::DbError) -> DeviceJoinError {
    DeviceJoinError::Database(error)
}

pub(super) fn provider_error(error: impl std::fmt::Display) -> DeviceJoinError {
    DeviceJoinError::Provider(error.to_string())
}
