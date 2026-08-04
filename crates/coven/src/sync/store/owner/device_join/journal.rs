use crate::protocol::store_commit::DeviceJoinAttemptId;

use super::DeviceJoinError;
use crate::database::StoreDatabase;
use crate::protocol::store_commit::device_join_exchange::{
    DeviceJoinActivation, DeviceJoinCleanupActivation, DeviceJoinReadiness,
};
use crate::protocol::store_commit::DeviceJoinAttemptRef;
use crate::protocol::store_commit::DeviceJoinOutcomeRef;

pub(crate) use crate::protocol::store_commit::device_join_journal::attempt_key;
pub(crate) use crate::protocol::store_commit::device_join_journal::{
    device_join_action, validate_initial_progress, DeviceJoinRoleProgress, JoinerJoinProgress,
    OwnerJoinProgress, PreparedDeviceJoinObject, ProviderAdminJoinProgress,
};
pub use crate::protocol::store_commit::device_join_journal::{
    DeviceJoinAction, DeviceJoinCleanupProgress, DeviceJoinJournalRecord, DeviceJoinRole,
    DeviceJoinStatus,
};

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
        previous.validate_successor(&next)?;
        let previous_payload = serde_json::to_string(previous)?;
        let next_payload = serde_json::to_string(&next)?;
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
                DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::RegistrationPrepared(_)
                        | JoinerJoinProgress::ProviderReady(_)
                        | JoinerJoinProgress::RegistrationCreateIntent(_)
                        | JoinerJoinProgress::RegistrationCreated(_)
                        | JoinerJoinProgress::AckCreateIntent(_)
                        | JoinerJoinProgress::AckCreated(_)
                        | JoinerJoinProgress::ResponseCreateIntent(_)
                        | JoinerJoinProgress::Ready(_)
                        | JoinerJoinProgress::CleanupIntent { .. }
                )
            )
            || !matches!(
                &*next.progress,
                DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(_))
            )
        {
            return Err(DeviceJoinError::JournalConflict);
        }
        let previous_payload = serde_json::to_string(previous)?;
        let next_payload = serde_json::to_string(&next)?;
        if !self
            .store
            .compare_and_swap(
                &attempt_key(previous.attempt_id),
                DeviceJoinRole::Joiner.as_str(),
                &previous_payload,
                &next_payload,
            )
            .map_err(database_error)?
        {
            return Err(DeviceJoinError::JournalConflict);
        }
        Ok(())
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
    DeviceJoinError::Store(error.into_message())
}

pub(super) fn provider_error(error: impl std::fmt::Display) -> DeviceJoinError {
    DeviceJoinError::Provider(error.to_string())
}
