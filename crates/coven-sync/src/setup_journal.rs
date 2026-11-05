//! The journal row of a storage setup (§11.6, §20.8): written before its
//! first write to storage, moved past its point of no return before it
//! claims anything, and ended with its outcome, which `coven_operations`
//! lists until the app acknowledges it.

use crate::{OperationKind, SyncError};
use coven_database::{
    operation_kind::STORAGE_SETUP, Database, DbError, NewOperation, OperationId, OperationOutcome,
    OperationRecord,
};
use coven_storage::StorageConfig;
use serde::{Deserialize, Serialize};

/// What a setup's row records.
#[derive(Serialize, Deserialize)]
struct SetupWork {
    /// Creating the store: no location was recorded when setup began.
    creating: bool,
    /// The name this device registers with, when setup creates the store.
    device_name: String,
    step: SetupStep,
}

#[derive(Serialize, Deserialize)]
enum SetupStep {
    /// Checking the provider, which claims nothing: a crash here is undone.
    Checking,
    /// Past the point of no return, claiming at `location` with the
    /// credentials setup keeps beside the committed ones: a crash here is
    /// finished.
    Claiming { location: StorageConfig },
}

/// One storage setup's journal row, as its owner last committed it.
pub struct SetupJournal {
    id: OperationId,
    last_step: u32,
    work: SetupWork,
}

impl SetupJournal {
    /// Journal a setup before its first write to storage, listed in
    /// `coven_operations` as `StorageSetup` from this commit on.
    pub async fn start(
        database: &Database,
        creating: bool,
        device_name: &str,
    ) -> Result<Self, SyncError> {
        let work =
            SetupWork { creating, device_name: device_name.into(), step: SetupStep::Checking };
        let id = database
            .start_listed_operation(
                NewOperation {
                    kind: STORAGE_SETUP.into(),
                    data: serde_json::to_vec(&work)?,
                    started_by: STORAGE_SETUP.into(),
                    pauses_writes: false,
                },
                OperationKind::StorageSetup.listed(),
            )
            .await?;
        Ok(Self { id, last_step: 0, work })
    }

    /// The setup a crash left running, if any.
    pub async fn running(database: &Database) -> Result<Option<Self>, SyncError> {
        database.first_operation(STORAGE_SETUP).await?.map(Self::read).transpose()
    }

    fn read(record: OperationRecord) -> Result<Self, SyncError> {
        Ok(Self {
            id: record.id,
            last_step: record.last_step,
            work: serde_json::from_slice(&record.data)?,
        })
    }

    /// The row's id, as `coven_operations` lists it.
    pub fn id(&self) -> OperationId {
        self.id
    }

    /// Whether setup creates the store, rather than reconnecting to it.
    pub fn creating(&self) -> bool {
        self.work.creating
    }

    /// The name this device registers with.
    pub fn device_name(&self) -> &str {
        &self.work.device_name
    }

    /// Where setup claims, once past its point of no return.
    pub fn location(&self) -> Option<&StorageConfig> {
        match &self.work.step {
            SetupStep::Checking => None,
            SetupStep::Claiming { location } => Some(location),
        }
    }

    /// Commit setup past its point of no return, claiming at `location`:
    /// from here on it finishes (§11.6).
    pub async fn claim(
        &mut self,
        database: &Database,
        location: StorageConfig,
    ) -> Result<(), SyncError> {
        let step = std::mem::replace(&mut self.work.step, SetupStep::Claiming { location });
        let advanced = async {
            let last_step = self
                .last_step
                .checked_add(1)
                .ok_or(DbError::Invariant { what: "an operation's steps fit u32" })?;
            database
                .advance_operation(coven_database::OperationUpdate {
                    id: self.id,
                    previous: self.last_step,
                    last_step,
                    data: serde_json::to_vec(&self.work)?,
                })
                .await?;
            Ok::<_, SyncError>(last_step)
        }
        .await;
        match advanced {
            Ok(last_step) => {
                self.last_step = last_step;
                Ok(())
            }
            Err(error) => {
                self.work.step = step;
                Err(error)
            }
        }
    }

    /// End the row with `outcome`, which `coven_operations` keeps until the
    /// app acknowledges it (§11.4).
    pub async fn end(
        &self,
        database: &Database,
        outcome: OperationOutcome,
    ) -> Result<(), SyncError> {
        Ok(database.finish_operation_with_outcome(self.id, outcome).await?)
    }
}
