//! The journal of a new Dropbox shared folder (§11.6, §20.3): one row for
//! each folder name an account asks for, written before the share request,
//! so that a call after a lost reply, or the next open, knows what it asked
//! for. A storage setup that commits ends every row.

use crate::SyncError;
use coven_database::{
    operation_kind::DROPBOX_FOLDER, NewOperation, OperationRecord, StoreDatabase,
};
use serde::{Deserialize, Serialize};

/// How far making one shared folder has come.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DropboxFolderStep {
    /// The share request may be sent, and its reply is unknown.
    Asked,
    /// Dropbox runs the share as the job with this id.
    Job {
        /// The id Dropbox gave the job.
        job: String,
    },
    /// The folder is made and shared.
    Made {
        /// The shared folder's id.
        shared_folder_id: String,
        /// Its name as Dropbox gave it.
        folder_name: String,
    },
}

/// What a row records.
#[derive(Serialize, Deserialize)]
struct FolderWork {
    /// The Dropbox id of the account that asked.
    account: String,
    /// The name it asked for at the top of its Dropbox.
    name: String,
    step: DropboxFolderStep,
}

/// One folder's journal row, as its owner last committed it.
pub struct DropboxFolderJournal {
    record: OperationRecord,
    work: FolderWork,
}

impl DropboxFolderJournal {
    /// Journal that `account` asks for a folder named `name`, before its
    /// share request.
    pub async fn start(
        database: &StoreDatabase,
        account: &str,
        name: &str,
    ) -> Result<Self, SyncError> {
        let work = FolderWork {
            account: account.into(),
            name: name.into(),
            step: DropboxFolderStep::Asked,
        };
        let data = serde_json::to_vec(&work)?;
        let id = database
            .start_operation(NewOperation {
                kind: DROPBOX_FOLDER.into(),
                data: data.clone(),
                started_by: DROPBOX_FOLDER.into(),
                pauses_writes: false,
            })
            .await?;
        let record = OperationRecord {
            id,
            kind: DROPBOX_FOLDER.into(),
            last_step: 0,
            data,
            started_by: DROPBOX_FOLDER.into(),
        };
        Ok(Self { record, work })
    }

    /// Every folder's row, oldest first.
    pub async fn all(database: &StoreDatabase) -> Result<Vec<Self>, SyncError> {
        database.operations_of_kind(DROPBOX_FOLDER).await?.into_iter().map(Self::read).collect()
    }

    /// The row of the folder `account` asked for as `name`. Dropbox matches
    /// names without regard to case, so this does too.
    pub async fn find(
        database: &StoreDatabase,
        account: &str,
        name: &str,
    ) -> Result<Option<Self>, SyncError> {
        let name = name.to_lowercase();
        Ok(Self::all(database)
            .await?
            .into_iter()
            .find(|row| row.work.account == account && row.work.name.to_lowercase() == name))
    }

    fn read(record: OperationRecord) -> Result<Self, SyncError> {
        let work = serde_json::from_slice(&record.data)?;
        Ok(Self { record, work })
    }

    /// The Dropbox id of the account that asked.
    pub fn account(&self) -> &str {
        &self.work.account
    }

    /// The name it asked for.
    pub fn name(&self) -> &str {
        &self.work.name
    }

    /// How far making the folder has come.
    pub fn step(&self) -> &DropboxFolderStep {
        &self.work.step
    }

    /// Commit that making the folder has come to `step`.
    pub async fn advance(
        &mut self,
        database: &StoreDatabase,
        step: DropboxFolderStep,
    ) -> Result<(), SyncError> {
        let work =
            FolderWork { account: self.work.account.clone(), name: self.work.name.clone(), step };
        let update = self.record.next_step(serde_json::to_vec(&work)?)?;
        let (last_step, data) = (update.last_step, update.data.clone());
        database.advance_operation(update).await?;
        (self.record.last_step, self.record.data) = (last_step, data);
        self.work = work;
        Ok(())
    }

    /// End the row.
    pub async fn end(&self, database: &StoreDatabase) -> Result<(), SyncError> {
        Ok(database.finish_operation(self.record.id).await?)
    }
}
