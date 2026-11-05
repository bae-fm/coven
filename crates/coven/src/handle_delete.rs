//! Deleting a store from this device without losing unsent work (E1).

use crate::*;
use coven_foundation::files::StoreDirAccess;

pub use coven_foundation::files::IfUnsent;

impl CovenHandle {
    /// The writes and files this device committed and has not uploaded, from
    /// one committed snapshot: what deleting the store would lose. With storage
    /// set up, `delete_store(IfUnsent::Refuse)` refuses with these counts.
    /// Available while disconnected.
    pub async fn unsent_work(&self) -> CovenResult<UnsentWork> {
        self.database.unsent().await.map_err(CovenError::from)
    }

    /// The same pre-built query, live (E4).
    pub fn subscribe_unsent_work(&self) -> LiveQuery<UnsentWork, CovenError> {
        self.database.subscribe_unsent()
    }

    /// Deletes this store from this device. With storage set up, a location
    /// recorded without credentials is refused with `SignInRequired`; then
    /// unsent writes and files are refused unless `unsent` discards them;
    /// both change nothing. The deletion then records itself in the store's
    /// directory and publishes this device's removal (§14.1), waiting until
    /// it applies. A failure before the removal is journaled removes the
    /// record and is `Sync`, the store as it was. From the journal on the
    /// deletion only goes forward: a failure closes the handle and is
    /// `Unfinished`, and the store lists as being deleted, refuses opens and
    /// is finished by `CovenBuilder::resume_deletion` (§11.6). Once the
    /// removal applied, or with no storage set up, it closes this handle, a
    /// failure there being `Closing`, and deletes as `Coven::discard_store`.
    /// The call runs to its end even if the app drops its future. Only this
    /// device leaves: the person's restore code and S3 key in iCloud
    /// Keychain stay for their other devices (§13.1).
    pub async fn delete_store(&self, unsent: IfUnsent) -> Result<(), StoreDeletionError> {
        self.leave(unsent).await
    }

    /// Delete the store from this device and leave it, as `delete_store`
    /// does, or finish a deletion recorded with `unsent`, in a task of its
    /// own that runs to its end even if the caller stops waiting.
    pub(crate) async fn leave(&self, unsent: IfUnsent) -> Result<(), StoreDeletionError> {
        let handle = self.clone();
        let bugs = self.database.bugs().clone();
        crate::task::spawn_completing("delete the store", bugs, async move {
            handle.leaving(unsent).await
        })
        .await?
    }

    async fn leaving(&self, unsent: IfUnsent) -> Result<(), StoreDeletionError> {
        if let Some(location) = self.codes.location().await? {
            let stopped = self.database.device_stop().await.map_err(SyncError::from)?.is_some();
            let journaled =
                stopped || coven_sync::removal_journaled(&self.database, self.device).await?;
            if self.codes.connection().await?.is_none() && !stopped {
                if !journaled {
                    self.forget_deletion().await?;
                }
                return Err(StoreDeletionError::SignInRequired(location.provider()));
            }
            if !journaled {
                let work = self.database.unsent().await.map_err(SyncError::from)?;
                if unsent == IfUnsent::Refuse && !work.is_empty() {
                    self.forget_deletion().await?;
                    return Err(StoreDeletionError::Unsent(work));
                }
                let directory = self.directory.clone();
                crate::task::blocking("record the deletion", move || {
                    directory.record_deletion(unsent)
                })
                .await??;
            }
            // A device stopped for good has nothing left to remove.
            if !stopped {
                let retired = async {
                    self.storage.connected().await?;
                    self.storage.retire().await
                }
                .await;
                if let Err(error) = retired {
                    return Err(self.retiring_failed(error).await);
                }
            }
        }
        self.close().await.map_err(StoreDeletionError::Closing)?;
        let directory = self.directory.clone();
        let keychain = self.keychain.clone();
        crate::task::blocking("delete the store", move || {
            crate::coven::delete::<StoreDeletionError>(&directory, &keychain)
        })
        .await?
    }

    /// The deletion failed connecting or publishing the removal. Before the
    /// removal is journaled nothing in storage changed, so the record goes
    /// and the store is as it was. From the journal on it only goes forward:
    /// the handle closes, and the record stays for `resume_deletion`.
    async fn retiring_failed(&self, error: SyncError) -> StoreDeletionError {
        let journaled = async {
            Ok::<_, SyncError>(
                self.database.device_stop().await?.is_some()
                    || coven_sync::removal_journaled(&self.database, self.device).await?,
            )
        }
        .await;
        match journaled {
            Ok(false) => match self.forget_deletion().await {
                Ok(()) => StoreDeletionError::from(error),
                Err(cleanup) => StoreDeletionError::from(SyncError::cleanup(error, cleanup)),
            },
            Ok(true) => StoreDeletionError::Unfinished(match self.close().await {
                Ok(()) => error,
                Err(cleanup) => SyncError::cleanup(error, cleanup),
            }),
            // Whether the removal was journaled can't be read: the record
            // stays, and the store lists as being deleted.
            Err(read) => StoreDeletionError::Unfinished(SyncError::cleanup(error, read)),
        }
    }

    /// Remove the deletion's record: it ended before its removal was
    /// journaled. An absent record succeeds.
    async fn forget_deletion(&self) -> Result<(), SyncError> {
        let directory = self.directory.clone();
        crate::task::blocking("forget the deletion", move || directory.forget_deletion())
            .await
            .map_err(SyncError::from)?
            .map_err(SyncError::from)
    }
}

#[cfg(test)]
#[path = "handle_delete_tests.rs"]
mod tests;
