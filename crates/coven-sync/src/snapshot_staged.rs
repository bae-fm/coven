//! Telling damage to a snapshot's staged records on this device from the
//! snapshot's own (§19.1).

use crate::{sequence_sync::SequenceSync, write_object::database_failure, SyncError};
use coven_crypto::StoreKeyring;
use coven_database::{DbError, SnapshotStaging, StagedDigest};
use coven_format::sequence::SnapshotAnnouncement;

impl SequenceSync {
    /// Judge `failed`, a failure of the snapshot `announcement` names met by
    /// a check of its records as one staging held them, `staged`. Staging is
    /// a function of the snapshot's bytes, so a refusal stands only when a
    /// second staging, from another read of the snapshot, holds the same
    /// records: different records mean this device's disk changed the first,
    /// and the failure is the local database's (§19.1). A failure that
    /// carries no digest had no staging to read back: a bug.
    pub(crate) async fn judge_staged(
        &self,
        failed: SyncError,
        staged: Option<StagedDigest>,
        announcement: &SnapshotAnnouncement,
        ring: &StoreKeyring,
    ) -> SyncError {
        if !matches!(failed, SyncError::Damaged(_)) {
            return failed;
        }
        let Some(staged) = staged else {
            return DbError::Invariant { what: "a staged body's failure carries its digest" }
                .into();
        };
        match self.stage_again(announcement, ring).await {
            Ok(again) if again == staged => failed,
            Ok(_) => {
                tracing::warn!(
                    error = %failed,
                    "a snapshot's staged records changed on this device"
                );
                DbError::DamagedDatabase.into()
            }
            Err(error) => error,
        }
    }

    /// End `staging`, which failed with `failed` before a load or file check
    /// took it: a refusal reads back its digest first, its records are
    /// removed (§16.2), and the refusal is judged as `judge_staged` does.
    pub(crate) async fn end_staged(
        &self,
        failed: SyncError,
        staging: SnapshotStaging,
        announcement: &SnapshotAnnouncement,
        ring: &StoreKeyring,
    ) -> SyncError {
        let staged = match failed {
            SyncError::Damaged(_) => Some(staging.digest().await),
            _ => None,
        };
        let discarded = staging.discard().await;
        let judged = match staged {
            None => failed,
            Some(Ok(staged)) => self.judge_staged(failed, Some(staged), announcement, ring).await,
            Some(Err(error)) => {
                tracing::warn!(
                    error = %failed,
                    "a snapshot's staged records couldn't be read back"
                );
                error.into()
            }
        };
        match discarded {
            Ok(()) => judged,
            Err(cleanup) => SyncError::cleanup(judged, cleanup.into()),
        }
    }

    /// Read the snapshot `announcement` names again into a new staging, and
    /// digest what it holds where staging ended: the stream's end, or a
    /// refusal, as the first staging's did.
    async fn stage_again(
        &self,
        announcement: &SnapshotAnnouncement,
        ring: &StoreKeyring,
    ) -> Result<StagedDigest, SyncError> {
        let object = self.snapshot_prefix(announcement).await?;
        let path = object.path().clone();
        let mut staging = self
            .database
            .stage_snapshot(announcement.snapshot.clone(), object.prefix().clone())
            .await?;
        let staged = async {
            object
                .read_body(self.storage.as_ref(), ring, announcement, Some(&mut staging), None)
                .await?;
            staging.finish().await.map_err(|error| database_failure(&path, error))
        }
        .await;
        let digest = match staged {
            Ok(()) | Err(SyncError::Damaged(_)) => staging.digest().await.map_err(SyncError::from),
            Err(error) => Err(error),
        };
        let digest = match staging.discard().await {
            Ok(()) => digest,
            Err(cleanup) => Err(match digest {
                Ok(_) => cleanup.into(),
                Err(error) => SyncError::cleanup(error, cleanup.into()),
            }),
        };
        match (digest, object.release().await) {
            (digest, Ok(())) => digest,
            (Ok(_), Err(cleanup)) => Err(cleanup),
            (Err(error), Err(cleanup)) => Err(SyncError::cleanup(error, cleanup)),
        }
    }
}

#[cfg(test)]
#[path = "snapshot_staged_tests.rs"]
mod tests;
