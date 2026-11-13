//! Reading one announced snapshot: its body streamed once, before it
//! authenticates, and staged for the one load that installs it (§16.2).

use crate::{
    floor_object::FloorObject,
    history::History,
    sequence_sync::SequenceSync,
    snapshot_object::{BodyFailure, SnapshotBody, SnapshotObject},
    write_object::database_failure,
    SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{LoadStep, SnapshotSource, SnapshotStaging};
use coven_format::sequence::SnapshotAnnouncement;
use coven_foundation::progress::ProgressReport;

/// A snapshot whose body streamed once, before it authenticates.
pub(crate) struct ReadSnapshot {
    announcement: SnapshotAnnouncement,
    object: SnapshotObject,
    body: SnapshotBody,
}

impl ReadSnapshot {
    pub(crate) fn announcement(&self) -> &SnapshotAnnouncement {
        &self.announcement
    }
    /// Its header, original slots and claim records, tentative until it
    /// authenticates.
    pub(crate) fn body(&self) -> &SnapshotBody {
        &self.body
    }
    /// Check the body against the announced digest, then its signature and prefix.
    pub(crate) fn authenticate(
        &self,
        history: &History,
    ) -> Result<coven_foundation::id_source::MemberId, SyncError> {
        self.object.authenticate(
            history,
            &self.announcement.snapshot,
            &self.body.digest,
            &self.announcement.digest,
        )
    }
}

/// A selected snapshot read once, with its records staged for the one load
/// that installs it after it authenticates (§16.2). It holds the staging
/// until that load takes it, or until the reload ends without loading and
/// discards it.
pub(crate) struct SelectedSnapshot {
    read: ReadSnapshot,
    staging: SnapshotStaging,
}

impl SelectedSnapshot {
    pub(crate) fn announcement(&self) -> &SnapshotAnnouncement {
        self.read.announcement()
    }
    pub(crate) fn body(&self) -> &SnapshotBody {
        self.read.body()
    }
    pub(crate) fn authenticate(
        &self,
        history: &History,
    ) -> Result<coven_foundation::id_source::MemberId, SyncError> {
        self.read.authenticate(history)
    }
    /// The snapshot object's path, where a failure of its body is judged.
    pub(crate) fn path(&self) -> &coven_storage::ObjectPath {
        self.read.object.path()
    }
    /// What the load reads: the snapshot's identity, its staging and the
    /// member `author` whose key signed it.
    pub(crate) fn into_source(
        self,
        author: coven_foundation::id_source::MemberId,
    ) -> SnapshotSource<SnapshotStaging> {
        SnapshotSource::Stored { id: self.read.announcement.snapshot, input: self.staging, author }
    }
    /// Remove its staged records.
    pub(crate) async fn discard(self) -> Result<(), SyncError> {
        Ok(self.staging.discard().await?)
    }
}

impl SequenceSync {
    /// Stream the body once, staging its plaintext into `staging` if given,
    /// and counting its bytes toward the step `progress` is on. Only the
    /// prefix it read is kept, released for eviction.
    pub(super) async fn read_snapshot(
        &self,
        announcement: SnapshotAnnouncement,
        object: SnapshotObject,
        ring: &StoreKeyring,
        staging: Option<&mut SnapshotStaging>,
        progress: Option<&mut ProgressReport<LoadStep>>,
    ) -> Result<ReadSnapshot, BodyFailure> {
        let started = self.clock.elapsed();
        let body = object
            .read_body_confirmed(self.storage.as_ref(), ring, &announcement, staging, progress)
            .await;
        let body = match (body, object.release().await) {
            (Ok(body), Ok(())) => body,
            (Ok(_), Err(cleanup)) => return Err(cleanup.into()),
            (Err(failure), Ok(())) => return Err(failure),
            (Err(failure), Err(cleanup)) => {
                return Err(BodyFailure {
                    error: SyncError::cleanup(failure.error, cleanup),
                    confirmed: failure.confirmed,
                })
            }
        };
        tracing::info!(
            audience = ?announcement.snapshot.audience,
            covers = announcement.covers,
            bytes = object.size(),
            time = ?(self.clock.elapsed() - started),
            "streamed snapshot"
        );
        Ok(ReadSnapshot { announcement, object, body })
    }

    /// Read a selected snapshot once and stage its records for loading,
    /// reporting it to `progress` as a Streaming step for its audience. A
    /// failed read removes what it staged before reporting (§16.2), and a
    /// refusal stands only once a second staging agrees (§19.1).
    pub(crate) async fn select(
        &self,
        announcement: SnapshotAnnouncement,
        ring: &StoreKeyring,
        progress: Option<&mut ProgressReport<LoadStep>>,
    ) -> Result<SelectedSnapshot, SyncError> {
        self.select_into(announcement, ring, progress, None).await
    }

    /// [`Self::select`], staging into the rebuild's store `rebuild` when
    /// given, which loads it, and into this one's otherwise.
    pub(crate) async fn select_into(
        &self,
        announcement: SnapshotAnnouncement,
        ring: &StoreKeyring,
        mut progress: Option<&mut ProgressReport<LoadStep>>,
        rebuild: Option<&coven_database::RebuildDatabase>,
    ) -> Result<SelectedSnapshot, SyncError> {
        let object = self.snapshot_prefix(&announcement).await?;
        let path = object.path().clone();
        if let Some(progress) = progress.as_deref_mut() {
            progress
                .begin(LoadStep::Streaming(announcement.snapshot.audience.clone()), object.size());
        }
        let mut staging = match rebuild {
            Some(rebuild) => {
                rebuild
                    .stage_snapshot(announcement.snapshot.clone(), object.prefix().clone())
                    .await?
            }
            None => {
                self.database
                    .stage_snapshot(announcement.snapshot.clone(), object.prefix().clone())
                    .await?
            }
        };
        let read = self
            .read_snapshot(announcement.clone(), object, ring, Some(&mut staging), progress)
            .await;
        let failed = match read {
            Ok(read) => match staging.finish().await {
                Ok(()) => return Ok(SelectedSnapshot { read, staging }),
                Err(error) => database_failure(&path, error),
            },
            // A refusal the stored object's second read confirmed judged no
            // staged record, so a second staging has nothing to add (§19.1).
            Err(failure) if failure.confirmed => {
                return Err(match staging.discard().await {
                    Ok(()) => failure.error,
                    Err(cleanup) => SyncError::cleanup(failure.error, cleanup.into()),
                })
            }
            Err(failure) => failure.error,
        };
        Err(self.end_staged(failed, staging, &announcement, ring).await)
    }

    /// Bootstrap the floor's history from its store snapshot, read once,
    /// then authenticate the snapshot and the floor against it.
    pub(super) async fn history_from(
        &self,
        floor: &FloorObject,
        snapshot: &ReadSnapshot,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<History, SyncError> {
        let restoring = self.clock.elapsed();
        let originals = &snapshot.body().originals;
        let mut history = History::restore(originals, self.store(), ring, member)?;
        let authenticating = self.clock.elapsed();
        snapshot.authenticate(&history)?;
        tracing::info!(
            originals = originals.len(),
            restoring = ?(authenticating - restoring),
            authenticating = ?(self.clock.elapsed() - authenticating),
            "read the history in the floor's store snapshot"
        );
        self.advance_history(
            &mut history,
            snapshot.announcement().covers,
            floor.header.at,
            ring,
            member,
        )
        .await?;
        self.authenticate_floor(floor, &history, ring, member).await?;
        Ok(history)
    }

    /// [`Self::history_from`] for a selected snapshot, whose read it holds.
    pub(super) async fn history_from_selected(
        &self,
        floor: &FloorObject,
        selected: &SelectedSnapshot,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<History, SyncError> {
        self.history_from(floor, &selected.read, ring, member).await
    }
}
