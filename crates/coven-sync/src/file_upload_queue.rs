//! The upload queue as the app observes it (E7): the committed queue, with
//! the uploads this process is sending now, and what each upload is doing.

use crate::{PendingReason, PendingRecord};
pub use coven_database::UploadProgress;
use coven_database::{DbError, QueueFacts};
use coven_foundation::id_source::FileId;
use std::time::SystemTime;

/// Observable upload queue state (E7): one committed snapshot of the queue,
/// with the uploads this process is sending as it is read.
#[derive(Clone, Debug, PartialEq)]
pub struct UploadQueue {
    /// Whether transfer work is paused; persisted across reopening.
    pub paused: bool,
    /// Waiting files, oldest first. Files sharing one upload share its
    /// place, progress and phase.
    pub files: Vec<QueuedUpload>,
}
/// One queued upload: what it is doing, and its committed progress.
#[derive(Clone, Debug, PartialEq)]
pub struct QueuedUpload {
    /// The file its upload switches to the cloud.
    pub file: FileId,
    /// What the upload is doing (§17.9).
    pub phase: UploadPhase,
    /// Progress the provider confirmed and this device persisted in the
    /// file's resumable session; `None` while it has no session, as a file
    /// sent in one request never does.
    pub progress: Option<UploadProgress>,
    /// Enqueue time.
    pub queued_at: SystemTime,
}
/// What a queued upload is doing, bound and trailing alike: the sync state
/// names bound files only, so what a trailing file waits for, and its
/// failure, are shown here (§17.9, §19.1). Whether it is sending is
/// persisted nowhere: a reopened store starts with every file waiting.
#[derive(Clone, Debug, PartialEq)]
pub enum UploadPhase {
    /// An attempt is sending it now, in one request or in parts.
    Uploading,
    /// Storage holds its bytes; it waits for the rest of its move (§17.3).
    Stored,
    /// Not being sent, for this reason.
    Waiting(UploadWait),
    /// Its pending record names a failure only a person clears, or one that
    /// never clears: a file whose original is missing or changed, coven's
    /// copy failing its check, stored bytes lost or refused, a full account.
    Failed(PendingRecord),
}

/// What a queued upload waits for, the first that holds (§17.9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadWait {
    /// Sync isn't running (`start_sync`, `stop_sync`).
    NotSyncing,
    /// The provider asked every request to wait until then (§6.7).
    CoolingDown {
        /// When the cooldown ends, by the store's clock.
        until: SystemTime,
    },
    /// A slot can't apply yet, so no transfer goes out until it does (§5.4).
    Sync,
    /// The app paused uploads, and the file's reference trails
    /// (`set_uploads_paused`, §17.4).
    Paused,
    /// The network is metered, or may be, since the platform stopped
    /// answering after opening (E5); the app moves files only on unmetered
    /// networks (`set_unmetered_only`, §17.9), and the file's reference
    /// trails.
    Metered,
    /// A self-replacement of this member's keys pauses writes, and the
    /// file's reference trails (§12.3).
    KeysChanging,
    /// Its last attempt failed, and it retries then, or at the person's
    /// retry (§6.7).
    Retrying {
        /// When its backoff ends, by the store's clock.
        at: SystemTime,
    },
    /// Its turn hasn't come.
    Queued,
}

impl UploadQueue {
    /// The queue as one committed snapshot read it: each upload's phase from
    /// the facts the sync state reads, the upload's retry wait and what is
    /// recorded about it. A move's record names an original, and shows as the
    /// record of the upload that uploads that original, as the upload's own
    /// record does for its bytes.
    pub fn from_facts(facts: QueueFacts<PendingRecord>) -> Result<Self, DbError> {
        let QueueFacts { sync, queue, recorded } = facts;
        let (state, _) = crate::sync_status::Facts::read(sync)?;
        let mut files = Vec::new();
        for upload in queue.uploads {
            let recorded = recorded.iter().find(|recorded| recorded.upload == upload.id);
            let record = recorded.and_then(|recorded| {
                recorded.own.clone().or_else(|| {
                    recorded.moving.clone().filter(|record| {
                        names_original(&record.reason, recorded.original.as_deref())
                    })
                })
            });
            let sending = upload.files.iter().any(|file| queue.sending.contains(file));
            let phase = state.upload_phase(&crate::sync_status::Upload {
                progress: if sending {
                    crate::sync_status::Progress::Moving
                } else if upload.stored {
                    crate::sync_status::Progress::Stored
                } else {
                    crate::sync_status::Progress::Queued
                },
                bound: upload.holds.is_some(),
                retry_at: upload.retry.map(|retry| retry.not_before),
                record,
            });
            files.extend(upload.files.iter().map(|file| QueuedUpload {
                phase: phase.clone(),
                file: *file,
                progress: upload.progress,
                queued_at: upload.queued_at,
            }));
        }
        Ok(Self { paused: queue.paused, files })
    }
}

/// Whether `reason` names the original at `path`.
fn names_original(reason: &PendingReason, path: Option<&std::path::Path>) -> bool {
    matches!(reason, PendingReason::Original { path: named, .. } if Some(named.as_path()) == path)
}
