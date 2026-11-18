//! What each upload in the queue is doing (§17.9, api.md E7), computed from the
//! facts the sync state reads, as `Upload.phase` of the Lean model in
//! `spec/proofs/io/CovenIO/Transfers.lean` computes it. The state names bound
//! uploads only; every upload, a trailing one included, has this phase, which
//! an app shows where files are. `uploads()` gives it, and the
//! `coven_upload_phases` view shows it through `_coven_upload_phase`
//! (`upload_phase_row`), so an app's live query reads the same phase.

use crate::files::{UploadPhase, UploadWait};
use crate::sync_status::{Facts, Progress, Upload};
use crate::{PendingRecord, SyncWait};
use coven_database::{DbError, TransferHolds, UploadPhaseFacts, UploadPhaseRow};

impl SyncWait {
    /// What the same hold shows as an upload's wait: only what holds every
    /// transfer, the pass's, each upload's and a queued write's alike,
    /// holds an upload as well (the model's `Facts.heldAll`).
    fn held_transfers(self) -> Option<UploadWait> {
        match self {
            Self::NotSyncing => Some(UploadWait::NotSyncing),
            Self::CoolingDown { until } => Some(UploadWait::CoolingDown { until }),
            Self::KeyCopy { .. } | Self::Snapshot { .. } => Some(UploadWait::Sync),
            Self::Retrying { .. } => None,
        }
    }
}

impl Facts {
    /// What one upload in the queue is doing, a trailing one's included (the
    /// model's `Upload.phase`), with what holds every transfer as these facts
    /// say it: `upload_phase`.
    pub(crate) fn upload_phase(&self, holds: &TransferHolds, upload: &Upload) -> UploadPhase {
        upload_phase(self.held_all(), holds, upload)
    }
}

/// What one upload in the queue is doing, a trailing one's included (the
/// model's `Upload.phase`): sending, stored, failed once its record names a
/// state, else waiting for the first that holds. What holds every transfer
/// (`held_all`) comes first, then what holds a trailing upload only
/// (`holds`: the pause, the network, a self-replacement's write pause),
/// then its backoff, then its turn. The network comes before the backoff,
/// since an upload whose backoff ends still can't go out on a metered
/// network.
fn upload_phase(held_all: Option<SyncWait>, holds: &TransferHolds, upload: &Upload) -> UploadPhase {
    match upload.progress {
        Progress::Moving => UploadPhase::Uploading,
        Progress::Stored => UploadPhase::Stored,
        Progress::Queued => {
            if let Some(record) =
                upload.record.as_ref().filter(|record| record.reason.names_a_state())
            {
                return UploadPhase::Failed(record.clone());
            }
            UploadPhase::Waiting(if let Some(held) = held_all.and_then(SyncWait::held_transfers) {
                held
            } else if holds.paused && !upload.bound {
                UploadWait::Paused
            } else if holds.unmetered_only
                && holds.metered
                && crate::files::held_on_metered(upload.bound)
            {
                UploadWait::Metered
            } else if holds.keys_changing && !upload.bound {
                UploadWait::KeysChanging
            } else if let Some(at) = upload.retry_at {
                UploadWait::Retrying { at }
            } else {
                UploadWait::Queued
            })
        }
    }
}

/// One upload's phase as `coven_upload_phases` shows it, from the facts the
/// view passes `_coven_upload_phase`: the phase `uploads()` gives the
/// upload, with its record chosen as `uploads()` chooses it, its own, else
/// its move's when that names the original the upload uploads.
fn upload_phase_row(facts: UploadPhaseFacts<PendingRecord>) -> Result<UploadPhaseRow, DbError> {
    let last = crate::sync_status::last_pass(facts.last_pass)?;
    let original = facts.original.as_deref();
    let record = facts.own.or_else(|| {
        facts.moving.filter(|(_, record)| crate::files::names_original(&record.reason, original))
    });
    let (position, record) = record.unzip();
    let upload = Upload {
        progress: if facts.sending {
            Progress::Moving
        } else if facts.stored {
            Progress::Stored
        } else {
            Progress::Queued
        },
        bound: facts.bound,
        retry_at: facts.retry_at,
        record,
    };
    let phase = upload_phase(
        crate::sync_status::held_all(facts.sync, last.as_ref()),
        &facts.holds,
        &upload,
    );
    Ok(UploadPhaseRow {
        phase: phase.kind(),
        wait: match &phase {
            UploadPhase::Waiting(wait) => Some(wait.kind()),
            UploadPhase::Uploading | UploadPhase::Stored | UploadPhase::Failed(_) => None,
        },
        pending: match phase {
            UploadPhase::Failed(_) => position,
            UploadPhase::Uploading | UploadPhase::Stored | UploadPhase::Waiting(_) => None,
        },
    })
}

/// `_coven_sync_state` and `_coven_upload_phase`, the functions
/// `coven_sync_state` and `coven_upload_phases` call, for the database to
/// register on every connection: the state and each upload's phase computed
/// in this crate's Rust code, so that it stays the one definition the Lean
/// model checks while an app's screen reads them in SQL (§17.9, §19.1).
pub fn sync_functions() -> coven_database::SyncFunctions {
    coven_database::SyncFunctions::new(crate::sync_status::SyncStatus::row, upload_phase_row)
}

#[cfg(test)]
#[path = "upload_phase_tests.rs"]
mod tests;
