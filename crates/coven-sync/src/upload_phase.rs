//! What each upload in the queue is doing (§17.9, api.md E7), computed from the
//! facts the sync state reads, as `Upload.phase` of the Lean model in
//! `spec/proofs/io/CovenIO/Transfers.lean` computes it. The state names bound
//! uploads only; every upload, a trailing one included, has this phase, which
//! an app shows where files are.

use crate::files::{UploadPhase, UploadWait};
use crate::sync_status::{Facts, Progress, Upload};
use crate::SyncWait;

impl SyncWait {
    /// What the same hold shows as an upload's wait: only what holds every
    /// transfer, the pass's, each upload's and a queued write's alike,
    /// holds an upload as well (the model's `Facts.heldAll`).
    fn held_transfers(self) -> Option<UploadWait> {
        match self {
            Self::NotSyncing => Some(UploadWait::NotSyncing),
            Self::CoolingDown { until } => Some(UploadWait::CoolingDown { until }),
            Self::Blocked => Some(UploadWait::Sync),
            Self::Retrying { .. } => None,
        }
    }
}

impl Facts {
    /// What one upload in the queue is doing, a trailing one's included (the
    /// model's `Upload.phase`): sending, stored, failed once its record
    /// names a state, else waiting for the first that holds. What holds
    /// every transfer comes first, then what holds a trailing upload only
    /// (the pause, the network, a self-replacement's write pause), then its
    /// backoff, then its turn. The network comes before the backoff, since an
    /// upload whose backoff ends still can't go out on a metered network.
    pub(crate) fn upload_phase(&self, upload: &Upload) -> UploadPhase {
        match upload.progress {
            Progress::Moving => UploadPhase::Uploading,
            Progress::Stored => UploadPhase::Stored,
            Progress::Queued => {
                if let Some(record) =
                    upload.record.as_ref().filter(|record| record.reason.names_a_state())
                {
                    return UploadPhase::Failed(record.clone());
                }
                UploadPhase::Waiting(
                    if let Some(held) = self.held_all().and_then(SyncWait::held_transfers) {
                        held
                    } else if self.paused && !upload.bound {
                        UploadWait::Paused
                    } else if self.unmetered_only
                        && self.metered
                        && crate::files::held_on_metered(upload.bound)
                    {
                        UploadWait::Metered
                    } else if self.keys_changing && !upload.bound {
                        UploadWait::KeysChanging
                    } else if let Some(at) = upload.retry_at {
                        UploadWait::Retrying { at }
                    } else {
                        UploadWait::Queued
                    },
                )
            }
        }
    }
}

#[cfg(test)]
#[path = "upload_phase_tests.rs"]
mod tests;
