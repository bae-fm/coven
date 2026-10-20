//! The app's report that the device's network changed (§6.7; IO
//! `WaitingNetwork`). A wait whose last attempt received no provider
//! response waits for the network, so a report ends it: the connection's
//! backoff, an operation's, a queued upload's and an eager download's. A wait
//! after a provider's answer, and the provider cooldown, hold.
//!
//! Each owner acts on reports for the waits it retries, when it next plans:
//! the connection and its sync loop after the pass in flight, the file worker
//! after the transfers in flight. So a report during an attempt also ends the
//! wait that attempt fails into (IO `signal_during_attempt_wakes`). Reports
//! an owner has not acted on yet coalesce into one.

use crate::{PendingRecord, PendingSubject, RetryWaits, SyncError};
use coven_database::{FileUpload, RetrySubject, RetryWait};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

/// An owner of waits that acts on network reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkOwner {
    /// The connection and its sync loop: the connection's backoff and
    /// coven's own operations.
    Connection,
    /// The file worker: queued uploads and eager downloads.
    Files,
}

impl NetworkOwner {
    fn retries(self, subject: &RetrySubject) -> bool {
        match self {
            Self::Connection => matches!(subject, RetrySubject::Sync | RetrySubject::Operation(_)),
            Self::Files => matches!(
                subject,
                RetrySubject::Upload(_) | RetrySubject::Download { .. }
            ),
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Connection => 0,
            Self::Files => 1,
        }
    }
}

/// Reports so far, and the count each owner has acted on; an owner acts
/// on a report only once its waits for the network ended.
pub(crate) struct Reports {
    reported: tokio::sync::watch::Sender<u64>,
    handled: [AtomicU64; 2],
}

impl Default for Reports {
    fn default() -> Self {
        Self {
            reported: tokio::sync::watch::channel(0).0,
            handled: [AtomicU64::new(0), AtomicU64::new(0)],
        }
    }
}

/// The pending record a wait belongs to: the one naming its failure.
fn record_for<'a>(
    subject: RetrySubject,
    pending: &'a [PendingRecord],
    uploads: &[FileUpload],
) -> Option<&'a PendingRecord> {
    let wanted = match subject {
        RetrySubject::Sync => PendingSubject::Connection,
        RetrySubject::Operation(id) => {
            return pending.iter().find(|record| {
                matches!(record.subject, PendingSubject::Operation { id: owned, .. } if owned == id)
            })
        }
        RetrySubject::Upload(id) => {
            let file = &uploads.iter().find(|upload| upload.id == id)?.file;
            PendingSubject::File {
                device: file.uploader(),
                file: file.id(),
            }
        }
        RetrySubject::Download { uploader, file } => PendingSubject::File {
            device: uploader,
            file,
        },
        RetrySubject::Join | RetrySubject::Provider => return None,
    };
    pending.iter().find(|record| record.subject == wanted)
}

impl RetryWaits {
    /// The app reports that the device's network changed: it became
    /// available, or moved to another network (§6.7). Each owner acts on it
    /// when it next plans; reports before then coalesce.
    pub fn network_changed(&self) {
        self.network.reported.send_modify(|count| *count += 1);
        // The connection's owners plan again on a changed schedule.
        self.rearmed
            .send_modify(|count| *count = count.wrapping_add(1));
    }

    /// Changes with every network report, for an owner that waits on its
    /// own timers.
    pub fn network_reports(&self) -> tokio::sync::watch::Receiver<u64> {
        self.network.reported.subscribe()
    }

    /// End `owner`'s waits that wait for the network, if a report arrived
    /// since it last acted (§6.7). Each such wait keeps its failure count, so
    /// the next failure continues the backoff, and is saved ended, so no
    /// reopen arms it again (IO `ended_reopens_due`). A wait after a
    /// provider's answer, one held because saving it failed, and the
    /// provider cooldown hold. Returns whether a report arrived, so the
    /// owner also runs the work it asks for. If reading or saving fails,
    /// every wait holds, the owner reports the error, and the report stays
    /// unacted on, so the owner's next plan acts on it again.
    pub async fn end_offline_waits(&self, owner: NetworkOwner) -> Result<bool, SyncError> {
        let reported = *self.network.reported.borrow();
        let handled = &self.network.handled[owner.index()];
        if handled.load(Ordering::SeqCst) >= reported {
            return Ok(false);
        }
        let Some(database) = &self.database else {
            handled.fetch_max(reported, Ordering::SeqCst);
            return Ok(true);
        };
        // A wait this session holds for the app stays held; one already
        // over needs no end unless a new failure replaced it.
        let (held, over) = {
            let elapsed = self.clock.elapsed();
            let state = self.lock();
            let mut held = BTreeSet::new();
            let mut over = BTreeMap::new();
            for (subject, armed) in &state.armed {
                if !owner.retries(subject) {
                    continue;
                }
                if armed.timer.is_none() {
                    held.insert(*subject);
                } else if armed.over(elapsed) {
                    over.insert(*subject, armed.not_before);
                }
            }
            (held, over)
        };
        let select = move |waits: &[(RetrySubject, RetryWait)],
                           pending: &[PendingRecord],
                           uploads: &[FileUpload]| {
            waits
                .iter()
                .filter(|(subject, wait)| {
                    owner.retries(subject)
                        && !held.contains(subject)
                        && over.get(subject) != Some(&wait.not_before)
                        && !wait.wait.is_zero()
                        && record_for(*subject, pending, uploads)
                            .is_some_and(|record| record.reason.offline())
                })
                .map(|(subject, _)| *subject)
                .collect()
        };
        let now: SystemTime = self.clock.now();
        for (subject, ended) in database
            .end_retry_waits::<PendingRecord>(now, select)
            .await?
        {
            self.arm(subject, &ended);
        }
        // Reports since `reported` was read stay for the next plan.
        handled.fetch_max(reported, Ordering::SeqCst);
        Ok(true)
    }
}
