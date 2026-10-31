//! The app's reports that ends waits early (§6.7; IO `WaitingNetwork`).
//!
//! A network report says the device's network changed. A wait whose last
//! attempt received no provider response waits for the network, so the
//! report ends it: the connection's backoff, an operation's and a queued
//! upload's. A wait after a provider's answer holds. A download has no wait:
//! only the app's next read or pin tries it again.
//! A retry report says the person asked to retry everything now: it ends
//! every ordinary wait, answered or not. Neither ends the provider cooldown,
//! and an ended wait keeps its failure count.
//!
//! Each owner acts on reports for the waits it retries, when it next plans:
//! the connection and its sync loop after the pass in flight, and the upload
//! worker each time it starts uploads. A report during an attempt also ends
//! the wait that attempt fails into (IO `signal_during_attempt_wakes`): the
//! passes act on it after the attempt, and the upload worker ends that wait
//! as the attempt ends. Reports an owner has not acted on yet coalesce into one, which ends
//! every ordinary wait if any of them was a retry report.

use crate::{PendingRecord, PendingSubject, RetryWaits, SyncError};
use coven_database::{FileUpload, RetrySubject, RetryWait};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

/// An owner of waits that acts on reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkOwner {
    /// The connection and its sync loop: the connection's backoff and
    /// coven's own operations.
    Connection,
    /// The upload worker: queued uploads, and the pins coven keeps, which it
    /// makes after each run.
    Uploads,
}

impl NetworkOwner {
    fn retries(self, subject: &RetrySubject) -> bool {
        match self {
            Self::Connection => matches!(subject, RetrySubject::Sync | RetrySubject::Operation(_)),
            Self::Uploads => matches!(subject, RetrySubject::Upload(_) | RetrySubject::Pin(_)),
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Connection => 0,
            Self::Uploads => 1,
        }
    }
}

/// Which waits a report ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ending {
    /// Waits whose last attempt received no provider response.
    Network,
    /// Every ordinary wait: the person asked to retry everything now.
    Every,
}

/// Reports so far, the latest retry report among them, and the count each
/// owner has acted on; an owner acts on a report only once the waits it
/// ends have ended.
pub(crate) struct Reports {
    reported: tokio::sync::watch::Sender<u64>,
    /// The number of the latest retry report; zero before any.
    retried: AtomicU64,
    handled: [AtomicU64; 2],
}

impl Default for Reports {
    fn default() -> Self {
        Self {
            reported: tokio::sync::watch::channel(0).0,
            retried: AtomicU64::new(0),
            handled: [AtomicU64::new(0), AtomicU64::new(0)],
        }
    }
}

impl Reports {
    /// Reports so far, and which waits those after `seen` end; `None` when
    /// none arrived since.
    fn since(&self, seen: u64) -> (u64, Option<Ending>) {
        let reported = self.reported.borrow();
        let ending = if *reported <= seen {
            None
        } else if self.retried.load(Ordering::SeqCst) > seen {
            Some(Ending::Every)
        } else {
            Some(Ending::Network)
        };
        (*reported, ending)
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
            let location = &uploads.iter().find(|upload| upload.id == id)?.location;
            PendingSubject::Upload {
                device: location.uploader(),
                file: location.object(),
            }
        }
        RetrySubject::Pin(file) => PendingSubject::Keep { file },
        RetrySubject::Join | RetrySubject::Provider => return None,
    };
    pending.iter().find(|record| record.subject == wanted)
}

impl RetryWaits {
    /// The app reports that the device's network changed: it became
    /// available, or moved to another network (§6.7). Each owner acts on it
    /// when it next plans; reports before then coalesce.
    pub fn network_changed(&self) {
        self.report(Ending::Network);
    }

    /// Report and return its number. The person's retry
    /// (`retry_requested`) reports this way too.
    pub(crate) fn report(&self, ending: Ending) -> u64 {
        let mut number = 0;
        self.network.reported.send_modify(|count| {
            *count += 1;
            number = *count;
            if ending == Ending::Every {
                self.network.retried.store(*count, Ordering::SeqCst);
            }
        });
        // The owners plan again on a changed schedule.
        self.rearmed.send_modify(|count| *count = count.wrapping_add(1));
        number
    }

    /// How many reports `owner` has acted on.
    pub(crate) fn handled(&self, owner: NetworkOwner) -> u64 {
        self.network.handled[owner.index()].load(Ordering::SeqCst)
    }

    /// Changes with every report, for an owner that waits on its own timers.
    pub fn network_reports(&self) -> tokio::sync::watch::Receiver<u64> {
        self.network.reported.subscribe()
    }

    /// End the waits the reports since `owner` last acted end (§6.7): its
    /// waits for the network, or every ordinary wait it retries after a
    /// retry report. Each ended wait keeps its failure count, so the next
    /// failure continues the backoff, and is saved ended, so no reopen arms
    /// it again (IO `ended_reopens_due`). The provider cooldown holds, and a
    /// network report leaves a wait after a provider's answer and one held
    /// because saving it failed. Returns whether a report arrived, so the
    /// owner also runs the work it asks for. If reading or saving fails,
    /// every wait holds, the owner reports the error, and the reports stay
    /// unacted on, so the owner's next plan acts on them again.
    pub async fn end_reported_waits(&self, owner: NetworkOwner) -> Result<bool, SyncError> {
        let handled = &self.network.handled[owner.index()];
        let (reported, ending) = self.network.since(handled.load(Ordering::SeqCst));
        let Some(ending) = ending else {
            return Ok(false);
        };
        self.end_waits(owner, move |_| Some(ending)).await?;
        // Reports since `reported` was read stay for the next plan.
        handled.fetch_max(reported, Ordering::SeqCst);
        Ok(true)
    }

    /// How many reports have arrived so far. An attempt notes it as it
    /// starts, so it can tell whether one arrived while it ran.
    pub(crate) fn network_reported(&self) -> u64 {
        *self.network.reported.borrow()
    }

    /// Which waits the reports since `seen` end; `None` when none arrived.
    pub(crate) fn reports_since(&self, seen: u64) -> Option<Ending> {
        self.network.since(seen).1
    }

    /// End each subject's wait as the reports that arrived while its attempt
    /// ran say, whatever reports their owner already acted on: the report
    /// ends the wait the attempt failed into (IO
    /// `signal_during_attempt_wakes`). Fails as `end_reported_waits` does.
    pub(crate) async fn end_waits_after_report(
        &self,
        owner: NetworkOwner,
        subjects: &BTreeMap<RetrySubject, Ending>,
    ) -> Result<(), SyncError> {
        let subjects = subjects.clone();
        self.end_waits(owner, move |subject| subjects.get(subject).copied()).await
    }

    /// End `owner`'s waits as `ending` says for each subject.
    async fn end_waits(
        &self,
        owner: NetworkOwner,
        ending: impl Fn(&RetrySubject) -> Option<Ending> + Send + 'static + Clone,
    ) -> Result<(), SyncError> {
        // A wait this session holds for the app stays held unless the person
        // retries; one already over needs no end unless a new failure
        // replaced it.
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
        // The person's retry releases a held wait: it was holding for them.
        let released: Vec<RetrySubject> =
            held.iter().filter(|subject| ending(subject) == Some(Ending::Every)).copied().collect();
        let Some(database) = &self.database else {
            // A handle that cannot persist keeps its waits in memory: the
            // person's retry forgets them.
            let every: Vec<RetrySubject> = self
                .lock()
                .armed
                .keys()
                .filter(|subject| owner.retries(subject) && ending(subject) == Some(Ending::Every))
                .copied()
                .collect();
            for subject in every {
                self.forget(subject);
            }
            return Ok(());
        };
        let chosen = ending.clone();
        let select = move |waits: &[(RetrySubject, RetryWait)],
                           pending: &[PendingRecord],
                           uploads: &[FileUpload]| {
            waits
                .iter()
                .filter(|(subject, wait)| {
                    let Some(ending) = chosen(subject) else {
                        return false;
                    };
                    owner.retries(subject)
                        && over.get(subject) != Some(&wait.not_before)
                        && !wait.wait.is_zero()
                        && match ending {
                            Ending::Every => true,
                            Ending::Network => {
                                !held.contains(subject)
                                    && record_for(*subject, pending, uploads)
                                        .is_some_and(|record| record.reason.offline())
                            }
                        }
                })
                .map(|(subject, _)| *subject)
                .collect()
        };
        let now: SystemTime = self.clock.now();
        let mut ended = BTreeSet::new();
        for (subject, wait) in database.end_retry_waits::<PendingRecord>(now, select).await? {
            self.arm(subject, &wait);
            ended.insert(subject);
        }
        // A held wait with no saved row to end is simply forgotten.
        for subject in released {
            if !ended.contains(&subject) {
                self.forget(subject);
            }
        }
        Ok(())
    }
}
