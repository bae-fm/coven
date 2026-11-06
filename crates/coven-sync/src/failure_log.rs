//! Each failure recorded for a subject is logged once: when the subject's
//! reason appears or changes (§19.1). The same failure met again on each
//! retry logs nothing more.
//!
//! The record and the text of its error live in `_coven_pending`, written
//! in one step; this keeps only the reason last logged for each subject,
//! which decides whether the next failure is new.

use crate::{PendingReason, PendingSubject};
use coven_database::OperationId;
use coven_foundation::id_source::FileId;
use coven_foundation::locks::lock_unpoisoned;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::sync::Mutex;

/// What a logged failure belongs to: the work its record names, or a pin
/// coven keeps, by its request, whose id is never reused. A later request
/// for the same file is new work, so its first failure logs, whatever the
/// ended request last logged.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Logged {
    /// The subject of the failure's record.
    Subject(PendingSubject),
    /// A pin coven keeps for a file kept here (§17.8), by its request.
    Kept { request: i64, file: FileId },
}

impl From<&PendingSubject> for Logged {
    fn from(subject: &PendingSubject) -> Self {
        Self::Subject(subject.clone())
    }
}

/// The reason last logged for each subject. The store's retry waits keep
/// one for the open store.
pub(crate) struct FailureLog {
    logged: Mutex<BTreeMap<Logged, PendingReason>>,
    /// How many failures were logged, for tests to see whether one was.
    #[cfg(test)]
    count: std::sync::atomic::AtomicUsize,
}

impl FailureLog {
    pub(crate) fn new() -> Self {
        Self {
            logged: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// How many failures were logged so far.
    #[cfg(test)]
    pub(crate) fn count(&self) -> usize {
        self.count.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<Logged, PendingReason>> {
        lock_unpoisoned(&self.logged)
    }

    /// Log that `subject` failed with `reason` because of `error`, when the
    /// subject's reason appears or changes: at warn, or at debug for a wait.
    /// Returns whether it was logged. The error's text names every cause,
    /// with secrets and URLs already cut (coven-storage).
    pub(crate) fn failed(
        &self,
        subject: Logged,
        reason: &PendingReason,
        error: &(dyn Error + 'static),
    ) -> bool {
        // A slot's record blocks the whole device (§5.4), whatever it waits for.
        let waits = reason.waits() && !matches!(subject, Logged::Subject(PendingSubject::Slot(_)));
        let changed = self.lock().insert(subject.clone(), reason.clone()).as_ref() != Some(reason);
        #[cfg(test)]
        if changed {
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if changed && waits {
            tracing::debug!(?subject, ?reason, "pending work waits: {error}");
        } else if changed {
            tracing::warn!(?subject, ?reason, "pending work failed: {error}");
        }
        changed
    }

    /// The subject's work advanced past a failure `ended` accepts: its record
    /// is gone, so a later failure is new and logs again.
    pub(crate) fn cleared(&self, subject: &Logged, ended: impl Fn(&PendingReason) -> bool) {
        let mut logged = self.lock();
        if logged.get(subject).is_some_and(ended) {
            logged.remove(subject);
        }
    }

    /// The operation advanced or finished: a later failure is new.
    pub(crate) fn cleared_operation(&self, id: OperationId) {
        self.lock().retain(|subject, _| !operation(subject, id));
    }

    /// Forget the failures of operations that no longer exist.
    pub(crate) fn retain_operations(&self, live: &BTreeSet<OperationId>) {
        self.lock().retain(|subject, _| match subject {
            Logged::Subject(PendingSubject::Operation { id, .. }) => live.contains(id),
            _ => true,
        });
    }

    /// Forget the failures of pin requests not in `live`, which have ended.
    pub(crate) fn retain_kept(&self, live: &BTreeSet<i64>) {
        self.lock().retain(|subject, _| match subject {
            Logged::Kept { request, .. } => live.contains(request),
            Logged::Subject(_) => true,
        });
    }
}

fn operation(subject: &Logged, operation: OperationId) -> bool {
    matches!(subject, Logged::Subject(PendingSubject::Operation { id, .. }) if *id == operation)
}

#[cfg(test)]
#[path = "failure_log_tests.rs"]
mod tests;
