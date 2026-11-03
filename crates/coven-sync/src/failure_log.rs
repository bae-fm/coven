//! Each failure recorded for a subject is logged once: when the subject's
//! reason appears or changes (§19.1). The same failure met again on each
//! retry logs nothing more.
//!
//! The record and the text of its error live in `_coven_pending`, written
//! in one step; this keeps only the reason last logged for each subject,
//! which decides whether the next failure is new.

use crate::{PendingReason, PendingSubject};
use coven_database::OperationId;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::sync::Mutex;

/// The reason last logged for each subject. The store's retry waits keep
/// one for the open store.
pub(crate) struct FailureLog {
    logged: Mutex<BTreeMap<PendingSubject, PendingReason>>,
}

impl FailureLog {
    pub(crate) fn new() -> Self {
        Self { logged: Mutex::new(BTreeMap::new()) }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<PendingSubject, PendingReason>> {
        self.logged.lock().expect("failure log poisoned")
    }

    /// Log that `subject` failed with `reason` because of `error`, when the
    /// subject's reason appears or changes: at warn, or at debug for a wait.
    /// Returns whether it was logged. The error's text names every cause,
    /// with secrets and URLs already cut (coven-storage).
    pub(crate) fn failed(
        &self,
        subject: &PendingSubject,
        reason: &PendingReason,
        error: &(dyn Error + 'static),
    ) -> bool {
        let changed = self.lock().insert(subject.clone(), reason.clone()).as_ref() != Some(reason);
        // A slot's record blocks the whole device (§5.4), whatever it waits for.
        let waits = reason.waits() && !matches!(subject, PendingSubject::Slot(_));
        if changed && waits {
            tracing::debug!(?subject, ?reason, "pending work waits: {error}");
        } else if changed {
            tracing::warn!(?subject, ?reason, "pending work failed: {error}");
        }
        changed
    }

    /// The subject's work advanced past a failure `ended` accepts: its record
    /// is gone, so a later failure is new and logs again.
    pub(crate) fn cleared(&self, subject: &PendingSubject, ended: impl Fn(&PendingReason) -> bool) {
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
            PendingSubject::Operation { id, .. } => live.contains(id),
            _ => true,
        });
    }
}

fn operation(subject: &PendingSubject, operation: OperationId) -> bool {
    matches!(subject, PendingSubject::Operation { id, .. } if *id == operation)
}

#[cfg(test)]
#[path = "failure_log_tests.rs"]
mod tests;
