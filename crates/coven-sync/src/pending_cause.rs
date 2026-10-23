//! The error behind each pending record this process wrote (§19.1, E5).
//!
//! A pending record persists only its typed reason, which is all that
//! survives a restart. The process that met the failure holds the error
//! itself, with every cause below it, and `PendingRecord::cause` carries it
//! while the record still states that failure. Every failure recorded for a
//! subject goes through `failed`, which also logs it once, when the
//! subject's reason appears or changes: the same failure met again on each
//! retry logs nothing more.

use crate::{PendingReason, PendingRecord, PendingSubject};
use coven_database::OperationId;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::sync::{Arc, Mutex};

/// An error held as a pending record's cause.
pub(crate) type Cause = Arc<dyn Error + Send + Sync>;

/// The error behind a failure recorded for a subject.
pub(crate) enum Failure<'a> {
    /// No caller receives this error: the record keeps it as its cause.
    Held(Cause),
    /// An app call returns this error to its caller, which holds it; the
    /// record keeps no cause, so it never names an earlier failure's.
    Returned(&'a (dyn Error + 'static)),
    /// Work stopped at this failure again, without the error that caused
    /// it: the record keeps the error it holds for the same reason.
    Repeated(&'a (dyn Error + 'static)),
}

/// Each subject's last recorded failure and, when held, its error. One
/// shared owner per open store: clones share it.
#[derive(Clone, Default)]
pub struct PendingCauses {
    held: Arc<Mutex<BTreeMap<PendingSubject, Recorded>>>,
}

struct Recorded {
    reason: PendingReason,
    cause: Option<Cause>,
}

impl PendingCauses {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<PendingSubject, Recorded>> {
        self.held.lock().expect("pending causes poisoned")
    }

    /// Record that `subject` failed with `reason`, and keep its error as
    /// the record's cause as `failure` says. The failure is logged when the
    /// subject's reason appears or changes: at warn, or at debug for a wait.
    /// Returns whether it was. The error's text names every cause, with
    /// secrets and URLs already cut (coven-storage).
    pub(crate) fn failed(
        &self,
        subject: &PendingSubject,
        reason: &PendingReason,
        failure: Failure<'_>,
    ) -> bool {
        let mut held = self.lock();
        let kept = held
            .get(subject)
            .filter(|recorded| recorded.reason == *reason)
            .map(|recorded| recorded.cause.clone());
        let changed = kept.is_none();
        let (error, cause): (&(dyn Error + 'static), _) = match &failure {
            Failure::Held(cause) => (cause.as_ref(), Some(cause.clone())),
            Failure::Returned(error) => (*error, None),
            Failure::Repeated(error) => (*error, kept.flatten()),
        };
        // A slot's record blocks the whole device (§5.4), whatever it waits for.
        let waits = reason.waits() && !matches!(subject, PendingSubject::Slot(_));
        if changed && waits {
            tracing::debug!(?subject, ?reason, "pending work waits: {error}");
        } else if changed {
            tracing::warn!(?subject, ?reason, "pending work failed: {error}");
        }
        held.insert(
            subject.clone(),
            Recorded {
                reason: reason.clone(),
                cause,
            },
        );
        changed
    }

    /// The subject's work advanced past a failure `ended` accepts: its record
    /// is gone, so a later failure is new and logs again.
    pub(crate) fn cleared(&self, subject: &PendingSubject, ended: impl Fn(&PendingReason) -> bool) {
        let mut held = self.lock();
        if held
            .get(subject)
            .is_some_and(|recorded| ended(&recorded.reason))
        {
            held.remove(subject);
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

    /// `records`, each with the error behind it when this process holds the
    /// error for exactly the reason the record states.
    pub fn attach(&self, mut records: Vec<PendingRecord>) -> Vec<PendingRecord> {
        let held = self.lock();
        for record in &mut records {
            record.cause = held
                .get(&record.subject)
                .filter(|recorded| recorded.reason == record.reason)
                .and_then(|recorded| recorded.cause.clone());
        }
        records
    }
}

fn operation(subject: &PendingSubject, operation: OperationId) -> bool {
    matches!(subject, PendingSubject::Operation { id, .. } if *id == operation)
}

#[cfg(test)]
#[path = "pending_cause_tests.rs"]
mod tests;
