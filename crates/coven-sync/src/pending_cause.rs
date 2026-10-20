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

    /// Record that `subject` failed with `reason`, keeping a held error as
    /// its cause. Logs the failure when the subject's reason appears or
    /// changes, at warn, or at debug for a wait, and returns whether it did. The error's text names every
    /// cause with secrets and URLs already cut (coven-storage).
    pub(crate) fn failed(
        &self,
        subject: &PendingSubject,
        reason: &PendingReason,
        failure: Failure<'_>,
    ) -> bool {
        let (error, cause): (&(dyn Error + 'static), _) = match &failure {
            Failure::Held(cause) => (cause.as_ref(), Some(cause.clone())),
            Failure::Returned(error) => (*error, None),
        };
        let mut held = self.lock();
        let changed = held
            .get(subject)
            .is_none_or(|recorded| recorded.reason != *reason);
        if changed && reason.waits() {
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

    /// The error returned for the subject's last recorded failure came back
    /// to work that drops it: the record keeps it as its cause after all.
    pub(crate) fn kept(&self, subject: &PendingSubject, cause: Cause) {
        if let Some(recorded) = self.lock().get_mut(subject) {
            recorded.cause = Some(cause);
        }
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
