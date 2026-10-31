//! The error behind each pending record this process wrote (§19.1, E5).
//!
//! A pending record persists only its typed reason, which is all that
//! survives a restart. The process that met the failure holds the error
//! itself, with every cause below it, and `PendingRecord::cause` carries it
//! while the record still states that failure. Every failure recorded for a
//! subject goes through `failed`, which also logs it once, when the
//! subject's reason appears or changes: the same failure met again on each
//! retry logs nothing more.
//!
//! The errors have no table, so the app's SQL reads its records through
//! `coven_pending` and follows their causes here, on a watch channel keyed by
//! record: each change to a cause a record would show publishes the map again.

use crate::{PendingReason, PendingRecord, PendingSubject};
use coven_database::OperationId;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::watch;

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

/// The errors this process holds behind pending records, by record (E5).
/// A record read from `coven_pending`, or from `pending()`, finds its cause
/// here while this process holds the error for exactly the reason the record
/// states.
#[derive(Clone, Default)]
pub struct PendingCauses {
    held: BTreeMap<PendingSubject, Recorded>,
}

/// A subject's last recorded failure.
#[derive(Clone)]
struct Recorded {
    reason: PendingReason,
    cause: Option<Cause>,
}

impl Recorded {
    /// The cause a record with this reason shows.
    fn shown(&self) -> Option<(&PendingReason, &Cause)> {
        self.cause.as_ref().map(|cause| (&self.reason, cause))
    }
}

/// Whether a record would show a different cause.
fn shows_other(before: Option<&Recorded>, after: Option<&Recorded>) -> bool {
    match (before.and_then(Recorded::shown), after.and_then(Recorded::shown)) {
        (None, None) => false,
        (Some((reason, cause)), Some((other_reason, other_cause))) => {
            reason != other_reason || !Arc::ptr_eq(cause, other_cause)
        }
        _ => true,
    }
}

impl PendingCauses {
    /// The error behind `record`, with every cause below it, while this
    /// process holds it for exactly the reason the record states.
    pub fn cause(&self, record: &PendingRecord) -> Option<Arc<dyn Error + Send + Sync>> {
        self.held
            .get(&record.subject)
            .filter(|recorded| recorded.reason == record.reason)
            .and_then(|recorded| recorded.cause.clone())
    }

    /// `records`, each with the error behind it, as `cause` finds it.
    pub fn attach(&self, mut records: Vec<PendingRecord>) -> Vec<PendingRecord> {
        for record in &mut records {
            record.cause = self.cause(record);
        }
        records
    }
}

/// Each subject's last recorded failure and, when held, its error, published
/// as `PendingCauses`. One shared owner per open store: clones share it.
#[derive(Clone)]
pub(crate) struct HeldCauses {
    published: Arc<watch::Sender<PendingCauses>>,
}

impl Default for HeldCauses {
    fn default() -> Self {
        Self { published: Arc::new(watch::channel(PendingCauses::default()).0) }
    }
}

impl HeldCauses {
    /// The causes as they stand, and again each time one a record would show
    /// appears, changes or ends.
    pub(crate) fn subscribe(&self) -> watch::Receiver<PendingCauses> {
        self.published.subscribe()
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
        let mut changed = false;
        self.published.send_if_modified(|causes| {
            let before = causes.held.get(subject).cloned();
            let kept = before
                .as_ref()
                .filter(|recorded| recorded.reason == *reason)
                .map(|recorded| recorded.cause.clone());
            changed = kept.is_none();
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
            let after = Recorded { reason: reason.clone(), cause };
            let shown = shows_other(before.as_ref(), Some(&after));
            causes.held.insert(subject.clone(), after);
            shown
        });
        changed
    }

    /// The subject's work advanced past a failure `ended` accepts: its record
    /// is gone, so a later failure is new and logs again.
    pub(crate) fn cleared(&self, subject: &PendingSubject, ended: impl Fn(&PendingReason) -> bool) {
        self.published.send_if_modified(|causes| {
            let Some(recorded) = causes.held.get(subject).filter(|r| ended(&r.reason)) else {
                return false;
            };
            let shown = recorded.cause.is_some();
            causes.held.remove(subject);
            shown
        });
    }

    /// The operation advanced or finished: a later failure is new.
    pub(crate) fn cleared_operation(&self, id: OperationId) {
        self.retain(|subject| !operation(subject, id));
    }

    /// Forget the failures of operations that no longer exist.
    pub(crate) fn retain_operations(&self, live: &BTreeSet<OperationId>) {
        self.retain(|subject| match subject {
            PendingSubject::Operation { id, .. } => live.contains(id),
            _ => true,
        });
    }

    fn retain(&self, keep: impl Fn(&PendingSubject) -> bool) {
        self.published.send_if_modified(|causes| {
            let mut shown = false;
            causes.held.retain(|subject, recorded| {
                let kept = keep(subject);
                shown |= !kept && recorded.cause.is_some();
                kept
            });
            shown
        });
    }

    /// The error behind `record` now, as `PendingCauses::cause` finds it.
    pub(crate) fn cause(&self, record: &PendingRecord) -> Option<Cause> {
        self.published.borrow().cause(record)
    }

    /// `records`, each with the error behind it now.
    pub(crate) fn attach(&self, records: Vec<PendingRecord>) -> Vec<PendingRecord> {
        self.published.borrow().attach(records)
    }
}

fn operation(subject: &PendingSubject, operation: OperationId) -> bool {
    matches!(subject, PendingSubject::Operation { id, .. } if *id == operation)
}

#[cfg(test)]
#[path = "pending_cause_tests.rs"]
mod tests;
