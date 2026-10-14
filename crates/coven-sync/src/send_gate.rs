//! Session authority for every publisher, measured from read-only catch-up start.
use crate::{store_log_keys::ObservedCopy, SyncError};
use coven_foundation::{clock::ClockRef, files::DeviceResetReason};
use coven_storage::{ObjectPath, PublicationGate, StorageError, StorageFailure};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;

const FRESH_FOR: Duration = Duration::from_secs(300);

/// The binding established by a completed membership observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Binding {
    Device,
    Member,
    Absent,
    Retired,
}
struct Completed {
    started: Duration,
    finished: Duration,
    binding: Binding,
    copies: Vec<ObservedCopy>,
}
struct Session {
    starting: Option<Duration>,
    completed: Option<Completed>,
    bootstrap: Vec<Arc<Bootstrap>>,
    /// Snapshot and own-post evidence read once after opening or reconnecting.
    evidence: Option<crate::identity::SessionEvidence>,
    /// A copy that must reset never sends again in this process (§10).
    stopped: Option<DeviceResetReason>,
}
impl Session {
    fn closed(stopped: Option<DeviceResetReason>) -> Self {
        Self {
            starting: None,
            completed: None,
            bootstrap: Vec::new(),
            evidence: None,
            stopped,
        }
    }
}
struct Bootstrap {
    paths: BTreeSet<ObjectPath>,
    requires_membership: bool,
}

/// One installation's shared sending authority. Construct at the opening root
/// and inject the same owner into sync, file workers and provider connections.
/// A new owner starts closed, including after reopening the same database.
pub struct SendGate {
    clock: ClockRef,
    session: Mutex<Session>,
    changed: watch::Sender<u64>,
}
impl SendGate {
    /// Start without any reusable observation from a previous session.
    pub fn new(clock: ClockRef) -> Self {
        Self {
            clock,
            session: Mutex::new(Session::closed(None)),
            changed: watch::channel(0).0,
        }
    }
    pub(crate) fn changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
    fn notify(&self) {
        self.changed
            .send_modify(|version| *version = version.wrapping_add(1));
    }
    pub(crate) fn begin(&self) {
        self.session.lock().expect("send gate poisoned").starting = Some(self.clock.elapsed());
    }
    /// Only a complete key-copy observation can supply sending authority;
    /// an incomplete one leaves the previous completion to expire.
    pub(crate) fn complete(
        &self,
        binding: Binding,
        copies: Vec<ObservedCopy>,
    ) -> Result<(), SyncError> {
        let mut session = self.session.lock().expect("send gate poisoned");
        if let Some(reason) = session.stopped {
            return Err(SyncError::DeviceReset(reason));
        }
        let started = session
            .starting
            .take()
            .ok_or(StorageFailure::CatchUpRequired)?;
        let finished = self.clock.elapsed();
        if finished < started {
            return Err(StorageFailure::CatchUpRequired.into());
        }
        session.completed = Some(Completed {
            started,
            finished,
            binding,
            copies,
        });
        drop(session);
        self.notify();
        Ok(())
    }
    /// Forget observations when reconnecting or resetting this installation.
    pub fn invalidate(&self) {
        {
            let mut session = self.session.lock().expect("send gate poisoned");
            *session = Session::closed(session.stopped);
        }
        self.notify();
    }
    /// Close this installation for good: its copy is stale or replaced, and
    /// only a reset as a new device may send again (§10). The first reason wins.
    pub(crate) fn stop(&self, reason: DeviceResetReason) -> SyncError {
        let reason = {
            let mut session = self.session.lock().expect("send gate poisoned");
            let reason = *session.stopped.get_or_insert(reason);
            *session = Session::closed(Some(reason));
            reason
        };
        self.notify();
        SyncError::DeviceReset(reason)
    }
    pub(crate) fn stopped(&self) -> Option<DeviceResetReason> {
        self.session.lock().expect("send gate poisoned").stopped
    }
    pub(crate) fn session_evidence(&self) -> Option<crate::identity::SessionEvidence> {
        self.session.lock().expect("send gate poisoned").evidence
    }
    pub(crate) fn keep_session_evidence(&self, evidence: crate::identity::SessionEvidence) {
        self.session.lock().expect("send gate poisoned").evidence = Some(evidence);
    }
    fn fresh(&self, completed: &Completed) -> bool {
        let now = self.clock.elapsed();
        completed.started <= completed.finished
            && completed.finished <= now
            && now
                .checked_sub(completed.started)
                .is_some_and(|age| age < FRESH_FOR)
    }
    pub(crate) fn can_send(&self) -> bool {
        self.session
            .lock()
            .expect("send gate poisoned")
            .completed
            .as_ref()
            .is_some_and(|completed| completed.binding == Binding::Device && self.fresh(completed))
    }
    pub(crate) fn observation(&self) -> Result<Vec<ObservedCopy>, SyncError> {
        let session = self.session.lock().expect("send gate poisoned");
        let completed = session
            .completed
            .as_ref()
            .ok_or(StorageFailure::CatchUpRequired)?;
        if !matches!(completed.binding, Binding::Device | Binding::Member) || !self.fresh(completed)
        {
            return Err(StorageFailure::CatchUpRequired.into());
        }
        Ok(completed.copies.clone())
    }
    /// A confirmed local copy extends the checked copy set without another
    /// provider read or a newer catch-up time.
    pub(crate) fn published_copy(
        &self,
        store: coven_foundation::id_source::StoreId,
        copy: &coven_database::KeyUpload,
    ) -> Result<(), SyncError> {
        let mut session = self.session.lock().expect("send gate poisoned");
        let Some(Completed { copies, .. }) = &mut session.completed else {
            return Ok(());
        };
        let path = copy.path(store);
        if !copies.iter().any(|known| known.path == path) {
            copies.push(ObservedCopy {
                path,
                prefix: coven_format::sealed_key::SealedKey::decode(&copy.bytes)?
                    .prefix()
                    .clone(),
                bytes: copy.bytes.clone(),
            });
        }
        Ok(())
    }

    /// Authorize only the fresh sealed provider-check object required by §4.
    /// This scope supplies no permission for entries, app files or metadata.
    pub fn provider_check(&self, path: ObjectPath) -> PublicationScope<'_> {
        self.bootstrap([path].into(), false)
    }
    pub(crate) fn bootstrap(
        &self,
        paths: BTreeSet<ObjectPath>,
        requires_membership: bool,
    ) -> PublicationScope<'_> {
        let mut session = self.session.lock().expect("send gate poisoned");
        let bootstrap = Arc::new(Bootstrap {
            paths,
            requires_membership,
        });
        session.bootstrap.push(bootstrap.clone());
        PublicationScope {
            gate: self,
            bootstrap,
        }
    }
}
impl PublicationGate for SendGate {
    fn check(&self, path: &ObjectPath) -> Result<(), StorageError> {
        let session = self.session.lock().expect("send gate poisoned");
        if session.stopped.is_some() {
            return Err(StorageFailure::CatchUpRequired.into());
        }
        let current = session
            .completed
            .as_ref()
            .filter(|completed| self.fresh(completed));
        let allowed = current.is_some_and(|completed| completed.binding == Binding::Device)
            || session.bootstrap.iter().any(|bootstrap| {
                bootstrap.paths.contains(path)
                    && (!bootstrap.requires_membership
                        || current.is_some_and(|completed| {
                            matches!(completed.binding, Binding::Device | Binding::Member)
                        }))
            });
        if allowed {
            Ok(())
        } else {
            Err(StorageFailure::CatchUpRequired.into())
        }
    }
}
/// Temporary authority for exactly the objects required by setup or admission.
/// Dropping the scope ends that exception without granting ordinary authority.
pub struct PublicationScope<'a> {
    gate: &'a SendGate,
    bootstrap: Arc<Bootstrap>,
}
impl Drop for PublicationScope<'_> {
    fn drop(&mut self) {
        self.gate
            .session
            .lock()
            .expect("send gate poisoned")
            .bootstrap
            .retain(|active| !Arc::ptr_eq(active, &self.bootstrap));
    }
}

#[cfg(test)]
#[path = "send_gate_tests.rs"]
mod tests;
