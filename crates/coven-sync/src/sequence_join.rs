//! The joining device's wait for its approving device's binding (§13.2).

use super::{
    sequence_bootstrap::{Bootstrap, KeyScan, MemberBinding},
    SequenceSync,
};
use crate::{JoiningIdentity, SyncError};
use coven_foundation::progress::ProgressReport;
use coven_storage::{StorageFailure, StorageInvitation};
use std::time::{Duration, SystemTime};
use tokio::sync::watch;

/// When a waiting join checks for approval next (§13.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NextCheck {
    /// Now: a check is running.
    Checking,
    /// At this time, on the join's backoff; `check_now` ends the wait.
    At(SystemTime),
    /// The provider asked coven to slow down until this time; `check_now`
    /// can't end it.
    CoolingDown(SystemTime),
}

/// The joining member's outcome after checking storage (§13.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinOutcome {
    /// The request exists and no copy for it has opened.
    Waiting,
    /// Its copies opened and the store loaded, but the approving device's
    /// entry has not applied yet: approval has begun, and the entry follows
    /// the copies (§13.2).
    Approving,
    /// The approving device's applied entry binds this request's pair.
    Admitted,
    /// The request is gone and no binding admits it.
    Declined,
}

impl SequenceSync {
    /// Complete provider acceptance before submitting a join request.
    pub async fn accept_join(&self, invitation: &StorageInvitation) -> Result<(), SyncError> {
        Ok(self.storage.join(self.store(), invitation).await?)
    }

    /// Whether the invite still lets the request be sent first now: no
    /// first request goes out once the invite has ended by this device's
    /// clock (§13.2).
    pub fn join_open(&self, request: &JoiningIdentity) -> bool {
        request.open_at(self.clock.now())
    }

    /// Publish the recorded immutable request after provider acceptance.
    /// `false` when the invite ended before it was sent: its marker holds
    /// the request's path, and the join ends as declined (§13.2).
    pub async fn submit_join(&self, request: &JoiningIdentity) -> Result<bool, SyncError> {
        request.publish(self.storage.as_ref()).await
    }

    /// Inspect the slots after the invite for copies routed to the request's
    /// pair; once they open a store key, record that approval has begun,
    /// apply the sequence and look for the approving device's binding.
    /// Copies alone do not admit (§13.2). A check that finds no keys in
    /// custody reports nothing to `subscribe_progress` while it scans for the
    /// copies: the scan is part of the wait. Once they open, it reports the
    /// scan, then loading's steps, a resumed join's too, which may have
    /// applied slots before it stopped (E10).
    pub async fn join_outcome(
        &self,
        request: &mut JoiningIdentity,
    ) -> Result<JoinOutcome, SyncError> {
        let mut session = self.session.lock().await;
        let member = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        if member.keys().public_keys() != request.identity().keys().public_keys() {
            return Err(SyncError::InvitationChanged);
        }
        self.scan_after(&mut session, request.read_through());
        let held = self.keys.read()?;
        // The keys first open in a check whose custody held none: a new
        // installation's, or a resumed join's, which may have applied slots
        // before it stopped. Each call's custody starts empty (§13.2).
        let opening = held.is_none();
        // Such a check scans for this member's copies, and once they open
        // the store, loads it, or the rest of it, reporting each step as
        // `unlock` does (§13.4). The scan counts aside until then.
        let mut progress = opening.then(|| ProgressReport::of(&self.progress, self.clock.clone()));
        let (counted, scanned) = watch::channel(None);
        let mut counting = ProgressReport::new(counted, self.clock.clone());
        let mut scan = KeyScan::new(&mut counting);
        let mut ring = match held {
            Some(ring) => ring,
            None => match self.bootstrap_keys(&mut session, &member, Some(&mut scan)).await? {
                Bootstrap::Keys(ring) => ring,
                // The joining pair was bound and then replaced (§12.3).
                Bootstrap::Replaced => {
                    return Err(SyncError::MemberKeysReplaced { member: member.member() })
                }
                Bootstrap::Missing(_) | Bootstrap::Empty => {
                    if request.pending(self.storage.as_ref()).await? {
                        return Ok(JoinOutcome::Waiting);
                    }
                    // Approval may have raced the ending miss. Scan again
                    // after observing request deletion; bootstrap follows any
                    // floor that advances during this final scan as well.
                    match self.bootstrap_keys(&mut session, &member, Some(&mut scan)).await? {
                        Bootstrap::Keys(ring) => ring,
                        Bootstrap::Replaced => {
                            return Err(SyncError::MemberKeysReplaced { member: member.member() })
                        }
                        Bootstrap::Missing(_) | Bootstrap::Empty => {
                            return Ok(JoinOutcome::Declined)
                        }
                    }
                }
            },
        };
        // Approval has begun. The join keeps that before it reports loading,
        // so it never reports waiting for approval again, even resumed
        // (E10).
        if !request.opened() {
            request.record_opened()?;
        }
        // The scan that found the copies is loading's first step, reported
        // whole now that it has opened them.
        if let (Some(progress), Some(scanned)) = (progress.as_mut(), scanned.borrow().clone()) {
            progress.begin(scanned.step, scanned.total);
            progress.done(scanned.done);
        }
        self.restore_history(&mut session, &mut ring, &member).await?;
        self.read_forward_reporting(&mut session, &mut ring, &member, progress.as_mut()).await?;
        if self.admitted(&session, &member, request) {
            return Ok(JoinOutcome::Admitted);
        }
        if request.pending(self.storage.as_ref()).await? {
            return Ok(JoinOutcome::Approving);
        }
        self.read_forward(&mut session, &mut ring, &member).await?;
        Ok(if self.admitted(&session, &member, request) {
            JoinOutcome::Admitted
        } else {
            JoinOutcome::Declined
        })
    }

    /// Wait until the next approval poll is due: its persisted delay, armed
    /// again after reopening, and any provider cooldown (§6.7). `report`
    /// receives when the next check is due each time the wait begins or
    /// changes. A change on `check_now` ends the join's own wait, keeping its
    /// failure count; a provider cooldown still holds (§13.2).
    pub async fn join_poll_due(
        &self,
        check_now: &mut watch::Receiver<u64>,
        report: impl Fn(NextCheck),
    ) -> Result<(), SyncError> {
        let subject = coven_database::RetrySubject::Join;
        loop {
            let persisted = self.database.retry_wait(subject).await?;
            if self.waits.due(subject, persisted.as_ref()) {
                return Ok(());
            }
            report(self.next_check(subject)?);
            let wake = self
                .waits
                .next_wake(|candidate| *candidate == subject)
                .or_else(|| self.waits.cooldown_wake())
                .ok_or_else(|| {
                    StorageFailure::File.with_source("the approval poll's delay was not persisted")
                })?;
            let asked = async {
                if check_now.changed().await.is_err() {
                    // Nobody can ask any more; only the wait ends it.
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                () = wake => (),
                () = asked => self.waits.end_wait(subject).await?,
            }
        }
    }

    /// When a poll that is not due yet will run: at the join's own deadline,
    /// or when a provider cooldown that ends no earlier is over.
    fn next_check(&self, subject: coven_database::RetrySubject) -> Result<NextCheck, SyncError> {
        let at = |wait: Duration| {
            self.clock.now().checked_add(wait).ok_or_else(|| {
                StorageFailure::InvalidConfiguration
                    .with_source("the clock cannot represent the next approval check")
            })
        };
        Ok(match (self.waits.remaining(subject), self.waits.cooldown()) {
            (Some(wait), Some(cooldown)) if cooldown < wait => NextCheck::At(at(wait)?),
            (_, Some(cooldown)) => NextCheck::CoolingDown(at(cooldown)?),
            (Some(wait), None) => NextCheck::At(at(wait)?),
            (None, None) => {
                return Err(StorageFailure::File
                    .with_source("the approval poll's delay was not persisted")
                    .into())
            }
        })
    }

    /// A poll that found the request still waiting persists its next delay
    /// before waiting, continuing the backoff across reopening.
    pub async fn join_poll_waiting(&self) -> Result<(), SyncError> {
        let subject = coven_database::RetrySubject::Join;
        let previous = self.database.retry_wait(subject).await?;
        self.waits.fail(subject, previous.as_ref()).await
    }

    /// The outcome is known: approval polling ends.
    pub async fn join_poll_settled(&self) -> Result<(), SyncError> {
        let subject = coven_database::RetrySubject::Join;
        if self.database.retry_wait(subject).await?.is_some() {
            self.database.clear_retry(subject).await?;
        }
        self.waits.forget(subject);
        Ok(())
    }

    fn admitted(
        &self,
        session: &super::Session,
        member: &coven_crypto::MemberIdentity,
        request: &JoiningIdentity,
    ) -> bool {
        matches!(self.admission(session, member),
            MemberBinding::Bound { device } if device == request.approver())
    }
}

#[cfg(test)]
#[path = "sequence_join_tests.rs"]
mod tests;
