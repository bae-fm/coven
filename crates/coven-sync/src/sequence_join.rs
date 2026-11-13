//! The joining device's wait for its approving device's admission (§13.2).

use super::{
    sequence_bootstrap::{Bootstrap, KeyScan},
    SequenceSync,
};
use crate::{HeldIntervals, JoiningIdentity, SyncError};
use coven_foundation::progress::ProgressReport;
use coven_storage::{CloudProvider, StorageFailure, StorageInvitation};
use std::time::Duration;
use tokio::sync::watch;

/// The joining member's outcome after checking storage (§13.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinOutcome {
    /// The request exists and no copy for it has opened.
    Waiting,
    /// This request's copies opened, in the admission that carries them:
    /// the join publishes the store, whose first load applies that admission
    /// (§13.2).
    Admitted,
    /// The request is gone and no copy for it has opened.
    Declined,
}

impl SequenceSync {
    /// Complete provider acceptance before submitting a join request.
    pub async fn accept_join(&self, invitation: &StorageInvitation) -> Result<(), SyncError> {
        Ok(self.storage.join(self.store(), invitation).await?)
    }

    /// Take back `accept_join`'s provider acceptance, for a join cancelled
    /// before its request was sent; one already taken back is done (§13.2).
    pub async fn leave_join(&self, invitation: &StorageInvitation) -> Result<(), SyncError> {
        Ok(self.storage.leave(invitation).await?)
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
    /// `false` too when the join's access to the location is gone
    /// (`access_lost`).
    pub async fn submit_join(&self, request: &JoiningIdentity) -> Result<bool, SyncError> {
        self.unless_access_lost(request.publish(self.storage.as_ref()).await)
    }

    /// Send the request of a join that recorded its send in an earlier call
    /// and may have stopped before the create, while the invite is open.
    /// `false` when the join's access to the location is gone
    /// (`access_lost`).
    pub async fn resend_join(&self, request: &JoiningIdentity) -> Result<bool, SyncError> {
        if self.join_open(request) {
            return self
                .unless_access_lost(request.resend(self.storage.as_ref()).await.map(|()| true));
        }
        Ok(true)
    }

    /// Whether `error` says the access a join was shown to hold, to the
    /// invite's own location, is gone: the provider refused the account or
    /// found the location missing, or an S3 key the invite carries was
    /// deleted or disabled. A sign-in the provider rejects is no such answer:
    /// the person signs in again (§13.2).
    pub fn access_lost(&self, error: &SyncError) -> bool {
        let SyncError::Storage(error) = error else { return false };
        let failure = error.failure();
        failure.access_ended()
            || (self.storage.config().provider() == CloudProvider::S3
                && failure == StorageFailure::Authentication)
    }

    /// `result`, with a lost access as the join's end: `false`.
    fn unless_access_lost(&self, result: Result<bool, SyncError>) -> Result<bool, SyncError> {
        match result {
            Err(error) if self.access_lost(&error) => Ok(false),
            result => result,
        }
    }

    /// Inspect the slots after the invite for copies routed to the request's
    /// pair; once they open a store key, in the admission that carries them,
    /// record that and keep the keys in custody: the join has found its
    /// admission and publishes the store. Copies alone do not admit: the
    /// published store's first load applies the admission, and stops the
    /// store if its history refused it (§13.2). A check reports nothing to
    /// `subscribe_progress` while it scans for the copies: the scan is part
    /// of the wait. Once they open, it reports the scan as loading's first
    /// step (E10).
    ///
    /// A check that could not reach the shared location before the keys open
    /// has lost the join's access, and ends it as an absent request does
    /// (`access_lost`, `JoinScan.outcome`); one of a join whose keys already
    /// opened is the published store's lost access, which is no ending.
    pub async fn join_outcome(
        &self,
        request: &mut JoiningIdentity,
    ) -> Result<JoinOutcome, SyncError> {
        let opened = request.opened();
        match self.check_join(request).await {
            Err(error) if !opened && self.access_lost(&error) => Ok(JoinOutcome::Declined),
            result => result,
        }
    }

    async fn check_join(&self, request: &mut JoiningIdentity) -> Result<JoinOutcome, SyncError> {
        let mut session = self.session.lock().await;
        let member = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        if member.keys().public_keys() != request.identity().keys().public_keys() {
            return Err(SyncError::InvitationChanged);
        }
        self.scan_after(&mut session, request.read_through())?;
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
        let ring = match held {
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
        // The admission carrying its copies is found. The store the join
        // publishes keeps the keys, and loads with them; they are kept
        // before the join records the admission found, so a join whose
        // custody holds them has found it in this call (§13.2).
        self.keys.persist(&ring)?;
        // The join keeps that before it reports loading, so it never reports
        // waiting for approval again, even resumed (E10).
        if !request.opened() {
            request.record_opened()?;
        }
        // The scan that found the copies is loading's first step, reported
        // whole now that it has opened them.
        if let (Some(progress), Some(scanned)) = (progress.as_mut(), scanned.borrow().clone()) {
            progress.begin(scanned.step, scanned.total);
            progress.done(scanned.done);
        }
        Ok(JoinOutcome::Admitted)
    }
}

/// The wait between an approval's checks, which a join and a pairing keep:
/// its persisted delay, armed again after reopening, and any provider
/// cooldown (§6.7).
#[derive(Clone)]
pub struct ApprovalWaits {
    database: coven_database::Database,
    waits: std::sync::Arc<crate::RetryWaits>,
    clock: coven_foundation::clock::ClockRef,
}

impl ApprovalWaits {
    /// The waits of an installation with this database, waits and clock.
    pub fn new(
        database: coven_database::Database,
        waits: std::sync::Arc<crate::RetryWaits>,
        clock: coven_foundation::clock::ClockRef,
    ) -> Self {
        Self { database, waits, clock }
    }

    /// A poll that found the request still waiting persists its next delay
    /// before waiting, continuing the backoff across reopening.
    pub async fn waiting(&self) -> Result<(), SyncError> {
        let subject = coven_database::RetrySubject::Join;
        let previous = self.database.retry_wait(subject).await?;
        self.waits.fail(subject, previous.as_ref()).await
    }

    /// The outcome is known: approval polling ends.
    pub async fn settled(&self) -> Result<(), SyncError> {
        let subject = coven_database::RetrySubject::Join;
        if self.database.retry_wait(subject).await?.is_some() {
            self.database.clear_retry(subject).await?;
        }
        self.waits.forget(subject);
        Ok(())
    }
}

/// An approval's wait for its next poll: its persisted delay, armed again
/// after reopening, any provider cooldown (§6.7), and the intervals the app
/// holds (§13.2).
pub struct JoinPoll {
    waits: ApprovalWaits,
    held: HeldIntervals,
    parks: crate::Parks,
}

impl JoinPoll {
    /// The wait on `waits`, honoring the intervals `held` reads and telling
    /// `parks` where it parks.
    pub fn new(waits: ApprovalWaits, held: HeldIntervals, parks: crate::Parks) -> Self {
        Self { waits, held, parks }
    }

    /// Wait until the next approval poll is due. While a guard is held, the
    /// wait also ends once the shortest interval held has passed since the
    /// last check ended, keeping its failure count; a provider cooldown still
    /// holds (§13.2).
    pub async fn due(&mut self) -> Result<(), SyncError> {
        let sync = &self.waits;
        let subject = coven_database::RetrySubject::Join;
        loop {
            let persisted = sync.database.retry_wait(subject).await?;
            if sync.waits.due(subject, persisted.as_ref()) {
                return Ok(());
            }
            let wake = sync
                .waits
                .next_wake(|candidate| *candidate == subject)
                .or_else(|| sync.waits.cooldown_wake())
                .ok_or_else(|| {
                    StorageFailure::File.with_source("the approval poll's delay was not persisted")
                })?;
            let asked = Self::held_check(sync, persisted.as_ref(), self.held.shortest());
            tokio::select! {
                biased;
                never = self.parks.polling() => match never {},
                () = wake => (),
                asked = asked => {
                    asked?;
                    sync.waits.end_wait(subject).await?
                }
                () = self.held.changed() => (),
                never = self.parks.parked() => match never {},
            }
        }
    }

    /// Resolves when a guard's interval has passed since the last check ended,
    /// which the persisted wait's start, `T - W`, marks; never when no guard
    /// is held or the wait has ended already.
    async fn held_check(
        sync: &ApprovalWaits,
        persisted: Option<&coven_database::RetryWait>,
        shortest: Option<Duration>,
    ) -> Result<(), SyncError> {
        let (Some(wait), Some(interval)) =
            (persisted.filter(|wait| !wait.wait.is_zero()), shortest)
        else {
            return std::future::pending().await;
        };
        let checked = wait.not_before.checked_sub(wait.wait);
        let at = checked.and_then(|checked| checked.checked_add(interval)).ok_or_else(|| {
            StorageFailure::InvalidConfiguration
                .with_source("the clock cannot represent the next approval check")
        })?;
        sync.clock
            .sleep_until(at)
            .await
            .map_err(|error| StorageFailure::InvalidConfiguration.with_source(error).into())
    }
}

#[cfg(test)]
#[path = "sequence_join_tests.rs"]
mod tests;
