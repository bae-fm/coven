//! The joining device's wait for its approving device's binding (§13.2).

use super::{
    sequence_bootstrap::{Bootstrap, MemberBinding},
    SequenceSync,
};
use crate::{JoiningIdentity, SyncError};
use coven_storage::{StorageFailure, StorageInvitation};

/// The joining member's outcome after checking storage (§13.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinOutcome {
    /// The request exists, or copies arrived before the binding entry.
    Waiting,
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

    /// Publish the recorded immutable request after provider acceptance.
    pub async fn submit_join(&self, request: &JoiningIdentity) -> Result<(), SyncError> {
        request.publish(self.storage.as_ref()).await
    }

    /// Inspect the slots after the invite for copies routed to the request's
    /// pair; once they open a store key, apply the sequence and look for the
    /// approving device's binding. Copies alone do not admit (§13.2).
    pub async fn join_outcome(&self, request: &JoiningIdentity) -> Result<JoinOutcome, SyncError> {
        let mut session = self.session.lock().await;
        let member = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        if member.keys().public_keys() != request.identity().keys().public_keys() {
            return Err(SyncError::InvitationChanged);
        }
        self.scan_after(&mut session, request.read_through());
        let mut ring = match self.keys.read()? {
            Some(ring) => ring,
            None => match self.bootstrap_keys(&mut session, &member).await? {
                Bootstrap::Keys(ring) => ring,
                Bootstrap::Missing(_) | Bootstrap::Empty => {
                    if request.pending(self.storage.as_ref()).await? {
                        return Ok(JoinOutcome::Waiting);
                    }
                    // Approval may have raced the scan's ending miss: read
                    // again before deciding that the request was declined.
                    match self.bootstrap_keys(&mut session, &member).await? {
                        Bootstrap::Keys(ring) => ring,
                        Bootstrap::Missing(_) | Bootstrap::Empty => {
                            return Ok(JoinOutcome::Declined)
                        }
                    }
                }
            },
        };
        self.read_forward(&mut session, &mut ring, &member).await?;
        if self.admitted(&session, &member, request) {
            return Ok(JoinOutcome::Admitted);
        }
        if request.pending(self.storage.as_ref()).await? {
            return Ok(JoinOutcome::Waiting);
        }
        self.read_forward(&mut session, &mut ring, &member).await?;
        Ok(if self.admitted(&session, &member, request) {
            JoinOutcome::Admitted
        } else {
            JoinOutcome::Declined
        })
    }

    /// Wait until the next approval poll is due: its persisted delay, armed
    /// again after reopening, and any provider cooldown (§6.7).
    pub async fn join_poll_due(&self) -> Result<(), SyncError> {
        let subject = coven_database::RetrySubject::Join;
        loop {
            let persisted = self.database.retry_wait(subject).await?;
            if self.waits.due(subject, persisted.as_ref()) {
                return Ok(());
            }
            let wake = self
                .waits
                .next_wake(|candidate| *candidate == subject)
                .or_else(|| self.waits.cooldown_wake())
                .ok_or_else(|| {
                    StorageFailure::File.with_source("the approval poll's delay was not persisted")
                })?;
            wake.await;
        }
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
