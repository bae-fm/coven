//! The invite owns its claims' retries and rejection cleanup (§11).
use super::*;
use crate::sequence_sync::sequence_operations::Request;

impl SequenceSync {
    pub(super) async fn run_invite(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        id: OperationId,
    ) -> Result<InviteProgress, SyncError> {
        let scope = self.waits.begin_attempt(id);
        let mut step = None;
        let result = scope.run(self.advance_invite(session, ring, member, id, &mut step)).await;
        let pending = match &result {
            Ok(InviteProgress::Waiting(reason)) => {
                Some(SyncError::Pending(Box::new(PendingRecord::new(
                    PendingSubject::Operation { id, kind: OperationKind::Invite },
                    reason.clone(),
                ))))
            }
            _ => None,
        };
        if let Some(error) = result.as_ref().err().or(pending.as_ref()) {
            // An attempt that never read the invite's row failed reading the
            // journal: its error goes to the caller, and no step to record
            // it against was read.
            if let Some(step) = step {
                self.record_invite_error(
                    id,
                    step,
                    crate::sequence_sync::OperationFailure::Returned(error),
                    ring,
                )
                .await?;
            }
        } else {
            self.waits.operation_advanced(id);
        }
        result
    }

    /// Claim an entry the invite owns: its share, its share's end, or a
    /// kept replacement's new S3 key for its member (§13.2, §14.3). Resume
    /// the unfinished claim, or start it. Returns the operation, left for the
    /// invite to end, and its outcome; no operation when the caught-up
    /// history already has the entry or refuses it.
    pub(super) async fn claim_for_invite(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        change: StoreChange,
    ) -> Result<(Option<OperationId>, OperationOutcome), SyncError> {
        // The exact requested entry identifies this invite's unfinished claim.
        if let Some(id) = self.pending_change(&change, operation_kind::INVITE).await? {
            return Ok((Some(id), self.run_administration(session, ring, member, id).await?));
        }
        self.start_child(session, ring, member, Request::Change(change), operation_kind::INVITE)
            .await
    }
}

#[cfg(test)]
#[path = "sequence_invite_approval_tests.rs"]
mod tests;
