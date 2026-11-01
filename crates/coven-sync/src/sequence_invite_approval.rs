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

    /// Record a kept replacement's new S3 key for its member: resume its
    /// unfinished claim, or start it (§14.3).
    pub(super) async fn record_invite_access(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        change: StoreChange,
    ) -> Result<(), SyncError> {
        // The exact requested entry identifies this invite's unfinished claim.
        if let Some(id) = self.pending_change(&change, INVITE).await? {
            crate::operation_outcome::into_slot(
                self.run_administration(session, ring, member, id).await?,
            )?;
            return Ok(());
        }
        crate::operation_outcome::into_slot(
            self.start_request(session, ring, member, Request::Change(change), INVITE).await?,
        )?;
        Ok(())
    }

    /// Whether the applied history binds the pair `change` binds, by this
    /// device's claim.
    pub(super) fn binds(&self, session: &Session, change: &StoreChange) -> bool {
        let (StoreChange::AddMember { member, keys, .. }
        | StoreChange::ReplaceMemberKeys { member, keys, .. }) = change
        else {
            unreachable!("an approval binds a pair")
        };
        session.history.bindings.iter().any(|binding| {
            binding.member == *member && binding.keys == *keys && binding.device == self.device
        })
    }
}

#[cfg(test)]
#[path = "sequence_invite_approval_tests.rs"]
mod tests;
