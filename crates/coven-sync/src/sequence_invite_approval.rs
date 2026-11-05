//! The operations an invite claims its entries through: it starts each in
//! the step that names it, owns their retries and rejection cleanup, and
//! settles a claim it no longer needs (§11, §13.2).
use super::*;
use crate::sequence_sync::sequence_operations::{
    OperationProgress, OperationStep, Request, SequenceOperation,
};
use coven_database::{ClaimSettlement, ClaimWork};

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

    /// Journal `change` as an operation an invite starts, once the
    /// caught-up history allows it, in one transaction with the invite's
    /// step that names it: `parent` builds that step from its id. The invite
    /// runs it, and ends it in the transaction that moves the invite past it
    /// (`finish`), so a restart always finds it (§13.2).
    pub(crate) async fn start_child(
        &self,
        session: &Session,
        member: &MemberIdentity,
        change: StoreChange,
        parent: impl FnOnce(OperationId) -> Result<coven_database::OperationUpdate, coven_database::DbError>
            + Send
            + 'static,
    ) -> Result<Child, SyncError> {
        let request = Request::Change(change);
        match self.validate_request(session, member, &request) {
            Err(reason) => return Ok(Child::Decided(OperationOutcome::Rejected(reason))),
            Ok(false) => return Ok(Child::Decided(OperationOutcome::Satisfied)),
            Ok(true) => (),
        }
        let work = SequenceOperation {
            request,
            progress: OperationProgress::Running(OperationStep::Preparing),
        };
        let child = work.journaled(operation_kind::INVITE)?;
        Ok(Child::Journaled(self.database.start_operation_advancing(child, parent).await?))
    }

    /// Settle the open claim of `id`, an operation an invite started whose
    /// entry the invite no longer needs, and end that claim without
    /// claiming again. An attempt not yet read is read, and one whose slot is
    /// still empty is sent again with its exact bytes, since a request in
    /// flight may still land (§5.1). A landed claim leaves `id` at its slot,
    /// its entry applied; one that lost its slot ends with no entry. Without
    /// an open claim, `id` has nothing in flight.
    pub(crate) async fn settle_unneeded_claim(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        id: OperationId,
    ) -> Result<(), SyncError> {
        let Some(claim) = self.database.current_claim().await? else { return Ok(()) };
        if claim.work != ClaimWork::Operation(id) {
            return Ok(());
        }
        match self.settle_claim(session, ring, member, &claim).await? {
            ClaimSettlement::Landed => {
                let record = self
                    .database
                    .operation(id)
                    .await?
                    .ok_or(coven_database::DbError::OperationChanged(id))?;
                let SequenceOperation { request, progress } = serde_json::from_slice(&record.data)?;
                let slot = claim.attempts.last().expect("attempt").slot_number().expect("slot").0;
                let step = match progress {
                    OperationProgress::Running(OperationStep::Claiming) => {
                        OperationStep::Applied(slot)
                    }
                    OperationProgress::Running(OperationStep::Copy(_)) => OperationStep::Preparing,
                    _ => {
                        return Err(coven_database::DbError::Invariant {
                            what: "an unneeded claim is a change's entry or key copy",
                        }
                        .into())
                    }
                };
                let update =
                    SequenceOperation { request, progress: OperationProgress::Running(step) }
                        .update(&record)?;
                self.database.finish_claim(Some(update)).await?;
            }
            ClaimSettlement::NotLanded => self.database.finish_claim(None).await?,
            ClaimSettlement::DeviceIdInUse => {
                return Err(self
                    .stop_copy(crate::DeviceIdEvidence::UnattemptedSlot {
                        slot: claim.attempts.last().expect("attempt").slot_number().expect("slot"),
                    })
                    .await)
            }
            _ => unreachable!("settled create"),
        }
        Ok(())
    }
}

/// What starting an invite's operation came to (`start_child`).
pub(crate) enum Child {
    /// Journaled with the invite's step that names it.
    Journaled(OperationId),
    /// Nothing journaled: the caught-up history refuses the entry, or has it
    /// already.
    Decided(OperationOutcome),
}

#[cfg(test)]
#[path = "sequence_invite_approval_tests.rs"]
mod tests;
