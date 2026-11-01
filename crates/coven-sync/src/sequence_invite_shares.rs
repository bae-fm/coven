//! The share an invite grants, recorded in history so the owner's other
//! devices take it back once the invite ends or its device is gone for
//! good (§13.2, §14.2).

use super::*;

impl SequenceSync {
    /// Record the share an account invite grants, before granting it: the
    /// owner's other devices then know of the share, and take it back once
    /// the invite ends or this device is gone for good. Recording it again
    /// is satisfied, so a pass that stopped after the entry landed goes on.
    /// The reason the invite waits on, when the caught-up history refuses
    /// the entry: the app cancels the invite (§13.2).
    pub(super) async fn record_invite_share(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        work: &InviteWork,
    ) -> Result<Option<PendingReason>, SyncError> {
        let Some(account) = work.account() else { return Ok(None) };
        let change = StoreChange::AddInviteShare { invite: work.id, account: account.to_owned() };
        let (claim, outcome) = self.claim_for_invite(session, ring, member, change).await?;
        if let Some(claim) = claim {
            self.database.finish_operation_with_outcome(claim, outcome.clone()).await?;
        }
        Ok(match outcome {
            OperationOutcome::Rejected(reason) => {
                Some(self.error_reason(&SyncError::Rejected(reason)))
            }
            OperationOutcome::Landed(_) | OperationOutcome::Satisfied => None,
            OperationOutcome::Stopped(_) => {
                return Err(coven_database::DbError::Invariant {
                    what: "a stop for good ends the invite with its claims",
                }
                .into())
            }
        })
    }

    /// Record the end of the share this device recorded for an ended
    /// invite. Applying it takes the account back on every owner device,
    /// unless an active member or another open share still uses it (§14.2).
    /// An invite that ended before its share was recorded has none to end;
    /// one a reset carried was recorded under the old device, whose stop
    /// ended it (§19.3).
    pub(super) async fn end_invite_share(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        work: &InviteWork,
    ) -> Result<(), SyncError> {
        let open = session
            .history
            .invite_shares
            .get(&work.id)
            .is_some_and(|share| !share.ended && share.device == self.device);
        if !open {
            return Ok(());
        }
        let change = StoreChange::EndInviteShare { invite: work.id };
        let (claim, outcome) = self.claim_for_invite(session, ring, member, change).await?;
        if let OperationOutcome::Rejected(reason) = &outcome {
            tracing::warn!(invite = ?work.id, ?reason, "an invite share's end was refused");
        }
        if let Some(claim) = claim {
            self.database.finish_operation_with_outcome(claim, outcome).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_invite_shares_tests.rs"]
mod tests;
