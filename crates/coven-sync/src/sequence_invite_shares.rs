//! The share an invite grants, recorded in history so the owner's other
//! devices take it back once the invite ends or its device is gone for
//! good (§13.2, §14.2).

use super::*;

impl SequenceSync {
    /// Journal the record of the share an account invite grants, before
    /// granting it, with the invite's step that names it (`Sharing`): the
    /// owner's other devices then know of the share, and take it back once
    /// the invite ends or this device is gone for good. Nothing is
    /// journaled when the caught-up history has the share already, or
    /// refuses it: the refusal is the reason the invite waits on, and the
    /// app cancels the invite (§13.2).
    pub(super) async fn start_invite_share(
        &self,
        session: &Session,
        member: &MemberIdentity,
        record: &OperationRecord,
        work: &InviteWork,
        account: &str,
    ) -> Result<Child, SyncError> {
        let change = StoreChange::AddInviteShare { invite: work.id, account: account.to_owned() };
        self.start_child(
            session,
            member,
            change,
            InviteWork::naming(record, |share| InviteState::Sharing { share }),
        )
        .await
    }

    /// Journal the end of the share this device recorded for an ended
    /// invite, with the invite's step that names it, and return it.
    /// Applying it takes the account back on every owner device, unless an
    /// active member or another open share still uses it (§14.2). None
    /// when there is no share to end: an invite that ended before its share
    /// was recorded has none, and one a reset carried was recorded under the
    /// old device, whose stop ended it (§19.3). None too when the caught-up
    /// history refuses the end, which is logged.
    pub(super) async fn start_share_end(
        &self,
        session: &Session,
        member: &MemberIdentity,
        record: &OperationRecord,
        work: &InviteWork,
        share: Option<OperationId>,
    ) -> Result<Option<OperationId>, SyncError> {
        let open = session
            .history
            .invite_shares
            .get(&work.id)
            .is_some_and(|share| !share.ended && share.device == self.device);
        if !open {
            return Ok(None);
        }
        let change = StoreChange::EndInviteShare { invite: work.id };
        let settled = move |end| InviteState::Settled { share, end: Some(end) };
        Ok(
            match self
                .start_child(session, member, change, InviteWork::naming(record, settled))
                .await?
            {
                Child::Journaled(end) => Some(end),
                Child::Decided(outcome) => {
                    if let OperationOutcome::Rejected(reason) = &outcome {
                        tracing::warn!(invite = ?work.id, ?reason, "an invite share's end was refused");
                    }
                    None
                }
            },
        )
    }
}

#[cfg(test)]
#[path = "sequence_invite_shares_tests.rs"]
mod tests;
