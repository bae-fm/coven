//! The invite owns its binding claim's retries and rejection cleanup (§18).
use super::*;
use crate::sequence_sync::sequence_operations::{Request, SequenceOperation};

impl SequenceSync {
    pub(super) async fn run_invite(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        id: OperationId,
    ) -> Result<InviteProgress, SyncError> {
        let scope = self.waits.begin_attempt(id);
        let result = scope
            .run(self.advance_invite(session, ring, member, id))
            .await;
        let pending = match &result {
            Ok(InviteProgress::Waiting(reason)) => {
                Some(SyncError::Pending(Box::new(PendingRecord {
                    subject: PendingSubject::Operation {
                        id,
                        kind: OperationKind::Invite,
                    },
                    reason: reason.clone(),
                    reported_by: self.device,
                })))
            }
            _ => None,
        };
        if let Some(error) = result.as_ref().err().or(pending.as_ref()) {
            if self.database.operation(id).await?.is_some() {
                self.record_operation_error(id, OperationKind::Invite, error, ring)
                    .await?;
            }
        } else {
            self.waits.clear_evidence(id);
            self.waits
                .forget(coven_database::RetrySubject::Operation(id));
        }
        result
    }

    pub(super) async fn approve_invite_change(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        change: StoreChange,
    ) -> Result<(), SyncError> {
        // The exact requested entry identifies this invite's unfinished claim.
        // Resume it before testing completion: a landed claim still needs its
        // journal settled, and a rejection must reach its parent's undo.
        for record in self.database.operations().await? {
            if record.kind == "sequence" && record.started_by == INVITE {
                let work: SequenceOperation = serde_json::from_slice(&record.data)?;
                if matches!(&work.request, Request::Change(existing) if existing == &change) {
                    self.run_administration(session, ring, member, record.id)
                        .await?;
                    return Ok(());
                }
            }
        }
        let binding = match &change {
            StoreChange::AddMember { member, keys, .. }
            | StoreChange::ReplaceMemberKeys { member, keys, .. } => Some((member, keys)),
            _ => None,
        };
        if binding.is_some_and(|(member, keys)| {
            session.history.bindings.iter().any(|binding| {
                binding.member == *member && binding.keys == *keys && binding.device == self.device
            })
        }) {
            return Ok(());
        }
        self.start_request(session, ring, member, Request::Change(change), INVITE)
            .await?;
        Ok(())
    }
}
