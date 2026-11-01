//! Provider requests follow intentions committed with authenticated membership.
use crate::{
    history::History,
    sequence_sync::{SequenceSync, Session},
    OperationKind, PendingReason, PendingRecord, PendingSubject, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{
    NewOperation, OperationId, OperationUpdate, ProviderAccessChange, ProviderChanges,
};
use coven_format::{admin::MemberRole, MemberAccess};
use coven_foundation::id_source::MemberId;
use std::collections::BTreeSet;

impl History {
    pub(crate) fn owns_storage(&self, member: MemberId) -> bool {
        let Some(current) = self.state.members.get(&member).filter(|value| !value.removed) else {
            return false;
        };
        match &current.access {
            MemberAccess::S3AccessKey { .. } => current.role == MemberRole::Admin,
            access => self
                .recorded_access()
                .any(|(_, at, recorded)| at == 1 && recorded.same_credential(access)),
        }
    }

    /// Intentions after this state: an account stays shared while an active
    /// member reaches storage through it or an open invite share names it
    /// (§14.2). Only the owner's devices change sharing, so only they follow
    /// invite shares; a member's account is followed everywhere, so another
    /// admin's device shows the owner's work it waits for.
    pub(crate) fn provider_changes(&self, member: MemberId) -> Result<ProviderChanges, SyncError> {
        let owner = self.owns_storage(member);
        let mut changes = ProviderChanges::default();
        let mut accounts = BTreeSet::new();
        // Coven shares nothing on S3: the person deletes a key in the
        // provider's console, and coven records nothing (§14.3).
        let members = self.recorded_access().filter_map(|(_, _, access)| match access {
            MemberAccess::ProviderAccount(account) => Some(account.as_str()),
            MemberAccess::S3AccessKey { .. } => None,
        });
        let shares =
            self.invite_shares.values().filter(|_| owner).map(|share| share.account.as_str());
        for account in members.chain(shares) {
            let account = account.trim().to_ascii_lowercase();
            if !accounts.insert(account.clone()) {
                continue;
            }
            changes.accounts.push(ProviderAccessChange {
                operation: NewOperation {
                    kind: "provider-access".into(),
                    data: serde_json::to_vec(&account)?,
                    started_by: "coven".into(),
                    pauses_writes: false,
                },
                intended: self.account_intended(&account),
                account,
                // A member's access entry describes a grant already made,
                // and an invite share one its own device makes: a device
                // that first learns of an account never grants it itself.
                initially_granted: true,
                needs_invitation: false,
                owner,
            });
        }
        Ok(changes)
    }
}

impl SequenceSync {
    pub(crate) async fn resume_access(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        for record in self.database.operations().await? {
            if record.kind != "provider-access" || !self.operation_ready(&record, ring).await? {
                continue;
            }
            let scope = self.waits.begin_attempt(record.id);
            let result = scope.run(self.apply_access(session, ring, member, record.id)).await;
            if let Err(error) = result {
                self.record_operation_error(
                    record.id,
                    OperationKind::RevokeAccess,
                    crate::sequence_sync::OperationFailure::Held(error),
                    ring,
                )
                .await?;
                self.state.check()?;
            } else {
                self.waits.operation_advanced(record.id);
            }
        }
        Ok(())
    }

    pub(crate) async fn apply_access(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        id: OperationId,
    ) -> Result<(), SyncError> {
        loop {
            self.state.check()?;
            let record = self
                .database
                .operation(id)
                .await?
                .ok_or(coven_database::DbError::OperationChanged(id))?;
            let account: String = serde_json::from_slice(&record.data)?;
            let condition = |reason| {
                PendingRecord::new(
                    PendingSubject::Operation { id, kind: OperationKind::RevokeAccess },
                    reason,
                )
            };
            if !session.history.owns_storage(member.member()) {
                return Err(SyncError::Pending(Box::new(condition(PendingReason::PendingOwner {
                    account,
                }))));
            }
            let target = if record.last_step == 1 {
                self.database
                    .provider_access(account.clone())
                    .await?
                    .and_then(|state| state.request)
                    .and_then(|request| request.target)
                    .ok_or(coven_database::DbError::DamagedDatabase)?
            } else {
                self.database.start_provider_request::<PendingRecord>(account.clone(), id).await?
            };
            let invitation = if target {
                match self.storage.grant_access(&account).await? {
                    coven_storage::AccessGrant::Granted { invitation } => {
                        Some(invitation.encode()?)
                    }
                    coven_storage::AccessGrant::CreateAccessKey => {
                        return Err(coven_storage::StorageFailure::InvalidConfiguration
                            .with_source("account sharing cannot create an S3 key")
                            .into())
                    }
                }
            } else {
                let shares = self.storage.revoke_access(&account).await?;
                if !shares.is_empty() {
                    let pending = condition(PendingReason::AccessRemains {
                        account: account.clone(),
                        shares,
                    });
                    self.database
                        .fail_provider_request(
                            account,
                            OperationUpdate { id, previous: 1, last_step: 2, data: record.data },
                            pending.clone(),
                        )
                        .await?;
                    return Err(SyncError::Pending(Box::new(pending)));
                }
                None
            };
            self.read_forward(session, ring, member).await?;
            if self
                .database
                .complete_provider_request::<PendingRecord>(account, id, target, invitation)
                .await?
            {
                return Ok(());
            }
        }
    }
}
