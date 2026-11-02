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
    /// The access the store was created with (slot 1). Its kind is the
    /// store's: an account holds the store, or each member has an S3 key
    /// (§10.3).
    pub(crate) fn store_access(&self) -> Option<&MemberAccess> {
        self.recorded_access().find_map(|(_, at, access)| (at == 1).then_some(access))
    }

    /// Whether `member` administers the storage: on a store an account
    /// holds, the member whose account it is; on S3, any admin (§14.2).
    pub(crate) fn owns_storage(&self, member: MemberId) -> bool {
        match self.store_access() {
            Some(MemberAccess::ProviderAccount(_)) => self.holds_store(member),
            Some(MemberAccess::S3AccessKey { .. }) => crate::effects::member(&self.state, &member)
                .is_some_and(|current| current.role == MemberRole::Admin),
            None => false,
        }
    }

    /// Whether `member` is active and reaches storage through the account
    /// the store was created in, the account that holds it (§20.9). No
    /// account holds an S3 store.
    pub(crate) fn holds_store(&self, member: MemberId) -> bool {
        let Some(created @ MemberAccess::ProviderAccount(_)) = self.store_access() else {
            return false;
        };
        crate::effects::member(&self.state, &member)
            .is_some_and(|current| created.same_credential(&current.access))
    }

    /// Intentions after this state: an account stays shared while an active
    /// member reaches storage through it or an open invite share names it
    /// (§14.2). Only the owner's devices change sharing, so only they record
    /// the work: another device could neither do it nor see it done, and a
    /// wait it showed would never end.
    pub(crate) fn provider_changes(&self, member: MemberId) -> Result<ProviderChanges, SyncError> {
        let mut changes = ProviderChanges::default();
        if !self.owns_storage(member) {
            return Ok(changes);
        }
        let mut accounts = BTreeSet::new();
        // Coven shares nothing on S3: the person deletes a key in the
        // provider's console, and coven records nothing (§14.3).
        let members = self.recorded_access().filter_map(|(_, _, access)| match access {
            MemberAccess::ProviderAccount(account) => Some(account.as_str()),
            MemberAccess::S3AccessKey { .. } => None,
        });
        let shares = self.invite_shares.values().map(|share| share.account.as_str());
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
            // Only the owner's devices record provider work (`provider_changes`).
            if !session.history.owns_storage(member.member()) {
                return Err(coven_database::DbError::Invariant {
                    what: "provider work runs on a device whose member's account holds the store",
                }
                .into());
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

#[cfg(test)]
#[path = "sequence_access_tests.rs"]
mod tests;
