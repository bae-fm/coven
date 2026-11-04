//! Provider requests follow intentions committed with authenticated membership.
use crate::{
    history::History,
    sequence_sync::{SequenceSync, Session},
    OperationKind, OperationRejection, PendingReason, PendingRecord, PendingSubject, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{OperationId, ProviderAccessChange, ProviderChanges};
use coven_format::MemberAccess;
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
            Some(MemberAccess::S3AccessKey { .. }) => self.state.active_admin(&member),
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
        self.state
            .active_member(&member)
            .is_some_and(|current| created.same_credential(&current.access))
    }

    /// Whether `member` may reach storage with `access` (format.md D6): it
    /// is the kind of access the store's creation records, a provider
    /// account or an S3 key (§10.3), and the member whose account holds the
    /// store stays on that account. Moved to another, no member would reach
    /// storage through it, and nobody could share the store or take sharing
    /// back (§20.9).
    pub(crate) fn access_allowed(
        &self,
        member: MemberId,
        access: &MemberAccess,
    ) -> Result<(), OperationRejection> {
        let Some(created) = self.store_access().filter(|created| created.same_kind(access)) else {
            return Err(OperationRejection::NotAllowed);
        };
        if self.holds_store(member) && !created.same_credential(access) {
            return Err(OperationRejection::StoreOwner);
        }
        Ok(())
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
            let account = MemberAccess::normalized_account(account);
            if !accounts.insert(account.clone()) {
                continue;
            }
            let intended = self.account_intended(&account);
            // A member's access entry describes a grant already made,
            // and an invite share one its own device makes: a device
            // that first learns of an account never grants it itself.
            changes.accounts.push(ProviderAccessChange {
                initially_granted: true,
                ..crate::sequence_sync::account_change(&account, intended, false)?
            });
        }
        Ok(changes)
    }
}

impl SequenceSync {
    /// Refuse `access` as this member's own where history would refuse
    /// recording it (format.md D6). A restore checks before registering its
    /// device, so a member who could never record the access it signed in
    /// with leaves no device behind (§20.9).
    pub async fn check_access(&self, access: &MemberAccess) -> Result<(), SyncError> {
        let member = self.member.read()?.ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let session = self.session.lock().await;
        Ok(session.history.access_allowed(member.member(), access)?)
    }

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
            self.settle_resumed(record.id, OperationKind::RevokeAccess, result, ring).await?;
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
            let target = if record.last_step == coven_database::REQUEST_FIXED_STEP {
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
                        .fail_provider_request(account, id, record.data, pending.clone())
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
