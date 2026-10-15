//! One serialized provider-access phase, independent of membership publication.

use super::StoreLogSync;
use crate::{
    operation_data::{Data, InviteGrant, InviteState},
    operations::{Output, Progress},
    *,
};
use coven_database::{OperationRecord, ProviderAccessChange, StoreLog};
use coven_format::{store_log::StoreChange, MemberAccess};
use std::collections::{BTreeMap, BTreeSet};

impl StoreLogSync {
    pub(super) fn access_changes<'a>(
        &self,
        log: &StoreLog,
        operations: impl IntoIterator<Item = &'a Data>,
    ) -> Result<Vec<ProviderAccessChange>, SyncError> {
        let owner = self.owns_storage(log, &self.operation_member()?.member());
        let mut recorded = BTreeSet::new();
        for entry in &log.entries {
            if let Some((_, MemberAccess::ProviderAccount(account))) =
                crate::access_keys::recorded(&entry.entry)
            {
                recorded.insert(account.trim().to_ascii_lowercase());
            }
        }
        let mut invites = BTreeMap::new();
        for data in operations {
            if let Data::Invite(work) = data {
                if let InviteGrant::Account { email } = &work.access {
                    let open = !matches!(work.state, InviteState::Revoking | InviteState::Settled);
                    let needed = invites
                        .entry(email.trim().to_ascii_lowercase())
                        .or_insert(false);
                    *needed |= open;
                }
            }
        }
        let accounts: BTreeSet<_> = recorded.iter().chain(invites.keys()).cloned().collect();
        accounts
            .into_iter()
            .map(|account| {
                let needs_invitation = invites.get(&account) == Some(&true);
                let access = MemberAccess::ProviderAccount(account.clone());
                let intended = needs_invitation
                    || log
                        .replay
                        .state
                        .members
                        .values()
                        .any(|m| !m.removed && m.access.same_credential(&access));
                Ok(ProviderAccessChange {
                    initially_granted: recorded.contains(&account),
                    intended,
                    needs_invitation,
                    owner,
                    operation: Data::ProviderAccess {
                        account: account.clone(),
                    }
                    .new_operation("coven")?,
                    account,
                })
            })
            .collect()
    }

    /// An invitation transition and the resulting account intention commit together.
    pub(super) async fn save_invite(
        &self,
        record: Option<&OperationRecord>,
        data: &Data,
        step: u32,
    ) -> Result<OperationId, SyncError> {
        let log = self.database.local_store_log().await?.log;
        let mut operations = Vec::new();
        for row in self.database.operations().await? {
            if record.is_none_or(|record| record.id != row.id) {
                operations.push(Data::read(&row)?);
            }
        }
        let changes = self.access_changes(&log, operations.iter().chain(std::iter::once(data)))?;
        match record {
            Some(record) => {
                self.database
                    .advance_operation_with_access::<PendingRecord>(
                        data.update(record, step)?,
                        changes,
                    )
                    .await?;
                Ok(record.id)
            }
            None => Ok(self
                .database
                .start_operation_with_access::<PendingRecord>(
                    data.new_operation("create_invite")?,
                    changes,
                )
                .await?),
        }
    }

    pub(super) async fn finish_invite(&self, record: &OperationRecord) -> Result<(), SyncError> {
        let log = self.database.local_store_log().await?.log;
        let mut operations = Vec::new();
        for row in self.database.operations().await? {
            let mut data = Data::read(&row)?;
            if row.id == record.id {
                let Data::Invite(work) = &mut data else {
                    return Err(coven_database::DbError::Invariant {
                        what: "an invite's grant step reads its invite",
                    }
                    .into());
                };
                // Keep the account in the universe while withdrawing this invitation.
                work.state = InviteState::Settled;
            }
            operations.push(data);
        }
        let changes = self.access_changes(&log, operations.iter())?;
        self.database
            .finish_operation_with_access::<PendingRecord>(record.id, changes)
            .await?;
        Ok(())
    }

    /// Publish only membership, revalidating a dropped attempt against a fresh
    /// catch-up. The entry's publication queue survives interruption by itself.
    pub(super) async fn publish_removal(&mut self, change: StoreChange) -> Result<(), SyncError> {
        loop {
            self.catch_up().await?;
            let local = self.database.local_store_log().await?;
            let me = self.operation_member()?.member();
            self.validate_administration(&local, &me, &change)?;
            if crate::effects::change_in_place(&local.log.replay.state, &me, &change) {
                return Ok(());
            }
            let published = self.make_and_upload_entry(change.clone()).await;
            match published {
                Err(SyncError::Rejected(_)) => continue,
                Err(SyncError::Removed) => {
                    let log = self.database.local_store_log().await?.log;
                    if log.entries.iter().any(|e| {
                        e.entry.position.device == local.device
                            && !local.log.replay.entries.contains_key(&e.entry.position)
                            && e.entry.change == change
                            && log.replay.entries[&e.entry.position]
                                == coven_database::EntryOutcome::Kept
                    }) {
                        return Ok(());
                    }
                    return Err(SyncError::Removed);
                }
                Err(error) => return Err(error),
                Ok(entry) => {
                    self.catch_up().await?;
                    let log = self.database.local_store_log().await?.log;
                    if log.replay.entries[&entry] == coven_database::EntryOutcome::Kept {
                        return Ok(());
                    }
                }
            }
        }
    }

    pub(super) async fn provider_access_step(
        &mut self,
        record: &OperationRecord,
        account: String,
    ) -> Result<Progress, SyncError> {
        let local = self.database.local_store_log().await?;
        if !self.owns_storage(&local.log, &self.operation_member()?.member()) {
            return Ok(Progress::Waiting(PendingReason::PendingOwner { account }));
        }
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        if storage.config().provider() == coven_storage::CloudProvider::S3 {
            return Err(coven_storage::StorageFailure::InvalidConfiguration
                .with_source("S3 uses access-key operations")
                .into());
        }
        if record.last_step != 1 {
            self.database
                .start_provider_request::<PendingRecord>(account, record.id)
                .await?;
            return Ok(Progress::Advanced);
        }
        let state = self
            .database
            .provider_access(account.clone())
            .await?
            .ok_or(coven_database::DbError::OperationChanged(record.id))?;
        let target = state.request.as_ref().and_then(|r| r.target).ok_or(
            coven_database::DbError::Invariant {
                what: "a sent provider request records its target",
            },
        )?;
        let invitation = if target {
            match storage.grant_access(&account).await? {
                coven_storage::AccessGrant::Granted { invitation } => Some(invitation.encode()?),
                coven_storage::AccessGrant::CreateAccessKey => {
                    return Err(coven_storage::StorageFailure::InvalidConfiguration
                        .with_source("account sharing cannot create an S3 key")
                        .into())
                }
            }
        } else {
            let shares = storage.revoke_access(&account).await?;
            if !shares.is_empty() {
                let reason = PendingReason::AccessRemains {
                    account: account.clone(),
                    shares,
                };
                let pending = self.operation_condition(record, reason.clone()).await?;
                let data = Data::read(record)?;
                self.database
                    .fail_provider_request(account, data.update(record, 2)?, pending)
                    .await?;
                return Ok(Progress::Waiting(reason));
            }
            None
        };
        // Learn reversals that arrived while storage was answering. If this
        // fails, the fixed request stays unfinished and can safely repeat.
        self.catch_up().await?;
        let finished = self
            .database
            .complete_provider_request::<PendingRecord>(account, record.id, target, invitation)
            .await?;
        Ok(if finished {
            Progress::Finished(Output::Unit)
        } else {
            Progress::Advanced
        })
    }
}
