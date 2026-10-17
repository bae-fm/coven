//! Provider requests follow intentions committed with authenticated membership.
use crate::{
    history::History,
    sequence_sync::{SequenceSync, Session},
    OperationKind, PendingReason, PendingRecord, PendingSubject, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{
    AccessKeyDeletion, AccessKeyToDelete, NewOperation, OperationId, OperationUpdate,
    ProviderAccessChange, ProviderChanges,
};
use coven_format::{admin::MemberRole, MemberAccess};
use coven_foundation::id_source::MemberId;
use std::collections::BTreeSet;

impl History {
    pub(crate) fn owns_storage(&self, member: MemberId) -> bool {
        let Some(current) = self
            .state
            .members
            .get(&member)
            .filter(|value| !value.removed)
        else {
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
    /// member or one of this device's open invites uses it (§13).
    pub(crate) fn provider_changes(
        &self,
        member: MemberId,
        invited: &BTreeSet<String>,
    ) -> Result<ProviderChanges, SyncError> {
        let mut changes = ProviderChanges::default();
        let mut accounts = BTreeSet::new();
        let mut keys = BTreeSet::new();
        let active: Vec<_> = self
            .state
            .members
            .values()
            .filter(|value| !value.removed)
            .map(|value| &value.access)
            .collect();
        for (recorded_for, _, access) in self.recorded_access() {
            let intended = active.iter().any(|current| current.same_credential(access));
            match access {
                MemberAccess::ProviderAccount(account) => {
                    let account = account.trim().to_ascii_lowercase();
                    if !accounts.insert(account.clone()) {
                        continue;
                    }
                    let intended = intended || invited.contains(&account);
                    changes.accounts.push(ProviderAccessChange {
                        operation: NewOperation {
                            kind: "provider-access".into(),
                            data: serde_json::to_vec(&account)?,
                            started_by: "coven".into(),
                            pauses_writes: false,
                        },
                        account,
                        intended,
                        initially_granted: true,
                        needs_invitation: false,
                        owner: self.owns_storage(member),
                    });
                }
                MemberAccess::S3AccessKey {
                    access_key_id,
                    principal,
                } if !intended && keys.insert(access_key_id.clone()) => {
                    let key = AccessKeyToDelete {
                        access_key_id: access_key_id.clone(),
                        principal: principal.clone(),
                        member: Some(recorded_for),
                    };
                    let data = serde_json::to_vec(&crate::sequence_sync::access_keys::Deletion {
                        key: key.clone(),
                    })?;
                    changes.keys.push(AccessKeyDeletion {
                        key,
                        operation: NewOperation {
                            kind: "delete-access-key".into(),
                            data,
                            started_by: "coven".into(),
                            pauses_writes: false,
                        },
                    });
                }
                MemberAccess::S3AccessKey { .. } => (),
            }
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
            let result = scope
                .run(self.apply_access(session, ring, member, record.id))
                .await;
            if let Err(error) = result {
                self.record_operation_error(record.id, OperationKind::RevokeAccess, &error, ring)
                    .await?;
                self.state.check()?;
            } else {
                self.waits.clear_evidence(record.id);
                self.waits
                    .forget(coven_database::RetrySubject::Operation(record.id));
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
            let condition = |reason| PendingRecord {
                subject: PendingSubject::Operation {
                    id,
                    kind: OperationKind::RevokeAccess,
                },
                reason,
                reported_by: self.device,
            };
            if !session.history.owns_storage(member.member()) {
                return Err(SyncError::Pending(Box::new(condition(
                    PendingReason::PendingOwner { account },
                ))));
            }
            let target = if record.last_step == 1 {
                self.database
                    .provider_access(account.clone())
                    .await?
                    .and_then(|state| state.request)
                    .and_then(|request| request.target)
                    .ok_or(coven_database::DbError::DamagedDatabase)?
            } else {
                self.database
                    .start_provider_request::<PendingRecord>(account.clone(), id)
                    .await?
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
                            OperationUpdate {
                                id,
                                previous: 1,
                                last_step: 2,
                                data: record.data,
                            },
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
