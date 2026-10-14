//! The inviting device's one-time invitation and authenticated approval (§12.2).

use super::operation_minting::Minting;
use super::StoreLogSync;
use crate::{
    operation_data::{Data, InviteGrant, InviteState, InviteWork, MintWork},
    operations::{Begun, Output, Progress},
    *,
};
use coven_crypto::{InviteSecret, MemberId, ObjectHasher, SecretText};
use coven_database::{LocalStoreLog, OperationRecord};
use coven_format::{
    codes::InviteCode,
    sealed_single::SingleChunkObject,
    store_log::{MemberRole, StoreChange},
    Object,
};
use coven_foundation::id_source::InviteId;
use coven_storage::{
    CloudProvider, InviteStorage, MintScope, ObjectPath, S3Credentials, S3MemberKey,
    StorageFailure, StorageInvitation,
};
use std::time::Duration;

impl StoreLogSync {
    pub(crate) fn invite_expired(&self, data: &Data) -> bool {
        matches!(data, Data::Invite(work) if self.clock.now() >= work.expires_at && matches!(work.state, InviteState::Grant | InviteState::Waiting { .. } | InviteState::Approving { entry: None, .. }))
    }

    pub(super) async fn begin_invite(
        &mut self,
        local: &LocalStoreLog,
        me: &MemberId,
        role: MemberRole,
        access: InviteAccess,
    ) -> Result<Begun, SyncError> {
        crate::effects::Authority::Admin.check(&local.log.replay.state, me)?;
        if !self.owns_storage(&local.log, me) {
            return Err(StorageFailure::NotStoreOwner.into());
        }
        match &access {
            InviteAccess::ProviderAccount { email } if email.trim().is_empty() => {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("invite account is empty")
                    .into())
            }
            InviteAccess::S3AccessKey {
                access_key_id,
                secret_access_key,
            } if access_key_id.is_empty() || secret_access_key.as_str().is_empty() => {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("invite access key is empty")
                    .into())
            }
            _ => (),
        }
        let id = InviteId(self.ids.new_id());
        let mut admin = None;
        let grant = match access {
            InviteAccess::ProviderAccount { email } => InviteGrant::Account {
                email: email.trim().to_ascii_lowercase(),
            },
            InviteAccess::S3AccessKey {
                access_key_id,
                secret_access_key,
            } => InviteGrant::S3Key(S3MemberKey {
                credentials: S3Credentials {
                    access_key_id,
                    secret_access_key,
                },
                principal: coven_format::S3Principal::Console,
            }),
            InviteAccess::S3AdminAccess(access) => {
                admin = Some(access);
                // The key is tagged with the invite; its member id is not chosen yet.
                InviteGrant::Minting(MintWork::new(MintScope::new(
                    local.store,
                    &format!("invite-{}", id.0),
                    self.ids.new_id(),
                )))
            }
        };
        let secret = InviteSecret::generate()?;
        let data = Data::Invite(InviteWork {
            id,
            role,
            access: grant,
            secret: SecretText::new(serde_json::to_string(secret.to_secret_bytes().as_bytes())?),
            expires_at: self
                .clock
                .now()
                .checked_add(Duration::from_secs(86400))
                .ok_or(
                    StorageFailure::InvalidConfiguration
                        .with_source("clock cannot represent invite expiry"),
                )?,
            state: InviteState::Grant,
        });
        let operation = self.save_invite(None, &data, 0).await?;
        if let Some(admin) = admin {
            self.hold_admin_access(operation, admin);
        }
        Ok(Begun::Operation(operation))
    }

    pub(super) async fn settle_invite(
        &self,
        local: &LocalStoreLog,
        me: &MemberId,
        id: InviteId,
        request: Option<JoinRequest>,
        approve: bool,
    ) -> Result<Begun, SyncError> {
        crate::effects::Authority::Admin.check(&local.log.replay.state, me)?;
        if !self.owns_storage(&local.log, me) {
            return Err(StorageFailure::NotStoreOwner.into());
        }
        for record in self.database.operations().await? {
            let mut data = Data::read(&record)?;
            let Data::Invite(work) = &mut data else {
                continue;
            };
            if work.id != id {
                continue;
            }
            if self.clock.now() >= work.expires_at {
                return Err(SyncError::InvitationChanged);
            }
            let stored = match &work.state {
                InviteState::Waiting { request, .. } => request.as_ref(),
                InviteState::Grant if !approve && request.is_none() => None,
                _ => return Err(SyncError::InvitationChanged),
            };
            if let Some(request) = &request {
                let Some(bytes) = stored else {
                    return Err(SyncError::InvitationChanged);
                };
                if self.present_request(work, bytes)? != *request {
                    return Err(SyncError::InvitationChanged);
                }
            }
            work.state = if approve {
                let request = stored.ok_or(SyncError::InvitationChanged)?.clone();
                let decoded = self.decode_request(&request)?;
                self.validate_administration(
                    local,
                    me,
                    &coven_format::store_log::StoreChange::AddMember {
                        keys: decoded.keys,
                        role: work.role,
                        access: work.access.member_access()?,
                    },
                )?;
                InviteState::Approving {
                    request,
                    entry: None,
                }
            } else {
                InviteState::Revoking
            };
            self.save_invite(Some(&record), &data, 0).await?;
            return Ok(Begun::Operation(record.id));
        }
        Err(SyncError::InvitationChanged)
    }

    /// Discarding an invite takes back what it granted, as cancelling does
    /// (§13): its account share, its S3 key's deletion, or the rollback of a
    /// key still being made. A reserved approval entry is published by the
    /// generic discard instead, and a settled invite has granted nothing left.
    pub(super) async fn discard_invite(
        &self,
        record: &OperationRecord,
    ) -> Result<Option<Begun>, SyncError> {
        let mut data = Data::read(record)?;
        let Data::Invite(work) = &mut data else {
            return Ok(None);
        };
        match work.state {
            InviteState::Approving { entry: Some(_), .. } | InviteState::Settled => {
                return Ok(None)
            }
            InviteState::Revoking => (),
            InviteState::Grant | InviteState::Waiting { .. } | InviteState::Approving { .. } => {
                work.state = InviteState::Revoking;
                self.save_invite(Some(record), &data, 0).await?;
            }
        }
        Ok(Some(Begun::Operation(record.id)))
    }

    pub(super) async fn invite_step(
        &mut self,
        record: &OperationRecord,
        mut data: Data,
    ) -> Result<Progress, SyncError> {
        let expired = self.invite_expired(&data);
        let Data::Invite(work) = &mut data else {
            unreachable!()
        };
        // A fixed entry must be published even after the invitation's expiry.
        if expired {
            work.state = InviteState::Revoking;
            self.save_invite(Some(record), &data, 0).await?;
            return Ok(Progress::Reply(Err(SyncError::InvitationChanged)));
        }
        match &mut work.state {
            InviteState::Grant if matches!(work.access, InviteGrant::Minting(_)) => {
                let InviteGrant::Minting(mint) = &mut work.access else {
                    unreachable!()
                };
                let Some(admin) = self.admin_access(record.id) else {
                    return Ok(Progress::Waiting(PendingReason::AdminAccessRequired));
                };
                match self.advance_mint(admin.as_ref(), mint).await {
                    Minting::Ready(key) => {
                        work.access = InviteGrant::S3Key(key);
                        self.release_admin_access(record.id);
                    }
                    Minting::Advanced => {}
                    Minting::Failed { error, reset } => {
                        if reset {
                            self.database
                                .advance_operation(data.update(record, 0)?)
                                .await?;
                        }
                        self.release_admin_access(record.id);
                        return Err(error);
                    }
                }
                self.save_invite(Some(record), &data, 0).await?;
                Ok(Progress::Advanced)
            }
            InviteState::Grant => {
                self.catch_up().await?;
                let local = self.database.local_store_log().await?;
                let me = self.operation_member()?.member_id();
                crate::effects::Authority::Admin.check(&local.log.replay.state, &me)?;
                if !self.owns_storage(&local.log, &me) {
                    return Err(StorageFailure::NotStoreOwner.into());
                }
                let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
                let invitation = match &work.access {
                    InviteGrant::Account { email } => {
                        if storage.config().provider() == CloudProvider::S3 {
                            return Err(StorageFailure::InvalidConfiguration
                                .with_source("S3 invitation needs its own access key")
                                .into());
                        }
                        let access = self
                            .database
                            .provider_access(email.clone())
                            .await?
                            .ok_or(coven_database::DbError::DamagedDatabase)?;
                        if let Some(request) = access.request {
                            let pending = self
                                .database
                                .operation_pending::<PendingRecord>(request.operation)
                                .await?
                                .ok_or(coven_database::DbError::DamagedDatabase)?;
                            if pending.retry() != Retry::Automatic {
                                return Err(SyncError::Pending(Box::new(pending)));
                            }
                            return Ok(Progress::Waiting(PendingReason::ProviderPending {
                                action: ProviderAccessAction::Grant,
                                account: email.clone(),
                            }));
                        }
                        coven_storage::StorageInvitation::decode(
                            access
                                .invitation
                                .as_ref()
                                .map(coven_crypto::SecretBytes::as_bytes)
                                .ok_or(coven_database::DbError::DamagedDatabase)?,
                        )?
                    }
                    InviteGrant::Minting(_) => unreachable!("minted before granting"),
                    InviteGrant::S3Key(_) => {
                        if storage.config().provider() != CloudProvider::S3 {
                            return Err(StorageFailure::InvalidConfiguration
                                .with_source("access-key invitation requires S3")
                                .into());
                        }
                        StorageInvitation::for_account(storage.config())?
                    }
                };
                work.state = InviteState::Waiting {
                    invitation,
                    request: None,
                };
                let value = self.invite_value(&local, work).await?;
                let pending = self
                    .operation_condition(
                        record,
                        PendingReason::Missing {
                            path: ObjectPath::join_request(self.directory.id(), value.id),
                        },
                    )
                    .await?;
                self.database
                    .advance_operation_pending(data.update(record, 1)?, pending)
                    .await?;
                Ok(Progress::Reply(Ok(Output::Invite(value))))
            }
            InviteState::Waiting { .. } => {
                let path = ObjectPath::join_request(self.directory.id(), work.id);
                let Some(bytes) = self.read(&path).await? else {
                    return Ok(Progress::Waiting(PendingReason::Missing { path }));
                };
                let request = self.open_request(work, &bytes)?;
                let InviteState::Waiting {
                    request: current, ..
                } = &mut work.state
                else {
                    unreachable!()
                };
                if *current == Some(request.clone()) {
                    return Ok(Progress::Waiting(PendingReason::Paused));
                }
                *current = Some(request);
                let pending = self
                    .operation_condition(record, PendingReason::Paused)
                    .await?;
                self.database
                    .advance_operation_pending(data.update(record, 1)?, pending)
                    .await?;
                Ok(Progress::Advanced)
            }
            InviteState::Approving { .. } if record.last_step < 5 => {
                self.entry_step(record, data).await
            }
            InviteState::Approving { .. } => {
                work.state = InviteState::Settled;
                self.database
                    .advance_operation(data.update(record, 5)?)
                    .await?;
                Ok(Progress::Advanced)
            }
            InviteState::Revoking if matches!(work.access, InviteGrant::Minting(_)) => {
                let InviteGrant::Minting(mint) = &work.access else {
                    unreachable!()
                };
                if let Some(key) = &mint.minted {
                    // A made key is pending deletion like any invite's key.
                    work.access = InviteGrant::S3Key(key.clone());
                } else {
                    // Nothing reached the member; remove whatever the attempt made.
                    let Some(admin) = self.admin_access(record.id) else {
                        return Ok(Progress::Waiting(PendingReason::AdminAccessRequired));
                    };
                    admin.roll_back(&mint.scope).await?;
                    self.release_admin_access(record.id);
                    work.state = InviteState::Settled;
                }
                self.database
                    .advance_operation(data.update(record, record.last_step)?)
                    .await?;
                Ok(Progress::Advanced)
            }
            InviteState::Revoking => {
                let access = work.access.member_access()?;
                work.state = InviteState::Settled;
                // The invite's S3 key stays pending until its deletion clears (§12.2).
                let deletions = crate::access_keys::deletion(&access, None)
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;
                self.database
                    .record_access_key_deletions::<PendingRecord>(
                        Some(data.update(record, 5)?),
                        deletions,
                    )
                    .await?;
                Ok(Progress::Advanced)
            }
            InviteState::Settled => {
                // Provider deletes are idempotent, including an already absent request.
                match self
                    .storage
                    .as_deref()
                    .ok_or(SyncError::NoStorage)?
                    .delete(&ObjectPath::join_request(self.directory.id(), work.id))
                    .await
                {
                    Ok(()) => (),
                    Err(error) if error.failure() == coven_storage::StorageFailure::NotFound => (),
                    Err(error) => return Err(error.into()),
                }
                self.finish_invite(record).await?;
                Ok(Progress::Finished(Output::Unit))
            }
        }
    }

    async fn invite_value(
        &self,
        local: &LocalStoreLog,
        work: &InviteWork,
    ) -> Result<Invite, SyncError> {
        let InviteState::Waiting { invitation, .. } = &work.state else {
            unreachable!()
        };
        let storage = match &work.access {
            InviteGrant::Account { .. } => InviteStorage::Account(invitation.clone()),
            InviteGrant::S3Key(key) => InviteStorage::S3 {
                invitation: invitation.clone(),
                credentials: key.credentials.clone(),
            },
            InviteGrant::Minting(_) => unreachable!("minted before granting"),
        };
        let store = local
            .log
            .replay
            .state
            .store
            .as_ref()
            .ok_or(coven_database::DbError::DamagedDatabase)?;
        if store.id != local.store {
            return Err(SyncError::WrongStore {
                expected: local.store,
                actual: store.id,
            });
        }
        let initial_key = local
            .log
            .entries
            .iter()
            .find_map(|entry| match &entry.entry.change {
                StoreChange::CreateStore { store, key, .. }
                    if *store == local.store
                        && local.log.replay.entries.get(&entry.entry.position)
                            == Some(&coven_database::EntryOutcome::Kept) =>
                {
                    Some(*key)
                }
                _ => None,
            })
            .ok_or(coven_database::DbError::DamagedDatabase)?;
        let code = InviteCode {
            store: store.id,
            name: store.name.clone(),
            invite: work.id,
            secret: self.invite_secret(work)?,
            initial_key,
            inviting_writer: local.device,
            key_copy_number: self.database.published_key_copy_number().await?,
            storage: storage.encode()?,
        }
        .to_text()?;
        Ok(Invite {
            id: work.id,
            code: code.to_string(),
            role: work.role,
            expires_at: work.expires_at,
        })
    }

    fn invite_secret(&self, work: &InviteWork) -> Result<InviteSecret, SyncError> {
        Ok(InviteSecret::from_bytes(serde_json::from_str(
            work.secret.as_str(),
        )?))
    }

    fn open_request(&self, work: &InviteWork, bytes: &[u8]) -> Result<Vec<u8>, SyncError> {
        let envelope = SingleChunkObject::decode(bytes)?;
        let SingleChunkObject::JoinRequest { signature, .. } = &envelope else {
            return Err(SyncError::InvitationChanged);
        };
        let path = ObjectPath::join_request(self.directory.id(), work.id);
        let plain = self
            .invite_secret(work)?
            .join_request_key()
            .open_object_chunk(
                path.as_str(),
                &envelope.prefix().encode()?,
                0,
                0,
                envelope.chunk(),
            )?;
        let request = self.decode_request(&plain)?;
        if request.invite != work.id {
            return Err(SyncError::InvitationChanged);
        }
        let mut hasher = ObjectHasher::new();
        hasher.update(&envelope.signed_bytes()?);
        request
            .keys
            .signing
            .verify_object(path.as_str(), &hasher.finish(), signature)?;
        Ok(plain.to_vec())
    }

    pub(super) fn decode_request(
        &self,
        bytes: &[u8],
    ) -> Result<coven_format::objects::JoinRequest, SyncError> {
        match Object::decode(bytes)? {
            Object::JoinRequest(request) => Ok(request),
            _ => Err(SyncError::InvitationChanged),
        }
    }

    pub(super) fn present_request(
        &self,
        work: &InviteWork,
        bytes: &[u8],
    ) -> Result<JoinRequest, SyncError> {
        let request = self.decode_request(bytes)?;
        Ok(JoinRequest {
            invite: request.invite,
            member: request.keys.signing,
            device_name: request.device_name,
            provider_account_email: match &work.access {
                InviteGrant::Account { email } => Some(email.clone()),
                _ => None,
            },
            record: bytes.to_vec(),
        })
    }
}
