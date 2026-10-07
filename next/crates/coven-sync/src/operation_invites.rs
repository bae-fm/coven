//! The inviting device's one-time invitation and authenticated approval (§12.2).

use super::StoreLogSync;
use crate::{
    operation_data::{Data, InviteState, InviteWork},
    operations::{Begun, Output, Progress},
    *,
};
use coven_crypto::{InviteSecret, MemberId, ObjectHasher, SecretText};
use coven_database::{LocalStoreLog, OperationRecord};
use coven_format::{
    codes::InviteCode, sealed_single::SingleChunkObject, store_log::MemberRole, Object,
};
use coven_foundation::id_source::InviteId;
use coven_storage::{
    AccessGrant, CloudProvider, InviteStorage, MemberRemoval, ObjectPath, S3Credentials,
    StorageError, StorageInvitation,
};
use std::time::Duration;

impl StoreLogSync {
    pub(crate) fn invite_expired(&self, data: &Data) -> bool {
        matches!(data, Data::Invite(work) if self.clock.now() >= work.expires_at && matches!(work.state, InviteState::Grant | InviteState::Waiting { .. } | InviteState::Approving { entry: None, .. }))
    }

    pub(super) async fn begin_invite(
        &self,
        local: &LocalStoreLog,
        me: &MemberId,
        role: MemberRole,
        access: InviteAccess,
    ) -> Result<Begun, SyncError> {
        self.require_admin(&local.log.replay.state, me)?;
        if !self.owns_storage(&local.log, me) {
            return Err(StorageError::NotStoreOwner.into());
        }
        match &access {
            InviteAccess::ProviderAccount { email } if email.trim().is_empty() => {
                return Err(StorageError::InvalidConfiguration("invite account is empty").into())
            }
            InviteAccess::S3AccessKey {
                access_key_id,
                secret_access_key,
            } if access_key_id.is_empty() || secret_access_key.as_str().is_empty() => {
                return Err(StorageError::InvalidConfiguration("invite access key is empty").into())
            }
            _ => (),
        }
        let access = match access {
            InviteAccess::ProviderAccount { email } => InviteAccess::ProviderAccount {
                email: email.trim().to_ascii_lowercase(),
            },
            access => access,
        };
        let secret = InviteSecret::generate()?;
        let data = Data::Invite(InviteWork {
            id: InviteId(self.ids.new_id()),
            role,
            access,
            secret: SecretText::new(serde_json::to_string(secret.to_secret_bytes().as_bytes())?),
            expires_at: self
                .clock
                .now()
                .checked_add(Duration::from_secs(86400))
                .ok_or(StorageError::InvalidConfiguration(
                    "clock cannot represent invite expiry",
                ))?,
            state: InviteState::Grant,
        });
        Ok(Begun::Operation(
            self.database
                .start_operation(data.new_operation("create_invite")?)
                .await?,
        ))
    }

    pub(super) async fn settle_invite(
        &self,
        local: &LocalStoreLog,
        me: &MemberId,
        id: InviteId,
        request: Option<JoinRequest>,
        approve: bool,
    ) -> Result<Begun, SyncError> {
        self.require_admin(&local.log.replay.state, me)?;
        if !self.owns_storage(&local.log, me) {
            return Err(StorageError::NotStoreOwner.into());
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
                InviteState::Approving {
                    request,
                    entry: None,
                }
            } else {
                InviteState::Revoking
            };
            self.database
                .advance_operation(data.update(&record, 0)?)
                .await?;
            return Ok(Begun::Operation(record.id));
        }
        Err(SyncError::InvitationChanged)
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
            self.database
                .advance_operation(data.update(record, 0)?)
                .await?;
            return Ok(Progress::Reply(Err(SyncError::InvitationChanged)));
        }
        match &mut work.state {
            InviteState::Grant => {
                let local = self.database.local_store_log().await?;
                let me = self.operation_member()?.member_id();
                self.require_admin(&local.log.replay.state, &me)?;
                if !self.owns_storage(&local.log, &me) {
                    return Err(StorageError::NotStoreOwner.into());
                }
                let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
                let invitation = match &work.access {
                    InviteAccess::ProviderAccount { email } => {
                        if storage.config().provider() == CloudProvider::S3 {
                            return Err(StorageError::InvalidConfiguration(
                                "S3 invitation needs its own access key",
                            )
                            .into());
                        }
                        match storage.grant_access(email).await? {
                            AccessGrant::Granted { invitation } => invitation,
                            AccessGrant::CreateAccessKey => {
                                return Err(StorageError::InvalidConfiguration(
                                    "provider requires an access key",
                                )
                                .into())
                            }
                        }
                    }
                    InviteAccess::S3AccessKey { .. } => {
                        if storage.config().provider() != CloudProvider::S3 {
                            return Err(StorageError::InvalidConfiguration(
                                "access-key invitation requires S3",
                            )
                            .into());
                        }
                        StorageInvitation::for_account(storage.config())?
                    }
                };
                work.state = InviteState::Waiting {
                    invitation,
                    request: None,
                };
                let value = self.invite_value(&local, work)?;
                self.database
                    .advance_operation(data.update(record, 1)?)
                    .await?;
                Ok(Progress::Reply(Ok(Output::Invite(value))))
            }
            InviteState::Waiting { .. } => {
                let path = ObjectPath::join_request(work.id);
                let Some(bytes) = self.read(&path).await? else {
                    return Ok(Progress::Waiting);
                };
                let request = self.open_request(work, &bytes)?;
                let InviteState::Waiting {
                    request: current, ..
                } = &mut work.state
                else {
                    unreachable!()
                };
                if *current == Some(request.clone()) {
                    return Ok(Progress::Waiting);
                }
                *current = Some(request);
                self.database
                    .advance_operation(data.update(record, 1)?)
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
            InviteState::Revoking => {
                let result = self
                    .revoke_access(record.id, &work.access.member_access())
                    .await?;
                let key = match result {
                    MemberRemoval::AccessRemains { shares } => {
                        return Err(SyncError::AccessRemains(shares))
                    }
                    MemberRemoval::DeleteAccessKey { access_key_id } => Some(access_key_id),
                    _ => None,
                };
                work.state = InviteState::Settled;
                self.save_access_result(record, &data, 5, key).await?;
                Ok(Progress::Advanced)
            }
            InviteState::Settled => {
                // Provider deletes are idempotent, including an already absent request.
                match self
                    .storage
                    .as_deref()
                    .ok_or(SyncError::NoStorage)?
                    .delete(&ObjectPath::join_request(work.id))
                    .await
                {
                    Ok(()) | Err(StorageError::NotFound) => (),
                    Err(error) if error.failure() == coven_storage::StorageFailure::NotFound => (),
                    Err(error) => return Err(error.into()),
                }
                self.database.finish_operation(record.id).await?;
                Ok(Progress::Finished(Output::Unit))
            }
        }
    }

    fn invite_value(&self, local: &LocalStoreLog, work: &InviteWork) -> Result<Invite, SyncError> {
        let InviteState::Waiting { invitation, .. } = &work.state else {
            unreachable!()
        };
        let storage = match &work.access {
            InviteAccess::ProviderAccount { .. } => InviteStorage::Account(invitation.clone()),
            InviteAccess::S3AccessKey {
                access_key_id,
                secret_access_key,
            } => InviteStorage::S3 {
                invitation: invitation.clone(),
                credentials: S3Credentials {
                    access_key_id: access_key_id.clone(),
                    secret_access_key: secret_access_key.clone(),
                },
            },
        };
        let store = local
            .log
            .replay
            .state
            .store
            .as_ref()
            .ok_or(SyncError::PermissionDenied)?;
        let code = InviteCode {
            store: store.id,
            name: store.name.clone(),
            invite: work.id,
            secret: self.invite_secret(work)?,
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
        let path = ObjectPath::join_request(work.id);
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
                InviteAccess::ProviderAccount { email } => Some(email.clone()),
                _ => None,
            },
            record: bytes.to_vec(),
        })
    }
}
