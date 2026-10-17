//! Replacing a member's S3 key from the device holding admin access (§13):
//! make the key and check it from the member's side, deliver it sealed to the
//! member, record it for them, then delete the old key and read that it is
//! gone. Applying the record makes the old key pending deletion in the same
//! transaction, so a failed deletion leaves it listed rather than forgotten.

use super::{sequence_operations::Request, SequenceSync, Session};
use crate::{
    AccessKeyReplacement, OldKeyDeletion, OperationKind, OperationRejection, PendingReason,
    PendingRecord, PendingSubject, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{NewOperation, OperationId, OperationRecord, OperationUpdate};
use coven_format::{
    access_key_delivery::{self, DeliveredKey},
    admin::{MemberRole, StoreChange},
    MemberAccess, S3Principal,
};
use coven_foundation::id_source::MemberId;
use coven_storage::{
    AccessKeyAdmin, MintScope, ObjectPath, ObjectPrefix, S3MemberKey, StorageFailure,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const REPLACE_ACCESS_KEY: &str = "replace-access-key";

#[derive(Serialize, Deserialize)]
enum Step {
    /// Make the key with admin access and check it from the member's side.
    Minting,
    /// Deliver these fixed sealed bytes; retries resend them exactly.
    Delivering { key: S3MemberKey, delivery: Vec<u8> },
    /// Claim the entry recording the key for the member.
    Recording { key: S3MemberKey },
    /// The record applied: delete the key it replaced.
    Deleting { key: S3MemberKey },
    /// Validation refused the record: delete the made key and its delivery.
    Undo {
        key: S3MemberKey,
        reason: OperationRejection,
    },
}

#[derive(Serialize, Deserialize)]
struct ReplaceKeyWork {
    member: MemberId,
    attempt: uuid::Uuid,
    scope: MintScope,
    /// The member's access before the record, whose key the record retires.
    old: MemberAccess,
    step: Step,
}

impl ReplaceKeyWork {
    fn update(&self, record: &OperationRecord) -> Result<OperationUpdate, SyncError> {
        Ok(OperationUpdate {
            id: record.id,
            previous: record.last_step,
            last_step: record
                .last_step
                .checked_add(1)
                .ok_or(SyncError::SequenceExhausted)?,
            data: serde_json::to_vec(self)?,
        })
    }
}

impl SequenceSync {
    /// Replace `member`'s S3 key with admin access: this member's own, or
    /// another member's as an admin; any other caller is refused before admin
    /// access is used (§13, E9).
    pub async fn replace_member_access_key(
        self: &Arc<Self>,
        member: MemberId,
        admin: Arc<dyn AccessKeyAdmin>,
    ) -> Result<AccessKeyReplacement, SyncError> {
        let mut session = self.session.lock().await;
        let identity = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &identity).await?;
        self.read_forward(&mut session, &mut ring, &identity)
            .await?;
        self.settle_open(&mut session, &mut ring, &identity).await?;
        let state = &session.history.state;
        let current = state
            .members
            .get(&member)
            .filter(|value| !value.removed)
            .ok_or(SyncError::Rejected(OperationRejection::TargetGone(
                crate::EntryTarget::Member(member),
            )))?;
        if !matches!(current.access, MemberAccess::S3AccessKey { .. }) {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("only S3 member keys are replaced with admin access")
                .into());
        }
        let me = identity.member();
        let admin_author = state
            .members
            .get(&me)
            .is_some_and(|value| !value.removed && value.role == MemberRole::Admin);
        if !self.registered(&session) || (member != me && !admin_author) {
            return Err(SyncError::Rejected(OperationRejection::NotAllowed));
        }
        let attempt = self.ids.new_id();
        let work = ReplaceKeyWork {
            member,
            attempt,
            scope: MintScope::new(self.store(), &member.to_string(), self.device, attempt),
            old: current.access.clone(),
            step: Step::Minting,
        };
        let id = self
            .database
            .start_operation(NewOperation {
                kind: REPLACE_ACCESS_KEY.into(),
                data: serde_json::to_vec(&work)?,
                started_by: "replace_member_access_key".into(),
                pauses_writes: false,
            })
            .await?;
        self.admin_access
            .lock()
            .expect("admin access poisoned")
            .insert(id, admin);
        let result = self
            .advance_key_replacement(&mut session, &mut ring, &identity, id)
            .await;
        self.finish_key_replacement(id, result, &ring).await
    }

    /// Record a key made in the provider's console as this member's own (§13).
    pub async fn record_access_key(
        self: &Arc<Self>,
        access_key_id: String,
    ) -> Result<(), SyncError> {
        let member = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?
            .member();
        self.administer(StoreChange::SetAccess {
            member,
            access: MemberAccess::S3AccessKey {
                access_key_id,
                principal: S3Principal::Console,
            },
        })
        .await
    }

    /// Resume work waiting with `AdminAccessRequired`, holding this admin
    /// access for that operation only (§4).
    pub async fn retry_with_admin_access(
        self: &Arc<Self>,
        id: OperationId,
        admin: Arc<dyn AccessKeyAdmin>,
    ) -> Result<(), SyncError> {
        let record = self
            .database
            .operation(id)
            .await?
            .ok_or(SyncError::NotPending(id))?;
        self.admin_access
            .lock()
            .expect("admin access poisoned")
            .insert(id, admin);
        let mut session = self.session.lock().await;
        let identity = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &identity).await?;
        self.read_forward(&mut session, &mut ring, &identity)
            .await?;
        self.settle_open(&mut session, &mut ring, &identity).await?;
        match record.kind.as_str() {
            REPLACE_ACCESS_KEY => {
                let result = self
                    .advance_key_replacement(&mut session, &mut ring, &identity, id)
                    .await;
                self.finish_key_replacement(id, result, &ring)
                    .await
                    .map(drop)
            }
            super::sequence_invites::INVITE => {
                self.retry_invite(&mut session, &mut ring, &identity, id)
                    .await
            }
            _ => Err(SyncError::NotPending(id)),
        }
    }

    async fn finish_key_replacement(
        &self,
        id: OperationId,
        result: Result<KeyProgress, SyncError>,
        ring: &StoreKeyring,
    ) -> Result<AccessKeyReplacement, SyncError> {
        let error = match result {
            Ok(KeyProgress::Done(replacement)) => {
                self.admin_access
                    .lock()
                    .expect("admin access poisoned")
                    .remove(&id);
                return Ok(replacement);
            }
            Ok(KeyProgress::Waiting(reason)) => SyncError::Pending(Box::new(PendingRecord {
                subject: PendingSubject::Operation {
                    id,
                    kind: OperationKind::ReplaceAccessKey,
                },
                reason,
                reported_by: self.device,
            })),
            Err(error) => error,
        };
        if self.database.operation(id).await?.is_some() {
            self.record_operation_error(id, OperationKind::ReplaceAccessKey, &error, ring)
                .await?;
        } else {
            self.admin_access
                .lock()
                .expect("admin access poisoned")
                .remove(&id);
        }
        Err(error)
    }

    async fn advance_key_replacement(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        identity: &MemberIdentity,
        id: OperationId,
    ) -> Result<KeyProgress, SyncError> {
        loop {
            let record = self
                .database
                .operation(id)
                .await?
                .ok_or(SyncError::NotPending(id))?;
            let mut work: ReplaceKeyWork = serde_json::from_slice(&record.data)?;
            let admin = self
                .admin_access
                .lock()
                .expect("admin access poisoned")
                .get(&id)
                .cloned();
            let path = ObjectPath::access_key(self.store(), &work.member, work.attempt);
            match work.step {
                Step::Minting => {
                    let Some(admin) = admin else {
                        return Ok(KeyProgress::Waiting(PendingReason::AdminAccessRequired));
                    };
                    let key = self.mint_checked(admin.as_ref(), &work.scope).await?;
                    let sealing = session
                        .history
                        .state
                        .members
                        .get(&work.member)
                        .filter(|value| !value.removed)
                        .ok_or(SyncError::Rejected(OperationRejection::TargetGone(
                            crate::EntryTarget::Member(work.member),
                        )))?
                        .keys
                        .sealing;
                    let delivered = DeliveredKey {
                        access_key_id: key.credentials.access_key_id.clone(),
                        secret_access_key: key.credentials.secret_access_key.clone(),
                        principal: key.principal.clone(),
                    };
                    let delivery = access_key_delivery::seal(
                        identity,
                        &work.member,
                        &sealing,
                        path.as_str(),
                        &delivered,
                    )
                    .map_err(|error| SyncError::Failed {
                        failure: crate::LocalFailure::Crypto,
                        source: Arc::new(error),
                    })?;
                    work.step = Step::Delivering { key, delivery };
                    self.database
                        .advance_operation(work.update(&record)?)
                        .await?;
                }
                Step::Delivering { key, delivery } => {
                    // A lost reply may have stored these bytes; the attempt id
                    // is this device's own, so other bytes fail the attempt (§10).
                    match self.storage.create(&path, &delivery).await {
                        Ok(_) => (),
                        Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                            crate::identity::settle_occupied(
                                self.storage.as_ref(),
                                &path,
                                crate::identity::digest(&delivery),
                            )
                            .await?;
                        }
                        Err(error) => return Err(error.into()),
                    }
                    work.step = Step::Recording { key };
                    self.database
                        .advance_operation(work.update(&record)?)
                        .await?;
                }
                Step::Recording { key } => {
                    let change = StoreChange::SetAccess {
                        member: work.member,
                        access: MemberAccess::S3AccessKey {
                            access_key_id: key.credentials.access_key_id.clone(),
                            principal: key.principal.clone(),
                        },
                    };
                    work.step = match self
                        .start_request(
                            session,
                            ring,
                            identity,
                            Request::Change(change),
                            REPLACE_ACCESS_KEY,
                        )
                        .await
                    {
                        Ok(_) => Step::Deleting { key },
                        Err(SyncError::Rejected(reason)) => Step::Undo { key, reason },
                        Err(error) => return Err(error),
                    };
                    self.database
                        .advance_operation(work.update(&record)?)
                        .await?;
                }
                Step::Deleting { key } => {
                    let old_key = match &work.old {
                        MemberAccess::S3AccessKey { access_key_id, .. }
                            if *access_key_id != key.credentials.access_key_id =>
                        {
                            match self.deletion_operation(access_key_id).await? {
                                None => OldKeyDeletion::Deleted,
                                Some((operation, S3Principal::Console)) => {
                                    OldKeyDeletion::Console { operation }
                                }
                                Some((operation, _)) => match admin {
                                    // After a restart, deleting needs admin access again.
                                    None => {
                                        return Ok(KeyProgress::Waiting(
                                            PendingReason::AdminAccessRequired,
                                        ))
                                    }
                                    Some(admin) => match self
                                        .delete_access_key(operation, admin.as_ref())
                                        .await
                                    {
                                        Ok(()) => OldKeyDeletion::Deleted,
                                        Err(error) => OldKeyDeletion::Failed { operation, error },
                                    },
                                },
                            }
                        }
                        // Recording the same key again replaces no credential.
                        _ => OldKeyDeletion::Deleted,
                    };
                    self.database.finish_operation(id).await?;
                    return Ok(KeyProgress::Done(AccessKeyReplacement { key, old_key }));
                }
                Step::Undo { key, reason } => {
                    let Some(admin) = admin else {
                        return Ok(KeyProgress::Waiting(PendingReason::AdminAccessRequired));
                    };
                    match self.storage.delete(&path).await {
                        Ok(()) => (),
                        Err(error) if error.failure() == StorageFailure::NotFound => (),
                        Err(error) => return Err(error.into()),
                    }
                    if self.storage.status(&path).await?.is_some() {
                        return Err(StorageFailure::Protocol
                            .with_source("the key delivery remains after deletion")
                            .into());
                    }
                    admin
                        .revoke(&key.credentials.access_key_id, &key.principal)
                        .await?;
                    if admin
                        .key_exists(&key.credentials.access_key_id, &key.principal)
                        .await?
                    {
                        return Err(
                            crate::sequence_sync::access_keys::DeleteAccessKeyError::StillPresent
                                .into(),
                        );
                    }
                    self.database.finish_operation(id).await?;
                    return Err(SyncError::Rejected(reason));
                }
            }
        }
    }

    /// The open deletion obligation for a key id, and the principal it names.
    async fn deletion_operation(
        &self,
        access_key_id: &str,
    ) -> Result<Option<(OperationId, S3Principal)>, SyncError> {
        for record in self.database.operations().await? {
            if record.kind == "delete-access-key" {
                let deletion: super::access_keys::Deletion = serde_json::from_slice(&record.data)?;
                if deletion.key.access_key_id == access_key_id {
                    return Ok(Some((record.id, deletion.key.principal)));
                }
            }
        }
        Ok(None)
    }

    /// The key an admin delivered to this member that the applied history
    /// records as their current access, signed by the member who recorded
    /// it (§13). A device already holding the recorded key (`held`) reads
    /// nothing.
    pub async fn delivered_access_key(
        &self,
        held: Option<&str>,
    ) -> Result<Option<DeliveredKey>, SyncError> {
        let session = self.session.lock().await;
        let me = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        let Some(current) = session
            .history
            .state
            .members
            .get(&me.member())
            .filter(|value| !value.removed)
        else {
            return Ok(None);
        };
        let MemberAccess::S3AccessKey { access_key_id, .. } = &current.access else {
            return Ok(None);
        };
        if held == Some(access_key_id.as_str()) {
            return Ok(None);
        }
        let records: Vec<_> = session
            .history
            .key_records
            .iter()
            .filter(|record| record.member == me.member() && record.access_key_id == *access_key_id)
            .collect();
        if records.is_empty() {
            return Ok(None);
        }
        for object in self
            .storage
            .list(&ObjectPrefix::access_keys(self.store(), &me.member()))
            .await?
        {
            let bytes = self.storage.read(&object.path).await?;
            let Ok(routing) = access_key_delivery::routing(&bytes) else {
                continue;
            };
            for record in records
                .iter()
                .filter(|record| record.author == routing.author)
            {
                // §13, E9: later replacements do not revoke this entry's signature.
                let Ok(key) =
                    access_key_delivery::open(&me, &record.signing, object.path.as_str(), &bytes)
                else {
                    continue;
                };
                if key.access_key_id == *access_key_id {
                    return Ok(Some(key));
                }
            }
        }
        Ok(None)
    }
}

impl SequenceSync {
    /// While an operation holds admin access, list this store's keys once
    /// retired devices could have made stray ones, and keep each key a
    /// retired device made that no admin entry names pending deletion (§13).
    /// A live device's key, recorded or still being made, is never taken.
    pub(crate) async fn sweep_stray_access_keys(
        &self,
        session: &mut Session,
        identity: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let Some(admin) = self
            .admin_access
            .lock()
            .expect("admin access poisoned")
            .values()
            .next()
            .cloned()
        else {
            return Ok(());
        };
        let state = &session.history.state;
        if !state
            .members
            .get(&identity.member())
            .is_some_and(|value| !value.removed && value.role == MemberRole::Admin)
        {
            return Ok(());
        }
        let retired: std::collections::BTreeSet<_> = state
            .devices
            .iter()
            .filter(|(_, device)| !device.active())
            .map(|(id, _)| *id)
            .collect();
        if retired.is_empty() || session.swept.as_ref() == Some(&retired) {
            return Ok(());
        }
        let deletions = admin
            .list_keys(self.store())
            .await?
            .into_iter()
            .filter(|key| {
                retired.contains(&key.device)
                    && !session.history.named_keys.contains(&key.access_key_id)
            })
            .map(|key| {
                let key = coven_database::AccessKeyToDelete {
                    access_key_id: key.access_key_id,
                    principal: key.principal,
                    member: None,
                };
                let data = serde_json::to_vec(&super::access_keys::Deletion { key: key.clone() })?;
                Ok(coven_database::AccessKeyDeletion {
                    key,
                    operation: NewOperation {
                        kind: "delete-access-key".into(),
                        data,
                        started_by: "coven".into(),
                        pauses_writes: false,
                    },
                })
            })
            .collect::<Result<Vec<_>, SyncError>>()?;
        self.database
            .record_access_key_deletions::<PendingRecord>(None, deletions)
            .await?;
        session.swept = Some(retired);
        Ok(())
    }
}

enum KeyProgress {
    Done(AccessKeyReplacement),
    Waiting(PendingReason),
}

#[cfg(test)]
#[path = "sequence_access_keys_tests.rs"]
mod tests;
