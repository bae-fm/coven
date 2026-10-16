//! Replacing a member's S3 key from the desktop holding admin access (§13):
//! make the key with admin access and check it from the member's side in one
//! call, deliver it sealed to the member, record it for them in the store log,
//! then delete the old key and read that it is gone. Recording makes the old key
//! pending deletion in the same transaction (§13), so a failed deletion
//! leaves it listed rather than forgotten.

use super::StoreLogSync;
use crate::{
    operation_data::{Data, ReplaceKeyWork},
    operations::{Begun, Output, Progress},
    *,
};
use coven_database::{LocalStoreLog, OperationRecord};
use coven_format::{
    access_key_delivery::{self, DeliveredKey},
    store_log::StoreChange,
    MemberAccess, S3Principal,
};
use coven_foundation::id_source::MemberId;
use coven_storage::{AccessKeyAdmin, MintScope, ObjectPath, ObjectPrefix, S3MemberKey};
use std::sync::Arc;

impl StoreLogSync {
    /// Start a replacement after checking §9's authority for recording it.
    pub(super) async fn begin_replace_access_key(
        &mut self,
        local: &LocalStoreLog,
        me: &MemberId,
        member: MemberId,
        admin: Arc<dyn AccessKeyAdmin>,
    ) -> Result<Begun, SyncError> {
        let state = &local.log.replay.state;
        let current = crate::effects::member(state, &member).ok_or(SyncError::Rejected(
            coven_database::DropReason::TargetGone(coven_database::EntryTarget::Member(member)),
        ))?;
        if !matches!(current.access, MemberAccess::S3AccessKey { .. }) {
            return Err(coven_storage::StorageFailure::InvalidConfiguration
                .with_source("only S3 member keys are replaced with admin access")
                .into());
        }
        // The member's own replacement, or an admin's for another member.
        crate::effects::validate_change(
            state,
            me,
            local.device,
            &StoreChange::SetAccess {
                member,
                access: MemberAccess::S3AccessKey {
                    access_key_id: String::new(),
                    principal: S3Principal::Console,
                },
            },
        )?;
        let attempt = self.ids.new_id();
        let data = Data::ReplaceAccessKey(ReplaceKeyWork {
            member,
            attempt,
            scope: MintScope::new(local.store, &member.to_string(), local.device, attempt),
            minted: None,
            delivery: None,
            entry: None,
        });
        let operation = self
            .database
            .start_operation(data.new_operation("replace_member_access_key")?)
            .await?;
        self.hold_admin_access(operation, admin);
        Ok(Begun::Operation(operation))
    }

    /// Steps before the entry: make and check the key, then deliver it.
    pub(super) async fn replace_key_prelude(
        &mut self,
        record: &OperationRecord,
        mut data: Data,
    ) -> Result<Progress, SyncError> {
        let Data::ReplaceAccessKey(work) = &mut data else {
            unreachable!()
        };
        if record.last_step == 0 {
            let Some(admin) = self.admin_access(record.id) else {
                return Ok(Progress::Waiting(PendingReason::AdminAccessRequired));
            };
            let minted = self.mint_checked(admin.as_ref(), &work.scope).await;
            if minted.is_err() {
                // A failed call needs admin access supplied again (§4).
                self.release_admin_access(record.id);
            }
            work.minted = Some(minted?);
            self.database
                .advance_operation(data.update(record, 1)?)
                .await?;
            return Ok(Progress::Advanced);
        }
        let member = work.member;
        let path = ObjectPath::access_key(self.directory.id(), &member, work.attempt);
        let Some(bytes) = &work.delivery else {
            // Seal once; retries resend these exact bytes.
            let local = self.database.local_store_log().await?;
            let sealing = crate::effects::member(&local.log.replay.state, &member)
                .ok_or(SyncError::Rejected(coven_database::DropReason::TargetGone(
                    coven_database::EntryTarget::Member(member),
                )))?
                .keys
                .sealing;
            let key = minted(work)?;
            let delivered = DeliveredKey {
                access_key_id: key.credentials.access_key_id.clone(),
                secret_access_key: key.credentials.secret_access_key.clone(),
                principal: key.principal.clone(),
            };
            let sealed = access_key_delivery::seal(
                &self.operation_member()?,
                &member,
                &sealing,
                path.as_str(),
                &delivered,
            )
            .map_err(|error| SyncError::Failed {
                failure: LocalFailure::Crypto,
                source: Arc::new(error),
            })?;
            work.delivery = Some(sealed);
            self.database
                .advance_operation(data.update(record, 1)?)
                .await?;
            return Ok(Progress::Advanced);
        };
        // A lost reply may have stored these bytes. The attempt id is this
        // device's own, so different bytes mean another copy of it (§10).
        crate::store_log_object::create_own(
            self.storage.as_deref().ok_or(SyncError::NoStorage)?,
            &self.send,
            &path,
            bytes,
        )
        .await?;
        self.database
            .advance_operation(data.update(record, 2)?)
            .await?;
        Ok(Progress::Advanced)
    }

    /// The entry recording the new key for the member.
    pub(super) fn replacement_change(work: &ReplaceKeyWork) -> Result<StoreChange, SyncError> {
        let key = minted(work)?;
        Ok(StoreChange::SetAccess {
            member: work.member,
            access: MemberAccess::S3AccessKey {
                access_key_id: key.credentials.access_key_id.clone(),
                principal: key.principal.clone(),
            },
        })
    }

    /// After the entry is kept: delete the key it replaced in its recorded
    /// view, with admin access, and report the outcome with the new key.
    pub(super) async fn replace_key_finish(
        &mut self,
        record: &OperationRecord,
        data: Data,
    ) -> Result<Progress, SyncError> {
        let Data::ReplaceAccessKey(work) = &data else {
            unreachable!()
        };
        let key = minted(work)?.clone();
        let entry = data.entry()?.ok_or(coven_database::DbError::Invariant {
            what: "a key replacement publishing its entry has it fixed",
        })?;
        let local = self.database.local_store_log().await?;
        let positions = crate::replay::author_positions(&entry);
        let view = crate::replay::at(&local.log, &positions).state;
        let member = work.member;
        let old = crate::effects::member(&view, &member)
            .map(|member| member.access.clone())
            .ok_or(SyncError::Rejected(coven_database::DropReason::TargetGone(
                coven_database::EntryTarget::Member(member),
            )))?;
        let old_key = match old {
            MemberAccess::S3AccessKey { access_key_id, .. }
                if access_key_id != key.credentials.access_key_id =>
            {
                match self.access_key_operation(&access_key_id).await? {
                    None => OldKeyDeletion::Deleted,
                    Some((operation, principal)) => match principal {
                        S3Principal::Console => OldKeyDeletion::Console { operation },
                        _ => match self.admin_access(record.id) {
                            // After a restart, deleting needs admin access again.
                            None => {
                                return Ok(Progress::Waiting(PendingReason::AdminAccessRequired))
                            }
                            Some(admin) => {
                                match self.delete_access_key(operation, admin.as_ref()).await {
                                    Ok(()) => OldKeyDeletion::Deleted,
                                    Err(error) => OldKeyDeletion::Failed { operation, error },
                                }
                            }
                        },
                    },
                }
            }
            // Recording the same key again replaces no credential.
            _ => OldKeyDeletion::Deleted,
        };
        self.database.finish_operation(record.id).await?;
        self.release_admin_access(record.id);
        Ok(Progress::Finished(Output::AccessKeyReplaced(
            AccessKeyReplacement { key, old_key },
        )))
    }

    /// Discarding a replacement (§13, §18). A reserved entry is published by
    /// the generic discard and the store log covers its key, so this returns
    /// `None`. Before that the row just goes: another live copy of this
    /// database may still record the made key, so this one takes nothing, and
    /// §13's reclaim removes it once this device is retired (KeyListing
    /// `clone_discard_spares_recorded_key`).
    pub(super) async fn discard_replacement(
        &mut self,
        record: &OperationRecord,
    ) -> Result<Option<Begun>, SyncError> {
        let Data::ReplaceAccessKey(work) = Data::read(record)? else {
            return Ok(None);
        };
        if work.entry.is_some() {
            return Ok(None);
        }
        self.database.finish_operation(record.id).await?;
        self.release_admin_access(record.id);
        Ok(Some(Begun::Value(Output::Unit)))
    }

    /// The open deletion obligation for a key id, and the principal it names.
    async fn access_key_operation(
        &self,
        access_key_id: &str,
    ) -> Result<Option<(OperationId, S3Principal)>, SyncError> {
        for record in self.database.operations().await? {
            if let Data::DeleteAccessKey(key) = Data::read(&record)? {
                if key.access_key_id == access_key_id {
                    return Ok(Some((record.id, key.principal)));
                }
            }
        }
        Ok(None)
    }

    /// Delete the key a deletion obligation names with admin access, read the
    /// provider for that exact key, and clear the obligation only on absence.
    pub(super) async fn delete_access_key(
        &self,
        id: OperationId,
        admin: &dyn AccessKeyAdmin,
    ) -> Result<(), SyncError> {
        let record = self
            .database
            .operation(id)
            .await?
            .ok_or(SyncError::NotPending(id))?;
        let Data::DeleteAccessKey(key) = Data::read(&record)? else {
            return Err(SyncError::NotPending(id));
        };
        let deleted = admin.revoke(&key.access_key_id, &key.principal).await;
        // A read follows only an accepted deletion.
        let observation = match &deleted {
            Ok(()) => Some(crate::access_keys::KeyObservation {
                access_key_id: key.access_key_id.clone(),
                reply: admin
                    .key_exists(&key.access_key_id, &key.principal)
                    .await
                    .map(|present| match present {
                        true => crate::access_keys::KeyPresence::Present,
                        false => crate::access_keys::KeyPresence::Absent,
                    }),
            }),
            Err(_) => None,
        };
        crate::access_keys::verify_deletion(&key.access_key_id, deleted, observation)?;
        self.database.clear_access_key(key.access_key_id).await?;
        Ok(())
    }

    /// The key an admin delivered to this member that the store log records as
    /// their current access, signed by the admin who recorded it (§13). A
    /// device already holding the recorded key (`held`) reads nothing.
    pub(crate) async fn delivered_access_key(
        &self,
        held: Option<&str>,
    ) -> Result<Option<DeliveredKey>, SyncError> {
        let Some(storage) = self.storage.as_deref() else {
            return Ok(None);
        };
        let me = self.operation_member()?;
        let local = self.database.local_store_log().await?;
        let Some(current) = crate::effects::member(&local.log.replay.state, &me.member()) else {
            return Ok(None);
        };
        let MemberAccess::S3AccessKey { access_key_id, .. } = &current.access else {
            return Ok(None);
        };
        if held == Some(access_key_id.as_str()) {
            return Ok(None);
        }
        // The authors of kept entries recording this key for this member.
        let recorders: Vec<MemberId> = local
            .log
            .entries
            .iter()
            .filter(|entry| {
                local.log.replay.entries.get(&entry.entry.position)
                    == Some(&coven_database::EntryOutcome::Kept)
            })
            .filter_map(|entry| match &entry.entry.change {
                StoreChange::SetAccess {
                    member,
                    access:
                        MemberAccess::S3AccessKey {
                            access_key_id: id, ..
                        },
                } if *member == me.member() && id == access_key_id => Some(entry.entry.author),
                _ => None,
            })
            .collect();
        if recorders.is_empty() {
            return Ok(None);
        }
        let prefix = ObjectPrefix::access_keys(self.directory.id(), &me.member());
        for object in storage.list(&prefix).await? {
            let bytes = storage.read(&object.path).await?;
            let Ok(routing) = access_key_delivery::routing(&bytes) else {
                continue;
            };
            if !recorders.contains(&routing.author) {
                continue;
            }
            // The recorder's current signing key verifies the delivery (D9).
            let Some(author) = local.log.replay.state.members.get(&routing.author) else {
                continue;
            };
            let Ok(key) =
                access_key_delivery::open(&me, &author.keys.signing, object.path.as_str(), &bytes)
            else {
                continue;
            };
            if key.access_key_id == *access_key_id {
                return Ok(Some(key));
            }
        }
        Ok(None)
    }
}

fn minted(work: &ReplaceKeyWork) -> Result<&S3MemberKey, SyncError> {
    work.minted.as_ref().ok_or_else(|| {
        coven_database::DbError::Invariant {
            what: "a key replacement past minting holds its checked key",
        }
        .into()
    })
}

#[cfg(test)]
#[path = "operation_access_keys_tests.rs"]
mod tests;
