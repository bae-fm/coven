//! Operation app calls, borrowing the store-log owner's capabilities.

use super::StoreLogSync;
use crate::effects::Authority;
use crate::{
    effects,
    operation_data::{Data, EntryWork, Intent, InviteState},
    operations::{Begun, Command, Output},
    *,
};
use coven_crypto::MemberIdentity;
use coven_database::{LocalStoreLog, OperationRecord, StoreLog};
use coven_format::store_log::{MemberRole, StoreChange};
use coven_foundation::id_source::{CircleId, MemberId};

impl StoreLogSync {
    pub(crate) async fn operation_record(
        &self,
        id: OperationId,
    ) -> Result<Option<OperationRecord>, SyncError> {
        Ok(self.database.operation(id).await?)
    }
    pub(crate) async fn operation_records(&self) -> Result<Vec<OperationRecord>, SyncError> {
        Ok(self.database.operations().await?)
    }
    pub(crate) async fn begin_operation_call(
        &mut self,
        command: Command,
    ) -> Result<Begun, SyncError> {
        match command {
            Command::ForgetKeys => {
                self.store_keys.forget()?;
                self.storage = None;
                *self.finality.lock().expect("finality lock poisoned") = Default::default();
                return Ok(Begun::Value(Output::Unit));
            }
            Command::Credentials {
                previous,
                next,
                require_connected,
                commit,
            } => {
                if require_connected && self.storage.is_none() {
                    return Err(SyncError::NoStorage);
                }
                let connected = match &self.storage {
                    Some(storage) => {
                        if storage.config() != next.location {
                            return Err(coven_storage::StorageFailure::InvitationMismatch.into());
                        }
                        let previous = previous.ok_or_else(|| {
                            coven_storage::StorageFailure::InvalidConfiguration
                                .with_source("connected storage has no held credentials")
                        })?;
                        Some((storage, previous))
                    }
                    None => None,
                };
                let rollback = commit()?;
                if let Some((storage, previous)) = connected {
                    if let Err(error) =
                        crate::restore_codes::install_session(storage.as_ref(), next.credentials)
                            .await
                    {
                        let mut error = SyncError::Storage(error);
                        for result in [
                            rollback(),
                            crate::restore_codes::install_session(
                                storage.as_ref(),
                                previous.credentials,
                            )
                            .await
                            .map_err(SyncError::from),
                        ] {
                            if let Err(cleanup) = result {
                                error = SyncError::Cleanup {
                                    operation: Box::new(error),
                                    cleanup: Box::new(cleanup),
                                };
                            }
                        }
                        return Err(error);
                    }
                }
                return Ok(Begun::Value(Output::Unit));
            }
            Command::CheckKeys => {
                let local = self.database.local_store_log().await?;
                let member = self.operation_member()?;
                self.check_stopped(&local, &member)?;
                let ring = self.store_keys.read()?;
                crate::write_seal::readable_key(
                    &local.log,
                    ring.as_ref().ok_or(SyncError::KeyUnavailable(
                        crate::key_introductions::newest(&local.log, &coven_merge::Audience::Store)
                            .ok_or(SyncError::NoStorage)?,
                    ))?,
                    &coven_merge::Audience::Store,
                )?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::Storage(storage) => {
                self.storage = storage;
                *self.finality.lock().expect("finality lock poisoned") = Default::default();
                if self.storage.is_some() {
                    for pending in self.database.pending::<PendingRecord>().await? {
                        let PendingSubject::Operation { id, .. } = pending.subject else {
                            continue;
                        };
                        let record = self
                            .database
                            .operation(id)
                            .await?
                            .ok_or(coven_database::DbError::OperationChanged(id))?;
                        if matches!(pending.reason, PendingReason::NoStorage)
                            || (record.started_by == "coven"
                                && matches!(pending.reason, PendingReason::Storage { .. }))
                        {
                            self.retry_operation(&record).await?;
                        }
                    }
                }
                return Ok(Begun::Value(Output::Unit));
            }
            Command::ConfirmKey(key) => {
                self.database.clear_access_key(key).await?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::DeleteKey(id, admin) => {
                self.delete_access_key(id, admin.as_ref()).await?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::DeliveredKey(held) => {
                return Ok(Begun::Value(Output::DeliveredKey(
                    self.delivered_access_key(held.as_deref()).await?,
                )));
            }
            #[cfg(any(test, feature = "test-utils"))]
            Command::Barrier => return Ok(Begun::Value(Output::Unit)),
            #[cfg(any(test, feature = "test-utils"))]
            Command::Sync => {
                self.sync_store_log().await?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::RetryWithAdmin(id, admin) => {
                let record = self.pending_operation(id, false).await?;
                self.hold_admin_access(id, admin);
                self.retry_operation(&record).await?;
                return Ok(Begun::Operation(id));
            }
            Command::Retry(id) => {
                let record = self.pending_operation(id, false).await?;
                let data = Data::read(&record)?;
                if data.completing_reset(&record) {
                    if let Some(reload) = self.pending_reload().await? {
                        self.database.retry_operation(reload).await?;
                    }
                }
                self.retry_operation(&record).await?;
                return Ok(Begun::Operation(id));
            }
            Command::Discard(id) => {
                let record = self.pending_operation(id, true).await?;
                if let Some(begun) = self.discard_replacement(&record).await? {
                    return Ok(begun);
                }
                if let Some(begun) = self.discard_invite(&record).await? {
                    return Ok(begun);
                }
                self.release_admin_access(id);
                let data = Data::read(&record)?;
                if let Data::Snapshots(task) = &data {
                    self.discard_snapshot(&record, task.clone()).await?;
                }
                // Publication numbers cannot be abandoned. Publishing fixed bytes
                // does not perform any later access or invite step.
                if data.entry()?.is_some()
                    && self.database.local_store_log().await?.upload.is_some()
                {
                    let member = self.operation_member()?;
                    let mut local = self.database.local_store_log().await?;
                    let mut ring = self.store_keys.read()?;
                    self.publish_queued(&mut local, &member, &mut ring).await?;
                }
                if matches!(data, Data::Invite(_)) {
                    self.catch_up().await?;
                    self.finish_invite(&record).await?;
                } else {
                    self.database.finish_operation(id).await?;
                }
                return Ok(Begun::Value(Output::Unit));
            }
            _ => (),
        }
        // Local reads and recovery do not start an administrative change.
        if !matches!(command, Command::Reload) {
            self.catch_up().await?;
        }
        let local = self.database.local_store_log().await?;
        let member = self.operation_member()?;
        self.check_stopped(&local, &member)?;
        let me = member.member();
        let state = &local.log.replay.state;
        let intent = match command {
            Command::Reload => {
                Authority::Member.check(state, &me)?;
                let data = Data::Snapshots(crate::snapshot_data::SnapshotTask {
                    job: crate::snapshot_data::SnapshotJob::Reload {
                        scope: crate::snapshot_data::ReloadScope::All,
                        files: None,
                    },
                    temporary: Vec::new(),
                });
                return Ok(Begun::Operation(
                    self.database
                        .start_operation(data.new_operation("reload_from_snapshot")?)
                        .await?,
                ));
            }
            Command::Reset(audience) => {
                Authority::reset(&audience).check(state, &me)?;
                let method = if audience == coven_merge::Audience::Store {
                    "reset_store"
                } else {
                    "circles.reset"
                };
                let data = Data::Snapshots(crate::snapshot_data::SnapshotTask {
                    job: crate::snapshot_data::SnapshotJob::Write {
                        audience,
                        device: local.device,
                        trigger: crate::snapshot_data::SnapshotTrigger::Reset,
                        session: None,
                    },
                    temporary: Vec::new(),
                });
                return Ok(Begun::Operation(
                    self.database
                        .start_operation(data.new_operation(method)?)
                        .await?,
                ));
            }
            Command::SetAccess(access) => {
                let change = StoreChange::SetAccess { member: me, access };
                if !effects::validate_change(state, &me, &change)? {
                    self.make_and_upload_entry(change).await?;
                }
                return Ok(Begun::Value(Output::Unit));
            }
            Command::SetRole(target, role) => {
                self.make_and_upload_entry(StoreChange::ChangeRole {
                    member: target,
                    role,
                })
                .await?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::RemoveDevice(device) => {
                let signout = self
                    .storage
                    .as_deref()
                    .ok_or(SyncError::NoStorage)?
                    .config()
                    .provider()
                    .sign_out();
                match self
                    .make_and_upload_entry(StoreChange::RemoveDevice { device })
                    .await
                {
                    Ok(_) => (),
                    Err(SyncError::Removed) if device == local.device => {
                        let latest = self.database.local_store_log().await?;
                        if !latest
                            .log
                            .replay
                            .state
                            .devices
                            .get(&device)
                            .is_some_and(|d| d.removed())
                        {
                            return Err(SyncError::Removed);
                        }
                    }
                    Err(error) => return Err(error),
                }
                return Ok(Begun::Value(Output::SignOut(signout)));
            }
            Command::RemoveMember(target) => {
                self.publish_removal(StoreChange::RemoveMember { member: target })
                    .await?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::CreateCircle(name) => {
                Authority::Member.check(state, &me)?;
                Intent::CreateCircle {
                    circle: CircleId(self.ids.new_id()),
                    name,
                }
            }
            Command::RenameCircle(circle, name) => {
                self.make_and_upload_entry(StoreChange::RenameCircle { circle, name })
                    .await?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::DeleteCircle(circle) => {
                self.validate_administration(&local, &me, &StoreChange::DeleteCircle { circle })?;
                Intent::DeleteCircle { circle }
            }
            Command::AddCircleMember(circle, target) => {
                self.validate_administration(
                    &local,
                    &me,
                    &StoreChange::AddCircleMember {
                        circle,
                        member: target,
                    },
                )?;
                Intent::AddCircleMember {
                    circle,
                    member: target,
                }
            }
            Command::RemoveCircleMember(circle, target) => {
                self.publish_removal(StoreChange::RemoveCircleMember {
                    circle,
                    member: target,
                })
                .await?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::Invite(role, access) => {
                return self.begin_invite(&local, &me, role, access).await
            }
            Command::ReplaceAccessKey(member, admin) => {
                return self
                    .begin_replace_access_key(&local, &me, member, admin)
                    .await
            }
            Command::Approve(request) => {
                return self
                    .settle_invite(&local, &me, request.invite, Some(request), true)
                    .await
            }
            Command::Decline(request) => {
                return self
                    .settle_invite(&local, &me, request.invite, Some(request), false)
                    .await
            }
            Command::Cancel(invite) => {
                return self.settle_invite(&local, &me, invite, None, false).await
            }
            _ => unreachable!("handled command"),
        };
        let method = match &intent {
            Intent::CreateCircle { .. } => "circles.create",
            Intent::AddCircleMember { .. } => "circles.add_member",
            Intent::DeleteCircle { .. } => "circles.delete",
            Intent::Reset { .. } | Intent::RotateKey { .. } => unreachable!(),
        };
        let data = Data::Entry(EntryWork {
            intent,
            entry: None,
        });
        Ok(Begun::Operation(
            self.database
                .start_operation(data.new_operation(method)?)
                .await?,
        ))
    }

    async fn retry_operation(&self, record: &OperationRecord) -> Result<(), SyncError> {
        if let Data::ProviderAccess { account } = Data::read(record)? {
            let log = self.database.local_store_log().await?.log;
            let owner = self.owns_storage(&log, &self.operation_member()?.member());
            self.database
                .retry_provider_request::<PendingRecord>(account, record.id, owner)
                .await?;
        } else {
            self.database.retry_operation(record.id).await?;
        }
        // An app retry overrides the ordinary backoff, never a provider
        // cooldown (§16.5).
        self.waits
            .forget(coven_database::RetrySubject::Operation(record.id));
        Ok(())
    }

    pub(super) fn operation_member(&self) -> Result<MemberIdentity, SyncError> {
        self.member_keys
            .read()?
            .ok_or_else(|| coven_storage::StorageFailure::MemberKeysMissing.into())
    }

    /// Replay owns membership rejection; storage ownership is a separate provider rule.
    pub(super) fn validate_administration(
        &self,
        local: &LocalStoreLog,
        author: &MemberId,
        change: &StoreChange,
    ) -> Result<(), SyncError> {
        effects::validate_change(&local.log.replay.state, author, change)?;
        if let StoreChange::RemoveMember { member } = change {
            if let Some(target) = effects::member(&local.log.replay.state, member) {
                if matches!(
                    target.access,
                    coven_format::MemberAccess::ProviderAccount(_)
                ) && self
                    .owner_access(&local.log)
                    .is_some_and(|owner| owner.same_credential(&target.access))
                {
                    return Err(SyncError::StoreOwner);
                }
            }
        }
        Ok(())
    }

    pub(super) fn owner_access<'a>(
        &self,
        log: &'a StoreLog,
    ) -> Option<&'a coven_format::MemberAccess> {
        log.entries
            .iter()
            .find_map(|entry| match &entry.entry.change {
                StoreChange::CreateStore { access, .. }
                    if log.replay.entries.get(&entry.entry.position)
                        == Some(&coven_database::EntryOutcome::Kept) =>
                {
                    Some(access)
                }
                _ => None,
            })
    }
    pub(super) fn owns_storage(&self, log: &StoreLog, member: &MemberId) -> bool {
        let Some(m) = effects::member(&log.replay.state, member) else {
            return false;
        };
        match &m.access {
            coven_format::MemberAccess::S3AccessKey { .. } => m.role == MemberRole::Admin,
            access => self
                .owner_access(log)
                .is_some_and(|owner| owner.same_credential(access)),
        }
    }

    async fn pending_operation(
        &self,
        id: OperationId,
        discard: bool,
    ) -> Result<OperationRecord, SyncError> {
        let record = self
            .database
            .operation(id)
            .await?
            .ok_or(SyncError::NotPending(id))?;
        let data = Data::read(&record)?;
        let pending = self
            .database
            .operation_pending::<PendingRecord>(id)
            .await?
            .ok_or(SyncError::NotPending(id))?;
        let app_owned = record.started_by != "coven" || data.kind() == OperationKind::SchemaChange;
        // Provider access work can be retried, never discarded (§18).
        if !app_owned && (discard || !matches!(data, Data::ProviderAccess { .. })) {
            return Err(SyncError::NotPending(id));
        }
        if !discard
            && (pending.retry() == Retry::Never
                || (pending.retry() == Retry::AfterUpdate
                    && !self.operation_can_retry(&record).await?))
        {
            return Err(SyncError::Pending(Box::new(pending)));
        }
        Ok(record)
    }

    pub(crate) async fn current_join_requests(&self) -> Result<Vec<JoinRequest>, SyncError> {
        let mut requests = Vec::new();
        for record in self.database.operations().await? {
            if let Data::Invite(work) = Data::read(&record)? {
                if let InviteState::Waiting {
                    request: Some(request),
                    ..
                } = &work.state
                {
                    requests.push(self.present_request(&work, request)?);
                }
            }
        }
        Ok(requests)
    }
}
