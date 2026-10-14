//! Operation app calls, borrowing the store-log owner's capabilities.

use super::StoreLogSync;
use crate::effects::Authority;
use crate::{
    effects,
    operation_data::{Data, EntryWork, Intent, InviteState},
    operations::{Begun, Command, Output},
    *,
};
use coven_crypto::{MemberId, MemberKeys};
use coven_database::{LocalStoreLog, OperationRecord, StoreLog};
use coven_format::store_log::{MemberRole, StoreChange};
use coven_foundation::id_source::CircleId;

impl StoreLogSync {
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
                *self.finality.get_mut().expect("finality lock poisoned") = Default::default();
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
                crate::write_seal::current_store(
                    &local.log,
                    ring.as_ref().ok_or(SyncError::KeyUnavailable(
                        crate::key_introductions::newest(&local.log, &coven_merge::Audience::Store)
                            .ok_or(SyncError::NoStorage)?,
                    ))?,
                )?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::Storage(storage) => {
                self.storage = storage;
                *self.finality.get_mut().expect("finality lock poisoned") = Default::default();
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
                            self.database.retry_operation(id).await?;
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
                return Ok(Begun::Value(Output::Unit));
            }
            #[cfg(any(test, feature = "test-utils"))]
            Command::Sync => {
                self.sync_store_log().await?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::Retry(id) => {
                let record = self.pending_operation(id, false).await?;
                let mut data = Data::read(&record)?;
                if data.completing_reset(&record) {
                    if let Some(reload) = self.pending_reload().await? {
                        self.database.retry_operation(reload).await?;
                    }
                }
                let restart = match &mut data {
                    Data::Revoke { result, .. }
                        if matches!(
                            result,
                            Some(coven_storage::MemberRemoval::AccessRemains { .. })
                        ) =>
                    {
                        *result = None;
                        Some(0)
                    }
                    Data::Entry(work)
                        if matches!(
                            work.removal,
                            Some(coven_storage::MemberRemoval::AccessRemains { .. })
                        ) =>
                    {
                        work.removal = None;
                        Some(5)
                    }
                    _ => None,
                };
                match restart {
                    Some(step) => {
                        self.database
                            .advance_operation(data.update(&record, step)?)
                            .await?
                    }
                    None => self.database.retry_operation(id).await?,
                }
                return Ok(Begun::Operation(id));
            }
            Command::Discard(id) => {
                let record = self.pending_operation(id, true).await?;
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
                    self.publish_queued(&mut local, &member, &mut ring, &mut Vec::new())
                        .await?;
                }
                self.database.finish_operation(id).await?;
                return Ok(Begun::Value(Output::Unit));
            }
            _ => (),
        }
        // Local reads and recovery do not start an administrative change.
        if !matches!(
            command,
            Command::Members | Command::Circles | Command::CircleMembers(_) | Command::Reload
        ) {
            self.catch_up().await?;
        }
        let local = self.database.local_store_log().await?;
        let member = self.operation_member()?;
        self.check_stopped(&local, &member)?;
        let me = member.member_id();
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
            Command::Members => {
                return Ok(Begun::Value(Output::Members(
                    state
                        .members
                        .iter()
                        .filter(|(_, m)| !m.removed)
                        .map(|(id, m)| MemberInfo {
                            id: id.clone(),
                            role: m.role,
                            is_self: *id == me,
                            devices: state
                                .devices
                                .iter()
                                .filter(|(_, d)| !d.removed && d.member == *id)
                                .map(|(id, _)| *id)
                                .collect(),
                        })
                        .collect(),
                )))
            }
            Command::Circles => {
                return Ok(Begun::Value(Output::Circles(
                    state
                        .circles
                        .iter()
                        .filter(|(id, c)| {
                            !state.circle_deletions.contains_key(id) && c.members.contains(&me)
                        })
                        .map(|(id, c)| Circle {
                            id: *id,
                            name: c.name.clone(),
                        })
                        .collect(),
                )))
            }
            Command::CircleMembers(circle) => {
                let current = effects::circle(state, circle).ok_or(
                    coven_database::DropReason::TargetGone(coven_database::EntryTarget::Circle(
                        circle,
                    )),
                )?;
                Authority::Circle(circle).check(state, &me)?;
                return Ok(Begun::Value(Output::CircleMembers(
                    current
                        .members
                        .iter()
                        .filter(|id| effects::member(state, id).is_some())
                        .map(|id| CircleMemberInfo {
                            member: id.clone(),
                            is_self: *id == me,
                        })
                        .collect(),
                )));
            }
            Command::SetAccess(access) => {
                let change = StoreChange::SetAccess {
                    member: me.clone(),
                    access,
                };
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
                            .is_some_and(|d| d.removed)
                        {
                            return Err(SyncError::Removed);
                        }
                    }
                    Err(error) => return Err(error),
                }
                return Ok(Begun::Value(Output::SignOut(signout)));
            }
            Command::RemoveMember(target) => {
                self.validate_administration(
                    &local,
                    &me,
                    &StoreChange::RemoveMember {
                        member: target.clone(),
                    },
                )?;
                if !state.members.contains_key(&target) {
                    return Ok(Begun::Value(Output::Removal(
                        coven_storage::MemberRemoval::Revoked,
                    )));
                }
                Intent::RemoveMember {
                    member: target.to_string(),
                }
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
                        member: target.clone(),
                    },
                )?;
                Intent::AddCircleMember {
                    circle,
                    member: target.to_string(),
                }
            }
            Command::RemoveCircleMember(circle, target) => {
                self.validate_administration(
                    &local,
                    &me,
                    &StoreChange::RemoveCircleMember {
                        circle,
                        member: target.clone(),
                    },
                )?;
                Intent::RemoveCircleMember {
                    circle,
                    member: target.to_string(),
                }
            }
            Command::Invite(role, access) => {
                return self.begin_invite(&local, &me, role, access).await
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
            Intent::RemoveMember { .. } => "remove_member",
            Intent::CreateCircle { .. } => "circles.create",
            Intent::AddCircleMember { .. } => "circles.add_member",
            Intent::RemoveCircleMember { .. } => "circles.remove_member",
            Intent::DeleteCircle { .. } => "circles.delete",
            Intent::Reset { .. } => unreachable!(),
        };
        let data = Data::Entry(EntryWork {
            intent,
            entry: None,
            removal: None,
        });
        Ok(Begun::Operation(
            self.database
                .start_operation(data.new_operation(method)?)
                .await?,
        ))
    }

    pub(super) fn operation_member(&self) -> Result<MemberKeys, SyncError> {
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
        if !app_owned && (discard || !matches!(data, Data::Revoke { .. })) {
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
