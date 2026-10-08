//! Operation app calls, borrowing the store-log owner's capabilities.

use super::StoreLogSync;
use crate::{
    effects,
    operation_data::{CircleRows, Data, EntryWork, Intent, InviteState},
    operations::{Begun, Command, Output},
    *,
};
use coven_crypto::{MemberId, MemberKeys};
use coven_database::{LocalStoreLog, OperationRecord, StoreLog, StoreLogState};
use coven_format::store_log::{MemberRole, StoreChange};
use coven_foundation::id_source::CircleId;

impl StoreLogSync {
    pub(crate) async fn operation_records(&self) -> Result<Vec<OperationRecord>, SyncError> {
        Ok(self.database.operations().await?)
    }
    pub(crate) async fn block_operation(
        &self,
        id: OperationId,
        failure: String,
    ) -> Result<(), SyncError> {
        Ok(self.database.operation_failure(id, Some(failure)).await?)
    }

    pub(crate) async fn begin_operation_call(
        &mut self,
        command: Command,
    ) -> Result<Begun, SyncError> {
        match command {
            Command::ForgetKeys => {
                self.store_keys.forget()?;
                self.storage = None;
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
                if self
                    .storage
                    .as_ref()
                    .is_some_and(|storage| storage.config() != next.location)
                {
                    return Err(coven_storage::StorageFailure::InvitationMismatch.into());
                }
                let rollback = commit()?;
                if let Some(storage) = &self.storage {
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
                let key = local
                    .log
                    .replay
                    .state
                    .store
                    .as_ref()
                    .ok_or(SyncError::NoStorage)?
                    .key;
                self.store_keys
                    .unlock()?
                    .ok_or(SyncError::KeyUnavailable(key))?
                    .store_key(key)?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::Storage(storage) => {
                self.storage = storage;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::ConfirmKey(key) => {
                self.database.confirm_access_key_deleted(key).await?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::BlockedOperations => {
                return Ok(Begun::Value(Output::BlockedOperations(
                    self.blocked_operations().await?,
                )))
            }
            Command::AccessKeysToDelete => {
                return Ok(Begun::Value(Output::AccessKeysToDelete(
                    self.database.access_keys_to_delete().await?,
                )))
            }
            Command::Sync => {
                self.sync_store_log().await?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::Retry(id) => {
                let record = self.blocked(id).await?;
                let mut data = Data::read(&record)?;
                if data.completing_reset(&record) {
                    if let Some(reload) = self.pending_reload().await? {
                        self.database.operation_failure(reload, None).await?;
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
                    None => self.database.operation_failure(id, None).await?,
                }
                return Ok(Begun::Operation(id));
            }
            Command::Discard(id) => {
                let record = self.blocked(id).await?;
                if let Data::Snapshots(task) = Data::read(&record)? {
                    self.discard_snapshot(&record, task).await?;
                }
                // Publication numbers cannot be abandoned. Publishing fixed bytes
                // does not perform any later access or invite step.
                if Data::read(&record)?.entry()?.is_some()
                    && self.database.local_store_log().await?.upload.is_some()
                {
                    let member = self.operation_member()?;
                    let mut local = self.database.local_store_log().await?;
                    let mut ring = self.store_keys.unlock()?;
                    self.publish_queued(&mut local, &member, &mut ring, &mut Vec::new())
                        .await?;
                }
                self.database.finish_operation(id).await?;
                return Ok(Begun::Value(Output::Unit));
            }
            _ => (),
        }
        let local = self.database.local_store_log().await?;
        let member = self.operation_member()?;
        self.check_stopped(&local, &member)?;
        let me = member.member_id();
        let state = &local.log.replay.state;
        let intent = match command {
            Command::Reload => {
                self.require_member(state, &me)?;
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
                self.require_reset(state, &audience, &me)?;
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
                        .filter(|(_, c)| !c.deleted && c.members.contains(&me))
                        .map(|(id, c)| Circle {
                            id: *id,
                            name: c.name.clone(),
                        })
                        .collect(),
                )))
            }
            Command::CircleMembers(circle) => {
                self.require_circle(state, circle, &me)?;
                return Ok(Begun::Value(Output::CircleMembers(
                    state.circles[&circle]
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
                // Publish a previously reserved attempt and read concurrent removals
                // before deciding whether this key still needs a new entry.
                let damages = self.step().await?;
                if let Some(damaged) = damages.into_iter().next() {
                    return Err(damaged.into());
                }
                let latest = self.database.local_store_log().await?;
                self.require_member(&latest.log.replay.state, &me)?;
                if latest.log.replay.state.members[&me].access != access {
                    self.make_and_upload_entry(StoreChange::SetAccess { access })
                        .await?;
                }
                return Ok(Begun::Value(Output::Unit));
            }
            Command::SetRole(target, role) => {
                self.require_admin(state, &me)?;
                self.require_member(state, &target)?;
                if role != MemberRole::Admin {
                    self.require_other_admin(state, &target)?;
                }
                self.make_and_upload_entry(StoreChange::ChangeRole {
                    member: target,
                    role,
                })
                .await?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::RemoveDevice(device) => {
                let target = effects::device(state, device).ok_or(SyncError::PermissionDenied)?;
                if target.member != me {
                    self.require_admin(state, &me)?;
                }
                let signout = self
                    .storage
                    .as_deref()
                    .ok_or(SyncError::NoStorage)?
                    .sign_out();
                match self
                    .make_and_upload_entry(StoreChange::RemoveDevice { device })
                    .await
                {
                    Ok(_) => (),
                    Err(SyncError::Stopped(SyncFailure::Removed)) if device == local.device => {
                        let latest = self.database.local_store_log().await?;
                        if !latest
                            .log
                            .replay
                            .state
                            .devices
                            .get(&device)
                            .is_some_and(|d| d.removed)
                        {
                            return Err(SyncFailure::Removed.into());
                        }
                    }
                    Err(error) => return Err(error),
                }
                return Ok(Begun::Value(Output::SignOut(signout)));
            }
            Command::RemoveMember(target) => {
                self.check_removal(&local, &me, &target)?;
                Intent::RemoveMember {
                    member: target.to_string(),
                }
            }
            Command::CreateCircle(name) => {
                self.require_member(state, &me)?;
                Intent::CreateCircle {
                    circle: CircleId(self.ids.new_id()),
                    name,
                }
            }
            Command::RenameCircle(circle, name) => {
                self.require_circle(state, circle, &me)?;
                self.make_and_upload_entry(StoreChange::RenameCircle { circle, name })
                    .await?;
                return Ok(Begun::Value(Output::Unit));
            }
            Command::DeleteCircle(circle) => {
                self.require_circle(state, circle, &me)?;
                let id = self
                    .database
                    .delete_circle_rows(circle, move |write| {
                        let data = Data::Entry(EntryWork {
                            intent: Intent::DeleteCircle {
                                circle,
                                rows: CircleRows::Deleted {
                                    write: write.map(|w| (w.device, w.number)),
                                },
                            },
                            entry: None,
                            removal: None,
                        });
                        Ok(coven_database::OperationCommit::Start(
                            coven_database::NewOperation {
                                kind: data.kind().name().into(),
                                data: serde_json::to_vec(&data)?,
                                started_by: "circles.delete".into(),
                            },
                        ))
                    })
                    .await?;
                return Ok(Begun::Operation(id));
            }
            Command::AddCircleMember(circle, target) => {
                self.require_circle(state, circle, &me)?;
                self.require_member(state, &target)?;
                Intent::AddCircleMember {
                    circle,
                    member: target.to_string(),
                }
            }
            Command::RemoveCircleMember(circle, target) => {
                self.require_circle(state, circle, &me)?;
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
            Intent::DeleteCircle { .. } | Intent::Reset { .. } => unreachable!(),
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
            .unlock()?
            .ok_or_else(|| coven_storage::StorageFailure::MemberKeysMissing.into())
    }

    pub(super) fn check_removal(
        &self,
        local: &LocalStoreLog,
        author: &MemberId,
        target: &MemberId,
    ) -> Result<(), SyncError> {
        let state = &local.log.replay.state;
        self.require_admin(state, author)?;
        self.require_member(state, target)?;
        let access = &state.members[target].access;
        if matches!(access, coven_format::MemberAccess::ProviderAccount(_))
            && self
                .owner_access(&local.log)
                .is_some_and(|owner| same_access(owner, access))
        {
            return Err(SyncError::StoreOwner);
        }
        self.require_other_admin(state, target)?;
        Ok(())
    }

    pub(super) fn require_reset(
        &self,
        state: &StoreLogState,
        audience: &coven_merge::Audience,
        member: &MemberId,
    ) -> Result<(), SyncError> {
        match audience {
            coven_merge::Audience::Store => self.require_admin(state, member),
            coven_merge::Audience::Circle(circle) => self.require_circle(state, *circle, member),
        }
    }

    pub(super) fn require_admin(
        &self,
        state: &StoreLogState,
        member: &MemberId,
    ) -> Result<(), SyncError> {
        if !effects::member(state, member).is_some_and(|m| m.role == MemberRole::Admin) {
            return Err(SyncError::PermissionDenied);
        }
        Ok(())
    }
    pub(super) fn require_member(
        &self,
        state: &StoreLogState,
        member: &MemberId,
    ) -> Result<(), SyncError> {
        if effects::member(state, member).is_none() {
            return Err(SyncError::NotStoreMember(member.clone()));
        }
        Ok(())
    }
    pub(super) fn require_other_admin(
        &self,
        state: &StoreLogState,
        target: &MemberId,
    ) -> Result<(), SyncError> {
        if !state
            .members
            .iter()
            .any(|(id, m)| id != target && !m.removed && m.role == MemberRole::Admin)
        {
            return Err(SyncError::LastAdmin);
        }
        Ok(())
    }
    pub(super) fn require_circle(
        &self,
        state: &StoreLogState,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<(), SyncError> {
        let circle_state =
            effects::circle(state, circle).ok_or(SyncError::CircleDeleted(circle))?;
        if !circle_state.members.contains(member) || effects::member(state, member).is_none() {
            return Err(SyncError::CircleNotMember(circle));
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
                .is_some_and(|owner| same_access(owner, access)),
        }
    }

    async fn blocked(&self, id: OperationId) -> Result<OperationRecord, SyncError> {
        self.database
            .operations()
            .await?
            .into_iter()
            .find(|r| r.id == id && r.failure.is_some())
            .ok_or(SyncError::NotBlocked(id))
    }

    pub(crate) async fn blocked_operations(&self) -> Result<Vec<BlockedOperation>, SyncError> {
        let mut operations = Vec::new();
        for record in self.database.operations().await? {
            if let Some(failure) = &record.failure {
                operations.push(BlockedOperation {
                    id: record.id,
                    kind: Data::read(&record)?.kind(),
                    last_step: record.last_step,
                    started_by: if record.started_by == "coven" {
                        StartedBy::Coven
                    } else {
                        StartedBy::AppCall(record.started_by.clone())
                    },
                    failure: failure.clone(),
                });
            }
        }
        Ok(operations)
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

pub(super) fn same_access(a: &coven_format::MemberAccess, b: &coven_format::MemberAccess) -> bool {
    match (a, b) {
        (
            coven_format::MemberAccess::ProviderAccount(a),
            coven_format::MemberAccess::ProviderAccount(b),
        ) => a.trim().eq_ignore_ascii_case(b.trim()),
        _ => a == b,
    }
}
