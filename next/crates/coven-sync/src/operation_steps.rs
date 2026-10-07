//! Step execution: fixed bytes, atomic journal transitions and kept-entry completion.

use super::{operation_calls::same_access, StoreLogSync};
use crate::{
    effects,
    operation_data::{CircleRows, Data, Intent, InviteState},
    operations::{Output, Progress},
    *,
};
use coven_crypto::{MemberId, MemberKeys};
use coven_database::{
    EntryOutcome, LocalStoreLog, NewOperation, OperationRecord, ReplayEntry, StoreLog,
    StoreLogReplay,
};
use coven_format::{
    store_log::{CircleKeyId, StoreChange},
    MemberAccess,
};
use coven_foundation::id_source::KeyId;
use coven_storage::{MemberRemoval, ObjectPath};

impl StoreLogSync {
    pub(crate) async fn operation_step(
        &mut self,
        record: &OperationRecord,
        mut data: Data,
    ) -> Result<Progress, SyncError> {
        if matches!(data, Data::Invite(_)) {
            return self.invite_step(record, data).await;
        }
        if let Data::Revoke {
            member,
            access,
            result,
        } = &data
        {
            if let Some(result) = result {
                if let MemberRemoval::AccessRemains { shares } = result {
                    return Err(SyncError::AccessRemains(shares.clone()));
                }
                let result = result.clone();
                self.database.finish_operation(record.id).await?;
                return Ok(Progress::Finished(Output::Removal(result)));
            }
            let result = self.revoke_access(record.id, access).await?;
            let key = match &result {
                MemberRemoval::DeleteAccessKey { access_key_id } => Some(access_key_id.clone()),
                _ => None,
            };
            data = Data::Revoke {
                member: member.clone(),
                access: access.clone(),
                result: Some(result),
            };
            self.save_access_result(record, &data, 1, key).await?;
            return Ok(Progress::Advanced);
        }
        if record.last_step < data.entry_step_number(5) {
            return self.entry_step(record, data).await;
        }
        let Data::Entry(work) = &mut data else {
            unreachable!()
        };
        let output = match &work.intent {
            Intent::CreateCircle { circle, .. } => Output::CircleId(*circle),
            Intent::RemoveMember { access, .. } => {
                if let Some(result) = &work.removal {
                    if let MemberRemoval::AccessRemains { shares } = result {
                        return Err(SyncError::AccessRemains(shares.clone()));
                    }
                    Output::Removal(result.clone())
                } else {
                    let result = self.revoke_access(record.id, access).await?;
                    let key = match &result {
                        MemberRemoval::DeleteAccessKey { access_key_id } => {
                            Some(access_key_id.clone())
                        }
                        _ => None,
                    };
                    work.removal = Some(result);
                    self.save_access_result(record, &data, 6, key).await?;
                    return Ok(Progress::Advanced);
                }
            }
            _ => Output::Unit,
        };
        self.database.finish_operation(record.id).await?;
        Ok(Progress::Finished(output))
    }

    pub(super) async fn entry_step(
        &mut self,
        record: &OperationRecord,
        mut data: Data,
    ) -> Result<Progress, SyncError> {
        let member = self.operation_member()?;
        if data.entry()?.is_none() {
            // Fresh authoring always uses the latest available replay. No entry
            // number or replacement key id survives a dropped attempt's restart.
            self.step().await?;
            let local = self.database.local_store_log().await?;
            let Some(change) = self.operation_change(&local, &member.member_id(), &mut data)?
            else {
                self.database
                    .advance_operation(data.update(record, data.entry_step_number(5))?)
                    .await?;
                return Ok(Progress::Advanced);
            };
            if let Data::Entry(work) = &data {
                if let Intent::DeleteCircle {
                    circle,
                    rows: CircleRows::Delete,
                } = work.intent
                {
                    let record = record.clone();
                    self.database
                        .delete_circle_rows(circle, move |write| {
                            let Data::Entry(work) = &mut data else {
                                unreachable!()
                            };
                            work.intent = Intent::DeleteCircle {
                                circle,
                                rows: CircleRows::Deleted {
                                    write: write.map(|w| (w.device, w.number)),
                                },
                            };
                            Ok(coven_database::OperationCommit::Advance(
                                coven_database::OperationUpdate {
                                    id: record.id,
                                    previous: record.last_step,
                                    last_step: 1,
                                    data: serde_json::to_vec(&data)?,
                                },
                            ))
                        })
                        .await?;
                    return Ok(Progress::Advanced);
                }
                if let Intent::DeleteCircle {
                    rows:
                        CircleRows::Deleted {
                            write: Some((device, number)),
                        },
                    ..
                } = work.intent
                {
                    if !self
                        .database
                        .write_is_uploaded(coven_merge::WriteId { device, number })
                        .await?
                    {
                        return Ok(Progress::Waiting);
                    }
                }
            }
            let mut ring = self.store_keys.unlock()?;
            let mut report = SyncReport::default();
            self.update_keys(&local.log, &member, &mut ring, &mut report)
                .await?;
            if let Some(damaged) = report.damaged_objects.into_iter().next() {
                return Err(damaged.into());
            }
            crate::store_log_keys::check_shared_keys(&local.log, &change, &ring)?;
            let record = record.clone();
            self.database
                .prepare_operation_entry(member.member_id(), change, move |log, entry| {
                    let sealed = crate::store_log_keys::seal(log, entry, ring.as_ref(), &member)?;
                    data.set_entry(Some(entry.clone()))?;
                    Ok::<_, SyncError>((sealed, data.update(&record, data.entry_step_number(1))?))
                })
                .await?;
            return Ok(Progress::Advanced);
        }
        let entry = data.entry()?.expect("fixed entry");
        let mut local = self.database.local_store_log().await?;
        if !local.log.replay.entries.contains_key(&entry.position) {
            let upload = local
                .upload
                .as_ref()
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            if upload.entry != entry {
                return Err(coven_database::DbError::StoreLogEntryChanged(entry.position).into());
            }
            match record.last_step {
                n if n == data.entry_step_number(1) => {
                    for key in &upload.sealed.keys {
                        self.storage
                            .as_deref()
                            .ok_or(SyncError::NoStorage)?
                            .create_once(&ObjectPath::parse(&key.path)?, &key.bytes)
                            .await?;
                    }
                    self.database
                        .advance_operation(data.update(record, data.entry_step_number(2))?)
                        .await?;
                    return Ok(Progress::Advanced);
                }
                n if n == data.entry_step_number(2) => {
                    self.storage
                        .as_deref()
                        .ok_or(SyncError::NoStorage)?
                        .create_once(
                            &crate::store_log_object::path(entry.position),
                            &upload.sealed.bytes,
                        )
                        .await?;
                    self.database
                        .advance_operation(data.update(record, data.entry_step_number(3))?)
                        .await?;
                    return Ok(Progress::Advanced);
                }
                n if n == data.entry_step_number(3) => {
                    let mut ring = self.store_keys.unlock()?;
                    match self
                        .apply(
                            &mut local,
                            entry.clone(),
                            &member,
                            &mut ring,
                            &mut SyncReport::default(),
                        )
                        .await
                    {
                        Ok(()) | Err(SyncError::Stopped(SyncFailure::Removed)) => (),
                        Err(error) => return Err(error),
                    }
                }
                _ => return Err(coven_database::DbError::DamagedDatabase.into()),
            }
        }
        // Completion uses replay after downloading concurrent entries. Applying
        // our own removal can stop this device; its kept result is still final.
        match self.step().await {
            Ok(_) | Err(SyncError::Stopped(SyncFailure::Removed)) => (),
            Err(error) => return Err(error),
        }
        local = self.database.local_store_log().await?;
        let current = self
            .database
            .operations()
            .await?
            .into_iter()
            .find(|r| r.id == record.id)
            .ok_or(coven_database::DbError::OperationChanged(record.id))?;
        match local.log.replay.entries.get(&entry.position) {
            Some(EntryOutcome::Kept) => {
                self.database
                    .advance_operation(data.update(&current, data.entry_step_number(5))?)
                    .await?
            }
            Some(EntryOutcome::Dropped(_)) => {
                data.set_entry(None)?;
                if let Data::Entry(work) = &mut data {
                    if let Intent::DeleteCircle { rows, .. } = &mut work.intent {
                        *rows = CircleRows::Delete;
                    }
                }
                self.database
                    .advance_operation(data.update(&current, 0)?)
                    .await?;
            }
            None => return Err(coven_database::DbError::DamagedDatabase.into()),
        }
        Ok(Progress::Advanced)
    }

    fn operation_change(
        &self,
        local: &LocalStoreLog,
        me: &MemberId,
        data: &mut Data,
    ) -> Result<Option<StoreChange>, SyncError> {
        let state = &local.log.replay.state;
        let key = || KeyId(self.ids.new_id());
        let intent = match data {
            Data::Entry(work) => &mut work.intent,
            Data::Invite(work) => {
                self.require_admin(state, me)?;
                let InviteState::Approving { request, .. } = &work.state else {
                    unreachable!()
                };
                let request = self.decode_request(request)?;
                if let Some(existing) = effects::member(state, &request.keys.signing) {
                    if existing.sealing == request.keys.sealing
                        && existing.role == work.role
                        && same_access(&existing.access, &work.access.member_access())
                    {
                        return Ok(None);
                    }
                    return Err(SyncError::InvitationChanged);
                }
                return Ok(Some(StoreChange::AddMember {
                    keys: request.keys,
                    role: work.role,
                    access: work.access.member_access(),
                }));
            }
            Data::Revoke { .. } | Data::KeepFile(_) => unreachable!(),
        };
        Ok(Some(match intent {
            Intent::RemoveMember { member, access } => {
                let target = member.parse()?;
                if effects::member(state, &target).is_none() {
                    return Ok(None);
                }
                *access = self.removal_access(local, me, &target)?;
                StoreChange::RemoveMember {
                    circle_keys: state
                        .circles
                        .iter()
                        .filter(|(_, c)| {
                            !c.deleted && c.members.contains(&target) && c.members.len() > 1
                        })
                        .map(|(circle, _)| CircleKeyId {
                            circle: *circle,
                            key: key(),
                        })
                        .collect(),
                    member: target,
                    key: key(),
                }
            }
            Intent::CreateCircle { circle, name } => {
                self.require_member(state, me)?;
                if effects::circle(state, *circle).is_some() {
                    return Ok(None);
                }
                StoreChange::CreateCircle {
                    circle: *circle,
                    name: name.clone(),
                    key: key(),
                }
            }
            Intent::AddCircleMember { circle, member } => {
                let target = member.parse()?;
                self.require_circle(state, *circle, me)?;
                self.require_member(state, &target)?;
                if state.circles[circle].members.contains(&target) {
                    return Ok(None);
                }
                StoreChange::AddCircleMember {
                    circle: *circle,
                    member: target,
                }
            }
            Intent::RemoveCircleMember { circle, member } => {
                let target = member.parse()?;
                if !effects::circle(state, *circle).is_some_and(|c| c.members.contains(&target)) {
                    return Ok(None);
                }
                self.require_circle(state, *circle, me)?;
                StoreChange::RemoveCircleMember {
                    circle: *circle,
                    member: target,
                    key: key(),
                }
            }
            Intent::DeleteCircle { circle, .. } => {
                if effects::circle(state, *circle).is_none() {
                    return Ok(None);
                }
                self.require_circle(state, *circle, me)?;
                StoreChange::DeleteCircle { circle: *circle }
            }
        }))
    }

    pub(super) async fn save_access_result(
        &self,
        record: &OperationRecord,
        data: &Data,
        step: u32,
        key: Option<String>,
    ) -> Result<(), SyncError> {
        let update = data.update(record, step)?;
        match key {
            Some(access_key_id) => {
                let member = match data {
                    Data::Revoke { member, .. } => Some(member.parse()?),
                    Data::Entry(work) => {
                        let Intent::RemoveMember { member, .. } = &work.intent else {
                            return Err(coven_database::DbError::DamagedDatabase.into());
                        };
                        Some(member.parse()?)
                    }
                    Data::Invite(_) => None,
                    Data::KeepFile(_) => {
                        return Err(coven_database::DbError::DamagedDatabase.into())
                    }
                };
                let key = AccessKeyToDelete {
                    access_key_id,
                    member,
                };
                self.database
                    .record_access_key_deletion(update, key)
                    .await?
            }
            None => self.database.advance_operation(update).await?,
        }
        Ok(())
    }

    pub(super) async fn revoke_access(
        &self,
        operation: OperationId,
        access: &MemberAccess,
    ) -> Result<MemberRemoval, SyncError> {
        let local = self.database.local_store_log().await?;
        let me = self.operation_member()?.member_id();
        if let MemberAccess::S3AccessKey { access_key_id } = access {
            return Ok(MemberRemoval::DeleteAccessKey {
                access_key_id: access_key_id.clone(),
            });
        }
        if !self.owns_storage(&local.log, &me) {
            return Ok(MemberRemoval::PendingOwner);
        }
        let mut in_use = local
            .log
            .replay
            .state
            .members
            .values()
            .any(|m| !m.removed && same_access(&m.access, access));
        for record in self.database.operations().await? {
            if record.id == operation {
                continue;
            }
            if let Data::Invite(work) = Data::read(&record)? {
                if !matches!(work.state, InviteState::Revoking | InviteState::Settled)
                    && same_access(&work.access.member_access(), access)
                {
                    in_use = true;
                }
            }
        }
        if in_use {
            let MemberAccess::ProviderAccount(account) = access else {
                unreachable!()
            };
            return Ok(MemberRemoval::AccountInUse {
                account: account.clone(),
            });
        }
        Ok(self
            .storage
            .as_deref()
            .ok_or(SyncError::NoStorage)?
            .revoke_access(access)
            .await?)
    }

    /// Record owner-side access work in the very transaction applying a kept
    /// removal. Later replay drops cannot erase an already initiated revocation.
    pub(super) async fn removal_work(
        &self,
        previous: &StoreLog,
        incoming: &ReplayEntry,
        replay: &StoreLogReplay,
        member: &MemberKeys,
    ) -> Result<Vec<NewOperation>, SyncError> {
        let log = StoreLog {
            entries: previous
                .entries
                .iter()
                .cloned()
                .chain(std::iter::once(incoming.clone()))
                .collect(),
            replay: replay.clone(),
        };
        if !self.owns_storage(&log, &member.member_id()) {
            return Ok(Vec::new());
        }
        let reserved: Vec<_> = self
            .database
            .operations()
            .await?
            .iter()
            .map(|r| Data::read(r)?.entry())
            .collect::<Result<Vec<_>, SyncError>>()?
            .into_iter()
            .flatten()
            .map(|e| e.position)
            .collect();
        let mut operations = Vec::new();
        for (id, outcome) in &replay.entries {
            if *outcome != EntryOutcome::Kept
                || previous.replay.entries.get(id) == Some(outcome)
                || reserved.contains(id)
            {
                continue;
            }
            let entry = log
                .entries
                .iter()
                .find(|e| e.entry.position == *id)
                .expect("replayed entry");
            if let StoreChange::RemoveMember { member, .. } = &entry.entry.change {
                let access = replay
                    .state
                    .members
                    .get(member)
                    .or_else(|| previous.replay.state.members.get(member))
                    .ok_or(coven_database::DbError::DamagedDatabase)?
                    .access
                    .clone();
                operations.push(
                    Data::Revoke {
                        member: member.to_string(),
                        access,
                        result: None,
                    }
                    .new_operation("coven")?,
                );
            }
        }
        Ok(operations)
    }
}
