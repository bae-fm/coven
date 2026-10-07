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
        report: &mut SyncReport,
    ) -> Result<Progress, SyncError> {
        if let Data::PublishSchema { version } = data {
            return self.schema_publication_step(record, version).await;
        }
        if let Data::Snapshots(task) = data {
            return self.snapshot_step(record, task, report).await;
        }
        if matches!(data, Data::Invite(_)) {
            return self.invite_step(record, data).await;
        }
        if let Data::Revoke { member, result } = &data {
            if let Some(result) = result {
                if let MemberRemoval::AccessRemains { shares } = result {
                    return Err(SyncError::AccessRemains(shares.clone()));
                }
                let result = result.clone();
                self.database.finish_operation(record.id).await?;
                return Ok(Progress::Finished(Output::Removal(result)));
            }
            let (result, keys) = self.revoke_member_access(record.id, member).await?;
            data = Data::Revoke {
                member: member.clone(),
                result: Some(result),
            };
            self.save_access_result(record, &data, 1, keys).await?;
            return Ok(Progress::Advanced);
        }
        if record.last_step < data.entry_step_number(5) {
            return self.entry_step(record, data).await;
        }
        let Data::Entry(work) = &mut data else {
            unreachable!()
        };
        let output = match &work.intent {
            Intent::Reset { .. } => {
                if let Some(id) = self.pending_reload().await? {
                    let reload = self
                        .database
                        .operations()
                        .await?
                        .into_iter()
                        .find(|r| r.id == id)
                        .ok_or(coven_database::DbError::OperationChanged(id))?;
                    if let Some(failure) = reload.failure {
                        return Err(SyncError::RecoveryBlocked {
                            operation: id,
                            failure,
                        });
                    }
                    return Ok(Progress::Waiting);
                }
                Output::Unit
            }
            Intent::CreateCircle { circle, .. } => Output::CircleId(*circle),
            Intent::RemoveMember { member } => {
                if let Some(result) = &work.removal {
                    if let MemberRemoval::AccessRemains { shares } = result {
                        return Err(SyncError::AccessRemains(shares.clone()));
                    }
                    Output::Removal(result.clone())
                } else {
                    let (result, keys) = self.revoke_member_access(record.id, member).await?;
                    work.removal = Some(result);
                    self.save_access_result(record, &data, 6, keys).await?;
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
            if self.pending_reload().await?.is_some() {
                return Ok(Progress::Waiting);
            }
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
                            .create_once(
                                &ObjectPath::parse(&key.path)
                                    .map_err(coven_storage::StorageError::from)?,
                                &key.bytes,
                            )
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
            Some(EntryOutcome::Dropped(_)) if matches!(&data, Data::Entry(work) if matches!(work.intent, Intent::Reset { .. })) =>
            {
                // A competing reset settles this request. Reauthoring it after
                // reading the winner would override §19.3's timestamp choice.
                self.database
                    .advance_operation(data.update(&current, data.entry_step_number(5))?)
                    .await?;
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
            Data::Revoke { .. }
            | Data::KeepFile(_)
            | Data::Snapshots(_)
            | Data::PublishSchema { .. } => unreachable!(),
        };
        Ok(Some(match intent {
            Intent::Reset { snapshot } => {
                self.require_reset(state, &snapshot.audience, me)?;
                StoreChange::Reset {
                    snapshot: snapshot.clone(),
                }
            }
            Intent::RemoveMember { member } => {
                let target = member.parse()?;
                if effects::member(state, &target).is_none() {
                    return Ok(None);
                }
                self.check_removal(local, me, &target)?;
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
        keys: Vec<String>,
    ) -> Result<(), SyncError> {
        let update = data.update(record, step)?;
        if keys.is_empty() {
            self.database.advance_operation(update).await?;
        } else {
            let member = match data {
                Data::Revoke { member, .. } => Some(member.parse()?),
                Data::Entry(work) => {
                    let Intent::RemoveMember { member, .. } = &work.intent else {
                        return Err(coven_database::DbError::DamagedDatabase.into());
                    };
                    Some(member.parse()?)
                }
                Data::Invite(_) => None,
                Data::KeepFile(_) | Data::Snapshots(_) | Data::PublishSchema { .. } => {
                    return Err(coven_database::DbError::DamagedDatabase.into())
                }
            };
            let keys = keys
                .into_iter()
                .map(|access_key_id| AccessKeyToDelete {
                    access_key_id,
                    member: member.clone(),
                })
                .collect();
            self.database
                .record_access_key_deletions(update, keys)
                .await?
        }
        Ok(())
    }

    async fn revoke_member_access(
        &self,
        operation: OperationId,
        member: &str,
    ) -> Result<(MemberRemoval, Vec<String>), SyncError> {
        // Replay decides membership, but dropping an access entry cannot undo
        // its provider grant. Revoke every recorded access, including late entries.
        let local = self.database.local_store_log().await?;
        let member: MemberId = member.parse()?;
        let current = &local
            .log
            .replay
            .state
            .members
            .get(&member)
            .ok_or(coven_database::DbError::DamagedDatabase)?
            .access;
        let mut accesses = Vec::new();
        for entry in &local.log.entries {
            if let Some((target, access)) = recorded_access(&entry.entry) {
                if *target == member && !accesses.iter().any(|a| same_access(a, access)) {
                    accesses.push(access.clone());
                }
            }
        }
        let mut keys = Vec::new();
        let mut retained = Vec::new();
        let mut current_result = None;
        for access in accesses {
            let result = self.revoke_access(operation, &access).await?;
            match &result {
                MemberRemoval::DeleteAccessKey { access_key_id } => {
                    keys.push(access_key_id.clone())
                }
                MemberRemoval::AccessRemains { shares } => {
                    for share in shares {
                        if !retained.contains(share) {
                            retained.push(share.clone());
                        }
                    }
                }
                _ => (),
            }
            if same_access(current, &access) {
                current_result = Some(result);
            }
        }
        // The app call describes the replay's current access; its report contains
        // every S3 key. Retained grants from any account block the whole operation.
        let result = if retained.is_empty() {
            current_result.ok_or(coven_database::DbError::DamagedDatabase)?
        } else {
            MemberRemoval::AccessRemains { shares: retained }
        };
        Ok((result, keys))
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

    /// Record access work atomically with a kept removal or a later access entry
    /// for a removed member. Dropping either entry cannot undo provider access.
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
        let records = self.database.operations().await?;
        let data = records
            .iter()
            .map(Data::read)
            .collect::<Result<Vec<_>, _>>()?;
        let reserved: Vec<_> = data
            .iter()
            .map(Data::entry)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .map(|e| e.position)
            .collect();
        let mut targets = std::collections::BTreeSet::new();
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
                targets.insert(member.clone());
            }
        }
        if let Some((target, _)) = recorded_access(&incoming.entry) {
            if replay.state.members.get(target).is_some_and(|m| m.removed) {
                targets.insert(target.clone());
            }
        }
        let mut operations = Vec::new();
        for target in targets {
            let target = target.to_string();
            // Unfinished revocations read the full committed log when they run.
            // A step whose result is already fixed cannot absorb a later access.
            let pending = data.iter().any(|data| match data {
                Data::Revoke {
                    member,
                    result: None,
                } => *member == target,
                Data::Entry(work) if work.removal.is_none() => {
                    matches!(&work.intent, Intent::RemoveMember { member } if *member == target)
                }
                _ => false,
            });
            if !pending {
                operations.push(
                    Data::Revoke {
                        member: target,
                        result: None,
                    }
                    .new_operation("coven")?,
                );
            }
        }
        Ok(operations)
    }
}

/// An addition's access belongs to its named member; an update's to its signer.
fn recorded_access(
    entry: &coven_format::store_log::StoreLogEntry,
) -> Option<(&MemberId, &MemberAccess)> {
    match &entry.change {
        StoreChange::CreateStore { admin, access, .. } => Some((&admin.signing, access)),
        StoreChange::AddMember { keys, access, .. } => Some((&keys.signing, access)),
        StoreChange::SetAccess { access } => Some((&entry.author, access)),
        _ => None,
    }
}
