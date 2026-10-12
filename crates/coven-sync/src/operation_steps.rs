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
use coven_format::{store_log::StoreChange, MemberAccess};
use coven_foundation::id_source::KeyId;
use coven_storage::MemberRemoval;

struct AccessResult {
    result: MemberRemoval,
    keys: Vec<String>,
    pending: Option<PendingReason>,
}

impl StoreLogSync {
    pub(crate) async fn operation_step(
        &mut self,
        record: &OperationRecord,
        mut data: Data,
    ) -> Result<Progress, SyncError> {
        if let Data::PublishSchema { version } = data {
            return self.schema_publication_step(record, version).await;
        }
        if let Data::Snapshots(task) = data {
            return self.snapshot_step(record, task, &mut Vec::new()).await;
        }
        if matches!(data, Data::Invite(_)) {
            return self.invite_step(record, data).await;
        }
        if let Data::Revoke { member, result } = &data {
            if let Some(result) = result
                .as_ref()
                .filter(|result| !matches!(result, MemberRemoval::AccessRemains { .. }))
            {
                let result = result.clone();
                self.database.finish_operation(record.id).await?;
                return Ok(Progress::Finished(Output::Removal(result)));
            }
            let access = self.revoke_member_access(record.id, member).await?;
            data = Data::Revoke {
                member: member.clone(),
                result: Some(access.result),
            };
            return self
                .save_access_result(record, &data, 1, access.keys, access.pending)
                .await;
        }
        if record.last_step < data.entry_step_number(5) {
            return self.entry_step(record, data).await;
        }
        let Data::Entry(work) = &mut data else {
            unreachable!()
        };
        let output = match &work.intent {
            Intent::Reset { snapshot } => {
                if let Some(id) = self.pending_reload().await? {
                    if let Some(pending) =
                        self.database.operation_pending::<PendingRecord>(id).await?
                    {
                        return Err(SyncError::Pending(Box::new(pending)));
                    }
                    return Ok(Progress::Waiting(PendingReason::Waits(
                        Prerequisite::Reload(snapshot.audience.clone()),
                    )));
                }
                Output::Unit
            }
            Intent::CreateCircle { circle, .. } => Output::CircleId(*circle),
            Intent::RemoveMember { member } => {
                if let Some(result) = work
                    .removal
                    .as_ref()
                    .filter(|result| !matches!(result, MemberRemoval::AccessRemains { .. }))
                {
                    Output::Removal(result.clone())
                } else {
                    let access = self.revoke_member_access(record.id, member).await?;
                    work.removal = Some(access.result);
                    return self
                        .save_access_result(record, &data, 6, access.keys, access.pending)
                        .await;
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
            // number or introduced key id survives a dropped attempt's restart.
            self.catch_up().await?;
            if let Some(id) = self.pending_reload().await? {
                return Ok(Progress::Waiting(self.reload_condition(id).await?));
            }
            let local = self.database.local_store_log().await?;
            let mut ring = self.store_keys.read()?;
            let mut damages = Vec::new();
            self.update_keys(&local.log, &member, &mut ring, &mut damages)
                .await?;
            if let Some(damaged) = damages.into_iter().next() {
                return Err(damaged.into());
            }
            let Some(change) =
                self.operation_change(&local, &member.member_id(), &mut data, &mut ring)?
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
                        return Ok(Progress::Waiting(PendingReason::Waits(
                            Prerequisite::OwnUploads,
                        )));
                    }
                }
            }
            crate::store_log_keys::check_shared_keys(&local.log, &change, &ring)?;
            let mut damages = Vec::new();
            let observed = crate::store_log_keys::read_copies(
                self.storage.as_deref().ok_or(SyncError::NoStorage)?,
                local.store,
                &mut damages,
            )
            .await?;
            if let Some(damaged) = damages.into_iter().next() {
                return Err(damaged.into());
            }
            let record = record.clone();
            self.database
                .prepare_operation_entry(member.member_id(), change, move |log, entry, copies| {
                    let sealed = crate::store_log_keys::seal(
                        copies,
                        log,
                        entry,
                        ring.as_ref(),
                        &member,
                        &observed,
                    )?;
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
                    crate::store_log_object::upload_keys(
                        self.directory.id(),
                        self.storage.as_deref(),
                        &self.database,
                    )
                    .await?;
                    self.database
                        .advance_operation(data.update(record, data.entry_step_number(2))?)
                        .await?;
                    return Ok(Progress::Advanced);
                }
                n if n == data.entry_step_number(2) => {
                    let bytes = crate::store_log_object::seal_upload(
                        self.directory.id(),
                        upload,
                        self.store_keys.read()?.as_ref(),
                        &member,
                    )?;
                    self.storage
                        .as_deref()
                        .ok_or(SyncError::NoStorage)?
                        .create_once(
                            &crate::store_log_object::path(self.directory.id(), entry.position),
                            &bytes,
                        )
                        .await?;
                    self.database
                        .advance_operation(data.update(record, data.entry_step_number(3))?)
                        .await?;
                    return Ok(Progress::Advanced);
                }
                n if n == data.entry_step_number(3) => {
                    let mut ring = self.store_keys.read()?;
                    let publications = crate::store_log_landing::observe_publication(
                        self.directory.id(),
                        self.storage.as_deref(),
                        &local.log,
                        entry.position,
                    )
                    .await?;
                    match self
                        .apply(
                            &mut local,
                            entry.clone(),
                            &publications,
                            &member,
                            &mut ring,
                            &mut Vec::new(),
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
            .operation(record.id)
            .await?
            .ok_or(coven_database::DbError::OperationChanged(record.id))?;
        match local.log.replay.entries.get(&entry.position) {
            Some(EntryOutcome::Kept) => {
                self.database
                    .advance_operation(data.update(&current, data.entry_step_number(5))?)
                    .await?
            }
            Some(EntryOutcome::Dropped(reason))
                if *reason != coven_database::DropReason::LandedTooLate
                    && matches!(&data, Data::Entry(work) if matches!(work.intent, Intent::Reset { .. })) =>
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
        ring: &mut Option<coven_crypto::StoreKeyring>,
    ) -> Result<Option<StoreChange>, SyncError> {
        let state = &local.log.replay.state;
        let intent = match data {
            Data::Entry(work) => &mut work.intent,
            Data::Invite(work) => {
                let InviteState::Approving { request, .. } = &work.state else {
                    unreachable!()
                };
                let request = self.decode_request(request)?;
                let change = StoreChange::AddMember {
                    keys: request.keys.clone(),
                    role: work.role,
                    access: work.access.member_access(),
                };
                self.validate_administration(local, me, &change)?;
                if let Some(existing) = effects::member(state, &request.keys.signing) {
                    if existing.sealing == request.keys.sealing
                        && existing.role == work.role
                        && same_access(&existing.access, &work.access.member_access())
                    {
                        return Ok(None);
                    }
                    return Err(SyncError::InvitationChanged);
                }
                return Ok(Some(change));
            }
            Data::Revoke { .. } | Data::Snapshots(_) | Data::PublishSchema { .. } => unreachable!(),
        };
        let change = match intent {
            Intent::Reset { snapshot } => StoreChange::Reset {
                snapshot: snapshot.clone(),
            },
            Intent::RemoveMember { member } => StoreChange::RemoveMember {
                member: member.parse()?,
            },
            Intent::CreateCircle { circle, name } => {
                effects::Authority::Member.check(state, me)?;
                let material =
                    coven_crypto::CircleKey::generate(*circle, KeyId(self.ids.new_id()))?;
                let change = StoreChange::CreateCircle {
                    circle: *circle,
                    name: name.clone(),
                    key: material.id(),
                    key_hash: material.hash(),
                };
                ring.as_mut()
                    .ok_or(SyncError::KeyUnavailable(
                        state.store.as_ref().expect("member has a store").key,
                    ))?
                    .insert_circle_key(material)?;
                change
            }
            Intent::AddCircleMember { circle, member } => StoreChange::AddCircleMember {
                circle: *circle,
                member: member.parse()?,
            },
            Intent::RemoveCircleMember { circle, member } => StoreChange::RemoveCircleMember {
                circle: *circle,
                member: member.parse()?,
            },
            Intent::DeleteCircle { circle, .. } => StoreChange::DeleteCircle { circle: *circle },
        };
        // A started operation finishes without another entry if its effect has
        // already happened (§18), even if that effect removed its authority.
        if effects::change_in_place(state, me, &change) {
            tracing::debug!(?change, "operation change is already in place");
            Ok(None)
        } else {
            self.validate_administration(local, me, &change)?;
            Ok(Some(change))
        }
    }

    pub(super) async fn save_access_result(
        &self,
        record: &OperationRecord,
        data: &Data,
        step: u32,
        keys: Vec<String>,
        reason: Option<PendingReason>,
    ) -> Result<Progress, SyncError> {
        let update = data.update(record, step)?;
        let retained = match data {
            Data::Revoke { result, .. } => result.as_ref(),
            Data::Entry(work) => work.removal.as_ref(),
            _ => None,
        };
        let pending = match reason {
            Some(reason) => Some(self.operation_condition(record, reason).await?),
            None => None,
        };
        if keys.is_empty() {
            match pending {
                Some(pending) => {
                    self.database
                        .advance_operation_pending(update, pending)
                        .await?
                }
                None => self.database.advance_operation(update).await?,
            }
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
                Data::Snapshots(_) | Data::PublishSchema { .. } => {
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
                .record_access_key_deletions(update, keys, pending)
                .await?
        }
        Ok(match retained {
            Some(MemberRemoval::AccessRemains { shares }) => {
                Progress::Reply(Err(SyncError::AccessRemains(shares.clone())))
            }
            _ => Progress::Advanced,
        })
    }

    async fn revoke_member_access(
        &self,
        operation: OperationId,
        member: &str,
    ) -> Result<AccessResult, SyncError> {
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
        let mut pending = None;
        let mut current_result = None;
        for access in accesses {
            let result = self.revoke_access(operation, &access).await?;
            match &result {
                MemberRemoval::DeleteAccessKey { access_key_id } => {
                    keys.push(access_key_id.clone())
                }
                MemberRemoval::AccessRemains { shares } => {
                    let MemberAccess::ProviderAccount(account) = &access else {
                        return Err(coven_database::DbError::DamagedDatabase.into());
                    };
                    if pending.is_none() {
                        pending = Some(PendingReason::AccessRemains {
                            account: account.clone(),
                            shares: shares.clone(),
                        });
                    }
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
        // The app call describes the replay's current access; the deletion list contains
        // every S3 key. Retained grants from any account block the whole operation.
        let result = if retained.is_empty() {
            current_result.ok_or(coven_database::DbError::DamagedDatabase)?
        } else {
            MemberRemoval::AccessRemains { shares: retained }
        };
        Ok(AccessResult {
            result,
            keys,
            pending,
        })
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
