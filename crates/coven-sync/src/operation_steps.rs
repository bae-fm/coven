//! Step execution: fixed bytes, atomic journal transitions and kept-entry completion.

use super::StoreLogSync;
use crate::{
    effects,
    operation_data::{Data, Intent, InviteState},
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
use coven_storage::{MemberRemoval, RetainedAccess};

/// Every recorded access of one member, revoked: the call's summary and each
/// account whose grants remain, with those grants.
struct AccessResult {
    result: MemberRemoval,
    retained: Vec<(String, Vec<RetainedAccess>)>,
}

impl StoreLogSync {
    pub(crate) async fn operation_step(
        &mut self,
        record: &OperationRecord,
        data: Data,
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
        if let Data::DeleteAccessKey(key) = data {
            // Only explicit confirmation or a verified admin deletion clears it.
            return Ok(Progress::Waiting(PendingReason::DeleteAccessKey(key)));
        }
        if let Data::ReplaceAccessKey(work) = &data {
            if work.abandoned {
                return self.abandon_replacement(record, &data).await;
            }
            if record.last_step < data.entry_step_number(0) {
                return self.replace_key_prelude(record, data).await;
            }
            if record.last_step >= data.entry_step_number(5) {
                return self.replace_key_finish(record, data).await;
            }
        }
        if let Data::Revoke { member } = &data {
            return self.finish_revocation(record, member).await;
        }
        if let Data::RetainedAccess { account } = &data {
            let access = MemberAccess::ProviderAccount(account.clone());
            return Ok(match self.revoke_access(record.id, &access).await? {
                MemberRemoval::AccessRemains { shares } => {
                    Progress::Waiting(PendingReason::AccessRemains {
                        account: account.clone(),
                        shares,
                    })
                }
                // Removed by the owner, needed again by another member or
                // invite, or no longer this device's to revoke.
                _ => {
                    self.database.finish_operation(record.id).await?;
                    Progress::Finished(Output::Unit)
                }
            });
        }
        if record.last_step < data.entry_step_number(5) {
            return self.entry_step(record, data).await;
        }
        let Data::Entry(work) = &data else {
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
                return self.finish_revocation(record, member).await;
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
                        Ok(()) | Err(SyncError::Removed) => (),
                        Err(error) => return Err(error),
                    }
                }
                _ => return Err(coven_database::DbError::DamagedDatabase.into()),
            }
        }
        // Completion uses replay after downloading concurrent entries. Applying
        // our own removal can stop this device; its kept result is still final.
        match self.step().await {
            Ok(_) | Err(SyncError::Removed) => (),
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
            Some(EntryOutcome::Dropped(reason)) if matches!(&data, Data::ReplaceAccessKey(_)) => {
                // A lost replacement is reported, never made again with a new
                // key: its key is recorded, so it is pending deletion (§13).
                let reason = reason.clone();
                self.database.finish_operation(current.id).await?;
                self.release_admin_access(current.id);
                return Ok(Progress::Reply(Err(SyncError::Rejected(reason))));
            }
            Some(EntryOutcome::Dropped(_)) => {
                data.set_entry(None)?;
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
                    access: work.access.member_access()?,
                };
                self.validate_administration(local, me, &change)?;
                if let Some(existing) = effects::member(state, &request.keys.signing) {
                    if existing.sealing == request.keys.sealing
                        && existing.role == work.role
                        && existing
                            .access
                            .same_credential(&work.access.member_access()?)
                    {
                        return Ok(None);
                    }
                    return Err(SyncError::InvitationChanged);
                }
                return Ok(Some(change));
            }
            Data::ReplaceAccessKey(work) => {
                let change = Self::replacement_change(work)?;
                if effects::change_in_place(state, me, &change) {
                    return Ok(None);
                }
                self.validate_administration(local, me, &change)?;
                return Ok(Some(change));
            }
            Data::Revoke { .. }
            | Data::RetainedAccess { .. }
            | Data::DeleteAccessKey(_)
            | Data::Snapshots(_)
            | Data::PublishSchema { .. } => unreachable!(),
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
                        crate::key_introductions::newest(&local.log, &coven_merge::Audience::Store)
                            .expect("member has a store"),
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
            Intent::DeleteCircle { circle } => StoreChange::DeleteCircle { circle: *circle },
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

    /// Revoke a removed member's access and finish the operation in one commit
    /// with one retained-access row per account whose grants remain, so
    /// pending() names each such account (E9).
    async fn finish_revocation(
        &self,
        record: &OperationRecord,
        member: &str,
    ) -> Result<Progress, SyncError> {
        let access = self.revoke_member_access(record.id, member).await?;
        let reported_by = self.database.sync_state(Vec::new()).await?.device;
        let follow_ups = access
            .retained
            .into_iter()
            .map(|(account, shares)| {
                let data = Data::RetainedAccess {
                    account: account.clone(),
                };
                let kind = data.kind();
                Ok((data.new_operation("coven")?, move |id| PendingRecord {
                    subject: PendingSubject::Operation { id, kind },
                    reason: PendingReason::AccessRemains { account, shares },
                    reported_by,
                }))
            })
            .collect::<Result<Vec<_>, SyncError>>()?;
        self.database
            .finish_operation_with_work(record.id, follow_ups)
            .await?;
        Ok(Progress::Finished(Output::Removal(access.result)))
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
            if let Some((target, access)) = crate::access_keys::recorded(&entry.entry) {
                if *target == member && !accesses.iter().any(|a| access.same_credential(a)) {
                    accesses.push(access.clone());
                }
            }
        }
        let mut retained = Vec::new();
        let mut current_result = None;
        for access in accesses {
            let result = self.revoke_access(operation, &access).await?;
            if let MemberRemoval::AccessRemains { shares } = &result {
                let MemberAccess::ProviderAccount(account) = &access else {
                    return Err(coven_database::DbError::DamagedDatabase.into());
                };
                retained.push((account.clone(), shares.clone()));
            }
            if current.same_credential(&access) {
                current_result = Some(result);
            }
        }
        // The app call describes the replay's current access, or every
        // account's retained grants. S3 deletion work was recorded when each
        // entry applied.
        let result = if retained.is_empty() {
            current_result.ok_or(coven_database::DbError::DamagedDatabase)?
        } else {
            let mut shares = Vec::new();
            for share in retained.iter().flat_map(|(_, shares)| shares) {
                if !shares.contains(share) {
                    shares.push(share.clone());
                }
            }
            MemberRemoval::AccessRemains { shares }
        };
        Ok(AccessResult { result, retained })
    }

    pub(super) async fn revoke_access(
        &self,
        operation: OperationId,
        access: &MemberAccess,
    ) -> Result<MemberRemoval, SyncError> {
        let local = self.database.local_store_log().await?;
        let me = self.operation_member()?.member_id();
        if let MemberAccess::S3AccessKey { access_key_id, .. } = access {
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
            .any(|m| !m.removed && m.access.same_credential(access));
        for record in self.database.operations().await? {
            if record.id == operation {
                continue;
            }
            if let Data::Invite(work) = Data::read(&record)? {
                if !matches!(work.state, InviteState::Revoking | InviteState::Settled)
                    && work
                        .access
                        .member_access()
                        .is_ok_and(|recorded| recorded.same_credential(access))
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
        incoming: &[ReplayEntry],
        replay: &StoreLogReplay,
        member: &MemberKeys,
    ) -> Result<Vec<NewOperation>, SyncError> {
        let log = StoreLog {
            entries: previous
                .entries
                .iter()
                .cloned()
                .chain(incoming.iter().cloned())
                .collect(),
            replay: replay.clone(),
            horizon: previous.horizon,
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
        for incoming in incoming {
            if let Some((target, _)) = crate::access_keys::recorded(&incoming.entry) {
                if replay.state.members.get(target).is_some_and(|m| m.removed) {
                    targets.insert(target.clone());
                }
            }
        }
        // S3 keys are deletion obligations recorded with each application;
        // only shared provider accounts need a revoking operation.
        targets.retain(|target| {
            !matches!(
                replay.state.members.get(target).map(|m| &m.access),
                Some(MemberAccess::S3AccessKey { .. })
            )
        });
        let mut operations = Vec::new();
        for target in targets {
            let target = target.to_string();
            // An unfinished revocation reads the full committed log when it
            // runs and finishes in the commit that records its result.
            let pending = data.iter().any(|data| match data {
                Data::Revoke { member } => *member == target,
                Data::Entry(work) => {
                    matches!(&work.intent, Intent::RemoveMember { member } if *member == target)
                }
                _ => false,
            });
            if !pending {
                operations.push(Data::Revoke { member: target }.new_operation("coven")?);
            }
        }
        Ok(operations)
    }
}
