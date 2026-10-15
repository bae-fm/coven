//! Step execution: fixed bytes, atomic journal transitions and kept-entry completion.

use super::StoreLogSync;
use crate::{
    effects,
    operation_data::{Data, Intent, InviteBinding, InviteState},
    operations::{Output, Progress},
    *,
};
use coven_database::{EntryOutcome, LocalStoreLog, OperationRecord};
use coven_format::store_log::StoreChange;
use coven_foundation::id_source::{KeyId, MemberId};

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
            return self.snapshot_step(record, task).await;
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
        if let Data::ProviderAccess { account } = data {
            return self.provider_access_step(record, account).await;
        }
        if let Data::ReplaceMemberKeys(work) = &data {
            // Until its entry is reserved, a self-replacement waits for every
            // sealed snapshot to be stored under the pair it is about to retire.
            if work.own && data.entry()?.is_none() {
                if let Some(path) = self.sealed_snapshot().await? {
                    return Ok(Progress::Waiting(PendingReason::Waits(
                        Prerequisite::Object(path),
                    )));
                }
            }
            if record.last_step < data.entry_step_number(0) {
                return self.member_keys_prelude(record, data).await;
            }
            if record.last_step >= data.entry_step_number(5) {
                return self.member_keys_finish(record, work).await;
            }
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
            self.update_keys(&local.log, &member, &mut ring).await?;
            let Some(change) =
                self.operation_change(&local, &member.member(), &mut data, &mut ring)?
            else {
                self.database
                    .advance_operation(data.update(record, data.entry_step_number(5))?)
                    .await?;
                return Ok(Progress::Advanced);
            };
            crate::store_log_keys::check_shared_keys(&local.log, &change, &ring)?;
            let observed = self.send.observation()?;
            let record = record.clone();
            self.database
                .prepare_operation_entry(member.member(), change, move |log, entry, copies| {
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
                .ok_or(coven_database::DbError::Invariant {
                    what: "an unapplied fixed entry is queued",
                })?;
            if upload.entry != entry {
                return Err(coven_database::DbError::StoreLogEntryChanged(entry.position).into());
            }
            match record.last_step {
                n if n == data.entry_step_number(1) => {
                    crate::store_log_object::upload_keys(
                        self.directory.id(),
                        self.storage.as_deref(),
                        &self.database,
                        &self.send,
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
                        &local.log,
                        upload,
                        self.store_keys.read()?.as_ref(),
                        &member,
                    )?;
                    crate::store_log_object::create_own(
                        self.storage.as_deref().ok_or(SyncError::NoStorage)?,
                        &self.send,
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
                        &self.database,
                        &self.reads,
                        self.download_limit,
                        self.directory.id(),
                        self.storage.as_deref(),
                        &local.log,
                        entry.position,
                    )
                    .await?;
                    // Applying a removal or a replacement of this member's
                    // keys can stop this installation; the entry's committed
                    // outcome is still final and settles the operation.
                    match self
                        .apply(&mut local, entry.clone(), &publications, &member, &mut ring)
                        .await
                    {
                        Ok(()) | Err(SyncError::Removed | SyncError::MemberKeysReplaced { .. }) => {
                        }
                        Err(error) => return Err(error),
                    }
                }
                _ => {
                    return Err(coven_database::DbError::Invariant {
                        what: "an unapplied entry's step is one of its publication steps",
                    }
                    .into())
                }
            }
        }
        // Completion uses replay after downloading concurrent entries; within
        // a pass, its first phase already did, and own publication extended
        // it (SP step 4). Applying our own removal, or a replacement of this
        // member's keys, can stop this device; its kept result is still final.
        if self.reads.membership().is_none() {
            match self.step().await {
                Ok(_) | Err(SyncError::Removed | SyncError::MemberKeysReplaced { .. }) => (),
                Err(error) => return Err(error),
            }
        }
        local = self.database.local_store_log().await?;
        let current = self
            .database
            .operation(record.id)
            .await?
            .ok_or(coven_database::DbError::OperationChanged(record.id))?;
        match local.log.replay.entries.get(&entry.position) {
            Some(EntryOutcome::Kept) => {
                // A kept self-replacement retired the held pair: install its
                // candidate before any other work signs (§10, §18).
                if let Data::ReplaceMemberKeys(work) = &data {
                    if work.own {
                        self.install_member_keys(work)?;
                    }
                }
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
            Some(EntryOutcome::Dropped(reason))
                if matches!(&data, Data::Invite(work)
                    if matches!(work.binding, InviteBinding::Replace)
                        && matches!(work.state, InviteState::Approving { .. })) =>
            {
                // A losing replacement is reported, never published again to
                // override the winner; the invite takes back only its own
                // new grants (§12.2).
                let reason = reason.clone();
                let Data::Invite(work) = &mut data else {
                    unreachable!()
                };
                work.state = InviteState::Revoking;
                self.save_invite(Some(&current), &data, data.entry_step_number(5))
                    .await?;
                return Ok(Progress::Reply(Err(SyncError::Rejected(reason))));
            }
            Some(EntryOutcome::Dropped(reason)) if matches!(&data, Data::ReplaceMemberKeys(_)) => {
                // A losing replacement is reported with its replay reason,
                // never published again over the winner (§9, E9).
                let reason = reason.clone();
                let Data::ReplaceMemberKeys(work) = &data else {
                    unreachable!()
                };
                self.reject_member_keys(&current, work).await?;
                return Ok(Progress::Reply(Err(SyncError::Rejected(reason))));
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
            None => {
                return Err(coven_database::DbError::Invariant {
                    what: "a published entry has a replay outcome",
                }
                .into())
            }
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
                let change = match &work.state {
                    InviteState::Approving { request, .. } => {
                        work.binding_change(&self.decode_request(request)?)?
                    }
                    InviteState::Recording { .. } => StoreChange::SetAccess {
                        member: work.member,
                        access: work.access.member_access()?,
                    },
                    _ => unreachable!("only approval and recording publish entries"),
                };
                // A repeated addition or a recorded key is already in place;
                // a replacement never is, and approval publishes its own (§9).
                if effects::change_in_place(state, me, &change) {
                    return Ok(None);
                }
                self.validate_administration(local, me, &change)?;
                if let StoreChange::AddMember { member, .. } = &change {
                    if effects::member(state, member).is_some() {
                        return Err(SyncError::InvitationChanged);
                    }
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
            Data::ReplaceMemberKeys(work) => {
                return self.member_keys_change(local, work).map(Some);
            }
            Data::ProviderAccess { .. }
            | Data::DeleteAccessKey(_)
            | Data::Snapshots(_)
            | Data::PublishSchema { .. } => unreachable!(),
        };
        let change = match intent {
            Intent::RotateKey { audience } => {
                effects::Authority::audience(audience).check(state, me)?;
                self.rotation_change(audience.clone(), ring)?
            }
            Intent::Reset { snapshot } => StoreChange::Reset {
                snapshot: snapshot.clone(),
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
                member: *member,
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
}
