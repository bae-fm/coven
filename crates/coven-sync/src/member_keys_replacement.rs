//! Authoring a replacement of a member's signing and sealing pair (§9, §10,
//! §18): this member's own, which pauses app writes, confirms the queue and
//! keeps a candidate in identity custody before its entry, or a pair an admin
//! prepared elsewhere, which is only the entry. Settlement is
//! `member_keys_settlement`.

use super::StoreLogSync;
use crate::{
    effects,
    operation_data::{Data, MemberKeysWork},
    operations::{Begun, Progress},
    *,
};
use coven_crypto::{MemberIdentity, MemberKeys, MemberPublicKeys};
use coven_database::{DropReason, LocalStoreLog, OperationRecord};
use coven_format::store_log::StoreChange;
use coven_foundation::id_source::MemberId;
use coven_storage::ObjectPath;

/// The held active pair is this member's current binding: only it can sign
/// a self-replacement, or cancel one without leaving the installation's
/// pause and custody behind a binding it does not hold (§10, §18).
pub(super) fn held_current(
    local: &LocalStoreLog,
    identity: &MemberIdentity,
) -> Result<(), SyncError> {
    match effects::member(&local.log.replay.state, &identity.member()) {
        None => Err(SyncError::Removed),
        Some(bound) if bound.keys == identity.keys().public_keys() => Ok(()),
        Some(_) => Err(SyncError::MemberKeysReplaced {
            member: identity.member(),
        }),
    }
}

impl StoreLogSync {
    /// Start replacing this member's own pair (`None`) or another member's
    /// with a prepared pair. A self target needs a pair this installation
    /// keeps, so it is only ever the own replacement (§9, E9).
    pub(super) async fn begin_member_keys(
        &self,
        local: &LocalStoreLog,
        member: &MemberIdentity,
        target: Option<(MemberId, MemberPublicKeys)>,
    ) -> Result<Begun, SyncError> {
        let me = member.member();
        let (data, method) = match target {
            None => {
                // A second self-replacement would wait behind the first's
                // pause and bind a candidate the first never installed.
                for record in self.database.operations().await? {
                    if matches!(Data::read(&record)?, Data::ReplaceMemberKeys(work) if work.own) {
                        return Err(coven_database::DbError::MemberKeysChanging.into());
                    }
                }
                held_current(local, member)?;
                let work = MemberKeysWork {
                    member: me,
                    own: true,
                    keys: None,
                    entry: None,
                };
                (work, "replace_member_keys")
            }
            Some((target, _)) if effects::member(&local.log.replay.state, &target).is_none() => {
                return Err(SyncError::Rejected(DropReason::TargetGone(
                    coven_database::EntryTarget::Member(target),
                )))
            }
            Some((target, _)) if target == me => {
                return Err(SyncError::Rejected(DropReason::NotAllowed))
            }
            Some((target, keys)) => {
                self.validate_administration(
                    local,
                    &me,
                    &StoreChange::ReplaceMemberKeys {
                        member: target,
                        keys,
                    },
                )?;
                let work = MemberKeysWork {
                    member: target,
                    own: false,
                    keys: Some(keys),
                    entry: None,
                };
                (work, "replace_member_keys_for")
            }
        };
        let data = Data::ReplaceMemberKeys(data);
        Ok(Begun::Operation(
            self.database
                .start_operation(data.new_operation(method)?)
                .await?,
        ))
    }

    /// Self-replacement's step before its entry. This installation's queued
    /// entry and writes are confirmed stored first, so none is left fixed
    /// under the pair about to be retired (§10). Then a fresh candidate is
    /// kept durably before anything names it; one kept by an interrupted
    /// attempt is reused.
    pub(super) async fn member_keys_prelude(
        &self,
        record: &OperationRecord,
        mut data: Data,
    ) -> Result<Progress, SyncError> {
        let local = self.database.local_store_log().await?;
        if let Some(upload) = &local.upload {
            return Ok(Progress::Waiting(PendingReason::Waits(
                Prerequisite::Object(crate::store_log_object::path(
                    self.directory.id(),
                    upload.entry.position,
                )),
            )));
        }
        if let Some(write) = self
            .database
            .unconfirmed_own_prefix(coven_merge::WriteId {
                device: local.device,
                number: u64::MAX,
            })
            .await?
        {
            return Ok(Progress::Waiting(PendingReason::Waits(
                Prerequisite::Object(crate::write_seal::path(self.directory.id(), write)),
            )));
        }
        let identity = self.operation_member()?;
        held_current(&local, &identity)?;
        let candidate = match identity.candidate() {
            Some(candidate) => candidate.public_keys(),
            None => {
                let candidate = MemberKeys::generate()?;
                let keys = candidate.public_keys();
                self.member_keys
                    .persist(&identity.with_candidate(candidate))?;
                keys
            }
        };
        let Data::ReplaceMemberKeys(work) = &mut data else {
            unreachable!("routed by kind")
        };
        work.keys = Some(candidate);
        self.database
            .advance_operation(data.update(record, 1)?)
            .await?;
        Ok(Progress::Advanced)
    }

    /// The entry a replacement publishes. The own one is signed by the held
    /// current pair; the candidate's prerequisite copies go with it (§12.2).
    pub(super) fn member_keys_change(
        &self,
        local: &LocalStoreLog,
        work: &MemberKeysWork,
    ) -> Result<StoreChange, SyncError> {
        let keys = work.keys.ok_or(coven_database::DbError::Invariant {
            what: "a replacement names its pair before its entry",
        })?;
        let member = self.operation_member()?;
        if work.own {
            held_current(local, &member)?;
        }
        let change = StoreChange::ReplaceMemberKeys {
            member: work.member,
            keys,
        };
        self.validate_administration(local, &member.member(), &change)?;
        Ok(change)
    }

    /// This installation's first snapshot sealed but not yet confirmed
    /// stored. Its bytes are signed by the held pair; once a kept replacement
    /// retires that pair they could never be sent (§10, D9), so a
    /// self-replacement reserves its entry only when there is none.
    pub(super) async fn sealed_snapshot(&self) -> Result<Option<ObjectPath>, SyncError> {
        for record in self.database.operations().await? {
            if let Data::Snapshots(crate::snapshot_data::SnapshotTask {
                job:
                    crate::snapshot_data::SnapshotJob::Write {
                        audience,
                        device,
                        attempt: Some(attempt),
                        ..
                    },
                ..
            }) = Data::read(&record)?
            {
                // Step one holds the sealed attempt until it is confirmed stored.
                if record.last_step == 1 {
                    let id = coven_format::store_log::SnapshotId {
                        audience,
                        device,
                        number: attempt.number.get(),
                    };
                    return super::snapshots::snapshot_path(self.directory.id(), &id).map(Some);
                }
            }
        }
        Ok(None)
    }

    /// While this member's own reserved replacement is unsettled, no snapshot
    /// is sealed: one signed by the pair it may retire could never be sent
    /// (§10). It waits for that entry and seals under the installed pair.
    pub(super) async fn replacement_holds_snapshots(
        &self,
    ) -> Result<Option<PendingReason>, SyncError> {
        for record in self.database.operations().await? {
            let data = Data::read(&record)?;
            if !matches!(&data, Data::ReplaceMemberKeys(work) if work.own) {
                continue;
            }
            if let Some(entry) = data.entry()? {
                return Ok(Some(PendingReason::Waits(Prerequisite::Object(
                    crate::store_log_object::path(self.directory.id(), entry.position),
                ))));
            }
        }
        Ok(None)
    }
}
