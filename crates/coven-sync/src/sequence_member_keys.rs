//! Replacing a member's signing and sealing pair (§10.1, §12.3, §11.5): this
//! member's own, which pauses app writes, confirms the queue and keeps a
//! candidate before claiming its copies and binding, or a prepared public
//! pair for another member, which is only the entry and its copies.

use super::{sequence_operations::Request, SequenceSync, Session};
use crate::{
    OperationKind, OperationRejection, PendingReason, PendingRecord, PendingSubject, Prerequisite,
    SyncError,
};
use coven_crypto::{MemberIdentity, MemberKeys, MemberPublicKeys, StoreKeyring};
use coven_database::{NewOperation, OperationId, OperationUpdate};
use coven_format::admin::StoreChange;
use coven_foundation::id_source::MemberId;
use coven_storage::StorageFailure;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub(crate) const MEMBER_KEYS: &str = "member-keys";

/// Self-replacement keeps the write pause through the restore-code commit.
#[derive(Serialize, Deserialize)]
enum MemberKeysWork {
    Draining,
    Replacing(#[serde(with = "crate::pending_encoding::public_keys")] MemberPublicKeys),
    RefreshingCode,
}

/// This member's held active pair, or its kept candidate, must be its
/// current binding: only those sign or install the replacement (§12.3).
fn held_current(session: &Session, identity: &MemberIdentity) -> Result<(), SyncError> {
    match session.history.state.members.get(&identity.member()) {
        None => Err(SyncError::Rejected(OperationRejection::TargetGone(
            crate::EntryTarget::Member(identity.member()),
        ))),
        Some(current) if current.removed => Err(SyncError::Removed),
        Some(current)
            if current.keys == identity.keys().public_keys()
                || identity
                    .candidate()
                    .is_some_and(|keys| keys.public_keys() == current.keys) =>
        {
            Ok(())
        }
        Some(_) => Err(SyncError::MemberKeysReplaced {
            member: identity.member(),
        }),
    }
}

impl SequenceSync {
    /// Replace this member's own pair and return the installed identity.
    /// App writes wait with `MemberKeysChanging` until the binding applies
    /// and identity and restore-code custody finish, or replacement fails (§12.3, E9).
    pub async fn replace_member_keys(self: &Arc<Self>) -> Result<MemberIdentity, SyncError> {
        let mut session = self.session.lock().await;
        let identity = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &identity).await?;
        self.read_forward(&mut session, &mut ring, &identity)
            .await?;
        self.settle_open(&mut session, &mut ring, &identity).await?;
        if !self.registered(&session) {
            return Err(SyncError::Rejected(OperationRejection::NotAllowed));
        }
        let id = match self
            .database
            .operations()
            .await?
            .into_iter()
            .find(|record| record.kind == MEMBER_KEYS)
        {
            Some(record) => record.id,
            None => {
                held_current(&session, &identity)?;
                self.database
                    .start_operation(NewOperation {
                        kind: MEMBER_KEYS.into(),
                        data: serde_json::to_vec(&MemberKeysWork::Draining)?,
                        started_by: "replace_member_keys".into(),
                        pauses_writes: true,
                    })
                    .await?
            }
        };
        match self.advance_member_keys(&mut session, &mut ring, id).await {
            Ok(Some(identity)) => Ok(identity),
            Ok(None) => unreachable!("a self-replacement call runs to its binding"),
            Err(error) => {
                if self.database.operation(id).await?.is_some() {
                    self.record_operation_error(
                        id,
                        OperationKind::ReplaceMemberKeys,
                        &error,
                        &ring,
                    )
                    .await?;
                }
                Err(error)
            }
        }
    }

    /// Complete code custody before releasing this replacement's write pause.
    /// Background attempts honor the operation's wait; its initiating call
    /// reaches this step immediately after installing the selected identity.
    pub(crate) async fn finish_member_keys(
        &self,
        foreground: bool,
        refresh: impl FnOnce(
            &MemberIdentity,
            &coven_database::StoreIdentity,
        ) -> Result<coven_crypto::SecretText, SyncError>,
    ) -> Result<Option<coven_crypto::SecretText>, SyncError> {
        let mut session = self.session.lock().await;
        let Some(record) = self
            .database
            .operations()
            .await?
            .into_iter()
            .find(|record| record.kind == MEMBER_KEYS)
        else {
            return Ok(None);
        };
        let work: MemberKeysWork = serde_json::from_slice(&record.data)?;
        if !matches!(work, MemberKeysWork::RefreshingCode) {
            return Ok(None);
        }
        let identity = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        let ring = self.ring(&mut session, &identity).await?;
        if !foreground && !self.operation_ready(&record, &ring).await? {
            return Ok(None);
        }
        let store = session
            .history
            .state
            .store
            .as_ref()
            .ok_or(SyncError::NoStorage)?;
        match refresh(&identity, store) {
            Ok(code) => {
                self.database.finish_operation(record.id).await?;
                self.waits.clear_evidence(record.id);
                self.waits
                    .forget(coven_database::RetrySubject::Operation(record.id));
                Ok(Some(code))
            }
            Err(error) => {
                self.record_operation_error(
                    record.id,
                    OperationKind::ReplaceMemberKeys,
                    &error,
                    &ring,
                )
                .await?;
                Err(error)
            }
        }
    }

    /// Publish a prepared public pair for another active member, with copies
    /// of every store key and held keys of that member's circles first (§10, E9).
    pub async fn replace_member_keys_for(
        self: &Arc<Self>,
        member: MemberId,
        keys: MemberPublicKeys,
    ) -> Result<(), SyncError> {
        let expected = {
            let mut session = self.session.lock().await;
            let identity = self
                .member
                .read()?
                .ok_or(StorageFailure::MemberKeysMissing)?;
            let mut ring = self.ring(&mut session, &identity).await?;
            self.read_forward(&mut session, &mut ring, &identity)
                .await?;
            // Replacing this installation's own pair is a self-replacement.
            if member == identity.member() {
                return Err(SyncError::Rejected(OperationRejection::NotAllowed));
            }
            session
                .history
                .state
                .members
                .get(&member)
                .filter(|value| !value.removed)
                .ok_or(SyncError::Rejected(OperationRejection::TargetGone(
                    crate::EntryTarget::Member(member),
                )))?
                .keys
        };
        self.administer(StoreChange::ReplaceMemberKeys {
            member,
            expected,
            keys,
        })
        .await
    }

    /// Resume an interrupted self-replacement in a pass.
    pub(crate) async fn resume_member_keys(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
    ) -> Result<(), SyncError> {
        for record in self.database.operations().await? {
            if record.kind != MEMBER_KEYS || !self.operation_ready(&record, ring).await? {
                continue;
            }
            if let Err(error) = self.advance_member_keys(session, ring, record.id).await {
                if self.database.operation(record.id).await?.is_some() {
                    self.record_operation_error(
                        record.id,
                        OperationKind::ReplaceMemberKeys,
                        &error,
                        ring,
                    )
                    .await?;
                }
                self.state.check()?;
            }
        }
        Ok(())
    }

    async fn advance_member_keys(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        id: OperationId,
    ) -> Result<Option<MemberIdentity>, SyncError> {
        loop {
            let record = self
                .database
                .operation(id)
                .await?
                .ok_or(SyncError::NotPending(id))?;
            let mut work: MemberKeysWork = serde_json::from_slice(&record.data)?;
            let identity = self
                .member
                .read()?
                .ok_or(StorageFailure::MemberKeysMissing)?;
            let candidate = match work {
                MemberKeysWork::RefreshingCode => return Ok(Some(identity)),
                MemberKeysWork::Replacing(candidate) => Some(candidate),
                MemberKeysWork::Draining => None,
            };
            let Some(candidate) = candidate else {
                // Writes fixed under the pair about to be retired must reach
                // storage first: after the binding they count for nothing (§12.3).
                if let Some(last) = self.database.waiting_snapshot_headers().await?.last() {
                    let through = last.header.position.number;
                    self.send_writes(session, ring, &identity, through).await?;
                    if self
                        .database
                        .waiting_snapshot_headers()
                        .await?
                        .last()
                        .is_some()
                    {
                        return Err(SyncError::Pending(Box::new(PendingRecord::new(
                            PendingSubject::Operation {
                                id,
                                kind: OperationKind::ReplaceMemberKeys,
                            },
                            PendingReason::Waits(Prerequisite::SequenceCatchUp),
                        ))));
                    }
                }
                held_current(session, &identity)?;
                let candidate = match identity.candidate() {
                    Some(candidate) => candidate.public_keys(),
                    None => {
                        let candidate = MemberKeys::generate()?;
                        let keys = candidate.public_keys();
                        self.member.persist(&identity.with_candidate(candidate))?;
                        keys
                    }
                };
                work = MemberKeysWork::Replacing(candidate);
                self.database
                    .advance_operation(OperationUpdate {
                        id,
                        previous: record.last_step,
                        last_step: record.last_step + 1,
                        data: serde_json::to_vec(&work)?,
                    })
                    .await?;
                continue;
            };
            let current = session
                .history
                .state
                .members
                .get(&identity.member())
                .map(|value| value.keys);
            if current == Some(candidate) {
                // The binding applied: install its pair, keeping the previous
                // one for copies that name it (§12.3). Code custody still owns the pause.
                let installed =
                    identity
                        .activate(&candidate)
                        .ok_or(SyncError::MemberKeysReplaced {
                            member: identity.member(),
                        })?;
                self.member.persist(&installed)?;
                self.database
                    .advance_operation(OperationUpdate {
                        id,
                        previous: record.last_step,
                        last_step: record.last_step + 1,
                        data: serde_json::to_vec(&MemberKeysWork::RefreshingCode)?,
                    })
                    .await?;
                return Ok(Some(installed));
            }
            let change = StoreChange::ReplaceMemberKeys {
                member: identity.member(),
                expected: identity.keys().public_keys(),
                keys: candidate,
            };
            match self
                .start_request(
                    session,
                    ring,
                    &identity,
                    Request::Change(change),
                    MEMBER_KEYS,
                )
                .await
            {
                Ok(_) => continue,
                Err(SyncError::Rejected(reason)) => {
                    // Contention may have installed an unheld pair or this
                    // candidate. Neither permits releasing the pause here (§12.3).
                    held_current(session, &identity)?;
                    if session.history.state.members[&identity.member()].keys == candidate {
                        continue;
                    }
                    // A replacement that never became current leaves its
                    // copies, so its pair stays held as retired (§12.3).
                    self.member.persist(&identity.retire_candidate())?;
                    self.database.finish_operation(id).await?;
                    return Err(SyncError::Rejected(reason));
                }
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
#[path = "sequence_member_keys_tests.rs"]
mod tests;
