//! Replacing a member's signing and sealing pair (§9, §10, §18.1): this
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

/// The candidate's public pair, fixed once kept in identity custody.
#[derive(Serialize, Deserialize)]
struct MemberKeysWork {
    /// Signing and sealing keys.
    candidate: Option<([u8; 32], [u8; 32])>,
}

impl MemberKeysWork {
    fn candidate(&self) -> Result<Option<MemberPublicKeys>, SyncError> {
        self.candidate
            .map(|(signing, sealing)| {
                Ok(MemberPublicKeys {
                    signing: coven_crypto::SigningKey::from_bytes(signing)?,
                    sealing: coven_crypto::SealingKey::from_bytes(sealing),
                })
            })
            .transpose()
    }
}

/// This member's held active pair, or its kept candidate, must be its
/// current binding: only those sign or install the replacement (§10).
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
    /// and custody installs it, or the replacement fails (§10, E9).
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
                        data: serde_json::to_vec(&MemberKeysWork { candidate: None })?,
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

    /// Publish a prepared public pair for another active member, with copies
    /// of every store key and held keys of that member's circles first (§9, E9).
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
            let Some(candidate) = work.candidate()? else {
                // Writes fixed under the pair about to be retired must reach
                // storage first: after the binding they count for nothing (§10).
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
                        return Err(SyncError::Pending(Box::new(PendingRecord {
                            subject: PendingSubject::Operation {
                                id,
                                kind: OperationKind::ReplaceMemberKeys,
                            },
                            reason: PendingReason::Waits(Prerequisite::SequenceCatchUp),
                            reported_by: self.device,
                        })));
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
                work.candidate =
                    Some((*candidate.signing.as_bytes(), *candidate.sealing.as_bytes()));
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
                // one for copies that name it (§11), then release the pause.
                let installed =
                    identity
                        .activate(&candidate)
                        .ok_or(SyncError::MemberKeysReplaced {
                            member: identity.member(),
                        })?;
                self.member.persist(&installed)?;
                self.database.finish_operation(id).await?;
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
                    // A replacement that never became current leaves its
                    // copies, so its pair stays held as retired (§11).
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
