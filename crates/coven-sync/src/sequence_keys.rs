//! Fresh audience keys enter custody only after their operation records them.
use crate::sequence_sync::OperationState;
use crate::{
    sequence_sync::sequence_operations::{Progress, Request, SequenceOperation, Step},
    sequence_sync::{SequenceSync, Session},
    SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{NewOperation, OperationRecord};
use coven_format::admin::StoreChange;
use coven_foundation::id_source::{CircleId, KeyId};
use coven_merge::Audience;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The journal names material in custody; it never carries the material (§12.4).
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum KeyPreparation {
    Circle {
        circle: CircleId,
        name: String,
        key: KeyId,
    },
    Rotation {
        #[serde(with = "crate::pending_encoding::audience")]
        audience: Audience,
        key: KeyId,
    },
}

impl KeyPreparation {
    pub(crate) fn kind(&self) -> crate::OperationKind {
        match self {
            Self::Circle { .. } => crate::OperationKind::CreateCircle,
            Self::Rotation { .. } => crate::OperationKind::RotateKey,
        }
    }
}

impl SequenceSync {
    /// Create a new circle and the copies that introduce its first key.
    pub async fn create_circle(self: &Arc<Self>, name: String) -> Result<CircleId, SyncError> {
        let circle = CircleId(self.ids.new_id());
        self.change_key(Audience::Circle(circle), Some(name)).await?;
        Ok(circle)
    }
    /// Replace an audience key through the administrative journal.
    pub async fn rotate_key(self: &Arc<Self>, audience: Audience) -> Result<(), SyncError> {
        self.change_key(audience, None).await
    }
    async fn change_key(
        self: &Arc<Self>,
        audience: Audience,
        name: Option<String>,
    ) -> Result<(), SyncError> {
        let owner = self.clone();
        tokio::spawn(async move {
            let mut session = owner.session.lock().await;
            let member =
                owner.member.read()?.ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
            let mut ring = owner.ring(&mut session, &member).await?;
            owner.read_forward(&mut session, &mut ring, &member).await?;
            owner.settle_open(&mut session, &mut ring, &member).await?;
            owner.resume_administration(&mut session, &mut ring, &member).await?;
            owner.install_key(&mut session, &mut ring, &member, audience, name).await
        })
        .await
        .map_err(|error| SyncError::disk(std::io::Error::other(error)))?
    }
    async fn install_key(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        audience: Audience,
        name: Option<String>,
    ) -> Result<(), SyncError> {
        let id = KeyId(self.ids.new_id());
        let target = match (audience, name) {
            (Audience::Circle(circle), Some(name)) => {
                KeyPreparation::Circle { circle, name, key: id }
            }
            (audience, None) => KeyPreparation::Rotation { audience, key: id },
            (Audience::Store, Some(_)) => unreachable!("only circle creation names its audience"),
        };
        let work = SequenceOperation {
            request: Request::Key(target),
            progress: Progress::Running(Step::Preparing),
        };
        let id = self
            .database
            .start_operation(NewOperation {
                kind: "sequence".into(),
                data: serde_json::to_vec(&work)?,
                started_by: "key".into(),
                pauses_writes: false,
            })
            .await?;
        self.run_administration(session, ring, member, id).await.map(drop)
    }

    pub(crate) async fn prepare_audience_key(
        &self,
        session: &Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        record: &OperationRecord,
        target: KeyPreparation,
    ) -> Result<(), SyncError> {
        let (audience, key) = match &target {
            KeyPreparation::Circle { circle, key, .. } => (Audience::Circle(*circle), *key),
            KeyPreparation::Rotation { audience, key } => (audience.clone(), *key),
        };
        let mut prepared = ring.clone();
        // A crash after custody committed but before the journal advanced
        // reuses that exact key. Before custody committed, nothing used it (§12.4).
        let hash = match audience {
            Audience::Store => {
                if !prepared.store_key_ids().any(|id| id == key) {
                    prepared.insert_store_key(coven_crypto::StoreKey::generate(key)?)?;
                }
                prepared.store_key(key)?.hash()
            }
            Audience::Circle(circle) => {
                if !prepared.circle_key_ids(circle).any(|id| id == key) {
                    prepared.insert_circle_key(coven_crypto::CircleKey::generate(circle, key)?)?;
                }
                prepared.circle_key(circle, key)?.hash()
            }
        };
        let change = match target {
            KeyPreparation::Circle { circle, name, key } => {
                StoreChange::CreateCircle { circle, name, key, key_hash: hash }
            }
            KeyPreparation::Rotation { audience, key } => {
                StoreChange::RotateKey { audience, key, key_hash: hash }
            }
        };
        let progress = match session.history.validate(
            &coven_format::sequence::SlotHeader {
                device: self.device,
                claim: 1,
                author: member.member(),
                after: session.through,
            },
            coven_merge::Slot(session.through + 1),
            &change,
        ) {
            Ok(_) => {
                self.keys.persist(&prepared)?;
                *ring = prepared;
                Progress::Running(Step::Preparing)
            }
            Err(reason) => Progress::Undo { step: Step::Preparing, reason },
        };
        self.database
            .advance_operation(
                SequenceOperation { request: Request::Change(change), progress }.update(record)?,
            )
            .await?;
        Ok(())
    }
    pub(crate) async fn rotate_exposed(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let audiences: std::collections::BTreeSet<_> =
            session.history.introductions.iter().map(|intro| intro.audience.clone()).collect();
        for audience in audiences {
            if crate::effects::belongs(&session.history.state, &audience, &member.member())
                && session
                    .history
                    .introductions
                    .iter()
                    .rev()
                    .find(|intro| intro.audience == audience)
                    .is_some_and(|intro| session.history.exposed(&audience, intro.key))
            {
                let rotates = |request: &Request| {
                    matches!(request,
                    Request::Change(StoreChange::RotateKey { audience: rotated, .. })
                    | Request::Key(KeyPreparation::Rotation { audience: rotated, .. })
                    if *rotated == audience)
                };
                // A journaled rotation resumes under its own backoff.
                if self.journaled(rotates).await? {
                    continue;
                }
                let result = self.install_key(session, ring, member, audience.clone(), None).await;
                self.own_work(result, rotates).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_keys_tests.rs"]
mod tests;
