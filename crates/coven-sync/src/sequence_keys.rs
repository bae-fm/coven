//! Fresh audience keys enter custody only after their operation records them.
use crate::{
    sequence_sync::sequence_operations::{Progress, Request, SequenceOperation, Step},
    sequence_sync::{SequenceSync, Session},
    SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::NewOperation;
use coven_format::admin::StoreChange;
use coven_foundation::id_source::{CircleId, KeyId};
use coven_merge::Audience;
use std::sync::Arc;

impl SequenceSync {
    /// Create a new circle and the copies that introduce its first key.
    pub async fn create_circle(self: &Arc<Self>, name: String) -> Result<CircleId, SyncError> {
        let circle = CircleId(self.ids.new_id());
        self.change_key(Audience::Circle(circle), Some(name))
            .await?;
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
            let member = owner
                .member
                .read()?
                .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
            let mut ring = owner.ring(&mut session, &member).await?;
            owner.read_forward(&mut session, &mut ring, &member).await?;
            owner.settle_open(&mut session, &mut ring, &member).await?;
            owner
                .resume_administration(&mut session, &mut ring, &member)
                .await?;
            owner
                .install_key(&mut session, &mut ring, &member, audience, name)
                .await
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
        let mut prepared = ring.clone();
        let hash = match audience {
            Audience::Store => {
                let key = coven_crypto::StoreKey::generate(id)?;
                let hash = key.hash();
                prepared.insert_store_key(key)?;
                hash
            }
            Audience::Circle(circle) => {
                let key = coven_crypto::CircleKey::generate(circle, id)?;
                let hash = key.hash();
                prepared.insert_circle_key(key)?;
                hash
            }
        };
        let change = match (audience, name) {
            (Audience::Circle(circle), Some(name)) => StoreChange::CreateCircle {
                circle,
                name,
                key: id,
                key_hash: hash,
            },
            (audience, None) => StoreChange::RotateKey {
                audience,
                key: id,
                key_hash: hash,
            },
            (Audience::Store, Some(_)) => unreachable!("only circle creation names its audience"),
        };
        session.history.validate(
            &coven_format::sequence::SlotHeader {
                device: self.device,
                claim: 1,
                author: member.member(),
                after: session.through,
            },
            coven_merge::Slot(session.through + 1),
            &change,
        )?;
        let work = SequenceOperation {
            request: Request::Change(change),
            progress: Progress::Running(Step::InstallingKey(
                prepared.to_secret_bytes().as_bytes().to_vec(),
            )),
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
        self.run_administration(session, ring, member, id)
            .await
            .map(drop)
    }
    pub(crate) async fn rotate_exposed(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let audiences: std::collections::BTreeSet<_> = session
            .history
            .introductions
            .iter()
            .map(|intro| intro.audience.clone())
            .collect();
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
                self.install_key(session, ring, member, audience, None)
                    .await?;
            }
        }
        Ok(())
    }
}
