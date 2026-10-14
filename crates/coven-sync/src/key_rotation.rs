//! Phase 2 of a pass: replace an exposed key before sending with it (§11).

use super::*;
use crate::key_introductions::{belongs, introductions, select, CopyObservation, Wait};

impl StoreLogSync {
    /// For each audience this member belongs to, use an existing usable key;
    /// if none is held, publish a rotation to a fresh key sealed only to the
    /// audience's current members (§11). The store goes first, since every
    /// other rotation entry is sealed with a store key. A queued entry is
    /// published first; its own rotation then becomes usable once applied.
    pub(crate) async fn rotate_exposed_keys(&mut self) -> Result<(), SyncError> {
        let local = self.database.local_store_log().await?;
        if local.upload.is_some() || local.log.replay.state.store.is_none() {
            return Ok(());
        }
        let member = self.operation_member()?;
        let me = member.member_id();
        let state = &local.log.replay.state;
        let mut audiences = vec![Audience::Store];
        audiences.extend(
            state
                .circles
                .keys()
                .map(|circle| Audience::Circle(*circle))
                .filter(|audience| belongs(state, audience, &me)),
        );
        for audience in audiences {
            self.rotate_if_exposed(audience, &me).await?;
        }
        Ok(())
    }

    /// Rotate one audience's key when no held key is usable for first attempts.
    pub(super) async fn rotate_if_exposed(
        &mut self,
        audience: Audience,
        me: &coven_crypto::MemberId,
    ) -> Result<(), SyncError> {
        let local = self.database.local_store_log().await?;
        let observation = self.copy_observation().await?;
        let CopyObservation::Complete(copies) = &observation else {
            // Without this pass's complete observation, sends wait (§11).
            return Ok(());
        };
        let copies: Vec<_> = copies.iter().map(|copy| copy.prefix.clone()).collect();
        let ring = self.store_keys.read()?;
        match select(
            &introductions(&local.log),
            &local.log.replay.state,
            me,
            Some(&copies),
            crate::key_introductions::held_hash(ring.as_ref()),
            &audience,
        ) {
            Err(Wait::Key) => self.rotate(audience).await,
            Ok(_) | Err(Wait::Membership | Wait::Listing) => Ok(()),
        }
    }

    /// Make a fresh key, keep it in custody, then publish its introduction
    /// with copies for the audience's current members.
    async fn rotate(&mut self, audience: Audience) -> Result<(), SyncError> {
        let id = KeyId(self.ids.new_id());
        let mut ring = self.store_keys.read()?;
        let key_hash = match &audience {
            Audience::Store => {
                let key = coven_crypto::StoreKey::generate(id)?;
                let hash = key.hash();
                match &mut ring {
                    Some(ring) => ring.insert_store_key(key)?,
                    None => ring = Some(StoreKeyring::new(key)),
                }
                hash
            }
            Audience::Circle(circle) => {
                let key = coven_crypto::CircleKey::generate(*circle, id)?;
                let hash = key.hash();
                ring.as_mut()
                    .ok_or(SyncError::KeyUnavailable(id))?
                    .insert_circle_key(key)?;
                hash
            }
        };
        // Custody holds the material before any entry names it.
        self.store_keys
            .persist(ring.as_ref().expect("fresh keyring"))?;
        self.make_and_upload_entry(StoreChange::RotateKey {
            audience,
            key: id,
            key_hash,
        })
        .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "key_rotation_tests.rs"]
mod tests;
