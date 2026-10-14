//! Phase 2 of a pass: replace an exposed key before sending with it (§11).

use super::*;
use crate::key_introductions::{belongs, introductions, select, Wait};
use crate::operation_data::{Data, EntryWork, Intent};
use crate::operations::Progress;

impl StoreLogSync {
    /// Queue durable rotations for audiences with no usable held key. Store
    /// rotation comes first because every rotation entry needs a store key.
    pub(crate) async fn rotate_exposed_keys(&mut self) -> Result<bool, SyncError> {
        let local = self.database.local_store_log().await?;
        if local.upload.is_some() || local.log.replay.state.store.is_none() {
            return Ok(false);
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
        let mut scheduled = false;
        for audience in audiences {
            scheduled |= self.schedule_rotation(audience, &me).await?;
        }
        Ok(scheduled)
    }

    /// Requested snapshots also use the durable rotation steps before sealing.
    pub(crate) async fn rotate_if_exposed(
        &mut self,
        audience: Audience,
        me: &coven_crypto::MemberId,
    ) -> Result<(), SyncError> {
        self.schedule_rotation(audience.clone(), me).await?;
        for record in self.database.operations().await? {
            if matches!(Data::read(&record)?, Data::Entry(EntryWork {
                intent: Intent::RotateKey { audience: ref target }, ..
            }) if *target == audience)
            {
                loop {
                    let record = self
                        .database
                        .operation(record.id)
                        .await?
                        .ok_or(coven_database::DbError::OperationChanged(record.id))?;
                    if !self.operation_can_retry(&record).await? {
                        let pending = self
                            .database
                            .operation_pending::<crate::PendingRecord>(record.id)
                            .await?
                            .ok_or(coven_database::DbError::DamagedDatabase)?;
                        return Err(SyncError::Pending(Box::new(pending)));
                    }
                    match self.operation_step(&record, Data::read(&record)?).await {
                        Ok(Progress::Advanced) => (),
                        Ok(Progress::Finished(_)) => break,
                        Ok(Progress::Waiting(reason)) => {
                            let pending = self.record_operation_condition(&record, reason).await?;
                            return Err(SyncError::Pending(Box::new(pending)));
                        }
                        Ok(Progress::Reply(result)) => {
                            result?;
                            break;
                        }
                        Err(error) => {
                            self.record_operation_error(&record, &error).await?;
                            return Err(error);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn schedule_rotation(
        &self,
        audience: Audience,
        me: &coven_crypto::MemberId,
    ) -> Result<bool, SyncError> {
        let local = self.database.local_store_log().await?;
        let observed = match self.copy_observation() {
            Ok(observed) => observed,
            // Without a fresh completed catch-up, rotation waits for a pass (§11).
            Err(SyncError::Storage(error))
                if error.failure() == coven_storage::StorageFailure::CatchUpRequired =>
            {
                return Ok(false)
            }
            Err(error) => return Err(error),
        };
        let copies: Vec<_> = observed.into_iter().map(|copy| copy.prefix).collect();
        let ring = self.store_keys.read()?;
        match select(
            &introductions(&local.log),
            &local.log.replay.state,
            me,
            Some(&copies),
            crate::key_introductions::held_hash(ring.as_ref()),
            &audience,
        ) {
            Err(Wait::Key) => {
                for record in self.database.operations().await? {
                    if matches!(Data::read(&record)?, Data::Entry(EntryWork {
                        intent: Intent::RotateKey { audience: ref target }, ..
                    }) if *target == audience)
                    {
                        return Ok(false);
                    }
                }
                self.database
                    .start_operation(
                        Data::Entry(EntryWork {
                            intent: Intent::RotateKey { audience },
                            entry: None,
                        })
                        .new_operation("coven")?,
                    )
                    .await?;
                Ok(true)
            }
            Ok(_) | Err(Wait::Membership | Wait::Listing) => Ok(false),
        }
    }

    /// Custody precedes the reservation naming this material. A dropped entry
    /// restarts preparation and therefore gets a fresh id and material.
    pub(super) fn rotation_change(
        &self,
        audience: Audience,
        ring: &mut Option<StoreKeyring>,
    ) -> Result<StoreChange, SyncError> {
        let id = KeyId(self.ids.new_id());
        let key_hash = match &audience {
            Audience::Store => {
                let key = coven_crypto::StoreKey::generate(id)?;
                let hash = key.hash();
                match ring {
                    Some(ring) => ring.insert_store_key(key)?,
                    None => *ring = Some(StoreKeyring::new(key)),
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
        Ok(StoreChange::RotateKey {
            audience,
            key: id,
            key_hash,
        })
    }
}

#[cfg(test)]
#[path = "key_rotation_tests.rs"]
mod tests;
