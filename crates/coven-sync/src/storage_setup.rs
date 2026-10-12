//! Verify a location before publishing local connection state.

use super::{keys, object, StoreLogSync};
use crate::{StorageCommit, SyncError};
use coven_crypto::{MemberKeys, StoreKey, StoreKeyring};
use coven_format::{
    sealed_single::SingleChunkObject,
    store_log::{MemberPublicKeys, StoreChange, StoreLogEntry},
};
use coven_storage::{ObjectPath, Storage, StorageSetupError};
use std::sync::Arc;

impl StoreLogSync {
    pub(crate) async fn unlock_storage(
        &mut self,
        storage: Arc<dyn Storage>,
    ) -> Result<(), SyncError> {
        let previous = self.storage.replace(storage);
        let result = async {
            let damages = self.sync_store_log().await?;
            if let Some(damage) = damages.into_iter().next() {
                return Err(SyncError::Damaged(damage));
            }
            let local = self.database.local_store_log().await?;
            let key = local
                .log
                .replay
                .state
                .store
                .as_ref()
                .ok_or(coven_storage::StorageFailure::NotFound)?
                .key;
            let keys = self
                .store_keys
                .read()?
                .ok_or(SyncError::KeyUnavailable(key))?;
            keys.store_key(key)?;
            Ok(())
        }
        .await;
        if result.is_err() {
            self.storage = previous;
        }
        result
    }

    pub(crate) async fn setup_storage(
        &mut self,
        storage: Arc<dyn Storage>,
        access: coven_format::MemberAccess,
        device_name: String,
        commit: StorageCommit,
    ) -> Result<(), SyncError> {
        let member = self
            .member_keys
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let previous = self.store_keys.read()?;
        let mut local = self.database.local_store_log().await?;
        let store_id = local.store;
        let (entry, key) = if local.log.replay.state.store.is_some() {
            let recorded = local
                .log
                .entries
                .iter()
                .find(|entry| matches!(entry.entry.change, StoreChange::CreateStore { .. }))
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            let path = object::path(store_id, recorded.entry.position);
            read_setup_entry(storage.as_ref(), &path, &member).await?
        } else {
            if local.upload.is_none() {
                let settings = self.directory.settings().map_err(|error| {
                    SyncError::Setup(Box::new(StorageSetupError::Internal(Box::new(error))))
                })?;
                let key = coven_crypto::StoreKey::generate(coven_foundation::id_source::KeyId(
                    self.ids.new_id(),
                ))?;
                let change = StoreChange::CreateStore {
                    store: local.store,
                    name: settings.name,
                    admin: MemberPublicKeys {
                        signing: member.member_id(),
                        sealing: member.sealing_public_key(),
                    },
                    key: key.id(),
                    key_hash: key.hash(),
                    access: access.clone(),
                    device_name: device_name.clone(),
                };
                let author = member.clone();
                let ring = StoreKeyring::new(key);
                self.database
                    .prepare_store_log(member.member_id(), change, move |log, entry, copies| {
                        keys::seal(copies, log, entry, Some(&ring), &author, &[])
                    })
                    .await?;
                local = self.database.local_store_log().await?;
            }
            let upload = local.upload.as_ref().expect("reserved setup entry");
            match &upload.entry.change {
                StoreChange::CreateStore {
                    store,
                    access: reserved_access,
                    device_name: reserved_name,
                    ..
                } if *store == local.store
                    && *reserved_access == access
                    && *reserved_name == device_name => {}
                _ => {
                    return Err(coven_storage::StorageFailure::InvalidConfiguration
                        .with_source("setup retry must retain its original device name and access")
                        .into())
                }
            }
            let recovered;
            let sealing_ring = if upload.sealing.keys.is_empty() && previous.is_none() {
                recovered = StoreKeyring::new(
                    keys::read_store_key(storage.as_ref(), store_id, upload.sealing.key, &member)
                        .await?,
                );
                Some(&recovered)
            } else {
                previous.as_ref()
            };
            let bytes = object::seal_upload(store_id, upload, sealing_ring, &member)?;
            object::upload(
                store_id,
                Some(storage.as_ref()),
                &self.database,
                upload,
                &bytes,
            )
            .await?;
            // Read what storage actually retained, including a lost-reply retry.
            let path = object::path(store_id, upload.entry.position);
            read_setup_entry(storage.as_ref(), &path, &member).await?
        };
        if !matches!(entry.change, StoreChange::CreateStore { store, .. } if store == local.store) {
            return Err(crate::write_object::damaged(
                &object::path(store_id, entry.position),
                crate::Refusal::WrongIdentity { cause: None },
            ));
        }
        if local.log.replay.state.store.is_none()
            && !matches!(&entry.change, StoreChange::CreateStore { access: recorded, device_name: name, .. } if recorded == &access && name == &device_name)
        {
            return Err(coven_storage::StorageFailure::InvalidConfiguration
                .with_source("setup retry must retain its original device name and access")
                .into());
        }
        let mut ring = match &previous {
            Some(ring) => {
                let mut ring = ring.clone();
                ring.insert_store_key(key)?;
                ring
            }
            None => StoreKeyring::new(key),
        };
        let mut creation = Some(entry);
        if let Some(store) = &local.log.replay.state.store {
            let opened =
                keys::read_store_key(storage.as_ref(), store_id, store.key, &member).await?;
            ring.insert_store_key(opened)?;
            let current = local
                .log
                .replay
                .state
                .members
                .get(&member.member_id())
                .ok_or(SyncError::PermissionDenied)?;
            if current.removed {
                return Err(crate::SyncFailure::Removed.into());
            }
            creation = None;
            if current.access != access {
                let change = StoreChange::SetAccess { access };
                match &local.upload {
                    Some(upload) if upload.entry.change != change => {
                        return Err(coven_database::DbError::StoreLogUploadPending(
                            upload.entry.position,
                        )
                        .into())
                    }
                    Some(_) => {}
                    None => {
                        let author = member.clone();
                        let keys = ring.clone();
                        self.database
                            .prepare_store_log(
                                member.member_id(),
                                change,
                                move |log, entry, copies| {
                                    keys::seal(copies, log, entry, Some(&keys), &author, &[])
                                },
                            )
                            .await?;
                        local = self.database.local_store_log().await?;
                    }
                }
                let upload = local.upload.as_ref().expect("reserved access entry");
                let path = object::path(store_id, upload.entry.position);
                let bytes = object::seal_upload(store_id, upload, Some(&ring), &member)?;
                storage.create_once(&path, &bytes).await?;
                if storage.read(&path).await? != bytes {
                    return Err(crate::write_object::damaged(
                        &path,
                        crate::Refusal::WrongIdentity { cause: None },
                    ));
                }
                creation = Some(upload.entry.clone());
            }
        }
        let rollback = commit()?;
        let result = async {
            self.store_keys.persist(&ring)?;
            if let Some(creation) = creation {
                let (entry, replay) = crate::replay_entry(&local.log, creation);
                self.database.apply_store_log(entry, replay).await?;
            }
            Ok::<_, SyncError>(())
        }
        .await;
        if let Err(error) = result {
            let restored_keys = match &previous {
                Some(keys) => self.store_keys.persist(keys),
                None => self.store_keys.forget(),
            };
            let error = compensate(error, restored_keys.map_err(SyncError::from));
            return Err(compensate(error, rollback()));
        }
        self.storage = Some(storage);
        Ok(())
    }
}

async fn read_setup_entry(
    storage: &dyn Storage,
    path: &ObjectPath,
    member: &MemberKeys,
) -> Result<(StoreLogEntry, StoreKey), SyncError> {
    let bytes = storage.read(path).await?;
    let envelope = SingleChunkObject::decode(&bytes)?;
    let SingleChunkObject::StoreLog { key, .. } = &envelope else {
        return Err(crate::write_object::damaged(
            path,
            crate::write_object::invalid("expected a store-log envelope"),
        ));
    };
    let key = keys::read_store_key(storage, path.store(), *key, member).await?;
    let entry = object::open(&envelope, path, &key).map_err(|failure| crate::DamagedObject {
        path: path.as_str().into(),
        failure,
    })?;
    Ok((entry, key))
}

fn compensate(operation: SyncError, rollback: Result<(), SyncError>) -> SyncError {
    match rollback {
        Ok(()) => operation,
        Err(rollback) => SyncError::Cleanup {
            operation: Box::new(operation),
            cleanup: Box::new(rollback),
        },
    }
}
