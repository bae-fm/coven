//! Verify a location before publishing local connection state.

use super::{keys, object, StoreLogSync};
use crate::{StorageCommit, SyncError};
use coven_crypto::StoreKeyring;
use coven_format::{
    sealed_single::SingleChunkObject,
    store_log::{MemberPublicKeys, StoreChange},
};
use coven_storage::{ObjectPath, ObjectPrefix, Storage, StorageSetupError};
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
                .ok_or(coven_storage::StorageError::NotFound)?
                .key;
            let keys = self
                .store_keys
                .unlock()?
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
            .unlock()?
            .ok_or(SyncError::MissingMemberKeys)?;
        let previous = self.store_keys.unlock()?;
        let mut local = self.database.local_store_log().await?;
        let objects = match storage.list(&ObjectPrefix::all()).await {
            Ok(objects) => objects,
            Err(coven_storage::StorageError::InvalidPath) => return Err(occupied()),
            Err(error) => return Err(error.into()),
        };
        let mut origins = Vec::new();
        for stored in &objects {
            if stored
                .path
                .store_log_position()
                .is_some_and(|(_, n)| n.get() == 1)
            {
                let bytes = storage.read(&stored.path).await?;
                let envelope = SingleChunkObject::decode(&bytes).map_err(|_| occupied())?;
                object::check_origin(&envelope, &stored.path).map_err(|_| occupied())?;
                if let SingleChunkObject::StoreLog {
                    origin: Some(origin),
                    key,
                    ..
                } = envelope
                {
                    if origin.store != local.store {
                        return Err(occupied());
                    }
                    origins.push((stored.path.clone(), bytes, key));
                }
            }
        }
        let relocating = local.log.replay.state.store.is_some()
            && self
                .storage
                .as_ref()
                .is_some_and(|source| source.config() != storage.config());
        if relocating {
            self.copy_storage(&storage, &local.log, &objects).await?;
            if origins.is_empty() {
                let origin = local
                    .log
                    .entries
                    .iter()
                    .find(|entry| matches!(entry.entry.change, StoreChange::CreateStore { .. }))
                    .ok_or(coven_database::DbError::DamagedDatabase)?;
                let path = object::path(origin.entry.position);
                let bytes = storage.read(&path).await?;
                let SingleChunkObject::StoreLog { key, .. } = SingleChunkObject::decode(&bytes)?
                else {
                    return Err(occupied());
                };
                origins.push((path, bytes, key));
            }
        }
        if !objects.is_empty() && origins.is_empty() {
            // A retry may find only this attempt's prerequisite sealed key.
            let own_attempt = local.upload.as_ref().is_some_and(|upload| {
                matches!(upload.entry.change, StoreChange::CreateStore { .. })
                    && objects.iter().all(|object| {
                        upload
                            .sealing
                            .keys
                            .iter()
                            .any(|key| key.path == object.path.as_str())
                    })
            });
            if !own_attempt {
                return Err(occupied());
            }
        }
        if origins.is_empty() {
            if local.log.replay.state.store.is_some() {
                return Err(SyncError::NoStorage);
            }
            if local.upload.is_none() {
                let settings = self.directory.settings().map_err(|error| {
                    SyncError::Setup(Box::new(StorageSetupError::Internal(Box::new(error))))
                })?;
                let change = StoreChange::CreateStore {
                    store: local.store,
                    name: settings.name,
                    admin: MemberPublicKeys {
                        signing: member.member_id(),
                        sealing: member.sealing_public_key(),
                    },
                    key: coven_foundation::id_source::KeyId(self.ids.new_id()),
                    access: access.clone(),
                    device_name: device_name.clone(),
                };
                let author = member.clone();
                let ring = previous.clone();
                self.database
                    .prepare_store_log(member.member_id(), change, move |log, entry| {
                        keys::seal(log, entry, ring.as_ref(), &author)
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
                    return Err(SyncError::Setup(Box::new(StorageSetupError::Storage(
                        coven_storage::StorageError::InvalidConfiguration(
                            "setup retry must retain its original device name and access",
                        ),
                    ))))
                }
            }
            let bytes = object::seal_upload(upload, previous.as_ref(), &member)?;
            for key in &upload.sealing.keys {
                storage
                    .create_once(
                        &ObjectPath::parse(&key.path).map_err(coven_storage::StorageError::from)?,
                        &key.bytes,
                    )
                    .await?;
            }
            storage
                .create_once(&object::path(upload.entry.position), &bytes)
                .await?;
            // Read what storage actually retained, including a lost-reply retry.
            let path = object::path(upload.entry.position);
            let bytes = storage.read(&path).await?;
            let SingleChunkObject::StoreLog { key, .. } = SingleChunkObject::decode(&bytes)? else {
                return Err(occupied());
            };
            origins.push((path, bytes, key));
        }
        let mut ring = previous.clone();
        let mut creation = None;
        for (path, bytes, key) in origins {
            let key_path = ObjectPath::store_key(key, &member.member_id());
            let opened =
                member.open_store_key(key_path.as_str(), &storage.read(&key_path).await?)?;
            if opened.id() != key {
                return Err(occupied());
            }
            match &mut ring {
                Some(ring) => ring.insert_store_key(opened)?,
                None => ring = Some(StoreKeyring::new(opened)),
            }
            let envelope = SingleChunkObject::decode(&bytes)?;
            let entry = object::open(
                &envelope,
                &path,
                ring.as_ref().expect("opened store key").store_key(key)?,
            )
            .map_err(|failure| crate::DamagedObject {
                path: path.into(),
                failure,
            })?;
            if !matches!(entry.change, StoreChange::CreateStore { store, .. } if store == local.store)
            {
                return Err(occupied());
            }
            if local.log.replay.state.store.is_none()
                && !matches!(&entry.change, StoreChange::CreateStore { access: recorded, device_name: name, .. } if recorded == &access && name == &device_name)
            {
                return Err(SyncError::Setup(Box::new(StorageSetupError::Storage(
                    coven_storage::StorageError::InvalidConfiguration(
                        "setup retry must retain its original device name and access",
                    ),
                ))));
            }
            creation = Some(entry);
        }
        if let Some(store) = &local.log.replay.state.store {
            let key_path = ObjectPath::store_key(store.key, &member.member_id());
            let opened =
                member.open_store_key(key_path.as_str(), &storage.read(&key_path).await?)?;
            if opened.id() != store.key {
                return Err(occupied());
            }
            ring.as_mut()
                .expect("checked origin")
                .insert_store_key(opened)?;
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
                            .prepare_store_log(member.member_id(), change, move |log, entry| {
                                keys::seal(log, entry, keys.as_ref(), &author)
                            })
                            .await?;
                        local = self.database.local_store_log().await?;
                    }
                }
                let upload = local.upload.as_ref().expect("reserved access entry");
                let path = object::path(upload.entry.position);
                let bytes = object::seal_upload(upload, ring.as_ref(), &member)?;
                storage.create_once(&path, &bytes).await?;
                if storage.read(&path).await? != bytes {
                    return Err(occupied());
                }
                creation = Some(upload.entry.clone());
            }
        }
        let rollback = commit()?;
        let result = async {
            self.store_keys
                .persist(ring.as_ref().expect("checked origin and key"))?;
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

fn occupied() -> SyncError {
    SyncError::Setup(Box::new(StorageSetupError::LocationOccupied))
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
