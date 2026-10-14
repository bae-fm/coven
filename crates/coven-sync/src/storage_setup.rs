//! Verify a location before publishing local connection state.

use super::{keys, object, StoreLogSync};
use crate::{StorageCommit, SyncError};
use coven_crypto::{
    custody::{InMemoryCustody, KeySession},
    MemberKeys, StoreKey, StoreKeyring,
};
use coven_format::{
    sealed_single::SingleChunkObject,
    store_log::{MemberPublicKeys, StoreChange, StoreLogEntry},
};
use coven_merge::Audience;
use coven_storage::{ObjectPath, Storage};
use std::sync::Arc;

impl StoreLogSync {
    pub(crate) async fn unlock_storage(
        &mut self,
        storage: Arc<dyn Storage>,
    ) -> Result<(), SyncError> {
        let previous = self.storage.replace(storage);
        let previous_finality =
            std::mem::take(&mut *self.finality.lock().expect("finality lock poisoned"));
        let result = async {
            // Unlock needs only a usable store key (§11). Other damaged
            // objects and refused key copies stay in their pending records.
            self.sync_store_log().await?;
            let local = self.database.local_store_log().await?;
            let member = self.operation_member()?.member_id();
            let store_copy = |record: &crate::PendingRecord| {
                record.reported_by == local.device
                    && matches!(&record.subject, crate::PendingSubject::KeyCopy {
                        audience: Audience::Store,
                        member: recipient,
                        ..
                    } if *recipient == member)
            };
            if local.log.replay.state.store.is_none() {
                // Without its store key this device cannot read the creation.
                return Err(self
                    .database
                    .pending::<crate::PendingRecord>()
                    .await?
                    .into_iter()
                    .find(store_copy)
                    .map_or(coven_storage::StorageFailure::NotFound.into(), |record| {
                        SyncError::Pending(Box::new(record))
                    }));
            }
            let key = crate::key_introductions::newest(&local.log, &Audience::Store)
                .ok_or(coven_storage::StorageFailure::NotFound)?;
            let selected = match self.store_keys.read()? {
                Some(ring) => crate::write_seal::readable_key(&local.log, &ring, &Audience::Store),
                None => Err(SyncError::KeyUnavailable(key)),
            };
            match selected {
                Ok(_) => Ok(()),
                Err(SyncError::KeyUnavailable(key)) => {
                    // The store key's copy subject names why it cannot open yet.
                    Err(self
                        .database
                        .pending::<crate::PendingRecord>()
                        .await?
                        .into_iter()
                        .find(|record| {
                            store_copy(record)
                                && matches!(&record.subject, crate::PendingSubject::KeyCopy { key: k, .. } if *k == key)
                        })
                        .map_or(SyncError::KeyUnavailable(key), |record| {
                            SyncError::Pending(Box::new(record))
                        }))
                }
                Err(error) => Err(error),
            }
        }
        .await;
        if result.is_err() {
            self.storage = previous;
            *self.finality.lock().expect("finality lock poisoned") = previous_finality;
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
            read_setup_entry(
                &self.database,
                &self.reads,
                self.download_limit,
                storage.as_ref(),
                &path,
                &member,
            )
            .await?
        } else {
            if local.upload.is_none() {
                let settings = self.directory.settings().map_err(SyncError::disk)?;
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
                // Creation seals with its own fresh key; no copy can expose it yet.
                let observed = crate::key_introductions::CopyObservation::Complete(Vec::new());
                self.database
                    .prepare_store_log(member.member_id(), change, move |log, entry, copies| {
                        keys::seal(copies, log, entry, Some(&ring), &author, &observed)
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
                let StoreChange::CreateStore { key_hash, .. } = &upload.entry.change else {
                    unreachable!("checked reserved creation")
                };
                recovered = StoreKeyring::new(
                    keys::read_store_key(
                        &self.database,
                        &self.reads,
                        self.download_limit,
                        storage.as_ref(),
                        store_id,
                        upload.sealing.key,
                        Some(key_hash),
                        &member,
                    )
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
            read_setup_entry(
                &self.database,
                &self.reads,
                self.download_limit,
                storage.as_ref(),
                &path,
                &member,
            )
            .await?
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
        if local.log.replay.state.store.is_some() && local.upload.is_none() {
            // A new access attempt reads every stored witness through the
            // candidate connection. Reserved attempts retain their recorded past.
            ring = self.catch_up_setup(storage.clone(), ring).await?;
            local = self.database.local_store_log().await?;
        }
        if local.log.replay.state.store.is_some() {
            let needed = crate::key_introductions::needed(&local.log);
            let key = crate::key_introductions::newest(&local.log, &Audience::Store)
                .ok_or(coven_database::DbError::DamagedDatabase)?;
            let opened = keys::read_store_key(
                &self.database,
                &self.reads,
                self.download_limit,
                storage.as_ref(),
                store_id,
                key,
                Some(&needed[&(Audience::Store, key)]),
                &member,
            )
            .await?;
            ring.insert_store_key(opened)?;
            let current = local
                .log
                .replay
                .state
                .members
                .get(&member.member_id())
                .ok_or(SyncError::Rejected(coven_database::DropReason::NotAllowed))?;
            if current.removed {
                return Err(SyncError::Removed);
            }
            creation = None;
            if !current.access.same_credential(&access) {
                let change = StoreChange::SetAccess {
                    member: member.member_id(),
                    access,
                };
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
                        let held = ring.clone();
                        let observed = keys::observe(
                            &self.database,
                            &self.reads,
                            self.download_limit,
                            storage.as_ref(),
                            store_id,
                        )
                        .await?;
                        self.database
                            .prepare_store_log(
                                member.member_id(),
                                change,
                                move |log, entry, copies| {
                                    keys::seal(copies, log, entry, Some(&held), &author, &observed)
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
        let applied = if let Some(entry) = creation {
            let publications = crate::store_log_landing::observe_publication(
                &self.database,
                &self.reads,
                self.download_limit,
                store_id,
                Some(storage.as_ref()),
                &local.log,
                entry.position,
            )
            .await?;
            let (entry, replay) = crate::replay_entry(&local.log, entry, &publications);
            let deletions = crate::access_keys::deletions(
                &local.log.entries,
                std::slice::from_ref(&entry),
                &replay,
            )?;
            if let coven_database::EntryOutcome::Dropped(reason) =
                &replay.entries[&entry.entry.position]
            {
                let reason = reason.clone();
                self.database
                    .apply_store_log::<crate::PendingRecord>(entry, replay, deletions)
                    .await?;
                return Err(SyncError::Rejected(reason));
            }
            Some((entry, replay, deletions))
        } else {
            None
        };
        let rollback = commit()?;
        let result = async {
            self.store_keys.persist(&ring)?;
            if let Some((entry, replay, deletions)) = applied {
                self.database
                    .apply_store_log::<crate::PendingRecord>(entry, replay, deletions)
                    .await?;
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
        *self.finality.lock().expect("finality lock poisoned") = Default::default();
        Ok(())
    }

    // Candidate-provider reads and acquired keys belong to this setup attempt.
    // Final custody remains unchanged until setup commits.
    async fn catch_up_setup(
        &self,
        storage: Arc<dyn Storage>,
        ring: StoreKeyring,
    ) -> Result<StoreKeyring, SyncError> {
        let staged_keys = Arc::new(KeySession::store(Arc::new(InMemoryCustody::new(ring)))?);
        let mut candidate = Self::new(
            storage,
            self.database.clone(),
            staged_keys.clone(),
            self.member_keys.clone(),
            self.clock.clone(),
            self.ids.clone(),
            self.directory.clone(),
        );
        candidate.set_discovery_download_limit(self.download_limit);
        candidate.catch_up().await?;
        Ok(staged_keys
            .read()?
            .expect("setup retains the opened keyring"))
    }
}

async fn read_setup_entry(
    database: &coven_database::Database,
    reads: &crate::pass_reads::PassReads,
    limit: std::num::NonZeroUsize,
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
    // Tentative until the creation it opens commits to its hash (§11).
    let key = keys::read_store_key(
        database,
        reads,
        limit,
        storage,
        path.store(),
        *key,
        None,
        member,
    )
    .await?;
    let entry = object::open(&envelope, path, &key).map_err(|failure| crate::DamagedObject {
        path: path.as_str().into(),
        failure,
    })?;
    if let StoreChange::CreateStore {
        key: introduced,
        key_hash,
        ..
    } = &entry.change
    {
        if *introduced == key.id() {
            key.check_hash(key_hash)?;
        }
    }
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
