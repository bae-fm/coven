//! Moving a store preserves immutable ciphertext and every author's signatures.

use super::StoreLogSync;
use crate::{operation_data::Data, SyncError};
use coven_database::StoreLog;
use coven_storage::{
    ByteRange, ObjectPath, ObjectPrefix, Storage, StorageError, StorageFailure, StorageSetupError,
    StoredObject,
};
use std::{collections::BTreeSet, sync::Arc};

impl StoreLogSync {
    pub(super) async fn copy_storage(
        &self,
        destination: &Arc<dyn Storage>,
        log: &StoreLog,
        existing: &[StoredObject],
    ) -> Result<(), SyncError> {
        let source = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let mut shared = BTreeSet::new();
        for entry in &log.entries {
            for (audience, key) in crate::store_log_keys::introduced(&entry.entry.change) {
                for member in log.replay.state.members.keys() {
                    shared.insert(crate::store_log_keys::path(&audience, key, member));
                }
            }
        }
        for record in self.database.operations().await? {
            if let Data::Invite(work) = Data::read(&record)? {
                shared.insert(ObjectPath::join_request(work.id));
            }
        }
        let mut objects: Vec<_> = source
            .list(&ObjectPrefix::all())
            .await?
            .into_iter()
            .filter(|object| !object.path.is_replaceable())
            .filter(|object| {
                object
                    .path
                    .device()
                    .is_some_and(|device| log.replay.state.devices.contains_key(&device))
                    || shared.contains(&object.path)
            })
            .collect();
        // Before the origin is published, an interrupted destination may contain
        // only a subset of this copy. Anything else is an occupied location.
        if !existing
            .iter()
            .any(|object| object.path.is_first_store_entry())
            && existing
                .iter()
                .any(|object| !objects.iter().any(|source| source.path == object.path))
        {
            return Err(SyncError::Setup(Box::new(
                StorageSetupError::LocationOccupied,
            )));
        }
        objects.sort_by_key(|object| object.path.is_first_store_entry());
        for object in objects {
            copy_object(source, destination.as_ref(), &object).await?;
        }
        Ok(())
    }
}

async fn copy_object(
    source: &dyn Storage,
    destination: &dyn Storage,
    object: &StoredObject,
) -> Result<(), StorageError> {
    let mut session = match destination.begin_upload(&object.path, object.size).await {
        Ok(session) => session,
        Err(error) if error.failure() == StorageFailure::AlreadyExists => return Ok(()),
        Err(error) => return Err(error),
    };
    let copied = async {
        while session.confirmed_bytes() < object.size {
            let start = session.confirmed_bytes();
            let end = object.size.min(start + session.part_size() as u64);
            let bytes = source
                .read_range(&object.path, ByteRange::new(start, end)?)
                .await?;
            destination.upload_part(&mut session, &bytes).await?;
            if session.confirmed_bytes() <= start {
                return Err(StorageFailure::Protocol.with_source("copy did not advance"));
            }
        }
        destination.finish_upload(&mut session).await
    }
    .await;
    match copied {
        Ok(()) => Ok(()),
        Err(operation) => match destination.abort_upload(&session).await {
            Ok(()) if operation.failure() == StorageFailure::AlreadyExists => Ok(()),
            Ok(()) => Err(operation),
            Err(cleanup) => Err(StorageError::Cleanup {
                operation: Box::new(operation),
                cleanup: Box::new(cleanup),
            }),
        },
    }
}
