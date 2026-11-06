//! Create identity custody while the store remains unpublished.

use super::*;
use coven_foundation::files::{BootstrapDirectoryError, BootstrapStore};
use coven_foundation::task::caught_sync;

pub(super) async fn create(
    builder: CovenBuilder,
    name: String,
    publish: impl FnOnce(&BootstrapStore) -> Result<(), BootstrapDirectoryError> + Send + 'static,
) -> Result<CovenHandle, StoreCreationError> {
    let id = StoreId(builder.ids.new_id());
    let pending = Arc::new(builder.directories.begin_bootstrap(id, builder.ids.as_ref()).await?);
    let bugs = builder.directories.bugs().clone();
    match prepare(builder, pending.clone(), name, publish).await {
        Ok(handle) => Ok(handle),
        Err(error @ StoreCreationError::PublishedButNotSynced { .. }) => Err(error),
        Err(error) => {
            Err(
                match crate::task::blocking_completing(
                    "remove the unpublished store",
                    bugs,
                    move || pending.cancel(),
                )
                .await
                {
                    Ok(Ok(())) => error,
                    Ok(Err(cleanup)) => error.with_cleanup(cleanup.into()),
                    // The removal panicked or never ran, so the unpublished
                    // store may stay on disk.
                    Err(failed) => error.with_cleanup(failed.into()),
                },
            )
        }
    }
}

async fn prepare(
    builder: CovenBuilder,
    pending: Arc<BootstrapStore>,
    name: String,
    publish: impl FnOnce(&BootstrapStore) -> Result<(), BootstrapDirectoryError> + Send + 'static,
) -> Result<CovenHandle, StoreCreationError> {
    let directory = pending.directory();
    let database = builder.database(directory.clone())?.open().await?;
    if let Err(error) = database.initialize_store_name(name).await {
        return Err(close_failed(&database, CovenError::from(error).into()).await);
    }
    let waits = match coven_sync::RetryWaits::open(database.clone(), builder.clock.clone()).await {
        Ok(waits) => Arc::new(waits),
        Err(error) => return Err(close_failed(&database, CovenError::from(error).into()).await),
    };
    let bugs = builder.directories.bugs().clone();
    let prepared =
        crate::task::blocking_completing("create the store's custody", bugs, move || {
            let keychain = Arc::new(StoreKeychain::new(
                builder.keychain().map_err(CovenError::from)?,
                directory.id(),
            ));
            let owners = builder.owners(directory, keychain, crate::builder::Continuing::Fresh)?;
            // Each step catches its panic, so a panic undoes custody as any
            // failure before publication does (E15). Making the identity writes
            // nothing, and refuses one custody already holds, so a failure there
            // has nothing to undo; the undo takes back only what this creation
            // wrote.
            let member = MemberId(owners.ids.new_id());
            let identity =
                caught_sync("make the member identity", || owners.custody.new_identity(member))
                    .unwrap_or_else(|bug| Err(IdentityError::Bug(bug)))?;
            let result =
                caught_sync("save the member identity", || owners.identity.persist(&identity))
                    .unwrap_or_else(|bug| Err(KeyError::Bug(bug)))
                    .map_err(|error| StoreCreationError::from(IdentityError::from(error)))
                    .and_then(|()| {
                        caught_sync("record the device id", || {
                            owners.keychain.set_device_id(owners.device)
                        })
                        .unwrap_or_else(|bug| Err(KeyError::Bug(bug)))
                        .map_err(CovenError::from)?;
                        Ok(published(&pending, publish))
                    });
            let mut error = match result {
                Ok(Publication::Done) => return Ok((owners, None)),
                Ok(Publication::Failed(error)) => return Ok((owners, Some(error))),
                Ok(Publication::Unknown(error)) => return Err(error),
                Ok(Publication::Refused(error)) | Err(error) => error,
            };
            // Try every compensation, retaining every failure. The marker stays hidden.
            for cleanup in [owners.identity.forget(), owners.keychain.delete_device_id()] {
                if let Err(cleanup) = cleanup {
                    error = error.with_cleanup(CovenError::from(cleanup).into());
                }
            }
            Err(error)
        })
        .await
        .unwrap_or_else(|shutdown| Err(shutdown.into()));
    let (owners, publication) = match prepared {
        Ok(owners) => owners,
        Err(error) => return Err(close_failed(&database, error).await),
    };
    if let Err(error) = crate::storage::record_opened(
        &database,
        &owners.clock,
        coven_database::RecordedLocation::None,
        None,
    )
    .await
    {
        return Err(close_failed(&database, error.into()).await);
    }
    let handle = owners.into_handle(database, waits, false, false);
    match publication {
        None => Ok(handle),
        Some(source) => {
            let store = handle.store_id();
            let source = match handle.close().await {
                Ok(()) => source,
                Err(cleanup) => source.with_cleanup(cleanup.into()),
            };
            Err(StoreCreationError::PublishedButNotSynced { store, source: Box::new(source) })
        }
    }
}

/// How far publishing the new store got.
enum Publication {
    /// The store is published.
    Done,
    /// The store is published, but the step failed after its marker went,
    /// as syncing the directory does: its custody stays.
    Failed(StoreCreationError),
    /// The store isn't published: its custody is undone.
    Refused(StoreCreationError),
    /// Publishing panicked, and the marker that says how far it got couldn't
    /// be read: custody stays, and listing the stores shows which (E15).
    Unknown(StoreCreationError),
}

/// Publish the store with `publish`. Its panic leaves it unknown whether
/// the marker went, so the marker says: creation keeps custody for a
/// published store and undoes it for one that isn't (E15).
fn published(
    pending: &BootstrapStore,
    publish: impl FnOnce(&BootstrapStore) -> Result<(), BootstrapDirectoryError>,
) -> Publication {
    let bug = match caught_sync("publish the store", || publish(pending)) {
        Ok(Ok(())) => return Publication::Done,
        Ok(Err(error)) if error.published_unsynced() => {
            return Publication::Failed(StoreCreationError::Directory(error))
        }
        Ok(Err(error)) => return Publication::Refused(error.into()),
        Err(bug) => StoreCreationError::Bug(bug),
    };
    match pending.is_published() {
        Ok(true) => Publication::Failed(bug),
        Ok(false) => Publication::Refused(bug),
        Err(read) => {
            Publication::Unknown(bug.with_cleanup(BootstrapDirectoryError::from(read).into()))
        }
    }
}

async fn close_failed(
    database: &coven_database::Database,
    error: StoreCreationError,
) -> StoreCreationError {
    match database.close().await {
        Ok(()) => error,
        Err(cleanup) => error.with_cleanup(SyncError::from(cleanup).into()),
    }
}

#[cfg(test)]
#[path = "builder_creation_tests.rs"]
mod tests;
