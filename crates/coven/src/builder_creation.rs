//! Create identity custody while the store remains unpublished.

use super::*;
use coven_foundation::files::{BootstrapDirectoryError, BootstrapStore};

pub(super) async fn create(
    builder: CovenBuilder,
    name: String,
    publish: impl FnOnce(&BootstrapStore) -> Result<(), BootstrapDirectoryError> + Send + 'static,
) -> Result<CovenHandle, StoreCreationError> {
    let id = StoreId(builder.ids.new_id());
    let pending = Arc::new(builder.layout.begin_bootstrap(id, builder.ids.as_ref()).await?);
    match prepare(builder, pending.clone(), name, publish).await {
        Ok(handle) => Ok(handle),
        Err(error @ StoreCreationError::PublishedButNotSynced { .. }) => Err(error),
        Err(error) => Err(match crate::coven::blocking(move || pending.cancel()).await {
            Ok(()) => error,
            Err(cleanup) => error.with_cleanup(cleanup.into()),
        }),
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
    let prepared = crate::coven::blocking(move || {
        let keychain = Arc::new(StoreKeychain::new(
            builder.keychain().map_err(CovenError::from)?,
            directory.id(),
        ));
        let mut owners = builder.owners(directory, keychain)?;
        let result = owners.custody.initialize_identity(MemberId(owners.ids.new_id()));
        if matches!(result, Err(IdentityError::AlreadyInitialized)) {
            return Err(IdentityError::AlreadyInitialized.into());
        }
        let result = result.map_err(StoreCreationError::from).and_then(|_| {
            owners.keychain.set_device_id(owners.device).map_err(CovenError::from)?;
            publish(&pending).map_err(StoreCreationError::from)
        });
        match result {
            Ok(()) => Ok((owners, None)),
            Err(
                error @ StoreCreationError::Directory(BootstrapDirectoryError::Create(
                    coven_foundation::files::DirectoryCreationError::Published { .. },
                )),
            ) => Ok((owners, Some(error))),
            Err(mut error) => {
                // Try every compensation, retaining every failure. The marker stays hidden.
                for cleanup in [owners.identity.forget(), owners.keychain.delete_device_id()] {
                    if let Err(cleanup) = cleanup {
                        error = error.with_cleanup(CovenError::from(cleanup).into());
                    }
                }
                Err(error)
            }
        }
    })
    .await;
    let (owners, publication) = match prepared {
        Ok(owners) => owners,
        Err(error) => return Err(close_failed(&database, error).await),
    };
    let handle = owners.into_handle(database, waits, false);
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
