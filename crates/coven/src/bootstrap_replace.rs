//! Replace this device's installation of a store with a new one, loaded from
//! storage beside the old one until it is ready (§19.3).

use super::*;
use coven_foundation::files::DirectoryCreationError;

/// Load the new installation beside the old one, swap it into the store's
/// directory, commit its custody, then publish it. Until the swap, the old
/// installation and every keychain entry stay as they were, and the store
/// opens as before. A failure after this call's own swap, with custody
/// unchanged, puts the old installation back. A swap interrupted any other
/// way only moves forward: the next call finishes it.
pub(super) async fn replace_installation(
    mut builder: CovenBuilder,
    store: StoreId,
    request: BootstrapRequest,
    status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<CovenHandle, BootstrapError> {
    check_cancel(cancel)?;
    let keychain = builder.keychain()?;
    status(BootstrapStatus::Connecting);
    let replacement = builder
        .layout
        .begin_replacement(store, builder.ids.as_ref())
        .await?;
    let swapping = !replacement.is_swapped();
    let loaded = prepare_and_load(
        &mut builder,
        Installing::Replacing(&replacement),
        &request,
        &status,
        cancel,
    )
    .await;
    let Loaded {
        code,
        credentials,
        sign_in,
        ring,
        database,
        waits,
        ..
    } = match loaded {
        Ok(Some(loaded)) => loaded,
        Ok(None) => unreachable!("only a join waits for approval"),
        Err(error) if swapping && abandons(&error) => {
            return combine(Err(error), replacement.cancel().map_err(Into::into))
        }
        // The loaded work, and once registered its fresh id, wait for a retry.
        Err(error) => return Err(error),
    };
    let (database, waits) = if swapping {
        // The loading database closes before its files move.
        database.close().await.map_err(CovenError::from)?;
        if let Err(error) = check_cancel(cancel) {
            return combine(Err(error), replacement.cancel().map_err(Into::into));
        }
        replacement.swap()?;
        #[cfg(test)]
        if builder.interrupt_after_swap {
            // As if the process stopped here: nothing is undone.
            return Err(BootstrapError::Cancelled);
        }
        match open_swapped(&builder, &replacement).await {
            Ok(opened) => opened,
            Err(error) => {
                return combine(Err(error), replacement.unswap().await.map_err(Into::into))
            }
        }
    } else {
        (database, waits)
    };
    status(BootstrapStatus::Loading);
    builder.authentication.take();
    let directory = replacement.directory();
    let owners = match builder.owners(
        directory.clone(),
        Arc::new(StoreKeychain::new(keychain, store)),
    ) {
        Ok(owners) => owners,
        Err(error) => return undo(error.into(), database, swapping.then_some(replacement)).await,
    };
    // The swapped store's own publication follows, and moves only forward.
    if let Err(uncommitted) = commit::commit(
        &directory,
        &code,
        &credentials,
        sign_in.as_ref(),
        &ring,
        &owners,
        || Ok(()),
    ) {
        return match uncommitted {
            commit::Uncommitted::Restored(error) => {
                undo(error, database, swapping.then_some(replacement)).await
            }
            // Custody may name the new device: only finishing is safe.
            commit::Uncommitted::Partly(error) => undo(error, database, None).await,
        };
    }
    publish(&replacement, owners, database, waits, true).await
}

/// Publish the committed replacement and hand back its open store. A failure
/// before publication leaves the swap for the next call to finish.
pub(super) async fn publish(
    replacement: &ReplacementStore,
    owners: OpeningOwners,
    database: Database,
    waits: Arc<coven_sync::RetryWaits>,
    configured: bool,
) -> Result<CovenHandle, BootstrapError> {
    match replacement.publish() {
        Ok(()) => Ok(owners.into_handle(database, waits, configured)),
        Err(error @ BootstrapDirectoryError::Create(DirectoryCreationError::Published { .. })) => {
            Err(BootstrapError::Published {
                handle: owners.into_handle(database, waits, configured),
                source: Box::new(error.into()),
            })
        }
        Err(error) => combine(Err(error.into()), close(database).await),
    }
}

/// A refusal no retry can change, or cancellation, removes the unswapped
/// replacement: the store stays exactly as it was.
pub(super) fn abandons(error: &BootstrapError) -> bool {
    matches!(
        error,
        BootstrapError::Cancelled
            | BootstrapError::Sync(
                SyncError::MemberKeysReplaced { .. }
                    | SyncError::Removed
                    | SyncError::Rejected(coven_sync::OperationRejection::TargetGone(
                        coven_sync::EntryTarget::Member(_)
                    ))
            )
    )
}

/// Open the swapped installation's database in the store's directory.
pub(super) async fn open_swapped(
    builder: &CovenBuilder,
    replacement: &ReplacementStore,
) -> Result<(Database, Arc<coven_sync::RetryWaits>), BootstrapError> {
    let database = builder.database(replacement.directory())?.open().await?;
    match coven_sync::RetryWaits::open(database.clone(), builder.clock.clone()).await {
        Ok(waits) => Ok((database, Arc::new(waits))),
        Err(error) => combine(Err(CovenError::from(error).into()), close(database).await),
    }
}

/// Close the swapped installation's database after a failure, then put the
/// old installation back when this call swapped it and nothing is committed.
pub(super) async fn undo(
    error: BootstrapError,
    database: Database,
    unswap: Option<ReplacementStore>,
) -> Result<CovenHandle, BootstrapError> {
    let result = combine(Err(error), close(database).await);
    match unswap {
        Some(replacement) => combine(result, replacement.unswap().await.map_err(Into::into)),
        None => result,
    }
}

async fn close(database: Database) -> Result<(), BootstrapError> {
    database
        .close()
        .await
        .map_err(CovenError::from)
        .map_err(BootstrapError::from)
}
