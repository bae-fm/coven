//! Replace this device's installation of a store with a new one, loaded from
//! storage beside the old one until it is ready (§19.3).

use super::*;
use coven_foundation::files::DirectoryCreationError;

/// Restore this device's installation of `store` from the person's current
/// restore code, in place of the installation it holds (§13.1). The code must
/// name this store; its member keys are checked against history before the
/// store or keychain changes. A device whose custody holds the code's pair and whose
/// applied history binds it has nothing to restore, which is also what a
/// retry finds after an interruption once the restore was published: the
/// store simply opens.
pub(crate) async fn restore_device(
    mut builder: CovenBuilder,
    store: StoreId,
    code: &str,
    s3_credentials: Option<S3Credentials>,
    device_name: &str,
    status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<CovenHandle, BootstrapError> {
    check_cancel(cancel)?;
    let code = coven_sync::read_restore_code(code)?;
    if code.store != store {
        return Err(CodeError::WrongStore {
            expected: store,
            actual: code.store,
        }
        .into());
    }
    let location = StorageConfig::decode(code.storage.as_bytes()).map_err(SyncError::from)?;
    let (data, sign_in) = restore_connection(&builder, location, s3_credentials)?;
    let directory = builder.layout.store_dir(&store);
    let keychain = Arc::new(StoreKeychain::new(builder.keychain()?, store));
    // One identity session reads now and keeps the code's pair at publication.
    let identity = CovenBuilder::make_identity(
        std::mem::replace(
            &mut builder.identity,
            CustodyChoice::Configured(IdentityCustody::InMemory),
        ),
        &directory,
        store,
        keychain,
    )?;
    builder.identity = CustodyChoice::Session(identity.clone());
    let pair = code.member_keys.public_keys();
    let held = identity
        .read()?
        .is_some_and(|held| held.member() == code.member && held.keys().public_keys() == pair);
    let unfinished = directory
        .unfinished_replacement()
        .map_err(CovenError::from)?;
    if held && unfinished.is_none() {
        // Custody alone also holds a stale pair; the applied history decides.
        let database = builder.database(directory)?.open_read_only().await?;
        let state = database.store_state().await;
        database.close().await.map_err(CovenError::from)?;
        let bound = state?
            .members
            .get(&code.member)
            .is_some_and(|member| !member.removed && member.keys == pair);
        if bound {
            return Ok(builder.open(store).await?);
        }
    }
    replace_installation(
        builder,
        store,
        BootstrapRequest::Restore {
            code,
            data,
            sign_in,
            name: device_name.into(),
        },
        ReplacedBy::Restore,
        status,
        cancel,
    )
    .await
}

/// Load the new installation beside the old one, swap it into the store's
/// directory, commit its custody, settle the old installation's local state,
/// then publish it. A reset keeps the old local state for inspection; a
/// restore removes it (§13.1). Until the swap, the old installation and every
/// keychain entry stay as they were, and the store opens as before. A failure
/// after this call's own swap, with custody unchanged, puts the old
/// installation back. A swap interrupted any other way only moves forward:
/// the next call of the same kind finishes it, and the other is refused.
pub(super) async fn replace_installation(
    mut builder: CovenBuilder,
    store: StoreId,
    request: BootstrapRequest,
    by: ReplacedBy,
    status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<CovenHandle, BootstrapError> {
    check_cancel(cancel)?;
    let keychain = builder.keychain()?;
    status(BootstrapStatus::Connecting);
    let replacement = builder
        .layout
        .begin_replacement(store, by, builder.ids.as_ref())
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
        // Listings show this name until the swap finishes.
        let name = database.store_name().await;
        // The loading database closes before its files move.
        database.close().await.map_err(CovenError::from)?;
        let name = name?;
        if let Err(error) = check_cancel(cancel) {
            return combine(Err(error), replacement.cancel().map_err(Into::into));
        }
        replacement.swap(name)?;
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
    let name = match commit::loaded_name(&database).await {
        Ok(name) => name,
        Err(error) => return undo(error, database, swapping.then_some(replacement)).await,
    };
    // The swapped store's own publication follows, and moves only forward.
    if let Err(uncommitted) = commit::commit(
        &directory,
        &code,
        &name,
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
    if by == ReplacedBy::Restore {
        if let Err(error) = replacement.remove_replaced() {
            return combine(Err(error.into()), close(database).await);
        }
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

#[cfg(test)]
#[path = "bootstrap_replace_tests.rs"]
mod tests;
