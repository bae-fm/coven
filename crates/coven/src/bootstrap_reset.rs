//! Finish a recorded device reset as a new installation (§10, §19.2).

use super::*;

/// Replace this installation with a new device loaded from storage: a
/// recorded stale-copy reset (§10), or a damaged database (§19.2). Settings,
/// credentials, the connection and both key custodies are checked before
/// anything moves; a failure leaves the files in place with its typed error.
/// The new device names the old id as the one it replaces (§12.1). Nothing
/// is read from the old database.
pub(crate) async fn reset_device(
    mut builder: CovenBuilder,
    store: StoreId,
    device_name: &str,
) -> Result<CovenHandle, BootstrapError> {
    let directory = builder.layout.store_dir(&store);
    let recorded = directory
        .unfinished_device_reset()
        .map_err(CovenError::from)?;
    let settings = directory.settings().map_err(CovenError::from)?;
    let keychain = Arc::new(StoreKeychain::new(builder.keychain()?, store));
    let location = StorageSettings::new(directory.clone());
    if location.read().map_err(SyncError::from)?.is_none() {
        // Only a decided reset may proceed without storage to load from.
        if recorded.is_none() {
            return Err(SyncError::NoStorage.into());
        }
        return reset_unconnected(builder, store, &settings.name, keychain).await;
    }
    let mut data =
        coven_sync::read_connection(&location, &keychain)?.ok_or(SyncError::NoStorage)?;
    if let Some(credentials) =
        coven_sync::refreshed_credentials(&data, builder.oauth_flow().as_ref(), builder.clock.now())
            .await?
    {
        coven_sync::commit_credentials(&keychain, &credentials, None)?;
        data.credentials = credentials;
    }
    // One custody instance per key kind reads now and keeps them at publication.
    let identity = CovenBuilder::make_identity(
        std::mem::replace(&mut builder.identity, IdentityCustody::InMemory),
        &directory,
        store,
        keychain.clone(),
    );
    builder.identity = IdentityCustody::Custom(identity.clone());
    let member_keys = KeySession::member(identity)?
        .read()?
        .ok_or(SyncError::from(StorageFailure::MemberKeysMissing))?;
    let keys = CovenBuilder::make_keys(
        std::mem::replace(&mut builder.keys, KeyCustody::InMemory),
        &directory,
        store,
        keychain,
    );
    builder.keys = KeyCustody::Custom(keys.clone());
    // Unlocking must succeed; loading acquires any store key it lacks.
    KeySession::store(keys)?;
    // The connection is reached before the damaged files move.
    let probe = Arc::new(coven_sync::SendGate::new(builder.clock.clone()));
    builder
        .connector()
        .connect(
            data.location.clone(),
            data.credentials.clone(),
            settings.device_id,
            probe,
        )
        .await
        .map_err(SyncError::from)?
        .list_writers(store)
        .await
        .map_err(SyncError::from)?;
    if recorded.is_none() {
        // The writer lock excludes a live handle while the decision is made.
        let lock = directory.lock_for_open().await.map_err(CovenError::from)?;
        directory
            .record_device_reset(&DeviceReset {
                old_device: settings.device_id,
                new_device: builder.ids.new_device_id(),
                reason: DeviceResetReason::DatabaseDamage,
            })
            .map_err(SyncError::from)?;
        drop(lock);
    }
    let code = RestoreCode {
        store,
        name: settings.name,
        member_keys,
        storage: data.location.encode().map_err(SyncError::from)?,
    };
    let (_, never) = watch::channel(false);
    bootstrap_device(
        builder,
        BootstrapRequest::Restore {
            code,
            data,
            name: device_name.into(),
        },
        |_| {},
        &never,
    )
    .await?
    .ok_or_else(|| SyncError::from(StorageFailure::MemberKeysMissing).into())
}

/// A store that never set up storage has no stored history and no published
/// device. Its replacement is an empty database under the fresh id, with the
/// notice that unsent local work was discarded (§10).
async fn reset_unconnected(
    builder: CovenBuilder,
    store: StoreId,
    name: &str,
    keychain: Arc<StoreKeychain>,
) -> Result<CovenHandle, BootstrapError> {
    let pending = builder
        .layout
        .begin_bootstrap(store, name, builder.ids.as_ref())
        .await?;
    let reset = pending
        .device_reset()
        .map_err(SyncError::from)?
        .ok_or(SyncError::NoStorage)?;
    let directory = pending.directory();
    let database = builder.database(directory.clone())?.open().await?;
    let published = async {
        database
            .record_device_reset(reset)
            .await
            .map_err(CovenError::from)?;
        keychain.set_device_id(reset.new_device)?;
        pending.publish()?;
        Ok::<_, BootstrapError>(())
    }
    .await;
    if let Err(error) = published {
        return combine(
            Err(error),
            database
                .close()
                .await
                .map_err(CovenError::from)
                .map_err(BootstrapError::from),
        );
    }
    let owners = builder.owners(directory, keychain)?;
    let sync = owners.sync(database.clone());
    Ok(owners.handle(database, sync))
}

#[cfg(test)]
#[path = "bootstrap_reset_tests.rs"]
mod tests;
