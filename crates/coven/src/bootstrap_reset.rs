//! Finish a recorded device reset as a new installation (§10, §19.2).

use super::*;

/// Replace this installation with a new device loaded from storage: a
/// recorded stale-copy reset (§10), or a damaged database (§19.2). Settings,
/// credentials, the connection and both key custodies are checked before
/// anything moves; a failure leaves the files in place with its typed error.
/// The new device names the old id as the one it replaces (§12.1). Only the
/// old database's operation journal is read, for the S3 keys it made or owed
/// (T203).
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
    let data = coven_sync::read_connection(&location, &keychain)?.ok_or(SyncError::NoStorage)?;
    // The held sign-in refreshes in this session until the new device
    // publishes it, like any bootstrap's (§4).
    let sign_in = match &data.credentials {
        StorageCredentials::OAuth(tokens) => Some(coven_sync::SessionSignIn::new(
            tokens.clone(),
            builder.oauth.clone(),
        )),
        _ => None,
    };
    // One custody instance per key kind reads now and keeps them at publication.
    let identity = CovenBuilder::make_identity(
        std::mem::replace(&mut builder.identity, IdentityCustody::InMemory),
        &directory,
        store,
        keychain.clone(),
    );
    builder.identity = IdentityCustody::Custom(identity.clone());
    let member = KeySession::member(identity)?
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
            super::provider_auth(&data, sign_in.as_ref())?,
            settings.device_id,
            probe,
            // The damaged database cannot hold a cooldown for this one probe.
            Arc::new(coven_storage::NoCooldown),
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
        member: member.member(),
        member_keys: member.keys().clone(),
        storage: data.location.encode().map_err(SyncError::from)?,
    };
    let (_, never) = watch::channel(false);
    bootstrap_device(
        builder,
        BootstrapRequest::Restore {
            code,
            data,
            sign_in,
            name: device_name.into(),
        },
        |_| {},
        &never,
    )
    .await?
    .ok_or_else(|| SyncError::from(StorageFailure::MemberKeysMissing).into())
}

/// Keep what the discarded database made or owed at the provider pending
/// deletion in its replacement, which has loaded the store log (§10, T203).
/// A database too damaged to read, whatever the reset's reason, carries
/// nothing: its keys are left to an admin's catch-up, which lists the
/// provider's keys for the store (§13). Any other failure fails the reset,
/// which retries.
pub(super) async fn carry_discarded_keys(
    database: &coven_database::Database,
    pending: &BootstrapStore,
    reset: &DeviceReset,
) -> Result<(), BootstrapError> {
    let path = pending.discarded_database_path(reset);
    let records = match coven_database::discarded_operations(path).await {
        Ok(records) => records,
        Err(coven_database::DbError::DamagedDatabase) => return Ok(()),
        Err(error) => return Err(SyncError::from(error).into()),
    };
    // A journal row that does not decode is damage too.
    let Ok(keys) = coven_sync::discarded_access_keys(&records) else {
        return Ok(());
    };
    coven_sync::carry_access_keys(database, keys)
        .await
        .map_err(Into::into)
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
    let waits = Arc::new(
        coven_sync::RetryWaits::open(database.clone(), builder.clock.clone())
            .await
            .map_err(CovenError::from)?,
    );
    let owners = builder.owners(directory, keychain)?;
    let sync = owners.sync(database.clone(), waits.clone());
    Ok(owners.handle(database, sync, waits))
}

#[cfg(test)]
#[path = "bootstrap_reset_tests.rs"]
mod tests;
