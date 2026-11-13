//! Reset this installation as a new device, at the app's call (§19.3).

use super::*;

/// Replace this installation with a new device loaded from storage, whatever
/// the app's reason: another copy using this device's id, a missing device
/// entry, or a damaged database (§19.3). Settings, credentials, the
/// connection and both key custodies are checked first; the new device then
/// loads beside the old one, which stays in place until the member's keys
/// have been checked and the load is complete. The new device names the old
/// id as the one it replaces (§13.1). Only the old database's operation
/// journal is read for owed provider cleanup and open invites.
pub(crate) async fn reset_device(
    mut builder: CovenBuilder,
    store: StoreId,
    device_name: &str,
    status: impl Fn(BootstrapStatus),
) -> Result<CovenHandle, BootstrapError> {
    let directory = builder.directories.store_dir(&store);
    let keychain = Arc::new(StoreKeychain::new(builder.keychain()?, store));
    // A store that never set up storage has nothing in storage to load, and
    // its creation names the device a reset would replace (§19.3).
    if coven_storage::recorded_config(&directory).map_err(SyncError::from)?.is_none() {
        return Err(SyncError::NoStorage.into());
    }
    let data = coven_sync::read_connection(&directory, &keychain)?.ok_or(SyncError::NoStorage)?;
    // The held sign-in refreshes in this session until the new device
    // publishes it, like any bootstrap's (§20.2).
    let sign_in = match &data.credentials {
        StorageCredentials::OAuth(tokens) => {
            Some(coven_sync::SessionSignIn::new(tokens.clone(), builder.oauth_exchange()))
        }
        _ => None,
    };
    // One custody instance per key kind reads now and keeps them at publication.
    let identity = CovenBuilder::make_session(
        std::mem::replace(
            &mut builder.identity,
            SessionSource::Configured(CustodyChoice::InMemory),
        ),
        &directory,
        store,
        keychain.clone(),
    )?;
    builder.identity = SessionSource::Session(identity.clone());
    let member = identity.read()?.ok_or(SyncError::from(StorageFailure::MemberKeysMissing))?;
    let keys = CovenBuilder::make_session(
        std::mem::replace(&mut builder.keys, SessionSource::Configured(CustodyChoice::InMemory)),
        &directory,
        store,
        keychain,
    )?;
    // Unlocking must succeed; loading acquires any store key it lacks.
    builder.keys = SessionSource::Session(keys);
    // Check the location marker and slot 1 before anything is loaded.
    let storage = builder
        .connector()
        .connect(
            data.location.clone(),
            super::provider_auth(
                &data,
                &sign_in.clone().map_or(SignInCustody::None, SignInCustody::Session),
            )?,
            // The probe only reads, as this installation's device.
            directory.settings().map_err(CovenError::from)?.device_id,
            Arc::new(crate::storage::NoPublications),
            // The damaged database cannot hold a cooldown for this one probe.
            Arc::new(coven_storage::NoCooldown),
            builder.transfers.clone(),
        )
        .await
        .map_err(SyncError::from)?;
    storage.check_location_claim(store).await.map_err(SyncError::from)?;
    storage
        .status(&coven_storage::ObjectPath::slot(store, std::num::NonZeroU64::MIN))
        .await
        .map_err(SyncError::from)?;
    let code = coven_sync::location_code(store, &member, data.location.clone())?;
    let (_, never) = watch::channel(false);
    // Saved keys that history has replaced are refused before the swap.
    replace::replace_installation(
        builder,
        store,
        BootstrapRequest::Restore {
            code,
            data,
            sign_in: sign_in.map_or(SignInCustody::None, SignInCustody::Session),
            name: device_name.into(),
        },
        replace::Replacing::Reset,
        status,
        &never,
    )
    .await
}

/// Carry provider cleanup from the replaced installation's journal and
/// commit the reset notice together (§19.3). A journal SQLite reports
/// damaged carries nothing; any other failure fails the replacement, which
/// retries.
pub(super) async fn carry_discarded_provider_work(
    sync: &SequenceSync,
    database: &Database,
    replacement: &ReplacementStore,
) -> Result<(), BootstrapError> {
    // A resumed swap already holds the notice, and a restore may have
    // removed the old database since.
    if database.device_reset().await? == Some(replacement.device_reset()) {
        return Ok(());
    }
    let path = replacement.discarded_database_path();
    let records = match coven_database::discarded_operations(path, database.bugs()).await {
        Ok(records) => records,
        Err(coven_database::DbError::DamagedDatabase) => Vec::new(),
        Err(error) => return Err(SyncError::from(error).into()),
    };
    Ok(sync.record_device_reset(replacement.device_reset(), &records).await?)
}

#[cfg(test)]
#[path = "bootstrap_reset_tests.rs"]
mod tests;
