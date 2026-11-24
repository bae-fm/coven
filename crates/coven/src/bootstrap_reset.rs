//! Reset this installation as a new device, at the app's call (§19.3).

use super::*;

#[path = "bootstrap_reset_copy.rs"]
mod copy;

/// Replace this installation with a new device loaded from storage, whatever
/// the app's reason: another copy using this device's id, a missing device
/// entry, or a damaged database (§19.3). Settings, credentials, the
/// connection and both key custodies are checked first; the new device then
/// loads beside the old one, which stays in place until the member's keys
/// have been checked and the load is complete. The new device names the old
/// id as the one it replaces (§13.1). Only the old database's operation
/// journal is read for owed provider cleanup and open invites. On Dropbox
/// and OneDrive it connects through a sign-in the builder holds, else the
/// one the store kept; the provider refusing the kept one ends the call with
/// `OAuthError::Reauthorize`.
pub(crate) async fn reset_device(
    builder: CovenBuilder,
    store: StoreId,
    device_name: &str,
    status: impl Fn(BootstrapStatus),
) -> Result<CovenHandle, BootstrapError> {
    let directory = builder.directories.store_dir(&store);
    let keychain = Arc::new(StoreKeychain::new(builder.keychain()?, store));
    let (data, sign_in) = connection(&builder, &directory, &keychain)?;
    match sign_in {
        ResetSignIn::Kept(kept) => {
            let provider = data.location.provider();
            let reset =
                reset_through(builder, store, keychain, data, Some(kept), device_name, status);
            kept_sign_in_refused(provider, reset.await)
        }
        ResetSignIn::Held(held) => {
            reset_through(builder, store, keychain, data, Some(held), device_name, status).await
        }
        ResetSignIn::None => {
            reset_through(builder, store, keychain, data, None, device_name, status).await
        }
    }
}

/// Reset `store` through `data`, with the sign-in it connects through on
/// Dropbox and OneDrive.
async fn reset_through(
    mut builder: CovenBuilder,
    store: StoreId,
    keychain: Arc<StoreKeychain>,
    data: ConnectionCredentials,
    sign_in: Option<Arc<SessionSignIn>>,
    device_name: &str,
    status: impl Fn(BootstrapStatus),
) -> Result<CovenHandle, BootstrapError> {
    let directory = builder.directories.store_dir(&store);
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
    // The old database is read only once the checks above passed, so a
    // reset they refuse leaves its files exactly as they were.
    let registration = copy::registration_of(&directory).await?;
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
        replace::Replacing::Reset(registration),
        status,
        &never,
    )
    .await
}

/// The sign-in a reset on Dropbox or OneDrive connects through. It refreshes
/// in this session until the new device publishes it, like any bootstrap's
/// (§20.2).
enum ResetSignIn {
    /// An S3 key or the Apple account: nothing signs in.
    None,
    /// The new sign-in the builder holds.
    Held(Arc<SessionSignIn>),
    /// The sign-in the store kept, which a refusal can't fix: only a new one
    /// can.
    Kept(Arc<SessionSignIn>),
}

/// The store's recorded location with the credentials a reset reaches it
/// with: on Dropbox and OneDrive a sign-in the builder holds, else the one
/// the store kept, and with neither the call needs a sign-in; any other
/// provider's credentials from the store's custody. A store that never set
/// up storage has nothing in storage to load, and its creation names the
/// device a reset would replace (§19.3).
fn connection(
    builder: &CovenBuilder,
    directory: &StoreDir,
    keychain: &StoreKeychain,
) -> Result<(ConnectionCredentials, ResetSignIn), BootstrapError> {
    let location = coven_storage::recorded_config(directory)
        .map_err(SyncError::from)?
        .ok_or(SyncError::NoStorage)?;
    let provider = location.provider();
    if provider.signs_in_with_oauth() && builder.authentication.is_some() {
        let (held, tokens) = held_sign_in(builder, &location)?;
        let data =
            ConnectionCredentials { location, credentials: StorageCredentials::OAuth(tokens) };
        return Ok((data, ResetSignIn::Held(held)));
    }
    let data = match coven_sync::read_connection(directory, keychain)? {
        Some(data) => data,
        None if provider.signs_in_with_oauth() => {
            return Err(sign_in_missing(builder, provider).into())
        }
        None => return Err(SyncError::NoStorage.into()),
    };
    let sign_in = match &data.credentials {
        StorageCredentials::OAuth(tokens) => {
            ResetSignIn::Kept(SessionSignIn::new(tokens.clone(), builder.oauth_exchange()))
        }
        _ => ResetSignIn::None,
    };
    Ok((data, sign_in))
}

/// Carry provider cleanup from the replaced installation's journal and
/// commit the reset notice together (§19.3). A journal SQLite reports
/// damaged carries nothing; any other failure fails the replacement, which
/// retries.
pub(super) async fn carry_discarded_provider_work(
    sync: &SequenceSync,
    database: &StoreDatabase,
    replacement: &ReplacementStore,
) -> Result<(), BootstrapError> {
    // A resumed swap already holds the notice, and a restore may have
    // removed the old database since.
    if database.device_reset().await? == Some(replacement.device_reset()) {
        return Ok(());
    }
    // A copy that found its id in use owes the provider nothing it carries:
    // the work owed from before the copies parted is the keeper's, and the
    // copy granted nothing since (§19.3).
    let records = match replacement.registration() {
        Registration::NewDevice => Vec::new(),
        Registration::ReplacesOld => {
            let path = replacement.discarded_database_path();
            match coven_database::discarded_operations(path, database.bugs()).await {
                Ok(records) => records,
                Err(coven_database::DbError::DamagedDatabase) => Vec::new(),
                Err(error) => return Err(SyncError::from(error).into()),
            }
        }
    };
    Ok(sync.record_device_reset(replacement.device_reset(), &records).await?)
}

#[cfg(test)]
#[path = "bootstrap_reset_tests.rs"]
mod tests;
