//! Reset this installation as a new device, at the app's call (§19.3).

use super::*;

/// Replace this installation with a new device loaded from storage, whatever
/// the app's reason: another copy using this device's id, a missing device
/// entry, or a damaged database (§19.3). Settings, credentials, the
/// connection and both key custodies are checked first; the new device then
/// loads beside the old one, which stays in place until the member's keys
/// have been checked and the load is complete. The new device names the old
/// id as the one it replaces (§13.1). Only the old database's operation
/// journal is read for owed provider cleanup and open invites; an unconnected
/// store also keeps its local creation name.
pub(crate) async fn reset_device(
    mut builder: CovenBuilder,
    store: StoreId,
    device_name: &str,
    status: impl Fn(BootstrapStatus),
) -> Result<CovenHandle, BootstrapError> {
    let directory = builder.directories.store_dir(&store);
    let keychain = Arc::new(StoreKeychain::new(builder.keychain()?, store));
    if coven_storage::recorded_config(&directory).map_err(SyncError::from)?.is_none() {
        return reset_unconnected(builder, store, keychain, &status).await;
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

/// A store that never set up storage has no stored history and no published
/// device. Its replacement is an empty database under the fresh id, with its
/// local creation name, the S3 detection runs whose cleanup it still owes
/// (§20.7), and the notice that unsent local work was discarded (§19.3).
async fn reset_unconnected(
    builder: CovenBuilder,
    store: StoreId,
    keychain: Arc<StoreKeychain>,
    status: &impl Fn(BootstrapStatus),
) -> Result<CovenHandle, BootstrapError> {
    let replacement = builder
        .directories
        .begin_replacement(store, ReplacedBy::Reset, builder.ids.as_ref())
        .await?;
    let reset = replacement.device_reset();
    if !replacement.is_swapped() {
        let prepared = async {
            let name = coven_database::discarded_store_name(
                replacement.discarded_database_path(),
                builder.directories.bugs(),
            )
            .await
            .map_err(CovenError::from)?;
            // As a connected reset reads it: a journal SQLite reports
            // damaged carries nothing.
            let path = replacement.discarded_database_path();
            let carried = match coven_database::discarded_operations(
                path,
                builder.directories.bugs(),
            )
            .await
            {
                Ok(records) => coven_sync::DetectionRecord::carried(&records),
                Err(coven_database::DbError::DamagedDatabase) => Vec::new(),
                Err(error) => return Err(CovenError::from(error).into()),
            };
            let database =
                super::open_installation(builder.database(replacement.directory())?, status)
                    .await?;
            let recorded = async {
                database
                    .initialize_store_name(name.clone(), coven_merge::Slot::FIRST)
                    .await
                    .map_err(CovenError::from)?;
                #[cfg(test)]
                if builder.fail_device_reset_notice {
                    database.test_fail_device_reset().await.map_err(CovenError::from)?;
                }
                database
                    .record_device_reset::<coven_sync::PendingRecord>(reset, carried, Vec::new())
                    .await
                    .map_err(CovenError::from)?;
                Ok::<_, BootstrapError>(())
            }
            .await;
            combine(recorded, close_database(&database).await).map(|()| name)
        }
        .await;
        let name = match prepared {
            Ok(name) => name,
            Err(error) => return combine(Err(error), replacement.cancel().map_err(Into::into)),
        };
        replacement.swap(name)?;
        #[cfg(test)]
        if builder.interrupt_after_swap {
            // As if the process stopped here: nothing is undone.
            return Err(BootstrapError::Cancelled);
        }
    }
    // From the swap on, a failure leaves it for the next reset to finish.
    let (database, waits) = replace::open_swapped(&builder, &replacement, status).await?;
    let owners = match builder.owners(
        replacement.directory(),
        keychain.clone(),
        crate::builder::Continuing::Fresh,
    ) {
        Ok(owners) => owners,
        Err(error) => return replace::undo(error.into(), database).await,
    };
    if let Err(error) = keychain.set_device_id(reset.new_device) {
        return replace::undo(error.into(), database).await;
    }
    replace::publish(
        &replacement,
        owners,
        database,
        waits,
        coven_database::RecordedLocation::None,
        None,
    )
    .await
}

#[cfg(test)]
#[path = "bootstrap_reset_tests.rs"]
mod tests;
