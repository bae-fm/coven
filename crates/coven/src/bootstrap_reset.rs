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
) -> Result<CovenHandle, BootstrapError> {
    let directory = builder.layout.store_dir(&store);
    let keychain = Arc::new(StoreKeychain::new(builder.keychain()?, store));
    let location = StorageSettings::new(directory.clone());
    if location.read().map_err(SyncError::from)?.is_none() {
        return reset_unconnected(builder, store, keychain).await;
    }
    let data = coven_sync::read_connection(&location, &keychain)?.ok_or(SyncError::NoStorage)?;
    // The held sign-in refreshes in this session until the new device
    // publishes it, like any bootstrap's (§20.2).
    let sign_in = match &data.credentials {
        StorageCredentials::OAuth(tokens) => Some(coven_sync::SessionSignIn::new(
            tokens.clone(),
            builder.oauth.clone(),
        )),
        _ => None,
    };
    // One custody instance per key kind reads now and keeps them at publication.
    let identity = CovenBuilder::make_identity(
        std::mem::replace(
            &mut builder.identity,
            CustodyChoice::Configured(IdentityCustody::InMemory),
        ),
        &directory,
        store,
        keychain.clone(),
    )?;
    builder.identity = CustodyChoice::Session(identity.clone());
    let member = identity
        .read()?
        .ok_or(SyncError::from(StorageFailure::MemberKeysMissing))?;
    let keys = CovenBuilder::make_keys(
        std::mem::replace(
            &mut builder.keys,
            CustodyChoice::Configured(KeyCustody::InMemory),
        ),
        &directory,
        store,
        keychain,
    )?;
    // Unlocking must succeed; loading acquires any store key it lacks.
    builder.keys = CustodyChoice::Session(keys);
    // Check bucket ownership and slot 1 before anything is loaded.
    let storage = builder
        .connector()
        .connect(
            data.location.clone(),
            super::provider_auth(
                &data,
                &sign_in
                    .clone()
                    .map_or(SignInCustody::None, SignInCustody::Session),
            )?,
            Arc::new(crate::storage::NoPublications),
            // The damaged database cannot hold a cooldown for this one probe.
            Arc::new(coven_storage::NoCooldown),
            builder.transfers.clone(),
        )
        .await
        .map_err(SyncError::from)?;
    storage.check_bucket(store).await.map_err(SyncError::from)?;
    storage
        .status(&coven_storage::ObjectPath::slot(
            store,
            std::num::NonZeroU64::MIN,
        ))
        .await
        .map_err(SyncError::from)?;
    let code = RestoreCode {
        store,
        member: member.member(),
        member_keys: member.keys().clone(),
        storage: data.location.encode().map_err(SyncError::from)?,
    };
    let (_, never) = watch::channel(false);
    // Saved keys that history has replaced are refused before the swap.
    replace::replace_installation(
        builder,
        store,
        BootstrapRequest::Restore {
            code,
            data,
            sign_in,
            name: device_name.into(),
        },
        ReplacedBy::Reset,
        |_| {},
        &never,
    )
    .await
}

/// Carry provider cleanup from the replaced installation's journal and
/// commit the reset notice together (§19.3). A database too damaged to read
/// carries nothing; any other failure fails the replacement, which retries.
/// Reading it checks its integrity, app indexes included, with the app's
/// collations.
pub(super) async fn carry_discarded_provider_work(
    sync: &SequenceSync,
    database: &Database,
    replacement: &ReplacementStore,
    collations: coven_database::Collations,
) -> Result<(), BootstrapError> {
    // A resumed swap already holds the notice, and a restore may have
    // removed the old database since.
    if database.device_reset().await? == Some(replacement.device_reset()) {
        return Ok(());
    }
    let path = replacement.discarded_database_path();
    let records = match coven_database::discarded_operations(path, collations).await {
        Ok(records) => records,
        Err(coven_database::DbError::DamagedDatabase) => Vec::new(),
        Err(error) => return Err(SyncError::from(error).into()),
    };
    Ok(sync
        .record_device_reset(replacement.device_reset(), &records)
        .await?)
}

/// A store that never set up storage has no stored history and no published
/// device. Its replacement is an empty database under the fresh id, with its
/// local creation name and the notice that unsent local work was discarded
/// (§19.3).
async fn reset_unconnected(
    builder: CovenBuilder,
    store: StoreId,
    keychain: Arc<StoreKeychain>,
) -> Result<CovenHandle, BootstrapError> {
    let replacement = builder
        .layout
        .begin_replacement(store, ReplacedBy::Reset, builder.ids.as_ref())
        .await?;
    let reset = replacement.device_reset();
    let swapping = !replacement.is_swapped();
    let (database, waits) = if swapping {
        let prepared = async {
            let name = coven_database::discarded_store_name(replacement.discarded_database_path())
                .await
                .map_err(CovenError::from)?;
            let database = builder.database(replacement.directory())?.open().await?;
            let recorded = async {
                database
                    .initialize_store_name(name.clone())
                    .await
                    .map_err(CovenError::from)?;
                database
                    .record_device_reset::<coven_sync::PendingRecord>(
                        reset,
                        Vec::new(),
                        coven_database::ProviderChanges::default(),
                    )
                    .await
                    .map_err(CovenError::from)?;
                Ok::<_, BootstrapError>(())
            }
            .await;
            combine(
                recorded,
                database
                    .close()
                    .await
                    .map_err(CovenError::from)
                    .map_err(BootstrapError::from),
            )
            .map(|()| name)
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
        match replace::open_swapped(&builder, &replacement).await {
            Ok(opened) => opened,
            Err(error) => {
                return combine(Err(error), replacement.unswap().await.map_err(Into::into))
            }
        }
    } else {
        replace::open_swapped(&builder, &replacement).await?
    };
    let owners = match builder.owners(replacement.directory(), keychain.clone()) {
        Ok(owners) => owners,
        Err(error) => {
            return replace::undo(error.into(), database, swapping.then_some(replacement)).await
        }
    };
    if let Err(error) = keychain.set_device_id(reset.new_device) {
        return replace::undo(error.into(), database, swapping.then_some(replacement)).await;
    }
    replace::publish(&replacement, owners, database, waits, false).await
}

#[cfg(test)]
#[path = "bootstrap_reset_tests.rs"]
mod tests;
