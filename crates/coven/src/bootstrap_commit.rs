//! Publish settings, custody and the loaded directory as the final bootstrap step.

use super::*;

pub(super) fn publish_bootstrap(
    pending: &BootstrapStore,
    code: RestoreCode,
    credentials: StorageCredentials,
    ring: StoreKeyring,
    owners: OpeningOwners,
    database: Database,
) -> Result<CovenHandle, BootstrapError> {
    let directory = pending.directory();
    let keys = &owners.keys;
    let identity = &owners.identity;
    let keychain = &owners.keychain;
    let old_keys = keys.read()?;
    let old_identity = identity.read()?;
    let old_device = keychain.device_id()?;
    let old_credentials = keychain.storage_credentials()?;
    let old_code = if keychain.supports_synced_restore_codes() {
        keychain.synced_restore_code()?
    } else {
        None
    };
    let data = RestoreStorage::decode(code.storage.as_bytes()).map_err(SyncError::from)?;
    let settings = StorageSettings::new(directory.clone());
    let old_settings = settings.read().map_err(SyncError::from)?;
    let commit = || -> Result<(), BootstrapError> {
        settings.commit(data.location()).map_err(SyncError::from)?;
        keys.persist(&ring)?;
        identity.persist(&code.member_keys)?;
        keychain.set_device_id(owners.device)?;
        coven_sync::commit_credentials(keychain, &credentials, Some(&code))?;
        Ok(pending.publish()?)
    };
    let publication_error = match commit() {
        Ok(()) => None,
        Err(
            error @ BootstrapError::Directory(BootstrapDirectoryError::Create(
                coven_foundation::files::StoreCreationError::Published { .. },
            )),
        ) => Some(error),
        Err(mut error) => {
            // Attempt every rollback even if an earlier one fails. A failure is
            // returned to this initiator; no background repair is installed.
            for rollback in [
                match old_keys {
                    Some(keys_before) => keys.persist(&keys_before),
                    None => keys.forget(),
                },
                match old_identity {
                    Some(identity_before) => identity.persist(&identity_before),
                    None => identity.forget(),
                },
                match old_device {
                    Some(device) => keychain.set_device_id(device),
                    None => keychain.delete_device_id(),
                },
                match old_credentials {
                    Some(bytes) => keychain.set_storage_credentials(&bytes),
                    None => keychain.delete_storage_credentials(),
                },
            ] {
                if let Err(cleanup) = rollback {
                    error =
                        combine::<()>(Err(error), Err(cleanup.into())).expect_err("failed commit");
                }
            }
            if keychain.supports_synced_restore_codes() {
                let rollback = match old_code {
                    Some(bytes) => keychain.set_synced_restore_code(&bytes),
                    None => keychain.delete_synced_restore_code(),
                };
                if let Err(cleanup) = rollback {
                    error =
                        combine::<()>(Err(error), Err(cleanup.into())).expect_err("failed commit");
                }
            }
            let rollback = match old_settings {
                Some(location) => settings.commit(&location),
                None => settings.remove(),
            };
            if let Err(cleanup) = rollback {
                error = combine::<()>(Err(error), Err(SyncError::from(cleanup).into()))
                    .expect_err("failed commit");
            }
            return Err(error);
        }
    };
    let sync = owners.sync(database.clone());
    let handle = owners.handle(database, sync);
    let cleanup = directory
        .owned_file(StoreFile::Bootstrap)
        .remove()
        .map_err(|error| BootstrapError::from(SyncError::Disk(error)));
    let result = match publication_error {
        Some(error) => combine(Err(error), cleanup),
        None => cleanup,
    };
    match result {
        Ok(()) => Ok(handle),
        Err(source) => Err(BootstrapError::Published {
            handle,
            source: Box::new(source),
        }),
    }
}

#[cfg(test)]
#[path = "bootstrap_commit_tests.rs"]
mod tests;
