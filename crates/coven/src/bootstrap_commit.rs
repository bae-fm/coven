//! Publish custody and the loaded directory as the final bootstrap step.

use super::*;

pub(super) fn publish_bootstrap(
    pending: BootstrapStore,
    code: RestoreCode,
    ring: StoreKeyring,
    keys: KeyCustody,
    identity: IdentityCustody,
    keychain: Arc<Keychain>,
) -> Result<StoreDir, BootstrapError> {
    let directory = pending.directory();
    let settings = directory.settings().map_err(CovenError::from)?;
    let keychain = Arc::new(StoreKeychain::new(keychain, code.store));
    let keys = CovenBuilder::make_keys(keys, &directory, code.store, keychain.clone());
    let identity = CovenBuilder::make_identity(identity, &directory, code.store, keychain.clone());
    let old_keys = keys.unlock()?;
    let old_identity = identity.unlock()?;
    let old_device = keychain.device_id()?;
    let old_credentials = keychain.storage_credentials()?;
    let old_code = if keychain.supports_synced_restore_codes() {
        keychain.synced_restore_code()?
    } else {
        None
    };
    let data = RestoreStorage::decode(code.storage.as_bytes()).map_err(SyncError::from)?;
    StorageSettings::new(directory.clone())
        .commit(&data.location)
        .map_err(SyncError::from)?;
    let commit = || -> Result<StoreDir, BootstrapError> {
        keys.persist(&ring)?;
        identity.persist(&code.member_keys)?;
        keychain.set_device_id(settings.device_id)?;
        coven_sync::commit_restore_code(&keychain, &code)?;
        Ok(pending.publish()?)
    };
    let result = match commit() {
        Ok(published) => published,
        Err(
            error @ BootstrapError::Directory(BootstrapDirectoryError::Create(
                coven_foundation::files::StoreCreationError::Published { .. },
            )),
        ) => return Err(error),
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
            return Err(error);
        }
    };
    result
        .owned_file(StoreFile::Bootstrap)
        .remove()
        .map_err(|error| BootstrapError::Published {
            store: result.clone(),
            source: Box::new(SyncError::Disk(error).into()),
        })?;
    Ok(result)
}

#[cfg(test)]
#[path = "bootstrap_commit_tests.rs"]
mod tests;
