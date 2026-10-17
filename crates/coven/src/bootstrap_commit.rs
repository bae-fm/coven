//! Publish settings, custody and the loaded directory as the final bootstrap step.

use super::*;

/// An OAuth store commits `sign_in`'s current tokens, which then refresh in
/// the handle's device custody; a rollback holds them in the session again.
pub(super) fn publish_bootstrap(
    pending: &BootstrapStore,
    code: RestoreCode,
    credentials: StorageCredentials,
    sign_in: Option<Arc<coven_sync::SessionSignIn>>,
    ring: StoreKeyring,
    owners: OpeningOwners,
    database: Database,
    waits: Arc<coven_sync::RetryWaits>,
) -> Result<CovenHandle, BootstrapError> {
    let directory = pending.directory();
    let keys = &owners.keys;
    let identity = &owners.identity;
    let keychain = &owners.keychain;
    let old_keys = keys.read()?;
    let old_identity = identity.read()?;
    let old_device = keychain.device_id()?;
    let mut credential_rollback = None;
    let location = StorageConfig::decode(code.storage.as_bytes()).map_err(SyncError::from)?;
    let settings = StorageSettings::new(directory.clone());
    let old_settings = settings.read().map_err(SyncError::from)?;
    let mut commit = || -> Result<(), BootstrapError> {
        settings.commit(&location).map_err(SyncError::from)?;
        keys.persist(&ring)?;
        identity.persist(&MemberIdentity::new(code.member, code.member_keys.clone()))?;
        keychain.set_device_id(owners.device)?;
        credential_rollback = Some(match &sign_in {
            Some(session) => session.publish(&owners.credentials, |keychain, tokens| {
                coven_sync::commit_credentials(
                    keychain,
                    &StorageCredentials::OAuth(tokens.clone()),
                    Some(&code),
                )
            })?,
            None => owners.credentials.write(|keychain| {
                coven_sync::commit_credentials(keychain, &credentials, Some(&code))
            })?,
        });
        Ok(pending.publish()?)
    };
    let publication_error = match commit() {
        Ok(()) => None,
        Err(
            error @ BootstrapError::Directory(BootstrapDirectoryError::Create(
                coven_foundation::files::DirectoryCreationError::Published { .. },
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
            ] {
                if let Err(cleanup) = rollback {
                    error =
                        combine::<()>(Err(error), Err(cleanup.into())).expect_err("failed commit");
                }
            }
            if let Some(previous) = credential_rollback {
                if let Err(cleanup) = owners
                    .credentials
                    .write(|keychain| previous.restore(keychain))
                {
                    error =
                        combine::<()>(Err(error), Err(cleanup.into())).expect_err("failed commit");
                }
                if let Some(session) = &sign_in {
                    session.unpublish();
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
    let sync = owners.sync(database.clone(), waits.clone());
    let handle = owners.handle(database, sync, waits);
    let cleanup = directory
        .owned_file(StoreFile::Bootstrap)
        .remove()
        .map_err(|error| BootstrapError::from(SyncError::from(error)));
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
