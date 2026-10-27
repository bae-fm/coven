//! Publish settings, custody and the loaded directory as the final bootstrap step.

use super::*;
use coven_foundation::files::StoreDirAccess;

/// Committing a loaded installation's custody failed before publication.
pub(super) enum Uncommitted {
    /// Every value the commit changed holds its prior value again.
    Restored(BootstrapError),
    /// Restoring a prior value failed too; the error keeps both causes.
    Partly(BootstrapError),
}

impl Uncommitted {
    pub(super) fn into_error(self) -> BootstrapError {
        match self {
            Self::Restored(error) | Self::Partly(error) => error,
        }
    }
}

/// Commit the installation's storage settings, store keys, member identity,
/// device id and credentials, with the restore code saved beside the loaded
/// store's `name` (§13.1), then `publish` it. A restore's OAuth store commits
/// its session sign-in's current tokens, which then refresh in device
/// custody; a rollback holds them in the session again. A join's sign-in is
/// already the device's, refreshes included, and stays as it is: only the
/// code commits beside it, so a rollback leaves the join its kept sign-in
/// (§13.2). A failure before publication restores every prior value.
/// `Ok(Some(_))` is a failure after publication, which keeps custody
/// committed.
pub(super) fn commit(
    directory: &StoreDir,
    code: &RestoreCode,
    name: &coven_sync::SavedName,
    credentials: &StorageCredentials,
    sign_in: &SignInCustody,
    ring: &StoreKeyring,
    owners: &OpeningOwners,
    publish: impl FnOnce() -> Result<(), BootstrapDirectoryError>,
) -> Result<Option<BootstrapError>, Uncommitted> {
    let unchanged = Uncommitted::Restored;
    let keys = &owners.keys;
    let identity = &owners.identity;
    let keychain = &owners.keychain;
    let old_keys = keys.read().map_err(|error| unchanged(error.into()))?;
    let old_identity = identity.read().map_err(|error| unchanged(error.into()))?;
    let old_device = keychain
        .device_id()
        .map_err(|error| unchanged(error.into()))?;
    let location = RestoreStorage::decode(code.storage.as_bytes())
        .map_err(|error| unchanged(SyncError::from(error).into()))?
        .location;
    let settings = StorageSettings::new(directory.clone());
    let old_settings = settings
        .read()
        .map_err(|error| unchanged(SyncError::from(error).into()))?;
    let mut credential_rollback = None;
    let commit = || -> Result<(), BootstrapError> {
        settings.commit(&location).map_err(SyncError::from)?;
        keys.persist(ring)?;
        identity.persist(&MemberIdentity::new(code.member, code.member_keys.clone()))?;
        keychain.set_device_id(owners.device)?;
        credential_rollback = Some(match sign_in {
            SignInCustody::Session(session) => {
                session.publish(&owners.credentials, |keychain, tokens| {
                    coven_sync::commit_credentials(
                        keychain,
                        &StorageCredentials::OAuth(tokens.clone()),
                        Some((code, name)),
                    )
                })?
            }
            SignInCustody::Device { .. } => owners.credentials.commit_code(code, name)?,
            SignInCustody::None => owners.credentials.write(|keychain| {
                coven_sync::commit_credentials(keychain, credentials, Some((code, name)))
            })?,
        });
        Ok(publish()?)
    };
    let mut error = match commit() {
        Ok(()) => return Ok(None),
        Err(
            error @ BootstrapError::Directory(BootstrapDirectoryError::Create(
                coven_foundation::files::DirectoryCreationError::Published { .. },
            )),
        ) => return Ok(Some(error)),
        Err(error) => error,
    };
    // Attempt every rollback even if an earlier one fails. A failure is
    // returned to this initiator; no background repair is installed.
    let mut restored = true;
    let mut failed = |error: BootstrapError, cleanup: BootstrapError| {
        restored = false;
        combine::<()>(Err(error), Err(cleanup)).expect_err("failed commit")
    };
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
            error = failed(error, cleanup.into());
        }
    }
    if let Some(previous) = credential_rollback {
        if let Err(cleanup) = owners
            .credentials
            .write(|keychain| previous.restore(keychain))
        {
            error = failed(error, cleanup.into());
        }
        if let SignInCustody::Session(session) = sign_in {
            session.unpublish();
        }
    }
    let rollback = match old_settings {
        Some(location) => settings.commit(&location),
        None => settings.remove(),
    };
    if let Err(cleanup) = rollback {
        error = failed(error, SyncError::from(cleanup).into());
    }
    Err(if restored {
        Uncommitted::Restored(error)
    } else {
        Uncommitted::Partly(error)
    })
}

/// Commit custody and publish a new device's loaded store, returning its
/// handle. Publication removes the store's marker, the final step.
pub(super) async fn publish_bootstrap(
    pending: &BootstrapStore,
    code: RestoreCode,
    name: coven_sync::SavedName,
    credentials: StorageCredentials,
    sign_in: SignInCustody,
    ring: StoreKeyring,
    owners: OpeningOwners,
    database: Database,
    waits: Arc<coven_sync::RetryWaits>,
) -> Result<CovenHandle, BootstrapError> {
    let directory = pending.directory();
    let publication_error = commit(
        &directory,
        &code,
        &name,
        &credentials,
        &sign_in,
        &ring,
        &owners,
        || pending.publish(),
    )
    .map_err(Uncommitted::into_error)?;
    let handle = owners.into_handle(database, waits, true);
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

/// The loaded store's name and the slot that set it, for the saved code.
pub(super) async fn loaded_name(
    database: &Database,
) -> Result<coven_sync::SavedName, BootstrapError> {
    let (name, named_at) = database.named_store().await?;
    Ok(coven_sync::SavedName { name, named_at })
}

#[cfg(test)]
#[path = "bootstrap_commit_tests.rs"]
mod tests;
