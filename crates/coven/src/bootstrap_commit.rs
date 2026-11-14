//! Publish settings, custody and the loaded directory as the final bootstrap step.

use super::*;
use coven_foundation::files::{FileError, StoreDirAccess};
use coven_foundation::task::caught_sync;

/// The step that publishes what `commit` committed, its last.
pub(super) trait Publish {
    /// Publish the store.
    fn publish(&self) -> Result<(), BootstrapDirectoryError>;
    /// Whether the store is published, which `commit` reads after
    /// `publish` panicked to learn how far it got.
    fn published(&self) -> Result<bool, FileError>;
}

impl Publish for BootstrapStore {
    fn publish(&self) -> Result<(), BootstrapDirectoryError> {
        BootstrapStore::publish(self)
    }

    fn published(&self) -> Result<bool, FileError> {
        self.is_published()
    }
}

/// A replacement's commit: its store publishes after the commit
/// (`bootstrap_replace.rs`), so the commit publishes nothing, and the store
/// stays unpublished meanwhile.
pub(super) struct PublishedLater;

impl Publish for PublishedLater {
    fn publish(&self) -> Result<(), BootstrapDirectoryError> {
        Ok(())
    }

    fn published(&self) -> Result<bool, FileError> {
        Ok(false)
    }
}

/// Commit the installation's storage settings (`location`, which loading's
/// connection labelled, §20.2), store keys, member identity,
/// device id and credentials, with the restore code saved beside the loaded
/// store's `name` (§13.1), then `publish` it. A restore's OAuth store commits
/// its session sign-in's current tokens, which then refresh in device
/// custody. A join's sign-in, or a recorded restore's, is already the
/// device's, refreshes included, and stays as it is: only the code commits
/// beside it (§13.2).
///
/// The writes go to the keychain and to files, so no transaction holds them
/// together. The commit runs past its call's point of no return and only goes
/// forward: each write sets a value the loaded installation fixes, so a
/// commit that fails, panics (as `BootstrapError::Bug`, E15) or is cut keeps
/// what it wrote, and the call that resumes the installation loads it again
/// and writes everything again (§13.4, §11.6). A panic in `publish` leaves
/// unknown whether the store was published, so its marker says.
/// `Ok(Some(_))` is a failure after publication.
pub(super) fn commit(
    directory: &StoreDir,
    code: &RestoreCode,
    location: &StorageConfig,
    name: &coven_sync::SavedName,
    credentials: &StorageCredentials,
    sign_in: &SignInCustody,
    ring: &StoreKeyring,
    owners: &OpeningOwners,
    publish: &impl Publish,
) -> Result<Option<BootstrapError>, BootstrapError> {
    let keys = &owners.keys;
    let identity = &owners.identity;
    let keychain = &owners.keychain;
    let commit = || -> Result<(), BootstrapError> {
        coven_storage::record_config(directory, location).map_err(SyncError::from)?;
        keys.persist(ring)?;
        identity.persist(&MemberIdentity::new(code.member, code.member_keys.clone()))?;
        keychain.set_device_id(owners.device)?;
        #[cfg(test)]
        if owners.interrupt_commit {
            // As if the process stopped here.
            return Err(BootstrapError::Cancelled);
        }
        match sign_in {
            SignInCustody::Session(session) => {
                session.publish(&owners.credentials, |keychain, tokens| {
                    coven_sync::commit_credentials(
                        keychain,
                        &StorageCredentials::OAuth(tokens.clone()),
                        Some((code, name)),
                    )
                    .map(drop)
                })?
            }
            SignInCustody::Device { .. } => owners.credentials.commit_code(code, name).map(drop)?,
            SignInCustody::None => owners.credentials.write(|keychain| {
                coven_sync::commit_credentials(keychain, credentials, Some((code, name))).map(drop)
            })?,
        }
        Ok(())
    };
    caught_sync("commit the loaded store's custody", commit)
        .unwrap_or_else(|bug| Err(BootstrapError::Bug(bug)))?;
    match caught_sync("publish the loaded store", || publish.publish()) {
        Ok(Ok(())) => Ok(None),
        Ok(Err(error)) if error.published_unsynced() => Ok(Some(BootstrapError::Directory(error))),
        Ok(Err(error)) => Err(error.into()),
        Err(bug) => match publish.published() {
            Ok(true) => Ok(Some(BootstrapError::Bug(bug))),
            Ok(false) => Err(BootstrapError::Bug(bug)),
            // How far publication got is unknown: listing the stores shows
            // whether it was published.
            Err(read) => combine(
                Err(BootstrapError::Bug(bug)),
                Err(BootstrapDirectoryError::from(read).into()),
            ),
        },
    }
}

/// Commit custody and publish a new device's loaded store, returning its
/// handle. Publication removes the store's marker, the final step.
pub(super) async fn publish_bootstrap(
    pending: &BootstrapStore,
    code: RestoreCode,
    location: StorageConfig,
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
        &location,
        &name,
        &credentials,
        &sign_in,
        &ring,
        &owners,
        pending,
    )?;
    let handle = owners.into_handle(database, waits, true, false);
    let cleanup = directory
        .owned_file(StoreFile::Bootstrap)
        .remove_with_temporaries()
        .map_err(|error| BootstrapError::from(SyncError::from(error)));
    let result = match publication_error {
        Some(error) => combine(Err(error), cleanup),
        None => cleanup,
    };
    match result {
        Ok(()) => Ok(handle),
        Err(source) => Err(BootstrapError::Published { handle, source: Box::new(source) }),
    }
}

/// The name of the store a join publishes before it loads: the name its
/// invite carries, current at the slot the inviting device had applied when
/// it made the invite, which the store keeps until its history creates it.
/// The app lists the store by it meanwhile, and the saved code keeps it: a
/// rename the store applies later is newer, and one before that slot older
/// (§13.1, §13.2).
pub(super) async fn joined_name(
    database: &Database,
    code: &InviteCode,
) -> Result<coven_sync::SavedName, BootstrapError> {
    let named_at = coven_merge::Slot::new(code.read_through)
        .ok_or(BootstrapError::Code(CodeError::Invalid))?;
    database
        .initialize_joined_store(code.name.clone(), named_at)
        .await
        .map_err(CovenError::from)?;
    loaded_name(database).await
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
