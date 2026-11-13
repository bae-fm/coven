//! Create identity custody while the store remains unpublished.

use super::*;
use coven_foundation::files::{BootstrapDirectoryError, BootstrapStore};
use coven_foundation::task::caught_sync;

impl CovenBuilder {
    /// Create and open a store, initializing its member, first store key and
    /// slot 1 before publication; `device_name` names this installation.
    /// Slot 1 also makes a circle for each of `circles`, named in order, with
    /// the creator as its first member and its key in custody. Then
    /// `creating`, the app's first write, runs on a [`MemberWrite`] with the
    /// circles' ids in that order, after slot 1 has registered the creator
    /// and this device and made the circles, so its rows can name and go into
    /// them (§10.3). The store is published only once that write has
    /// committed: a failed one leaves no store and nothing in custody.
    /// Cancellation does not interrupt creation or rollback. Errors carry no
    /// handle; a published store is closed before returning.
    pub async fn create_store<F>(
        self,
        name: &str,
        device_name: &str,
        circles: &[&str],
        creating: F,
    ) -> Result<CovenHandle, StoreCreationError>
    where
        F: FnOnce(&MemberWrite<'_>, &[CircleId]) -> CovenResult<()> + Send + 'static,
    {
        let (name, device_name) = (name.to_owned(), device_name.to_owned());
        let circles = circles.iter().map(|circle| (*circle).to_owned()).collect();
        let bugs = self.directories.bugs().clone();
        let publish = coven_foundation::files::BootstrapStore::publish;
        let creating = create(self, name, device_name, circles, creating, publish);
        crate::task::spawn_completing("create the store", bugs, creating).await?
    }
}

/// The app's first write, which runs once slot 1 is applied and before the
/// store is published (§10.3).
pub(super) trait Creating:
    FnOnce(&MemberWrite<'_>, &[CircleId]) -> CovenResult<()> + Send + 'static
{
}
impl<F> Creating for F where
    F: FnOnce(&MemberWrite<'_>, &[CircleId]) -> CovenResult<()> + Send + 'static
{
}

pub(super) async fn create(
    builder: CovenBuilder,
    name: String,
    device_name: String,
    circles: Vec<String>,
    creating: impl Creating,
    publish: impl FnOnce(&BootstrapStore) -> Result<(), BootstrapDirectoryError> + Send + 'static,
) -> Result<CovenHandle, StoreCreationError> {
    // A name the format refuses is refused before anything is made.
    for circle in &circles {
        coven_format::value::name(circle).map_err(SyncError::from)?;
    }
    let id = StoreId(builder.ids.new_id());
    let pending = Arc::new(builder.directories.begin_bootstrap(id, builder.ids.as_ref()).await?);
    let bugs = builder.directories.bugs().clone();
    match prepare(builder, pending.clone(), name, device_name, circles, creating, publish).await {
        Ok(handle) => Ok(handle),
        Err(error @ StoreCreationError::PublishedButNotSynced { .. }) => Err(error),
        Err(error) => {
            Err(
                match crate::task::blocking_completing(
                    "remove the unpublished store",
                    bugs,
                    move || pending.cancel(),
                )
                .await
                {
                    Ok(Ok(())) => error,
                    Ok(Err(cleanup)) => error.with_cleanup(cleanup.into()),
                    // The removal panicked or never ran, so the unpublished
                    // store may stay on disk.
                    Err(failed) => error.with_cleanup(failed.into()),
                },
            )
        }
    }
}

async fn prepare(
    builder: CovenBuilder,
    pending: Arc<BootstrapStore>,
    name: String,
    device_name: String,
    circles: Vec<String>,
    creating: impl Creating,
    publish: impl FnOnce(&BootstrapStore) -> Result<(), BootstrapDirectoryError> + Send + 'static,
) -> Result<CovenHandle, StoreCreationError> {
    let directory = pending.directory();
    let database = builder.database(directory.clone())?.open().await?;
    let waits = match coven_sync::RetryWaits::open(database.clone(), builder.clock.clone()).await {
        Ok(waits) => Arc::new(waits),
        Err(error) => return Err(close_failed(&database, CovenError::from(error).into()).await),
    };
    let bugs = builder.directories.bugs().clone();
    let custody = crate::task::blocking_completing("create the store's custody", bugs.clone(), {
        let directory = directory.clone();
        move || {
            let keychain = Arc::new(StoreKeychain::new(
                builder.keychain().map_err(CovenError::from)?,
                directory.id(),
            ));
            let owners = builder.owners(directory, keychain, crate::builder::Continuing::Fresh)?;
            // Each step catches its panic, so a panic undoes custody as any
            // failure before publication does (E15). Making the identity
            // writes nothing, and refuses one custody already holds, so a
            // failure there has nothing to undo; the undo takes back only
            // what this creation wrote.
            let member = MemberId(owners.ids.new_id());
            let identity =
                caught_sync("make the member identity", || owners.custody.new_identity(member))
                    .unwrap_or_else(|bug| Err(IdentityError::Bug(bug)))?;
            match keep_custody(&owners, &identity, circles) {
                Ok((ring, circles)) => Ok((owners, identity, ring, circles)),
                Err(error) => Err(undo_custody(&owners, error)),
            }
        }
    })
    .await
    .unwrap_or_else(|shutdown| Err(shutdown.into()));
    let (owners, identity, ring, circles) = match custody {
        Ok(kept) => kept,
        Err(error) => return Err(close_failed(&database, error).await),
    };
    // Slot 1 is decided and applied before the store is visible: the creator
    // is the store's first admin and this device its first device from the
    // moment it exists (§10.3).
    let creation = coven_sync::Creation {
        store: directory.id(),
        name,
        device: owners.device,
        device_name,
        member: &identity,
        circles: circles.clone(),
        ring: &ring,
    };
    if let Err(error) = coven_sync::decide_creation(&database, creation).await {
        let undone = undo_blocking(owners, bugs, StoreCreationError::from(error)).await;
        return Err(close_failed(&database, undone).await);
    }
    // The app's first write sees the creator, this device and the circles,
    // and commits before the store is visible: a failure leaves no store and
    // nothing in custody (§10.3).
    let made: Vec<CircleId> = circles.iter().map(|circle| circle.circle).collect();
    let written = database.write_as(move |sql| creating(&MemberWrite::new(sql), &made)).await;
    if let Err(error) = written {
        let undone = undo_blocking(owners, bugs, StoreCreationError::from(error)).await;
        return Err(close_failed(&database, undone).await);
    }
    let published = crate::task::blocking_completing("publish the store", bugs, move || {
        match published(&pending, publish) {
            Publication::Done => Ok((owners, None)),
            Publication::Failed(error) => Ok((owners, Some(error))),
            Publication::Unknown(error) => Err(error),
            Publication::Refused(error) => Err(undo_custody(&owners, error)),
        }
    })
    .await
    .unwrap_or_else(|shutdown| Err(shutdown.into()));
    let (owners, publication) = match published {
        Ok(owners) => owners,
        Err(error) => return Err(close_failed(&database, error).await),
    };
    if let Err(error) = crate::storage::record_opened(
        &database,
        &owners.clock,
        coven_database::RecordedLocation::None,
        None,
    )
    .await
    {
        return Err(close_failed(&database, error.into()).await);
    }
    let handle = owners.into_handle(database, waits, false, false);
    match publication {
        None => Ok(handle),
        Some(source) => {
            let store = handle.store_id();
            let source = match handle.close().await {
                Ok(()) => source,
                Err(cleanup) => source.with_cleanup(cleanup.into()),
            };
            Err(StoreCreationError::PublishedButNotSynced { store, source: Box::new(source) })
        }
    }
}

/// Save the new member's identity, this installation's device id, the first
/// store key and the first key of each circle named in `circles` in custody,
/// which the store's slot 1 introduces (§12.4).
fn keep_custody(
    owners: &crate::builder::OpeningOwners,
    identity: &MemberIdentity,
    circles: Vec<String>,
) -> Result<(StoreKeyring, Vec<coven_sync::NewCircle>), StoreCreationError> {
    caught_sync("save the member identity", || owners.identity.persist(identity))
        .unwrap_or_else(|bug| Err(KeyError::Bug(bug)))
        .map_err(|error| StoreCreationError::from(IdentityError::from(error)))?;
    caught_sync("record the device id", || owners.keychain.set_device_id(owners.device))
        .unwrap_or_else(|bug| Err(KeyError::Bug(bug)))
        .map_err(CovenError::from)?;
    let kept = caught_sync("keep the first keys", || {
        let key =
            coven_crypto::StoreKey::generate(KeyId(owners.ids.new_id())).map_err(KeyError::from)?;
        let mut ring = StoreKeyring::new(key);
        let mut made = Vec::with_capacity(circles.len());
        for name in circles {
            let (circle, key) = (CircleId(owners.ids.new_id()), KeyId(owners.ids.new_id()));
            ring.insert_circle_key(
                coven_crypto::CircleKey::generate(circle, key).map_err(KeyError::from)?,
            )
            .map_err(KeyError::from)?;
            made.push(coven_sync::NewCircle { circle, name, key });
        }
        owners.keys.persist(&ring)?;
        Ok::<_, KeyError>((ring, made))
    })
    .unwrap_or_else(|bug| Err(KeyError::Bug(bug)))
    .map_err(CovenError::from)?;
    Ok(kept)
}

/// Take back what creation wrote to custody, trying every step and
/// retaining every failure. The marker stays hidden.
fn undo_custody(
    owners: &crate::builder::OpeningOwners,
    mut error: StoreCreationError,
) -> StoreCreationError {
    for cleanup in
        [owners.identity.forget(), owners.keychain.delete_device_id(), owners.keys.forget()]
    {
        if let Err(cleanup) = cleanup {
            error = error.with_cleanup(CovenError::from(cleanup).into());
        }
    }
    error
}

/// [`undo_custody`] on the blocking pool.
async fn undo_blocking(
    owners: crate::builder::OpeningOwners,
    bugs: coven_foundation::task::BugSinkRef,
    error: StoreCreationError,
) -> StoreCreationError {
    match crate::task::blocking_completing("undo the store's custody", bugs, move || {
        Err::<(), _>(undo_custody(&owners, error))
    })
    .await
    {
        Ok(Err(error)) => error,
        Ok(Ok(())) => unreachable!("the undo answers the error it undid"),
        Err(failed) => StoreCreationError::from(failed),
    }
}

/// How far publishing the new store got.
enum Publication {
    /// The store is published.
    Done,
    /// The store is published, but the step failed after its marker went,
    /// as syncing the directory does: its custody stays.
    Failed(StoreCreationError),
    /// The store isn't published: its custody is undone.
    Refused(StoreCreationError),
    /// Publishing panicked, and the marker that says how far it got couldn't
    /// be read: custody stays, and listing the stores shows which (E15).
    Unknown(StoreCreationError),
}

/// Publish the store with `publish`. Its panic leaves it unknown whether
/// the marker went, so the marker says: creation keeps custody for a
/// published store and undoes it for one that isn't (E15).
fn published(
    pending: &BootstrapStore,
    publish: impl FnOnce(&BootstrapStore) -> Result<(), BootstrapDirectoryError>,
) -> Publication {
    let bug = match caught_sync("publish the store", || publish(pending)) {
        Ok(Ok(())) => return Publication::Done,
        Ok(Err(error)) if error.published_unsynced() => {
            return Publication::Failed(StoreCreationError::Directory(error))
        }
        Ok(Err(error)) => return Publication::Refused(error.into()),
        Err(bug) => StoreCreationError::Bug(bug),
    };
    match pending.is_published() {
        Ok(true) => Publication::Failed(bug),
        Ok(false) => Publication::Refused(bug),
        Err(read) => {
            Publication::Unknown(bug.with_cleanup(BootstrapDirectoryError::from(read).into()))
        }
    }
}

async fn close_failed(
    database: &coven_database::Database,
    error: StoreCreationError,
) -> StoreCreationError {
    match database.close().await {
        Ok(()) => error,
        Err(cleanup) => error.with_cleanup(SyncError::from(cleanup).into()),
    }
}

#[cfg(test)]
#[path = "builder_creation_tests.rs"]
mod tests;
