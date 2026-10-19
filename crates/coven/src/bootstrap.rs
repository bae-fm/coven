//! New-device composition roots. Sync owns admission and loading; foundation
//! owns unpublished directories and crypto owns final key custody (§13, E10).

use super::*;
#[cfg(test)]
use coven_crypto::custody::Keychain;
use coven_crypto::custody::{InMemoryCustody, StoreKeychain, SyncedKeychain};
use coven_database::Database;
use coven_format::codes::{InviteCode, RestoreCode};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::files::{BootstrapDirectoryError, BootstrapStore};
use coven_storage::{
    ConnectionCredentials, InviteStorage, StorageCredentials, StorageFailure, StorageSettings,
};
use coven_sync::SequenceSync;
use std::sync::Arc;
use tokio::sync::watch;

#[path = "bootstrap_authentication.rs"]
mod authentication;
use authentication::{account_sign_in, provider_auth};
use coven_sync::SessionSignIn;

#[path = "bootstrap_commit.rs"]
mod commit;

#[path = "bootstrap_join.rs"]
mod join;

#[path = "bootstrap_reset.rs"]
mod reset;
pub(super) use reset::reset_device;

#[path = "restore_credentials.rs"]
mod credentials;
use credentials::restore_connection;

/// Opening a new installation failed; unpublished work remains available for
/// an explicit retry unless the call was cancelled or the invite was settled.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    /// Cancellation removes unpublished local data and leaves final custody alone.
    #[error("opening cancelled")]
    Cancelled,
    /// The code cannot be used for this operation.
    #[error(transparent)]
    Code(#[from] CodeError),
    /// Preparing, publishing or removing the unpublished directory failed.
    #[error(transparent)]
    Directory(#[from] BootstrapDirectoryError),
    /// Opening the database failed.
    #[error(transparent)]
    Store(#[from] CovenError),
    /// Provider admission, signed membership or snapshot loading failed.
    #[error(transparent)]
    Sync(#[from] SyncError),
    /// Provider sign-in failed.
    #[error(transparent)]
    OAuth(#[from] coven_storage::providers::OAuthError),
    /// Final custody could not be read or written.
    #[error(transparent)]
    SecureStorage(#[from] KeyError),
    /// Cleanup failed too; retry can inspect both failures.
    #[error("{operation}; cleanup failed: {cleanup}")]
    Cleanup {
        /// The initial failure.
        operation: Box<BootstrapError>,
        /// Its cleanup failure.
        #[source]
        cleanup: Box<BootstrapError>,
    },
    /// The loaded store is published; confirming durability or final cleanup
    /// failed. Custody remains committed and the handle retains session-only keys.
    #[error("store is published; final durability or cleanup failed: {source}")]
    Published {
        /// The open store, including its session-only custody.
        handle: CovenHandle,
        /// Failure after publication.
        #[source]
        source: Box<BootstrapError>,
    },
}

/// Bootstrap progress (E10), before an open handle exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapStatus {
    /// Preparing this installation, accepting provider access and sending a
    /// join request.
    Connecting,
    /// The join request waits for an admin's approval.
    WaitingForApproval,
    /// Loading the floor's snapshots and the slots after them, then keeping
    /// keys and credentials.
    Loading,
    /// The join waits on these first unmet conditions, the records
    /// `pending()` returns once the store is open (§19.1).
    Pending(Vec<coven_sync::PendingRecord>),
}

/// Restore this person's store on a new device, returning its open handle.
/// S3 requires a key entered separately or transferred beside the code. Other
/// providers require None and use the builder's own sign-in. Invalid key inputs
/// fail before bootstrap begins.
pub async fn restore_from_code(
    builder: CovenBuilder,
    code: &str,
    s3_credentials: Option<S3Credentials>,
    device_name: &str,
    on_status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<CovenHandle, BootstrapError> {
    check_cancel(cancel)?;
    let code = coven_sync::read_restore_code(code)?;
    let location = StorageConfig::decode(code.storage.as_bytes()).map_err(SyncError::from)?;
    let (data, sign_in) = restore_connection(&builder, location, s3_credentials)?;
    bootstrap_device(
        builder,
        BootstrapRequest::Restore {
            code,
            data,
            sign_in,
            name: device_name.into(),
        },
        on_status,
        cancel,
    )
    .await?
    .ok_or_else(|| SyncError::from(StorageFailure::MemberKeysMissing).into())
}

/// Restore the entry selected from `saved_restore_codes`. Reads the current
/// code by store id, including a replacement saved since listing. Returns None
/// if that store's code is no longer saved.
/// S3 uses the explicit key when supplied, otherwise its separately synced key.
/// Missing keys, wrong-provider keys and keychain failures precede bootstrap.
pub async fn restore_from_keychain(
    builder: CovenBuilder,
    saved: &SavedRestoreCode,
    s3_credentials: Option<S3Credentials>,
    device_name: &str,
    on_status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<CovenHandle>, BootstrapError> {
    check_cancel(cancel)?;
    let synced = SyncedKeychain::new(builder.keychain()?, saved.store_id);
    let Some((code, location)) = keychain_code(&synced, saved.store_id)? else {
        return Ok(None);
    };
    let s3_credentials = if location.provider() == CloudProvider::S3 && s3_credentials.is_none() {
        synced
            .s3_credentials()?
            .map(|bytes| S3Credentials::decode(bytes.as_bytes()).map_err(SyncError::from))
            .transpose()?
    } else {
        s3_credentials
    };
    let (data, sign_in) = restore_connection(&builder, location, s3_credentials)?;
    bootstrap_device(
        builder,
        BootstrapRequest::Restore {
            code,
            data,
            sign_in,
            name: device_name.into(),
        },
        on_status,
        cancel,
    )
    .await
}

/// Accept provider access, retain one signed request across restarts, and wait
/// for approval. Returns the open store, or None when declined or expired.
/// Cancellation removes the unpublished installation; dropping the future
/// preserves it for a subsequent call with the same invite and device name.
pub async fn join_with_invite(
    builder: CovenBuilder,
    code: &str,
    device_name: &str,
    on_status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<CovenHandle>, BootstrapError> {
    check_cancel(cancel)?;
    bootstrap_device(
        builder,
        BootstrapRequest::Join {
            code: coven_sync::read_invite_code(code)?,
            name: device_name.into(),
        },
        on_status,
        cancel,
    )
    .await
}

fn keychain_code(
    keychain: &SyncedKeychain,
    store: StoreId,
) -> Result<Option<(RestoreCode, StorageConfig)>, BootstrapError> {
    keychain
        .restore_code()?
        .map(|bytes| crate::saved_restore::decode(store, bytes.as_bytes()).map_err(Into::into))
        .transpose()
}

enum BootstrapRequest {
    Restore {
        code: RestoreCode,
        data: ConnectionCredentials,
        /// The session custody an OAuth connection refreshes in.
        sign_in: Option<Arc<SessionSignIn>>,
        name: String,
    },
    Join {
        code: InviteCode,
        name: String,
    },
}

async fn bootstrap_device(
    mut builder: CovenBuilder,
    request: BootstrapRequest,
    status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<CovenHandle>, BootstrapError> {
    check_cancel(cancel)?;
    let keychain = builder.keychain()?;
    let id = match &request {
        BootstrapRequest::Restore { code, .. } => code.store,
        BootstrapRequest::Join { code, .. } => code.store,
    };
    status(BootstrapStatus::Connecting);
    let pending = builder
        .layout
        .begin_bootstrap(id, builder.ids.as_ref())
        .await?;
    let result = prepare_and_load(&mut builder, &pending, &request, &status, cancel).await;
    match result {
        Ok(Some(Loaded {
            code,
            credentials,
            sign_in,
            ring,
            database,
            storage,
            state,
            publications,
            waits,
        })) => {
            status(BootstrapStatus::Loading);
            let result = match check_cancel(cancel).and_then(|()| {
                builder.authentication.take();
                let mut owners = builder.owners(
                    pending.directory(),
                    Arc::new(StoreKeychain::new(keychain, id)),
                )?;
                owners.state = state;
                owners.publications = publications;
                owners.storage = Some(storage);
                Ok(owners)
            }) {
                // No cancellable await separates final custody, publication
                // and the handle.
                Ok(owners) => {
                    commit::publish_bootstrap(
                        &pending,
                        code,
                        credentials,
                        sign_in,
                        ring,
                        owners,
                        database.clone(),
                        waits,
                    )
                    .await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(handle) => Ok(Some(handle)),
                Err(error @ BootstrapError::Published { .. }) => Err(error),
                Err(error) => {
                    let result = combine(
                        Err(error),
                        database
                            .close()
                            .await
                            .map_err(CovenError::from)
                            .map_err(BootstrapError::from),
                    );
                    if matches!(result, Err(BootstrapError::Cancelled)) {
                        cleanup(&pending, result)
                    } else {
                        result
                    }
                }
            }
        }
        Ok(None) => cleanup(&pending, Ok(None)),
        Err(BootstrapError::Cancelled) => cleanup(&pending, Err(BootstrapError::Cancelled)),
        Err(error) => Err(error),
    }
}

enum Admission<'a> {
    Restoring,
    Joining(join::Joining<'a>),
}

/// A loaded store awaiting publication, with what reaches its storage.
struct Loaded {
    code: RestoreCode,
    credentials: StorageCredentials,
    sign_in: Option<Arc<SessionSignIn>>,
    ring: StoreKeyring,
    database: Database,
    storage: Arc<coven_storage::StorageConnection>,
    /// The sync state the handle continues, and the connection's gate around it.
    state: Arc<coven_sync::SyncState>,
    publications: Arc<crate::storage::Publications>,
    waits: Arc<coven_sync::RetryWaits>,
}

async fn prepare_and_load(
    builder: &mut CovenBuilder,
    pending: &BootstrapStore,
    request: &BootstrapRequest,
    status: &impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<Loaded>, BootstrapError> {
    let directory = pending.directory();
    let (member, mut data, sign_in, device_name, mut admission) = match request {
        BootstrapRequest::Restore {
            code,
            data,
            sign_in,
            name,
        } => (
            MemberIdentity::new(code.member, code.member_keys.clone()),
            data.clone(),
            sign_in.clone(),
            name.as_str(),
            Admission::Restoring,
        ),
        BootstrapRequest::Join { code, name } => {
            let (joining, data, sign_in) = join::prepare_request(builder, &directory, code, name)?;
            (
                joining.identity(),
                data,
                sign_in,
                name.as_str(),
                Admission::Joining(joining),
            )
        }
    };
    check_cancel(cancel)?;
    let settings = directory.settings().map_err(CovenError::from)?;
    let state = Arc::new(coven_sync::SyncState::new());
    let publications = Arc::new(crate::storage::Publications::new(state.clone()));
    let ring = Arc::new(KeySession::store(Arc::new(
        InMemoryCustody::<StoreKeyring>::empty(),
    ))?);
    let identity = Arc::new(KeySession::member(Arc::new(InMemoryCustody::new(
        member.clone(),
    )))?);
    // Completing a reset registers the fresh id as the old one's replacement.
    let reset = pending.device_reset().map_err(SyncError::from)?;
    // The database opens before connecting: its persisted waits and provider
    // cooldown hold the joiner's polls and every request across restarts (§6.7).
    let database = builder.database(directory.clone())?.open().await?;
    let connected = async {
        let waits = Arc::new(
            coven_sync::RetryWaits::open(database.clone(), builder.clock.clone())
                .await
                .map_err(CovenError::from)?,
        );
        data.location = crate::authentication::label_location(
            builder.connector().as_ref(),
            data.location.clone(),
            provider_auth(&data, sign_in.as_ref())?,
            waits.clone(),
        )
        .await
        .map_err(SyncError::from)?;
        let storage = builder
            .connector()
            .connect(
                data.location.clone(),
                provider_auth(&data, sign_in.as_ref())?,
                publications.clone(),
                waits.clone(),
                builder.transfers.clone(),
            )
            .await
            .map_err(SyncError::from)?;
        if matches!(admission, Admission::Restoring) {
            storage
                .check_bucket(settings.id)
                .await
                .map_err(SyncError::from)?;
        }
        // The loading owner's transfers end with loading; the handle's start
        // with the handle.
        let files = coven_sync::Files::new(
            coven_database::FileDatabase::new(database.clone()),
            directory.clone(),
            None,
            state.clone(),
            waits.clone(),
            builder.ids.clone(),
            builder.transfers.clone(),
        );
        let sync = SequenceSync::open(
            database.clone(),
            storage.clone(),
            directory.clone(),
            settings.device_id,
            ring.clone(),
            identity,
            Arc::new(SyncedKeychain::new(builder.keychain()?, settings.id)),
            state.clone(),
            builder.clock.clone(),
            builder.ids.clone(),
            waits.clone(),
            files.clone(),
        )
        .await;
        match sync {
            Ok(sync) => Ok::<_, BootstrapError>((waits, storage, files, Arc::new(sync))),
            Err(error) => {
                files.close().await;
                Err(error.into())
            }
        }
    }
    .await;
    let (waits, storage, files, sync) = match connected {
        Ok(connected) => connected,
        Err(error) => {
            return combine(
                Err(error),
                database
                    .close()
                    .await
                    .map_err(CovenError::from)
                    .map_err(BootstrapError::from),
            )
        }
    };
    let loading = async {
        if let Admission::Joining(joining) = &mut admission {
            if !joining.admit(&sync, &database, status).await? {
                return Ok(false);
            }
        }
        status(BootstrapStatus::Loading);
        // Reading forward opens this member's copies and loads the floor's
        // snapshots and the slots after them; an interrupted bootstrap's
        // registration is already applied.
        sync.unlock().await?;
        let registered = database
            .store_state()
            .await?
            .devices
            .get(&settings.device_id)
            .is_some_and(|device| device.member == member.member());
        if !registered {
            sync.register_device(device_name.into(), reset.map(|reset| reset.old_device))
                .await?;
        }
        // The notice commits before any handle is published (E5).
        if let Some(reset) = reset {
            reset::carry_discarded_provider_work(&sync, pending, reset).await?;
        }
        Ok::<_, BootstrapError>(true)
    };
    let mut cancellation = cancel.clone();
    let result = tokio::select! {
        biased;
        _ = cancelled(&mut cancellation) => Err(BootstrapError::Cancelled),
        result = loading => result,
    };
    files.close().await;
    drop(sync);
    let result = result.and_then(|loaded| {
        if !loaded {
            return Ok(None);
        }
        let ring = ring
            .read()?
            .ok_or(SyncError::from(StorageFailure::MemberKeysMissing))?;
        Ok(Some((
            RestoreCode {
                store: settings.id,
                member: member.member(),
                member_keys: member.keys().clone(),
                storage: data.location.encode().map_err(SyncError::from)?,
            },
            data.credentials,
            ring,
        )))
    });
    match result {
        Ok(Some((code, credentials, ring))) => Ok(Some(Loaded {
            code,
            credentials,
            sign_in,
            ring,
            database,
            storage,
            state,
            publications,
            waits,
        })),
        result => {
            let closed = database
                .close()
                .await
                .map_err(CovenError::from)
                .map_err(BootstrapError::from);
            combine(result.map(|_| None), closed)
        }
    }
}

fn check_cancel(cancel: &watch::Receiver<bool>) -> Result<(), BootstrapError> {
    if *cancel.borrow() {
        Err(BootstrapError::Cancelled)
    } else {
        Ok(())
    }
}

async fn cancelled(cancel: &mut watch::Receiver<bool>) {
    loop {
        if *cancel.borrow_and_update() {
            return;
        }
        if cancel.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

fn cleanup<T>(
    pending: &BootstrapStore,
    result: Result<T, BootstrapError>,
) -> Result<T, BootstrapError> {
    combine(result, pending.cancel().map_err(BootstrapError::from))
}

fn combine<T>(
    result: Result<T, BootstrapError>,
    cleanup: Result<(), BootstrapError>,
) -> Result<T, BootstrapError> {
    match (result, cleanup) {
        (result, Ok(())) => result,
        (Ok(_), Err(error)) => Err(error),
        (Err(operation), Err(cleanup)) => Err(BootstrapError::Cleanup {
            operation: Box::new(operation),
            cleanup: Box::new(cleanup),
        }),
    }
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
