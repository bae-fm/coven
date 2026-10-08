//! New-device composition roots. Sync owns admission and loading; foundation
//! owns unpublished directories and crypto owns final key custody (§12, E10).

use super::*;
use coven_crypto::custody::{InMemoryCustody, Keychain, StoreKeychain};
use coven_database::Database;
use coven_format::codes::{InviteCode, RestoreCode};
use coven_foundation::files::{BootstrapDirectoryError, BootstrapStore, StoreFile};
use coven_storage::{
    ConnectionCredentials, InviteStorage, RestoreStorage, StorageCredentials, StorageFailure,
    StorageSettings,
};
use coven_sync::{JoiningIdentity, StoreLogSync};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

#[path = "bootstrap_authentication.rs"]
mod authentication;
use authentication::{account_credentials, refresh_sign_in};

#[path = "bootstrap_commit.rs"]
mod commit;

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
    /// The singular keychain API cannot choose among several stores.
    #[error("keychain contains multiple stores: {0:?}")]
    MultipleStores(Vec<StoreId>),
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

/// Restore this person's store on a new device, returning its open handle.
/// The builder supplies all opening choices and holds this device's sign-in.
pub async fn restore_from_code(
    builder: CovenBuilder,
    code: &str,
    device_name: &str,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<CovenHandle, BootstrapError> {
    check_cancel(cancel)?;
    bootstrap_device(
        builder,
        BootstrapRequest::Restore {
            code: coven_sync::read_restore_code(code)?,
            name: device_name.into(),
        },
        on_status,
        cancel,
    )
    .await?
    .ok_or_else(|| SyncError::from(StorageFailure::MemberKeysMissing).into())
}

/// Restore from the one discoverable iCloud code. Multiple stores require the
/// app to choose an explicit code; this call never selects one arbitrarily.
pub async fn restore_from_keychain(
    builder: CovenBuilder,
    device_name: &str,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<CovenHandle>, BootstrapError> {
    check_cancel(cancel)?;
    let Some(code) = keychain_code(builder.keychain()?.as_ref())? else {
        return Ok(None);
    };
    bootstrap_device(
        builder,
        BootstrapRequest::Restore {
            code,
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
    on_status: impl Fn(&str),
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

fn keychain_code(keychain: &Keychain) -> Result<Option<RestoreCode>, BootstrapError> {
    let mut codes = keychain.synced_restore_codes()?;
    if codes.len() > 1 {
        return Err(BootstrapError::MultipleStores(
            codes.into_iter().map(|(id, _)| id).collect(),
        ));
    }
    let Some((id, bytes)) = codes.pop() else {
        return Ok(None);
    };
    let code = RestoreCode::from_bytes(bytes.as_bytes()).map_err(|_| CodeError::Invalid)?;
    if code.store != id {
        return Err(CodeError::Invalid.into());
    }
    RestoreStorage::decode(code.storage.as_bytes()).map_err(|_| CodeError::Invalid)?;
    Ok(Some(code))
}

enum BootstrapRequest {
    Restore { code: RestoreCode, name: String },
    Join { code: InviteCode, name: String },
}

async fn bootstrap_device(
    mut builder: CovenBuilder,
    request: BootstrapRequest,
    status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<CovenHandle>, BootstrapError> {
    check_cancel(cancel)?;
    let keychain = builder.keychain()?;
    let (id, name) = match &request {
        BootstrapRequest::Restore { code, .. } => (code.store, &code.name),
        BootstrapRequest::Join { code, .. } => (code.store, &code.name),
    };
    status("Preparing this device");
    let pending = builder
        .layout
        .begin_bootstrap(id, name, builder.ids.as_ref())?;
    let result = prepare_and_load(&mut builder, &pending, &request, &status, cancel).await;
    match result {
        Ok(Some((code, credentials, ring, database, storage))) => {
            status("Keeping keys and credentials");
            let result = check_cancel(cancel).and_then(|()| {
                builder.authentication.take();
                let mut owners = builder.owners(
                    pending.directory(),
                    Arc::new(StoreKeychain::new(keychain, id)),
                )?;
                storage.reset_reachability();
                owners.storage = Some(storage);
                // No await separates final custody, publication and the handle.
                commit::publish_bootstrap(
                    &pending,
                    code,
                    credentials,
                    ring,
                    owners,
                    database.clone(),
                )
            });
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
    Joining {
        identity: JoiningIdentity,
        invitation: coven_storage::StorageInvitation,
        code: &'a InviteCode,
    },
}

async fn prepare_and_load(
    builder: &mut CovenBuilder,
    pending: &BootstrapStore,
    request: &BootstrapRequest,
    status: &impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<
    Option<(
        RestoreCode,
        StorageCredentials,
        StoreKeyring,
        Database,
        Arc<coven_storage::StorageConnection>,
    )>,
    BootstrapError,
> {
    let directory = pending.directory();
    let file = directory.owned_file(StoreFile::Bootstrap);
    let (member, mut data, device_name, mut admission) = match request {
        BootstrapRequest::Restore { code, name } => {
            let data =
                match RestoreStorage::decode(code.storage.as_bytes()).map_err(SyncError::from)? {
                    RestoreStorage::S3 {
                        location,
                        credentials,
                    } => ConnectionCredentials {
                        location,
                        credentials: StorageCredentials::S3(credentials),
                    },
                    RestoreStorage::Account(location) => {
                        let credentials =
                            account_credentials(builder, location.provider(), cancel).await?;
                        ConnectionCredentials {
                            location,
                            credentials,
                        }
                    }
                };
            (
                code.member_keys.clone(),
                data,
                name.as_str(),
                Admission::Restoring,
            )
        }
        BootstrapRequest::Join { code, name } => {
            let identity = JoiningIdentity::prepare(&file, code, name)?;
            let (invitation, credentials) =
                match InviteStorage::decode(code.storage.as_bytes()).map_err(SyncError::from)? {
                    InviteStorage::S3 {
                        invitation,
                        credentials,
                    } => (invitation, StorageCredentials::S3(credentials)),
                    InviteStorage::Account(invitation) => {
                        let provider = invitation.location().provider();
                        let credentials = account_credentials(builder, provider, cancel).await?;
                        (invitation, credentials)
                    }
                };
            (
                identity.member_keys(),
                ConnectionCredentials {
                    location: invitation.location().clone(),
                    credentials,
                },
                name.as_str(),
                Admission::Joining {
                    identity,
                    invitation,
                    code,
                },
            )
        }
    };
    check_cancel(cancel)?;
    let settings = directory.settings().map_err(CovenError::from)?;
    let storage = builder
        .connector()
        .connect(
            data.location.clone(),
            data.credentials.clone(),
            settings.device_id,
        )
        .await
        .map_err(SyncError::from)?;
    let ring: Arc<dyn StoreKeyCustody> = Arc::new(InMemoryCustody::<StoreKeyring>::empty());
    let identity: Arc<dyn MemberKeyCustody> = Arc::new(InMemoryCustody::new(member.clone()));
    let database = builder.database(directory.clone())?.open().await?;
    let mut sync = StoreLogSync::new(
        storage.clone(),
        database.clone(),
        ring.clone(),
        identity,
        builder.clock.clone(),
        builder.ids.clone(),
        directory,
    );
    let loading = async {
        if let Admission::Joining {
            identity: joining,
            invitation,
            code,
        } = &mut admission
        {
            status("Accepting provider access");
            sync.accept_join(invitation).await?;
            if !joining.attempted() {
                status("Sending the join request");
                joining.record_attempt(&file, code)?;
                sync.submit_join(joining).await?;
            }
            status("Waiting for approval");
            loop {
                refresh_sign_in(builder, &mut data, storage.as_ref(), cancel).await?;
                match sync.join_outcome(joining).await? {
                    coven_sync::JoinOutcome::Admitted => break,
                    coven_sync::JoinOutcome::Declined => return Ok(false),
                    coven_sync::JoinOutcome::Waiting => {}
                }
                builder.clock.sleep(Duration::from_secs(1)).await;
            }
        } else {
            status("Opening member keys and the store log");
            if !sync.bootstrap_member().await? {
                return Err(SyncError::NotStoreMember(member.member_id()).into());
            }
        }
        status("Loading snapshots and later writes");
        refresh_sign_in(builder, &mut data, storage.as_ref(), cancel).await?;
        sync.load_new_device(device_name).await?;
        Ok::<_, BootstrapError>(true)
    };
    let mut cancellation = cancel.clone();
    let result = tokio::select! {
        biased;
        _ = cancelled(&mut cancellation) => Err(BootstrapError::Cancelled),
        result = loading => result,
    };
    drop(sync);
    let result = result.and_then(|loaded| {
        if !loaded {
            return Ok(None);
        }
        let ring = ring
            .unlock()?
            .ok_or(SyncError::from(StorageFailure::MemberKeysMissing))?;
        Ok(Some((
            RestoreCode {
                store: settings.id,
                name: settings.name,
                member_keys: member,
                storage: RestoreStorage::from_connection(&data)
                    .encode()
                    .map_err(SyncError::from)?,
            },
            data.credentials,
            ring,
        )))
    });
    match result {
        Ok(Some((code, credentials, ring))) => {
            Ok(Some((code, credentials, ring, database, storage)))
        }
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
