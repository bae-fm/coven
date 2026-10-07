//! New-device composition roots. Sync owns admission and loading; foundation
//! owns unpublished directories and crypto owns final key custody (§12, §20.10).

use crate::*;
use coven_crypto::custody::{InMemoryCustody, Keychain, StoreKeychain};
use coven_database::DatabaseBuilder;
use coven_format::codes::{InviteCode, RestoreCode};
use coven_foundation::files::{BootstrapDirectoryError, BootstrapStore, StoreFile};
use coven_storage::{
    InviteStorage, OAuthTokens, RestoreStorage, Storage, StorageCredentials, StorageSettings,
};
use coven_sync::{JoiningIdentity, StoreLogSync};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

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
    /// Returning a directory cannot retain a session-only custody value. Use
    /// Custom with an app-retained InMemoryCustody for an in-memory installation.
    #[error("bootstrap requires retained custody; use Custom for shared in-memory custody")]
    EphemeralCustody,
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
    /// The loaded store is published; its final cleanup failed. Custody remains
    /// committed and this directory can be opened.
    #[error("store {store:?} is published; final cleanup failed: {source}")]
    Published {
        /// The directory that can already be opened.
        store: StoreDir,
        /// Failure after publication.
        #[source]
        source: Box<BootstrapError>,
    },
}

/// Restore this person's store on a new device, registering a fresh device id.
/// OAuth providers use this device's supplied tokens or browser sign-in. No file
/// transfers run here: snapshots and device logs load through the snapshot owner.
pub async fn restore_from_code(
    code: &str,
    device_name: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    coven_migration_policy: CovenMigrationPolicy,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    oauth_tokens: Option<OAuthTokens>,
    layout: &StoreLayout,
    oauth_clients: Arc<OAuthClients>,
    cloudkit_ops: Option<Arc<dyn CloudKitOps>>,
    clock: ClockRef,
    ids: IdSourceRef,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<StoreDir, BootstrapError> {
    check_cancel(cancel)?;
    let request = BootstrapRequest::Restore {
        code: coven_sync::read_restore_code(code)?,
        name: device_name.into(),
    };
    bootstrap_device(
        request,
        synced_tables,
        migrations,
        coven_migration_policy,
        key_custody,
        identity_custody,
        oauth_tokens,
        layout,
        oauth_clients,
        cloudkit_ops,
        clock,
        ids,
        on_status,
        cancel,
        Keychain::registered()?,
        None,
    )
    .await?
    .ok_or_else(|| SyncError::MissingMemberKeys.into())
}

/// Restore from the one discoverable iCloud code. Multiple stores require the
/// app to choose an explicit code; this call never selects one arbitrarily.
pub async fn restore_from_keychain(
    device_name: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    coven_migration_policy: CovenMigrationPolicy,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    oauth_tokens: Option<OAuthTokens>,
    layout: &StoreLayout,
    oauth_clients: Arc<OAuthClients>,
    cloudkit_ops: Option<Arc<dyn CloudKitOps>>,
    clock: ClockRef,
    ids: IdSourceRef,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<StoreDir>, BootstrapError> {
    check_cancel(cancel)?;
    let keychain = Keychain::registered()?;
    let Some(code) = keychain_code(&keychain)? else {
        return Ok(None);
    };
    bootstrap_device(
        BootstrapRequest::Restore {
            code,
            name: device_name.into(),
        },
        synced_tables,
        migrations,
        coven_migration_policy,
        key_custody,
        identity_custody,
        oauth_tokens,
        layout,
        oauth_clients,
        cloudkit_ops,
        clock,
        ids,
        on_status,
        cancel,
        keychain,
        None,
    )
    .await
}

/// Accept provider access, retain one signed request across restarts, and wait
/// for approval. Request deletion returns None; a provider refusal is an error.
/// Explicit cancellation removes the unpublished installation. Dropping the
/// future preserves it for a subsequent call with the same invite and name.
pub async fn join_with_invite(
    code: &str,
    device_name: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    coven_migration_policy: CovenMigrationPolicy,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    oauth_tokens: Option<OAuthTokens>,
    layout: &StoreLayout,
    oauth_clients: Arc<OAuthClients>,
    cloudkit_ops: Option<Arc<dyn CloudKitOps>>,
    clock: ClockRef,
    ids: IdSourceRef,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<StoreDir>, BootstrapError> {
    check_cancel(cancel)?;
    bootstrap_device(
        BootstrapRequest::Join {
            code: coven_sync::read_invite_code(code)?,
            name: device_name.into(),
        },
        synced_tables,
        migrations,
        coven_migration_policy,
        key_custody,
        identity_custody,
        oauth_tokens,
        layout,
        oauth_clients,
        cloudkit_ops,
        clock,
        ids,
        on_status,
        cancel,
        Keychain::registered()?,
        None,
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

pub(crate) enum BootstrapRequest {
    Restore { code: RestoreCode, name: String },
    Join { code: InviteCode, name: String },
}

pub(crate) async fn bootstrap_device(
    request: BootstrapRequest,
    tables: &[SyncedTable],
    migrations: &[Migration],
    policy: CovenMigrationPolicy,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    tokens: Option<OAuthTokens>,
    layout: &StoreLayout,
    oauth: Arc<OAuthClients>,
    cloudkit: Option<Arc<dyn CloudKitOps>>,
    clock: ClockRef,
    ids: IdSourceRef,
    status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
    keychain: Arc<Keychain>,
    supplied_storage: Option<Arc<dyn Storage>>,
) -> Result<Option<StoreDir>, BootstrapError> {
    check_cancel(cancel)?;
    if matches!(key_custody, KeyCustody::InMemory(_))
        || matches!(identity_custody, IdentityCustody::InMemory(_))
    {
        return Err(BootstrapError::EphemeralCustody);
    }
    let (id, name) = match &request {
        BootstrapRequest::Restore { code, .. } => (code.store, &code.name),
        BootstrapRequest::Join { code, .. } => (code.store, &code.name),
    };
    status("Preparing this device");
    let pending = layout.begin_bootstrap(id, name, ids.as_ref())?;
    let result = prepare_and_load(
        &pending,
        &request,
        tables,
        migrations,
        policy,
        tokens,
        oauth,
        cloudkit,
        clock,
        ids,
        &status,
        cancel,
        supplied_storage,
    )
    .await;
    match result {
        Ok(Some((code, ring))) => {
            status("Keeping keys and credentials");
            if let Err(error) = check_cancel(cancel) {
                return cleanup(&pending, Err(error));
            }
            // Publication is the commit point. There are no cancellable awaits
            // between final custody writes, rollback and directory publication.
            commit::publish_bootstrap(pending, code, ring, key_custody, identity_custody, keychain)
                .map(Some)
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
    pending: &BootstrapStore,
    request: &BootstrapRequest,
    tables: &[SyncedTable],
    migrations: &[Migration],
    policy: CovenMigrationPolicy,
    tokens: Option<OAuthTokens>,
    oauth: Arc<OAuthClients>,
    cloudkit: Option<Arc<dyn CloudKitOps>>,
    clock: ClockRef,
    ids: IdSourceRef,
    status: &impl Fn(&str),
    cancel: &watch::Receiver<bool>,
    supplied_storage: Option<Arc<dyn Storage>>,
) -> Result<Option<(RestoreCode, StoreKeyring)>, BootstrapError> {
    let directory = pending.directory();
    let file = directory.owned_file(StoreFile::Bootstrap);
    let (member, data, device_name, mut admission) = match request {
        BootstrapRequest::Restore { code, name } => {
            let mut data =
                RestoreStorage::decode(code.storage.as_bytes()).map_err(SyncError::from)?;
            if matches!(data.credentials, StorageCredentials::OAuth(_)) {
                data.credentials = StorageCredentials::OAuth(
                    sign_in(data.location.provider(), tokens, &oauth, cancel).await?,
                );
            }
            (
                code.member_keys.clone(),
                data,
                name.as_str(),
                Admission::Restoring,
            )
        }
        BootstrapRequest::Join { code, name } => {
            let identity = JoiningIdentity::prepare(&file, code, name)?;
            let (invitation, credentials) = match InviteStorage::decode(code.storage.as_bytes())
                .map_err(SyncError::from)?
            {
                InviteStorage::S3 {
                    invitation,
                    credentials,
                } => (invitation, StorageCredentials::S3(credentials)),
                InviteStorage::Account(invitation) => {
                    let provider = invitation.location().provider();
                    let credentials = if provider == CloudProvider::CloudKit {
                        StorageCredentials::CloudKit
                    } else {
                        StorageCredentials::OAuth(sign_in(provider, tokens, &oauth, cancel).await?)
                    };
                    (invitation, credentials)
                }
            };
            (
                identity.member_keys(),
                RestoreStorage {
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
    let storage = match supplied_storage {
        Some(storage) => {
            if storage.config() != data.location {
                return Err(SyncError::Storage(StorageError::InvitationMismatch).into());
            }
            storage
        }
        None => crate::connection::connect(
            &data,
            settings.device_id,
            cloudkit,
            clock.clone(),
            ids.clone(),
        )
        .await
        .map_err(SyncError::from)?,
    };
    let ring: Arc<dyn StoreKeyCustody> = Arc::new(InMemoryCustody::<StoreKeyring>::empty());
    let identity: Arc<dyn MemberKeyCustody> = Arc::new(InMemoryCustody::new(member.clone()));
    let database = DatabaseBuilder::new(directory.clone())
        .migration_operation(StoreLogSync::migration_operation)
        .synced_tables(tables.to_vec())
        .migrations(migrations.to_vec())
        .coven_migration_policy(policy)
        .clock(clock.clone())
        .id_source(ids.clone())
        .open()
        .await?;
    let mut sync = StoreLogSync::new(
        storage,
        database.clone(),
        ring.clone(),
        identity,
        clock,
        ids,
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
                match sync.join_outcome(joining).await? {
                    coven_sync::JoinOutcome::Admitted => break,
                    coven_sync::JoinOutcome::Declined => return Ok(false),
                    coven_sync::JoinOutcome::Waiting => {}
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        } else {
            status("Opening member keys and the store log");
            if !sync.bootstrap_member().await? {
                return Err(SyncError::NotStoreMember(member.member_id()).into());
            }
        }
        status("Loading snapshots and later writes");
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
    let closed = database
        .close()
        .await
        .map_err(CovenError::from)
        .map_err(BootstrapError::from);
    let loaded = combine(result, closed)?;
    if !loaded {
        return Ok(None);
    }
    let ring = ring.unlock()?.ok_or(SyncError::MissingMemberKeys)?;
    Ok(Some((
        RestoreCode {
            store: settings.id,
            name: settings.name,
            member_keys: member,
            storage: data.encode().map_err(SyncError::from)?,
        },
        ring,
    )))
}

async fn sign_in(
    provider: CloudProvider,
    tokens: Option<OAuthTokens>,
    oauth: &OAuthClients,
    cancel: &watch::Receiver<bool>,
) -> Result<OAuthTokens, BootstrapError> {
    match tokens {
        Some(tokens) => Ok(tokens),
        None => match oauth.authorize(provider, cancel.clone()).await {
            Err(OAuthError::Cancelled) => Err(BootstrapError::Cancelled),
            result => Ok(result?),
        },
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
