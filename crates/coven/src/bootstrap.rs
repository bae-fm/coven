//! New-device composition roots. Sync owns admission and loading; foundation
//! owns unpublished directories and crypto owns final key custody (§13, E10).

use super::*;
#[cfg(test)]
use coven_crypto::custody::Keychain;
use coven_crypto::custody::{InMemoryCustody, PersonEntries, StoreKeychain};
use coven_database::Database;
use coven_format::codes::{InviteCode, RestoreCode};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::files::{
    BootstrapDirectoryError, BootstrapStore, ReplacedBy, ReplacementStore,
};
use coven_storage::{
    ConnectionCredentials, InviteStorage, RestoreStorage, StorageCredentials, StorageFailure,
    StorageSettings,
};
use coven_sync::SequenceSync;
use std::sync::Arc;
use tokio::sync::watch;

#[path = "bootstrap_authentication.rs"]
mod authentication;
use authentication::{account_sign_in, join_sign_in, provider_auth, SignInCustody};
use coven_sync::SessionSignIn;

#[path = "bootstrap_commit.rs"]
mod commit;

#[path = "bootstrap_join.rs"]
mod join;

#[path = "bootstrap_replace.rs"]
mod replace;
pub(super) use replace::restore_device;

#[path = "bootstrap_reset.rs"]
mod reset;
pub(super) use reset::reset_device;

#[path = "bootstrap_restore.rs"]
mod restore;
#[cfg(test)]
use restore::keychain_code;
pub use restore::{restore_from_code, restore_from_saved_code};
pub(super) use restore::{resume_restore, unfinished_restores};

#[path = "restore_credentials.rs"]
mod credentials;
use credentials::restore_connection;

#[path = "bootstrap_status.rs"]
mod status;
pub use status::BootstrapStatus;
use status::{loading, open_installation};

/// Opening a new installation failed; unpublished work remains available for
/// an explicit retry unless the call was cancelled or the invite was settled.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    /// Cancellation removes unpublished local data and leaves final custody alone.
    #[error("opening cancelled")]
    Cancelled,
    /// Cancelling a join after its request may have been sent. The join stays
    /// unfinished and resumable; it ends only by approval, decline or expiry
    /// (§13.2).
    #[error("the join request may have been sent; the join stays unfinished")]
    JoinRequestSent,
    /// No join is under way for this store on this device: none was started
    /// here, or it has ended.
    #[error("no join is under way for store {0}")]
    NoUnfinishedJoin(StoreId),
    /// Cancelling a restore once its device may be registered. The restore
    /// stays unfinished and listed, and only finishes (§13.1).
    #[error("the device may be registered; the restore stays unfinished")]
    RestoreRegistered,
    /// No restore is under way for this store on this device: none recorded
    /// itself here, or it has ended.
    #[error("no restore is under way for store {0}")]
    NoUnfinishedRestore(StoreId),
    /// The code cannot be used for this operation.
    #[error(transparent)]
    Code(#[from] CodeError),
    /// Preparing, publishing or removing the unpublished directory failed.
    #[error(transparent)]
    Directory(BootstrapDirectoryError),
    /// Opening the database failed.
    #[error(transparent)]
    Store(CovenError),
    /// The device is locked, and the protection class the app chose keeps
    /// the installation's files closed until it unlocks (api.md E1). The
    /// unfinished installation stays for the retry; the app waits for the
    /// unlock and calls again.
    #[error("{}: the device is locked, and the store's files are protected until it unlocks", path.display())]
    DeviceLocked {
        /// The first protected file the lock refused.
        path: std::path::PathBuf,
    },
    /// Provider admission, signed membership or snapshot loading failed.
    #[error(transparent)]
    Sync(SyncError),
    /// Provider sign-in failed.
    #[error(transparent)]
    OAuth(coven_storage::providers::OAuthError),
    /// Final custody could not be read or written.
    #[error(transparent)]
    SecureStorage(KeyError),
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
    /// The call's runtime is shutting down and dropped its keychain work
    /// before it started, so nothing changed (E15).
    #[error(transparent)]
    Shutdown(#[from] crate::task::Shutdown),
    /// The call's work panicked: a bug, which the caller reports (E15).
    #[error(transparent)]
    Bug(#[from] crate::task::Bug),
}

impl From<BootstrapDirectoryError> for BootstrapError {
    fn from(error: BootstrapDirectoryError) -> Self {
        if let BootstrapDirectoryError::Bug(bug) = error {
            return Self::Bug(bug);
        }
        match error.device_locked() {
            Some(path) => Self::DeviceLocked { path: path.to_owned() },
            None => Self::Directory(error),
        }
    }
}

/// A code sync refuses is refused as the call's own code: a join's, as a
/// restore's.
impl From<SyncError> for BootstrapError {
    fn from(error: SyncError) -> Self {
        match error {
            SyncError::Code(error) => Self::Code(error),
            SyncError::Bug(bug) => Self::Bug(bug),
            error => Self::Sync(error),
        }
    }
}

impl From<CovenError> for BootstrapError {
    fn from(error: CovenError) -> Self {
        match error {
            CovenError::DeviceLocked { path } => Self::DeviceLocked { path },
            CovenError::Bug(bug) => Self::Bug(bug),
            error => Self::Store(error),
        }
    }
}

impl From<coven_storage::providers::OAuthError> for BootstrapError {
    fn from(error: coven_storage::providers::OAuthError) -> Self {
        match error {
            coven_storage::providers::OAuthError::Storage(StorageError::Bug(bug)) => Self::Bug(bug),
            error => Self::OAuth(error),
        }
    }
}

impl From<KeyError> for BootstrapError {
    fn from(error: KeyError) -> Self {
        match error {
            KeyError::Bug(bug) => Self::Bug(bug),
            error => Self::SecureStorage(error),
        }
    }
}

impl crate::task::CarriesBug for BootstrapError {
    fn bug(&self) -> Option<&crate::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Directory(error) => error.bug(),
            Self::Store(error) => error.bug(),
            Self::Sync(error) => error.bug(),
            Self::OAuth(error) => error.bug(),
            Self::SecureStorage(error) => error.bug(),
            Self::Cleanup { operation, cleanup } => operation.bug().or_else(|| cleanup.bug()),
            Self::Published { source, .. } => source.bug(),
            _ => None,
        }
    }
}

impl From<crate::task::TaskFailed> for BootstrapError {
    fn from(failed: crate::task::TaskFailed) -> Self {
        failed.into_error()
    }
}

/// Accept provider access, retain one signed request across restarts, and wait
/// for approval. Returns the open store, or None when declined or expired.
/// Dropping the future preserves the unfinished join for a subsequent call
/// with the same invite and device name, or `resume_join`. On Dropbox and
/// OneDrive the builder's sign-in is kept on the device beside the saved code
/// until the join ends, and a later call without one uses it (§13.2).
/// `control` cancels the join or asks for a check for approval at once.
/// Cancelling before the request may have been sent removes the unpublished
/// installation; after, the call ends with `BootstrapError::JoinRequestSent`
/// and keeps it, as dropping does.
pub async fn join_with_invite(
    builder: CovenBuilder,
    code: &str,
    device_name: &str,
    on_status: impl Fn(BootstrapStatus),
    control: &JoinControl,
) -> Result<Option<CovenHandle>, BootstrapError> {
    // Whether a cancel may still remove the join is known only once its
    // saved request is read, so the call checks after preparing it.
    let cancel = control.cancel.subscribe();
    Box::pin(bootstrap_device(
        builder,
        BootstrapRequest::Join {
            code: coven_sync::read_invite_code(code)?,
            name: DeviceName::Given(device_name.into()),
            check: control.check.subscribe(),
        },
        on_status,
        &cancel,
    ))
    .await
}

/// The joins under way on this device, ordered by store id: each unfinished
/// first installation with a saved invite code, as `decode_code_info` reads
/// it (`CovenBuilder::unfinished_joins`).
pub(super) async fn unfinished_joins(
    builder: &CovenBuilder,
) -> Result<Vec<InviteInfo>, BootstrapError> {
    let keychain = builder.keychain()?;
    let mut joins = Vec::new();
    for store in builder
        .directories
        .unfinished_first_installations()
        .await
        .map_err(BootstrapDirectoryError::from)?
    {
        // An unfinished restore, or a join that can't have sent its
        // request, saves no code: it is not a join to resume.
        let Some(text) = StoreKeychain::new(keychain.clone(), store).invite_code()? else {
            continue;
        };
        let info = match coven_sync::decode_code_info(text.as_str())? {
            CodeInfo::Invite(info) => info,
            CodeInfo::Restore(_) => {
                return Err(CodeError::WrongKind {
                    expected: CodeKind::Invite,
                    actual: CodeKind::Restore,
                }
                .into())
            }
        };
        CodeError::require_store(store, info.metadata.store_id)?;
        joins.push(info);
    }
    Ok(joins)
}

/// Continue an unfinished join from its saved code and the device name its
/// saved request carries (`CovenBuilder::resume_join`).
pub(super) async fn resume_join(
    builder: CovenBuilder,
    store: StoreId,
    on_status: impl Fn(BootstrapStatus),
    control: &JoinControl,
) -> Result<Option<CovenHandle>, BootstrapError> {
    let code = saved_invite_code(&StoreKeychain::new(builder.keychain()?, store), store)?
        .ok_or(BootstrapError::NoUnfinishedJoin(store))?;
    let cancel = control.cancel.subscribe();
    Box::pin(bootstrap_device(
        builder,
        BootstrapRequest::Join { code, name: DeviceName::Saved, check: control.check.subscribe() },
        on_status,
        &cancel,
    ))
    .await
}

/// The invite code a join under way saved for `store`.
fn saved_invite_code(
    keychain: &StoreKeychain,
    store: StoreId,
) -> Result<Option<InviteCode>, BootstrapError> {
    let Some(text) = keychain.invite_code()? else {
        return Ok(None);
    };
    let code = coven_sync::read_invite_code(text.as_str())?;
    CodeError::require_store(store, code.store)?;
    Ok(Some(code))
}

/// The app's hold on a running join: cancel it, or check for approval at once.
pub struct JoinControl {
    cancel: watch::Sender<bool>,
    check: watch::Sender<u64>,
}

impl JoinControl {
    /// A control no join has used yet.
    pub fn new() -> Self {
        Self { cancel: watch::channel(false).0, check: watch::channel(0).0 }
    }

    /// Cancel the join this control was passed to.
    pub fn cancel(&self) {
        self.cancel.send_replace(true);
    }

    /// End the join's wait so the next check runs now, keeping its backoff's
    /// failure count. A provider cooldown still holds (§13.2).
    pub fn check_now(&self) {
        self.check.send_modify(|asked| *asked = asked.wrapping_add(1));
    }
}

enum BootstrapRequest {
    Restore {
        code: RestoreCode,
        data: ConnectionCredentials,
        /// Where an OAuth connection refreshes: the session, or the store's
        /// device credentials a recorded restore kept its sign-in in.
        sign_in: SignInCustody,
        name: String,
    },
    Join {
        code: InviteCode,
        name: DeviceName,
        /// Changes each time the app asks for a check for approval now.
        check: watch::Receiver<u64>,
    },
}

/// The device name a join's request carries.
enum DeviceName {
    /// The name the app passed, which a saved request must repeat.
    Given(String),
    /// The name the saved request carries: the join resumes from it.
    Saved,
}

/// Its state runs to tens of KiB, so the public calls await it boxed and the
/// futures apps hold stay small (application_api_tests bounds them).
async fn bootstrap_device(
    mut builder: CovenBuilder,
    request: BootstrapRequest,
    status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<CovenHandle>, BootstrapError> {
    if matches!(request, BootstrapRequest::Restore { .. }) {
        check_cancel(cancel)?;
    }
    let keychain = builder.keychain()?;
    let id = match &request {
        BootstrapRequest::Restore { code, .. } => code.store,
        BootstrapRequest::Join { code, .. } => code.store,
    };
    let device_entries = StoreKeychain::new(keychain.clone(), id);
    let clock = builder.clock.clone();
    let started = clock.elapsed();
    let pending = builder.directories.begin_bootstrap(id, builder.ids.as_ref()).await?;
    let result =
        prepare_and_load(&mut builder, Installing::New(&pending), &request, &status, cancel).await;
    match result {
        Ok(Some(Loaded {
            code,
            provider,
            credentials,
            sign_in,
            ring,
            database,
            storage,
            state,
            publications,
            waits,
        })) => {
            let requests = storage.requests();
            status(BootstrapStatus::Committing);
            // The saved code's name, read before the final steps, which no
            // await separates; so is the status the published store opens
            // with (E5).
            let name = match commit::loaded_name(&database).await {
                Ok(name) => crate::storage::record_opened(
                    &database,
                    &clock,
                    coven_database::RecordedLocation::Connected(provider),
                    Some(clock.now()),
                )
                .await
                .map(|()| name)
                .map_err(BootstrapError::from),
                Err(error) => Err(error),
            };
            let result = match name.and_then(|name| {
                // A loaded join was approved: its request was sent. A loaded
                // restore registered its device (§13.1).
                if check_cancel(cancel).is_err() {
                    return Err(match &request {
                        BootstrapRequest::Join { .. } => BootstrapError::JoinRequestSent,
                        BootstrapRequest::Restore { .. } => BootstrapError::RestoreRegistered,
                    });
                }
                builder.authentication.take();
                let mut owners = builder
                    .owners(pending.directory(), Arc::new(StoreKeychain::new(keychain, id)))?;
                owners.state = state;
                owners.publications = publications;
                owners.storage = Some(storage);
                // A join's kept sign-in is already this store's device
                // credentials: the handle refreshes through the same custody
                // as the connection it continues.
                if let SignInCustody::Device { credentials, .. } = &sign_in {
                    owners.credentials = credentials.clone();
                }
                Ok((owners, name))
            }) {
                // No cancellable await separates final custody, publication
                // and the handle.
                Ok((owners, name)) => {
                    let publishing = clock.elapsed();
                    let published = commit::publish_bootstrap(
                        &pending,
                        code,
                        name,
                        credentials,
                        sign_in,
                        ring,
                        owners,
                        database.clone(),
                        waits,
                    )
                    .await;
                    tracing::info!(
                        published = published.is_ok(),
                        time = ?(clock.elapsed() - publishing),
                        "committed keys and credentials and published the store"
                    );
                    tracing::info!(
                        restore = matches!(request, BootstrapRequest::Restore { .. }),
                        published = published.is_ok(),
                        time = ?(clock.elapsed() - started),
                        %requests,
                        "ended the new installation's bootstrap"
                    );
                    published
                }
                Err(error) => Err(error),
            };
            // A published store no longer needs what its join or restore
            // kept to finish it.
            let result = match &request {
                BootstrapRequest::Join { .. } => {
                    forget_kept(result, || device_entries.delete_invite_code())
                }
                BootstrapRequest::Restore { .. } => {
                    forget_kept(result, || device_entries.delete_restore_record())
                }
            };
            match result {
                Ok(handle) => Ok(Some(handle)),
                Err(error @ BootstrapError::Published { .. }) => Err(error),
                // A loaded join or restore is past cancelling: it stays for
                // the next call.
                Err(error) => combine(Err(error), close_database(&database).await),
            }
        }
        // Only a join ends without a store: declined or expired.
        Ok(None) => remove_unfinished(&device_entries, &pending, Ok(None)),
        Err(error) => match &request {
            BootstrapRequest::Restore { .. } => {
                restore::ended(&builder, &pending, &device_entries, error).await
            }
            BootstrapRequest::Join { .. } if matches!(error, BootstrapError::Cancelled) => {
                remove_unfinished(&device_entries, &pending, Err(error))
            }
            BootstrapRequest::Join { .. } => Err(error),
        },
    }
}

/// Remove an unfinished first installation that ends without a store: a join
/// declined, expired or cancelled before its request was sent, or a restore
/// cancelled, or ended by the facts once recorded (§13.1), which may have
/// loaded into a join's installation. The device's provider credentials go
/// first, which only a join's or a recorded restore's kept sign-in can be
/// before publication; then the saved code or record; then the installation.
/// A failure part way leaves the code or record and installation, which list
/// as the join or restore it was and a retry ends again (signing in first
/// once the kept sign-in is gone), or the installation alone, which lists as
/// neither. Credentials are never left without their code or record (§13.2).
fn remove_unfinished<T>(
    device_entries: &StoreKeychain,
    pending: &BootstrapStore,
    result: Result<T, BootstrapError>,
) -> Result<T, BootstrapError> {
    match device_entries
        .delete_storage_credentials()
        .and_then(|()| device_entries.delete_invite_code())
        .and_then(|()| device_entries.delete_restore_record())
    {
        Ok(()) => cleanup(pending, result),
        Err(error) => combine(result, Err(error.into())),
    }
}

/// A published join no longer needs its saved code, nor a published restore
/// its record: `delete` deletes it. Failing to is reported with the open
/// handle, as other failures after publication are.
fn forget_kept(
    result: Result<CovenHandle, BootstrapError>,
    delete: impl FnOnce() -> Result<(), KeyError>,
) -> Result<CovenHandle, BootstrapError> {
    let (handle, published) = match result {
        Ok(handle) => (handle, None),
        Err(BootstrapError::Published { handle, source }) => (handle, Some(*source)),
        Err(error) => return Err(error),
    };
    let deleted = delete().map_err(BootstrapError::from);
    match (published.map_or(Ok(()), Err), deleted) {
        (Ok(()), Ok(())) => Ok(handle),
        (failed, deleted) => Err(BootstrapError::Published {
            handle,
            source: Box::new(combine(failed, deleted).expect_err("a failure")),
        }),
    }
}

/// Where a new installation loads: a new device's unpublished store, or the
/// replacement of this device's installation (§19.3).
#[derive(Clone, Copy)]
enum Installing<'a> {
    New(&'a BootstrapStore),
    Replacing(&'a ReplacementStore),
}

impl Installing<'_> {
    fn directory(&self) -> StoreDir {
        match self {
            Self::New(pending) => pending.directory(),
            Self::Replacing(replacement) => replacement.directory(),
        }
    }
}

enum Admission {
    Restoring,
    Joining(join::Joining),
}

impl Admission {
    /// Once a join's request may have been sent, cancelling can't take it
    /// back: approval could land after its keys were thrown away, leaving a
    /// member no device can act as (§13.2). The join stays unfinished.
    fn cancellable<T>(&self, result: Result<T, BootstrapError>) -> Result<T, BootstrapError> {
        match (result, self) {
            (Err(BootstrapError::Cancelled), Self::Joining(joining)) if joining.sent() => {
                Err(BootstrapError::JoinRequestSent)
            }
            (result, _) => result,
        }
    }
}

/// A loaded store awaiting publication, with what reaches its storage.
struct Loaded {
    code: RestoreCode,
    /// The provider of the location loaded from.
    provider: CloudProvider,
    credentials: StorageCredentials,
    sign_in: SignInCustody,
    ring: StoreKeyring,
    database: Database,
    storage: Arc<coven_storage::StorageConnection>,
    /// The sequence state the handle continues, and the connection's gate around it.
    state: Arc<coven_sync::SequenceState>,
    publications: Arc<crate::storage::Publications>,
    waits: Arc<coven_sync::RetryWaits>,
}

async fn prepare_and_load(
    builder: &mut CovenBuilder,
    pending: Installing<'_>,
    request: &BootstrapRequest,
    status: &impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<Loaded>, BootstrapError> {
    let directory = pending.directory();
    let (member, mut data, mut sign_in, device_name, mut admission) = match request {
        BootstrapRequest::Restore { code, data, sign_in, name } => (
            MemberIdentity::new(code.member, code.member_keys.clone()),
            data.clone(),
            sign_in.clone(),
            name.clone(),
            Admission::Restoring,
        ),
        BootstrapRequest::Join { code, name, check } => {
            let (joining, data, sign_in) =
                join::prepare_request(builder, &directory, code, name, check.clone())?;
            (
                joining.identity(),
                data,
                sign_in,
                joining.device_name().to_owned(),
                Admission::Joining(joining),
            )
        }
    };
    admission.cancellable(check_cancel(cancel))?;
    // A new device's restore records itself before it registers (§13.1).
    let recording = match (request, pending) {
        (BootstrapRequest::Restore { code, name, .. }, Installing::New(_)) => {
            Some(restore::Recording::new(builder, code.store, name)?)
        }
        _ => None,
    };
    let provider = data.location.provider();
    let settings = directory.settings().map_err(CovenError::from)?;
    let state = Arc::new(coven_sync::SequenceState::new());
    let publications = Arc::new(crate::storage::Publications::new(state.clone()));
    let ring = Arc::new(KeySession::store(Arc::new(InMemoryCustody::<StoreKeyring>::empty()))?);
    let identity = Arc::new(KeySession::member(Arc::new(InMemoryCustody::new(member.clone())))?);
    // A replacement registers the fresh id as the old one's replacement.
    let replacement = match pending {
        Installing::Replacing(replacement) => Some(replacement),
        Installing::New(_) => None,
    };
    // The database opens before connecting: its persisted waits and provider
    // cooldown hold the joiner's polls and every request across restarts (§6.7).
    let clock = builder.clock.clone();
    let database = open_installation(builder.database(directory.clone())?, status).await?;
    status(BootstrapStatus::Connecting);
    let connected = async {
        let connecting = clock.elapsed();
        let waits = Arc::new(
            coven_sync::RetryWaits::open(database.clone(), builder.clock.clone())
                .await
                .map_err(CovenError::from)?,
        );
        data.location = crate::authentication::label_location(
            builder.connector().as_ref(),
            data.location.clone(),
            provider_auth(&data, &sign_in)?,
            waits.clone(),
        )
        .await
        .map_err(SyncError::from)?;
        let storage = builder
            .connector()
            .connect(
                data.location.clone(),
                provider_auth(&data, &sign_in)?,
                settings.device_id,
                publications.clone(),
                waits.clone(),
                builder.transfers.clone(),
            )
            .await
            .map_err(SyncError::from)?;
        if matches!(admission, Admission::Restoring) {
            storage.check_bucket(settings.id).await.map_err(SyncError::from)?;
        }
        tracing::info!(
            ?provider,
            time = ?(clock.elapsed() - connecting),
            requests = %storage.requests(),
            "connected to storage"
        );
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
            clock.clone(),
            coven_foundation::network_watch::NetworkWatch::unwatched().view(),
        );
        let sync = SequenceSync::open(
            database.clone(),
            storage.clone(),
            directory.clone(),
            settings.device_id,
            ring.clone(),
            identity,
            Arc::new(PersonEntries::new(builder.keychain()?, settings.id)),
            Arc::new(StorageSettings::new(directory.clone())),
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
            return combine(sign_in.refused(provider, Err(error)), close_database(&database).await)
        }
    };
    #[cfg(test)]
    if let Some(recorder) = &builder.loading_recorder {
        recorder.watch_progress(sync.subscribe_progress());
    }
    let loading = async {
        if let Admission::Joining(joining) = &mut admission {
            if !joining.admit(&sync, &database, status).await? {
                return Ok(None);
            }
        }
        // Reading forward opens this member's copies and loads the floor's
        // snapshots and the slots after them; an interrupted bootstrap's
        // registration is already applied.
        let unlocking = (clock.elapsed(), storage.requests());
        loading(&sync, sync.unlock(), status).await?;
        tracing::info!(
            time = ?(clock.elapsed() - unlocking.0),
            requests = %storage.requests().since(&unlocking.1),
            "read the store forward with this member's keys"
        );
        status(BootstrapStatus::Registering);
        // A restore checks the access it reaches storage with before it
        // registers, and records it after, as setup does, so the member's
        // recorded access is the one in use (§13.1). A joiner's is the one
        // its approval recorded from the invite.
        let access = match admission {
            Admission::Restoring => {
                let access = crate::storage::member_access(&data.credentials, &storage)
                    .await
                    .map_err(SyncError::from)?;
                sync.check_access(&access).await?;
                Some(access)
            }
            Admission::Joining(_) => None,
        };
        let registered = database
            .store_state()
            .await?
            .devices
            .get(&settings.device_id)
            .is_some_and(|device| device.member == member.member());
        Ok::<_, BootstrapError>(Some((access, registered)))
    };
    let mut cancellation = cancel.clone();
    let loaded = tokio::select! {
        biased;
        _ = cancelled(&mut cancellation) => Err(BootstrapError::Cancelled),
        result = loading => result,
    };
    let result = match loaded {
        Ok(Some((access, registered))) => {
            // The work that writes to storage. A cancel stops it only between
            // its steps: one under way, a registration on its way above all,
            // runs to its end first (§11.6).
            let registering = async {
                if !registered {
                    check_cancel(cancel)?;
                    if let Some(recording) = &recording {
                        sign_in = recording.record(&member, settings.id, &data, sign_in.clone())?;
                    }
                    let replaces =
                        replacement.map(|replacement| replacement.device_reset().old_device);
                    let registering = (clock.elapsed(), storage.requests());
                    sync.register_device(device_name, replaces).await?;
                    tracing::info!(
                        time = ?(clock.elapsed() - registering.0),
                        requests = %storage.requests().since(&registering.1),
                        "registered this device"
                    );
                }
                if let Some(access) = access {
                    let state = database.store_state().await?;
                    if let Some(change) =
                        crate::storage::access_change(&state, member.member(), access)?
                    {
                        check_cancel(cancel)?;
                        sync.administer(change).await?;
                    }
                }
                // The notice commits before any handle is published (E5).
                if let Some(replacement) = replacement {
                    check_cancel(cancel)?;
                    reset::carry_discarded_provider_work(&sync, &database, replacement).await?;
                }
                Ok::<_, BootstrapError>(true)
            };
            tokio::pin!(registering);
            let ended = tokio::select! {
                biased;
                _ = cancelled(&mut cancellation) => None,
                result = &mut registering => Some(result),
            };
            match ended {
                Some(result) => result,
                None => match registering.await {
                    Err(error) if crate::task::CarriesBug::bug(&error).is_some() => Err(error),
                    _ => Err(BootstrapError::Cancelled),
                },
            }
        }
        Ok(None) => Ok(false),
        Err(error) => Err(error),
    };
    let result = sign_in.refused(provider, admission.cancellable(result));
    files.close().await;
    drop(sync);
    let result = result.and_then(|loaded| {
        if !loaded {
            return Ok(None);
        }
        let ring = ring.read()?.ok_or(SyncError::from(StorageFailure::MemberKeysMissing))?;
        Ok(Some((
            coven_sync::location_code(settings.id, &member, data.location.clone())?,
            data.credentials,
            ring,
        )))
    });
    match result {
        Ok(Some((code, credentials, ring))) => Ok(Some(Loaded {
            code,
            provider,
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
            let closed = close_database(&database).await;
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

/// Close the installation's database; its failure is the bootstrap's.
async fn close_database(database: &Database) -> Result<(), BootstrapError> {
    database.close().await.map_err(CovenError::from).map_err(BootstrapError::from)
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

/// Every entry under `path` with its contents, sorted, so a test can show
/// that a refused replacement changed nothing.
#[cfg(test)]
fn files_under(path: &std::path::Path) -> Vec<(std::path::PathBuf, Option<Vec<u8>>)> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let relative = std::path::PathBuf::from(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            found.extend(
                files_under(&entry.path())
                    .into_iter()
                    .map(|(inner, bytes)| (relative.join(inner), bytes)),
            );
            found.push((relative, None));
        } else {
            found.push((relative, Some(std::fs::read(entry.path()).unwrap())));
        }
    }
    found.sort();
    found
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
