//! New-device composition roots. Sync owns admission and loading; foundation
//! owns unpublished directories and crypto owns final key custody (§13, E10).

use super::*;
#[cfg(test)]
use coven_crypto::custody::Keychain;
use coven_crypto::custody::{InMemoryCustody, StoreKeychain, SyncedKeychain};
use coven_database::Database;
use coven_format::codes::{InviteCode, RestoreCode};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::files::{
    BootstrapDirectoryError, BootstrapStore, ReplacedBy, ReplacementStore,
};
use coven_storage::{
    ConnectionCredentials, InviteStorage, StorageCredentials, StorageFailure, StorageSettings,
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
    /// Cancelling a join after its request may have been sent. The join stays
    /// unfinished and resumable; it ends only by approval, decline or expiry
    /// (§13.2).
    #[error("the join request may have been sent; the join stays unfinished")]
    JoinRequestSent,
    /// No join is under way for this store on this device: none was started
    /// here, or it has ended.
    #[error("no join is under way for store {0}")]
    NoUnfinishedJoin(StoreId),
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
    /// The join request is sent and waits for approval (§13.2).
    WaitingForApproval {
        /// The number the admin of a sent invite compares; `None` in person.
        confirmation: Option<ConfirmationNumber>,
        /// When the join checks for approval next.
        next_check: NextCheck,
    },
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
    Box::pin(bootstrap_device(
        builder,
        BootstrapRequest::Restore {
            code,
            data,
            sign_in,
            name: device_name.into(),
        },
        on_status,
        cancel,
    ))
    .await?
    .ok_or_else(|| SyncError::from(StorageFailure::MemberKeysMissing).into())
}

/// Restore a readable entry selected from `saved_restore_codes`. Reads the current
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
    Box::pin(bootstrap_device(
        builder,
        BootstrapRequest::Restore {
            code,
            data,
            sign_in,
            name: device_name.into(),
        },
        on_status,
        cancel,
    ))
    .await
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
        .layout
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
        if info.metadata.store_id != store {
            return Err(CodeError::WrongStore {
                expected: store,
                actual: info.metadata.store_id,
            }
            .into());
        }
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
        BootstrapRequest::Join {
            code,
            name: DeviceName::Saved,
            check: control.check.subscribe(),
        },
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
    if code.store != store {
        return Err(CodeError::WrongStore {
            expected: store,
            actual: code.store,
        }
        .into());
    }
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
        Self {
            cancel: watch::channel(false).0,
            check: watch::channel(0).0,
        }
    }

    /// Cancel the join this control was passed to.
    pub fn cancel(&self) {
        self.cancel.send_replace(true);
    }

    /// End the join's wait so the next check runs now, keeping its backoff's
    /// failure count. A provider cooldown still holds (§13.2).
    pub fn check_now(&self) {
        self.check
            .send_modify(|asked| *asked = asked.wrapping_add(1));
    }
}

impl Default for JoinControl {
    fn default() -> Self {
        Self::new()
    }
}

#[expect(
    clippy::map_err_ignore,
    reason = "read_saved_code logs why a saved code is unreadable; restoring sees Invalid"
)]
fn keychain_code(
    keychain: &SyncedKeychain,
    store: StoreId,
) -> Result<Option<(RestoreCode, StorageConfig)>, BootstrapError> {
    keychain
        .restore_code()?
        .map(|bytes| {
            coven_sync::read_saved_code(store, bytes.as_bytes())
                .map(|(saved, location)| (saved.code, location))
                .map_err(|_| CodeError::Invalid.into())
        })
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
    status(BootstrapStatus::Connecting);
    let pending = builder
        .layout
        .begin_bootstrap(id, builder.ids.as_ref())
        .await?;
    let result = prepare_and_load(
        &mut builder,
        Installing::New(&pending),
        &request,
        &status,
        cancel,
    )
    .await;
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
            // The saved code's name, read before the final steps, which no
            // await separates.
            let name = commit::loaded_name(&database).await;
            let result = match name.and_then(|name| {
                // A loaded join was approved: its request was sent.
                check_cancel(cancel).map_err(|error| match &request {
                    BootstrapRequest::Join { .. } => BootstrapError::JoinRequestSent,
                    BootstrapRequest::Restore { .. } => error,
                })?;
                builder.authentication.take();
                let mut owners = builder.owners(
                    pending.directory(),
                    Arc::new(StoreKeychain::new(keychain, id)),
                )?;
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
                    commit::publish_bootstrap(
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
                    .await
                }
                Err(error) => Err(error),
            };
            let result = match &request {
                BootstrapRequest::Join { .. } => forget_joined_code(&device_entries, result),
                BootstrapRequest::Restore { .. } => result,
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
                        remove_unfinished(&device_entries, &pending, result)
                    } else {
                        result
                    }
                }
            }
        }
        // Only a join ends without a store: declined or expired.
        Ok(None) => remove_unfinished(&device_entries, &pending, Ok(None)),
        Err(BootstrapError::Cancelled) => {
            remove_unfinished(&device_entries, &pending, Err(BootstrapError::Cancelled))
        }
        Err(error) => Err(error),
    }
}

/// Remove an unfinished first installation that ends without a store: a join
/// declined, expired or cancelled before its request was sent, or a cancelled
/// restore, which may have loaded into a join's installation. The device's
/// provider credentials go first, which only a join's kept sign-in can be
/// before publication; then the saved code; then the installation. A failure
/// part way leaves the code and installation, which list as the join it was
/// and a retry ends again (signing in first once the kept sign-in is gone),
/// or the installation alone, which lists as no join. Credentials are never
/// left without their code (§13.2).
fn remove_unfinished<T>(
    device_entries: &StoreKeychain,
    pending: &BootstrapStore,
    result: Result<T, BootstrapError>,
) -> Result<T, BootstrapError> {
    match device_entries
        .delete_storage_credentials()
        .and_then(|()| device_entries.delete_invite_code())
    {
        Ok(()) => cleanup(pending, result),
        Err(error) => combine(result, Err(error.into())),
    }
}

/// A published join no longer needs its saved code. Failing to delete it is
/// reported with the open handle, as other failures after publication are.
fn forget_joined_code(
    device_entries: &StoreKeychain,
    result: Result<CovenHandle, BootstrapError>,
) -> Result<CovenHandle, BootstrapError> {
    let (handle, published) = match result {
        Ok(handle) => (handle, None),
        Err(BootstrapError::Published { handle, source }) => (handle, Some(*source)),
        Err(error) => return Err(error),
    };
    let deleted = device_entries
        .delete_invite_code()
        .map_err(BootstrapError::from);
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

enum Admission<'a> {
    Restoring,
    Joining(join::Joining<'a>),
}

impl Admission<'_> {
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
    credentials: StorageCredentials,
    sign_in: SignInCustody,
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
    pending: Installing<'_>,
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
            sign_in
                .clone()
                .map_or(SignInCustody::None, SignInCustody::Session),
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
    let provider = data.location.provider();
    let settings = directory.settings().map_err(CovenError::from)?;
    let state = Arc::new(coven_sync::SyncState::new());
    let publications = Arc::new(crate::storage::Publications::new(state.clone()));
    let ring = Arc::new(KeySession::store(Arc::new(
        InMemoryCustody::<StoreKeyring>::empty(),
    ))?);
    let identity = Arc::new(KeySession::member(Arc::new(InMemoryCustody::new(
        member.clone(),
    )))?);
    // A replacement registers the fresh id as the old one's replacement.
    let replacement = match pending {
        Installing::Replacing(replacement) => Some(replacement),
        Installing::New(_) => None,
    };
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
            return combine(
                sign_in.refused(provider, Err(error)),
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
            let replaces = replacement.map(|replacement| replacement.device_reset().old_device);
            sync.register_device(device_name, replaces).await?;
        }
        // The notice commits before any handle is published (E5).
        if let Some(replacement) = replacement {
            reset::carry_discarded_provider_work(&sync, &database, replacement).await?;
        }
        Ok::<_, BootstrapError>(true)
    };
    let mut cancellation = cancel.clone();
    let result = tokio::select! {
        biased;
        _ = cancelled(&mut cancellation) => Err(BootstrapError::Cancelled),
        result = loading => result,
    };
    let result = sign_in.refused(provider, admission.cancellable(result));
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
