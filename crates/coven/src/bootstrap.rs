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
};
use coven_sync::{HeldIntervals, Parks, SequenceSync, SyncOften};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

#[path = "bootstrap_authentication.rs"]
mod authentication;
use authentication::{account_sign_in, join_sign_in, provider_auth, SignInCustody};
use coven_sync::SessionSignIn;

#[path = "bootstrap_commit.rs"]
mod commit;

#[path = "bootstrap_join.rs"]
mod join;

#[path = "bootstrap_admitted.rs"]
mod admitted;

#[path = "bootstrap_replace.rs"]
mod replace;
pub(super) use replace::{restore_device, resume_replacement};

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

#[path = "bootstrap_unfinished.rs"]
mod unfinished;
use unfinished::{begin_installation, kept, Kept};

#[path = "bootstrap_end.rs"]
mod end;
use end::{record_ending, remove_unfinished};

#[path = "bootstrap_error.rs"]
mod error;
pub use error::BootstrapError;

#[path = "bootstrap_status.rs"]
mod status;
pub use status::BootstrapStatus;
use status::{loading, open_installation};

/// Accept provider access, retain one signed request across restarts, and wait
/// for approval. Returns the open store, or None when declined or expired.
/// `joiner` is the new person with the name they typed, or a recovery, as the
/// invite's `InviteInfo::kind` says; the other kind fails with
/// `CodeError::WrongJoiner` before anything changes. Dropping the future
/// before the join finds its admission preserves the unfinished join for a
/// subsequent call with the same invite, device name and person, or
/// `resume_join`. On Dropbox and
/// OneDrive the builder's sign-in is kept on the device beside the saved code
/// until the join ends, and a later call without one uses it (§13.2).
/// `control` cancels the join or asks for a check for approval at once.
/// Cancelling before the request may have been sent removes the unpublished
/// installation; after, the call ends with `BootstrapError::JoinRequestSent`
/// and keeps it, as dropping does. Once the join finds its admission it
/// publishes the store, a cancel notwithstanding, then loads it, reporting
/// `Loading`; a failure while it loads returns `BootstrapError::Published`
/// with the open handle (§13.2). A store whose unfinished installation
/// holds a restore's record is refused with `StoreLockError::BootstrapPending`
/// before anything changes (§13.4).
pub async fn join_with_invite(
    builder: CovenBuilder,
    code: &str,
    joiner: Joiner,
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
            terms: RequestTerms::Given { device_name: device_name.into(), joiner },
            held: control.often.subscribe(),
            parks: control.parks.clone(),
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
    for store in builder.directories.unfinished_first_installations().await? {
        // An unfinished restore saves no code; an installation that keeps
        // nothing is reclaimed.
        let entries = StoreKeychain::new(keychain.clone(), store);
        let Kept::Join(text) = kept(builder, &entries, store).await? else {
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

/// Continue an unfinished join from its saved code and the device name and
/// person its saved request carries (`CovenBuilder::resume_join`).
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
            terms: RequestTerms::Saved,
            held: control.often.subscribe(),
            parks: control.parks.clone(),
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
    CodeError::require_store(store, code.store)?;
    Ok(Some(code))
}

/// The app's hold on a running join: cancel it, or ask it to check for
/// approval often while the app shows the wait.
pub struct JoinControl {
    cancel: watch::Sender<bool>,
    often: SyncOften,
    /// Where the join's wait parks, for tests.
    parks: Parks,
}

impl JoinControl {
    /// A control no join has used yet.
    pub fn new() -> Self {
        Self { cancel: watch::channel(false).0, often: SyncOften::new(), parks: Parks::new() }
    }

    /// Cancel the join this control was passed to.
    pub fn cancel(&self) {
        self.cancel.send_replace(true);
    }

    /// Have the join check for approval at most `interval` after its last
    /// check ended, until the guard drops; a check already that overdue runs
    /// at once. With several guards held, the shortest interval wins. Checks
    /// that find the request waiting still count toward the backoff, and a
    /// provider cooldown still holds every check (§13.2).
    pub fn check_often(&self, interval: Duration) -> SyncOftenGuard {
        self.often.hold(interval)
    }

    /// Wait until the join's wait parks in a poll begun after this call,
    /// having seen all the caller did before.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn parked_from_now(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        self.parks.parked_from_now()
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
        terms: RequestTerms,
        /// The intervals the app asks the join to check at.
        held: HeldIntervals,
        parks: Parks,
    },
}

/// The device name and person a join's request carries.
enum RequestTerms {
    /// What the app passed, which a saved request must repeat.
    Given { device_name: String, joiner: Joiner },
    /// What the saved request carries: the join resumes from it.
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
    #[cfg(test)]
    let recorder = builder.loading_recorder.clone();
    // A deletion of the store a cut call left recorded still holds its id
    // (§19.3): it is finished first.
    if builder.directories.deletion_recorded(id).await.map_err(BootstrapDirectoryError::from)? {
        let directory = builder.directories.store_dir(&id);
        let keys = StoreKeychain::new(keychain.clone(), id);
        crate::coven::finish_deletion::<BootstrapError>(&directory, keys).await?;
    }
    let pending = begin_installation(&builder, id, &device_entries).await?;
    // The store's one unfinished installation lists one call. A restore on
    // a waiting join's installation, or a join on a recorded restore's,
    // would remove the other's listing when it ends without the store, with
    // its request or registration possibly in storage (§13.4).
    let other = match &request {
        BootstrapRequest::Restore { .. } => device_entries.invite_code()?.is_some(),
        BootstrapRequest::Join { .. } => device_entries.restore_record()?.is_some(),
    };
    if other {
        return Err(BootstrapDirectoryError::from(StoreLockError::BootstrapPending(id)).into());
    }
    // A join that decided to end and stopped part way through deleting
    // what it kept finishes that, without connecting: the sign-in it would
    // connect with may be gone already (§13.4).
    if let BootstrapRequest::Join { code, .. } = &request {
        if coven_sync::ended_for(&pending.directory(), code)? {
            return remove_unfinished(&device_entries, &pending, Ok(None));
        }
    }
    let result =
        prepare_and_load(&mut builder, Installing::New(&pending), &request, None, &status, cancel)
            .await;
    match result {
        Ok(Some(Loaded {
            code,
            location,
            provider,
            credentials,
            sign_in,
            ring,
            database,
            storage,
            state,
            waits,
            invitee,
        })) => {
            let requests = storage.requests();
            status(BootstrapStatus::Committing);
            // The saved code's name, read before the final steps, which no
            // await separates; so is the status the published store opens
            // with (E5). A join publishes before it loads, with the name its
            // invite carries (§13.2).
            let name = match &request {
                BootstrapRequest::Join { code, .. } => {
                    commit::joined_name(&database, code, invitee).await
                }
                BootstrapRequest::Restore { .. } => commit::loaded_name(&database).await,
            };
            let name = match name {
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
                // A loaded restore registered its device: a cancel can't take
                // it back, and ends the call (§13.1). A join that found its
                // admission publishes, cancelled or not (§13.2).
                if matches!(request, BootstrapRequest::Restore { .. })
                    && check_cancel(cancel).is_err()
                {
                    return Err(BootstrapError::RestoreRegistered);
                }
                builder.authentication.take();
                let credentials = match &sign_in {
                    // A join's kept sign-in is already this store's device
                    // credentials: the handle refreshes through the same
                    // custody as the connection it continues.
                    SignInCustody::Device { credentials, .. } => Some(credentials.clone()),
                    _ => None,
                };
                let owners = builder.owners(
                    pending.directory(),
                    Arc::new(StoreKeychain::new(keychain, id)),
                    crate::builder::Continuing::Loaded { state, storage, credentials },
                )?;
                Ok((owners, name))
            }) {
                // No cancellable await separates final custody, publication
                // and the handle.
                Ok((owners, name)) => {
                    let publishing = clock.elapsed();
                    let published = commit::publish_bootstrap(
                        &pending,
                        code,
                        location,
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
                Ok(handle) => match &request {
                    BootstrapRequest::Join { .. } => {
                        let (progress, watch) = watch::channel(None);
                        #[cfg(test)]
                        if let Some(recorder) = &recorder {
                            recorder.watch(watch.clone());
                        }
                        admitted::load_joined(handle, &status, &clock, progress, watch)
                            .await
                            .map(Some)
                    }
                    BootstrapRequest::Restore { .. } => Ok(Some(handle)),
                },
                Err(error @ BootstrapError::Published { .. }) => Err(error),
                // A loaded join or restore is past cancelling: it stays for
                // the next call.
                Err(error) => combine(Err(error), close_database(&database).await),
            }
        }
        // Only a join ends without a store: declined or expired.
        Ok(None) => match record_ending(&request, &pending) {
            Ok(()) => remove_unfinished(&device_entries, &pending, Ok(None)),
            Err(error) => Err(error),
        },
        Err(error) => match &request {
            BootstrapRequest::Restore { .. } => {
                restore::ended(&builder, &pending, &device_entries, error).await
            }
            BootstrapRequest::Join { .. } if matches!(error, BootstrapError::Cancelled) => {
                match record_ending(&request, &pending) {
                    Ok(()) => remove_unfinished(&device_entries, &pending, Err(error)),
                    Err(recording) => combine(Err(error), Err(recording)),
                }
            }
            // Before its admission nothing ends a join but its outcome: it
            // stays listed for the next call (§13.2).
            BootstrapRequest::Join { .. } => Err(error),
        },
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

    /// Whether a join may have accepted the share without sending its
    /// request.
    fn accepted(&self) -> bool {
        matches!(self, Self::Joining(joining) if joining.accepted())
    }

    /// A join cancelled before sending takes back the acceptance it recorded
    /// before it is removed. An undo that fails fails the cancel, and the
    /// join stays listed for another (§13.2).
    async fn leave_cancelled<T>(
        &self,
        sync: &SequenceSync,
        result: Result<T, BootstrapError>,
    ) -> Result<T, BootstrapError> {
        match (result, self) {
            (Err(BootstrapError::Cancelled), Self::Joining(joining)) if joining.accepted() => {
                combine(Err(BootstrapError::Cancelled), joining.leave(sync).await)
            }
            (result, _) => result,
        }
    }
}

/// What a new installation does once what it reads from storage is read.
enum Finishing {
    /// A join that found its admission, which registered its device,
    /// publishes the store, recording `labelled`, the location as the
    /// joining account names it; the store loads after (§13.2).
    Publish { labelled: StorageConfig },
    /// A restore or replacement, loaded, registers its device unless an
    /// interrupted call's registration is `registered` already, and records
    /// `access`, the access it reaches storage with (§13.1, §19.3).
    Register { access: coven_format::MemberAccess, registered: bool, labelled: StorageConfig },
}

/// A loaded store awaiting publication, with what reaches its storage.
struct Loaded {
    code: RestoreCode,
    /// The location as loading's connection labelled it, which the commit
    /// records (§20.2).
    location: StorageConfig,
    /// The provider of the location loaded from.
    provider: CloudProvider,
    credentials: StorageCredentials,
    sign_in: SignInCustody,
    ring: StoreKeyring,
    database: Database,
    storage: Arc<coven_storage::StorageConnection>,
    /// The sequence state the handle continues, which gates the connection.
    state: Arc<coven_sync::SequenceState>,
    waits: Arc<coven_sync::RetryWaits>,
    /// The new person's name a join's request carried, for the store it
    /// publishes to keep; none for a restore and for a recovery.
    invitee: Option<String>,
}

/// Load the store into `pending` and register its device. A join loads
/// nothing here: once it finds its admission, which registered its device,
/// it returns the store to publish, which loads after (§13.2). A replacement
/// runs the app's `prepare` step on its loaded installation, then keeps what
/// finishing needs and records itself, before it registers (§19.3).
async fn prepare_and_load(
    builder: &mut CovenBuilder,
    pending: Installing<'_>,
    request: &BootstrapRequest,
    prepare: Option<replace::Prepare>,
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
        BootstrapRequest::Join { code, terms, held, parks } => {
            let (joining, data, sign_in) = join::prepare_request(
                builder,
                &directory,
                code,
                terms,
                held.clone(),
                parks.clone(),
            )?;
            (
                joining.identity(),
                data,
                sign_in,
                joining.device_name().to_owned(),
                Admission::Joining(joining),
            )
        }
    };
    // A join that may have accepted the share connects even when cancelled
    // already, so its cancel can take the acceptance back (§13.2).
    if !admission.accepted() {
        admission.cancellable(check_cancel(cancel))?;
    }
    // A restore records itself before it registers: a new device's (§13.1),
    // and a replacement of this device's installation (§19.3).
    let recording = match (request, pending) {
        (BootstrapRequest::Restore { code, name, .. }, Installing::New(_)) => {
            Some(restore::Recording::new(builder, code.store, name)?)
        }
        (BootstrapRequest::Restore { code, name, .. }, Installing::Replacing(_)) => {
            Some(restore::Recording::replacement(builder, code.store, name)?)
        }
        _ => None,
    };
    let mut prepare = prepare;
    let provider = data.location.provider();
    let settings = directory.settings().map_err(CovenError::from)?;
    let state = Arc::new(coven_sync::SequenceState::new());
    let ring = Arc::new(KeySession::open(Arc::new(InMemoryCustody::<StoreKeyring>::empty()))?);
    let identity = Arc::new(KeySession::open(Arc::new(InMemoryCustody::new(member.clone())))?);
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
                state.clone(),
                waits.clone(),
                builder.transfers.clone(),
            )
            .await
            .map_err(SyncError::from)?;
        if matches!(admission, Admission::Restoring) {
            storage.check_location_claim(settings.id).await.map_err(SyncError::from)?;
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
    // The location as the joining account names it, read once the share is
    // accepted (§20.2): the store a join publishes records it, and no storage
    // request comes between finding the admission and publishing (§13.2).
    let mut accepted_label = None;
    let loading = async {
        if let Admission::Joining(joining) = &mut admission {
            if !joining.accept(&sync, status).await? {
                return Ok(None);
            }
            let labelled = storage.labelled_config().await.map_err(SyncError::from)?;
            accepted_label = Some(labelled.clone());
            if !joining.admit(&sync, &database, status).await? {
                return Ok(None);
            }
            // Its admission found, which registered its device, the join
            // publishes the store, which loads after (§13.2): nothing is
            // left to register, and no access to record.
            return Ok(Some(Finishing::Publish { labelled }));
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
        // The folder's label as this member's account names it (§20.2).
        let labelled = storage.labelled_config().await.map_err(SyncError::from)?;
        let registered = database
            .store_state()
            .await?
            .devices
            .get(&settings.device_id)
            .is_some_and(|device| device.member == member.member());
        // A restore checks the access it reaches storage with before it
        // registers, and records it after, as setup does, so the member's
        // recorded access is the one in use (§13.1).
        status(BootstrapStatus::Registering);
        let access = crate::storage::member_access(&data.credentials, &storage)
            .await
            .map_err(SyncError::from)?;
        sync.check_access(&access).await?;
        Ok::<_, BootstrapError>(Some(Finishing::Register { access, registered, labelled }))
    };
    let mut cancellation = cancel.clone();
    let loaded = tokio::select! {
        biased;
        _ = cancelled(&mut cancellation) => Err(BootstrapError::Cancelled),
        result = loading => result,
    };
    // A join whose keys opened in this call has found its admission, which
    // registered its device: it publishes the store, cancelled or not, since
    // nothing is left for a cancel to take back (§13.2).
    let loaded = match (loaded, &admission, accepted_label) {
        (Err(BootstrapError::Cancelled), Admission::Joining(_), Some(labelled))
            if ring.read()?.is_some() =>
        {
            Ok(Some(Finishing::Publish { labelled }))
        }
        (loaded, ..) => loaded,
    };
    let result = match loaded {
        // A join that found its admission has nothing left to write.
        Ok(Some(Finishing::Publish { labelled })) => {
            data.location = labelled;
            Ok(true)
        }
        Ok(Some(Finishing::Register { access, registered, labelled })) => {
            data.location = labelled;
            // The work that writes to storage. A cancel stops it only between
            // its steps: one under way, a registration on its way above all,
            // runs to its end first (§11.6).
            let registering = async {
                if !registered {
                    check_cancel(cancel)?;
                    match (replacement, &recording) {
                        (Some(replacement), Some(recording)) => {
                            sign_in = replace::record_replacement(
                                replacement,
                                recording,
                                &database,
                                prepare.take(),
                                (&member, settings.id, &data),
                                sign_in.clone(),
                            )
                            .await?;
                        }
                        (None, Some(recording)) => {
                            sign_in =
                                recording.record(&member, settings.id, &data, sign_in.clone())?;
                        }
                        _ => {}
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
                let state = database.store_state().await?;
                if let Some(change) =
                    crate::storage::access_change(&state, member.member(), access)?
                {
                    check_cancel(cancel)?;
                    sync.administer(change).await?;
                }
                // The notice commits before any handle is published (E5).
                if let Some(replacement) = replacement {
                    check_cancel(cancel)?;
                    #[cfg(test)]
                    if builder.fail_device_reset_notice {
                        database.test_fail_device_reset().await.map_err(CovenError::from)?;
                    }
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
    let result = admission.leave_cancelled(&sync, admission.cancellable(result)).await;
    let result = sign_in.refused(provider, result);
    files.close().await;
    drop(sync);
    let result = result.and_then(|loaded| {
        if !loaded {
            return Ok(None);
        }
        let ring = ring.read()?.ok_or(SyncError::from(StorageFailure::MemberKeysMissing))?;
        Ok(Some((
            coven_sync::location_code(settings.id, &member, data.location.clone())?,
            data.location,
            data.credentials,
            ring,
        )))
    });
    let invitee = match &admission {
        Admission::Joining(joining) => joining.invitee(),
        Admission::Restoring => None,
    };
    match result {
        Ok(Some((code, location, credentials, ring))) => Ok(Some(Loaded {
            code,
            location,
            provider,
            credentials,
            sign_in,
            ring,
            database,
            storage,
            state,
            waits,
            invitee,
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
