//! Application storage setup, credential custody and connection control.

use crate::{
    error::{setup_error, unlock_error},
    *,
};
use coven_crypto::custody::{KeySession, PersonEntries};
use coven_foundation::locks::lock_unpoisoned;
use coven_foundation::progress::ProgressReport;
use coven_storage::{
    providers::{OAuthExchange, OAuthFlow, StorageConnector},
    ConnectionCredentials, ProviderAuth, PublicationGate, SetupWaits, Storage, StorageConnection,
    StorageCredentials,
};
use coven_sync::{
    DeviceCredentials, LastPass, RestoreCodes, SequenceState, SequenceSync, SessionSignIn, SyncLoop,
};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

/// Whether this device's custody holds opened store keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreKeyState {
    /// Store keys are held by custody.
    Available,
    /// Store keys must be opened from this member's sealed copy.
    Locked,
}

/// The configured location and the result of opening this device's keys.
#[derive(Clone, Debug)]
pub struct ConnectedStorage {
    /// The connected provider location.
    pub storage: StorageConfig,
    /// This device's key availability.
    pub key_state: StoreKeyState,
}

/// Storage setup's committed connection and this member's restore code.
#[derive(Clone, Debug)]
pub struct StorageSetup {
    /// The location and availability of its store keys.
    pub connection: ConnectedStorage,
    /// The code for restoring this member on another installation.
    pub restore_code: SecretText,
}

/// Where a setup call reports its steps, for the handle to forward to the app.
pub(crate) type SetupProgress = tokio::sync::watch::Sender<Option<Progress<StorageSetupStep>>>;

/// Write what the sync state starts from as a store opens: whether a
/// storage location is recorded here and its credentials held, that no pass
/// moves, and, for a store joining or restoring read to its end, when sync
/// finished (E5); then end each invite whose lifetime ended while it was
/// closed (§13.2). Opening writes them before the handle exists, so no app
/// reads a move a process that closed left behind, or an invite that has
/// expired.
pub(crate) async fn record_opened(
    database: &coven_database::Database,
    clock: &ClockRef,
    location: coven_database::RecordedLocation<CloudProvider>,
    finished: Option<std::time::SystemTime>,
) -> Result<(), SyncError> {
    coven_sync::record_opened(database, location, finished).await.map_err(SyncError::from)?;
    coven_sync::InviteExpiry::opened(database, clock.now()).await.map_err(SyncError::from)
}

/// A read-only handle publishes nothing.
pub(crate) struct NoPublications;

impl PublicationGate for NoPublications {
    fn check(&self, _: &ObjectPath) -> Result<(), StorageError> {
        Err(StorageFailure::PermissionDenied.with_source("a read-only handle publishes nothing"))
    }
}

/// One provider connection's sequence owner and loop.
struct Live {
    sync: Arc<SequenceSync>,
    sync_loop: SyncLoop,
}

/// Bootstrap retains its loaded provider before its sequence worker is needed.
enum ConnectionState {
    Released,
    Loaded(Arc<StorageConnection>),
    Active(Live),
}

struct ConnectionAttempt {
    epoch: u64,
    task: crate::task::OwnedTask,
    /// Each time the attempt has taken everything that can wake its
    /// backoff and waits.
    #[cfg(test)]
    parks: coven_sync::Parks,
}

pub(crate) struct StorageConnections {
    /// The store's credential owner, connected or not; a change that needs
    /// the connection is given its sequence owner.
    codes: RestoreCodes,
    database: coven_database::Database,
    files: coven_sync::Files,
    identity: Arc<KeySession<MemberIdentity>>,
    /// The person's entries, for each connection's sequence owner.
    person: Arc<PersonEntries>,
    credentials: Arc<DeviceCredentials>,
    /// The sequence state whose gate every connection publishes through.
    state: Arc<SequenceState>,
    directory: StoreDir,
    waits: Arc<coven_sync::RetryWaits>,
    transfers: Arc<coven_storage::TransferBudget>,
    keys: std::sync::Mutex<Option<Arc<KeySession<StoreKeyring>>>>,
    connector: Arc<dyn StorageConnector>,
    oauth: Option<OAuthFlow>,
    /// The app's clients, which refresh a session sign-in.
    clients: Option<OAuthExchange>,
    /// The latest sign-in, held for this session until setup publishes it.
    authentication: tokio::sync::Mutex<Option<Arc<SessionSignIn>>>,
    /// The connection, if any; held while it changes or works.
    live: tokio::sync::Mutex<ConnectionState>,
    /// Ends invites as their lifetimes end, connected or not (§13.2).
    expiry: coven_sync::InviteExpiry,
    /// The connection's loop, for requests that return at once.
    control: std::sync::Mutex<Option<SyncLoop>>,
    /// Whether custody held storage credentials when last recorded: start
    /// connects only then, without reading custody before the first attempt.
    configured: AtomicBool,
    /// Changed by a new start, stop or close: a connection attempt or a
    /// queued stop acts only while no later request has superseded it.
    epoch: AtomicU64,
    /// Wakes a connection attempt's backoff when stopping is requested.
    stopped: tokio::sync::Notify,
    /// The started loop's connection attempts, awaited when closing.
    attempts: std::sync::Mutex<Option<ConnectionAttempt>>,
    device: DeviceId,
    ids: IdSourceRef,
    clock: ClockRef,
    calls: tokio::sync::Mutex<()>,
    /// How many times a call has found the call lock held and waited.
    #[cfg(test)]
    call_waits: tokio::sync::watch::Sender<usize>,
    /// A failure the next close returns after closing, for a test of what
    /// follows a failed close.
    #[cfg(test)]
    close_failure: std::sync::Mutex<Option<SyncError>>,
    /// This store's device-only entries, where a setup keeps its own
    /// credentials until it finishes (§11.6).
    keychain: Arc<coven_crypto::custody::StoreKeychain>,
    /// Where this session's storage setup stands (E5).
    setup: std::sync::Mutex<setup::SetupRun>,
    /// Wakes a setup checking the provider when the app cancels it.
    setup_cancelled: tokio::sync::Notify,
    /// What the next setup does at one of its stops, for a test.
    #[cfg(test)]
    setup_hook: std::sync::Mutex<Option<setup::SetupHook>>,
}

impl StorageConnections {
    pub(crate) fn new(
        database: coven_database::Database,
        files: coven_sync::Files,
        identity: Arc<KeySession<MemberIdentity>>,
        person: Arc<PersonEntries>,
        credentials: Arc<DeviceCredentials>,
        codes: RestoreCodes,
        state: Arc<SequenceState>,
        directory: StoreDir,
        waits: Arc<coven_sync::RetryWaits>,
        transfers: Arc<coven_storage::TransferBudget>,
        keys: Arc<KeySession<StoreKeyring>>,
        connector: Arc<dyn StorageConnector>,
        oauth: Option<OAuthFlow>,
        clients: Option<OAuthExchange>,
        authentication: Option<Arc<SessionSignIn>>,
        storage: Option<Arc<StorageConnection>>,
        configured: bool,
        keychain: Arc<coven_crypto::custody::StoreKeychain>,
        setup_finishing: bool,
        device: DeviceId,
        ids: IdSourceRef,
        clock: ClockRef,
    ) -> Self {
        let expiry = coven_sync::InviteExpiry::start(
            database.clone(),
            clock.clone(),
            waits.clone(),
            state.clone(),
        );
        Self {
            codes,
            expiry,
            database,
            files,
            identity,
            person,
            credentials,
            state,
            directory,
            waits,
            transfers,
            keys: std::sync::Mutex::new(Some(keys)),
            connector,
            oauth,
            clients,
            authentication: tokio::sync::Mutex::new(authentication),
            live: tokio::sync::Mutex::new(match storage {
                Some(storage) => ConnectionState::Loaded(storage),
                None => ConnectionState::Released,
            }),
            control: std::sync::Mutex::new(None),
            configured: AtomicBool::new(configured),
            epoch: AtomicU64::new(0),
            stopped: tokio::sync::Notify::new(),
            attempts: std::sync::Mutex::new(None),
            device,
            ids,
            clock,
            calls: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            call_waits: tokio::sync::watch::channel(0).0,
            #[cfg(test)]
            close_failure: std::sync::Mutex::new(None),
            keychain,
            setup: std::sync::Mutex::new(match setup_finishing {
                true => setup::SetupRun::Finishing,
                false => setup::SetupRun::Idle,
            }),
            setup_cancelled: tokio::sync::Notify::new(),
            #[cfg(test)]
            setup_hook: std::sync::Mutex::new(None),
        }
    }

    /// Initialize a loaded connection when a caller first needs its sequence owner.
    async fn ready(&self, live: &mut ConnectionState) -> Result<(), SyncError> {
        if let ConnectionState::Loaded(storage) = &*live {
            let storage = storage.clone();
            self.attach(live, storage).await?;
        }
        Ok(())
    }

    fn store(&self) -> StoreId {
        self.directory.id()
    }

    /// Write how sync failed outside a pass's end, starting or stopping, as
    /// the last pass's failure, in the step that fails (E5). A failed write
    /// has failed the store's handle with its cause, which the caller
    /// returns.
    pub(crate) async fn show(&self, failure: LastPass) -> Result<(), SyncError> {
        failure.record(&self.database).await.map_err(SyncError::from)
    }

    /// Write whether a storage location is recorded here and custody holds
    /// its credentials, and keep whether starting connects (E5). `finished`,
    /// for setup, is when it read the store to its end. A failed write has
    /// failed the store's handle with its cause, which the caller returns.
    pub(crate) async fn record_location(
        &self,
        location: coven_database::RecordedLocation<CloudProvider>,
        finished: Option<std::time::SystemTime>,
    ) -> Result<(), SyncError> {
        self.configured.store(location.is_connected(), Ordering::SeqCst);
        coven_sync::record_location(&self.database, location, finished)
            .await
            .map_err(SyncError::from)
    }

    /// Record the location as custody holds it now, as `record_location`
    /// does: after credentials change, or a connection is released.
    pub(crate) async fn record_custody(&self) -> Result<(), SyncError> {
        let location = self.credentials.location()?;
        self.record_location(location, None).await
    }

    pub(crate) async fn storage_config(&self) -> Result<Option<StorageConfig>, SyncError> {
        let _call = self.lock_open().await?;
        self.codes.location().await
    }

    /// Run `use_keys` on this session's store keys, holding the keys lock
    /// throughout so closing cannot take them away mid-use. `StoreClosed`
    /// once they are gone.
    fn with_keys<T>(
        &self,
        use_keys: impl FnOnce(&Arc<KeySession<StoreKeyring>>) -> Result<T, KeyError>,
    ) -> Result<T, KeyError> {
        let keys = lock_unpoisoned(&self.keys);
        use_keys(keys.as_ref().ok_or(KeyError::StoreClosed)?)
    }

    /// The call lock, which one call holds at a time; a test hears each
    /// call that finds it held and waits.
    pub(super) async fn lock_calls(&self) -> tokio::sync::MutexGuard<'_, ()> {
        #[cfg(test)]
        match self.calls.try_lock() {
            Ok(call) => return call,
            Err(_) => self.call_waits.send_modify(|waits| *waits += 1),
        }
        self.calls.lock().await
    }

    async fn lock_open(&self) -> Result<tokio::sync::MutexGuard<'_, ()>, KeyError> {
        let call = self.lock_calls().await;
        self.with_keys(|_| Ok(()))?;
        Ok(call)
    }

    // The task owns both the call and its lock until it finishes, even if the
    // awaiting app future is dropped. Sign-in deliberately does not spawn.
    async fn call<T, E, F>(
        self: &Arc<Self>,
        run: impl FnOnce(Arc<Self>) -> F + Send + 'static,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: From<KeyError>
            + From<crate::task::TaskFailed>
            + crate::task::CarriesBug
            + Send
            + 'static,
        F: Future<Output = Result<T, E>> + Send,
    {
        let owner = self.clone();
        let call = async move {
            let _call = owner.lock_open().await?;
            run(owner.clone()).await
        };
        crate::task::spawn_completing("storage call", self.database.bugs().clone(), call).await?
    }

    /// Compose a sequence owner and its loop on `storage` as this store's
    /// connection.
    async fn attach(
        &self,
        live: &mut ConnectionState,
        storage: Arc<StorageConnection>,
    ) -> Result<(), SyncError> {
        let sync = self.sequence(storage.clone(), live).await?;
        self.files.set_storage(Some(storage), std::future::ready(Ok(()))).await?;
        self.activate(live, sync, false).await
    }

    /// Compose a candidate without replacing the committed connection.
    async fn sequence(
        &self,
        storage: Arc<StorageConnection>,
        previous: &ConnectionState,
    ) -> Result<Arc<SequenceSync>, SyncError> {
        if let ConnectionState::Active(previous) = previous {
            return Ok(Arc::new(previous.sync.reconnect(storage)));
        }
        let keys = self.with_keys(|keys| Ok(keys.clone()))?;
        Ok(Arc::new(
            SequenceSync::open(
                self.database.clone(),
                storage,
                self.directory.clone(),
                self.device,
                keys,
                self.identity.clone(),
                self.person.clone(),
                self.state.clone(),
                self.clock.clone(),
                self.ids.clone(),
                self.waits.clone(),
                self.files.clone(),
            )
            .await?,
        ))
    }

    /// Install the sequence owner only after connection custody commits.
    async fn activate(
        &self,
        live: &mut ConnectionState,
        sync: Arc<SequenceSync>,
        start: bool,
    ) -> Result<(), SyncError> {
        let sync_loop = SyncLoop::new(sync.clone(), self.codes.clone());
        let started = if start { sync_loop.start().await } else { Ok(()) };
        *lock_unpoisoned(&self.control) = Some(sync_loop.clone());
        *live = ConnectionState::Active(Live { sync, sync_loop });
        started
    }

    /// Run `work` with the active connection's passes held: uploads run
    /// beside them, as during a connection change (§17.4). Work that fails
    /// or panics lets the passes run again, keeping a failure to resume them
    /// as cleanup; work that succeeds leaves them held for the caller to
    /// end. Unwound, a panic would leave them held for as long as the store
    /// stays open (E15).
    async fn with_passes_held<T>(
        live: &ConnectionState,
        work: impl std::future::Future<Output = Result<T, SyncError>>,
    ) -> Result<T, SyncError> {
        let ConnectionState::Active(previous) = live else {
            return work.await;
        };
        previous.sync_loop.pause().await?;
        let done = crate::task::caught("work with the passes held", work)
            .await
            .unwrap_or_else(|bug| Err(SyncError::Bug(bug)));
        done.map_err(|error| match previous.sync_loop.resume() {
            Ok(()) => error,
            Err(cleanup) => SyncError::cleanup(error, cleanup),
        })
    }

    /// Leave the connection released. An active one closes after its pass
    /// and transfers, and after the credential change running on it, even
    /// one whose caller stopped waiting.
    async fn close_live(&self, live: &mut ConnectionState) -> Result<(), SyncError> {
        match std::mem::replace(live, ConnectionState::Released) {
            ConnectionState::Active(previous) => {
                let result = previous.sync_loop.close().await;
                self.codes.settled().await;
                result
            }
            ConnectionState::Released | ConnectionState::Loaded(_) => Ok(()),
        }
    }

    /// End the sync loop: retries waiting for a pass are answered
    /// `NotSyncing`, as no connection plans afterwards, and the connection's
    /// owners close after its active pass. Answers whether the retries heard
    /// so, then whether the owners closed.
    async fn end_loop(
        &self,
        live: &mut ConnectionState,
    ) -> (Result<(), DbError>, Result<(), SyncError>) {
        let stopped = self.waits.sync_stopped().await;
        lock_unpoisoned(&self.control).take();
        (stopped, self.close_live(live).await)
    }

    /// Release the current connection after its active pass and transfers,
    /// with the files' provider client.
    async fn release(&self, live: &mut ConnectionState) -> Result<(), SyncError> {
        let (stopped, closed) = self.end_loop(live).await;
        let released = self.files.set_storage(None, std::future::ready(Ok(()))).await;
        closed.and(released).and(stopped.map_err(SyncError::from))
    }

    /// Connect with custody's credentials unless this session already has a
    /// connection, without starting the loop.
    pub(crate) async fn connected(&self) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        if let ConnectionState::Active(_) = &*live {
            return Ok(());
        }
        self.connect(&mut live).await
    }

    /// Reach the recorded location with custody's credentials, or finish
    /// the setup a crash cut past its point of no return (§11.6).
    async fn connect(&self, live: &mut ConnectionState) -> Result<(), SyncError> {
        if self.setup_finishing() {
            if let Some(journal) = coven_sync::SetupJournal::running(&self.database).await? {
                return self.resume_setup(live, journal).await;
            }
        }
        if let ConnectionState::Loaded(storage) = live {
            let storage = storage.clone();
            return self.attach(live, storage).await;
        }
        let data = self.codes.connection().await?.ok_or(SyncError::NoStorage)?;
        let storage = self
            .connector
            .connect(
                data.location,
                self.codes.provider_auth(data.credentials),
                self.device,
                self.state.clone(),
                self.waits.clone(),
                self.transfers.clone(),
            )
            .await?;
        storage.check_bucket(self.store()).await?;
        self.attach(live, storage).await
    }

    pub(crate) fn key_state(&self) -> Result<StoreKeyState, KeyError> {
        let held = self.with_keys(|keys| keys.read())?;
        Ok(match held {
            Some(_) => StoreKeyState::Available,
            None => StoreKeyState::Locked,
        })
    }

    pub(crate) async fn authenticate(&self, access: OAuthAccess) -> Result<(), StorageSetupError> {
        let _call = self.lock_open().await?;
        let flow = self.oauth.as_ref().ok_or(OAuthError::Unavailable(access.provider()))?;
        let tokens = flow.authenticate(access).await?;
        *self.authentication.lock().await = Some(SessionSignIn::new(tokens, self.clients.clone()));
        Ok(())
    }

    pub(crate) async fn setup_oauth(
        self: &Arc<Self>,
        storage: StorageConfig,
        device_name: &str,
        progress: SetupProgress,
    ) -> Result<StorageSetup, StorageSetupError> {
        let device_name = device_name.to_owned();
        self.call(move |owner| async move {
            let mut progress = ProgressReport::new(progress, owner.clock.clone());
            let waits = SetupWaits::default();
            let provider = storage.provider();
            if !provider.signs_in_with_oauth() {
                return Err(OAuthError::Unavailable(provider).into());
            }
            owner.check_location(&storage).await?;
            let mut held = owner.authentication.lock().await;
            let (auth, sign_in) = match held.as_ref() {
                Some(sign_in) => {
                    let auth = crate::authentication::held_auth(sign_in, provider)?;
                    (auth, Some(sign_in.clone()))
                }
                None => {
                    let data = owner
                        .codes
                        .connection()
                        .await
                        .map_err(setup_error)?
                        .ok_or(OAuthError::Reauthorize(provider))?;
                    if data.location.provider() != provider {
                        return Err(OAuthError::Reauthorize(provider).into());
                    }
                    (owner.codes.provider_auth(data.credentials), None)
                }
            };
            let journal = owner.begin_setup(&device_name, &auth.credentials()).await?;
            let setup = owner.run_setup(journal, async |journal| {
                let (connector, cooldown) = (owner.connector.as_ref(), &owner.waits);
                let (location, signed_in) = (&storage, &auth);
                let reading = waits.resend(owner.clock.as_ref(), move || {
                    crate::authentication::label_location(
                        connector,
                        location.clone(),
                        signed_in.clone(),
                        cooldown.clone(),
                    )
                });
                let storage =
                    setup_step::single(&mut progress, StorageSetupStep::ReadingAccount, reading)
                        .await?;
                owner.setup_connection(journal, storage, auth, sign_in, waits, &mut progress).await
            });
            let result = setup.await?;
            held.take();
            Ok(result)
        })
        .await
    }

    pub(crate) async fn setup_cloudkit(
        self: &Arc<Self>,
        container: &str,
        device_name: &str,
        progress: SetupProgress,
    ) -> Result<StorageSetup, StorageSetupError> {
        let container = container.to_owned();
        let device_name = device_name.to_owned();
        self.call(move |owner| async move {
            let mut progress = ProgressReport::new(progress, owner.clock.clone());
            let waits = SetupWaits::default();
            let recorded = owner.codes.location().await.map_err(setup_error)?;
            if recorded.as_ref().is_some_and(|recorded| {
                !matches!(recorded, StorageConfig::CloudKit { container: kept, .. } if *kept == container)
            }) {
                return Err(StorageSetupError::LocationChangeUnsupported);
            }
            let journal = owner.begin_setup(&device_name, &StorageCredentials::CloudKit).await?;
            owner
                .run_setup(journal, async |journal| {
                    let storage = match recorded {
                        Some(storage) => storage,
                        None => {
                            // Preparing reads the account and saves the zone,
                            // as it does for a zone that already exists: a
                            // refusal can be sent again.
                            let (connector, container, store) =
                                (owner.connector.as_ref(), container.as_str(), owner.store());
                            let preparing = waits.resend(owner.clock.as_ref(), move || {
                                connector.prepare_cloudkit(container, store)
                            });
                            setup_step::single(
                                &mut progress,
                                StorageSetupStep::PreparingZone,
                                preparing,
                            )
                            .await?
                        }
                    };
                    owner
                        .setup_connection(
                            journal,
                            storage,
                            ProviderAuth::CloudKit,
                            None,
                            waits,
                            &mut progress,
                        )
                        .await
                })
                .await
        })
        .await
    }

    async fn check_location(&self, config: &StorageConfig) -> Result<(), StorageSetupError> {
        if self
            .codes
            .location()
            .await
            .map_err(setup_error)?
            .is_some_and(|recorded| !recorded.same_location(config))
        {
            return Err(StorageSetupError::LocationChangeUnsupported);
        }
        Ok(())
    }

    fn persist_keys(&self, ring: &StoreKeyring) -> Result<(), KeyError> {
        self.with_keys(|keys| keys.persist(ring))
    }

    fn forget_keys(&self) -> Result<(), KeyError> {
        self.with_keys(|keys| keys.forget())
    }

    pub(crate) async fn unlock(self: &Arc<Self>) -> Result<ConnectedStorage, StoreKeyUnlockError> {
        self.call(|owner| async move {
            let data = owner
                .codes
                .connection()
                .await
                .map_err(unlock_error)?
                .ok_or(StoreKeyUnlockError::NoStorage)?;
            let config = data.location.clone();
            let mut live = owner.live.lock().await;
            owner.release(&mut live).await.map_err(unlock_error)?;
            owner.connect(&mut live).await.map_err(unlock_error)?;
            let unlocked = match &*live {
                ConnectionState::Active(candidate) => candidate.sync.unlock().await,
                _ => Err(SyncError::NoStorage),
            };
            if let Err(error) = unlocked {
                let error = match owner.release(&mut live).await {
                    Ok(()) => error,
                    Err(cleanup) => SyncError::cleanup(error, cleanup),
                };
                let error = match owner.publish_released(Ok(())).await {
                    Ok(()) => error,
                    Err(cleanup) => SyncError::cleanup(error, cleanup),
                };
                return Err(unlock_error(error));
            }
            // Connected without syncing.
            owner.publish_released(Ok(())).await.map_err(unlock_error)?;
            Ok(ConnectedStorage { storage: config, key_state: StoreKeyState::Available })
        })
        .await
    }

    pub(crate) async fn forget_store_keys(self: &Arc<Self>) -> Result<(), KeyError> {
        self.call(|owner| async move {
            let mut live = owner.live.lock().await;
            let result = async {
                Self::with_passes_held(
                    &live,
                    owner
                        .files
                        .set_storage(None, async { owner.forget_keys().map_err(SyncError::from) }),
                )
                .await?;
                let (stopped, released) = owner.end_loop(&mut live).await;
                // Forgetting succeeded. A worker release failure is recorded
                // as the last pass's failure; no later custody read can turn
                // removal into an error. Stopping writes nothing else the
                // sync state reads. A failed write has failed the handle.
                if let Err(error) = released {
                    owner.show(LastPass::failed(error)).await?;
                }
                Ok(stopped?)
            }
            .await;
            result.map_err(|error| match error {
                SyncError::SecureStorage(error) => error,
                SyncError::Database(DbError::StoreClosed) => KeyError::StoreClosed,
                // A bug is the call's own `Bug`, never custody that is
                // unavailable (E15).
                error => match crate::task::CarriesBug::bug(&error) {
                    Some(bug) => KeyError::Bug(bug.clone()),
                    None => KeyError::Unavailable(Box::new(error)),
                },
            })
        })
        .await
    }
}

#[path = "storage_control.rs"]
mod control;

#[path = "storage_folders.rs"]
mod folders;

#[path = "storage_s3.rs"]
mod s3;

#[path = "storage_setup_step.rs"]
mod setup_step;
pub use setup_step::StorageSetupStep;

#[path = "storage_setup.rs"]
mod setup;
pub(crate) use setup::settle_opened;
pub use setup::SetupCancellation;
#[cfg(test)]
pub(crate) use setup::SetupHook;

#[path = "storage_admin.rs"]
mod admin;
pub use admin::GuidedAccessKey;

#[path = "storage_operations.rs"]
mod operations;

/// How this member reaches storage over `storage` signed in with
/// `credentials`: the S3 key, or the signed-in account (§20.2).
pub(crate) async fn member_access(
    credentials: &StorageCredentials,
    storage: &StorageConnection,
) -> Result<coven_format::MemberAccess, coven_storage::StorageError> {
    Ok(match credentials {
        StorageCredentials::S3(keys) => {
            coven_format::MemberAccess::S3AccessKey { access_key_id: keys.access_key_id.clone() }
        }
        _ => coven_format::MemberAccess::ProviderAccount(storage.account().await?),
    })
}

/// The entry recording `access` as `member`'s own, when history records
/// another: setup and restore keep the recorded access the one the member
/// uses (§14.2).
pub(crate) fn access_change(
    state: &coven_database::StoreState,
    member: MemberId,
    access: coven_format::MemberAccess,
) -> Result<Option<coven_format::admin::StoreChange>, SyncError> {
    let recorded = state.active_member(&member).ok_or(SyncError::Removed)?;
    Ok((!recorded.access.same_credential(&access))
        .then_some(coven_format::admin::StoreChange::SetAccess { member, access }))
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
