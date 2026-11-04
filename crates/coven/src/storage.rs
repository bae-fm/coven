//! Application storage setup, credential custody and connection control.

use crate::{
    error::{setup_error, unlock_error},
    *,
};
use coven_crypto::custody::{KeySession, PersonEntries};
use coven_foundation::progress::ProgressReport;
use coven_storage::{
    providers::{OAuthClients, OAuthFlow, StorageConnector},
    ConnectionCredentials, ProviderAuth, PublicationGate, SetupWaits, Storage, StorageConnection,
    StorageCredentials,
};
use coven_sync::{
    DeviceCredentials, LastPass, RestoreCodes, SequenceState, SequenceSync, SessionSignIn, SyncLoop,
};
use std::{
    collections::BTreeSet,
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

/// What a connection may publish: the sequence owner's gate, and the fresh
/// objects of a provider check setup is running (§20.8, E5).
pub(crate) struct Publications {
    state: Arc<SequenceState>,
    checks: std::sync::Mutex<BTreeSet<ObjectPath>>,
}

impl Publications {
    pub(crate) fn new(state: Arc<SequenceState>) -> Self {
        Self { state, checks: std::sync::Mutex::new(BTreeSet::new()) }
    }
    /// Permit the provider check's own object while `scope` runs.
    async fn check<T>(&self, path: &ObjectPath, scope: impl Future<Output = T>) -> T {
        self.checks.lock().expect("provider checks poisoned").insert(path.clone());
        let result = scope.await;
        self.checks.lock().expect("provider checks poisoned").remove(path);
        result
    }
}

impl PublicationGate for Publications {
    fn check(&self, path: &ObjectPath) -> Result<(), StorageError> {
        if self.checks.lock().expect("provider checks poisoned").contains(path) {
            return Ok(());
        }
        PublicationGate::check(self.state.as_ref(), path)
    }
}

/// A read-only handle publishes nothing.
pub(crate) struct NoPublications;

impl PublicationGate for NoPublications {
    fn check(&self, _: &ObjectPath) -> Result<(), StorageError> {
        Err(StorageFailure::PermissionDenied.with_source("a read-only handle publishes nothing"))
    }
}

/// One provider connection's sequence owner, loop and credential owner.
struct Live {
    sync: Arc<SequenceSync>,
    sync_loop: SyncLoop,
    codes: RestoreCodes,
}

impl Live {
    /// Finish the active pass and transfers, then release the connection.
    async fn close(self) -> Result<(), SyncError> {
        let result = self.sync_loop.close().await;
        self.codes.close().await;
        result
    }
}

/// Bootstrap retains its loaded provider before its sequence worker is needed.
enum ConnectionState {
    Released,
    Loaded(Arc<StorageConnection>),
    Active(Live),
}

struct ConnectionAttempt {
    epoch: u64,
    task: tokio::task::JoinHandle<()>,
}

pub(crate) struct StorageConnections {
    /// The credential owner while no connection exists.
    codes: RestoreCodes,
    database: coven_database::Database,
    files: coven_sync::Files,
    identity: Arc<KeySession<MemberIdentity>>,
    /// The person's entries, for each connection's sequence owner.
    person: Arc<PersonEntries>,
    credentials: Arc<DeviceCredentials>,
    publications: Arc<Publications>,
    directory: StoreDir,
    /// The recorded location, which with custody's credentials decides
    /// whether storage is set up here.
    settings: coven_storage::StorageSettings,
    waits: Arc<coven_sync::RetryWaits>,
    transfers: Arc<coven_storage::TransferBudget>,
    keys: std::sync::Mutex<Option<Arc<KeySession<StoreKeyring>>>>,
    connector: Arc<dyn StorageConnector>,
    oauth: Option<OAuthFlow>,
    /// The app's clients, which refresh a session sign-in.
    clients: Option<OAuthClients>,
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
    /// A failure the next close returns after closing, for a test of what
    /// follows a failed close.
    #[cfg(test)]
    close_failure: std::sync::Mutex<Option<SyncError>>,
}

impl StorageConnections {
    pub(crate) fn new(
        database: coven_database::Database,
        files: coven_sync::Files,
        identity: Arc<KeySession<MemberIdentity>>,
        person: Arc<PersonEntries>,
        credentials: Arc<DeviceCredentials>,
        publications: Arc<Publications>,
        directory: StoreDir,
        waits: Arc<coven_sync::RetryWaits>,
        transfers: Arc<coven_storage::TransferBudget>,
        keys: Arc<KeySession<StoreKeyring>>,
        connector: Arc<dyn StorageConnector>,
        oauth: Option<OAuthFlow>,
        clients: Option<OAuthClients>,
        authentication: Option<Arc<SessionSignIn>>,
        storage: Option<Arc<StorageConnection>>,
        configured: bool,
        device: DeviceId,
        ids: IdSourceRef,
        clock: ClockRef,
    ) -> Self {
        let codes = RestoreCodes::new(
            directory.id(),
            database.clone(),
            identity.clone(),
            credentials.clone(),
            coven_storage::StorageSettings::new(directory.clone()),
            None,
        );
        let expiry = coven_sync::InviteExpiry::start(
            database.clone(),
            clock.clone(),
            waits.clone(),
            publications.state.clone(),
        );
        Self {
            codes,
            expiry,
            database,
            files,
            identity,
            person,
            credentials,
            publications,
            settings: coven_storage::StorageSettings::new(directory.clone()),
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
            close_failure: std::sync::Mutex::new(None),
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
        self.configured.store(
            matches!(location, coven_database::RecordedLocation::Connected(_)),
            Ordering::SeqCst,
        );
        coven_sync::record_location(&self.database, location, finished)
            .await
            .map_err(SyncError::from)
    }

    /// Record the location as custody holds it now, as `record_location`
    /// does: after credentials change, or a connection is released.
    pub(crate) async fn record_custody(&self) -> Result<(), SyncError> {
        let location = self.credentials.location(&self.settings)?;
        self.record_location(location, None).await
    }

    /// Make an operation due now, as a step of an app's write: once the
    /// write commits, its wait ends in this session and a pass is asked
    /// for (E6).
    pub(crate) fn retry_pending_operation(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        operation: OperationId,
    ) -> Result<(), SyncError> {
        coven_sync::retry_pending_operation(
            sql,
            self.waits.clone(),
            self.publications.state.clone(),
            operation,
        )
    }

    /// Pause trailing uploads, or resume them, as a step of an app's write
    /// (E7).
    pub(crate) fn set_uploads_paused(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        paused: bool,
    ) -> Result<(), DbError> {
        self.files.set_uploads_paused(sql, paused)
    }

    /// Move files only on unmetered networks, or on any, as a step of an
    /// app's write (E7).
    pub(crate) fn set_unmetered_only(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        unmetered_only: bool,
    ) -> Result<(), DbError> {
        self.files.set_unmetered_only(sql, unmetered_only)
    }

    pub(crate) async fn storage_config(&self) -> Result<Option<StorageConfig>, SyncError> {
        let _call = self.lock_open().await?;
        self.codes.location().await
    }

    async fn lock_open(&self) -> Result<tokio::sync::MutexGuard<'_, ()>, KeyError> {
        let call = self.calls.lock().await;
        self.keys
            .lock()
            .expect("store keys lock poisoned")
            .as_ref()
            .ok_or(KeyError::StoreClosed)?;
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
        E: From<KeyError> + From<crate::task::Shutdown> + Send + 'static,
        F: Future<Output = Result<T, E>> + Send,
    {
        let owner = self.clone();
        let call = tokio::spawn(async move {
            let _call = owner.lock_open().await?;
            run(owner.clone()).await
        });
        crate::coven::completion("storage call", call).await?
    }

    /// Compose a sequence owner, its loop and its credential owner on
    /// `storage` as this store's connection.
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
        let keys = self
            .keys
            .lock()
            .expect("store keys lock poisoned")
            .clone()
            .ok_or(KeyError::StoreClosed)?;
        Ok(Arc::new(
            SequenceSync::open(
                self.database.clone(),
                storage,
                self.directory.clone(),
                self.device,
                keys,
                self.identity.clone(),
                self.person.clone(),
                Arc::new(coven_storage::StorageSettings::new(self.directory.clone())),
                self.publications.state.clone(),
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
        let codes = RestoreCodes::new(
            self.store(),
            self.database.clone(),
            self.identity.clone(),
            self.credentials.clone(),
            coven_storage::StorageSettings::new(self.directory.clone()),
            Some(sync.clone()),
        );
        let sync_loop = SyncLoop::new(sync.clone(), codes.clone());
        let started = if start { sync_loop.start().await } else { Ok(()) };
        *self.control.lock().expect("loop control poisoned") = Some(sync_loop.clone());
        *live = ConnectionState::Active(Live { sync, sync_loop, codes });
        started
    }

    /// Release the current connection after its active pass and transfers.
    /// No connection plans afterwards, so retries waiting for a pass are
    /// answered `NotSyncing`.
    async fn release(&self, live: &mut ConnectionState) -> Result<(), SyncError> {
        let stopped = self.waits.sync_stopped().await;
        self.control.lock().expect("loop control poisoned").take();
        let closed = match std::mem::replace(live, ConnectionState::Released) {
            ConnectionState::Active(previous) => previous.close().await,
            ConnectionState::Released | ConnectionState::Loaded(_) => Ok(()),
        };
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

    /// Reach the recorded location with custody's credentials.
    async fn connect(&self, live: &mut ConnectionState) -> Result<(), SyncError> {
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
                self.publications.clone(),
                self.waits.clone(),
                self.transfers.clone(),
            )
            .await?;
        storage.check_bucket(self.store()).await?;
        self.attach(live, storage).await
    }

    pub(crate) fn key_state(&self) -> Result<StoreKeyState, KeyError> {
        let held = self
            .keys
            .lock()
            .expect("store keys lock poisoned")
            .as_ref()
            .ok_or(KeyError::StoreClosed)?
            .read()?;
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
            if !matches!(provider, CloudProvider::Dropbox | CloudProvider::OneDrive) {
                return Err(OAuthError::Unavailable(provider).into());
            }
            owner.check_location(&storage).await?;
            let mut held = owner.authentication.lock().await;
            let (auth, sign_in) = match held.as_ref() {
                Some(sign_in) => {
                    crate::authentication::check_provider(sign_in, provider)?;
                    let auth = ProviderAuth::OAuth {
                        tokens: sign_in.tokens().map_err(setup_error)?,
                        custody: sign_in.clone(),
                    };
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
            let result = owner
                .setup_connection(storage, &device_name, auth, sign_in, waits, &mut progress)
                .await?;
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
                let storage = match owner.codes.location().await.map_err(setup_error)? {
                    Some(storage) => {
                        if !matches!(&storage, StorageConfig::CloudKit { container: recorded, .. } if *recorded == container) {
                            return Err(StorageSetupError::LocationChangeUnsupported);
                        }
                        storage
                    }
                    None => {
                        // Preparing reads the account and saves the zone, as it
                        // does for a zone that already exists: a refusal can be
                        // sent again.
                        let (connector, container, store) =
                            (owner.connector.as_ref(), container.as_str(), owner.store());
                        let preparing = waits.resend(owner.clock.as_ref(), move || {
                            connector.prepare_cloudkit(container, store)
                        });
                        setup_step::single(&mut progress, StorageSetupStep::PreparingZone, preparing)
                            .await?
                    }
                };
                owner
                    .setup_connection(
                        storage,
                        &device_name,
                        ProviderAuth::CloudKit,
                        None,
                        waits,
                        &mut progress,
                    )
                    .await
        }).await
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

    /// Check and publish `config` reached through `auth`. A new OAuth
    /// sign-in is `sign_in`, the session custody `auth` refreshes in until
    /// setup commits it to device custody. Custody, location and code commit
    /// only after the candidate connection has claimed what setup needs.
    /// The setup call's own requests, its checks and its claim, go through a
    /// connection made beside the store's that takes the call's `waits` and
    /// sends rate-limited requests again while they allow; the store's
    /// connection, which the sequence, the files and the sync loop keep,
    /// never does (§20.8).
    /// Its steps go to `progress`, from `Connecting` on.
    async fn setup_connection(
        &self,
        config: StorageConfig,
        device_name: &str,
        auth: ProviderAuth,
        sign_in: Option<Arc<SessionSignIn>>,
        waits: SetupWaits,
        progress: &mut ProgressReport<StorageSetupStep>,
    ) -> Result<StorageSetup, StorageSetupError> {
        progress.begin(StorageSetupStep::Connecting, 1);
        let credentials = auth.credentials();
        let storage = self
            .connector
            .connect(
                config.clone(),
                auth,
                self.device,
                self.publications.clone(),
                self.waits.clone(),
                self.transfers.clone(),
            )
            .await?;
        let setup = Arc::new(storage.during_setup(waits));
        let new_store = self.codes.location().await.map_err(setup_error)?.is_none();
        if !new_store {
            setup.check_bucket(self.store()).await?;
        }
        let path = ObjectPath::file(self.store(), self.device, FileId(self.ids.new_id()));
        let plaintext = b"coven storage check";
        let header = coven_format::file::FileHeader::new(plaintext.len() as u64);
        let key = coven_crypto::FileKey::generate()
            .map_err(|error| StorageSetupError::Internal(Box::new(error)))?;
        let mut bytes = header.encode().to_vec();
        bytes.extend(
            header
                .seal_chunk(&key, path.as_str(), 0, plaintext)
                .map_err(|error| StorageSetupError::Internal(Box::new(error)))?,
        );
        self.publications
            .check(&path, coven_storage::check_provider(setup.as_ref(), &path, &bytes))
            .await?;
        if new_store {
            setup.claim_bucket(self.store()).await?;
        }
        let access = member_access(&credentials, &setup).await?;
        progress.done(1);
        let mut live = self.live.lock().await;
        // The old provider remains installed until every fallible setup step
        // succeeds. Its stopped or started state survives a rejected candidate.
        if let ConnectionState::Active(previous) = &*live {
            previous.sync_loop.pause().await.map_err(setup_error)?;
        }
        let prepared = async {
            let (commit, restore_code) = self
                .codes
                .prepare_setup(
                    ConnectionCredentials { location: config.clone(), credentials },
                    sign_in,
                )
                .await?;
            let sync = self.sequence(storage.clone(), &live).await?;
            // The claim, and the catch-up an existing store needs, are the
            // setup call's own requests, sent on its connection.
            let claiming = Arc::new(sync.reconnect(setup.clone()));
            self.claim_setup(&claiming, access, device_name, progress).await?;
            // Claims may use file transfers; only the custody commit runs
            // inside the transfer exclusion needed to replace their provider.
            self.files.set_storage(Some(storage), async { commit().map(drop) }).await?;
            Ok::<_, SyncError>((sync, restore_code))
        }
        .await;
        let (sync, restore_code) = match prepared {
            Ok(prepared) => prepared,
            Err(mut error) => {
                if let ConnectionState::Active(previous) = &*live {
                    if let Err(cleanup) = previous.sync_loop.resume() {
                        error = SyncError::Cleanup {
                            operation: Box::new(error),
                            cleanup: Box::new(cleanup),
                        };
                    }
                }
                return Err(setup_error(error));
            }
        };
        let released = match std::mem::replace(&mut *live, ConnectionState::Released) {
            ConnectionState::Active(previous) => previous.close().await,
            _ => Ok(()),
        };
        let started = self.activate(&mut live, sync, true).await;
        // Custody and provider are committed, and setup read the store to its
        // end. Worker failures now belong to the sync state; setup must
        // return the connection it published. A row that can't be written
        // has failed the handle, which every later call reports; what it
        // would have recorded goes to the log.
        let location = coven_database::RecordedLocation::Connected(config.provider());
        if let Err(written) = self.record_location(location, Some(self.clock.now())).await {
            tracing::warn!("storage set up after setup can't be recorded: {written}");
        }
        let result = match (released, started) {
            (Ok(()), result) | (result, Ok(())) => result,
            (Err(operation), Err(cleanup)) => Err(SyncError::Cleanup {
                operation: Box::new(operation),
                cleanup: Box::new(cleanup),
            }),
        };
        if let Err(error) = result {
            let failure = LastPass::failed(error);
            if let Err(written) = self.show(failure.clone()).await {
                tracing::warn!(
                    ?failure,
                    "how sync failed after setup can't be recorded: {written}"
                );
            }
        }
        Ok(StorageSetup {
            connection: ConnectedStorage { storage: config, key_state: StoreKeyState::Available },
            restore_code,
        })
    }

    /// Create the store at slot 1 when this device's history has none, or
    /// catch up and record changed access for an existing device (§10.1, §10.3).
    async fn claim_setup(
        &self,
        sync: &Arc<SequenceSync>,
        access: coven_format::MemberAccess,
        device_name: &str,
        progress: &mut ProgressReport<StorageSetupStep>,
    ) -> Result<(), SyncError> {
        let state = self.database.store_state().await?;
        let member = self.identity.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        if state.store.is_none() {
            let held = self
                .keys
                .lock()
                .expect("store keys lock poisoned")
                .as_ref()
                .ok_or(KeyError::StoreClosed)?
                .read()?;
            let ring = match &held {
                Some(ring) => ring.clone(),
                None => {
                    StoreKeyring::new(coven_crypto::StoreKey::generate(KeyId(self.ids.new_id()))?)
                }
            };
            let key = ring.store_key_ids().next().ok_or(StorageFailure::MemberKeysMissing)?;
            let key_hash = ring.store_key(key)?.hash();
            // The creator's sequence owner seals slot 1 and its key copy
            // with the store key custody holds.
            self.persist_keys(&ring)?;
            progress.begin(StorageSetupStep::CreatingStore, 1);
            let result = sync
                .administer(coven_format::admin::StoreChange::CreateStore {
                    store: self.store(),
                    name: self.database.store_name().await.map_err(SyncError::from)?,
                    admin: member.keys().public_keys(),
                    access,
                    device_name: device_name.into(),
                    key,
                    key_hash,
                })
                .await;
            // A key no journaled claim can still use is taken back.
            if result.is_err() && held.is_none() && self.database.current_claim().await?.is_none() {
                self.forget_keys()?;
            }
            result?;
            progress.done(1);
            return Ok(());
        }
        let relayed = std::sync::Mutex::new(&mut *progress);
        sync.subscribe_progress()
            .forward(sync.unlock(), |load| {
                relayed.lock().expect("setup progress poisoned").relay(setup_step::loading(load))
            })
            .await?;
        let state = self.database.store_state().await?;
        if let Some(change) = access_change(&state, member.member(), access)? {
            let recording = sync.administer(change);
            setup_step::single(progress, StorageSetupStep::RecordingAccess, recording).await?;
        }
        Ok(())
    }

    fn persist_keys(&self, ring: &StoreKeyring) -> Result<(), KeyError> {
        self.keys
            .lock()
            .expect("store keys lock poisoned")
            .as_ref()
            .ok_or(KeyError::StoreClosed)?
            .persist(ring)
    }

    fn forget_keys(&self) -> Result<(), KeyError> {
        self.keys
            .lock()
            .expect("store keys lock poisoned")
            .as_ref()
            .ok_or(KeyError::StoreClosed)?
            .forget()
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
                    Err(cleanup) => SyncError::Cleanup {
                        operation: Box::new(error),
                        cleanup: Box::new(cleanup),
                    },
                };
                let error = match owner.publish_released(Ok(())).await {
                    Ok(()) => error,
                    Err(cleanup) => SyncError::Cleanup {
                        operation: Box::new(error),
                        cleanup: Box::new(cleanup),
                    },
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
                if let ConnectionState::Active(previous) = &*live {
                    previous.sync_loop.pause().await?;
                }
                if let Err(mut error) = owner
                    .files
                    .set_storage(None, async { owner.forget_keys().map_err(SyncError::from) })
                    .await
                {
                    if let ConnectionState::Active(previous) = &*live {
                        if let Err(cleanup) = previous.sync_loop.resume() {
                            error = SyncError::Cleanup {
                                operation: Box::new(error),
                                cleanup: Box::new(cleanup),
                            };
                        }
                    }
                    return Err(error);
                }
                let stopped = owner.waits.sync_stopped().await;
                owner.control.lock().expect("loop control poisoned").take();
                let released = match std::mem::replace(&mut *live, ConnectionState::Released) {
                    ConnectionState::Active(previous) => previous.close().await,
                    _ => Ok(()),
                };
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
                error => KeyError::Unavailable(Box::new(error)),
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
    let recorded =
        state.members.get(&member).filter(|value| !value.removed).ok_or(SyncError::Removed)?;
    Ok((!recorded.access.same_credential(&access))
        .then_some(coven_format::admin::StoreChange::SetAccess { member, access }))
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
