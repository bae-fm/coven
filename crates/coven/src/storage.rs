//! Application storage setup, credential custody and connection control.

use crate::{
    error::{setup_error, unlock_error},
    *,
};
use coven_crypto::custody::KeySession;
use coven_storage::{
    providers::{OAuthClients, OAuthFlow, StorageConnector},
    ConnectionCredentials, ProviderAuth, PublicationGate, Storage, StorageConnection,
    StorageCredentials,
};
use coven_sync::{
    DeviceCredentials, RestoreCodes, SequenceSync, SessionSignIn, SyncLoop, SyncState,
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

/// What a connection may publish: the sequence owner's gate, and the fresh
/// objects of a provider check setup is running (§4, E5).
pub(crate) struct Publications {
    state: Arc<SyncState>,
    checks: std::sync::Mutex<BTreeSet<ObjectPath>>,
}

impl Publications {
    pub(crate) fn new(state: Arc<SyncState>) -> Self {
        Self {
            state,
            checks: std::sync::Mutex::new(BTreeSet::new()),
        }
    }
    /// Publish what the app sees of sync.
    fn publish(&self, status: SyncStatus) {
        self.state.publish(status);
    }
    /// Permit the provider check's own object while `scope` runs.
    async fn check<T>(&self, path: &ObjectPath, scope: impl Future<Output = T>) -> T {
        self.checks
            .lock()
            .expect("provider checks poisoned")
            .insert(path.clone());
        let result = scope.await;
        self.checks
            .lock()
            .expect("provider checks poisoned")
            .remove(path);
        result
    }
}

impl PublicationGate for Publications {
    fn check(&self, path: &ObjectPath) -> Result<(), StorageError> {
        if self
            .checks
            .lock()
            .expect("provider checks poisoned")
            .contains(path)
        {
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
enum Connection {
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
    credentials: Arc<DeviceCredentials>,
    publications: Arc<Publications>,
    directory: StoreDir,
    /// The recorded location, whose credentials decide `Stopped` or `Disconnected`.
    settings: coven_storage::StorageSettings,
    waits: Arc<coven_sync::RetryWaits>,
    transfers: Arc<coven_storage::TransferBudget>,
    initial_name: String,
    keys: std::sync::Mutex<Option<Arc<KeySession<StoreKeyring>>>>,
    connector: Arc<dyn StorageConnector>,
    oauth: Option<OAuthFlow>,
    /// The app's clients, which refresh a session sign-in.
    clients: Option<OAuthClients>,
    /// The latest sign-in, held for this session until setup publishes it.
    authentication: tokio::sync::Mutex<Option<Arc<SessionSignIn>>>,
    /// The connection, if any; held while it changes or works.
    live: tokio::sync::Mutex<Connection>,
    /// The connection's loop, for requests that return at once.
    control: std::sync::Mutex<Option<SyncLoop>>,
    /// Whether custody held storage credentials when last observed: start
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
}

impl StorageConnections {
    pub(crate) fn new(
        database: coven_database::Database,
        files: coven_sync::Files,
        identity: Arc<KeySession<MemberIdentity>>,
        credentials: Arc<DeviceCredentials>,
        publications: Arc<Publications>,
        directory: StoreDir,
        waits: Arc<coven_sync::RetryWaits>,
        transfers: Arc<coven_storage::TransferBudget>,
        initial_name: String,
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
        let owner = Self {
            codes,
            database,
            files,
            identity,
            credentials,
            publications,
            settings: coven_storage::StorageSettings::new(directory.clone()),
            directory,
            waits,
            transfers,
            initial_name,
            keys: std::sync::Mutex::new(Some(keys)),
            connector,
            oauth,
            clients,
            authentication: tokio::sync::Mutex::new(authentication),
            live: tokio::sync::Mutex::new(match storage {
                Some(storage) => Connection::Loaded(storage),
                None => Connection::Released,
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
        };
        owner.publications.publish(if configured {
            SyncStatus::Stopped
        } else {
            SyncStatus::Disconnected
        });
        owner
    }

    /// Initialize a loaded connection when a caller first needs its sequence owner.
    async fn ready(&self, live: &mut Connection) -> Result<(), SyncError> {
        if let Connection::Loaded(storage) = &*live {
            let storage = storage.clone();
            self.attach(live, storage).await?;
        }
        Ok(())
    }

    fn store(&self) -> StoreId {
        self.directory.id()
    }

    /// Refuse an invite the location cannot share before any provider change.
    pub(crate) async fn prepare_invite(&self) -> Result<(), SyncError> {
        let location = self.codes.location().await?.ok_or(SyncError::NoStorage)?;
        if matches!(
            location,
            StorageConfig::Dropbox {
                location: DropboxLocation::AppFolder,
            }
        ) {
            return Err(StorageFailure::DropboxAppFolderNotShareable.into());
        }
        Ok(())
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
        E: From<KeyError> + Send + 'static,
        F: Future<Output = Result<T, E>> + Send,
    {
        let owner = self.clone();
        crate::coven::completion(tokio::spawn(async move {
            let _call = owner.lock_open().await?;
            run(owner.clone()).await
        }))
        .await
    }

    /// Compose a sequence owner, its loop and its credential owner on
    /// `storage` as this store's connection.
    async fn attach(
        &self,
        live: &mut Connection,
        storage: Arc<StorageConnection>,
    ) -> Result<(), SyncError> {
        let sync = self.sequence(storage.clone(), live).await?;
        self.files
            .set_storage(Some(storage), std::future::ready(Ok(())))
            .await?;
        self.activate(live, sync, false)
    }

    /// Compose a candidate without replacing the committed connection.
    async fn sequence(
        &self,
        storage: Arc<StorageConnection>,
        previous: &Connection,
    ) -> Result<Arc<SequenceSync>, SyncError> {
        if let Connection::Active(previous) = previous {
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
    fn activate(
        &self,
        live: &mut Connection,
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
        let sync_loop = SyncLoop::new(sync.clone(), Some(codes.clone()));
        let started = if start { sync_loop.start() } else { Ok(()) };
        *self.control.lock().expect("loop control poisoned") = Some(sync_loop.clone());
        *live = Connection::Active(Live {
            sync,
            sync_loop,
            codes,
        });
        started
    }

    /// Release the current connection after its active pass and transfers.
    async fn release(&self, live: &mut Connection) -> Result<(), SyncError> {
        self.control.lock().expect("loop control poisoned").take();
        let closed = match std::mem::replace(live, Connection::Released) {
            Connection::Active(previous) => previous.close().await,
            Connection::Released | Connection::Loaded(_) => Ok(()),
        };
        let released = self
            .files
            .set_storage(None, std::future::ready(Ok(())))
            .await;
        closed.and(released)
    }

    /// Connect with custody's credentials unless this session already has a
    /// connection, without starting the loop.
    pub(crate) async fn connected(&self) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        if let Connection::Active(_) = &*live {
            return Ok(());
        }
        self.connect(&mut live).await
    }

    /// Reach the recorded location with custody's credentials.
    async fn connect(&self, live: &mut Connection) -> Result<(), SyncError> {
        if let Connection::Loaded(storage) = live {
            let storage = storage.clone();
            return self.attach(live, storage).await;
        }
        let data = self.codes.connection().await?.ok_or(SyncError::NoStorage)?;
        let storage = self
            .connector
            .connect(
                data.location,
                self.codes.provider_auth(data.credentials),
                self.publications.clone(),
                self.waits.clone(),
                self.transfers.clone(),
            )
            .await?;
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
        let flow = self
            .oauth
            .as_ref()
            .ok_or(OAuthError::Unavailable(access.provider()))?;
        let tokens = flow.authenticate(access).await?;
        *self.authentication.lock().await = Some(SessionSignIn::new(tokens, self.clients.clone()));
        Ok(())
    }

    pub(crate) async fn setup_oauth(
        self: &Arc<Self>,
        storage: StorageConfig,
        device_name: &str,
    ) -> Result<StorageSetup, StorageSetupError> {
        let device_name = device_name.to_owned();
        self.call(move |owner| async move {
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
            let result = owner
                .setup_connection(storage, &device_name, auth, sign_in)
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
    ) -> Result<StorageSetup, StorageSetupError> {
        let container = container.to_owned();
        let device_name = device_name.to_owned();
        self.call(move |owner| async move {
            let storage = match owner.codes.location().await.map_err(setup_error)? {
                Some(storage) => {
                    if !matches!(&storage, StorageConfig::CloudKit { container: recorded, .. } if *recorded == container) {
                        return Err(StorageSetupError::LocationChangeUnsupported);
                    }
                    storage
                }
                None => owner.connector.prepare_cloudkit(&container, owner.store()).await?,
            };
            owner.setup_connection(storage, &device_name, ProviderAuth::CloudKit, None).await
        }).await
    }

    async fn check_location(&self, config: &StorageConfig) -> Result<(), StorageSetupError> {
        if self
            .codes
            .location()
            .await
            .map_err(setup_error)?
            .is_some_and(|recorded| &recorded != config)
        {
            return Err(StorageSetupError::LocationChangeUnsupported);
        }
        Ok(())
    }

    /// Check and publish `config` reached through `auth`. A new OAuth
    /// sign-in is `sign_in`, the session custody `auth` refreshes in until
    /// setup commits it to device custody. Custody, location and code commit
    /// only after the candidate connection has claimed what setup needs.
    async fn setup_connection(
        &self,
        config: StorageConfig,
        device_name: &str,
        auth: ProviderAuth,
        sign_in: Option<Arc<SessionSignIn>>,
    ) -> Result<StorageSetup, StorageSetupError> {
        let credentials = auth.credentials();
        let storage = self
            .connector
            .connect(
                config.clone(),
                auth,
                self.publications.clone(),
                self.waits.clone(),
                self.transfers.clone(),
            )
            .await?;
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
            .check(
                &path,
                coven_storage::check_provider(storage.as_ref(), &path, &bytes),
            )
            .await?;
        let access = match &credentials {
            StorageCredentials::S3(keys) => coven_format::MemberAccess::S3AccessKey {
                access_key_id: keys.access_key_id.clone(),
                principal: coven_format::S3Principal::Console,
            },
            _ => coven_format::MemberAccess::ProviderAccount(storage.account().await?),
        };
        let mut live = self.live.lock().await;
        // The old provider remains installed until every fallible setup step
        // succeeds. Its stopped or started state survives a rejected candidate.
        if let Connection::Active(previous) = &*live {
            previous.sync_loop.pause().await.map_err(setup_error)?;
        }
        let prepared = async {
            let (commit, restore_code) = self
                .codes
                .prepare_setup(
                    ConnectionCredentials {
                        location: config.clone(),
                        credentials,
                    },
                    sign_in,
                    self.initial_name.clone(),
                )
                .await?;
            let sync = self.sequence(storage.clone(), &live).await?;
            self.claim_setup(&sync, access, device_name).await?;
            // Claims may use file transfers; only the custody commit runs
            // inside the transfer exclusion needed to replace their provider.
            self.files
                .set_storage(Some(storage), async { commit().map(drop) })
                .await?;
            Ok::<_, SyncError>((sync, restore_code))
        }
        .await;
        let (sync, restore_code) = match prepared {
            Ok(prepared) => prepared,
            Err(mut error) => {
                if let Connection::Active(previous) = &*live {
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
        let released = match std::mem::replace(&mut *live, Connection::Released) {
            Connection::Active(previous) => previous.close().await,
            _ => Ok(()),
        };
        let started = self.activate(&mut live, sync, true);
        self.configured.store(true, Ordering::SeqCst);
        // Custody and provider are committed. Worker failures now belong to
        // sync status; setup must return the connection it published.
        let result = match (released, started) {
            (Ok(()), result) | (result, Ok(())) => result,
            (Err(operation), Err(cleanup)) => Err(SyncError::Cleanup {
                operation: Box::new(operation),
                cleanup: Box::new(cleanup),
            }),
        };
        if let Err(error) = result {
            self.publications.publish(SyncStatus::Failed {
                error: error.into(),
            });
        }
        Ok(StorageSetup {
            connection: ConnectedStorage {
                storage: config,
                key_state: StoreKeyState::Available,
            },
            restore_code,
        })
    }

    /// Create the store at slot 1 when this device's history has none, or
    /// catch up and record changed access for an existing device (§9, §10).
    async fn claim_setup(
        &self,
        sync: &Arc<SequenceSync>,
        access: coven_format::MemberAccess,
        device_name: &str,
    ) -> Result<(), SyncError> {
        let state = self.database.store_state().await?;
        let member = self
            .identity
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
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
            let key = ring
                .store_key_ids()
                .next()
                .ok_or(StorageFailure::MemberKeysMissing)?;
            let key_hash = ring.store_key(key)?.hash();
            // The creator's sequence owner seals slot 1 and its key copy
            // with the store key custody holds.
            self.persist_keys(&ring)?;
            let result = sync
                .administer(coven_format::admin::StoreChange::CreateStore {
                    store: self.store(),
                    name: self.initial_name.clone(),
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
            return result;
        }
        sync.unlock().await?;
        let state = self.database.store_state().await?;
        let recorded = state
            .members
            .get(&member.member())
            .filter(|value| !value.removed)
            .ok_or(SyncError::Removed)?;
        if !recorded.access.same_credential(&access) {
            sync.administer(coven_format::admin::StoreChange::SetAccess {
                member: member.member(),
                access,
            })
            .await?;
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
                Connection::Active(candidate) => candidate.sync.unlock().await,
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
                let error = match owner.publish_released(Ok(())) {
                    Ok(()) => error,
                    Err(cleanup) => SyncError::Cleanup {
                        operation: Box::new(error),
                        cleanup: Box::new(cleanup),
                    },
                };
                return Err(unlock_error(error));
            }
            // Connected without syncing.
            owner.publish_released(Ok(())).map_err(unlock_error)?;
            Ok(ConnectedStorage {
                storage: config,
                key_state: StoreKeyState::Available,
            })
        })
        .await
    }

    pub(crate) async fn disconnect(self: &Arc<Self>) -> Result<(), SyncError> {
        self.call(|owner| async move {
            let mut live = owner.live.lock().await;
            owner.codes.forget_credentials().await?;
            let released = owner.release(&mut live).await;
            owner.authentication.lock().await.take();
            owner.publish_released(released)
        })
        .await
    }

    pub(crate) async fn forget_store_keys(self: &Arc<Self>) -> Result<(), KeyError> {
        self.call(|owner| async move {
            let mut live = owner.live.lock().await;
            let result = async {
                if let Connection::Active(previous) = &*live {
                    previous.sync_loop.pause().await?;
                }
                if let Err(mut error) = owner
                    .files
                    .set_storage(None, async { owner.forget_keys().map_err(SyncError::from) })
                    .await
                {
                    if let Connection::Active(previous) = &*live {
                        if let Err(cleanup) = previous.sync_loop.resume() {
                            error = SyncError::Cleanup {
                                operation: Box::new(error),
                                cleanup: Box::new(cleanup),
                            };
                        }
                    }
                    return Err(error);
                }
                owner.control.lock().expect("loop control poisoned").take();
                let released = match std::mem::replace(&mut *live, Connection::Released) {
                    Connection::Active(previous) => previous.close().await,
                    _ => Ok(()),
                };
                // Forgetting succeeded. A worker release failure belongs to
                // status; no later custody read can turn removal into an error.
                owner.publications.publish(match released {
                    Err(error) => SyncStatus::Failed {
                        error: error.into(),
                    },
                    Ok(()) if owner.configured.load(Ordering::SeqCst) => SyncStatus::Stopped,
                    Ok(()) => SyncStatus::Disconnected,
                });
                Ok(())
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

#[path = "storage_s3.rs"]
mod s3;

#[path = "storage_admin.rs"]
mod admin;
pub use admin::GuidedAccessKey;

#[path = "storage_operations.rs"]
mod operations;

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
