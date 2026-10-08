//! The open store's sync lifetime. The operation worker owns each complete pass.

use crate::{Operations, SyncError, SyncFailure};
use coven_database::{DatabaseChanges, DbError};
use coven_foundation::{clock::ClockRef, id_source::DeviceId};
use coven_storage::{providers::StorageConnector, Storage, StorageConnection, StorageFailure};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::{mpsc, oneshot, watch};

/// Current connection and synchronization state (E5).
#[derive(Debug)]
pub enum SyncStatus {
    /// No storage is set up on this device.
    Disconnected,
    /// Storage is set up and synchronization is stopped; a client may be absent.
    Stopped,
    /// Storage has not been reached since connecting.
    Offline,
    /// The initial pass is queued or a pass is running.
    Syncing,
    /// The last pass completed.
    Synced {
        /// The injected wall clock after every step completed.
        finished_at: SystemTime,
    },
    /// The last pass failed as a whole.
    Failed {
        /// The failure and its original cause.
        error: SyncFailure,
    },
}

/// Shared control of one loop per open store. SyncCommands survive cancellation of
/// their caller. Dropping the last control finishes the active pass and exits.
#[derive(Clone)]
pub struct SyncLoop {
    commands: mpsc::UnboundedSender<SyncCommand>,
    status: watch::Receiver<SyncStatus>,
}

enum SyncCommand {
    Setup {
        storage: Arc<StorageConnection>,
        access: coven_format::MemberAccess,
        device_name: String,
        connection: coven_storage::RestoreStorage,
        store_name: String,
        reply: Reply,
    },
    Unlock {
        storage: Arc<StorageConnection>,
        reply: Reply,
    },
    Start(Reply),
    ForgetKeys(Reply),
    Stop,
    ForgetStorage(Reply),
    Now,
    Close(Reply),
}
type Reply = oneshot::Sender<Result<(), SyncError>>;

struct SyncRun {
    operations: Operations,
    codes: crate::RestoreCodes,
    changes: DatabaseChanges,
    clock: ClockRef,
    commands: mpsc::UnboundedReceiver<SyncCommand>,
    status: watch::Sender<SyncStatus>,
    connection: Option<Arc<StorageConnection>>,
    connector: Arc<dyn StorageConnector>,
    device: DeviceId,
    configured: bool,
    started: bool,
}

impl SyncLoop {
    /// Compose beside operations and database at opening. Opening a store never
    /// starts synchronization, even when its provider has already been supplied.
    /// Credential presence distinguishes configured storage without a live client.
    pub fn new(
        operations: Operations,
        codes: crate::RestoreCodes,
        changes: DatabaseChanges,
        clock: ClockRef,
        connection: Option<Arc<StorageConnection>>,
        connector: Arc<dyn StorageConnector>,
        device: DeviceId,
        has_storage_credentials: bool,
    ) -> Self {
        let configured = connection.is_some() || has_storage_credentials;
        let (commands, receiver) = mpsc::unbounded_channel();
        let (status, subscription) = watch::channel(if configured {
            SyncStatus::Stopped
        } else {
            SyncStatus::Disconnected
        });
        tokio::spawn(
            SyncRun {
                operations,
                codes,
                changes,
                clock,
                commands: receiver,
                status,
                connection,
                connector,
                device,
                configured,
                started: false,
            }
            .run(),
        );
        Self {
            commands,
            status: subscription,
        }
    }

    /// Finish setup under this loop's connection lifetime, then start syncing.
    pub async fn setup(
        &self,
        storage: Arc<dyn Storage>,
        access: coven_format::MemberAccess,
        device_name: String,
        connection: coven_storage::RestoreStorage,
        store_name: String,
    ) -> Result<(), SyncError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SyncCommand::Setup {
                storage: Arc::new(StorageConnection::new(storage)),
                access,
                device_name,
                connection,
                store_name,
                reply,
            })
            .map_err(|_| DbError::StoreClosed)?;
        result.await.map_err(|_| DbError::StoreClosed)?
    }
    /// Open the member's sealed keys and retain a stopped connection.
    pub async fn unlock(&self, storage: Arc<dyn Storage>) -> Result<(), SyncError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SyncCommand::Unlock {
                storage: Arc::new(StorageConnection::new(storage)),
                reply,
            })
            .map_err(|_| DbError::StoreClosed)?;
        result.await.map_err(|_| DbError::StoreClosed)?
    }

    /// Start configured storage, reconstructing an absent client from custody.
    /// Repeated starts and stores without configured storage are no-ops.
    pub async fn start(&self) -> Result<(), SyncError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SyncCommand::Start(reply))
            .map_err(|_| DbError::StoreClosed)?;
        result.await.map_err(|_| DbError::StoreClosed)?
    }

    /// Finish the active pass and remove custody before dropping the connection.
    /// A failed custody removal preserves the current connection and loop state.
    pub async fn forget_store_keys(&self) -> Result<(), SyncError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SyncCommand::ForgetKeys(reply))
            .map_err(|_| DbError::StoreClosed)?;
        result.await.map_err(|_| DbError::StoreClosed)?
    }

    /// Finish the active pass and transfers, then release every worker's provider
    /// reference. Unlocked keys are scoped to work and dropped before it returns.
    /// Credentials remain in custody for the next start; failures publish Failed.
    pub fn stop(&self) {
        let _ = self.commands.send(SyncCommand::Stop);
    }

    /// Finish the active pass, forget credentials, and release storage. A failed
    /// custody removal preserves the connection and loop state.
    pub async fn forget_storage(&self) -> Result<(), SyncError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SyncCommand::ForgetStorage(reply))
            .map_err(|_| DbError::StoreClosed)?;
        result.await.map_err(|_| DbError::StoreClosed)?
    }

    /// Request a pass while started. Requests during a pass cause another pass.
    pub fn sync_now(&self) {
        let _ = self.commands.send(SyncCommand::Now);
    }

    /// Subscribe with the current value immediately available.
    pub fn subscribe(&self) -> watch::Receiver<SyncStatus> {
        self.status.clone()
    }

    /// Finish the active pass and end this loop before closing its collaborators.
    pub async fn close(&self) -> Result<(), SyncError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SyncCommand::Close(reply))
            .map_err(|_| DbError::StoreClosed)?;
        result.await.map_err(|_| DbError::StoreClosed)?
    }
}

impl SyncRun {
    /// Reconstruct a configured provider through the injected connector. Callers
    /// serialize this with stop, setup and credential removal in the command loop.
    async fn connect(&mut self) -> Result<(), SyncError> {
        self.codes.refresh_if_expired(self.clock.now()).await?;
        let data = self.codes.connection().await?.ok_or(SyncError::NoStorage)?;
        let storage = self
            .connector
            .connect(data.location, data.credentials, self.device)
            .await?;
        let connection = Arc::new(StorageConnection::new(storage));
        self.operations
            .set_storage(Some(connection.clone()))
            .await?;
        self.connection = Some(connection);
        Ok(())
    }

    async fn release_connection(&mut self) -> Result<(), SyncError> {
        self.operations.set_storage(None).await?;
        self.connection = None;
        Ok(())
    }

    fn publish_stopped(&self, result: Result<(), SyncError>) -> Result<(), SyncError> {
        match result {
            Ok(()) => {
                self.status.send_replace(if self.configured {
                    SyncStatus::Stopped
                } else {
                    SyncStatus::Disconnected
                });
                Ok(())
            }
            Err(error) => {
                let error = SyncFailure::from(error);
                self.status.send_replace(SyncStatus::Failed {
                    error: error.clone(),
                });
                Err(error.into())
            }
        }
    }

    async fn setup(
        &mut self,
        storage: Arc<StorageConnection>,
        access: coven_format::MemberAccess,
        device_name: String,
        connection: coven_storage::RestoreStorage,
        store_name: String,
    ) -> Result<(), SyncError> {
        // Relocation needs the old provider even when stop has released it.
        let reconnect = self.configured
            && self.connection.is_none()
            && self
                .codes
                .connection()
                .await?
                .ok_or(SyncError::NoStorage)?
                .location
                != connection.location;
        if reconnect {
            self.connect().await?;
        }
        let result = async {
            let commit = self.codes.prepare_setup(connection, store_name).await?;
            self.operations
                .setup_storage(storage, access, device_name, commit)
                .await
        }
        .await;
        if let Err(operation) = result {
            if reconnect {
                if let Err(cleanup) = self.release_connection().await {
                    return self.publish_stopped(Err(SyncError::Cleanup {
                        operation: Box::new(operation),
                        cleanup: Box::new(cleanup),
                    }));
                }
            }
            return Err(operation);
        }
        Ok(())
    }

    async fn run(mut self) {
        let mut delay = self.clock.sleep(Duration::from_secs(30));
        let mut pending = false;
        loop {
            tokio::select! {
                biased;
                command = self.commands.recv() => {
                    match command {
                        None => break,
                        Some(SyncCommand::Close(reply)) => {
                            self.started = false;
                            let result = self.release_connection().await;
                            let result = self.publish_stopped(result);
                            let _ = reply.send(result);
                            break;
                        }
                        Some(SyncCommand::Setup { storage, access, device_name, connection, store_name, reply }) => {
                            // Acquire custody only after this command owns the loop.
                            // A queued setup must not hold the lock a running pass
                            // needs before it can finish and receive this command.
                            let result = self.setup(storage.clone(), access, device_name, connection, store_name).await;
                            if result.is_ok() {
                                self.connection = Some(storage); self.configured = true;
                                self.started = true; pending = true;
                                self.status.send_replace(SyncStatus::Syncing);
                            }
                            let _ = reply.send(result);
                        }
                        Some(SyncCommand::Unlock { storage, reply }) => {
                            let result = self.operations.unlock_storage(storage.clone()).await;
                            if result.is_ok() {
                                self.connection = Some(storage); self.configured = true; self.started = false; pending = false;
                                self.status.send_replace(SyncStatus::Stopped);
                            }
                            let _ = reply.send(result);
                        }
                        Some(SyncCommand::Start(reply)) => {
                            let result = if self.configured && !self.started {
                                let result = async {
                                    self.operations.check_sync_keys().await?;
                                    if self.connection.is_none() {
                                        self.connect().await?;
                                    }
                                    Ok(())
                                }.await;
                                if result.is_ok() {
                                    self.started = true; pending = true;
                                    self.status.send_replace(SyncStatus::Syncing);
                                }
                                result
                            } else { Ok(()) };
                            let _ = reply.send(result);
                        }
                        Some(SyncCommand::Stop) => {
                            self.started = false;
                            pending = false;
                            let result = self.release_connection().await;
                            // No waiter exists for stop; the watch channel carries failure.
                            let _ = self.publish_stopped(result);
                        }
                        Some(SyncCommand::ForgetKeys(reply)) => {
                            let result = self.operations.forget_store_keys().await;
                            let result = if result.is_ok() {
                                self.started = false; self.connection = None; pending = false;
                                self.publish_stopped(result)
                            } else { result };
                            let _ = reply.send(result);
                        }
                        Some(SyncCommand::ForgetStorage(reply)) => {
                            let result = async {
                                self.codes.forget_credentials().await?;
                                self.configured = false;
                                self.started = false;
                                pending = false;
                                let result = self.release_connection().await;
                                self.publish_stopped(result)
                            }.await;
                            let _ = reply.send(result);
                        }
                        Some(SyncCommand::Now) => { pending |= self.started; }
                    }
                }
                change = self.changes.next(), if self.started => {
                    match change {
                        Ok(()) => pending = true,
                        Err(DbError::StoreClosed) => break,
                        Err(error) => {
                            self.started = false;
                            self.status.send_replace(SyncStatus::Failed { error: SyncError::from(error).into() });
                        }
                    }
                }
                _ = &mut delay, if self.started && !pending => { pending = true; }
                _ = std::future::ready(()), if self.started && pending => {
                    pending = false;
                    self.status.send_replace(SyncStatus::Syncing);
                    let result = async {
                        self.codes.refresh_if_expired(self.clock.now()).await?;
                        self.operations.sync().await
                    }.await;
                    match result {
                        Ok(()) => {
                            self.status.send_replace(SyncStatus::Synced { finished_at: self.clock.now() });
                        }
                        Err(error) => {
                            let error = SyncFailure::from(error);
                            self.started = !matches!(error, SyncFailure::Removed | SyncFailure::LocationTaken | SyncFailure::UpdateRequired);
                            let offline = !self.connection.as_ref().is_some_and(|connection| connection.reached()) && matches!(&error, SyncFailure::Storage(error) if error.failure() == StorageFailure::Network);
                            let status = if offline { SyncStatus::Offline } else { SyncStatus::Failed { error } };
                            self.status.send_replace(status);
                        }
                    }
                    delay = self.clock.sleep(Duration::from_secs(30));
                }
            }
        }
    }
}
