//! The open store's sync lifetime. The operation worker owns each complete pass.

use crate::{Operations, SyncError, SyncFailure, SyncReport};
use coven_database::{DatabaseChanges, DbError};
use coven_foundation::clock::ClockRef;
use coven_storage::{Storage, StorageConnection, StorageFailure};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot, watch};

/// Current connection and synchronization state (§20.5).
#[derive(Debug)]
pub enum SyncStatus {
    /// No provider is connected.
    Disconnected,
    /// A provider is connected and synchronization is stopped.
    Stopped,
    /// Storage has not been reached since connecting.
    Offline,
    /// One complete pass is running.
    Syncing,
    /// The last pass completed.
    Synced(SyncReport),
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
    Connect {
        storage: Arc<StorageConnection>,
        start: bool,
        reply: Reply,
    },
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
    Disconnect(Option<Reply>),
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
    started: bool,
}

impl SyncLoop {
    /// Compose beside operations and database at opening. Opening a store never
    /// starts synchronization, even when its provider has already been supplied.
    pub fn new(
        operations: Operations,
        codes: crate::RestoreCodes,
        changes: DatabaseChanges,
        clock: ClockRef,
        connection: Option<Arc<StorageConnection>>,
    ) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let (status, subscription) = watch::channel(if connection.is_some() {
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
                started: false,
            }
            .run(),
        );
        Self {
            commands,
            status: subscription,
        }
    }

    /// Connect the supplied provider after its credentials and keys have been
    /// checked. Replacing a connection waits for the active pass to finish.
    pub async fn connect(&self, storage: Arc<dyn Storage>, start: bool) -> Result<(), SyncError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SyncCommand::Connect {
                storage: Arc::new(StorageConnection::new(storage)),
                start,
                reply,
            })
            .map_err(|_| DbError::StoreClosed)?;
        result.await.map_err(|_| DbError::StoreClosed)?
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

    /// Start the connected store; an absent connection is a no-op.
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

    /// Finish the active pass and keep the idle connection. Each step's unlocked
    /// keys are scoped to that step and have been dropped when the pass returns.
    pub fn stop(&self) {
        let _ = self.commands.send(SyncCommand::Stop);
    }

    /// Finish the active pass and release every worker's provider reference.
    pub fn disconnect(&self) {
        let _ = self.commands.send(SyncCommand::Disconnect(None));
    }

    /// Disconnect after the active pass and wait until collaborators have released storage.
    pub async fn disconnect_wait(&self) -> Result<(), SyncError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SyncCommand::Disconnect(Some(reply)))
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
                            self.status.send_replace(if self.connection.is_some() { SyncStatus::Stopped } else { SyncStatus::Disconnected });
                            let _ = reply.send(Ok(()));
                            break;
                        }
                        Some(SyncCommand::Connect { storage, start, reply }) => {
                            let result = async {
                                self.operations.check_sync_keys().await?;
                                self.operations.set_storage(Some(storage.clone())).await
                            }.await;
                            match result {
                                Ok(()) => {
                                    self.connection = Some(storage);
                                    self.started = start;
                                    pending = start;
                                    self.status.send_replace(if start { SyncStatus::Offline } else { SyncStatus::Stopped });
                                    let _ = reply.send(Ok(()));
                                }
                                Err(error) => { let _ = reply.send(Err(error)); }
                            }
                        }
                        Some(SyncCommand::Setup { storage, access, device_name, connection, store_name, reply }) => {
                            // Acquire custody only after this command owns the loop.
                            // A queued setup must not hold the lock a running pass
                            // needs before it can finish and receive this command.
                            let result = async {
                                let commit = self.codes.prepare_setup(connection, store_name).await?;
                                self.operations.setup_storage(storage.clone(), access, device_name, commit).await
                            }.await;
                            if result.is_ok() {
                                self.connection = Some(storage); self.started = true; pending = true;
                                self.status.send_replace(SyncStatus::Stopped);
                            }
                            let _ = reply.send(result);
                        }
                        Some(SyncCommand::Unlock { storage, reply }) => {
                            let result = self.operations.unlock_storage(storage.clone()).await;
                            if result.is_ok() {
                                self.connection = Some(storage); self.started = false; pending = false;
                                self.status.send_replace(SyncStatus::Stopped);
                            }
                            let _ = reply.send(result);
                        }
                        Some(SyncCommand::Start(reply)) => {
                            let result = if self.connection.is_some() && !self.started {
                                match self.operations.check_sync_keys().await {
                                    Ok(()) => { self.started = true; pending = true; Ok(()) }
                                    Err(error) => Err(error),
                                }
                            } else { Ok(()) };
                            let _ = reply.send(result);
                        }
                        Some(SyncCommand::Stop) => {
                            self.started = false;
                            pending = false;
                            self.status.send_replace(if self.connection.is_some() { SyncStatus::Stopped } else { SyncStatus::Disconnected });
                        }
                        Some(SyncCommand::ForgetKeys(reply)) => {
                            let result = self.operations.forget_store_keys().await;
                            if result.is_ok() {
                                self.started = false; self.connection = None; pending = false;
                                self.status.send_replace(SyncStatus::Disconnected);
                            }
                            let _ = reply.send(result);
                        }
                        Some(SyncCommand::Disconnect(reply)) => {
                            self.started = false;
                            pending = false;
                            match self.operations.set_storage(None).await {
                                Ok(()) => {
                                    self.connection = None;
                                    self.status.send_replace(SyncStatus::Disconnected);
                                    if let Some(reply) = reply { let _ = reply.send(Ok(())); }
                                }
                                Err(error) => {
                                    let error = SyncFailure::from(error);
                                    self.status.send_replace(SyncStatus::Failed { error: error.clone() });
                                    if let Some(reply) = reply { let _ = reply.send(Err(error.into())); }
                                }
                            }
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
                        Ok(results) => {
                            self.status.send_replace(SyncStatus::Synced(results.finish(self.clock.now())));
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
