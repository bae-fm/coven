//! The open store's sync lifetime. The operation worker owns each complete pass.

use crate::{Operations, SyncError, SyncFailure};
use coven_database::{DatabaseChanges, DbError};
use coven_foundation::{clock::ClockRef, id_source::DeviceId};
use coven_storage::{providers::StorageConnector, StorageConnection, StorageError, StorageFailure};
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
    /// The last attempt could not reach storage, regardless of earlier
    /// success: the request preventing the pass received no provider response.
    Offline {
        /// The network failure and its original cause.
        error: Arc<StorageError>,
    },
    /// The initial pass is queued or a pass is running.
    Syncing,
    /// The last pass completed.
    Synced {
        /// The injected wall clock after every step completed.
        finished_at: SystemTime,
    },
    /// The last pass failed as a whole, including a provider's error
    /// response, or coven's own work failed its last attempt; work that
    /// never clears also stops sync. Other per-subject failures belong only
    /// in pending().
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
        connection: coven_storage::ConnectionCredentials,
        sign_in: Option<Arc<crate::SessionSignIn>>,
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
type Backoff =
    futures_util::future::Shared<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>;

struct SyncRun {
    operations: Operations,
    codes: crate::RestoreCodes,
    changes: DatabaseChanges,
    clock: ClockRef,
    send: Arc<crate::SendGate>,
    waits: Arc<crate::RetryWaits>,
    /// Consecutive failed passes on this connection, and the monotonic
    /// deadline and timer before which no new automatic pass starts (SP waiting).
    failures: u32,
    backoff: Option<(Duration, Backoff)>,
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
        send: Arc<crate::SendGate>,
        waits: Arc<crate::RetryWaits>,
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
                send,
                waits,
                failures: 0,
                backoff: None,
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
    /// An OAuth setup's `sign_in` is the custody `storage` refreshes in; setup
    /// publishes it to device custody.
    pub async fn setup(
        &self,
        storage: Arc<StorageConnection>,
        access: coven_format::MemberAccess,
        device_name: String,
        connection: coven_storage::ConnectionCredentials,
        sign_in: Option<Arc<crate::SessionSignIn>>,
        store_name: String,
    ) -> Result<(), SyncError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SyncCommand::Setup {
                storage,
                access,
                device_name,
                connection,
                sign_in,
                store_name,
                reply,
            })
            .map_err(|_| DbError::StoreClosed)?;
        result.await.map_err(|_| DbError::StoreClosed)?
    }
    /// Open the member's sealed keys and retain a stopped connection.
    pub async fn unlock(&self, storage: Arc<StorageConnection>) -> Result<(), SyncError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SyncCommand::Unlock { storage, reply })
            .map_err(|_| DbError::StoreClosed)?;
        result.await.map_err(|_| DbError::StoreClosed)?
    }

    /// Start configured storage once its keys check. The first pass builds an
    /// absent client from custody; a failure there is a failed pass, shown in
    /// status and retried after backoff. Repeated starts and stores without
    /// configured storage are no-ops.
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
    /// reference. Custody sessions keep their keys until forget or handle close.
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

    /// Request a pass while started. Requests during a pass cause another pass;
    /// while a failed connection backs off or a provider cooldown is live,
    /// requests coalesce into the pass that follows it.
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
        self.send.invalidate();
        let data = self.codes.connection().await?.ok_or(SyncError::NoStorage)?;
        let connection = self
            .connector
            .connect(
                data.location,
                self.codes.provider_auth(data.credentials),
                self.device,
                self.send.clone(),
                self.waits.clone(),
            )
            .await?;
        self.operations
            .set_storage(Some(connection.clone()))
            .await?;
        self.connection = Some(connection);
        self.new_connection();
        Ok(())
    }

    /// A newly installed connection starts its own backoff. Provider
    /// cooldowns are the provider's, shared through `waits`, and hold
    /// across it (SP waiting).
    fn new_connection(&mut self) {
        self.failures = 0;
        self.backoff = None;
    }

    async fn release_connection(&mut self) -> Result<(), SyncError> {
        self.send.invalidate();
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
                // The status and the waiting call share one cause.
                let (failure, source) = error.into_shared();
                self.status.send_replace(SyncStatus::Failed {
                    error: SyncFailure::Other(source.clone()),
                });
                Err(SyncError::Failed { failure, source })
            }
        }
    }

    async fn setup(
        &mut self,
        storage: Arc<StorageConnection>,
        access: coven_format::MemberAccess,
        device_name: String,
        connection: coven_storage::ConnectionCredentials,
        sign_in: Option<Arc<crate::SessionSignIn>>,
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
            let commit = self
                .codes
                .prepare_setup(connection, sign_in, store_name)
                .await?;
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
            self.send.invalidate();
            return Err(operation);
        }
        Ok(())
    }

    async fn start(&mut self) -> Result<(), SyncError> {
        // Credentials may have been entered after disconnect removed the old key.
        if !self.configured {
            self.configured = self.codes.connection().await?.is_some();
        }
        if self.configured && !self.started {
            self.operations.check_sync_keys().await?;
            if self.connection.is_none() {
                // The next connection starts its own backoff.
                self.new_connection();
            }
            self.started = true;
            self.status.send_replace(SyncStatus::Syncing);
        }
        Ok(())
    }

    /// What a pass must still wait for: the failed connection's backoff and
    /// a provider cooldown. Requests made meanwhile coalesce (SP waiting).
    fn held(&self) -> Option<crate::retry_waits::Wake> {
        use futures_util::FutureExt;
        let backoff = self.backoff.as_ref().and_then(|(due, timer)| {
            (*due > self.clock.elapsed() && timer.clone().now_or_never().is_none())
                .then(|| timer.clone())
        });
        match (backoff, self.waits.cooldown_wake()) {
            (None, None) => None,
            (backoff, cooldown) => Some(Box::pin(async move {
                if let Some(backoff) = backoff {
                    backoff.await;
                }
                if let Some(cooldown) = cooldown {
                    cooldown.await;
                }
            })),
        }
    }

    async fn run(mut self) {
        let mut delay = self.clock.sleep(Duration::from_secs(30));
        let mut pending = false;
        loop {
            let mut held = if self.started && pending {
                self.held()
            } else {
                None
            };
            let holding = held.is_some();
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
                        Some(SyncCommand::Setup { storage, access, device_name, connection, sign_in, store_name, reply }) => {
                            // Acquire custody only after this command owns the loop.
                            // A queued setup must not hold the lock a running pass
                            // needs before it can finish and receive this command.
                            let result = self.setup(storage.clone(), access, device_name, connection, sign_in, store_name).await;
                            if result.is_ok() {
                                self.connection = Some(storage); self.new_connection(); self.configured = true;
                                self.started = true; pending = true;
                                self.status.send_replace(SyncStatus::Syncing);
                            }
                            let _ = reply.send(result);
                        }
                        Some(SyncCommand::Unlock { storage, reply }) => {
                            let result = self.operations.unlock_storage(storage.clone()).await;
                            if result.is_ok() {
                                self.connection = Some(storage); self.new_connection(); self.configured = true; self.started = false; pending = false;
                                self.status.send_replace(SyncStatus::Stopped);
                            }
                            let _ = reply.send(result);
                        }
                        Some(SyncCommand::Start(reply)) => {
                            let was_started = self.started;
                            let result = self.start().await;
                            pending |= result.is_ok() && self.started && !was_started;
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
                                self.send.invalidate();
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
                _ = async { if let Some(held) = held.as_mut() { held.await } }, if holding => {}
                _ = std::future::ready(()), if self.started && pending && !holding => {
                    pending = false;
                    // This pass serves every local write committed so far; one
                    // committed after this point retains one more pass. Its own
                    // upload confirmations and other bookkeeping wake nothing.
                    match self.changes.take_changed() {
                        Ok(_) => {}
                        Err(DbError::StoreClosed) => break,
                        Err(error) => {
                            self.started = false;
                            self.status.send_replace(SyncStatus::Failed { error: SyncError::from(error).into() });
                            continue;
                        }
                    }
                    self.status.send_replace(SyncStatus::Syncing);
                    let result = async {
                        if self.connection.is_none() {
                            self.connect().await?;
                        }
                        let failing = self.operations.sync().await?;
                        // An admin's replacement of this member's key arrives
                        // sealed to them; adopt it before the old key goes (§13).
                        let held = match self.codes.connection().await? {
                            Some(coven_storage::ConnectionCredentials {
                                credentials: coven_storage::StorageCredentials::S3(held),
                                ..
                            }) => Some(held.access_key_id),
                            _ => None,
                        };
                        if let Some(key) = self.operations.delivered_access_key(held).await? {
                            self.codes.adopt_delivered_key(key).await?;
                        }
                        Ok::<_, SyncError>(failing)
                    }.await;
                    match result {
                        // A finished pass ends the connection's backoff; failing
                        // work retries under its own.
                        Ok(failing) => {
                            self.failures = 0;
                            self.backoff = None;
                            self.status.send_replace(match failing {
                                None => SyncStatus::Synced { finished_at: self.clock.now() },
                                Some(record) => {
                                    // Work that never clears stalls sync until
                                    // a person resets it (E5).
                                    self.started = record.retry() != crate::Retry::Never;
                                    SyncStatus::Failed { error: SyncFailure::Operation(Box::new(record)) }
                                }
                            });
                        }
                        Err(error) => {
                            self.failures = self.failures.saturating_add(1);
                            let wait = coven_storage::retry_delay(self.failures - 1);
                            self.backoff = Some((
                                self.clock.elapsed() + wait,
                                futures_util::FutureExt::shared(self.clock.sleep(wait)),
                            ));
                            let error = SyncFailure::from(error);
                            self.started = !matches!(error, SyncFailure::Removed | SyncFailure::DeviceIdInUse(_) | SyncFailure::UpdateRequired(_));
                            // Only this attempt decides: a provider's error
                            // response, even one classified Network, is Failed.
                            let status = match error {
                                SyncFailure::Storage(error) if error.failure() == StorageFailure::Network && !error.answered() => SyncStatus::Offline { error },
                                error => SyncStatus::Failed { error },
                            };
                            self.status.send_replace(status);
                        }
                    }
                    delay = self.clock.sleep(Duration::from_secs(30));
                }
            }
        }
        self.send.invalidate();
    }
}

#[cfg(test)]
#[path = "sync_loop_tests.rs"]
mod tests;
