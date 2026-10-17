//! Event-driven sequence passes with durable backoff and retained app wakes.
use crate::{SequenceSync, SyncError, SyncFailure};
use coven_database::{DbError, RetrySubject};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot, watch};

/// Activity of the automatic pass owner; slot progress belongs to `SyncState`.
#[derive(Clone, Debug)]
pub enum LoopStatus {
    /// No automatic pass is requested.
    Stopped,
    /// A pass owns the connection.
    Running,
    /// Waiting for app work, an operation prerequisite, or the idle timer.
    Waiting,
    /// The last attempt failed, with its retained cause.
    Failed(SyncFailure),
}
#[derive(Clone)]
/// Control of one serialized pass loop. Commands survive caller cancellation.
pub struct SyncLoop {
    commands: mpsc::UnboundedSender<Command>,
    status: watch::Receiver<LoopStatus>,
}
enum Command {
    Start,
    Now,
    Stop(oneshot::Sender<Result<(), SyncError>>),
    Close(oneshot::Sender<Result<(), SyncError>>),
}
impl SyncLoop {
    /// Compose a stopped loop for an open, connected sequence owner.
    /// `codes` installs a delivered S3 key on this device after each pass (§13).
    pub fn new(sync: Arc<SequenceSync>, codes: Option<crate::RestoreCodes>) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let (status, subscription) = watch::channel(LoopStatus::Stopped);
        tokio::spawn(run(sync, codes, receiver, status));
        Self {
            commands,
            status: subscription,
        }
    }
    /// Start and request an immediate pass.
    pub fn start(&self) -> Result<(), SyncError> {
        self.commands
            .send(Command::Start)
            .map_err(|_| DbError::StoreClosed.into())
    }
    /// Retain one pass request, including while a pass is running.
    pub fn sync_now(&self) -> Result<(), SyncError> {
        self.commands
            .send(Command::Now)
            .map_err(|_| DbError::StoreClosed.into())
    }
    /// Observe automatic activity and failures.
    pub fn subscribe(&self) -> watch::Receiver<LoopStatus> {
        self.status.clone()
    }
    /// Finish the active pass and transfers before stopping automatic work.
    pub async fn stop(&self) -> Result<(), SyncError> {
        let (send, receive) = oneshot::channel();
        self.commands
            .send(Command::Stop(send))
            .map_err(|_| DbError::StoreClosed)?;
        receive.await.map_err(|_| DbError::StoreClosed)?
    }
    /// Finish active work and end this loop before closing its database.
    pub async fn close(&self) -> Result<(), SyncError> {
        let (send, receive) = oneshot::channel();
        self.commands
            .send(Command::Close(send))
            .map_err(|_| DbError::StoreClosed)?;
        receive.await.map_err(|_| DbError::StoreClosed)?
    }
}
/// A pass, then the delivered S3 key this member's applied history records,
/// unless this device already uses it (§13).
async fn pass(sync: &SequenceSync, codes: Option<&crate::RestoreCodes>) -> Result<(), SyncError> {
    sync.pass().await?;
    if let Some(codes) = codes {
        let held = match codes.connection().await? {
            Some(coven_storage::ConnectionCredentials {
                credentials: coven_storage::StorageCredentials::S3(held),
                ..
            }) => Some(held.access_key_id),
            _ => None,
        };
        if let Some(key) = sync.delivered_access_key(held.as_deref()).await? {
            codes.adopt_delivered_key(key).await?;
        }
    }
    Ok(())
}

async fn run(
    sync: Arc<SequenceSync>,
    codes: Option<crate::RestoreCodes>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    status: watch::Sender<LoopStatus>,
) {
    let mut changes = sync.database.sync_changes();
    let mut started = false;
    let mut pending = false;
    let mut idle = sync.clock.sleep(Duration::from_secs(30));
    loop {
        let mut wakes = sync.waits.evidence_wakes();
        wakes.extend(sync.waits.next_wake(|subject| {
            matches!(subject, RetrySubject::Operation(_) | RetrySubject::Sync)
        }));
        let work_wake = async {
            if wakes.is_empty() {
                std::future::pending::<()>().await;
            } else {
                futures_util::future::select_all(wakes).await;
            }
        };
        let previous = match sync.database.retry_wait(RetrySubject::Sync).await {
            Ok(wait) => wait,
            Err(error) => {
                status.send_replace(LoopStatus::Failed(SyncError::from(error).into()));
                break;
            }
        };
        let due = sync.waits.due(RetrySubject::Sync, previous.as_ref());
        let cooldown = sync.waits.cooldown_wake();
        tokio::select! {
            biased;
            command = commands.recv() => match command {
                Some(Command::Start) => {
                    if !started {
                        match sync.files.set_storage(Some(sync.storage.clone()), std::future::ready(Ok(()))).await {
                            Ok(()) => {
                                started = true;
                                pending = true;
                                sync.waits.connection_changed();
                            }
                            Err(error) => {
                                status.send_replace(LoopStatus::Failed(error.into()));
                            }
                        }
                    }
                }
                Some(Command::Now) => pending |= started,
                Some(Command::Stop(reply)) => {
                    started = false;
                    pending = false;
                    let result = sync.files.set_storage(None, std::future::ready(Ok(()))).await;
                    status.send_replace(LoopStatus::Stopped);
                    let _ = reply.send(result);
                }
                Some(Command::Close(reply)) => {
                    sync.files.close().await;
                    let _ = reply.send(Ok(()));
                    break;
                }
                None => {
                    sync.files.close().await;
                    break;
                }
            },
            changed = changes.next(), if started => match changed {
                Ok(()) => pending = true,
                Err(DbError::StoreClosed) => break,
                Err(error) => {
                    status.send_replace(LoopStatus::Failed(SyncError::from(error).into()));
                    started = false;
                }
            },
            _ = &mut idle, if started && !pending => pending = true,
            _ = work_wake, if started => pending = true,
            _ = async {
                if let Some(wake) = cooldown { wake.await } else { std::future::pending::<()>().await }
            }, if started && pending && !due => {}
            _ = std::future::ready(()), if started && pending && due => {
                pending = false;
                if let Err(error) = changes.take_changed() {
                    status.send_replace(LoopStatus::Failed(SyncError::from(error).into()));
                    break;
                }
                status.send_replace(LoopStatus::Running);
                match pass(&sync, codes.as_ref()).await {
                    Ok(_) => match sync.waits.clear(RetrySubject::Sync).await {
                        Ok(()) => {
                            status.send_replace(LoopStatus::Waiting);
                        }
                        Err(error) => {
                            status.send_replace(LoopStatus::Failed(error.into()));
                            started = false;
                        }
                    },
                    Err(error) => {
                        if let Err(persist) = sync.waits.fail(RetrySubject::Sync, previous.as_ref()).await {
                            status.send_replace(LoopStatus::Failed(persist.into()));
                            started = false;
                        } else {
                            started = !matches!(error, SyncError::Removed | SyncError::DeviceIdInUse(_));
                            status.send_replace(LoopStatus::Failed(error.into()));
                            pending = started;
                        }
                    }
                }
                idle = sync.clock.sleep(Duration::from_secs(30));
            }
        }
    }
}

#[cfg(test)]
#[path = "sync_loop_tests.rs"]
mod tests;
