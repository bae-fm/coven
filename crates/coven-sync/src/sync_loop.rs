//! Event-driven sequence passes with durable backoff and retained app wakes.
use crate::pending_cause::Failure;
use crate::{SequenceStatus, SequenceSync, SyncError, SyncFailure, SyncStatus};
use coven_database::{DbError, RetrySubject};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
/// Control of one serialized pass loop. Commands survive caller cancellation.
/// It publishes its activity to the owner's `SyncState`.
pub struct SyncLoop {
    commands: mpsc::UnboundedSender<Command>,
}
enum Command {
    Start,
    Now,
    Stop,
    Pause(oneshot::Sender<()>),
    Resume,
    Close(oneshot::Sender<Result<(), SyncError>>),
}
impl SyncLoop {
    /// Compose a stopped loop for an open, connected sequence owner.
    /// `codes` completes member-key replacement and installs delivered S3 keys
    /// on this device after each pass (§11.5, §14.3).
    pub fn new(sync: Arc<SequenceSync>, codes: crate::RestoreCodes) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run(sync, codes, receiver));
        Self { commands }
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
    /// Queue stopping after the active pass and this connection's transfers.
    /// Completion publishes `Stopped`; a release failure publishes `Failed`.
    pub fn stop(&self) -> Result<(), SyncError> {
        self.commands
            .send(Command::Stop)
            .map_err(|_| DbError::StoreClosed.into())
    }
    /// Wait for the active pass, retaining its started state, pending work and
    /// retry schedule while the owner prepares a connection or custody change.
    pub async fn pause(&self) -> Result<(), SyncError> {
        let (send, receive) = oneshot::channel();
        self.commands
            .send(Command::Pause(send))
            .map_err(|_| DbError::StoreClosed)?;
        receive.await.map_err(|_| DbError::StoreClosed.into())
    }
    /// Continue the retained loop after a failed connection or custody change.
    pub fn resume(&self) -> Result<(), SyncError> {
        self.commands
            .send(Command::Resume)
            .map_err(|_| DbError::StoreClosed.into())
    }
    /// Finish active work and this connection's transfers, then end this
    /// loop. The store's owner closes its transfers and database.
    pub async fn close(&self) -> Result<(), SyncError> {
        let (send, receive) = oneshot::channel();
        self.commands
            .send(Command::Close(send))
            .map_err(|_| DbError::StoreClosed)?;
        receive.await.map_err(|_| DbError::StoreClosed)?
    }
}
/// Coven's own operation whose last attempt failed, if any, with the error
/// behind it while this process holds it: it retries under its own backoff,
/// but the pass that left it is not `Synced` (E5).
async fn failing(sync: &SequenceSync) -> Result<Option<crate::PendingRecord>, SyncError> {
    let records = sync.database.pending::<crate::PendingRecord>().await?;
    Ok(sync
        .waits
        .causes()
        .attach(records)
        .into_iter()
        .find(|record| {
            matches!(record.subject, crate::PendingSubject::Operation { kind, .. } if kind.coven_owned())
                && matches!(
                    record.reason.retry_cause(),
                    crate::PendingReason::Storage { .. }
                        | crate::PendingReason::Refused(_)
                        | crate::PendingReason::Failed(_)
                )
        }))
}

/// Complete a pass and member-key custody (§11.5), then install any delivered
/// S3 key this device does not already use (§14.3).
async fn pass(
    sync: &SequenceSync,
    codes: &crate::RestoreCodes,
) -> Result<Option<crate::PendingRecord>, SyncError> {
    sync.pass().await?;
    codes.resume_member_keys().await?;
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
    failing(sync).await
}

/// What a failed pass shows: a blocked slot, an attempt that received no
/// provider response, or the failure itself (E5). A blocked slot's error is
/// logged when the slot or its reason changes.
fn failed(sync: &SequenceSync, error: SyncError) -> SyncStatus {
    let blocked = match &*sync.state.subscribe().borrow() {
        SequenceStatus::Blocked(record) => Some(record.as_ref().clone()),
        SequenceStatus::Opening | SequenceStatus::Synced { .. } | SequenceStatus::Stopped(_) => {
            None
        }
    };
    let Some(record) = blocked else {
        return crate::connection_failure(error);
    };
    let crate::PendingSubject::Slot(slot) = record.subject else {
        return crate::connection_failure(error);
    };
    let causes = sync.waits.causes();
    let error = match error {
        // A later pass stops at the block itself: the slot keeps the error
        // the pass that blocked it returned.
        SyncError::Pending(block) if *block == record => {
            causes.failed(
                &record.subject,
                &record.reason,
                Failure::Repeated(&SyncError::Pending(block)),
            );
            causes
                .attach(vec![record.clone()])
                .pop()
                .and_then(|record| record.cause)
        }
        // The pass that blocked the slot returns the error that blocked it.
        error => {
            let cause = error.into_shared().1;
            causes.failed(
                &record.subject,
                &record.reason,
                Failure::Held(cause.clone()),
            );
            Some(cause)
        }
    };
    SyncStatus::Blocked {
        slot,
        reason: record.reason,
        error,
    }
}

async fn run(
    sync: Arc<SequenceSync>,
    codes: crate::RestoreCodes,
    mut commands: mpsc::UnboundedReceiver<Command>,
) {
    let status = &sync.state;
    let mut changes = sync.database.sync_changes();
    let mut rearmed = sync.waits.rearmed();
    let (mut retry, retained) = match crate::ConnectionRetry::open(&sync.database).await {
        Ok(retry) => retry,
        Err(error) => {
            status.publish(crate::connection_failure(error));
            (crate::ConnectionRetry::Never, None)
        }
    };
    if !retry.ready(&sync.waits) {
        if let Some(error) = retained {
            status.publish(SyncStatus::Failed { error });
        }
    }
    let mut started = false;
    let mut paused = false;
    let mut pending = false;
    let mut idle = sync.clock.sleep(Duration::from_secs(30));
    let mut closing = None;
    loop {
        // The app's network report ends the connection's and operations'
        // waits for the network and asks for a pass; reports during a pass
        // are handled after it, so they end the waits it failed into (§6.7).
        if !paused {
            match sync
                .waits
                .end_offline_waits(crate::NetworkOwner::Connection)
                .await
            {
                Ok(reported) => pending |= reported && started,
                Err(error) => {
                    status.publish(SyncStatus::Failed {
                        error: error.into(),
                    });
                    break;
                }
            }
        }
        // A requested pass waits only for the connection's backoff; an idle
        // loop also wakes for operations' waits and their evidence.
        let mut wakes = if pending {
            Vec::new()
        } else {
            sync.waits.evidence_wakes()
        };
        wakes.extend(sync.waits.next_wake(|subject| {
            *subject == RetrySubject::Sync
                || (!pending && matches!(subject, RetrySubject::Operation(_)))
        }));
        let work_wake = async {
            if wakes.is_empty() {
                std::future::pending::<()>().await;
            } else {
                futures_util::future::select_all(wakes).await;
            }
        };
        let previous = match if paused {
            Ok(None)
        } else {
            sync.database.retry_wait(RetrySubject::Sync).await
        } {
            Ok(wait) => wait,
            Err(error) => {
                status.publish(SyncStatus::Failed {
                    error: SyncError::from(error).into(),
                });
                break;
            }
        };
        let due = !paused
            && retry.ready(&sync.waits)
            && sync.waits.due(RetrySubject::Sync, previous.as_ref());
        let cooldown = sync.waits.cooldown_wake();
        tokio::select! {
            biased;
            command = commands.recv() => match command {
                Some(Command::Start) => {
                    if !started {
                        let ready = sync.files.set_storage(Some(sync.storage.clone()), std::future::ready(Ok(())));
                        match ready.await {
                            Ok(()) => {
                                started = true;
                                pending = true;
                                sync.waits.connection_changed();
                                if retry.ready(&sync.waits) { status.publish(SyncStatus::Syncing); }
                            }
                            Err(error) => {
                                status.publish(SyncStatus::Failed { error: error.into() });
                            }
                        }
                    }
                }
                Some(Command::Now) => pending |= started,
                Some(Command::Pause(reply)) => {
                    paused = true;
                    let _ = reply.send(());
                }
                Some(Command::Resume) => paused = false,
                Some(Command::Stop) => {
                    started = false;
                    pending = false;
                    // No caller waits for stopping; the status carries a failure.
                    status.publish(
                        match sync.files.set_storage(None, std::future::ready(Ok(()))).await {
                            Ok(()) => SyncStatus::Stopped,
                            Err(error) => SyncStatus::Failed { error: error.into() },
                        },
                    );
                }
                // Transfers outlive this connection's loop; the store's
                // owner closes them. This connection's transfers finish first.
                Some(Command::Close(reply)) => {
                    let result = if started && !paused {
                        sync.files.set_storage(None, std::future::ready(Ok(()))).await
                    } else {
                        Ok(())
                    };
                    closing = Some((reply, result));
                    break;
                }
                None => {
                    if started && !paused {
                        let _ = sync.files.set_storage(None, std::future::ready(Ok(()))).await;
                    }
                    break;
                }
            },
            changed = changes.next(), if started && !paused => match changed {
                Ok(()) => pending = true,
                Err(DbError::StoreClosed) => break,
                Err(error) => {
                    status.publish(SyncStatus::Failed { error: SyncError::from(error).into() });
                    started = false;
                }
            },
            _ = &mut idle, if started && !paused && !pending => pending = true,
            _ = sync.state.pass_requested(), if started && !paused && !pending => pending = true,
            // A wait armed outside this loop changes the schedule to wake on.
            changed = rearmed.changed(), if started && !paused => if changed.is_err() { break },
            _ = work_wake, if started && !paused && !(pending && due) => pending = true,
            _ = async {
                if let Some(wake) = cooldown { wake.await } else { std::future::pending::<()>().await }
            }, if started && !paused && pending && !due => {}
            _ = std::future::ready(()), if started && !paused && pending && due => {
                pending = false;
                if let Err(error) = changes.take_changed() {
                    status.publish(SyncStatus::Failed { error: SyncError::from(error).into() });
                    break;
                }
                status.publish(SyncStatus::Syncing);
                let connection_version = sync.waits.connection_version();
                match pass(&sync, &codes).await {
                    Ok(failing) => match retry.succeeded(&sync.database, &sync.waits).await {
                        Ok(()) => match failing {
                            None => status.publish(SyncStatus::Synced { finished_at: sync.clock.now() }),
                            Some(record) => {
                                // Work that never clears stops sync until a
                                // person resets it (E5).
                                started = record.retry() != crate::Retry::Never;
                                pending = false;
                                status.publish(SyncStatus::Failed {
                                    error: SyncFailure::Operation(Box::new(record)),
                                });
                            }
                        },
                        Err(error) => {
                            status.publish(SyncStatus::Failed { error: error.into() });
                            started = false;
                        }
                    },
                    Err(error) => {
                        let reason = sync.error_reason(&error);
                        if let Err(persist) = retry.failed(&sync.database, &sync.waits, reason, connection_version, &error).await {
                            status.publish(SyncStatus::Failed { error: SyncError::Cleanup {
                                operation: Box::new(error), cleanup: Box::new(persist),
                            }.into() });
                            started = false;
                        } else {
                            status.publish(failed(&sync, error));
                            pending = true;
                        }
                    }
                }
                idle = sync.clock.sleep(Duration::from_secs(30));
            }
        }
    }
    // The connection's owners are released before the closer learns so.
    drop(changes);
    drop(codes);
    drop(sync);
    if let Some((reply, result)) = closing {
        let _ = reply.send(result);
    }
}

#[cfg(test)]
#[path = "sync_loop_tests.rs"]
mod tests;
