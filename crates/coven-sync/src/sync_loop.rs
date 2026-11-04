//! Event-driven sequence passes with durable backoff and retained app wakes.
use crate::{
    LastPass, PassFailure, RetryOutcome, RetryWaits, SequenceStatus, SequenceSync, SyncError,
    SyncFailure,
};
use coven_database::{DbError, RetrySubject};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
/// Control of one serialized pass loop. Commands survive caller cancellation.
/// It publishes its activity to the owner's `SequenceState`.
pub struct SyncLoop {
    commands: mpsc::UnboundedSender<Command>,
    /// Where the person's retry requests wait for this loop's passes.
    waits: Arc<RetryWaits>,
    /// The bug the loop ended on, if it panicked: what a command to it
    /// fails with from then on, where a loop that ended with its store
    /// fails with `StoreClosed`.
    panicked: Arc<std::sync::OnceLock<coven_foundation::task::Bug>>,
}
enum Command {
    Start,
    Now,
    Stop,
    Pause(oneshot::Sender<()>),
    Resume,
    Close(oneshot::Sender<Result<(), SyncError>>),
}
#[expect(
    clippy::map_err_ignore,
    reason = "a command fails only when the loop has ended, and `gone` says why"
)]
impl SyncLoop {
    /// Compose a stopped loop for an open, connected sequence owner.
    /// `codes` completes member-key replacement and installs delivered S3 keys
    /// on this device after each pass (§11.5, §14.3).
    /// A panic in the loop goes to the store's bug sink, and ends the loop
    /// as a failure: the last pass is recorded failed with the bug, sync
    /// stops, and the retries waiting, those the panicking pass took among
    /// them, are answered with the bug (E15).
    pub fn new(sync: Arc<SequenceSync>, codes: crate::RestoreCodes) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let waits = sync.waits.clone();
        let owner = sync.clone();
        let panicked = Arc::new(std::sync::OnceLock::new());
        let ended = panicked.clone();
        drop(coven_foundation::task::spawn_owned(
            "the sync loop",
            sync.database.bugs().clone(),
            run(sync, codes, receiver),
            |bug| async move {
                ended.get_or_init(|| bug.clone());
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "the loop has ended either way; a failed write has failed the handle, which reports it"
                )]
                let _ = show(&owner, LastPass::failed(SyncError::Bug(bug.clone()))).await;
                if let Err(error) = owner.waits.sync_panicked(bug).await {
                    tracing::debug!("stopping sync on a failed handle: {error}");
                }
            },
        ));
        Self { commands, waits, panicked }
    }

    /// Why a command found the loop gone: the bug it ended on, or the store
    /// closing.
    fn gone(&self) -> SyncError {
        match self.panicked.get() {
            Some(bug) => SyncError::Bug(bug.clone()),
            None => DbError::StoreClosed.into(),
        }
    }
    /// Start and request an immediate pass. From now on the person's retry
    /// requests wait for this loop's passes.
    pub async fn start(&self) -> Result<(), SyncError> {
        self.waits.sync_started().await?;
        if self.commands.send(Command::Start).is_err() {
            self.waits.sync_stopped().await?;
            return Err(self.gone());
        }
        Ok(())
    }
    /// Retain one pass request, including while a pass is running.
    pub fn sync_now(&self) -> Result<(), SyncError> {
        self.commands.send(Command::Now).map_err(|_| self.gone())
    }
    /// Queue stopping after the active pass and this connection's transfers.
    /// Stopping writes nothing the sync state reads; a release failure is
    /// recorded as the last pass's failure.
    /// Retry requests no pass has taken are answered `NotSyncing` now. The
    /// loop stops even when writing that fails, which has failed the handle.
    pub async fn stop(&self) -> Result<(), SyncError> {
        let stopped = self.waits.sync_stopped().await;
        self.commands.send(Command::Stop).map_err(|_| self.gone())?;
        Ok(stopped?)
    }
    /// Wait for the active pass, retaining its started state, pending work and
    /// retry schedule while the owner prepares a connection or custody change.
    pub async fn pause(&self) -> Result<(), SyncError> {
        let (send, receive) = oneshot::channel();
        self.commands.send(Command::Pause(send)).map_err(|_| self.gone())?;
        receive.await.map_err(|_| self.gone())
    }
    /// Continue the retained loop after a failed connection or custody change.
    pub fn resume(&self) -> Result<(), SyncError> {
        self.commands.send(Command::Resume).map_err(|_| self.gone())
    }
    /// Finish active work and this connection's transfers, then end this
    /// loop. The store's owner closes its transfers and database.
    pub async fn close(&self) -> Result<(), SyncError> {
        let (send, receive) = oneshot::channel();
        self.commands.send(Command::Close(send)).map_err(|_| self.gone())?;
        receive.await.map_err(|_| self.gone())?
    }
}
/// Coven's own operation whose last attempt failed, if any, with the cause
/// its record keeps: it retries under its own backoff, but the pass that
/// left it did not end synced (E5).
async fn failing(sync: &SequenceSync) -> Result<Option<crate::PendingRecord>, SyncError> {
    let records = sync.database.pending::<crate::PendingRecord>().await?;
    Ok(records.into_iter().find(|record| {
        let coven_owned = matches!(
            record.subject,
            crate::PendingSubject::Operation { kind, .. } if kind.coven_owned()
        );
        coven_owned
            && matches!(
                record.reason.retry_cause(),
                crate::PendingReason::Storage { .. }
                    | crate::PendingReason::Refused(_)
                    | crate::PendingReason::Failed(_)
            )
    }))
}

/// Complete a pass and member-key custody (§11.5).
async fn pass(
    sync: &SequenceSync,
    codes: &crate::RestoreCodes,
) -> Result<Option<crate::PendingRecord>, SyncError> {
    sync.pass().await?;
    codes.resume_member_keys().await?;
    failing(sync).await
}

/// How a failed pass ended: a blocked slot, with the cause its record
/// keeps, an attempt that received no provider response, or the failure
/// itself (E5).
fn failed(sync: &SequenceSync, error: SyncError) -> PassFailure {
    let blocked = match sync.state.status() {
        SequenceStatus::Blocked(record) => Some(*record),
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
    PassFailure::Blocked { slot, reason: record.reason, error: record.cause }
}

/// `error`, once a bug it carries has gone to the store's bug sink: nobody
/// waits for the loop, so its failures reach no caller (E15).
fn reported(sync: &SequenceSync, error: impl Into<SyncError>) -> SyncError {
    coven_foundation::task::reported(sync.database.bugs().as_ref(), error.into())
}

/// Write how sync last failed outside a pass's end, in the step that fails.
/// A failed write has failed the store's handle, which every call and live
/// query now report, so the loop ends on it.
async fn show(sync: &SequenceSync, failure: LastPass) -> Result<(), DbError> {
    failure.record(&sync.database).await
}

/// Write how the pass ended, `now` by the store's clock, and that it moves
/// no more. A failed write has failed the store's handle, as `show`'s does.
async fn ended(sync: &SequenceSync, last: LastPass) -> Result<(), DbError> {
    last.record_end(&sync.database, sync.clock.now()).await
}

/// Run the pass, writing that it moved as it first reads a slot it hadn't,
/// or claims one: until then the sync state shows what it did before, so a
/// pass that finds nothing shows nothing (§19.1). A failed write has failed
/// the store's handle; the pass still finishes, and the failure comes back
/// beside its result.
async fn moving_pass(
    sync: &SequenceSync,
    codes: &crate::RestoreCodes,
) -> (Result<Option<crate::PendingRecord>, SyncError>, Result<(), DbError>) {
    let mut moves = sync.state.pass_begins();
    let passing = pass(sync, codes);
    tokio::pin!(passing);
    tokio::select! {
        biased;
        passed = &mut passing => (passed, Ok(())),
        () = moves.moved() => {
            let written = sync.database.record_moving().await;
            (passing.await, written)
        }
    }
}

/// Sync stopped as the loop ends or gives up: the waiting retries hear so.
/// A failed write has failed the handle and answered them with its failure,
/// which every call reports, so the loop goes on to end either way.
async fn stopped(waits: &Arc<RetryWaits>) {
    if let Err(error) = waits.sync_stopped().await {
        tracing::debug!("stopping sync on a failed handle: {error}");
    }
}

/// Wait until `interval` after `from`, by the store's monotonic clock.
fn idle_wait(
    sync: &SequenceSync,
    from: Duration,
    interval: Duration,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    sync.clock.sleep((from + interval).saturating_sub(sync.clock.elapsed()))
}

async fn run(
    sync: Arc<SequenceSync>,
    codes: crate::RestoreCodes,
    mut commands: mpsc::UnboundedReceiver<Command>,
) {
    let mut changes = sync.database.sync_changes();
    let mut rearmed = sync.waits.rearmed();
    let mut retry = match crate::ConnectionRetry::open(&sync.database).await {
        Ok(retry) => retry,
        Err(error) => crate::ConnectionRetry::Never(crate::connection_failure(error)),
    };
    let held = retry.held(&sync.waits);
    if sync.waits.holds(held).await.is_err() {
        return;
    }
    if let Some(failure) = held {
        if show(&sync, failure.clone().into()).await.is_err() {
            stopped(&sync.waits).await;
            return;
        }
    }
    let mut started = false;
    let mut paused = false;
    let mut pending = false;
    // An idle pass comes the idle interval after the last pass ended, or
    // after the loop began; an app holding a shorter one moves it (E5).
    let mut interval = sync.state.idle_interval();
    let mut idle_from = sync.clock.elapsed();
    let mut idle = idle_wait(&sync, idle_from, interval.current());
    let mut closing = None;
    loop {
        // The network report ends the connection's and operations'
        // waits for the network, and the person's retry all their waits; each
        // asks for a pass. Reports during a pass are handled after it, so
        // they end the waits it failed into (§6.7).
        if !paused {
            match sync.waits.end_reported_waits(crate::NetworkOwner::Connection).await {
                Ok(reported) => pending |= reported && started,
                Err(error) => {
                    #[expect(
                        clippy::let_underscore_must_use,
                        reason = "the loop ends either way; a failed write has failed the handle, which reports it"
                    )]
                    let _ = show(&sync, LastPass::failed(reported(&sync, error))).await;
                    break;
                }
            }
        }
        // The cooldown's end wakes the loop to write it. Taken before the
        // write below, so a cooldown that write still finds live, but that
        // ends before the loop waits, wakes it at once instead of leaving
        // the row saying it holds.
        let cooldown = sync.waits.cooldown_wake();
        // A pass held by a cooldown, or by a failure only the person can
        // clear, answers the retries acted on at once (IO `held_answers`),
        // and what a retry would do is written again, with a cooldown that
        // ended, paused too.
        if started && sync.waits.answer_held(retry.held(&sync.waits)).await.is_err() {
            break;
        }
        // A requested pass waits only for the connection's backoff; an idle
        // loop also wakes for operations' waits and their evidence.
        let mut wakes = if pending { Vec::new() } else { sync.waits.evidence_wakes() };
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
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "the loop ends either way; a failed write has failed the handle, which reports it"
                )]
                let _ = show(&sync, LastPass::failed(reported(&sync, error))).await;
                break;
            }
        };
        let due = !paused
            && retry.held(&sync.waits).is_none()
            && sync.waits.due(RetrySubject::Sync, previous.as_ref());
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
                            }
                            Err(error) => {
                                stopped(&sync.waits).await;
                                if show(&sync, LastPass::failed(reported(&sync, error))).await.is_err() { break; }
                            }
                        }
                    }
                }
                Some(Command::Now) => pending |= started,
                Some(Command::Pause(reply)) => {
                    paused = true;
                    #[expect(
                        clippy::let_underscore_must_use,
                        reason = "the caller may have stopped waiting for the pause"
                    )]
                    let _ = reply.send(());
                }
                Some(Command::Resume) => paused = false,
                Some(Command::Stop) => {
                    started = false;
                    pending = false;
                    // No caller waits for stopping; a failure is recorded
                    // as the last pass's.
                    if let Err(error) = sync.files.set_storage(None, std::future::ready(Ok(()))).await {
                        if show(&sync, LastPass::failed(reported(&sync, error))).await.is_err() { break; }
                    }
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
                    // Nobody is left to receive the outcome, so a failure
                    // to release the transfers is logged.
                    if started && !paused {
                        let released = sync.files.set_storage(None, std::future::ready(Ok(())));
                        if let Err(error) = released.await {
                            tracing::warn!("releasing transfers without a handle failed: {error}");
                        }
                    }
                    break;
                }
            },
            changed = changes.next(), if started && !paused => match changed {
                Ok(()) => pending = true,
                Err(DbError::StoreClosed) => break,
                Err(error) => {
                    started = false;
                    stopped(&sync.waits).await;
                    if show(&sync, LastPass::failed(reported(&sync, error))).await.is_err() { break; }
                }
            },
            () = interval.changed() => idle = idle_wait(&sync, idle_from, interval.current()),
            _ = &mut idle, if started && !paused && !pending => pending = true,
            _ = sync.state.pass_requested(), if started && !paused && !pending => pending = true,
            // A wait armed outside this loop changes the schedule to wake on.
            changed = rearmed.changed(), if started && !paused => if changed.is_err() { break },
            _ = work_wake, if started && !paused && !(pending && due) => pending = true,
            // The cooldown's end, which the loop writes as it plans again,
            // lets a requested pass run, and changes what the person's
            // retry would do even with no pass requested or while paused.
            _ = async {
                if let Some(wake) = cooldown { wake.await } else { std::future::pending::<()>().await }
            }, if started => {}
            _ = std::future::ready(()), if started && !paused && pending && due => {
                pending = false;
                if let Err(error) = changes.take_changed() {
                    #[expect(clippy::let_underscore_must_use, reason = "the loop ends either way; a failed write has failed the handle, which reports it")]
                    let _ = show(&sync, LastPass::failed(reported(&sync, error))).await;
                    break;
                }
                // The pass answers the retries acted on before it began
                // (IO `pass_answers_acted_on`). It begins only as the
                // retry effect's row lets it: a cooldown that began since
                // the plan holds it, and the loop plans again with the pass
                // still requested.
                let requests = match sync.waits.begin_pass().await {
                    Ok(Some(requests)) => requests,
                    Ok(None) => {
                        pending = true;
                        continue;
                    }
                    Err(_) => break,
                };
                let connection_version = sync.waits.connection_version();
                let (passed, moving) = moving_pass(&sync, &codes).await;
                if let Err(error) = moving {
                    requests.answer(&PassFailure::Failed { error: reported(&sync, error).into() }.into());
                    break;
                }
                // The idle interval counts from the pass's end, before the
                // steps that record how it ended.
                idle_from = sync.clock.elapsed();
                let outcome = match passed {
                    Ok(failing) => match retry.succeeded(&sync.database, &sync.waits).await {
                        Ok(()) => match failing {
                            None => {
                                let finished_at = sync.clock.now();
                                if let Err(error) = LastPass::Synced.record_end(&sync.database, finished_at).await {
                                    requests.answer(&PassFailure::Failed { error: reported(&sync, error).into() }.into());
                                    break;
                                }
                                RetryOutcome::Synced { finished_at }
                            }
                            Some(record) => {
                                // Work that never clears stops sync until a
                                // person resets it (E5).
                                started = record.retry() != crate::Retry::Never;
                                pending = false;
                                PassFailure::Failed { error: SyncFailure::Operation(Box::new(record)) }.into()
                            }
                        },
                        Err(error) => {
                            started = false;
                            PassFailure::Failed { error: reported(&sync, error).into() }.into()
                        }
                    },
                    Err(error) => {
                        let error = reported(&sync, error);
                        let reason = sync.error_reason(&error);
                        match retry.failed(&sync.database, &sync.waits, reason, connection_version, error, |error| failed(&sync, error)).await {
                            Ok(failure) => {
                                pending = true;
                                failure.into()
                            }
                            Err(failure) => {
                                started = false;
                                failure.into()
                            }
                        }
                    }
                };
                // What a retry would do changes with the pass's failure, or
                // with sync stopping, before the row records it or the
                // person hears it.
                if let Err(error) = sync.waits.holds(retry.held(&sync.waits)).await {
                    requests.answer(&PassFailure::Failed { error: reported(&sync, error).into() }.into());
                    break;
                }
                if !started {
                    stopped(&sync.waits).await;
                }
                if let RetryOutcome::Failed(failure) = &outcome {
                    if let Err(error) = ended(&sync, failure.clone().into()).await {
                        requests.answer(&PassFailure::Failed { error: reported(&sync, error).into() }.into());
                        break;
                    }
                }
                requests.answer(&outcome);
                idle = idle_wait(&sync, idle_from, interval.current());
            }
        }
    }
    // Closing hands waiting retries to the connection that replaces this
    // one, or its owner answers them as it releases; any other end leaves no
    // pass to answer them.
    if closing.is_none() {
        stopped(&sync.waits).await;
    }
    // The connection's owners are released before the closer learns so.
    drop(changes);
    drop(codes);
    drop(sync);
    if let Some((reply, result)) = closing {
        // A closer that stopped waiting leaves a failure to the log.
        if let Err(Err(error)) = reply.send(result) {
            tracing::warn!("closing sync failed after its caller stopped waiting: {error}");
        }
    }
}

#[cfg(test)]
#[path = "sync_loop_tests.rs"]
mod tests;
