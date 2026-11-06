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
    /// Why the loop ended, once it has: what a command to it fails with
    /// from then on (E15).
    ended: Arc<std::sync::OnceLock<LoopEnd>>,
    /// Each time the loop has taken everything that can wake it and waits.
    #[cfg(any(test, feature = "test-utils"))]
    parks: crate::Parks,
    /// How many pauses have been asked of the loop.
    #[cfg(any(test, feature = "test-utils"))]
    pauses: Arc<tokio::sync::watch::Sender<usize>>,
}
/// Why a sync loop ended (E15).
#[derive(Clone)]
enum LoopEnd {
    /// It panicked, or its own work hit a bug, which the sink heard.
    Bug(coven_foundation::task::Bug),
    /// Its own work failed otherwise, as a write does once it has failed
    /// the handle.
    Failed { failure: crate::LocalFailure, source: Arc<dyn std::error::Error + Send + Sync> },
    /// Its store closed.
    StoreClosed,
    /// Its owner closed it as it released the connection, or dropped every
    /// handle to it.
    Released,
}

impl LoopEnd {
    /// The loop ends on `error` from its own work, whose bug, if any, has
    /// gone to the sink; with it, how the last pass shows `error`.
    fn on(error: SyncError) -> (Self, SyncFailure) {
        use coven_foundation::task::CarriesBug;
        if let Some(bug) = error.bug().cloned() {
            return (Self::Bug(bug), error.into());
        }
        let failure = error.local_failure();
        let shown = SyncFailure::from(error);
        let source: Arc<dyn std::error::Error + Send + Sync> = match &shown {
            SyncFailure::Other(source) => source.clone(),
            SyncFailure::Storage(source) => source.clone(),
            shown => Arc::new(shown.clone()),
        };
        (Self::Failed { failure, source }, shown)
    }

    /// What a command to the ended loop fails with.
    fn error(&self) -> SyncError {
        match self {
            Self::Bug(bug) => SyncError::WorkerFailed(bug.clone()),
            Self::Failed { failure, source } => {
                SyncError::Failed { failure: *failure, source: source.clone() }
            }
            Self::StoreClosed => DbError::StoreClosed.into(),
            Self::Released => SyncError::NoStorage,
        }
    }
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
    /// them, are answered with it as `SyncError::WorkerFailed` (E15).
    pub fn new(sync: Arc<SequenceSync>, codes: crate::RestoreCodes) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let waits = sync.waits.clone();
        // Weak, so the connection's owners go when the loop lets them go.
        let owner = Arc::downgrade(&sync);
        let ended = Arc::new(std::sync::OnceLock::new());
        let unwound = ended.clone();
        let parks = crate::Parks::new();
        // The loop catches a panic in its work itself; this one hears a
        // panic in how the loop ends.
        drop(coven_foundation::task::spawn_owned(
            "the sync loop",
            sync.database.bugs().clone(),
            run(sync, codes, receiver, parks.clone(), ended.clone()),
            |bug| async move {
                unwound.get_or_init(|| LoopEnd::Bug(bug.clone()));
                if let Some(owner) = owner.upgrade() {
                    panicked(&owner, bug).await;
                }
            },
        ));
        Self {
            commands,
            waits,
            ended,
            #[cfg(any(test, feature = "test-utils"))]
            parks,
            #[cfg(any(test, feature = "test-utils"))]
            pauses: Arc::new(tokio::sync::watch::channel(0).0),
        }
    }

    /// Each time the loop has taken every command, change and wake that
    /// was ready, and waits for the next.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn parked_from_now(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        self.parks.parked_from_now()
    }

    /// Wait until a pause is asked of the loop after this call; the pause
    /// then waits for the loop to take it, after any pass under way.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn pause_asked_from_now(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut pauses = self.pauses.subscribe();
        let now = *pauses.borrow_and_update();
        async move {
            coven_foundation::clock::within(pauses.wait_for(|pauses| *pauses > now))
                .await
                .expect("a pause is asked")
                .expect("the loop's pauses are kept");
        }
    }

    /// Why a command found the loop gone: a bug it ended on, which the
    /// sink heard already, as `WorkerFailed`; another failure of its own
    /// work; its store closing; or its connection released (E15).
    fn gone(&self) -> SyncError {
        match self.ended.get() {
            Some(end) => end.error(),
            // The loop records why it ends before it lets its commands go,
            // except on a panic: the panic drops them as it unwinds, before
            // its bug is recorded, so a command in between hears this.
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
    /// loop stops even when writing that fails, and the call returns why.
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
        #[cfg(any(test, feature = "test-utils"))]
        self.pauses.send_modify(|pauses| *pauses += 1);
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
    codes.resume_member_keys(sync).await?;
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

/// `result` of a database call the loop makes on its own, once a bug its
/// failure carries has gone to the store's bug sink: no caller hears of it
/// (E15). Any other failure of the loop's own writes has failed the
/// store's handle, which every call and live query now report.
fn own_write<T>(sync: &SequenceSync, result: Result<T, DbError>) -> Result<T, DbError> {
    result.map_err(|error| coven_foundation::task::reported(sync.database.bugs().as_ref(), error))
}

/// Write how sync last failed outside a pass's end, in the step that fails.
/// A failed write ends the loop: its bug has gone to the sink, or it has
/// failed the store's handle.
async fn show(sync: &SequenceSync, failure: LastPass) -> Result<(), DbError> {
    own_write(sync, failure.record(&sync.database).await)
}

/// Write how the pass ended, `now` by the store's clock, and that it moves
/// no more. A failed write ends the loop, as `show`'s does.
async fn ended(sync: &SequenceSync, last: LastPass) -> Result<(), DbError> {
    own_write(sync, last.record_end(&sync.database, sync.clock.now()).await)
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

/// The loop's own work failed with `error`: its bug goes to the sink, the
/// last pass shows it, and the loop ends on it (E15).
async fn give_up(sync: &SequenceSync, error: impl Into<SyncError>) -> LoopEnd {
    let (end, shown) = LoopEnd::on(reported(sync, error));
    #[expect(
        clippy::let_underscore_must_use,
        reason = "the loop ends either way; a bug has gone to the sink, and any other failed write has failed the handle, which reports it"
    )]
    let _ = show(sync, LastPass::failed(shown)).await;
    end
}

/// Sync stopped as the loop ends or gives up: the waiting retries hear so.
/// A failed write has answered them with its failure, and its bug has gone
/// to the sink or it has failed the handle, so the loop goes on to end
/// either way.
async fn stopped(sync: &SequenceSync) {
    if let Err(error) = own_write(sync, sync.waits.sync_stopped().await) {
        tracing::debug!("stopping sync failed: {error}");
    }
}

/// Wait until `interval` after `from`, by the store's monotonic clock.
fn idle_wait(
    sync: &SequenceSync,
    from: Duration,
    interval: Duration,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    sync.clock.deadline((from + interval).saturating_sub(sync.clock.elapsed()))
}

/// The loop panicked with `bug`, which has gone to the sink: the last pass
/// is recorded failed with it, sync stops, and the retries waiting, those
/// the panicking pass took among them, are answered with it as
/// `SyncError::WorkerFailed` (E15).
async fn panicked(sync: &SequenceSync, bug: coven_foundation::task::Bug) {
    #[expect(
        clippy::let_underscore_must_use,
        reason = "the loop has ended either way; a bug has gone to the sink, and any other failed write has failed the handle, which reports it"
    )]
    let _ = show(sync, LastPass::failed(SyncError::WorkerFailed(bug.clone()))).await;
    if let Err(error) = own_write(sync, sync.waits.sync_panicked(bug).await) {
        tracing::debug!("stopping sync after the loop's panic failed: {error}");
    }
}

/// Run the loop, catching a panic in its work while it still holds its
/// commands and a closer's reply: it records why it ended before it lets
/// either go, so each hears that, a panic's bug as `WorkerFailed` (E15).
async fn run(
    sync: Arc<SequenceSync>,
    codes: crate::RestoreCodes,
    mut commands: mpsc::UnboundedReceiver<Command>,
    parks: crate::Parks,
    why_ended: Arc<std::sync::OnceLock<LoopEnd>>,
) {
    let _running = parks.running();
    let bugs = sync.database.bugs().clone();
    let owner = sync.clone();
    let mut closing = None;
    let served = coven_foundation::task::caught(
        "the sync loop",
        serve(sync, codes, &mut commands, &parks, &mut closing),
    )
    .await;
    let (end, closed) = match served {
        Ok(served) => served,
        Err(bug) => {
            coven_foundation::task::report(bugs.as_ref(), &bug);
            panicked(&owner, bug.clone()).await;
            (LoopEnd::Bug(bug.clone()), Err(SyncError::WorkerFailed(bug)))
        }
    };
    // The connection's owners are released before the closer learns so.
    drop(owner);
    // Commands from now on hear why the loop ended; those it never took
    // go as `commands` does, after this.
    why_ended.get_or_init(|| end);
    if let Some(reply) = closing {
        // A closer that stopped waiting leaves a bug to the sink and any
        // other failure to the log.
        if let Err(Err(error)) = reply.send(closed) {
            if !coven_foundation::task::report_carried(bugs.as_ref(), &error) {
                tracing::warn!("closing sync failed after its caller stopped waiting: {error}");
            }
        }
    }
}

/// The loop's work: why it ended, and how a closer's release of its
/// transfers went. A closer's reply goes in `closing` as the loop takes it.
async fn serve(
    sync: Arc<SequenceSync>,
    codes: crate::RestoreCodes,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    parks: &crate::Parks,
    closing: &mut Option<oneshot::Sender<Result<(), SyncError>>>,
) -> (LoopEnd, Result<(), SyncError>) {
    let mut changes = sync.database.sync_changes();
    let mut rearmed = sync.waits.rearmed();
    let mut retry = match crate::ConnectionRetry::open(&sync.database).await {
        Ok(retry) => retry,
        Err(error) => {
            crate::ConnectionRetry::Never(crate::connection_failure(reported(&sync, error)))
        }
    };
    let mut closed = Ok(());
    let end = 'run: {
        let held = retry.held(&sync.waits);
        if let Err(error) = sync.waits.holds(held).await {
            break 'run give_up(&sync, error).await;
        }
        if let Some(failure) = held {
            if let Err(error) = show(&sync, failure.clone().into()).await {
                break 'run LoopEnd::on(error.into()).0;
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
        loop {
            // The network report ends the connection's and operations'
            // waits for the network, and the person's retry all their waits; each
            // asks for a pass. Reports during a pass are handled after it, so
            // they end the waits it failed into (§6.7).
            if !paused {
                match sync.waits.end_reported_waits(crate::NetworkOwner::Connection).await {
                    Ok(reported) => pending |= reported && started,
                    Err(error) => break give_up(&sync, error).await,
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
            if started {
                if let Err(error) = sync.waits.answer_held(retry.held(&sync.waits)).await {
                    break give_up(&sync, error).await;
                }
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
                Err(error) => break give_up(&sync, error).await,
            };
            let due = !paused
                && retry.held(&sync.waits).is_none()
                && sync.waits.due(RetrySubject::Sync, previous.as_ref());
            tokio::select! {
                biased;
                never = parks.polling() => match never {},
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
                                    stopped(&sync).await;
                                    if let Err(error) = show(&sync, LastPass::failed(reported(&sync, error))).await { break LoopEnd::on(error.into()).0; }
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
                            if let Err(error) = show(&sync, LastPass::failed(reported(&sync, error))).await { break LoopEnd::on(error.into()).0; }
                        }
                    }
                    // Transfers outlive this connection's loop; the store's
                    // owner closes them. This connection's transfers finish first.
                    Some(Command::Close(reply)) => {
                        // Held outside the work, so a panic below answers it.
                        *closing = Some(reply);
                        if started && !paused {
                            closed = sync.files.set_storage(None, std::future::ready(Ok(()))).await;
                        }
                        break LoopEnd::Released;
                    }
                    None => {
                        // Nobody is left to receive the outcome, so a failure
                        // to release the transfers is logged.
                        if started && !paused {
                            let released = sync.files.set_storage(None, std::future::ready(Ok(())));
                            if let Err(error) = released.await {
                                if !coven_foundation::task::report_carried(sync.database.bugs().as_ref(), &error) {
                                    tracing::warn!("releasing transfers without a handle failed: {error}");
                                }
                            }
                        }
                        break LoopEnd::Released;
                    }
                },
                changed = changes.next(), if started && !paused => match changed {
                    Ok(()) => pending = true,
                    Err(DbError::StoreClosed) => break LoopEnd::StoreClosed,
                    Err(error) => {
                        started = false;
                        stopped(&sync).await;
                        if let Err(error) = show(&sync, LastPass::failed(reported(&sync, error))).await { break LoopEnd::on(error.into()).0; }
                    }
                },
                () = interval.changed() => idle = idle_wait(&sync, idle_from, interval.current()),
                _ = &mut idle, if started && !paused && !pending => pending = true,
                _ = sync.state.pass_requested(), if started && !paused && !pending => pending = true,
                // A wait armed outside this loop changes the schedule to wake on.
                // The waits, which send it, go only with the store.
                changed = rearmed.changed(), if started && !paused => if changed.is_err() { break LoopEnd::StoreClosed },
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
                        break give_up(&sync, error).await;
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
                        Err(error) => break give_up(&sync, error).await,
                    };
                    let connection_version = sync.waits.connection_version();
                    let (passed, moving) = moving_pass(&sync, &codes).await;
                    if let Err(error) = moving {
                        let (end, shown) = LoopEnd::on(reported(&sync, error));
                        requests.answer(&PassFailure::Failed { error: shown }.into());
                        break end;
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
                                        let (end, shown) = LoopEnd::on(reported(&sync, error));
                                        requests.answer(&PassFailure::Failed { error: shown }.into());
                                        break end;
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
                        let (end, shown) = LoopEnd::on(reported(&sync, error));
                        requests.answer(&PassFailure::Failed { error: shown }.into());
                        break end;
                    }
                    if !started {
                        stopped(&sync).await;
                    }
                    if let RetryOutcome::Failed(failure) = &outcome {
                        // `ended` has sent a bug it failed on to the sink.
                        if let Err(error) = ended(&sync, failure.clone().into()).await {
                            let (end, shown) = LoopEnd::on(error.into());
                            requests.answer(&PassFailure::Failed { error: shown }.into());
                            break end;
                        }
                    }
                    requests.answer(&outcome);
                    idle = idle_wait(&sync, idle_from, interval.current());
                }
                never = parks.parked() => match never {},
            }
        }
    };
    // Closing hands waiting retries to the connection that replaces this
    // one, or its owner answers them as it releases; any other end leaves no
    // pass to answer them.
    if closing.is_none() {
        stopped(&sync).await;
    }
    (end, closed)
}

#[cfg(test)]
#[path = "sync_loop_tests.rs"]
mod tests;
