//! The upload worker's runs (§17.9, E7). While uploads can run, a run keeps
//! as many attempts in flight as the upload limit allows, in each of two
//! lanes: bound uploads, which writes wait for, and trailing ones (§17.4).
//! As each one ends, the next eligible upload of its lane starts, files
//! queued meanwhile included; no upload waits for the others to finish, and
//! no bound upload for a trailing one. Pausing holds the trailing lane only.
//! An app's early retry joins the run in progress.

use crate::{
    files::file_failure::upload_subject, files::FilesInner, files::TrailingHold,
    retry_waits::Ending, NetworkOwner, PendingRecord, Retry, SyncError,
};
use coven_database::{DbError, FileUpload, RetrySubject};
use coven_foundation::locks::{lock_unpoisoned, read_unpoisoned};
use coven_storage::StorageFailure;
use futures_util::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::atomic::Ordering,
    time::Duration,
};
use tokio::sync::oneshot;

/// Result of an app retry (`retry_uploads_now`), or of one upload run.
/// Individual failures stay in `pending()`.
#[derive(Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// Every upload the retry covered was attempted; this many stored a
    /// file.
    Drained {
        /// Uploads whose bytes storage confirmed for files waiting for them.
        /// One whose move failed or was cancelled while it was sent stored no
        /// file, and doesn't count (§17.3).
        uploaded: usize,
    },
    /// No waiting upload exists.
    QueueEmpty,
    /// Uploads were paused: no trailing upload was tried. Each bound upload
    /// the retry covered was, since the pause holds none (§17.4).
    Paused,
    /// The network is metered and the app moves files only on unmetered
    /// networks: no trailing upload was tried. Each bound upload the retry
    /// covered was, since the setting holds none (§17.9).
    Metered,
    /// Sync is stopped: no upload was tried (§17.4).
    NotSyncing,
}

/// An app's early retry, waiting for its answer. A run takes it when it
/// next reads the queue: the retry then covers every upload that can ever
/// retry, each eligible whatever its wait, and is answered once each covered
/// upload's attempt has ended.
pub(super) struct AppRetry {
    /// The queue ids it covers, fixed when a run takes it; each leaves as its
    /// attempt ends. `None` until taken.
    covers: Option<BTreeSet<i64>>,
    /// How many covered uploads were stored.
    uploaded: usize,
    /// Its place among the retries asked, which says whether a run's reads
    /// of the queue and the holds came after it.
    asked: u64,
    reply: oneshot::Sender<Result<DrainOutcome, SyncError>>,
}

/// The answer to an app's early retry: how its covered uploads' attempts
/// ended, or nothing if the worker stopped with the store first.
pub(super) struct UploadRetryAnswer(oneshot::Receiver<Result<DrainOutcome, SyncError>>);

impl UploadRetryAnswer {
    /// Wait for the answer; the store closed if the worker stopped first.
    pub(super) async fn answered(self) -> Result<DrainOutcome, SyncError> {
        self.0.await.unwrap_or_else(|_| Err(DbError::StoreClosed.into()))
    }

    /// The answer if the run already gave it; `None` when it did not.
    #[cfg(test)]
    pub(super) fn try_answered(&mut self) -> Option<Result<DrainOutcome, SyncError>> {
        self.0.try_recv().ok()
    }
}

/// Why the upload worker ended, which each app retry from then on hears.
#[derive(Debug)]
pub(super) enum WorkerEnd {
    /// It panicked with this bug, which the store's bug sink heard: each
    /// retry hears it as `SyncError::WorkerFailed` (E15).
    Panicked(coven_foundation::task::Bug),
    /// The store closed under it.
    Closed,
    /// What it watches failed, as when the handle fails, with one shared
    /// cause.
    Failed(crate::LocalFailure, std::sync::Arc<dyn std::error::Error + Send + Sync>),
}

impl WorkerEnd {
    /// The end of a worker that stopped on `error`, the failure of what it
    /// watches.
    pub(super) fn of(error: SyncError) -> Self {
        match error {
            SyncError::Database(DbError::StoreClosed) => Self::Closed,
            error => {
                let (failure, source) = error.into_shared();
                Self::Failed(failure, source)
            }
        }
    }

    fn error(&self) -> SyncError {
        match self {
            Self::Panicked(bug) => SyncError::WorkerFailed(bug.clone()),
            Self::Closed => DbError::StoreClosed.into(),
            Self::Failed(failure, source) => {
                SyncError::Failed { failure: *failure, source: source.clone() }
            }
        }
    }
}

/// The lane an upload runs in (§17.4): a bound upload's write and every
/// later one wait for it, so neither the pause nor trailing uploads hold it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Lane {
    Bound,
    Trailing,
}

impl Lane {
    pub(super) fn of(upload: &FileUpload) -> Self {
        match upload.holds {
            Some(_) => Self::Bound,
            None => Self::Trailing,
        }
    }
    pub(super) fn index(self) -> usize {
        match self {
            Self::Bound => 0,
            Self::Trailing => 1,
        }
    }
}

/// Why no upload may start now.
#[derive(Clone, Copy, Debug)]
enum Blocked {
    Closed,
    /// Sync is stopped, which stops both lanes (§17.4).
    NotSyncing,
    NoStorage,
    /// The pause or a metered network holds trailing uploads, and no bound
    /// upload waits.
    Held(TrailingHold),
    /// Files wait for the sequence to catch up (§5.4).
    CatchUp,
    /// The provider asked every consumer to wait this long (§6.7).
    Cooling(Duration),
    /// A connection change waits for the uploads in flight to end.
    Changing,
}

impl Blocked {
    /// What an app retry learns; `None` keeps it for the run after a
    /// connection change, which serves it on the new connection.
    fn retry_answer(self) -> Option<Result<DrainOutcome, SyncError>> {
        Some(match self {
            Self::Closed => Err(DbError::StoreClosed.into()),
            Self::NotSyncing => Ok(DrainOutcome::NotSyncing),
            Self::NoStorage => Err(SyncError::NoStorage),
            Self::Held(TrailingHold::Paused) => Ok(DrainOutcome::Paused),
            Self::Held(TrailingHold::Metered) => Ok(DrainOutcome::Metered),
            // As for an app write (§12.3).
            Self::Held(TrailingHold::MemberKeysChanging) => Err(DbError::MemberKeysChanging.into()),
            Self::CatchUp => Err(StorageFailure::CatchUpRequired.into()),
            // An app retry overrides ordinary backoff, never a provider cooldown.
            Self::Cooling(remaining) => Err(StorageFailure::RateLimited
                .with_source(coven_storage::CoolingDown { remaining })
                .into()),
            Self::Changing => return None,
        })
    }

    /// What the run itself returns. Automatic work waits out a cooldown, a
    /// connection change and a self-replacement's write pause without
    /// failing.
    fn run_outcome(self, uploaded: usize) -> Result<DrainOutcome, SyncError> {
        match self {
            Self::Cooling(_) | Self::Changing | Self::Held(TrailingHold::MemberKeysChanging) => {
                Ok(DrainOutcome::Drained { uploaded })
            }
            blocked => blocked.retry_answer().unwrap_or(Ok(DrainOutcome::Drained { uploaded })),
        }
    }
}

type Attempt<'a> = BoxFuture<'a, (i64, Result<super::file_upload::Attempted, SyncError>)>;

/// One run's scheduling state.
#[derive(Default)]
struct Run<'a> {
    /// Attempts in flight, each yielding its queue id and result.
    attempts: FuturesUnordered<Attempt<'a>>,
    /// The network reports seen as each attempt in flight started.
    started: BTreeMap<i64, u64>,
    /// The lane of each attempt in flight.
    lanes: BTreeMap<i64, Lane>,
    /// The bound uploads of the last read of the queue, whose retries wake
    /// the run while trailing uploads are held.
    bound: BTreeSet<i64>,
    /// A commit since that read may have queued a bound upload.
    committed: bool,
    /// Eligible uploads from the last read of the queue, oldest first, not
    /// started yet.
    ready: VecDeque<FileUpload>,
    /// Something may have made more uploads eligible since that read.
    stale: bool,
    /// Uploads whose attempt failed after a report arrived while it ran,
    /// with which waits those reports end; they end the wait each failed
    /// into.
    crossed: BTreeMap<RetrySubject, Ending>,
    /// Files this run stored.
    uploaded: usize,
}

impl Run<'_> {
    fn in_flight(&self, lane: Lane) -> usize {
        self.lanes.values().filter(|of| **of == lane).count()
    }

    /// The connections `lane` uses: one for each attempt in flight, and
    /// those its attempts took beside their own for parts sent together.
    fn used(&self, lane: Lane, owner: &FilesInner) -> usize {
        self.in_flight(lane) + owner.connections_beside(lane)
    }

    /// Whether an upload of some lane a hold leaves open could start: a
    /// lane with room, and for the bound lane, a bound upload the last read
    /// found or a commit since may have queued.
    fn room(&self, limit: usize, held: bool, owner: &FilesInner) -> bool {
        (self.used(Lane::Bound, owner) < limit && (self.committed || !self.bound.is_empty()))
            || (!held && self.used(Lane::Trailing, owner) < limit)
    }
}

impl FilesInner {
    /// Queue an app retry for the upload worker, and wake it. A worker that
    /// panicked serves none: the retry fails with its bug, as
    /// `SyncError::WorkerFailed`.
    pub(super) fn ask_retry(&self) -> Result<UploadRetryAnswer, SyncError> {
        let (reply, answer) = oneshot::channel();
        {
            // Closing sets the flag before clearing this list under its lock,
            // and the worker's panic sets its bug before answering it, so a
            // retry queued here is either cleared, answered or refused.
            let mut retries = lock_unpoisoned(&self.app_retries);
            self.check_open()?;
            if let Some(ended) = self.worker_ended.get() {
                return Err(ended.error());
            }
            let asked = self.retry_asked();
            retries.push(AppRetry { covers: None, uploaded: 0, asked, reply });
        }
        self.upload_wake.notify_one();
        Ok(UploadRetryAnswer(answer))
    }

    /// The upload worker ended, on a panic or on the failure of what it
    /// watches: no run will serve an app retry, so each waiting one is
    /// answered with why, as each asked from now on is (E15).
    pub(super) fn worker_ended(&self, end: WorkerEnd) {
        let mut retries = lock_unpoisoned(&self.app_retries);
        let ended = self.worker_ended.get_or_init(|| end);
        for retry in retries.drain(..) {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a retry whose caller stopped waiting needs no answer"
            )]
            let _ = retry.reply.send(Err(ended.error()));
        }
    }

    /// Answer every queued app retry with this; `None` keeps them all, as
    /// they are, for the next run.
    fn answer_retries(&self, answer: impl Fn() -> Option<Result<DrainOutcome, SyncError>>) {
        let mut retries = lock_unpoisoned(&self.app_retries);
        if answer().is_none() {
            return;
        }
        for retry in retries.drain(..) {
            if let Some(result) = answer() {
                // The caller may have stopped waiting; nothing is lost.
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "a retry whose caller stopped waiting needs no answer"
                )]
                let _ = retry.reply.send(result);
            }
        }
    }

    /// A test's run of the upload worker that begins with an app retry of its
    /// own, whose answer it returns.
    #[cfg(test)]
    pub(crate) async fn drain_uploads_retrying(&self) -> Result<DrainOutcome, SyncError> {
        let mut own = self.ask_retry()?;
        let run = self.drain_uploads().await;
        // Unanswered, its covered uploads were not all attempted: the run
        // failed, or a connection change stopped it.
        own.try_answered().unwrap_or(run)
    }

    /// One run of the upload worker: start uploads while they can run,
    /// keeping the upload limit's number in flight, and end once none is in
    /// flight and none can start. A run that fails on a bug returns it to
    /// the worker, which reports it; each waiting app retry hears it as
    /// `WorkerFailed` (E15).
    pub(crate) async fn drain_uploads(&self) -> Result<DrainOutcome, SyncError> {
        use coven_foundation::task::CarriesBug;
        let _run = self.drain.lock().await;
        let result = self.serve_uploads().await;
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) if lock_unpoisoned(&self.app_retries).is_empty() => Err(error),
            Err(error) => {
                if let Some(bug) = error.bug().cloned() {
                    self.answer_retries(|| Some(Err(SyncError::WorkerFailed(bug.clone()))));
                    return Err(error);
                }
                // Every waiting app retry learns the failure that ended the run.
                let (failure, source) = error.into_shared();
                self.answer_retries(|| {
                    Some(Err(SyncError::Failed { failure, source: source.clone() }))
                });
                Err(SyncError::Failed { failure, source })
            }
        }
    }

    /// Why no upload may start now, from the owner's current state.
    fn blocked(&self) -> Option<Blocked> {
        if self.check_open().is_err() {
            Some(Blocked::Closed)
        } else if !self.waits.started() {
            Some(Blocked::NotSyncing)
        } else if read_unpoisoned(&self.storage).is_none() {
            Some(Blocked::NoStorage)
        } else if self.changing.load(Ordering::Acquire) > 0 {
            Some(Blocked::Changing)
        } else if !self.send.files_allowed() {
            Some(Blocked::CatchUp)
        } else {
            self.waits.cooldown().map(Blocked::Cooling)
        }
    }

    fn end_blocked(&self, blocked: Blocked, uploaded: usize) -> Result<DrainOutcome, SyncError> {
        self.answer_retries(|| blocked.retry_answer());
        blocked.run_outcome(uploaded)
    }

    /// End a run that a hold on trailing uploads stops. Each app retry a
    /// read of the queue covered is answered with the hold: a trailing upload
    /// it covers was not tried, and each bound one was, or none waited. One
    /// asked since could cover a bound upload queued after that read, which
    /// the hold doesn't stop, or come after the hold ended, so it waits for
    /// the next run, which reads the queue and the hold again.
    fn end_held(
        &self,
        hold: TrailingHold,
        uploaded: usize,
        covered: impl Fn(&AppRetry) -> bool,
    ) -> Result<DrainOutcome, SyncError> {
        let blocked = Blocked::Held(hold);
        let mut retries = lock_unpoisoned(&self.app_retries);
        let (answered, asked_since): (Vec<_>, Vec<_>) =
            retries.drain(..).partition(|retry| covered(retry));
        *retries = asked_since;
        let waiting = !retries.is_empty();
        drop(retries);
        for retry in answered {
            if let Some(answer) = blocked.retry_answer() {
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "a retry whose caller stopped waiting needs no answer"
                )]
                let _ = retry.reply.send(answer);
            }
        }
        if waiting {
            self.upload_wake.notify_one();
        }
        blocked.run_outcome(uploaded)
    }

    #[cfg(test)]
    fn pass_blocked_end_gate(&self) {
        let gate = lock_unpoisoned(&self.blocked_end_gate).clone();
        if let Some(gate) = gate {
            gate();
        }
    }

    async fn serve_uploads(&self) -> Result<DrainOutcome, SyncError> {
        // Observe before reading the queue, so nothing that happens while it
        // is read is missed.
        let mut changes = self.database.queued_work_changes();
        let mut sending = self.send.watch();
        let mut reports = self.waits.network_reports();
        let mut rearmed = self.waits.rearmed();
        let mut syncing = self.waits.syncing();
        let mut network = self.network.clone();
        if let Some(blocked @ Blocked::Closed) = self.blocked() {
            return self.end_blocked(blocked, 0);
        }
        // Uploads no file waits for any more go first, paused, stopped or
        // with no storage too; an attempt in flight retires its own as it
        // ends (§17.9). One holding a provider session has it aborted
        // first, which needs storage and sync started.
        self.end_abandoned_sessions().await?;
        for id in self
            .database
            .retire_unlinked(self.database.uploads_in_flight().current(), upload_subject)
            .await?
        {
            self.waits.forget(RetrySubject::Upload(id));
            self.forget_connection_wait(RetrySubject::Upload(id));
        }
        if let Some(blocked @ (Blocked::NotSyncing | Blocked::NoStorage)) = self.blocked() {
            return self.end_blocked(blocked, 0);
        }
        // The persisted settings govern; parts check these copies between
        // sends. A change of either waits until this run has acted on what
        // it read, so the run cannot answer a retry asked after a resume
        // with the pause.
        let change = self.lock_hold_change().await;
        // The app retries asked before the reads below, which cover them.
        let read_after = self.retries_asked();
        let paused = self.database.uploads_paused().await?;
        self.set_paused(paused);
        let unmetered_only = self.database.unmetered_only().await?;
        self.unmetered_only.store(unmetered_only, Ordering::Release);
        #[cfg(test)]
        {
            let gate = self.take_settings_read_gate();
            if let Some((read, resume)) = gate {
                read.send(()).expect("the test waits for the read");
                resume.await.expect("the test resumes the run");
            }
        }
        self.read_writes_paused().await?;
        let work = self.database.upload_work::<PendingRecord>(upload_subject).await?;
        // The pause, a self-replacement's write pause and a metered network
        // hold trailing uploads only (§12.3, §17.4, §17.9).
        if let Some(hold) = self.trailing_hold() {
            if work.iter().all(|(item, _)| Lane::of(item) == Lane::Trailing) {
                #[cfg(test)]
                self.pass_blocked_end_gate();
                return self.end_held(hold, 0, |retry| retry.asked < read_after);
            }
        }
        drop(change);
        if work.is_empty() {
            self.answer_retries(|| Some(Ok(DrainOutcome::QueueEmpty)));
            return Ok(DrainOutcome::QueueEmpty);
        }
        let mut run = Run::default();
        let mut work = Some(work);
        loop {
            if let Some(blocked) = self.blocked() {
                if run.attempts.is_empty() {
                    #[cfg(test)]
                    self.pass_blocked_end_gate();
                    return self.end_blocked(blocked, run.uploaded);
                }
            } else {
                self.act_on_reports(&mut run).await;
                let limit = self.transfers.limits().uploads.get();
                if work.is_some() || (run.stale && run.room(limit, self.trailing_held(), self)) {
                    // The first read went stale if a report ended waits since.
                    let work = match work.take() {
                        Some(work) if !run.stale => work,
                        _ => {
                            // A self-replacement's pause starts and ends
                            // with a commit, which made the read stale.
                            self.read_writes_paused().await?;
                            self.database.upload_work::<PendingRecord>(upload_subject).await?
                        }
                    };
                    self.read_ready(&mut run, work);
                }
                self.start_ready(&mut run, limit);
                if run.attempts.is_empty() && run.ready.is_empty() {
                    // Held, the run ends once nothing it read is left to
                    // start and no bound upload could have been queued since.
                    let held = self.trailing_held();
                    if held && (!run.stale || !run.room(limit, held, self)) {
                        #[cfg(test)]
                        self.pass_blocked_end_gate();
                        // As with the read above, a setting's change waits
                        // while the run acts on the hold it saw; after a
                        // resume since the check, the run serves uploads
                        // again.
                        let _change = self.lock_hold_change().await;
                        self.read_writes_paused().await?;
                        if let Some(hold) = self.trailing_hold() {
                            return self
                                .end_held(hold, run.uploaded, |retry| retry.covers.is_some());
                        }
                        run.stale = true;
                        continue;
                    }
                    if !run.stale
                        && lock_unpoisoned(&self.app_retries)
                            .iter()
                            .all(|retry| retry.covers.is_some())
                    {
                        // Nothing a taken retry covers is left to start.
                        for retry in lock_unpoisoned(&self.app_retries).drain(..) {
                            #[expect(
                                clippy::let_underscore_must_use,
                                reason = "a retry whose caller stopped waiting needs no answer"
                            )]
                            let _ = retry
                                .reply
                                .send(Ok(DrainOutcome::Drained { uploaded: retry.uploaded }));
                        }
                        return Ok(DrainOutcome::Drained { uploaded: run.uploaded });
                    }
                    run.stale = true;
                    continue;
                }
            }
            // A due retry matters only while an upload could start; it
            // would wake at once for a wait an attempt in flight serves.
            let open = self.blocked().is_none()
                && run.room(self.transfers.limits().uploads.get(), self.trailing_held(), self);
            let retry = if open { self.next_upload_retry(&run.bound) } else { None };
            tokio::select! {
                Some((id, result)) = run.attempts.next(), if !run.attempts.is_empty() => {
                    self.finished(&mut run, id, result?);
                }
                () = self.upload_wake.notified() => run.stale = true,
                // A connection a part took beside its attempt's ended: an
                // upload waiting for one may start.
                () = self.connection_freed() => {}
                change = changes.next() => {
                    change?;
                    run.stale = true;
                    run.committed = true;
                }
                _ = sending.changed() => run.stale = true,
                _ = syncing.changed() => run.stale = true,
                _ = reports.changed() => {}
                () = network.changed() => run.stale = true,
                _ = rearmed.changed() => {}
                () = async {
                    match retry {
                        Some(retry) => retry.await,
                        None => std::future::pending().await,
                    }
                } => run.stale = true,
            }
        }
    }

    /// The run's own work failed with `error`, which no caller hears: a bug
    /// it carries goes to the sink, and any other failure is logged (E15).
    fn failed_on_its_own(&self, what: &'static str, error: &SyncError) {
        if !coven_foundation::task::report_carried(self.database.bugs().as_ref(), error) {
            tracing::error!(?error, "{what}");
        }
    }

    /// Act on reports for upload waits, as each plan does (§6.7).
    /// A failure keeps every wait and the report, for the next plan.
    async fn act_on_reports(&self, run: &mut Run<'_>) {
        if !run.crossed.is_empty() {
            let crossed = std::mem::take(&mut run.crossed);
            match self.waits.end_waits_after_report(NetworkOwner::Uploads, &crossed).await {
                Ok(()) => run.stale = true,
                Err(error) => {
                    self.failed_on_its_own("ending reported upload waits failed", &error);
                    run.crossed = crossed;
                }
            }
        }
        match self.waits.end_reported_waits(NetworkOwner::Uploads).await {
            Ok(true) => run.stale = true,
            Ok(false) => {}
            Err(error) => self.failed_on_its_own("ending reported upload waits failed", &error),
        }
    }

    /// Take the queue just read as the run's next uploads: each one not in
    /// flight that may retry and whose wait is over, or that an app retry
    /// covers. App retries not yet taken cover every upload that may retry.
    /// While trailing uploads are held, only bound ones are taken.
    fn read_ready(&self, run: &mut Run<'_>, work: Vec<(FileUpload, Option<PendingRecord>)>) {
        run.stale = false;
        run.committed = false;
        run.bound = work
            .iter()
            .filter(|(item, _)| Lane::of(item) == Lane::Bound)
            .map(|(item, _)| item.id)
            .collect();
        let held = self.trailing_held();
        let retryable: BTreeSet<i64> = work
            .iter()
            .filter(|(_, record)| {
                record.as_ref().is_none_or(|record| record.retry() != Retry::Never)
            })
            .map(|(item, _)| item.id)
            .collect();
        let forced: BTreeSet<i64> = {
            let mut retries = lock_unpoisoned(&self.app_retries);
            // A retry whose caller stopped waiting is dropped.
            retries.retain(|retry| !retry.reply.is_closed());
            for retry in retries.iter_mut() {
                retry.covers.get_or_insert_with(|| retryable.clone());
            }
            retries.iter().flat_map(|retry| retry.covers.iter().flatten().copied()).collect()
        };
        self.answer_covered();
        run.ready.clear();
        for (item, record) in work {
            let subject = RetrySubject::Upload(item.id);
            if !retryable.contains(&item.id) || (held && Lane::of(&item) == Lane::Trailing) {
                continue;
            }
            // An upload in flight, or one an app retry starts whatever its
            // wait, was evaluated: its ended wait wakes nothing.
            if run.started.contains_key(&item.id) {
                self.waits.seen(subject);
            } else if forced.contains(&item.id) {
                self.waits.seen(subject);
                run.ready.push_back(item);
            } else if self.retry_due(&item, record.as_ref()) {
                run.ready.push_back(item);
            }
        }
    }

    /// Start ready uploads, oldest first, until each lane has the limit's
    /// number in flight. A trailing upload taken before a hold began waits
    /// for the run's next read once the hold ends.
    fn start_ready<'a>(&'a self, run: &mut Run<'a>, limit: usize) {
        let held = self.trailing_held();
        let mut waiting = VecDeque::new();
        while let Some(item) = run.ready.pop_front() {
            let lane = Lane::of(&item);
            if lane == Lane::Trailing && held {
                continue;
            }
            if run.used(lane, self) >= limit {
                waiting.push_back(item);
                continue;
            }
            let id = item.id;
            run.lanes.insert(id, lane);
            self.set_lane_attempts(lane, run.in_flight(lane));
            run.started.insert(id, self.waits.network_reported());
            // A resumed session starts where the provider confirmed it.
            let progress = coven_database::TransferProgress {
                bytes_transferred: item.progress.map_or(0, |progress| progress.bytes_confirmed),
                bytes_total: item.bytes_total,
            };
            let sending = self.database.uploads_in_flight().start(
                item.location.uploader(),
                item.location.object(),
                item.files.clone(),
                progress,
            );
            run.attempts.push(Box::pin(async move {
                let attempted = self.attempt_upload(item, sending.meter()).await;
                sending.end(match &attempted {
                    Ok(attempted) => attempted.transfer_end(),
                    // The attempt failed, and so did recording its failure.
                    Err(_) => coven_database::TransferEnd::Failed,
                });
                (id, attempted)
            }));
        }
        // An upload waiting for a connection takes the next one freed,
        // before a part beside an attempt does.
        for lane in [Lane::Bound, Lane::Trailing] {
            let waits = waiting.iter().any(|item| Lane::of(item) == lane);
            self.set_lane_waiting(lane, waits);
        }
        run.ready = waiting;
    }

    /// One attempt ended: its slot is free, and the app retries covering it
    /// count it. Only an upload that stored a file counts as uploaded; one
    /// that retired because no file waited for it any more stored none.
    fn finished(&self, run: &mut Run<'_>, id: i64, attempted: super::file_upload::Attempted) {
        use super::file_upload::Attempted;
        if let Some(lane) = run.lanes.remove(&id) {
            self.set_lane_attempts(lane, run.in_flight(lane));
        }
        let started = run.started.remove(&id);
        let stored = attempted == Attempted::Stored;
        match attempted {
            Attempted::Stored => run.uploaded += 1,
            Attempted::Retired => {}
            Attempted::Failed | Attempted::Held => {
                if let Some(ending) = started.and_then(|seen| self.waits.reports_since(seen)) {
                    run.crossed.insert(RetrySubject::Upload(id), ending);
                }
            }
        }
        for retry in lock_unpoisoned(&self.app_retries).iter_mut() {
            if let Some(covers) = &mut retry.covers {
                if covers.remove(&id) && stored {
                    retry.uploaded += 1;
                }
            }
        }
        self.answer_covered();
    }

    /// Answer each taken app retry whose covered uploads were all attempted.
    fn answer_covered(&self) {
        let mut retries = lock_unpoisoned(&self.app_retries);
        let mut kept = Vec::new();
        for retry in retries.drain(..) {
            match &retry.covers {
                Some(covers) if covers.is_empty() => {
                    #[expect(
                        clippy::let_underscore_must_use,
                        reason = "a retry whose caller stopped waiting needs no answer"
                    )]
                    let _ =
                        retry.reply.send(Ok(DrainOutcome::Drained { uploaded: retry.uploaded }));
                }
                _ => kept.push(retry),
            }
        }
        *retries = kept;
    }
}
