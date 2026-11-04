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
    /// Its place among the retries asked, which says whether a read of the
    /// write pause came after it.
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

/// The lane an upload runs in (§17.4): a bound upload's write and every
/// later one wait for it, so neither the pause nor trailing uploads hold it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lane {
    Bound,
    Trailing,
}

impl Lane {
    fn of(upload: &FileUpload) -> Self {
        match upload.holds {
            Some(_) => Self::Bound,
            None => Self::Trailing,
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

    /// Whether an upload of some lane a hold leaves open could start: a
    /// lane with room, and for the bound lane, a bound upload the last read
    /// found or a commit since may have queued.
    fn room(&self, limit: usize, held: bool) -> bool {
        (self.in_flight(Lane::Bound) < limit && (self.committed || !self.bound.is_empty()))
            || (!held && self.in_flight(Lane::Trailing) < limit)
    }
}

impl FilesInner {
    /// Queue an app retry for the upload worker, and wake it. A worker that
    /// panicked serves none: the retry fails with its bug.
    pub(super) fn ask_retry(&self) -> Result<UploadRetryAnswer, SyncError> {
        let (reply, answer) = oneshot::channel();
        {
            // Closing sets the flag before clearing this list under its lock,
            // and the worker's panic sets its bug before answering it, so a
            // retry queued here is either cleared, answered or refused.
            let mut retries = self.app_retries.lock().expect("app retries poisoned");
            self.check_open()?;
            if let Some(bug) = self.worker_bug.get() {
                return Err(SyncError::Bug(bug.clone()));
            }
            let asked = self.retries_asked.fetch_add(1, Ordering::AcqRel);
            retries.push(AppRetry { covers: None, uploaded: 0, asked, reply });
        }
        self.upload_wake.notify_one();
        Ok(UploadRetryAnswer(answer))
    }

    /// The upload worker panicked with `bug`: no run will serve an app
    /// retry, so each waiting one is answered with the bug, as each asked
    /// from now on is (E15).
    pub(super) fn worker_panicked(&self, bug: coven_foundation::task::Bug) {
        let mut retries = self.app_retries.lock().expect("app retries poisoned");
        let bug = self.worker_bug.get_or_init(|| bug);
        for retry in retries.drain(..) {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a retry whose caller stopped waiting needs no answer"
            )]
            let _ = retry.reply.send(Err(SyncError::Bug(bug.clone())));
        }
    }

    /// Answer every queued app retry with this; `None` keeps them all, as
    /// they are, for the next run.
    fn answer_retries(&self, answer: impl Fn() -> Option<Result<DrainOutcome, SyncError>>) {
        let mut retries = self.app_retries.lock().expect("app retries poisoned");
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
    /// flight and none can start.
    pub(crate) async fn drain_uploads(&self) -> Result<DrainOutcome, SyncError> {
        let _run = self.drain.lock().await;
        let result = self.serve_uploads().await;
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) if self.app_retries.lock().expect("app retries poisoned").is_empty() => {
                Err(error)
            }
            // Every waiting app retry learns the failure that ended the run.
            Err(error) => {
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
        } else if self.storage.read().expect("storage lock poisoned").is_none() {
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
        if let Blocked::Held(TrailingHold::MemberKeysChanging) = blocked {
            self.answer_paused_writes();
        } else {
            self.answer_retries(|| blocked.retry_answer());
        }
        blocked.run_outcome(uploaded)
    }

    /// Answer `MemberKeysChanging` to each app retry asked before the read
    /// that found writes paused. One asked since may come after the pause
    /// ended, so it waits for the next run, which reads the pause again.
    fn answer_paused_writes(&self) {
        let read_after = self.writes_paused_read_after();
        let mut retries = self.app_retries.lock().expect("app retries poisoned");
        let (answered, kept): (Vec<_>, Vec<_>) =
            retries.drain(..).partition(|retry| retry.asked < read_after);
        *retries = kept;
        let waiting = !retries.is_empty();
        drop(retries);
        for retry in answered {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a retry whose caller stopped waiting needs no answer"
            )]
            let _ = retry.reply.send(Err(DbError::MemberKeysChanging.into()));
        }
        if waiting {
            self.upload_wake.notify_one();
        }
    }

    #[cfg(test)]
    fn pass_blocked_end_gate(&self) {
        let gate = self.blocked_end_gate.lock().expect("blocked end gate poisoned").clone();
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
        for id in self.database.retire_unlinked(self.in_flight.current(), upload_subject).await? {
            self.waits.forget(RetrySubject::Upload(id));
        }
        if let Some(blocked @ (Blocked::NotSyncing | Blocked::NoStorage)) = self.blocked() {
            return self.end_blocked(blocked, 0);
        }
        // The persisted settings govern; parts check these copies between
        // sends. A change of either waits until this run has acted on what
        // it read, so the run cannot answer a retry asked after a resume
        // with the pause.
        let change = self.hold_change.lock().await;
        let paused = self.database.uploads_paused().await?;
        self.paused.store(paused, Ordering::Release);
        let unmetered_only = self.database.unmetered_only().await?;
        self.unmetered_only.store(unmetered_only, Ordering::Release);
        self.read_writes_paused().await?;
        let work = self.database.upload_work::<PendingRecord>(upload_subject).await?;
        // The pause, a self-replacement's write pause and a metered network
        // hold trailing uploads only (§12.3, §17.4, §17.9).
        if let Some(hold) = self.trailing_hold() {
            if work.iter().all(|(item, _)| Lane::of(item) == Lane::Trailing) {
                return self.end_blocked(Blocked::Held(hold), 0);
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
                if work.is_some() || (run.stale && run.room(limit, self.trailing_held())) {
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
                    if held && (!run.stale || !run.room(limit, held)) {
                        #[cfg(test)]
                        self.pass_blocked_end_gate();
                        // As with the read above, a setting's change waits
                        // while the run acts on the hold it saw; after a
                        // resume since the check, the run serves uploads
                        // again.
                        let _change = self.hold_change.lock().await;
                        self.read_writes_paused().await?;
                        if let Some(hold) = self.trailing_hold() {
                            return self.end_blocked(Blocked::Held(hold), run.uploaded);
                        }
                        run.stale = true;
                        continue;
                    }
                    if !run.stale
                        && self
                            .app_retries
                            .lock()
                            .expect("app retries poisoned")
                            .iter()
                            .all(|retry| retry.covers.is_some())
                    {
                        // Nothing a taken retry covers is left to start.
                        for retry in
                            self.app_retries.lock().expect("app retries poisoned").drain(..)
                        {
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
                && run.room(self.transfers.limits().uploads.get(), self.trailing_held());
            let retry = if open { self.next_upload_retry(&run.bound) } else { None };
            tokio::select! {
                Some((id, result)) = run.attempts.next(), if !run.attempts.is_empty() => {
                    self.finished(&mut run, id, result?);
                }
                () = self.upload_wake.notified() => run.stale = true,
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

    /// Act on reports for upload waits, as each plan does (§6.7).
    /// A failure keeps every wait and the report, for the next plan.
    async fn act_on_reports(&self, run: &mut Run<'_>) {
        if !run.crossed.is_empty() {
            let crossed = std::mem::take(&mut run.crossed);
            match self.waits.end_waits_after_report(NetworkOwner::Uploads, &crossed).await {
                Ok(()) => run.stale = true,
                Err(error) => {
                    tracing::error!(?error, "ending reported upload waits failed");
                    run.crossed = crossed;
                }
            }
        }
        match self.waits.end_reported_waits(NetworkOwner::Uploads).await {
            Ok(true) => run.stale = true,
            Ok(false) => {}
            Err(error) => tracing::error!(?error, "ending reported upload waits failed"),
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
            let mut retries = self.app_retries.lock().expect("app retries poisoned");
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
            if run.in_flight(lane) >= limit {
                waiting.push_back(item);
                continue;
            }
            let id = item.id;
            run.lanes.insert(id, lane);
            run.started.insert(id, self.waits.network_reported());
            // A resumed session starts where the provider confirmed it.
            let progress = coven_database::TransferProgress {
                bytes_transferred: item.progress.map_or(0, |progress| progress.bytes_confirmed),
                bytes_total: item.bytes_total,
            };
            let sending = self.in_flight.start(
                item.location.uploader(),
                item.location.object(),
                item.files.clone(),
                progress,
            );
            run.attempts.push(Box::pin(async move {
                let meter = sending.meter();
                let _sending = sending;
                (id, self.attempt_upload(item, meter).await)
            }));
        }
        run.ready = waiting;
    }

    /// One attempt ended: its slot is free, and the app retries covering it
    /// count it. Only an upload that stored a file counts as uploaded; one
    /// that retired because no file waited for it any more stored none.
    fn finished(&self, run: &mut Run<'_>, id: i64, attempted: super::file_upload::Attempted) {
        use super::file_upload::Attempted;
        run.lanes.remove(&id);
        let started = run.started.remove(&id);
        let stored = attempted == Attempted::Stored;
        match attempted {
            Attempted::Stored => run.uploaded += 1,
            Attempted::Retired => {}
            Attempted::Queued => {
                if let Some(ending) = started.and_then(|seen| self.waits.reports_since(seen)) {
                    run.crossed.insert(RetrySubject::Upload(id), ending);
                }
            }
        }
        for retry in self.app_retries.lock().expect("app retries poisoned").iter_mut() {
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
        let mut retries = self.app_retries.lock().expect("app retries poisoned");
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
