//! The upload worker's runs (§17.7, E7). While uploads can run, a run keeps
//! as many attempts in flight as the upload limit allows. As each one ends,
//! the next eligible upload starts, files queued meanwhile included; no
//! upload waits for the others to finish. An app's early retry joins the run
//! in progress.

use crate::{
    files::file_failure::file_subject, files::FilesInner, NetworkOwner, PendingRecord, Retry,
    SyncError,
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
    /// Every upload the retry covered was attempted; this many were stored
    /// and retired.
    Drained {
        /// Files storage confirmed, each retired from the queue.
        uploaded: usize,
    },
    /// No waiting upload exists.
    QueueEmpty,
    /// Uploads were paused.
    Paused,
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
    reply: oneshot::Sender<Result<DrainOutcome, SyncError>>,
}

/// Why no upload may start now.
#[derive(Clone, Copy, Debug)]
enum Blocked {
    Closed,
    NoStorage,
    Paused,
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
            Self::NoStorage => Err(SyncError::NoStorage),
            Self::Paused => Ok(DrainOutcome::Paused),
            Self::CatchUp => Err(StorageFailure::CatchUpRequired.into()),
            // An app retry overrides ordinary backoff, never a provider cooldown.
            Self::Cooling(remaining) => Err(StorageFailure::RateLimited
                .with_source(coven_storage::CoolingDown { remaining })
                .into()),
            Self::Changing => return None,
        })
    }

    /// What the run itself returns. Automatic work waits out a cooldown and
    /// a connection change without failing.
    fn run_outcome(self, uploaded: usize) -> Result<DrainOutcome, SyncError> {
        match self {
            Self::Cooling(_) | Self::Changing => Ok(DrainOutcome::Drained { uploaded }),
            blocked => blocked
                .retry_answer()
                .unwrap_or(Ok(DrainOutcome::Drained { uploaded })),
        }
    }
}

type Attempt<'a> = BoxFuture<'a, (i64, Result<bool, SyncError>)>;

/// One run's scheduling state.
#[derive(Default)]
struct Run<'a> {
    /// Attempts in flight, each yielding its queue id and result.
    attempts: FuturesUnordered<Attempt<'a>>,
    /// The network reports seen as each attempt in flight started.
    started: BTreeMap<i64, u64>,
    /// Eligible uploads from the last read of the queue, oldest first, not
    /// started yet.
    ready: VecDeque<FileUpload>,
    /// Something may have made more uploads eligible since that read.
    stale: bool,
    /// Uploads whose attempt failed after a network report arrived while it
    /// ran; the report ends the wait each failed into.
    crossed: BTreeSet<RetrySubject>,
    /// Files this run stored.
    uploaded: usize,
}

impl FilesInner {
    /// Queue an app retry for the upload worker, and wake it.
    pub(super) fn ask_retry(
        &self,
    ) -> Result<oneshot::Receiver<Result<DrainOutcome, SyncError>>, DbError> {
        let (reply, answer) = oneshot::channel();
        {
            // Closing sets the flag before clearing this list under its lock,
            // so a retry queued here is either cleared or refused.
            let mut retries = self.retries();
            self.check_open()?;
            retries.push(AppRetry {
                covers: None,
                uploaded: 0,
                reply,
            });
        }
        self.upload_wake.notify_one();
        Ok(answer)
    }

    fn retries(&self) -> std::sync::MutexGuard<'_, Vec<AppRetry>> {
        self.app_retries.lock().expect("app retries poisoned")
    }

    /// Answer every queued app retry with this; `None` keeps them all, as
    /// they are, for the next run.
    fn answer_retries(&self, answer: impl Fn() -> Option<Result<DrainOutcome, SyncError>>) {
        let mut retries = self.retries();
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

    /// One run of the upload worker, or of a test driving it: start uploads
    /// while they can run, keeping the upload limit's number in flight, and
    /// end once none is in flight and none can start. `force` makes the run
    /// begin with an app retry of its own, whose answer it returns.
    pub(crate) async fn drain_uploads(&self, force: bool) -> Result<DrainOutcome, SyncError> {
        let own = if force { Some(self.ask_retry()?) } else { None };
        let run = self.run_uploads().await;
        match own.map(|mut answer| answer.try_recv()) {
            Some(Ok(answer)) => answer,
            // Unanswered, its covered uploads were not all attempted: the run
            // failed, or a connection change stopped it.
            Some(Err(_)) | None => run,
        }
    }

    async fn run_uploads(&self) -> Result<DrainOutcome, SyncError> {
        let _run = self.drain.lock().await;
        let result = self.serve_uploads().await;
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) if self.retries().is_empty() => Err(error),
            // Every waiting app retry learns the failure that ended the run.
            Err(error) => {
                let (failure, source) = error.into_shared();
                self.answer_retries(|| {
                    Some(Err(SyncError::Failed {
                        failure,
                        source: source.clone(),
                    }))
                });
                Err(SyncError::Failed { failure, source })
            }
        }
    }

    /// Why no upload may start now, from the owner's current state.
    fn blocked(&self) -> Option<Blocked> {
        if self.check_open().is_err() {
            Some(Blocked::Closed)
        } else if self
            .storage
            .read()
            .expect("storage lock poisoned")
            .is_none()
        {
            Some(Blocked::NoStorage)
        } else if self.changing.load(Ordering::Acquire) > 0 {
            Some(Blocked::Changing)
        } else if self.paused() {
            Some(Blocked::Paused)
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

    async fn serve_uploads(&self) -> Result<DrainOutcome, SyncError> {
        // Observe before reading the queue, so nothing that happens while it
        // is read is missed.
        let mut changes = self.database.queued_work_changes();
        let mut sending = self.send.subscribe();
        let mut reports = self.waits.network_reports();
        let mut rearmed = self.waits.rearmed();
        if let Some(blocked @ (Blocked::Closed | Blocked::NoStorage)) = self.blocked() {
            return self.end_blocked(blocked, 0);
        }
        // The persisted setting governs; parts check this copy between sends.
        let paused = self.database.uploads_paused().await?;
        self.paused.store(paused, Ordering::Release);
        if paused {
            return self.end_blocked(Blocked::Paused, 0);
        }
        let work = self
            .database
            .upload_work::<PendingRecord>(file_subject)
            .await?;
        if work.is_empty() {
            self.answer_retries(|| Some(Ok(DrainOutcome::QueueEmpty)));
            return Ok(DrainOutcome::QueueEmpty);
        }
        let mut run = Run::default();
        let mut work = Some(work);
        loop {
            if let Some(blocked) = self.blocked() {
                if run.attempts.is_empty() {
                    return self.end_blocked(blocked, run.uploaded);
                }
            } else {
                self.act_on_reports(&mut run).await;
                let limit = self.transfers.limits().uploads.get();
                if run.attempts.len() < limit && (run.stale || work.is_some()) {
                    // The first read went stale if a report ended waits since.
                    let work = match work.take() {
                        Some(work) if !run.stale => work,
                        _ => {
                            self.database
                                .upload_work::<PendingRecord>(file_subject)
                                .await?
                        }
                    };
                    self.read_ready(&mut run, work);
                }
                self.start_ready(&mut run, limit);
                if run.attempts.is_empty() && run.ready.is_empty() {
                    if !run.stale && self.retries().iter().all(|retry| retry.covers.is_some()) {
                        // Nothing a taken retry covers is left to start.
                        for retry in self.retries().drain(..) {
                            #[expect(
                                clippy::let_underscore_must_use,
                                reason = "a retry whose caller stopped waiting needs no answer"
                            )]
                            let _ = retry.reply.send(Ok(DrainOutcome::Drained {
                                uploaded: retry.uploaded,
                            }));
                        }
                        return Ok(DrainOutcome::Drained {
                            uploaded: run.uploaded,
                        });
                    }
                    run.stale = true;
                    continue;
                }
            }
            // A due retry matters only while an upload could start; it
            // would wake at once for a wait an attempt in flight serves.
            let open = self.blocked().is_none()
                && run.attempts.len() < self.transfers.limits().uploads.get();
            let retry = if open { self.next_upload_retry() } else { None };
            tokio::select! {
                Some((id, result)) = run.attempts.next(), if !run.attempts.is_empty() => {
                    self.finished(&mut run, id, result?);
                }
                () = self.upload_wake.notified() => run.stale = true,
                change = changes.next() => {
                    change?;
                    run.stale = true;
                }
                _ = sending.changed() => run.stale = true,
                _ = reports.changed() => {}
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

    /// Act on network reports for upload waits, as each plan does (§6.7).
    /// A failure keeps every wait and the report, for the next plan.
    async fn act_on_reports(&self, run: &mut Run<'_>) {
        if !run.crossed.is_empty() {
            let crossed = std::mem::take(&mut run.crossed);
            match self
                .waits
                .end_waits_after_report(NetworkOwner::Uploads, crossed.clone())
                .await
            {
                Ok(()) => run.stale = true,
                Err(error) => {
                    tracing::error!(?error, "ending upload waits for the network failed");
                    run.crossed = crossed;
                }
            }
        }
        match self.waits.end_offline_waits(NetworkOwner::Uploads).await {
            Ok(true) => run.stale = true,
            Ok(false) => {}
            Err(error) => tracing::error!(?error, "ending upload waits for the network failed"),
        }
    }

    /// Take the queue just read as the run's next uploads: each one not in
    /// flight that may retry and whose wait is over, or that an app retry
    /// covers. App retries not yet taken cover every upload that may retry.
    fn read_ready(&self, run: &mut Run<'_>, work: Vec<(FileUpload, Option<PendingRecord>)>) {
        run.stale = false;
        let retryable: BTreeSet<i64> = work
            .iter()
            .filter(|(_, record)| {
                record
                    .as_ref()
                    .is_none_or(|record| record.retry() != Retry::Never)
            })
            .map(|(item, _)| item.id)
            .collect();
        let forced: BTreeSet<i64> = {
            let mut retries = self.retries();
            // A retry whose caller stopped waiting is dropped.
            retries.retain(|retry| !retry.reply.is_closed());
            for retry in retries.iter_mut() {
                retry.covers.get_or_insert_with(|| retryable.clone());
            }
            retries
                .iter()
                .flat_map(|retry| retry.covers.iter().flatten().copied())
                .collect()
        };
        self.answer_covered();
        run.ready.clear();
        for (item, record) in work {
            let subject = RetrySubject::Upload(item.id);
            if !retryable.contains(&item.id) {
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

    /// Start ready uploads until the limit's number are in flight.
    fn start_ready<'a>(&'a self, run: &mut Run<'a>, limit: usize) {
        while run.attempts.len() < limit {
            let Some(item) = run.ready.pop_front() else {
                break;
            };
            let id = item.id;
            run.started.insert(id, self.waits.network_reported());
            let sending = self.in_flight.start(id);
            run.attempts.push(Box::pin(async move {
                let _sending = sending;
                (id, self.attempt_upload(item).await)
            }));
        }
    }

    /// One attempt ended: its slot is free, and the app retries covering it
    /// count it.
    fn finished(&self, run: &mut Run<'_>, id: i64, stored: bool) {
        let started = run.started.remove(&id);
        if stored {
            run.uploaded += 1;
            // Completion can leave eager work, such as a carried pin (§17.6).
            self.fill_wake.notify_one();
        } else if started.is_some_and(|seen| self.waits.network_reported() > seen) {
            run.crossed.insert(RetrySubject::Upload(id));
        }
        for retry in self.retries().iter_mut() {
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
        let mut retries = self.retries();
        let mut kept = Vec::new();
        for retry in retries.drain(..) {
            match &retry.covers {
                Some(covers) if covers.is_empty() => {
                    #[expect(
                        clippy::let_underscore_must_use,
                        reason = "a retry whose caller stopped waiting needs no answer"
                    )]
                    let _ = retry.reply.send(Ok(DrainOutcome::Drained {
                        uploaded: retry.uploaded,
                    }));
                }
                _ => kept.push(retry),
            }
        }
        *retries = kept;
    }
}
