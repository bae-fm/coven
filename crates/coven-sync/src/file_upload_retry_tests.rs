//! Upload waits: the queue's persisted retry delays, the category policy
//! and the provider cooldown shared with every consumer (§6.7, E7).

use super::*;
use crate::PendingReason;
use coven_database::RetrySubject;
use coven_foundation::clock::{Clock, FixedClock};
use coven_storage::{test_utils::Faults, StorageFailure};

async fn fixture(clock: &Arc<FixedClock>) -> Fixture {
    let f = Fixture::with_clock(Provenance::AppProvided, clock.clone()).await;
    // Only the explicit drains below attempt uploads.
    f.files.stop_background_for_counts().await;
    f.files.set_uploads_paused(false).await.unwrap();
    f
}

fn fail_next(failure: StorageFailure, retry_after: Option<u64>) -> Faults {
    Faults {
        fail_next: 1,
        failure,
        retry_after,
        ..Faults::none()
    }
}

fn network() -> PendingReason {
    PendingReason::Storage {
        provider: coven_storage::CloudProvider::Dropbox,
        failure: StorageFailure::Network,
        answered: false,
    }
}

/// Advance the retry clock.
fn advance(clock: &FixedClock, seconds: u64) {
    clock.set(clock.now() + Duration::from_secs(seconds));
}

/// How long until the worker's next timed drain, by the waits' deadlines.
fn retry_in(f: &Fixture) -> Option<Duration> {
    let waits = &f.files.inner.waits;
    let retry = waits.next_due(|subject| matches!(subject, RetrySubject::Upload(_)));
    match (retry, waits.cooldown()) {
        (Some(retry), Some(cooldown)) => Some(retry.max(cooldown)),
        (retry, cooldown) => retry.or(cooldown),
    }
}

async fn retry(f: &Fixture) -> Option<coven_database::RetryWait> {
    f.files.inner.database.uploads().await.unwrap()[0].retry
}

/// Files an automatic or explicit drain stored; failures stay in pending().
fn drained(outcome: DrainOutcome) -> usize {
    match outcome {
        DrainOutcome::Drained { uploaded } => uploaded,
        other => panic!("unexpected drain outcome {other:?}"),
    }
}

#[tokio::test]
async fn a_transient_failure_persists_its_delay_and_waits_it_out() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = fixture(&clock).await;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    for (attempt, wait) in [1u64, 2, 4].into_iter().enumerate() {
        f.storage
            .set_faults(fail_next(StorageFailure::Network, None))
            .await;
        assert_eq!(
            drained(f.files.inner.drain_uploads(false).await.unwrap()),
            0
        );
        // The failure is the file's pending record, and T and W persist with
        // it before the upload waits. The record carries the error itself.
        assert_eq!(f.upload_reason(&file).await, Some(network()));
        let cause = f.file_cause(&file).await.unwrap();
        assert!(cause.contains("Network"), "{cause}");
        let persisted = retry(&f).await.unwrap();
        assert_eq!(persisted.failures, attempt as u32 + 1);
        assert_eq!(persisted.wait, Duration::from_secs(wait));
        assert_eq!(persisted.not_before, clock.now() + persisted.wait);
        let requests = f.storage.request_trace().len();
        assert_eq!(
            f.files.inner.drain_uploads(false).await.unwrap(),
            DrainOutcome::Drained { uploaded: 0 }
        );
        assert_eq!(f.storage.request_trace().len(), requests);
        assert_eq!(retry_in(&f), Some(Duration::from_secs(wait)));
        advance(&clock, wait);
    }
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    // Completion retired the queue row and its record together.
    assert!(!f.queued(&file).await);
    assert_eq!(f.upload_reason(&file).await, None);
    assert_eq!(retry_in(&f), None);
    f.close().await;
}

#[tokio::test]
async fn reopening_keeps_the_remaining_delay_and_its_exponent() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let mut f = fixture(&clock).await;
    f.attach("files", "photo", b"photo".to_vec()).await;
    for _ in 0..4 {
        f.storage
            .set_faults(fail_next(StorageFailure::Network, None))
            .await;
        f.files.inner.drain_uploads(true).await.unwrap();
    }
    assert_eq!(retry(&f).await.unwrap().wait, Duration::from_secs(8));
    advance(&clock, 4);
    // The device clock moves back an hour while the store is closed.
    clock.set(clock.now() - Duration::from_secs(3600));
    f.reopen().await;
    f.files.stop_background_for_counts().await;
    f.files.set_uploads_paused(false).await.unwrap();
    advance(&clock, 0);
    // Restart waits min(T - now, W): the backward clock cannot stretch it.
    assert_eq!(retry_in(&f), Some(Duration::from_secs(8)));
    advance(&clock, 7);
    assert_eq!(
        f.files.inner.drain_uploads(false).await.unwrap(),
        DrainOutcome::Drained { uploaded: 0 }
    );
    advance(&clock, 1);
    f.storage
        .set_faults(fail_next(StorageFailure::Network, None))
        .await;
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    // Reopening kept the exponent: the fifth failure waits sixteen seconds.
    assert_eq!(retry(&f).await.unwrap().wait, Duration::from_secs(16));
    f.close().await;
}

#[tokio::test]
async fn a_provider_cooldown_holds_uploads_even_when_the_app_retries() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = fixture(&clock).await;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    f.storage
        .set_faults(fail_next(StorageFailure::RateLimited, Some(600)))
        .await;
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    // The provider's 600 seconds outlast the upload's own one-second delay
    // and the 300-second cap, and persist with the store.
    let cooldown = f
        .database
        .retry_wait(RetrySubject::Provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cooldown.wait, Duration::from_secs(600));
    assert_eq!(retry_in(&f), Some(Duration::from_secs(600)));
    advance(&clock, 1);
    let requests = f.storage.request_trace().len();
    assert_eq!(
        f.files.inner.drain_uploads(false).await.unwrap(),
        DrainOutcome::Drained { uploaded: 0 }
    );
    let Err(refused) = f.files.inner.drain_uploads(true).await else {
        panic!("an app retry bypassed the provider cooldown");
    };
    assert!(
        matches!(&refused, crate::SyncError::Storage(error) if error.failure() == StorageFailure::RateLimited),
        "{refused:?}"
    );
    assert_eq!(f.storage.request_trace().len(), requests);
    advance(&clock, 599);
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    assert!(!f.queued(&file).await);
    f.close().await;
}

#[tokio::test]
async fn the_worker_wakes_for_a_due_retry_without_another_commit() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = Fixture::with_clock(Provenance::AppProvided, clock.clone()).await;
    f.storage
        .set_faults(fail_next(StorageFailure::Network, None))
        .await;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    // The database's queue state includes each persisted wait.
    let mut uploads = f.database.subscribe_upload_queue();
    f.files.set_uploads_paused(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while retry(&f).await.is_none() {
            uploads.next().await.unwrap();
        }
        // The worker arms the persisted wait's timer after committing it.
        while retry_in(&f).is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    advance(&clock, 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.queued(&file).await {
            uploads.next().await.unwrap();
        }
    })
    .await
    .unwrap();
    f.close().await;
}

#[tokio::test]
async fn completed_catch_up_wakes_a_worker_blocked_between_file_sends() {
    use coven_storage::ObjectPrefix;
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let f = Fixture::with_clock(Provenance::AppProvided, clock.clone()).await;
    f.attach("files", "first", b"first".to_vec()).await;
    f.attach("files", "second", b"second".to_vec()).await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_creation(ObjectPrefix::files(f.directory.id()), started, held)
        .await;
    let mut uploads = f.subscribe_queue();
    f.files.set_uploads_paused(false).await.unwrap();
    observed.await.unwrap();
    f.send.set(crate::SequenceStatus::Opening);
    resume.send(()).unwrap();
    // The first file finishes; the second does not start until a catch-up.
    tokio::time::timeout(Duration::from_secs(5), async {
        while uploads.next().await.unwrap().files.len() != 1 {}
    })
    .await
    .unwrap();
    drop(f.files.inner.drain.lock().await);
    assert_eq!(
        f.storage
            .request_trace()
            .iter()
            .filter(|request| matches!(
                request,
                coven_storage::test_utils::MemoryRequest::Create(_)
            ))
            .count(),
        1
    );
    let first = f.database.file_ref("files", "first").await.unwrap();
    let second = f.database.file_ref("files", "second").await.unwrap();
    assert!(!f.queued(&first).await);
    assert!(f.queued(&second).await);
    f.send.set(crate::SequenceStatus::Opening);
    f.send.set(crate::SequenceStatus::Synced { through: 0 });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            uploads.next().await.unwrap();
            if !f.queued(&second).await {
                break;
            }
        }
    })
    .await
    .unwrap();
    f.close().await;
}

/// The upload worker's storage, traced: each call first moves the clock
/// `step` seconds, then records its time and whether the shared sync state let
/// it through. A create can also lose its reply after publishing.
struct StepStorage {
    storage: Arc<MemoryStorage>,
    clock: Arc<FixedClock>,
    step: Duration,
    lose_create_reply: std::sync::atomic::AtomicBool,
    /// Seconds the clock moves after a create returns, before the worker continues.
    after_create: std::sync::atomic::AtomicU64,
    /// Runs once after the next confirmed part.
    on_part: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
    trace: std::sync::Mutex<Vec<(serde_json::Value, bool)>>,
}

impl StepStorage {
    fn new(f: &Fixture, clock: &Arc<FixedClock>, step: u64) -> Arc<Self> {
        Arc::new(Self {
            storage: f.storage.clone(),
            clock: clock.clone(),
            step: Duration::from_secs(step),
            lose_create_reply: false.into(),
            after_create: 0.into(),
            on_part: std::sync::Mutex::new(None),
            trace: std::sync::Mutex::new(Vec::new()),
        })
    }
    fn seconds(&self) -> u64 {
        self.clock.elapsed().as_secs()
    }
    fn note(&self, event: serde_json::Value) {
        self.trace.lock().unwrap().push((event, true));
    }
    /// A completed catch-up: its phase begins now and completes a second later.
    fn catch_up(&self, send: &crate::SyncState) {
        send.set(crate::SequenceStatus::Opening);
        self.note(serde_json::json!({"op":"begin","time":self.seconds()}));
        self.clock.set(self.clock.now() + Duration::from_secs(1));
        send.set(crate::SequenceStatus::Synced { through: 0 });
        self.note(
            serde_json::json!({"op":"checked","time":self.seconds(),"send":true,"retired":false}),
        );
    }
    fn tick(&self) -> u64 {
        self.clock.set(self.clock.now() + self.step);
        self.seconds()
    }
    fn record<T>(
        &self,
        kind: &str,
        time: u64,
        result: Result<T, coven_storage::StorageError>,
    ) -> Result<T, coven_storage::StorageError> {
        let refused =
            matches!(&result, Err(error) if error.failure() == StorageFailure::CatchUpRequired);
        self.trace.lock().unwrap().push((
            serde_json::json!({"op":"request","actor":"upload","kind":kind,"time":time}),
            !refused,
        ));
        result
    }
    fn calls(&self, kind: &str) -> usize {
        self.trace
            .lock()
            .unwrap()
            .iter()
            .filter(|(event, _)| event["kind"] == kind)
            .count()
    }
}

#[async_trait::async_trait]
impl coven_storage::Storage for StepStorage {
    fn config(&self) -> coven_storage::StorageConfig {
        self.storage.config()
    }
    fn single_request_limit(&self) -> u64 {
        self.storage.single_request_limit()
    }
    async fn create(
        &self,
        path: &coven_storage::ObjectPath,
        bytes: &[u8],
    ) -> Result<Option<coven_storage::StoredObject>, coven_storage::StorageError> {
        let time = self.tick();
        let result = self.storage.create(path, bytes).await;
        let after = self.after_create.load(std::sync::atomic::Ordering::SeqCst);
        self.clock
            .set(self.clock.now() + Duration::from_secs(after));
        let result = match result {
            Ok(_)
                if self
                    .lose_create_reply
                    .swap(false, std::sync::atomic::Ordering::SeqCst) =>
            {
                Err(StorageFailure::Network.into())
            }
            result => result,
        };
        self.record("create", time, result)
    }
    async fn replace(
        &self,
        path: &coven_storage::StoredObject,
        bytes: &[u8],
    ) -> Result<(), coven_storage::StorageError> {
        self.storage.replace(path, bytes).await
    }
    async fn status(
        &self,
        path: &coven_storage::ObjectPath,
    ) -> Result<Option<coven_storage::StoredObject>, coven_storage::StorageError> {
        let time = self.tick();
        let result = self.storage.status(path).await;
        self.record("status", time, result)
    }
    async fn stream_object(
        &self,
        path: &coven_storage::ObjectPath,
        range: Option<coven_storage::ByteRange>,
    ) -> Result<coven_storage::ObjectStream<'_>, coven_storage::StorageError> {
        let time = self.tick();
        let result = self.storage.stream_object(path, range).await;
        self.record("read", time, result)
    }
    async fn list(
        &self,
        prefix: &coven_storage::ObjectPrefix,
    ) -> Result<Vec<coven_storage::StoredObject>, coven_storage::StorageError> {
        self.storage.list(prefix).await
    }
    async fn delete(
        &self,
        path: &coven_storage::ObjectPath,
    ) -> Result<(), coven_storage::StorageError> {
        self.storage.delete(path).await
    }
    async fn grant_access(
        &self,
        account: &str,
    ) -> Result<coven_storage::AccessGrant, coven_storage::StorageError> {
        self.storage.grant_access(account).await
    }
    async fn revoke_access(
        &self,
        account: &str,
    ) -> Result<Vec<coven_storage::RetainedAccess>, coven_storage::StorageError> {
        self.storage.revoke_access(account).await
    }
    async fn begin_upload(
        &self,
        path: &coven_storage::ObjectPath,
        total: u64,
    ) -> Result<coven_storage::UploadSession, coven_storage::StorageError> {
        let time = self.tick();
        let result = self.storage.begin_upload(path, total).await;
        self.record("begin", time, result)
    }
    async fn begin_replace(
        &self,
        previous: &coven_storage::StoredObject,
        total: u64,
    ) -> Result<coven_storage::UploadSession, coven_storage::StorageError> {
        self.storage.begin_replace(previous, total).await
    }
    async fn resume_upload(
        &self,
        session: &mut coven_storage::UploadSession,
    ) -> Result<(), coven_storage::StorageError> {
        let time = self.tick();
        let result = self.storage.resume_upload(session).await;
        self.record("resume", time, result)
    }
    async fn upload_part(
        &self,
        session: &mut coven_storage::UploadSession,
        bytes: &[u8],
    ) -> Result<(), coven_storage::StorageError> {
        let time = self.tick();
        let result = self.storage.upload_part(session, bytes).await;
        if result.is_ok() {
            if let Some(then) = self.on_part.lock().unwrap().take() {
                then();
            }
        }
        self.record("part", time, result)
    }
    async fn finish_upload(
        &self,
        session: &mut coven_storage::UploadSession,
    ) -> Result<(), coven_storage::StorageError> {
        let time = self.tick();
        let result = self.storage.finish_upload(session).await;
        self.record("finish", time, result)
    }
    async fn abort_upload(
        &self,
        session: &coven_storage::UploadSession,
    ) -> Result<(), coven_storage::StorageError> {
        let time = self.tick();
        let result = self.storage.abort_upload(session).await;
        self.record("abort", time, result)
    }
}

/// A fixture whose upload worker sends through `StepStorage`, with its
/// catch-up completed at second 0 and recorded in the trace.
async fn stepping(step: u64) -> (Fixture, Arc<FixedClock>, Arc<StepStorage>) {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let f = fixture(&clock).await;
    let storage = StepStorage::new(&f, &clock, step);
    storage.note(serde_json::json!({"op":"begin","time":0}));
    storage.note(serde_json::json!({"op":"checked","time":0,"send":true,"retired":false}));
    f.files
        .set_storage(Some(storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    (f, clock, storage)
}

fn three_parts_total() -> u64 {
    coven_format::file::FileHeader::new(three_parts().len() as u64)
        .encrypted_size()
        .unwrap()
}

/// Three 64-KiB provider parts in the fixture's Dropbox session.
fn three_parts() -> Vec<u8> {
    (0..150 * 1024).map(|i| (i % 251) as u8).collect()
}

async fn confirmed(f: &Fixture) -> u64 {
    f.files.inner.database.uploads().await.unwrap()[0]
        .session
        .as_ref()
        .map_or(0, |bytes| {
            coven_storage::UploadSession::decode(bytes.as_bytes())
                .unwrap()
                .confirmed_bytes()
        })
}

fn provider_parts(f: &Fixture) -> usize {
    f.storage
        .request_trace()
        .iter()
        .filter(|request| {
            matches!(
                request,
                coven_storage::test_utils::MemoryRequest::Other("upload_part")
            )
        })
        .count()
}

#[tokio::test]
async fn pausing_between_parts_and_reconnecting_resume_the_recorded_session() {
    let (f, clock, storage) = stepping(0).await;
    let file = f.attach("files", "large", three_parts()).await;
    // The app pauses while the first part is in flight: the upload stops at
    // the next part boundary, keeping its session.
    let inner = f.files.inner.clone();
    *storage.on_part.lock().unwrap() = Some(Box::new(move || {
        inner
            .paused
            .store(true, std::sync::atomic::Ordering::Release)
    }));
    assert_eq!(
        f.files.inner.drain_uploads(false).await.unwrap(),
        DrainOutcome::Paused
    );
    f.files.set_uploads_paused(true).await.unwrap();
    assert_eq!(confirmed(&f).await, CHUNK as u64);
    let paused = &f.queue().await.files[0];
    assert_eq!(
        (paused.phase, paused.progress),
        (
            UploadPhase::Waiting,
            Some(UploadProgress {
                bytes_confirmed: CHUNK as u64,
                bytes_total: three_parts_total(),
            })
        )
    );
    assert!(matches!(
        f.files.inner.drain_uploads(false).await.unwrap(),
        DrainOutcome::Paused
    ));
    // Reconnecting forgets the catch-up: nothing is sent until a new one.
    f.send.set(crate::SequenceStatus::Opening);
    let reconnected = StepStorage::new(&f, &clock, 0);
    f.files
        .set_storage(Some(reconnected.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    f.files.set_uploads_paused(false).await.unwrap();
    let requests = f.storage.request_trace().len();
    assert!(f.files.inner.drain_uploads(false).await.is_err());
    assert_eq!(f.storage.request_trace().len(), requests);
    reconnected.catch_up(&f.send);
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    // The session continued at its confirmed part on the new connection.
    assert_eq!(reconnected.calls("resume"), 1);
    assert_eq!(reconnected.calls("begin"), 0);
    assert_eq!(provider_parts(&f), 3);
    assert_eq!(f.files.read_file(&file).await.unwrap(), three_parts());
    f.close().await;
}

/// Bytes the provider holds are progress only once its confirmation is
/// recorded in the session: a lost part reply leaves the committed progress
/// where it was, and the retry resumes from the provider's state (§17.7, E7).
#[tokio::test]
async fn provider_bytes_are_progress_only_once_the_session_records_them() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = fixture(&clock).await;
    let file = f.attach("files", "large", three_parts()).await;
    assert_eq!(f.queue().await.files[0].progress, None);
    f.storage
        .set_faults(Faults {
            lose_part_reply: true,
            ..Faults::none()
        })
        .await;
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    let waiting = &f.queue().await.files[0];
    assert_eq!(
        (waiting.phase, waiting.progress),
        (
            UploadPhase::Waiting,
            Some(UploadProgress {
                bytes_confirmed: 0,
                bytes_total: three_parts_total(),
            })
        )
    );
    assert_eq!(f.upload_reason(&file).await, Some(network()));
    advance(&clock, 1);
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    assert!(f.queue().await.files.is_empty());
    assert_eq!(f.upload_reason(&file).await, None);
    assert_eq!(f.files.read_file(&file).await.unwrap(), three_parts());
    f.close().await;
}

/// The queue, its pause setting and confirmed progress come from the
/// database alone: reopening with storage unreachable reads them unchanged.
#[tokio::test]
async fn the_queue_reads_the_same_after_reopening_offline() {
    let (mut f, _clock, storage) = stepping(0).await;
    f.attach("files", "large", three_parts()).await;
    let inner = f.files.inner.clone();
    *storage.on_part.lock().unwrap() = Some(Box::new(move || {
        inner
            .paused
            .store(true, std::sync::atomic::Ordering::Release)
    }));
    assert_eq!(
        f.files.inner.drain_uploads(false).await.unwrap(),
        DrainOutcome::Paused
    );
    f.files.set_uploads_paused(true).await.unwrap();
    let before = f.queue().await;
    assert!(before.paused);
    f.storage.set_online(false);
    f.reopen().await;
    assert_eq!(f.queue().await, before);
    let mut live = f.subscribe_queue();
    assert_eq!(live.next().await.unwrap(), before);
    f.close().await;
}

#[tokio::test]
async fn elapsed_time_does_not_expire_an_applied_device_between_parts() {
    let (f, _, storage) = stepping(600).await;
    let file = f.attach("files", "large", three_parts()).await;
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    assert_eq!(storage.calls("begin"), 1);
    assert_eq!(storage.calls("finish"), 1);
    assert_eq!(provider_parts(&f), 3);
    assert_eq!(f.files.read_file(&file).await.unwrap(), three_parts());
    f.close().await;
}

#[tokio::test]
async fn other_bytes_at_an_own_file_path_refuse_only_that_upload() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let f = fixture(&clock).await;
    let file = f.attach("files", "first", b"ours".to_vec()).await;
    let path = coven_storage::ObjectPath::file(f.directory.id(), file.uploader(), file.id());
    f.storage
        .plant(&path, b"different complete object")
        .await
        .unwrap();
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    assert!(matches!(
        f.upload_reason(&file).await,
        Some(PendingReason::Refused(_))
    ));
    assert!(f.send.files_allowed());
    let other = f.attach("files", "second", b"next".to_vec()).await;
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    assert!(!f.queued(&other).await);
    assert!(f.queued(&file).await);
    f.close().await;
}

#[path = "file_upload_failure_tests.rs"]
mod failures;

/// Ana's phone locked mid-upload: the upload failed with no network and
/// waits its backoff. Her network report ends that wait, keeping its failure
/// count, and the upload runs (§6.7).
#[tokio::test]
async fn a_network_report_ends_an_offline_upload_wait() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = fixture(&clock).await;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    for _ in 0..2 {
        f.storage
            .set_faults(fail_next(StorageFailure::Network, None))
            .await;
        f.files.inner.drain_uploads(true).await.unwrap();
    }
    assert_eq!(retry(&f).await.unwrap().wait, Duration::from_secs(2));
    let waits = &f.files.inner.waits;
    waits.network_changed();
    assert!(waits
        .end_reported_waits(crate::NetworkOwner::Uploads)
        .await
        .unwrap());
    let ended = retry(&f).await.unwrap();
    assert_eq!(
        (ended.failures, ended.not_before, ended.wait),
        (2, clock.now(), Duration::ZERO)
    );
    // The connection's owner acts on the same report for its own waits.
    assert!(waits
        .end_reported_waits(crate::NetworkOwner::Connection)
        .await
        .unwrap());
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    assert!(!f.queued(&file).await);
    f.close().await;
}

/// A provider that answered decided the upload's wait, and its Retry-After
/// holds the queue: a network report changes neither (§6.7).
#[tokio::test]
async fn a_network_report_leaves_an_answered_upload_wait_and_the_cooldown() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = fixture(&clock).await;
    f.attach("files", "photo", b"photo".to_vec()).await;
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::Network,
            response: Some(503),
            ..Faults::none()
        })
        .await;
    f.files.inner.drain_uploads(true).await.unwrap();
    let answered = retry(&f).await;
    let waits = &f.files.inner.waits;
    waits.network_changed();
    waits
        .end_reported_waits(crate::NetworkOwner::Uploads)
        .await
        .unwrap();
    assert_eq!(retry(&f).await, answered);
    let requests = f.storage.request_trace().len();
    f.files.inner.drain_uploads(false).await.unwrap();
    assert_eq!(f.storage.request_trace().len(), requests);
    // An offline wait under a live cooldown ends, but the cooldown holds.
    advance(&clock, 1);
    f.storage
        .set_faults(fail_next(StorageFailure::Network, None))
        .await;
    f.files.inner.drain_uploads(false).await.unwrap();
    coven_storage::Cooldown::record(waits.as_ref(), Duration::from_secs(600))
        .await
        .unwrap();
    waits.network_changed();
    waits
        .end_reported_waits(crate::NetworkOwner::Uploads)
        .await
        .unwrap();
    assert!(retry(&f).await.unwrap().wait.is_zero());
    let requests = f.storage.request_trace().len();
    f.files.inner.drain_uploads(false).await.unwrap();
    assert_eq!(f.storage.request_trace().len(), requests);
    advance(&clock, 600);
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    f.close().await;
}

/// A report the upload worker could not act on, because ending the wait
/// failed to save, stays: every wait keeps its backoff, and the next run
/// ends the upload's wait (§6.7).
#[tokio::test]
async fn a_report_whose_end_failed_to_save_ends_the_wait_next_time() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = fixture(&clock).await;
    f.attach("files", "photo", b"photo".to_vec()).await;
    f.storage
        .set_faults(fail_next(StorageFailure::Network, None))
        .await;
    f.files.inner.drain_uploads(true).await.unwrap();
    let wait = retry(&f).await;
    let waits = &f.files.inner.waits;
    waits.network_changed();
    f.database.fail_retry_ends(1).await;
    assert!(waits
        .end_reported_waits(crate::NetworkOwner::Uploads)
        .await
        .is_err());
    assert_eq!(retry(&f).await, wait);
    // No new report: the upload worker's next run acts on the one the
    // failure left, ending the wait, and the upload runs.
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    assert!(!waits
        .end_reported_waits(crate::NetworkOwner::Uploads)
        .await
        .unwrap());
    f.close().await;
}

/// The person's retry ends an upload's own wait, but not the provider's
/// cooldown: nothing is sent until the cooldown ends (§6.7).
#[tokio::test]
async fn the_persons_retry_keeps_a_provider_cooldown() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = fixture(&clock).await;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    f.storage
        .set_faults(fail_next(StorageFailure::RateLimited, Some(600)))
        .await;
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    let waits = &f.files.inner.waits;
    waits.retry_requested();
    assert!(waits
        .end_reported_waits(crate::NetworkOwner::Uploads)
        .await
        .unwrap());
    let ended = retry(&f).await.unwrap();
    assert_eq!((ended.failures, ended.wait), (1, Duration::ZERO));
    assert!(waits.cooldown().is_some());
    let requests = f.storage.request_trace().len();
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    assert_eq!(f.storage.request_trace().len(), requests);
    advance(&clock, 600);
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    assert!(!f.queued(&file).await);
    f.close().await;
}
