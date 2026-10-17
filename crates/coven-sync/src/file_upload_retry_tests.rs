//! Upload waits: the queue's persisted retry delays, the category policy
//! and the provider cooldown shared with every consumer (§16.5, E7).

use super::*;
use crate::{PendingReason, Prerequisite};
use coven_database::RetrySubject;
use coven_foundation::clock::{Clock, FixedClock};
use coven_storage::{test_utils::Faults, StorageFailure};

async fn fixture(clock: &Arc<FixedClock>) -> Fixture {
    let f = Fixture::with_clock(Provenance::AppProvided, CacheFill::CacheLazy, clock.clone()).await;
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

fn catch_up() -> PendingReason {
    PendingReason::Waits(Prerequisite::StoreLogCatchUp)
}

fn network() -> PendingReason {
    PendingReason::Storage {
        provider: coven_storage::CloudProvider::Dropbox,
        failure: StorageFailure::Network,
    }
}

/// Keep §10's catch-up fresh while the test clock passes retry delays.
fn advance(f: &Fixture, clock: &FixedClock, seconds: u64) {
    clock.set(clock.now() + Duration::from_secs(seconds));
    f.send.begin();
    f.send
        .complete(crate::send_gate::Binding::Device, Vec::new())
        .unwrap();
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
        // it before the upload waits.
        assert_eq!(f.upload_reason(&file).await, Some(network()));
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
        advance(&f, &clock, wait);
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
    advance(&f, &clock, 4);
    // The device clock moves back an hour while the store is closed.
    clock.set(clock.now() - Duration::from_secs(3600));
    f.reopen().await;
    f.files.stop_background_for_counts().await;
    f.files.set_uploads_paused(false).await.unwrap();
    advance(&f, &clock, 0);
    // Restart waits min(T - now, W): the backward clock cannot stretch it.
    assert_eq!(retry_in(&f), Some(Duration::from_secs(8)));
    advance(&f, &clock, 7);
    assert_eq!(
        f.files.inner.drain_uploads(false).await.unwrap(),
        DrainOutcome::Drained { uploaded: 0 }
    );
    advance(&f, &clock, 1);
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
    advance(&f, &clock, 1);
    let requests = f.storage.request_trace().len();
    assert_eq!(
        f.files.inner.drain_uploads(false).await.unwrap(),
        DrainOutcome::Drained { uploaded: 0 }
    );
    let Err(refused) = f.files.retry_uploads_now().await else {
        panic!("an app retry bypassed the provider cooldown");
    };
    assert!(
        matches!(&refused, crate::SyncError::Storage(error) if error.failure() == StorageFailure::RateLimited),
        "{refused:?}"
    );
    assert_eq!(f.storage.request_trace().len(), requests);
    advance(&f, &clock, 599);
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
    let f = Fixture::with_clock(Provenance::AppProvided, CacheFill::CacheLazy, clock.clone()).await;
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
    advance(&f, &clock, 1);
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
    let f = Fixture::with_clock(Provenance::AppProvided, CacheFill::CacheLazy, clock.clone()).await;
    f.attach("files", "first", b"first".to_vec()).await;
    f.attach("files", "second", b"second".to_vec()).await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_creation(ObjectPrefix::files(f.directory.id()), started, held)
        .await;
    let mut uploads = f.subscribe_queue();
    let mut pending = f.database.subscribe_pending::<crate::PendingRecord>();
    f.files.set_uploads_paused(false).await.unwrap();
    observed.await.unwrap();
    clock.set(SystemTime::UNIX_EPOCH + Duration::from_secs(300));
    resume.send(()).unwrap();
    // The second file waits for the next catch-up; that wait is its record.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !pending
            .next()
            .await
            .unwrap()
            .iter()
            .any(|record| record.reason == PendingReason::Waits(Prerequisite::StoreLogCatchUp))
        {
        }
    })
    .await
    .unwrap();
    let first = f.database.file_ref("files", "first").await.unwrap();
    let second = f.database.file_ref("files", "second").await.unwrap();
    assert!(!f.queued(&first).await);
    assert!(f.queued(&second).await);
    f.send.begin();
    f.send
        .complete(crate::send_gate::Binding::Device, Vec::new())
        .unwrap();
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
/// `step` seconds, then records its time and whether the shared send gate let
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
    fn catch_up(&self, send: &crate::SendGate) {
        send.begin();
        self.note(serde_json::json!({"op":"begin","time":self.seconds()}));
        self.clock.set(self.clock.now() + Duration::from_secs(1));
        send.complete(crate::send_gate::Binding::Device, Vec::new())
            .unwrap();
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
        path: &coven_storage::ObjectPath,
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
    async fn list_writers(
        &self,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<Vec<coven_foundation::id_source::DeviceId>, coven_storage::StorageError> {
        self.storage.list_writers(store).await
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

/// Drain until the file is stored, completing a new catch-up after each
/// drain the gate stopped. Returns the catch-ups it took.
async fn drain_through_expiry(f: &Fixture, storage: &StepStorage, file: &FileRef) -> usize {
    let mut catch_ups = 0;
    while f.queued(file).await {
        assert!(catch_ups < 6, "the upload made no progress");
        if !f.send.can_send() {
            storage.catch_up(&f.send);
            catch_ups += 1;
        }
        let before = confirmed(f).await;
        if drained(f.files.inner.drain_uploads(false).await.unwrap()) == 1 {
            break;
        }
        assert_eq!(f.upload_reason(file).await, Some(catch_up()));
        // Expiry keeps every confirmed part: the next catch-up resumes there.
        assert!(confirmed(f).await >= before);
    }
    catch_ups
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
async fn freshness_expiring_between_parts_keeps_progress_until_the_next_catch_up() {
    let (f, _clock, storage) = stepping(100).await;
    let file = f.attach("files", "large", three_parts()).await;
    // Second 300 ends the catch-up from second 0 before the second part.
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    assert_eq!(f.upload_reason(&file).await, Some(catch_up()));
    assert_eq!(confirmed(&f).await, CHUNK as u64);
    assert_eq!(
        provider_parts(&f),
        1,
        "the refused part never reached the provider"
    );
    // Waiting for the next pass changes neither the path nor the key, and
    // until a catch-up completes the worker sends nothing at all.
    assert_eq!(
        f.files.inner.database.uploads().await.unwrap()[0].file,
        file
    );
    let requests = f.storage.request_trace().len();
    let Err(waiting) = f.files.inner.drain_uploads(false).await else {
        panic!("an expired catch-up let the worker drain");
    };
    assert!(
        matches!(&waiting, crate::SyncError::Storage(error) if error.failure() == StorageFailure::CatchUpRequired)
    );
    assert_eq!(f.storage.request_trace().len(), requests);
    drain_through_expiry(&f, &storage, &file).await;
    // Each part reached the provider once; none was sent again.
    assert_eq!(provider_parts(&f), 3);
    assert_eq!(f.files.read_file(&file).await.unwrap(), three_parts());
    f.close().await;
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
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    f.files.set_uploads_paused(true).await.unwrap();
    assert_eq!(confirmed(&f).await, CHUNK as u64);
    assert_eq!(
        f.queue().await.files[0].phase,
        UploadPhase::Uploading {
            bytes_confirmed: CHUNK as u64,
            bytes_total: three_parts_total(),
        }
    );
    assert!(matches!(
        f.files.inner.drain_uploads(false).await.unwrap(),
        DrainOutcome::Paused
    ));
    // Reconnecting forgets the catch-up: nothing is sent until a new one.
    f.send.invalidate();
    let reconnected = StepStorage::new(&f, &clock, 0);
    f.files
        .set_storage(Some(reconnected.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    f.files.set_uploads_paused(false).await.unwrap();
    let requests = f.storage.request_trace().len();
    assert!(f.files.inner.drain_uploads(false).await.is_err());
    assert_eq!(f.storage.request_trace().len(), requests);
    assert_eq!(drain_through_expiry(&f, &reconnected, &file).await, 1);
    // The session continued at its confirmed part on the new connection.
    assert_eq!(reconnected.calls("resume"), 1);
    assert_eq!(reconnected.calls("begin"), 0);
    assert_eq!(provider_parts(&f), 3);
    assert_eq!(f.files.read_file(&file).await.unwrap(), three_parts());
    f.close().await;
}

/// A lost create reply leaves the worker's own bytes at the path. Comparing
/// them is a worker request too: at the expired second it reads nothing.
async fn lost_reply_and_stale_comparison(f: &Fixture, clock: &FixedClock, storage: &StepStorage) {
    use coven_storage::test_utils::MemoryRequest;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    storage
        .lose_create_reply
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    let path = f.path(&file);
    assert!(f.storage.status(&path).await.unwrap().is_some());
    // The retry's create is checked at second 299 and refused as occupied;
    // the comparison would read at second 300.
    clock.set(SystemTime::UNIX_EPOCH + Duration::from_secs(299));
    storage
        .after_create
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let reads = |f: &Fixture| {
        f.storage
            .request_trace()
            .iter()
            .filter(|request| matches!(request, MemoryRequest::Read(read, _) if *read == path))
            .count()
    };
    assert_eq!(drained(f.files.retry_uploads_now().await.unwrap()), 0);
    assert_eq!(f.upload_reason(&file).await, Some(catch_up()));
    assert_eq!(reads(f), 0);
    assert!(f.queued(&file).await);
    storage
        .after_create
        .store(0, std::sync::atomic::Ordering::SeqCst);
    storage.catch_up(&f.send);
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    assert_eq!(reads(f), 1);
    assert!(!f.queued(&file).await);
}

#[tokio::test]
async fn the_occupied_paths_comparison_waits_for_a_fresh_catch_up() {
    let (f, clock, storage) = stepping(0).await;
    lost_reply_and_stale_comparison(&f, &clock, &storage).await;
    f.close().await;
}

/// The worker's actual requests against Trace's `gateStep` with the upload
/// actor (`upload_worker_gated`, `upload_parts_use_gate`): parts crossing the
/// five-minute boundary, resumption after each new catch-up, and the
/// comparison read after a lost create reply.
#[tokio::test]
#[ignore = "requires COVEN_IO_RUNNER"]
async fn io_differential_upload_worker() {
    let mut cases = Vec::new();
    let mut results = Vec::new();
    let mut trace = |storage: &StepStorage| {
        let events = std::mem::take(&mut *storage.trace.lock().unwrap());
        let (actions, allowed): (Vec<_>, Vec<_>) = events.into_iter().unzip();
        cases.push(serde_json::json!({"check":"send_gate","actions":actions}));
        results.push(serde_json::json!(allowed));
    };
    for step in [1, 60, 100, 149] {
        let (f, _clock, storage) = stepping(step).await;
        let file = f.attach("files", "large", three_parts()).await;
        drain_through_expiry(&f, &storage, &file).await;
        assert_eq!(provider_parts(&f), 3);
        trace(&storage);
        f.close().await;
    }
    let (f, clock, storage) = stepping(0).await;
    lost_reply_and_stale_comparison(&f, &clock, &storage).await;
    trace(&storage);
    f.close().await;
    assert_eq!(results, crate::discovery::tests::io_runner(&cases));
}

/// Bytes the provider holds are progress only once its confirmation is
/// recorded in the session: a lost part reply leaves the committed progress
/// where it was, and the retry resumes from the provider's state (§16.5, E7).
#[tokio::test]
async fn provider_bytes_are_progress_only_once_the_session_records_them() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = fixture(&clock).await;
    let file = f.attach("files", "large", three_parts()).await;
    assert_eq!(f.queue().await.files[0].phase, UploadPhase::Waiting);
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
    assert_eq!(
        f.queue().await.files[0].phase,
        UploadPhase::Uploading {
            bytes_confirmed: 0,
            bytes_total: three_parts_total(),
        }
    );
    assert_eq!(f.upload_reason(&file).await, Some(network()));
    advance(&f, &clock, 1);
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
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
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

#[path = "file_upload_failure_tests.rs"]
mod failures;
