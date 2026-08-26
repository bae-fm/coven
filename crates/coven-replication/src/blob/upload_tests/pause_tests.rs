use super::*;

struct PausingObserver {
    paused: tokio::sync::watch::Sender<bool>,
    pause_after_upload: bool,
    started: Mutex<Vec<String>>,
    uploaded: tokio::sync::Notify,
}

impl PausingObserver {
    fn new(paused: bool, pause_after_upload: bool) -> Self {
        Self {
            paused: tokio::sync::watch::channel(paused).0,
            pause_after_upload,
            started: Mutex::new(Vec::new()),
            uploaded: tokio::sync::Notify::new(),
        }
    }

    fn set_paused(&self, paused: bool) {
        self.paused.send_replace(paused);
    }

    fn started(&self) -> Vec<String> {
        self.started.lock().unwrap().clone()
    }
}

#[async_trait]
impl BlobTransitionObserver for PausingObserver {
    async fn on_blob_upload_started(&self, upload: &RowBlobRef) {
        self.started.lock().unwrap().push(upload.blob().id.clone());
    }

    async fn on_blob_uploaded(&self, _upload: &RowBlobRef) {
        if self.pause_after_upload {
            self.set_paused(true);
        }
        self.uploaded.notify_one();
    }
    async fn on_blob_upload_failed(&self, _upload: &RowBlobRef, _error: &str) {}

    fn should_skip_uploads(&self) -> bool {
        *self.paused.borrow()
    }

    async fn wait_until_uploads_paused(&self) {
        wait_for_pause_state(&self.paused, true).await;
    }

    async fn wait_until_uploads_resumed(&self) {
        wait_for_pause_state(&self.paused, false).await;
    }
}

async fn wait_for_pause_state(paused: &tokio::sync::watch::Sender<bool>, target: bool) {
    let mut state = paused.subscribe();
    loop {
        if *state.borrow_and_update() == target {
            return;
        }
        state
            .changed()
            .await
            .expect("pause observer owns the pause sender");
    }
}

struct ActivePauseObserver {
    paused: tokio::sync::watch::Sender<bool>,
    progress_count: AtomicUsize,
    progressed: tokio::sync::Notify,
}

impl ActivePauseObserver {
    fn new() -> Self {
        Self {
            paused: tokio::sync::watch::channel(false).0,
            progress_count: AtomicUsize::new(0),
            progressed: tokio::sync::Notify::new(),
        }
    }

    fn set_paused(&self, paused: bool) {
        self.paused.send_replace(paused);
    }
}

#[async_trait]
impl BlobTransitionObserver for ActivePauseObserver {
    async fn on_blob_upload_started(&self, _upload: &RowBlobRef) {}

    async fn on_blob_upload_progress(&self, _upload: &RowBlobRef, _done: u64, _total: u64) {
        self.progress_count.fetch_add(1, Ordering::SeqCst);
        self.progressed.notify_one();
    }

    async fn on_blob_uploaded(&self, _upload: &RowBlobRef) {}
    async fn on_blob_upload_failed(&self, _upload: &RowBlobRef, _error: &str) {}

    fn should_skip_uploads(&self) -> bool {
        *self.paused.borrow()
    }

    async fn wait_until_uploads_paused(&self) {
        wait_for_pause_state(&self.paused, true).await;
    }

    async fn wait_until_uploads_resumed(&self) {
        wait_for_pause_state(&self.paused, false).await;
    }
}

#[tokio::test]
async fn paused_queue_admits_nothing_under_concurrency() {
    let fixture = UploadFixture::new(3).await;
    let ids = fixture.seed_uploads(3).await;
    let observer = PausingObserver::new(true, false);

    let outcome = fixture
        .drain(&fixed_clock(T0), Some(&observer))
        .await
        .unwrap();
    assert!(matches!(outcome, DrainOutcome::Paused));
    assert_eq!(fixture.home.create_calls(), 0);
    assert!(observer.started().is_empty());
    for id in ids {
        assert!(!is_created(&fixture.journal(&id).await));
    }
}

#[tokio::test]
async fn pause_between_uploads_resumes_the_same_drain_in_order() {
    let fixture = UploadFixture::new(1).await;
    let ids = fixture.seed_uploads(2).await;
    let observer = PausingObserver::new(false, true);

    let clock = fixed_clock(T0);
    let drain = fixture.drain(&clock, Some(&observer));
    tokio::pin!(drain);

    tokio::select! {
        _ = observer.uploaded.notified() => {}
        result = &mut drain => panic!("drain completed before the first upload paused it: {result:?}"),
    }
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
        result = &mut drain => panic!("paused drain completed: {result:?}"),
    }
    assert_eq!(observer.started(), vec![ids[0].clone()]);
    assert!(is_created(&fixture.journal(&ids[0]).await));
    assert!(!is_created(&fixture.journal(&ids[1]).await));

    observer.set_paused(false);
    let outcome = drain.await.expect("resume the upload drain");
    assert_eq!(outcome.uploaded(), 2);
    assert_eq!(observer.started(), ids);
    assert!(is_created(&fixture.journal(&ids[1]).await));
}

#[tokio::test]
async fn pausing_suspends_an_active_upload_and_resume_continues_the_same_create() {
    let fixture = UploadFixture::new(1).await;
    let bytes = vec![7; 10_000];
    fixture.plant_uploads(&[("pausable", &bytes)], false).await;
    fixture
        .home
        .slow_creates(1_000, std::time::Duration::from_millis(25));
    let observer = ActivePauseObserver::new();
    let clock = fixed_clock(T0);
    let drain = fixture.drain(&clock, Some(&observer));
    tokio::pin!(drain);

    tokio::select! {
        _ = observer.progressed.notified() => {}
        result = &mut drain => panic!("upload completed before it could be paused: {result:?}"),
    }
    observer.set_paused(true);
    let progress_at_pause = observer.progress_count.load(Ordering::SeqCst);

    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
        result = &mut drain => panic!("paused upload completed: {result:?}"),
    }
    assert_eq!(
        observer.progress_count.load(Ordering::SeqCst),
        progress_at_pause,
        "provider progress advanced while uploads were paused",
    );

    observer.set_paused(false);
    let outcome = drain.await.expect("resume the upload drain");
    assert_eq!(outcome.uploaded(), 1);
    assert_eq!(fixture.home.create_calls(), 1);
}
