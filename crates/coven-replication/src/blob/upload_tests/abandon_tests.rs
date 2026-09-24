//! Attempts whose drain is dropped mid-transfer.

use super::*;

#[derive(Default)]
struct EndingObserver {
    events: Mutex<Vec<String>>,
    started: tokio::sync::Notify,
}

impl EndingObserver {
    fn push(&self, event: String) {
        self.events.lock().unwrap().push(event);
    }

    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait]
impl BlobTransitionObserver for EndingObserver {
    async fn on_blob_preparation_started(&self, upload: &RowBlobRef) {
        self.push(format!("preparing {}", upload.blob().id));
    }

    async fn on_blob_upload_started(&self, upload: &RowBlobRef) {
        self.push(format!("started {}", upload.blob().id));
        self.started.notify_one();
    }

    async fn on_blob_uploaded(&self, upload: &RowBlobRef) {
        self.push(format!("uploaded {}", upload.blob().id));
    }

    async fn on_blob_upload_failed(&self, upload: &RowBlobRef, _error: &str) {
        self.push(format!("failed {}", upload.blob().id));
    }

    fn on_blob_upload_abandoned(&self, upload: &RowBlobRef) {
        self.push(format!("abandoned {}", upload.blob().id));
    }
}

/// A host clears its in-flight transfer state from the observer's end
/// callbacks. A drain dropped while a transfer is open must end that attempt
/// too, or the host keeps showing a transfer nothing is running.
#[tokio::test]
async fn a_dropped_drain_reports_its_open_attempt_abandoned() {
    let fixture = UploadFixture::new(1).await;
    fixture
        .plant_uploads(&[("abandoned", &[5; 4096])], false)
        .await;
    fixture
        .home
        .slow_creates(64, std::time::Duration::from_millis(50));
    let observer = EndingObserver::default();
    let clock = fixed_clock(T0);

    {
        let drain = fixture.drain(&clock, Some(&observer));
        tokio::pin!(drain);
        tokio::select! {
            () = observer.started.notified() => {}
            outcome = &mut drain => panic!("the slow transfer finished first: {outcome:?}"),
        }
    }

    assert_eq!(
        observer.events(),
        vec![
            "preparing abandoned".to_string(),
            "started abandoned".to_string(),
            "abandoned abandoned".to_string(),
        ],
    );
    assert_eq!(
        fixture.journal_attempt("abandoned").await.0,
        0,
        "an abandoned attempt is not a failed one"
    );
}
