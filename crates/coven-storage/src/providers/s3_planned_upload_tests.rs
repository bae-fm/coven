//! Sessions on a profile that completes multipart uploads in place: a
//! recording names its upload before the first part starts it, and only a
//! name the device alone uses lets a restored recording end what an
//! unrecorded start left (Appendix E.10).

use super::*;

const PLANNED_PART: usize = 8 * 1024 * 1024;

async fn planned_fixture() -> (TestServer, StorageConnection<S3Storage>, Arc<Mutex<Remote>>) {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    (server, storage, state)
}

fn planned_path() -> ObjectPath {
    ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN)
}

/// A restored shared-path session without its native ID cannot claim any
/// upload.
#[tokio::test]
async fn missing_native_identity_retains_both_claimants_uploads_and_refuses_reinitiation() {
    for lost in ["initiation", "part"] {
        let (_server, storage, state) = planned_fixture().await;
        let path = planned_path();
        let mut other = storage.begin_upload(&path, 4).await.unwrap();
        storage.upload_part(&mut other, b"rival").await.unwrap_err();
        storage.upload_part(&mut other, b"peer").await.unwrap();
        let mut upload = storage.begin_upload(&path, 4).await.unwrap();
        // The caller saves the plan its first part's admission marked.
        let permit = storage.admit_part(&mut upload, b"ours").unwrap();
        assert!(permit.marked());
        let recorded = upload.encode().unwrap();
        if lost == "initiation" {
            state.lock().unwrap().lose_initiation_reply = true;
        } else {
            state.lock().unwrap().fail_part_reply = true;
        }
        assert_eq!(
            storage
                .send_part(permit, &mut upload, &ByteProgress::unread())
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Network
        );
        let mut restored = UploadSession::decode(recorded.as_bytes()).unwrap();
        let before = state.lock().unwrap().requests.len();
        assert_eq!(
            storage.abort_upload(&restored).await.unwrap_err().failure(),
            StorageFailure::SessionIdentityUnknown
        );
        assert_eq!(
            storage.upload_part(&mut restored, b"ours").await.unwrap_err().failure(),
            StorageFailure::SessionIdentityUnknown
        );
        assert_eq!(state.lock().unwrap().requests.len(), before);
        assert_eq!(
            storage.resume_upload(&mut restored).await.unwrap_err().failure(),
            StorageFailure::SessionIdentityUnknown
        );
        assert_eq!(restored.encode().unwrap().as_bytes(), recorded.as_bytes());
        assert_eq!(state.lock().unwrap().uploads.len(), 2);
        storage.finish_upload(&mut other).await.unwrap();
        assert_eq!(storage.read(&path).await.unwrap(), b"peer");
        assert_eq!(
            storage.resume_upload(&mut restored).await.unwrap_err().failure(),
            StorageFailure::SessionIdentityUnknown
        );
        assert_eq!(state.lock().unwrap().uploads.len(), 1);
    }
}

/// A claim's recording names its upload at the slot before the first part
/// starts it. The device stops, or the part is refused, before any request
/// starts the upload: nothing exists at the slot, and the restored recording
/// resumes and publishes (§17.9, Appendix E.10).
#[tokio::test]
async fn a_restored_plan_that_never_started_resumes_at_a_shared_slot() {
    let (_server, storage, state) = planned_fixture().await;
    let path = planned_path();
    let upload = storage.begin_upload(&path, 4).await.unwrap();
    let recorded = upload.encode().unwrap();
    assert!(state.lock().unwrap().requests.is_empty(), "named before any request");
    let mut restored = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.resume_upload(&mut restored).await.unwrap();
    assert!(state.lock().unwrap().requests.is_empty(), "nothing to look for at the slot");
    storage.upload_part(&mut restored, b"data").await.unwrap();
    storage.finish_upload(&mut restored).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

/// A cooldown armed by another consumer, which the next request waits out.
#[derive(Default)]
struct Cooling(std::sync::atomic::AtomicBool);
#[async_trait::async_trait]
impl Cooldown for Cooling {
    fn remaining(&self) -> Option<std::time::Duration> {
        self.0
            .load(std::sync::atomic::Ordering::SeqCst)
            .then_some(std::time::Duration::from_secs(30))
    }
    async fn record(&self, _: std::time::Duration) -> Result<(), StorageError> {
        Ok(())
    }
}

/// A sync state that requires a catch-up before anything is published.
#[derive(Default)]
struct CatchingUp(std::sync::atomic::AtomicBool);
impl PublicationGate for CatchingUp {
    fn check(&self, _: &ObjectPath) -> Result<(), StorageError> {
        if self.0.load(std::sync::atomic::Ordering::SeqCst) {
            Err(StorageFailure::CatchUpRequired.into())
        } else {
            Ok(())
        }
    }
}

/// The connection admits a claim's first part before anything may start its
/// upload. A provider cooldown, or a catch-up the slot waits for, refuses it
/// and sends nothing, and the plan stays unmarked: its recording, restored,
/// resumes at the shared slot and publishes once the refusal ends (§17.9,
/// Appendix E.10).
#[tokio::test]
async fn a_refused_first_part_leaves_its_plan_unmarked() {
    for refusal in [StorageFailure::RateLimited, StorageFailure::CatchUpRequired] {
        let (_server, storage, state) = planned_fixture().await;
        let cooldown = Arc::new(Cooling::default());
        let gate = Arc::new(CatchingUp::default());
        let storage = StorageConnection::with_gate(
            storage.provider().clone(),
            crate::test_utils::DEVICE,
            gate.clone(),
            cooldown.clone(),
            Arc::new(crate::TransferBudget::new(crate::TransferLimits::default())),
            Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
        );
        let path = planned_path();
        let mut upload = storage.begin_upload(&path, 4).await.unwrap();
        let lever = match refusal {
            StorageFailure::RateLimited => &cooldown.0,
            _ => &gate.0,
        };
        lever.store(true, std::sync::atomic::Ordering::SeqCst);
        let refused = storage.admit_part(&mut upload, b"data").err().unwrap();
        assert_eq!(refused.failure(), refusal);
        assert!(state.lock().unwrap().requests.is_empty(), "the refusal sent nothing");
        lever.store(false, std::sync::atomic::Ordering::SeqCst);
        let mut restored = UploadSession::decode(upload.encode().unwrap().as_bytes()).unwrap();
        storage.resume_upload(&mut restored).await.unwrap();
        storage.upload_part(&mut restored, b"data").await.unwrap();
        storage.finish_upload(&mut restored).await.unwrap();
        assert_eq!(storage.read(&path).await.unwrap(), b"data");
    }
}

#[tokio::test]
async fn restored_planned_session_can_confirm_its_completed_publication() {
    let (_server, storage, state) = planned_fixture().await;
    let path = planned_path();
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    let permit = storage.admit_part(&mut upload, b"ours").unwrap();
    let recording = upload.encode().unwrap();
    storage.send_part(permit, &mut upload, &ByteProgress::unread()).await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    let mut restored = UploadSession::decode(recording.as_bytes()).unwrap();
    storage.resume_upload(&mut restored).await.unwrap();
    assert!(restored.is_complete());
    assert!(state.lock().unwrap().uploads.is_empty());
}

/// On a profile that completes in place, a file's upload sits at its
/// destination in the device's own folder, which no other upload targets. A
/// restored plan whose initiation's reply was lost, or whose started upload
/// the device stopped before recording, aborts every upload at that name,
/// then starts over or is forgotten (Appendix E.10).
#[tokio::test]
async fn a_planned_file_upload_aborts_what_its_unrecorded_initiation_left() {
    for (lost, abandon) in [(true, false), (false, false), (true, true), (false, true)] {
        let (_server, storage, state) = planned_fixture().await;
        let object = coven_foundation::id_source::ObjectId(uuid::Uuid::from_u128(9));
        let path = ObjectPath::file(STORE_ID, crate::test_utils::DEVICE, object);
        let mut session = storage.begin_upload(&path, 4).await.unwrap();
        let permit = storage.admit_part(&mut session, b"data").unwrap();
        let recorded = session.encode().unwrap();
        state.lock().unwrap().lose_initiation_reply = lost;
        let sent = storage.send_part(permit, &mut session, &ByteProgress::unread()).await;
        if lost {
            assert_eq!(sent.unwrap_err().failure(), StorageFailure::Network);
        } else {
            sent.unwrap();
        }
        assert_eq!(state.lock().unwrap().uploads.len(), 1);
        let mut restored = UploadSession::decode(recorded.as_bytes()).unwrap();
        if abandon {
            storage.abort_upload(&restored).await.unwrap();
            assert!(state.lock().unwrap().uploads.is_empty());
            assert_eq!(storage.status(&path).await.unwrap(), None);
            continue;
        }
        storage.resume_upload(&mut restored).await.unwrap();
        assert_eq!(restored.confirmed_bytes(), 0);
        assert!(state.lock().unwrap().uploads.is_empty());
        storage.upload_part(&mut restored, b"data").await.unwrap();
        storage.finish_upload(&mut restored).await.unwrap();
        assert_eq!(storage.read(&path).await.unwrap(), b"data");
        assert!(state.lock().unwrap().uploads.is_empty());
    }
}

/// A recording with its native id whose upload is gone, and whose destination
/// is empty, has expired; its replacement starts at zero.
#[tokio::test]
async fn missing_session_and_destination_is_expired() {
    let (_server, storage, state) = planned_fixture().await;
    let bytes = vec![3; PLANNED_PART + 1];
    let mut upload = storage.begin_upload(&planned_path(), bytes.len() as u64).await.unwrap();
    storage.upload_part(&mut upload, &bytes[..PLANNED_PART]).await.unwrap();
    state.lock().unwrap().uploads.clear();
    let error = storage.resume_upload(&mut upload).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::SessionExpired);
    assert!(std::error::Error::source(&error).is_some());
    let mut replacement = storage.restart_upload(&upload).await.unwrap();
    assert_eq!(replacement.path(), &planned_path());
    assert_eq!(replacement.confirmed_bytes(), 0);
    for part in bytes.chunks(replacement.part_size()) {
        storage.upload_part(&mut replacement, part).await.unwrap();
    }
    storage.finish_upload(&mut replacement).await.unwrap();
    assert_eq!(storage.read(&planned_path()).await.unwrap(), bytes);
}

/// A part the provider stored but whose reply was lost is found by listing
/// the recorded upload's parts.
#[tokio::test]
async fn multipart_recovers_a_lost_part_reply() {
    let (_server, storage, state) = planned_fixture().await;
    assert_eq!(storage.single_request_limit(), 5 * 1024 * 1024 * 1024);
    let mut bytes = vec![42; 2 * PLANNED_PART];
    bytes.push(b'z');
    let mut upload = storage.begin_upload(&planned_path(), bytes.len() as u64).await.unwrap();
    storage.upload_part(&mut upload, &bytes[..PLANNED_PART]).await.unwrap();
    let recorded = upload.encode().unwrap();
    state.lock().unwrap().fail_part_reply = true;
    assert_eq!(
        storage
            .upload_part(&mut upload, &bytes[PLANNED_PART..2 * PLANNED_PART])
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Network
    );
    let mut upload = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.resume_upload(&mut upload).await.unwrap();
    assert_eq!(upload.confirmed_bytes(), 2 * PLANNED_PART as u64);
    storage.upload_part(&mut upload, b"z").await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    assert_eq!(storage.read(&planned_path()).await.unwrap(), bytes);
}

#[tokio::test]
async fn resumed_multipart_parts_follow_every_page_before_advancing() {
    let (_server, storage, state) = planned_fixture().await;
    let mut upload =
        storage.begin_upload(&planned_path(), PLANNED_PART as u64 * 2 + 1).await.unwrap();
    storage.upload_part(&mut upload, &vec![1; PLANNED_PART]).await.unwrap();
    let mut recorded = upload.clone();
    storage.upload_part(&mut upload, &vec![2; PLANNED_PART]).await.unwrap();
    storage.upload_part(&mut upload, b"z").await.unwrap();
    storage.resume_upload(&mut recorded).await.unwrap();
    assert_eq!(state.lock().unwrap().part_pages, 3);
    assert_eq!(recorded.confirmed_bytes(), recorded.total_bytes());
    storage.finish_upload(&mut recorded).await.unwrap();
}
