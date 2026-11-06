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
        let recorded = upload.encode().unwrap();
        if lost == "initiation" {
            state.lock().unwrap().lose_initiation_reply = true;
        } else {
            state.lock().unwrap().fail_part_reply = true;
        }
        assert_eq!(
            storage.upload_part(&mut upload, b"ours").await.unwrap_err().failure(),
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

#[tokio::test]
async fn restored_planned_session_can_confirm_its_completed_publication() {
    let (_server, storage, state) = planned_fixture().await;
    let path = planned_path();
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    let recording = upload.encode().unwrap();
    storage.upload_part(&mut upload, b"ours").await.unwrap();
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
        let recorded = session.encode().unwrap();
        if lost {
            state.lock().unwrap().lose_initiation_reply = true;
            assert_eq!(
                storage.upload_part(&mut session, b"data").await.unwrap_err().failure(),
                StorageFailure::Network
            );
        } else {
            storage.upload_part(&mut session, b"data").await.unwrap();
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
