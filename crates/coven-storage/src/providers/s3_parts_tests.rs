//! An S3 multipart upload's parts are independent: they may go in any
//! order, a lost reply's part is found again by listing the upload, and
//! completion lists them by number (§17.9, Appendix E.10).

use super::*;

const PART: usize = 8 * 1024 * 1024;

/// Three parts' bytes, the last shorter, each part's bytes its own.
fn object() -> Vec<u8> {
    (0..2 * PART + 5).map(|i| (i / PART) as u8 + 1).collect()
}

fn path() -> ObjectPath {
    ObjectPath::file(
        STORE_ID,
        crate::test_utils::DEVICE,
        coven_foundation::id_source::ObjectId(uuid::Uuid::from_bytes([0x5a; 16])),
    )
}

/// A session whose first part started its upload, with that part confirmed.
async fn started(storage: &StorageConnection<S3Storage>, bytes: &[u8]) -> UploadSession {
    let mut session = storage.begin_upload(&path(), bytes.len() as u64).await.unwrap();
    assert_eq!(session.part_size(), PART);
    assert!(!session.parts_independent(), "the first part starts the upload alone");
    storage.upload_part(&mut session, &bytes[..PART]).await.unwrap();
    assert!(session.parts_independent());
    session
}

async fn send(
    storage: &StorageConnection<S3Storage>,
    session: &UploadSession,
    bytes: &[u8],
    part: std::ops::Range<u64>,
) -> Result<crate::ConfirmedPart, StorageError> {
    storage
        .send_independent_part(
            session,
            part.start,
            &bytes[part.start as usize..part.end as usize],
            &ByteProgress::unread(),
        )
        .await
}

/// Ben's phone sends a video's last part before its second: both are
/// recorded as they come back, and completion publishes the parts in order.
#[tokio::test]
async fn parts_confirmed_out_of_order_publish_in_order() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let storage = provider(&server.url);
    let bytes = object();
    let mut session = started(&storage, &bytes).await;
    let rest = session.parts_to_send();
    assert_eq!(rest.len(), 2);
    let second = send(&storage, &session, &bytes, rest[0].clone());
    let third = send(&storage, &session, &bytes, rest[1].clone());
    let (second, third) = tokio::join!(second, third);
    session.confirm(third.unwrap()).unwrap();
    assert_eq!(session.confirmed_bytes(), PART as u64 + 5);
    let recorded = UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
    assert_eq!(recorded.parts_to_send(), [rest[0].clone()]);
    session.confirm(second.unwrap()).unwrap();
    assert!(session.parts_to_send().is_empty());
    storage.finish_upload(&mut session).await.unwrap();
    assert_eq!(storage.read(&path()).await.unwrap(), bytes);
}

/// The reply to Ben's third part is lost and his phone stops before the
/// second goes: resuming the recording lists the upload, finds the third
/// part as well as the first, and only the second is sent.
#[tokio::test]
async fn resuming_finds_a_part_whose_reply_was_lost_out_of_order() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let bytes = object();
    let session = started(&storage, &bytes).await;
    let recorded = session.encode().unwrap();
    let rest = session.parts_to_send();
    state.lock().unwrap().fail_part_reply = true;
    send(&storage, &session, &bytes, rest[1].clone()).await.unwrap_err();
    // The process stops; the store opens again from the recording.
    let mut session = UploadSession::decode(recorded.as_bytes()).unwrap();
    assert_eq!(session.parts_to_send(), rest);
    storage.resume_upload(&mut session).await.unwrap();
    assert_eq!(session.parts_to_send(), [rest[0].clone()]);
    assert_eq!(session.confirmed_bytes(), PART as u64 + 5);
    let second = send(&storage, &session, &bytes, rest[0].clone()).await.unwrap();
    session.confirm(second).unwrap();
    storage.finish_upload(&mut session).await.unwrap();
    assert_eq!(storage.read(&path()).await.unwrap(), bytes);
}

/// A part whose reply was lost goes again before the recording learns of
/// it: S3 keeps one part per number, so the second send takes its place
/// and the object holds the part once.
#[tokio::test]
async fn a_part_sent_again_takes_its_own_place() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let bytes = object();
    let mut session = started(&storage, &bytes).await;
    let rest = session.parts_to_send();
    state.lock().unwrap().fail_part_reply = true;
    send(&storage, &session, &bytes, rest[0].clone()).await.unwrap_err();
    for part in session.parts_to_send() {
        let confirmed = send(&storage, &session, &bytes, part).await.unwrap();
        session.confirm(confirmed).unwrap();
    }
    storage.finish_upload(&mut session).await.unwrap();
    assert_eq!(storage.read(&path()).await.unwrap(), bytes);
}

/// Only a part the session lacks may go, whole: a confirmed part, one cut
/// short, or one off a part's start is refused before any request.
#[tokio::test]
async fn only_a_whole_part_the_session_lacks_goes() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let bytes = object();
    let session = started(&storage, &bytes).await;
    let before = state.lock().unwrap().requests.len();
    for (offset, length) in [(0, PART), (PART as u64, PART - 1), (PART as u64 + 1, PART)] {
        let refused = storage
            .send_independent_part(
                &session,
                offset,
                &bytes[offset as usize..offset as usize + length],
                &ByteProgress::unread(),
            )
            .await
            .unwrap_err();
        assert_eq!(refused.failure(), StorageFailure::InvalidPart, "{offset} {length}");
    }
    assert_eq!(state.lock().unwrap().requests.len(), before);
}
