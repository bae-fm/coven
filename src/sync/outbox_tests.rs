//! Tests for outbox upload processing: record-and-continue, per-entry backoff,
//! and the upload lifecycle observer callbacks.
//!
//! These drive the real `process_uploads` against a real [`crate::database::Database`]
//! (carrying the `cloud_outbox` bookkeeping table), a `RecordingObserver`, and
//! `InMemoryCloudHome` / `FailingCloudHome` (the cloud backend). The unit under
//! test is `process_uploads` itself; only the cloud backend and observer are fakes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

use chrono::Duration;

use super::outbox::{backoff_window, process_uploads};
use crate::blob::BlobUploadObserver;
use crate::clock::{Clock, FixedClock};
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::sync::invite::{create_invitation, unwrap_library_key};
use crate::sync::membership::{
    sign_membership_entry, MemberRole, MembershipAction, MembershipChain, MembershipEntry,
};
use crate::sync::test_helpers::MockSyncStorage;
use rusqlite::OptionalExtension;

// --- Database under test ----------------------------------------------------

/// A `Database` over an in-memory connection with just the bookkeeping tables —
/// no synced tables (the outbox doesn't need them). The outbox lives in coven's
/// `cloud_outbox` migration table, created by `Database::open`.
fn open_outbox_db() -> Database {
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        Vec::new(),
        "test-device".to_string(),
        |_conn| Ok(()),
    )
    .expect("open outbox database");
    db
}

/// Insert a fully-specified `cloud_outbox` upload row.
async fn insert_upload(
    db: &Database,
    id: i64,
    file_id: &str,
    cloud_key: &str,
    source_path: Option<String>,
    attempt_count: i64,
    last_attempt_at: Option<String>,
) {
    let (file_id, cloud_key) = (file_id.to_string(), cloud_key.to_string());
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO cloud_outbox \
             (id, operation, file_id, cloud_key, source_path, created_at, \
              attempt_count, last_attempt_at) \
             VALUES (?1, 'upload', ?2, ?3, ?4, '2024-01-01T00:00:00Z', ?5, ?6)",
            rusqlite::params![
                id,
                file_id,
                cloud_key,
                source_path,
                attempt_count,
                last_attempt_at
            ],
        )
        .map(|_| ())
        .map_err(DbError::from)
    })
    .await
    .expect("insert outbox upload");
}

/// Read back `(attempt_count, last_error, last_attempt_at)` for an entry, or
/// `None` if it was removed.
async fn get_upload(db: &Database, id: i64) -> Option<(i64, Option<String>, Option<String>)> {
    db.call(move |conn| {
        conn.query_row(
            "SELECT attempt_count, last_error, last_attempt_at FROM cloud_outbox WHERE id = ?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(DbError::from)
    })
    .await
    .expect("query outbox entry")
}

// --- Fakes ------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum ObsEvent {
    Started(String),
    Progress(String, u64, u64),
    Uploaded(String),
    Failed(String, String),
}

/// Records the upload-lifecycle callbacks in arrival order.
struct RecordingObserver {
    events: Mutex<Vec<ObsEvent>>,
}

impl RecordingObserver {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }
    fn events(&self) -> Vec<ObsEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl BlobUploadObserver for RecordingObserver {
    async fn on_blob_upload_started(&self, file_id: &str) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Started(file_id.to_string()));
    }
    async fn on_blob_upload_progress(&self, file_id: &str, bytes_done: u64, bytes_total: u64) {
        self.events.lock().unwrap().push(ObsEvent::Progress(
            file_id.to_string(),
            bytes_done,
            bytes_total,
        ));
    }
    async fn on_blob_uploaded(&self, file_id: &str) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Uploaded(file_id.to_string()));
    }
    async fn on_blob_upload_failed(&self, file_id: &str, error: &str) {
        self.events
            .lock()
            .unwrap()
            .push(ObsEvent::Failed(file_id.to_string(), error.to_string()));
    }
}

/// A cloud backend whose `write` always fails. Counts write attempts so a test
/// can assert that a backed-off entry was not attempted.
struct FailingCloudHome {
    write_calls: AtomicUsize,
}

impl FailingCloudHome {
    fn new() -> Self {
        Self {
            write_calls: AtomicUsize::new(0),
        }
    }
    fn write_calls(&self) -> usize {
        self.write_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl CloudHome for FailingCloudHome {
    async fn write(
        &self,
        _key: &str,
        _data: Vec<u8>,
        _progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        self.write_calls.fetch_add(1, Ordering::SeqCst);
        Err(CloudHomeError::Storage("induced write failure".into()))
    }
    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn grant_access(&self, _member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn revoke_access(&self, _member_id: &str) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
}

/// A cloud backend whose `write` reports progress one chunk at a time with a
/// delay between chunks, so the upload spans several of `process_uploads`'
/// coalescing ticks.
struct SlowChunkedCloudHome {
    chunk: usize,
    per_chunk_delay: std::time::Duration,
}

#[async_trait::async_trait]
impl CloudHome for SlowChunkedCloudHome {
    async fn write(
        &self,
        _key: &str,
        data: Vec<u8>,
        progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        let mut sent = 0u64;
        for chunk in data.chunks(self.chunk) {
            tokio::time::sleep(self.per_chunk_delay).await;
            sent += chunk.len() as u64;
            progress(sent);
        }
        Ok(())
    }
    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn grant_access(&self, _member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
    async fn revoke_access(&self, _member_id: &str) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by process_uploads")
    }
}

/// A `CloudHome` that only answers `grant_access` (with a dummy S3 join info),
/// which is all `create_invitation` reads from the cloud home. The rest is
/// unreachable in these tests.
struct GrantingCloudHome;

#[async_trait::async_trait]
impl CloudHome for GrantingCloudHome {
    async fn write(
        &self,
        _key: &str,
        _data: Vec<u8>,
        _progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by create_invitation")
    }
    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by create_invitation")
    }
    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by create_invitation")
    }
    async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        unimplemented!("not exercised by create_invitation")
    }
    async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by create_invitation")
    }
    async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
        unimplemented!("not exercised by create_invitation")
    }
    async fn grant_access(&self, _member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        Ok(CloudHomeJoinInfo::S3 {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key: "test-access-key".to_string(),
            secret_key: "test-secret-key".to_string(),
            key_prefix: None,
        })
    }
    async fn revoke_access(&self, _member_id: &str) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by create_invitation")
    }
}

// --- Helpers ---------------------------------------------------------------

const T0: &str = "2024-06-01T00:00:00Z";

fn fixed_clock(rfc3339: &str) -> FixedClock {
    FixedClock(
        chrono::DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&chrono::Utc),
    )
}

fn enc() -> RwLock<EncryptionService> {
    RwLock::new(EncryptionService::new_with_key(&[0u8; 32]))
}

fn upload_entry(
    id: i64,
    file_id: &str,
    cloud_key: &str,
    source_path: Option<String>,
) -> OutboxEntry {
    OutboxEntry {
        id,
        operation: OutboxOperation::Upload,
        file_id: file_id.to_string(),
        cloud_key: cloud_key.to_string(),
        source_path,
        content_key: None,
        created_at: "2024-01-01T00:00:00Z".to_string(),
        min_seq: None,
        attempt_count: 0,
        last_error: None,
        last_attempt_at: None,
    }
}

fn write_temp_file(dir: &std::path::Path, name: &str, contents: &[u8]) -> String {
    let p = dir.join(name);
    std::fs::write(&p, contents).unwrap();
    p.to_string_lossy().to_string()
}

fn pubkey_hex(kp: &UserKeypair) -> String {
    hex::encode(kp.public_key)
}

/// A membership chain seeded with `owner` as the founding owner.
fn bootstrap_chain(owner: &UserKeypair) -> MembershipChain {
    let pk_hex = pubkey_hex(owner);
    let mut entry = MembershipEntry {
        action: MembershipAction::Add,
        user_pubkey: pk_hex.clone(),
        role: MemberRole::Owner,
        timestamp: "0000000001000-0000-dev1".to_string(),
        author_pubkey: pk_hex,
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, owner);
    let mut chain = MembershipChain::new();
    chain.add_entry(entry).unwrap();
    chain
}

// --- Tests -----------------------------------------------------------------

/// Record-and-continue: a failing entry no longer stops the drain, so a good
/// entry queued behind it still uploads in the same cycle.
#[tokio::test]
async fn bad_item_does_not_block_good_later_item() {
    let tmp = tempfile::tempdir().unwrap();
    let good_path = write_temp_file(tmp.path(), "good.bin", b"good-bytes");
    let missing_path = tmp.path().join("missing.bin").to_string_lossy().to_string();

    let db = open_outbox_db();
    insert_upload(&db, 1, "fa", "key-a", Some(missing_path), 0, None).await; // read fails
    insert_upload(&db, 2, "fb", "key-b", Some(good_path), 0, None).await; // uploads fine
    let cloud = InMemoryCloudHome::new();
    let observer = RecordingObserver::new();
    let clock = fixed_clock(T0);

    let n = process_uploads(&db, &cloud, &enc(), tmp.path(), &clock, Some(&observer))
        .await
        .unwrap();

    assert_eq!(n, 1, "the good entry uploads despite the earlier failure");
    assert!(cloud.get("key-b").is_some(), "good blob landed in cloud");
    assert!(cloud.get("key-a").is_none(), "failed blob did not land");

    let (attempt, err, last) = get_upload(&db, 1).await.expect("failed entry stays queued");
    assert_eq!(attempt, 1);
    assert!(err.is_some());
    let recorded = chrono::DateTime::parse_from_rfc3339(last.as_deref().unwrap()).unwrap();
    assert_eq!(recorded.with_timezone(&chrono::Utc), clock.now());

    assert!(get_upload(&db, 2).await.is_none(), "uploaded entry removed");
}

/// A failed attempt persists attempt_count + last_error, and a later cycle past
/// the backoff window retries and bumps the count again.
#[tokio::test]
async fn failure_persists_attempt_count_and_last_error() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = open_outbox_db();
    insert_upload(&db, 1, "f1", "k1", Some(path), 0, None).await;
    let cloud = FailingCloudHome::new();

    process_uploads(&db, &cloud, &enc(), tmp.path(), &fixed_clock(T0), None)
        .await
        .unwrap();
    let (attempt, err, _) = get_upload(&db, 1).await.unwrap();
    assert_eq!(attempt, 1);
    assert!(err.as_deref().unwrap().contains("cloud write failed"));

    // 31s later — past the 30s window for attempt_count==1 → retried.
    process_uploads(
        &db,
        &cloud,
        &enc(),
        tmp.path(),
        &fixed_clock("2024-06-01T00:00:31Z"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(get_upload(&db, 1).await.unwrap().0, 2);
    assert_eq!(cloud.write_calls(), 2);
}

/// An entry still inside its backoff window is skipped — not read, not written,
/// no started event, attempt_count untouched — then retried once the window
/// elapses.
#[tokio::test]
async fn backoff_skips_item_inside_window() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = open_outbox_db();
    insert_upload(&db, 1, "f1", "k1", Some(path), 1, Some(T0.to_string())).await;
    let cloud = FailingCloudHome::new();
    let observer = RecordingObserver::new();

    // 10s after last attempt: inside the 30s window for attempt_count==1.
    let n = process_uploads(
        &db,
        &cloud,
        &enc(),
        tmp.path(),
        &fixed_clock("2024-06-01T00:00:10Z"),
        Some(&observer),
    )
    .await
    .unwrap();
    assert_eq!(n, 0);
    assert_eq!(
        cloud.write_calls(),
        0,
        "no write attempted inside backoff window"
    );
    assert!(
        observer.events().is_empty(),
        "no started event for skipped entry"
    );
    assert_eq!(
        get_upload(&db, 1).await.unwrap().0,
        1,
        "attempt_count unchanged"
    );

    // 31s after last attempt: window elapsed → attempted (and fails again).
    process_uploads(
        &db,
        &cloud,
        &enc(),
        tmp.path(),
        &fixed_clock("2024-06-01T00:00:31Z"),
        Some(&observer),
    )
    .await
    .unwrap();
    assert_eq!(
        cloud.write_calls(),
        1,
        "write attempted past backoff window"
    );
    assert_eq!(get_upload(&db, 1).await.unwrap().0, 2);
}

#[tokio::test]
async fn observer_fires_started_then_uploaded_on_success() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = open_outbox_db();
    insert_upload(&db, 1, "fid", "k1", Some(path), 0, None).await;
    let cloud = InMemoryCloudHome::new();
    let observer = RecordingObserver::new();

    process_uploads(
        &db,
        &cloud,
        &enc(),
        tmp.path(),
        &fixed_clock(T0),
        Some(&observer),
    )
    .await
    .unwrap();

    // A small file uploads instantly, so the coalescing ticker never fires; the
    // terminal progress forward on success still emits one full-size Progress
    // between Started and Uploaded.
    let events = observer.events();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0], ObsEvent::Started("fid".into()));
    assert_eq!(events[2], ObsEvent::Uploaded("fid".into()));
    match events[1] {
        ObsEvent::Progress(ref fid, done, total) => {
            assert_eq!(fid, "fid");
            assert_eq!(done, total, "terminal forward reports done == total");
            assert!(total > 5, "encrypted size exceeds the 5 plaintext bytes");
        }
        ref other => panic!("expected Progress, got {other:?}"),
    }
}

#[tokio::test]
async fn observer_fires_started_then_failed_on_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let path = write_temp_file(tmp.path(), "f.bin", b"bytes");
    let db = open_outbox_db();
    insert_upload(&db, 1, "fid", "k1", Some(path), 0, None).await;
    let cloud = FailingCloudHome::new();
    let observer = RecordingObserver::new();

    process_uploads(
        &db,
        &cloud,
        &enc(),
        tmp.path(),
        &fixed_clock(T0),
        Some(&observer),
    )
    .await
    .unwrap();

    let events = observer.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0], ObsEvent::Started("fid".into()));
    match &events[1] {
        ObsEvent::Failed(fid, err) => {
            assert_eq!(fid, "fid");
            assert!(err.contains("cloud write failed"));
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

/// A slow, chunked upload reports mid-file progress: the coalescing ticker
/// forwards an advancing byte count to the observer between Started and Uploaded,
/// and the final forwarded value equals the total.
#[tokio::test(start_paused = true)]
async fn observer_receives_advancing_midfile_progress() {
    let tmp = tempfile::tempdir().unwrap();
    let total = 10_000usize;
    let path = write_temp_file(tmp.path(), "big.bin", &vec![7u8; total]);
    let db = open_outbox_db();
    insert_upload(&db, 1, "fid", "k1", Some(path), 0, None).await;
    let cloud = SlowChunkedCloudHome {
        chunk: 1000,
        per_chunk_delay: std::time::Duration::from_millis(500),
    };
    let observer = RecordingObserver::new();

    process_uploads(
        &db,
        &cloud,
        &enc(),
        tmp.path(),
        &fixed_clock(T0),
        Some(&observer),
    )
    .await
    .unwrap();

    let events = observer.events();
    assert_eq!(events.first(), Some(&ObsEvent::Started("fid".into())));
    assert_eq!(events.last(), Some(&ObsEvent::Uploaded("fid".into())));

    let progress: Vec<(u64, u64)> = events
        .iter()
        .filter_map(|e| match e {
            ObsEvent::Progress(fid, done, total) if fid == "fid" => Some((*done, *total)),
            _ => None,
        })
        .collect();
    assert!(
        progress.len() >= 2,
        "expected several mid-file progress ticks, got {progress:?}"
    );
    for w in progress.windows(2) {
        assert!(w[1].0 >= w[0].0, "progress went backwards: {progress:?}");
    }
    let (last_done, last_total) = *progress.last().unwrap();
    assert_eq!(
        last_done, last_total,
        "final progress reports done == total"
    );
    assert!(last_total >= total as u64, "total covers the whole payload");
}

#[test]
fn backoff_window_is_exponential_and_capped() {
    assert_eq!(backoff_window(0), Duration::zero());
    assert_eq!(backoff_window(1), Duration::seconds(30));
    assert_eq!(backoff_window(2), Duration::seconds(60));
    assert_eq!(backoff_window(3), Duration::seconds(120));
    // 30 · 2^7 = 3840, capped to the 3600s ceiling.
    assert_eq!(backoff_window(8), Duration::seconds(3600));
    assert_eq!(backoff_window(50), Duration::seconds(3600));
}

/// The whole multi-device flow for managed (cloud) content. Device A creates a
/// library, invites device B (wrapping the library master key to B's identity),
/// and uploads a release's audio through the real outbox encrypted with a random
/// per-release key. Device B joins (unwraps the master key with its own keypair),
/// then fetches the blob and decrypts it with the per-release key — the key that,
/// in the app, rides the synced `releases` row.
///
/// The load-bearing assertion is that the master key — which every member holds —
/// does NOT decrypt the content: the per-release key is what scopes a release so
/// it can be read (or handed to a share recipient) without exposing the whole
/// library. This test fails if `process_uploads` encrypts content with the master
/// key instead of the entry's `content_key`.
#[tokio::test]
async fn member_joins_then_fetches_and_decrypts_per_release_content() {
    // --- Device A: library master key, owner identity, membership chain. ---
    let master_key: [u8; 32] = [7u8; 32];
    let owner = UserKeypair::generate(); // device A
    let joiner = UserKeypair::generate(); // device B
    let mut chain = bootstrap_chain(&owner);

    // The content blob lives in the cloud home; the wrapped library key lives in
    // the membership storage. (In production both are paths in one cloud home;
    // the join API routes the wrapped key through `SyncStorage`, so the test uses
    // a `MockSyncStorage` for it and an `InMemoryCloudHome` for content.)
    let cloud = InMemoryCloudHome::new();
    let membership = MockSyncStorage::new();

    // Device A invites device B: seals the master key to B's pubkey and records a
    // signed membership entry.
    create_invitation(
        &membership,
        &GrantingCloudHome,
        &mut chain,
        &owner,
        &pubkey_hex(&joiner),
        MemberRole::Member,
        &master_key,
        "0000000002000-0000-dev1",
    )
    .await
    .expect("invite device B");

    // --- Device A uploads a release's audio through the real outbox, encrypted
    // with a random per-release key (distinct from the master key). ---
    let k_release: [u8; 32] = [9u8; 32];
    let plaintext = b"AUDIO-FILE-BYTES-for-one-release";
    let tmp = tempfile::tempdir().unwrap();
    let source = write_temp_file(tmp.path(), "track.flac", plaintext);

    let cloud_key = "storage/ab/cd/file-1";
    let mut entry = upload_entry(1, "file-1", cloud_key, Some(source));
    entry.content_key = Some(k_release);
    let db = MockBookkeeping::with_uploads(vec![entry]);
    let master_enc = RwLock::new(EncryptionService::from_key(master_key));

    let n = process_uploads(&db, &cloud, &master_enc, tmp.path(), &fixed_clock(T0), None)
        .await
        .expect("upload");
    assert_eq!(n, 1, "the release blob uploads");

    // At rest the blob is per-release ciphertext: not plaintext, and NOT
    // decryptable with the master key every member holds.
    let at_rest = cloud.get(cloud_key).expect("blob present in cloud");
    assert_ne!(at_rest, plaintext, "content is encrypted at rest");
    assert!(
        EncryptionService::from_key(master_key)
            .decrypt(&at_rest)
            .is_err(),
        "the master key must NOT decrypt per-release content"
    );
    assert_eq!(
        EncryptionService::from_key(k_release)
            .decrypt(&at_rest)
            .unwrap(),
        plaintext,
        "the per-release key decrypts the content"
    );

    // --- Device B joins: unwraps the library master key with its own identity. ---
    let joined_master = unwrap_library_key(&membership as &dyn CloudHome, &joiner)
        .await
        .expect("device B unwraps the library key by joining");
    assert_eq!(
        joined_master, master_key,
        "joining recovers the library master key"
    );

    // Device B fetches the blob. Membership alone (the master key) does not unlock
    // it; with the release's content key — which rides the synced `releases` row,
    // handed here as the sync would deliver it — B recovers the audio.
    let fetched = cloud.get(cloud_key).expect("device B fetches the blob");
    assert!(
        EncryptionService::from_key(joined_master)
            .decrypt(&fetched)
            .is_err(),
        "membership alone does not unlock a release's content"
    );
    let recovered = EncryptionService::from_key(k_release)
        .decrypt(&fetched)
        .expect("device B decrypts the content with the per-release key");
    assert_eq!(recovered, plaintext, "device B recovers the original audio");
}
