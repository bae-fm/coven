//! Tests for the blob engine's upload drain: record-and-continue, per-entry
//! backoff, scope-resolved sealing, and the upload lifecycle observer callbacks.
//!
//! These drive the real [`drain_uploads`] against a real [`crate::database::Database`]
//! (carrying the `cloud_outbox` bookkeeping table), a `RecordingObserver`, and
//! `InMemoryCloudHome` / `FailingCloudHome` (the cloud backend). The unit under
//! test is `drain_uploads` itself; only the cloud backend and observer are fakes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

use chrono::Duration;

use super::upload::{backoff_window, drain_uploads, DrainOutcome};
use crate::blob::BlobTransitionObserver;
use crate::clock::{Clock, FixedClock};
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::hlc::Hlc;
use crate::sync::invite::{create_invitation, unwrap_library_keyring_for_owners_with_activation};
use crate::sync::membership::MemberRole;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{
    bootstrap_chain, pubkey_hex, publish_membership_chain_head, MockSyncStorage,
};
use rusqlite::OptionalExtension;

/// Run the real [`drain_uploads`] with a throwaway HLC, the register coven stamps a
/// manage flip from. These drain tests carry no synced/gated tables (an `open_outbox_
/// db` has only the bookkeeping schema), so no upload resolves to a gated root — the
/// completion flip never fires and the HLC only ever mints the stamps that go unused.
async fn run_drain(
    db: &Database,
    cloud: &dyn CloudHome,
    cipher: &std::sync::RwLock<CloudCipher>,
    library_dir: &LibraryDir,
    clock: &dyn Clock,
    observer: Option<&dyn BlobTransitionObserver>,
) -> Result<DrainOutcome, String> {
    let hlc = Hlc::new("test-device".to_string());
    drain_uploads(
        db,
        cloud,
        cipher,
        "test-lib",
        library_dir,
        clock,
        &hlc,
        observer,
    )
    .await
}

// --- Database under test ----------------------------------------------------

/// A `Database` over an in-memory connection with just the bookkeeping tables —
/// no synced tables (the upload drain doesn't need them). The upload queue lives
/// in coven's `cloud_outbox` migration table, created by `Database::open`.
fn open_outbox_db() -> Database {
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        Vec::new(),
        "test-device".to_string(),
        &[],
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
    let scope = crate::blob::BlobScope::Master.to_outbox_str();
    db.call(move |conn| {
        conn.execute(
            &format!(
                "INSERT INTO cloud_outbox \
                 (id, operation, file_id, cloud_key, source_path, scope, created_at, \
                  attempt_count, last_attempt_at) \
                 VALUES (?1, 'upload', ?2, ?3, ?4, '{scope}', '2024-01-01T00:00:00Z', ?5, ?6)"
            ),
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BlobTransitionObserver for RecordingObserver {
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl CloudHome for FailingCloudHome {
    async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.write_calls.fetch_add(1, Ordering::SeqCst);
        Err(CloudHomeError::Storage("induced write failure".into()))
    }
    async fn open_multipart<'a>(
        &'a self,
        _key: &str,
        _total_len: u64,
    ) -> Result<crate::storage::cloud::BoxPartSink<'a>, CloudHomeError> {
        self.write_calls.fetch_add(1, Ordering::SeqCst);
        Err(CloudHomeError::Storage("induced write failure".into()))
    }
    fn multipart_threshold(&self) -> u64 {
        // Small upload payloads in these tests go via put_object.
        8 * 1024 * 1024
    }
    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn grant_access(
        &self,
        _grant: crate::storage::cloud::CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn revoke_access(
        &self,
        _revoke: crate::storage::cloud::CloudAccessRevoke,
    ) -> Result<crate::storage::cloud::RevokeOutcome, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
}

/// A cloud backend whose multipart sink accepts each part after a delay, so the
/// upload spans several of `drain_uploads`' coalescing ticks and the driver's
/// per-part progress advances over time.
struct SlowChunkedCloudHome {
    chunk: usize,
    per_chunk_delay: std::time::Duration,
}

/// The delay sink: each `send_part` sleeps, so the driver's progress advances one
/// part at a time across several ticks.
struct SlowPartSink {
    part_size: usize,
    per_chunk_delay: std::time::Duration,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl crate::storage::cloud::PartSink for SlowPartSink {
    fn part_size(&self) -> usize {
        self.part_size
    }
    async fn send_part(
        &mut self,
        _part: bytes::Bytes,
        _offset: u64,
        _is_last: bool,
    ) -> Result<(), CloudHomeError> {
        tokio::time::sleep(self.per_chunk_delay).await;
        Ok(())
    }
    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl CloudHome for SlowChunkedCloudHome {
    async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
        Ok(())
    }
    async fn open_multipart<'a>(
        &'a self,
        _key: &str,
        _total_len: u64,
    ) -> Result<crate::storage::cloud::BoxPartSink<'a>, CloudHomeError> {
        Ok(Box::new(SlowPartSink {
            part_size: self.chunk,
            per_chunk_delay: self.per_chunk_delay,
        }))
    }
    fn multipart_threshold(&self) -> u64 {
        // Stream every non-empty payload so the delay sink drives progress.
        0
    }
    async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
        // The upload drain cancels any tombstone for a key it just wrote, which
        // deletes the (absent) tombstone object. This fake stores nothing, so the
        // cancel is a successful no-op.
        Ok(())
    }
    async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn grant_access(
        &self,
        _grant: crate::storage::cloud::CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
    }
    async fn revoke_access(
        &self,
        _revoke: crate::storage::cloud::CloudAccessRevoke,
    ) -> Result<crate::storage::cloud::RevokeOutcome, CloudHomeError> {
        unimplemented!("not exercised by drain_uploads")
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

fn enc() -> RwLock<CloudCipher> {
    RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [0u8; 32],
    )))
}

fn write_temp_file(dir: &std::path::Path, name: &str, contents: &[u8]) -> String {
    let p = dir.join(name);
    std::fs::write(&p, contents).unwrap();
    p.to_string_lossy().to_string()
}

// --- Tests -----------------------------------------------------------------

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

    let n = run_drain(
        &db,
        &cloud,
        &enc(),
        &LibraryDir::new(tmp.path()),
        &clock,
        Some(&observer),
    )
    .await
    .unwrap()
    .uploaded;

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

    run_drain(
        &db,
        &cloud,
        &enc(),
        &LibraryDir::new(tmp.path()),
        &fixed_clock(T0),
        None,
    )
    .await
    .unwrap();
    let (attempt, err, _) = get_upload(&db, 1).await.unwrap();
    assert_eq!(attempt, 1);
    assert!(err.as_deref().unwrap().contains("cloud write failed"));

    // 31s later — past the 30s window for attempt_count==1 → retried.
    run_drain(
        &db,
        &cloud,
        &enc(),
        &LibraryDir::new(tmp.path()),
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
    let n = run_drain(
        &db,
        &cloud,
        &enc(),
        &LibraryDir::new(tmp.path()),
        &fixed_clock("2024-06-01T00:00:10Z"),
        Some(&observer),
    )
    .await
    .unwrap()
    .uploaded;
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
    run_drain(
        &db,
        &cloud,
        &enc(),
        &LibraryDir::new(tmp.path()),
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

    run_drain(
        &db,
        &cloud,
        &enc(),
        &LibraryDir::new(tmp.path()),
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

    run_drain(
        &db,
        &cloud,
        &enc(),
        &LibraryDir::new(tmp.path()),
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

    run_drain(
        &db,
        &cloud,
        &enc(),
        &LibraryDir::new(tmp.path()),
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

/// The whole multi-device flow for Remote (cloud) content. Device A creates a
/// library, invites device B (wrapping the library master key to B's identity),
/// mints a per-release item key, and uploads a release's audio through the real
/// upload drain scoped to that item. `drain_uploads` resolves the `Item` scope to
/// the minted key and encrypts under it. Device B joins (unwraps the master key
/// with its own keypair), then fetches the blob and decrypts it with the item key
/// — the key that, in the app, rides the synced `item_keys` table.
///
/// The load-bearing assertion is that the master key — which every member holds —
/// does NOT decrypt the content: the per-item key is what scopes a release so it
/// can be read (or handed to a share recipient) without exposing the whole
/// library. This test fails if `drain_uploads` encrypts content with the master
/// key instead of resolving the entry's `Item` scope to the item key.
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
    let owner_pk = pubkey_hex(&owner);
    let founder = chain.entries()[0].clone();
    membership
        .put_membership_entry(&owner_pk, 1, serde_json::to_vec(&founder).unwrap())
        .await
        .unwrap();
    publish_membership_chain_head(&membership, &chain, &owner).await;

    // Device A invites device B: seals the master key to B's pubkey, signs the
    // binding, and records a signed membership entry.
    create_invitation(
        &membership,
        &membership,
        &mut chain,
        &owner,
        &pubkey_hex(&joiner),
        None,
        MemberRole::Member,
        &master_key,
        "lib-outbox",
        "0000000002000-0000-dev1",
    )
    .await
    .expect("invite device B");

    // --- Device A mints a per-release item key (distinct from the master key)
    // and uploads the release's audio through the real upload drain scoped to
    // that item. `drain_uploads` resolves the `Item` scope to the minted key. ---
    let plaintext = b"AUDIO-FILE-BYTES-for-one-release";
    let tmp = tempfile::tempdir().unwrap();
    let source = write_temp_file(tmp.path(), "track.flac", plaintext);

    let cloud_key = "storage/ab/cd/file-1";
    let db = open_outbox_db();
    let k_release = db
        .mint_item_key("release-1")
        .await
        .expect("mint the per-release item key");
    assert_ne!(
        k_release, master_key,
        "a minted item key is independent of the master"
    );
    db.enqueue_upload(
        "file-1",
        cloud_key,
        Some(source.as_str()),
        crate::blob::BlobScope::Item("release-1".to_string()),
        false,
        T0,
    )
    .await
    .expect("enqueue the release blob scoped to its item");
    let master_enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        master_key,
    )));

    let n = run_drain(
        &db,
        &cloud,
        &master_enc,
        &LibraryDir::new(tmp.path()),
        &fixed_clock(T0),
        None,
    )
    .await
    .expect("upload")
    .uploaded;
    assert_eq!(n, 1, "the release blob uploads");

    // At rest the blob is per-release ciphertext: not plaintext, and NOT
    // decryptable with the master key every member holds.
    let at_rest = cloud.get(cloud_key).expect("blob present in cloud");
    let aad_context = crate::sync::cloud_storage::cloud_aad_context("test-lib", cloud_key);
    assert_ne!(at_rest, plaintext, "content is encrypted at rest");
    assert!(
        EncryptionService::from_key(master_key)
            .decrypt(&at_rest, &aad_context)
            .is_err(),
        "the master key must NOT decrypt per-release content"
    );
    assert_eq!(
        EncryptionService::from_key(k_release)
            .decrypt(&at_rest, &aad_context)
            .unwrap(),
        plaintext,
        "the per-release key decrypts the content"
    );

    // --- Device B joins: unwraps the library master key with its own identity,
    // authenticating it against the owner that signed it. ---
    let owner_pk = pubkey_hex(&owner);
    let joined_master = unwrap_library_keyring_for_owners_with_activation(
        &membership as &dyn CloudHome,
        &joiner,
        "lib-outbox",
        std::iter::once(owner_pk.as_str()),
        None,
    )
    .await
    .expect("device B unwraps the library key by joining")
    .key_bytes();
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
            .decrypt(&fetched, &aad_context)
            .is_err(),
        "membership alone does not unlock a release's content"
    );
    let recovered = EncryptionService::from_key(k_release)
        .decrypt(&fetched, &aad_context)
        .expect("device B decrypts the content with the per-release key");
    assert_eq!(recovered, plaintext, "device B recovers the original audio");
}

/// `enqueue_upload_on` composes with a host transaction: a rollback takes the
/// queued upload with it, and a commit lands it — so a host can make "row +
/// its upload intent" a single atomic fact.
#[tokio::test]
async fn enqueue_upload_on_is_transactional_with_host_writes() {
    let db = open_outbox_db();

    // Rolled-back host transaction: the enqueue must vanish with it.
    db.call(|conn| {
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        Database::enqueue_upload_on(
            &tx,
            "f-rollback",
            "k-rollback",
            None,
            crate::blob::BlobScope::Master,
            false,
            T0,
        )?;
        tx.rollback().map_err(DbError::from)
    })
    .await
    .expect("rolled-back transaction");

    // Committed host transaction: the enqueue lands.
    db.call(|conn| {
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        Database::enqueue_upload_on(
            &tx,
            "f-commit",
            "k-commit",
            Some("/tmp/source.flac"),
            crate::blob::BlobScope::Item("rel-1".to_string()),
            false,
            T0,
        )?;
        tx.commit().map_err(DbError::from)
    })
    .await
    .expect("committed transaction");

    let pending = db.get_pending_cloud_uploads().await.expect("pending");
    let keys: Vec<&str> = pending.iter().map(|e| e.cloud_key.as_str()).collect();
    assert_eq!(
        keys,
        vec!["k-commit"],
        "only the committed enqueue persists"
    );
}

/// A `retain_pinned` upload populates the PROTECTED cache folder from the
/// plaintext: after the drain, `storage/pinned/<id>` holds the plaintext bytes
/// (not the sealed ciphertext the cloud holds), and the evictable
/// `storage/cache/<id>` is untouched — the blob is kept local and budget-exempt
/// with no later cloud round-trip.
#[tokio::test]
async fn pinned_upload_populates_the_protected_cache_folder() {
    let tmp = tempfile::tempdir().unwrap();
    let plaintext = b"PINNED-AUDIO-BYTES";
    let source = write_temp_file(tmp.path(), "track.flac", plaintext);
    let ld = LibraryDir::new(tmp.path());

    let db = open_outbox_db();
    let file_id = "pinaaaa1";
    let namespace = "release_files";
    // The cache namespace is derived from the cloud key's first component, so it must
    // be the namespace the assertions below check.
    let cloud_key = "release_files/pi/na/pinaaaa1";
    db.enqueue_upload(
        file_id,
        cloud_key,
        Some(source.as_str()),
        crate::blob::BlobScope::Master,
        true, // retain_pinned
        T0,
    )
    .await
    .expect("enqueue a pinned upload");

    let cloud = InMemoryCloudHome::new();
    let n = run_drain(&db, &cloud, &enc(), &ld, &fixed_clock(T0), None)
        .await
        .expect("drain")
        .uploaded;
    assert_eq!(n, 1, "the pinned blob uploads");

    // The cloud holds the sealed (encrypted) bytes, not the plaintext.
    let at_rest = cloud.get(cloud_key).expect("blob present in cloud");
    assert_ne!(at_rest, plaintext, "the cloud copy is sealed ciphertext");

    // The protected folder holds the PLAINTEXT (what the cache serves), written
    // straight from the drain's read — no cloud round-trip.
    let pinned_path = ld.pinned_blob_path(namespace, file_id).unwrap();
    assert!(
        pinned_path.exists(),
        "a pinned upload writes storage/pinned/<namespace>/<id>",
    );
    assert_eq!(
        std::fs::read(&pinned_path).unwrap(),
        plaintext,
        "the pinned file is the plaintext, not the sealed cloud bytes",
    );

    // The evictable cache is untouched: a pin populates pinned/, never cache/.
    assert!(
        !ld.cache_blob_path(namespace, file_id).unwrap().exists(),
        "a pinned upload does not populate the evictable storage/cache/<namespace>/<id>",
    );
}

/// An unpinned upload populates NOTHING on write: after the drain the blob is in
/// the cloud but neither cache folder holds it — the evictable `storage/cache/<id>`
/// fills only on a later read-miss, never on the upload itself.
#[tokio::test]
async fn unpinned_upload_populates_nothing_on_write() {
    let tmp = tempfile::tempdir().unwrap();
    let plaintext = b"UNPINNED-AUDIO-BYTES";
    let source = write_temp_file(tmp.path(), "track.flac", plaintext);
    let ld = LibraryDir::new(tmp.path());

    let db = open_outbox_db();
    let file_id = "unpaaaa1";
    let namespace = "release_files";
    // The cache namespace is derived from the cloud key's first component.
    let cloud_key = "release_files/un/pa/unpaaaa1";
    db.enqueue_upload(
        file_id,
        cloud_key,
        Some(source.as_str()),
        crate::blob::BlobScope::Master,
        false, // retain_pinned
        T0,
    )
    .await
    .expect("enqueue an unpinned upload");

    let cloud = InMemoryCloudHome::new();
    let n = run_drain(&db, &cloud, &enc(), &ld, &fixed_clock(T0), None)
        .await
        .expect("drain")
        .uploaded;
    assert_eq!(n, 1, "the unpinned blob uploads");
    assert!(cloud.get(cloud_key).is_some(), "the blob is in the cloud");

    // Neither folder holds it: an unpinned upload writes no local cache copy.
    assert!(
        !ld.pinned_blob_path(namespace, file_id).unwrap().exists(),
        "an unpinned upload does not populate storage/pinned/<namespace>/<id>",
    );
    assert!(
        !ld.cache_blob_path(namespace, file_id).unwrap().exists(),
        "an unpinned upload does not populate storage/cache/<namespace>/<id>",
    );
}

/// A pin populate that fails does NOT fail the upload: the upload already
/// succeeded and the bytes are in the cloud, so a populate failure is logged and
/// swallowed (a later read re-fetches into the cache). Here the protected folder is
/// blocked by planting a FILE where the blob's shard directory must go, so the
/// atomic write into `storage/pinned/<id>` can't create its parent — yet the drain
/// still reports the upload done and clears the queue entry.
#[tokio::test]
async fn a_failed_pin_populate_does_not_fail_the_upload() {
    let tmp = tempfile::tempdir().unwrap();
    let plaintext = b"PIN-FAILS-BUT-UPLOAD-OK";
    let source = write_temp_file(tmp.path(), "track.flac", plaintext);
    let ld = LibraryDir::new(tmp.path());

    let db = open_outbox_db();
    let file_id = "pinfail1";
    let namespace = "release_files";
    // The cache namespace is derived from the cloud key's first component.
    let cloud_key = "release_files/pi/nf/pinfail1";

    // Block the populate: the pinned blob path is
    // storage/pinned/<namespace>/{ab}/{cd}/<id>; plant a regular FILE at the {ab}
    // level so creating the {ab}/{cd} shard directory fails, and with it the atomic
    // write into pinned/.
    let pinned_path = ld.pinned_blob_path(namespace, file_id).unwrap();
    let ab_dir = pinned_path.parent().unwrap().parent().unwrap(); // .../pinned/<namespace>/{ab}
    std::fs::create_dir_all(ab_dir.parent().unwrap()).unwrap(); // .../pinned/<namespace>
    std::fs::write(ab_dir, b"blocker").unwrap(); // {ab} is now a file, not a dir

    db.enqueue_upload(
        file_id,
        cloud_key,
        Some(source.as_str()),
        crate::blob::BlobScope::Master,
        true, // retain_pinned — but the populate will fail
        T0,
    )
    .await
    .expect("enqueue a pinned upload whose populate will fail");

    let cloud = InMemoryCloudHome::new();
    let n = run_drain(&db, &cloud, &enc(), &ld, &fixed_clock(T0), None)
        .await
        .expect("the drain succeeds despite the populate failure")
        .uploaded;

    // The upload counted, the blob reached the cloud, and the queue entry was
    // cleared — the failed populate rolled none of that back.
    assert_eq!(n, 1, "the upload succeeds even though pinning failed");
    assert!(cloud.get(cloud_key).is_some(), "the blob reached the cloud");
    assert!(
        get_upload(&db, 1).await.is_none(),
        "the completed upload's queue entry was removed",
    );
    // The pin did not land (its parent couldn't be created).
    assert!(
        !pinned_path.exists(),
        "the blocked populate left no storage/pinned/<id> file",
    );
}
