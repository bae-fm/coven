//! Tests for the blob-delete half: signed tombstones, the graced GC that performs
//! the actual deletion, upload-cancels-delete at both layers, the durable
//! tombstone-cancel retry, and the shared `cloud_outbox` row shape.
//!
//! The grace and forgery behaviors are the load-bearing ones — this code deletes
//! user data and trusts a signature, so a stale or forged tombstone is real data
//! loss.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::RwLock;

use async_trait::async_trait;

use crate::blob::delete::{
    drain_tombstone_cancels, drain_tombstones, gc_tombstones, BlobTombstoneJson,
    BLOB_TOMBSTONE_GRACE,
};
use crate::blob::BlobScope;
use crate::clock::FixedClock;
use crate::database::{Database, DbError};
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::{no_progress, CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::membership::{MemberRole, MembershipAction};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{founder_entry, make_entry, pubkey_hex, MockSyncStorage};
use rusqlite::OptionalExtension;

const T0: &str = "2024-06-01T00:00:00Z";

/// A wall-clock instant for the tests, fixed so the grace math is deterministic.
fn at(rfc3339: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .expect("valid rfc3339")
        .with_timezone(&chrono::Utc)
}

/// A `Database` over an in-memory connection with just the bookkeeping tables.
/// The `cloud_outbox` table both operations share is created by `Database::open`.
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

/// Read back `(attempt_count, last_error, last_attempt_at)` for a delete entry, or
/// `None` if it was removed.
async fn get_delete(db: &Database, id: i64) -> Option<(i64, Option<String>, Option<String>)> {
    db.call(move |conn| {
        conn.query_row(
            "SELECT attempt_count, last_error, last_attempt_at FROM cloud_outbox \
             WHERE id = ?1 AND operation = 'delete'",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(DbError::from)
    })
    .await
    .expect("query delete outbox entry")
}

/// A plaintext cipher (tests don't encrypt) behind the lock the drain/GC take.
fn plaintext_cipher() -> RwLock<CloudCipher> {
    RwLock::new(CloudCipher::Plaintext)
}

/// A `MockSyncStorage` (which is both a `SyncStorage` and a `CloudHome`) holding a
/// real two-member chain: `founder` (Owner) added `member` (Member). Returns the
/// storage plus both keypairs so a test can sign a tombstone as a member, a
/// non-member, or with the founder.
async fn storage_with_chain() -> (MockSyncStorage, UserKeypair, UserKeypair) {
    let founder = UserKeypair::generate();
    let member = UserKeypair::generate();
    let storage = MockSyncStorage::new();

    // The founder entry (seq 0) and the member's Add (seq 1). `put_membership_entry`
    // keys on the entry's author; both are authored by the founder, so they live
    // under `membership/{founder}/{seq}` where `list_membership_entries` finds them.
    let f_entry = founder_entry(&founder, "0000000001000-0000-dev1");
    let m_entry = make_entry(
        &founder,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "0000000002000-0000-dev1",
    );
    let founder_pk = pubkey_hex(&founder);
    storage
        .put_membership_entry(&founder_pk, 0, serde_json::to_vec(&f_entry).unwrap())
        .await
        .expect("put founder entry");
    storage
        .put_membership_entry(&founder_pk, 1, serde_json::to_vec(&m_entry).unwrap())
        .await
        .expect("put member entry");

    (storage, founder, member)
}

/// Write a tombstone object straight into the cloud at its key, bypassing the
/// signing drain — so a test can plant a forged or library-mismatched tombstone
/// the GC must reject. Mirrors how `drain_tombstones` lays the object out
/// (plaintext cipher → verbatim bytes, empty suffix).
async fn plant_tombstone(cloud: &dyn CloudHome, tombstone: &BlobTombstoneJson) {
    let key = format!("blob_tombstones/{}", tombstone.cloud_key);
    let bytes = serde_json::to_vec(tombstone).expect("serialize tombstone");
    cloud
        .write(
            &key,
            crate::storage::cloud::BlobBody::from_bytes(bytes),
            &no_progress(),
        )
        .await
        .expect("plant");
}

/// A `CloudHome` that delegates to an inner `MockSyncStorage`, but the first time
/// the GC re-checks the named tombstone key with `exists`, it simulates a
/// concurrent `cancel_tombstone` landing in the TOCTOU window: it deletes the
/// tombstone from the inner store and reports `false`. This drives the GC's
/// re-check-before-delete deterministically — the blob must then be left alone,
/// because the deletion was canceled mid-pass by a re-upload.
struct CancelTombstoneOnExists<'a> {
    inner: &'a MockSyncStorage,
    tombstone_key: String,
    fired: AtomicBool,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl CloudHome for CancelTombstoneOnExists<'_> {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.inner.put_object(key, data).await
    }

    async fn open_multipart<'b>(
        &'b self,
        key: &str,
        total_len: u64,
    ) -> Result<crate::storage::cloud::BoxPartSink<'b>, CloudHomeError> {
        self.inner.open_multipart(key, total_len).await
    }

    fn multipart_threshold(&self) -> u64 {
        self.inner.multipart_threshold()
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.inner.read(key).await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        self.inner.read_range(key, start, end).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        self.inner.delete(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        // The GC's pre-delete re-check: on the first hit for the tombstone key,
        // cancel it (delete the object) and report it gone, as a racing re-upload
        // would. Any later/other check delegates normally.
        if key == self.tombstone_key && !self.fired.swap(true, Ordering::SeqCst) {
            self.inner.delete(key).await?;
            return Ok(false);
        }
        self.inner.exists(key).await
    }

    async fn grant_access(
        &self,
        grant: crate::storage::cloud::CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        self.inner.grant_access(grant).await
    }

    async fn revoke_access(
        &self,
        revoke: crate::storage::cloud::CloudAccessRevoke,
    ) -> Result<(), CloudHomeError> {
        self.inner.revoke_access(revoke).await
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FailingCloudOp {
    Delete,
    Exists,
    PutObject,
}

/// A `CloudHome` wrapper that fails one operation on one named key for the first
/// `fail_times` matching calls, counts matching calls, then delegates normally.
struct FailCloudOpOnKey<'a, H: CloudHome + ?Sized> {
    inner: &'a H,
    op: FailingCloudOp,
    key: String,
    fail_times: usize,
    calls: AtomicUsize,
}

impl<'a, H: CloudHome + ?Sized> FailCloudOpOnKey<'a, H> {
    fn new(inner: &'a H, op: FailingCloudOp, key: &str, fail_times: usize) -> Self {
        Self {
            inner,
            op,
            key: key.to_string(),
            fail_times,
            calls: AtomicUsize::new(0),
        }
    }

    fn matching_calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn should_fail(&self, op: FailingCloudOp, key: &str) -> bool {
        if self.op == op && key == self.key {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            return call < self.fail_times;
        }
        false
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<H: CloudHome + ?Sized> CloudHome for FailCloudOpOnKey<'_, H> {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        if self.should_fail(FailingCloudOp::PutObject, key) {
            return Err(CloudHomeError::Storage(format!(
                "injected put_object failure for {key}"
            )));
        }
        self.inner.put_object(key, data).await
    }

    async fn open_multipart<'b>(
        &'b self,
        key: &str,
        total_len: u64,
    ) -> Result<crate::storage::cloud::BoxPartSink<'b>, CloudHomeError> {
        self.inner.open_multipart(key, total_len).await
    }

    fn multipart_threshold(&self) -> u64 {
        self.inner.multipart_threshold()
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.inner.read(key).await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        self.inner.read_range(key, start, end).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        if self.should_fail(FailingCloudOp::Delete, key) {
            return Err(CloudHomeError::Storage(format!(
                "injected delete failure for {key}"
            )));
        }
        self.inner.delete(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        if self.should_fail(FailingCloudOp::Exists, key) {
            return Err(CloudHomeError::Storage(format!(
                "injected exists failure for {key}"
            )));
        }
        self.inner.exists(key).await
    }

    async fn grant_access(
        &self,
        grant: crate::storage::cloud::CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        self.inner.grant_access(grant).await
    }

    async fn revoke_access(
        &self,
        revoke: crate::storage::cloud::CloudAccessRevoke,
    ) -> Result<(), CloudHomeError> {
        self.inner.revoke_access(revoke).await
    }
}

// ----- the outbox Delete row becomes a tombstone and is removed -----

/// A queued blob delete drains to a signed cloud tombstone (the deletion's durable
/// record) and the outbox row is cleared — and crucially the blob is NOT deleted
/// yet (it is kept for the convergence grace).
#[tokio::test]
async fn enqueued_delete_becomes_a_tombstone_and_clears_the_outbox() {
    let db = open_outbox_db();
    let cloud = InMemoryCloudHome::new();
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();
    // The blob exists in the cloud; deleting must not remove it yet.
    cloud
        .write(
            "blob-key",
            crate::storage::cloud::BlobBody::from_bytes(b"contents".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

    db.enqueue_delete("blob-key", T0).await.expect("enqueue");

    let clock = FixedClock(at("2024-06-10T00:00:00Z"));
    let n = drain_tombstones(&db, &cloud, &cipher, "lib", &kp, &clock)
        .await
        .expect("drain");
    assert_eq!(n, 1, "one tombstone written");

    // The blob is still present — the drain records the deletion, it doesn't
    // perform it.
    assert!(
        cloud.get("blob-key").is_some(),
        "the blob is kept for the grace, not deleted on drain",
    );
    assert!(cloud.deletes_seen().is_empty(), "the drain deletes nothing");

    // A signed tombstone landed at the derived key and verifies under the library.
    let stored = cloud
        .get("blob_tombstones/blob-key")
        .expect("tombstone object present");
    let tombstone: BlobTombstoneJson = serde_json::from_slice(&stored).expect("parse tombstone");
    assert_eq!(tombstone.cloud_key, "blob-key");
    assert_eq!(tombstone.deleted_at, "2024-06-10T00:00:00+00:00");
    assert_eq!(tombstone.author_pubkey, hex::encode(kp.public_key()));
    assert!(tombstone.verify("lib"), "the tombstone is validly signed");

    // The outbox row is gone.
    assert!(
        db.get_pending_cloud_deletes()
            .await
            .expect("pending")
            .is_empty(),
        "the drained delete row is removed",
    );
}

/// An upload row reads back as an `Upload` carrying its scope; a delete row reads
/// back as a `Delete`. The operation-specific fields live in the variant, so a
/// delete has no scope to be `None` — the shared `cloud_outbox` row-shape
/// contract.
#[tokio::test]
async fn upload_carries_scope_delete_carries_no_extra_fields() {
    use crate::db::OutboxOperation;

    let db = open_outbox_db();
    db.enqueue_upload("f1", "k-up", None, BlobScope::Master, false, T0)
        .await
        .expect("enqueue upload");
    db.enqueue_delete("k-del", T0)
        .await
        .expect("enqueue delete");

    let uploads = db.get_pending_cloud_uploads().await.expect("uploads");
    assert_eq!(uploads.len(), 1);
    assert_eq!(
        uploads[0].operation,
        OutboxOperation::Upload {
            file_id: "f1".to_string(),
            source_path: None,
            scope: BlobScope::Master,
            retain_pinned: false,
        },
        "an upload entry carries its scope in the variant"
    );

    let deletes = db.get_pending_cloud_deletes().await.expect("deletes");
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].operation, OutboxOperation::Delete);
}

/// A failed tombstone existence check records durable retry state, then the
/// delete drain skips the row inside the backoff window and retries once the
/// window has elapsed.
#[tokio::test]
async fn delete_existence_failure_backs_off_then_retries() {
    let db = open_outbox_db();
    let inner = InMemoryCloudHome::new();
    let cloud = FailCloudOpOnKey::new(
        &inner,
        FailingCloudOp::Exists,
        "blob_tombstones/blob-key",
        1,
    );
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();

    db.enqueue_delete("blob-key", T0).await.expect("enqueue");
    let first = FixedClock(at("2024-06-01T00:00:00Z"));
    let n = drain_tombstones(&db, &cloud, &cipher, "lib", &kp, &first)
        .await
        .expect("first drain");
    assert_eq!(n, 0, "the failed existence check writes no tombstone");
    assert_eq!(cloud.matching_calls(), 1);

    let first_row = get_delete(&db, 1).await.expect("delete row remains");
    assert_eq!(first_row.0, 1, "the failed attempt is counted");
    assert!(
        first_row
            .1
            .as_deref()
            .unwrap()
            .contains("tombstone existence check failed"),
        "the failure reason is recorded",
    );
    let recorded = chrono::DateTime::parse_from_rfc3339(first_row.2.as_deref().unwrap()).unwrap();
    assert_eq!(recorded.with_timezone(&chrono::Utc), first.0);

    let inside = FixedClock(at("2024-06-01T00:00:10Z"));
    let n = drain_tombstones(&db, &cloud, &cipher, "lib", &kp, &inside)
        .await
        .expect("inside backoff drain");
    assert_eq!(n, 0, "inside the backoff window no tombstone is written");
    assert_eq!(
        cloud.matching_calls(),
        1,
        "inside the backoff window no cloud existence check runs",
    );
    assert_eq!(
        get_delete(&db, 1).await.expect("delete row remains"),
        first_row,
        "the skipped row is unchanged",
    );

    let after = FixedClock(at("2024-06-01T00:00:31Z"));
    let n = drain_tombstones(&db, &cloud, &cipher, "lib", &kp, &after)
        .await
        .expect("after backoff drain");
    assert_eq!(n, 1, "the elapsed backoff allows the tombstone write");
    assert_eq!(cloud.matching_calls(), 2);
    assert!(
        inner.get("blob_tombstones/blob-key").is_some(),
        "the retried drain writes the tombstone",
    );
    assert!(
        get_delete(&db, 1).await.is_none(),
        "the successful retry clears the delete row",
    );
}

/// A failed tombstone write records the same durable retry state as an existence
/// check failure, and the backoff gate suppresses the write attempt until the
/// retry window has elapsed.
#[tokio::test]
async fn delete_write_failure_backs_off_then_retries() {
    let db = open_outbox_db();
    let inner = InMemoryCloudHome::new();
    let cloud = FailCloudOpOnKey::new(
        &inner,
        FailingCloudOp::PutObject,
        "blob_tombstones/blob-key",
        1,
    );
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();

    db.enqueue_delete("blob-key", T0).await.expect("enqueue");
    let first = FixedClock(at("2024-06-01T00:00:00Z"));
    let n = drain_tombstones(&db, &cloud, &cipher, "lib", &kp, &first)
        .await
        .expect("first drain");
    assert_eq!(n, 0, "the failed write creates no tombstone");
    assert_eq!(cloud.matching_calls(), 1);

    let first_row = get_delete(&db, 1).await.expect("delete row remains");
    assert_eq!(first_row.0, 1, "the failed write is counted");
    assert!(
        first_row
            .1
            .as_deref()
            .unwrap()
            .contains("tombstone write failed"),
        "the write failure is recorded",
    );

    let inside = FixedClock(at("2024-06-01T00:00:10Z"));
    let n = drain_tombstones(&db, &cloud, &cipher, "lib", &kp, &inside)
        .await
        .expect("inside backoff drain");
    assert_eq!(n, 0, "inside the backoff window no tombstone is written");
    assert_eq!(
        cloud.matching_calls(),
        1,
        "inside the backoff window no cloud write runs",
    );

    let after = FixedClock(at("2024-06-01T00:00:31Z"));
    let n = drain_tombstones(&db, &cloud, &cipher, "lib", &kp, &after)
        .await
        .expect("after backoff drain");
    assert_eq!(n, 1, "the elapsed backoff allows the tombstone write");
    assert_eq!(cloud.matching_calls(), 2);
    assert!(
        inner.get("blob_tombstones/blob-key").is_some(),
        "the retried drain writes the tombstone",
    );
}

/// Corrupt local retry metadata must not strand a delete row. The drain logs the
/// timestamp parse failure and retries the row.
#[tokio::test]
async fn corrupt_delete_backoff_timestamp_does_not_strand_the_row() {
    let db = open_outbox_db();
    let inner = InMemoryCloudHome::new();
    let cloud = FailCloudOpOnKey::new(
        &inner,
        FailingCloudOp::Exists,
        "blob_tombstones/blob-key",
        1,
    );
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();

    db.enqueue_delete("blob-key", T0).await.expect("enqueue");
    let first = FixedClock(at("2024-06-01T00:00:00Z"));
    drain_tombstones(&db, &cloud, &cipher, "lib", &kp, &first)
        .await
        .expect("first drain");

    db.call(|conn| {
        conn.execute(
            "UPDATE cloud_outbox SET last_attempt_at = 'not-a-timestamp' \
             WHERE id = 1 AND operation = 'delete'",
            [],
        )
        .map(|_| ())
        .map_err(DbError::from)
    })
    .await
    .expect("corrupt last_attempt_at");

    let inside = FixedClock(at("2024-06-01T00:00:10Z"));
    let n = drain_tombstones(&db, &cloud, &cipher, "lib", &kp, &inside)
        .await
        .expect("corrupt timestamp drain");
    assert_eq!(n, 1, "the corrupt timestamp does not suppress the retry");
    assert_eq!(cloud.matching_calls(), 2);
    assert!(
        get_delete(&db, 1).await.is_none(),
        "the retried delete row clears after the tombstone is present",
    );
}

// ----- the grace: kept before, reclaimed after -----

/// A blob with a valid, authorized tombstone survives a GC pass run inside the
/// convergence grace, and is deleted by one run after it: the blob is kept long
/// enough for a lagging peer to converge, then reclaimed once the grace has passed.
#[tokio::test]
async fn tombstone_is_reclaimed_only_after_the_grace() {
    let (storage, founder, member) = storage_with_chain().await;
    let cipher = plaintext_cipher();
    let owner = pubkey_hex(&founder);

    // The blob exists; a member tombstoned it at a known instant.
    storage
        .write(
            "blob-key",
            crate::storage::cloud::BlobBody::from_bytes(b"contents".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = BlobTombstoneJson::signed(
        "test-lib",
        "blob-key".to_string(),
        deleted_at.to_string(),
        &member,
    );
    plant_tombstone(&storage, &tombstone).await;

    // A GC one day later — well inside the 7-day grace — keeps the blob.
    let inside = FixedClock(at("2024-06-02T00:00:00Z"));
    let n = gc_tombstones(
        &storage,
        &storage,
        &cipher,
        "test-lib",
        Some(&owner),
        &inside,
    )
    .await
    .expect("gc inside grace");
    assert_eq!(n, 0, "nothing reclaimed inside the grace");
    assert!(
        storage.read("blob-key").await.is_ok(),
        "the blob survives a GC inside the grace",
    );
    assert!(
        storage.read("blob_tombstones/blob-key").await.is_ok(),
        "the tombstone is kept inside the grace",
    );

    // A GC just past the grace reclaims the blob and the tombstone.
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones(&storage, &storage, &cipher, "test-lib", Some(&owner), &past)
        .await
        .expect("gc past grace");
    assert_eq!(n, 1, "one blob reclaimed past the grace");
    assert!(
        storage.read("blob-key").await.is_err(),
        "the blob is deleted past the grace",
    );
    assert!(
        storage.read("blob_tombstones/blob-key").await.is_err(),
        "the tombstone is deleted after reclaiming its blob",
    );
}

/// An instant one minute past `deleted_at + BLOB_TOMBSTONE_GRACE`, as RFC 3339.
fn past_grace_instant(deleted_at: &str) -> String {
    (at(deleted_at) + BLOB_TOMBSTONE_GRACE + chrono::Duration::minutes(1)).to_rfc3339()
}

/// A tombstone canceled (by a concurrent re-upload's `cancel_tombstone`) *between*
/// the GC verifying/aging it and deleting the blob must NOT take the blob: the
/// re-uploaded blob is live data. The GC re-checks the tombstone still exists right
/// before the delete and skips when it's gone.
#[tokio::test]
async fn tombstone_canceled_mid_gc_leaves_the_reuploaded_blob() {
    let (storage, founder, member) = storage_with_chain().await;
    let owner = pubkey_hex(&founder);
    let cipher = plaintext_cipher();

    // A blob (here standing in for the re-uploaded one) and an authorized, past-grace
    // tombstone for it. Authorization passes, so the GC reaches the pre-delete
    // re-check — which is where the simulated cancel lands.
    storage
        .write(
            "blob-key",
            crate::storage::cloud::BlobBody::from_bytes(b"fresh re-uploaded contents".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = BlobTombstoneJson::signed(
        "test-lib",
        "blob-key".to_string(),
        deleted_at.to_string(),
        &member,
    );
    plant_tombstone(&storage, &tombstone).await;

    // The cloud_home cancels the tombstone exactly in the GC's re-check window.
    let racing_home = CancelTombstoneOnExists {
        inner: &storage,
        tombstone_key: "blob_tombstones/blob-key".to_string(),
        fired: AtomicBool::new(false),
    };

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones(
        &storage,
        &racing_home,
        &cipher,
        "test-lib",
        Some(&owner),
        &past,
    )
    .await
    .expect("gc");
    assert_eq!(n, 0, "a tombstone canceled mid-GC reclaims nothing");
    assert!(
        storage.read("blob-key").await.is_ok(),
        "the re-uploaded blob survives a tombstone canceled in the TOCTOU window",
    );
}

// ----- forgery: a bad signature or a non-member author is ignored -----

/// A tombstone whose author is NOT a current member is ignored by the GC: the blob
/// survives. This is the forgery defense — a bucket writer who isn't a member
/// can't delete a blob by planting a (validly self-signed) tombstone.
#[tokio::test]
async fn tombstone_by_a_non_member_is_ignored() {
    let (storage, founder, _member) = storage_with_chain().await;
    let cipher = plaintext_cipher();
    let owner = pubkey_hex(&founder);
    let outsider = UserKeypair::generate(); // not in the chain

    storage
        .write(
            "blob-key",
            crate::storage::cloud::BlobBody::from_bytes(b"contents".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
    // Validly signed by the outsider (the signature itself verifies), but the
    // outsider is not a member — long past the grace, so only authorization stands
    // between this and a deletion.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = BlobTombstoneJson::signed(
        "test-lib",
        "blob-key".to_string(),
        deleted_at.to_string(),
        &outsider,
    );
    assert!(
        tombstone.verify("test-lib"),
        "the forged tombstone is self-consistently signed (only authorization rejects it)",
    );
    plant_tombstone(&storage, &tombstone).await;

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones(&storage, &storage, &cipher, "test-lib", Some(&owner), &past)
        .await
        .expect("gc");
    assert_eq!(n, 0, "a non-member tombstone reclaims nothing");
    assert!(
        storage.read("blob-key").await.is_ok(),
        "the blob survives a non-member's forged deletion",
    );
}

/// The owner-anchoring defense: a tombstone authored by the *forged founder of a
/// wiped-and-refounded chain* is refused, so the victim blob survives — even though
/// the attacker controls `membership/*` and self-signed a founder entry that passes
/// `MembershipChain::validate`.
///
/// An attacker who can write the bucket wipes `membership/*`, writes a fresh
/// self-signed Owner founder under their own key (internally valid — `validate`
/// only proves consistency, not that the founder is the library's established
/// owner), then plants a tombstone signed by that forged founder with a backdated
/// `deleted_at`. Because the device pins the *real* owner, the refounded chain
/// fails `is_founded_by(pinned_owner)`, the author is never authorized, and the
/// backdated time never gets a chance to matter.
#[tokio::test]
async fn tombstone_by_a_forged_founder_of_a_refounded_chain_is_refused() {
    // The library's real established owner (the pinned founder). Its chain isn't
    // even needed for the test — the device only needs to *pin* this pubkey; the
    // attacker has replaced the on-bucket chain entirely.
    let real_owner = UserKeypair::generate();
    let pinned_owner = pubkey_hex(&real_owner);

    // The attacker controls the bucket. They wrote a forged self-signed Owner
    // founder under their OWN key — a valid chain in isolation, but founded by the
    // wrong key.
    let attacker = UserKeypair::generate();
    let storage = MockSyncStorage::new();
    let forged_founder = founder_entry(&attacker, "0000000001000-0000-evil");
    storage
        .put_membership_entry(
            &pubkey_hex(&attacker),
            1,
            serde_json::to_vec(&forged_founder).unwrap(),
        )
        .await
        .expect("plant forged founder");

    // The victim blob, and a tombstone the attacker signs as their forged founder,
    // backdated well past the grace so only authorization stands between it and the
    // deletion.
    storage
        .write(
            "blob-key",
            crate::storage::cloud::BlobBody::from_bytes(b"contents".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = BlobTombstoneJson::signed(
        "test-lib",
        "blob-key".to_string(),
        deleted_at.to_string(),
        &attacker,
    );
    assert!(
        tombstone.verify("test-lib"),
        "the forged tombstone is self-consistently signed (only owner-anchored \
         authorization rejects it)",
    );
    plant_tombstone(&storage, &tombstone).await;

    // GC anchored to the REAL pinned owner: the refounded chain's founder is the
    // attacker, not the pin, so authorization fails and the blob survives.
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let cipher = plaintext_cipher();
    let n = gc_tombstones(
        &storage,
        &storage,
        &cipher,
        "test-lib",
        Some(&pinned_owner),
        &past,
    )
    .await
    .expect("gc");
    assert_eq!(
        n, 0,
        "a tombstone by the forged founder of a refounded chain reclaims nothing",
    );
    assert!(
        storage.read("blob-key").await.is_ok(),
        "the victim blob survives a wiped-and-refounded-chain takeover",
    );
}

/// The empty-chain half of the same defense: an *entirely wiped* `membership/*`
/// (no founder at all) under a pinned owner is a takeover, not an open library, so
/// a tombstone authored over it is refused and the blob survives. This mirrors
/// snapshot `authorize_author`: empty + pinned owner = wiped = refuse; empty + no
/// pin = genuinely open = accept on signature.
#[tokio::test]
async fn tombstone_over_a_wiped_chain_with_a_pinned_owner_is_refused() {
    // The library has an established owner (pinned), but `membership/*` is wiped —
    // the storage holds no membership entries at all.
    let real_owner = UserKeypair::generate();
    let pinned_owner = pubkey_hex(&real_owner);
    let storage = MockSyncStorage::new();

    // A blob and a validly self-signed tombstone by some attacker, backdated past
    // the grace.
    let attacker = UserKeypair::generate();
    storage
        .write(
            "blob-key",
            crate::storage::cloud::BlobBody::from_bytes(b"contents".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = BlobTombstoneJson::signed(
        "test-lib",
        "blob-key".to_string(),
        deleted_at.to_string(),
        &attacker,
    );
    plant_tombstone(&storage, &tombstone).await;

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let cipher = plaintext_cipher();
    let n = gc_tombstones(
        &storage,
        &storage,
        &cipher,
        "test-lib",
        Some(&pinned_owner),
        &past,
    )
    .await
    .expect("gc");
    assert_eq!(
        n, 0,
        "an empty (wiped) chain under a pinned owner authorizes no deletion",
    );
    assert!(
        storage.read("blob-key").await.is_ok(),
        "the blob survives a tombstone over a wiped membership chain",
    );
}

/// A tombstone whose signature does not verify is ignored by the GC: the blob
/// survives. Covers a tampered object (here, a `cloud_key` changed after signing,
/// so the signature no longer matches) — the bucket is untrusted, so an
/// unauthenticated tombstone must never delete data.
#[tokio::test]
async fn tombstone_with_a_bad_signature_is_ignored() {
    let (storage, founder, member) = storage_with_chain().await;
    let cipher = plaintext_cipher();
    let owner = pubkey_hex(&founder);

    storage
        .write(
            "blob-key",
            crate::storage::cloud::BlobBody::from_bytes(b"contents".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
    // A member signs a tombstone for some OTHER key, then it's relocated to
    // blob-key's slot (its stored cloud_key changed after signing). The signature
    // no longer matches the slot, so it must be skipped.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let mut tombstone = BlobTombstoneJson::signed(
        "test-lib",
        "some-other-key".to_string(),
        deleted_at.to_string(),
        &member,
    );
    tombstone.cloud_key = "blob-key".to_string(); // tamper: now signature is invalid
    assert!(
        !tombstone.verify("test-lib"),
        "the tampered tombstone does not verify",
    );
    plant_tombstone(&storage, &tombstone).await;

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones(&storage, &storage, &cipher, "test-lib", Some(&owner), &past)
        .await
        .expect("gc");
    assert_eq!(n, 0, "a tombstone with a bad signature reclaims nothing");
    assert!(
        storage.read("blob-key").await.is_ok(),
        "the blob survives a tombstone whose signature doesn't verify",
    );
}

// ----- a tombstone bound to a different library is ignored -----

/// A tombstone validly signed for a DIFFERENT library is ignored when GC'd as this
/// library: the blob survives. A member of two libraries can't replay one
/// library's deletion against the other's bucket — the signature binds the library
/// id, so it fails to verify under any other.
#[tokio::test]
async fn tombstone_bound_to_a_different_library_is_ignored() {
    let (storage, founder, member) = storage_with_chain().await;
    let cipher = plaintext_cipher();
    let owner = pubkey_hex(&founder);

    storage
        .write(
            "blob-key",
            crate::storage::cloud::BlobBody::from_bytes(b"contents".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
    // Signed for "other-lib" by a real member of THIS library; the GC runs as
    // "test-lib", so the signature (taken over other-lib's id) fails here.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = BlobTombstoneJson::signed(
        "other-lib",
        "blob-key".to_string(),
        deleted_at.to_string(),
        &member,
    );
    assert!(
        tombstone.verify("other-lib") && !tombstone.verify("test-lib"),
        "the tombstone verifies only under the library it was signed for",
    );
    plant_tombstone(&storage, &tombstone).await;

    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones(&storage, &storage, &cipher, "test-lib", Some(&owner), &past)
        .await
        .expect("gc");
    assert_eq!(n, 0, "a foreign-library tombstone reclaims nothing");
    assert!(
        storage.read("blob-key").await.is_ok(),
        "the blob survives a tombstone bound to a different library",
    );
}

// ----- upload cancels the deletion, at both layers -----

/// Enqueue layer: queuing an upload for a key drops a pending delete for that key,
/// and queuing a delete drops a pending upload — latest intent wins, so the
/// same-cycle phase split (uploads before push, deletes after pull) can never see
/// both for one key.
#[tokio::test]
async fn enqueue_upload_and_delete_cancel_each_other_for_a_key() {
    // delete then upload → no delete row remains, the upload stands.
    let db = open_outbox_db();
    db.enqueue_delete("k", T0).await.expect("enqueue delete");
    db.enqueue_upload("f", "k", None, BlobScope::Master, false, T0)
        .await
        .expect("enqueue upload");
    assert!(
        db.get_pending_cloud_deletes().await.unwrap().is_empty(),
        "queuing an upload cancels a pending delete for the same key",
    );
    assert_eq!(
        db.get_pending_cloud_uploads().await.unwrap().len(),
        1,
        "the upload stands",
    );

    // upload then delete → no upload row remains, the delete stands.
    let db = open_outbox_db();
    db.enqueue_upload("f", "k", None, BlobScope::Master, false, T0)
        .await
        .expect("enqueue upload");
    db.enqueue_delete("k", T0).await.expect("enqueue delete");
    assert!(
        db.get_pending_cloud_uploads().await.unwrap().is_empty(),
        "queuing a delete cancels a pending upload for the same key",
    );
    assert_eq!(
        db.get_pending_cloud_deletes().await.unwrap().len(),
        1,
        "the delete stands",
    );

    // The cancel is key-scoped: a delete for one key doesn't touch an upload for
    // another.
    let db = open_outbox_db();
    db.enqueue_upload("f", "keep", None, BlobScope::Master, false, T0)
        .await
        .expect("enqueue upload");
    db.enqueue_delete("other", T0)
        .await
        .expect("enqueue delete");
    assert_eq!(
        db.get_pending_cloud_uploads().await.unwrap().len(),
        1,
        "an unrelated key's upload is untouched",
    );
}

/// Prior-cycle-tombstone layer: a tombstone written in an earlier cycle (possibly
/// on another device) is cancelled when the blob is re-uploaded, so a later GC
/// won't reclaim the re-uploaded blob. Drives the real upload drain
/// ([`crate::blob::upload::drain_uploads`]) so it exercises the wiring, not just
/// `cancel_tombstone` in isolation: the drain writes the blob, then cancels the
/// tombstone.
#[tokio::test]
async fn reupload_through_the_drain_cancels_a_prior_cycle_tombstone() {
    use crate::blob::upload::drain_uploads;

    let (storage, founder, member) = storage_with_chain().await;
    let cipher = plaintext_cipher();
    let owner = pubkey_hex(&founder);
    let tmp = tempfile::tempdir().unwrap();

    // A prior cycle tombstoned the blob.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = BlobTombstoneJson::signed(
        "test-lib",
        "blob-key".to_string(),
        deleted_at.to_string(),
        &member,
    );
    plant_tombstone(&storage, &tombstone).await;

    // Now the host re-stages the blob and queues an upload for the same key; the
    // drain writes it to the cloud and cancels the prior tombstone.
    let src = tmp.path().join("blob.bin");
    std::fs::write(&src, b"fresh contents").unwrap();
    let db = open_outbox_db();
    db.enqueue_upload(
        "blob-file",
        "blob-key",
        Some(&src.to_string_lossy()),
        BlobScope::Master,
        false,
        T0,
    )
    .await
    .expect("enqueue upload");

    let clock = FixedClock(at("2024-06-01T01:00:00Z"));
    let n = drain_uploads(
        &db,
        &storage,
        &cipher,
        &LibraryDir::new(tmp.path()),
        &clock,
        &crate::sync::hlc::Hlc::new("test-device".to_string()),
        None,
    )
    .await
    .expect("drain")
    .uploaded;
    assert_eq!(n, 1, "the blob is re-uploaded");
    assert!(
        storage.read("blob-key").await.is_ok(),
        "the re-uploaded blob is in the cloud",
    );
    assert!(
        storage.read("blob_tombstones/blob-key").await.is_err(),
        "the upload drain cancelled the prior-cycle tombstone",
    );

    // A GC long past the grace now finds no tombstone and leaves the live blob.
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let gc = gc_tombstones(&storage, &storage, &cipher, "test-lib", Some(&owner), &past)
        .await
        .expect("gc");
    assert_eq!(
        gc, 0,
        "no tombstone remains to reclaim the re-uploaded blob"
    );
    assert!(
        storage.read("blob-key").await.is_ok(),
        "the re-uploaded blob survives — its deletion was cancelled",
    );
}

// ----- the tombstone write is idempotent: a re-drain holds the grace deadline -----

/// A Delete row drained when a tombstone already exists for its key does not
/// overwrite the tombstone: it keeps the original `deleted_at`, so the 7-day grace
/// is measured from the first drain and never resets. The row is processed twice
/// here — the second time at a clock a day later, standing in for the row surviving
/// a failed removal and re-draining (or the host re-enqueuing the same deletion).
/// If the second drain reset `deleted_at`, a row that keeps re-draining would push
/// the reclaim deadline forward forever and the blob would never age out.
#[tokio::test]
async fn re_draining_a_delete_keeps_the_original_deleted_at() {
    let db = open_outbox_db();
    let cloud = InMemoryCloudHome::new();
    let cipher = plaintext_cipher();
    let kp = UserKeypair::generate();
    cloud
        .write(
            "blob-key",
            crate::storage::cloud::BlobBody::from_bytes(b"contents".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();

    // First drain at T0: writes the tombstone, removes the row.
    db.enqueue_delete("blob-key", T0).await.expect("enqueue");
    let first = FixedClock(at("2024-06-01T00:00:00Z"));
    let n = drain_tombstones(&db, &cloud, &cipher, "lib", &kp, &first)
        .await
        .expect("first drain");
    assert_eq!(n, 1, "the first drain writes the tombstone");
    let original_deleted_at = {
        let stored = cloud.get("blob_tombstones/blob-key").expect("tombstone");
        let t: BlobTombstoneJson = serde_json::from_slice(&stored).unwrap();
        t.deleted_at
    };

    // The same deletion is queued again (its row re-appears: the prior removal
    // failed, or the host re-enqueued it) and drained a day later. The tombstone
    // already exists, so this drain must not rewrite it.
    db.enqueue_delete("blob-key", "2024-06-02T00:00:00Z")
        .await
        .expect("re-enqueue");
    let second = FixedClock(at("2024-06-02T00:00:00Z"));
    let n = drain_tombstones(&db, &cloud, &cipher, "lib", &kp, &second)
        .await
        .expect("second drain");
    assert_eq!(n, 0, "the re-drain writes no new tombstone");

    // Exactly one tombstone, still carrying the first drain's deleted_at.
    let tombstones = cloud.list("blob_tombstones/").await.unwrap();
    assert_eq!(tombstones.len(), 1, "only one tombstone exists for the key");
    let stored = cloud.get("blob_tombstones/blob-key").expect("tombstone");
    let tombstone: BlobTombstoneJson = serde_json::from_slice(&stored).unwrap();
    assert_eq!(
        tombstone.deleted_at, original_deleted_at,
        "the re-drain preserves the original deleted_at — the grace is not reset",
    );

    // The re-enqueued row is removed too (the drain always removes the row once the
    // tombstone is present), so the queue converges.
    assert!(
        db.get_pending_cloud_deletes().await.unwrap().is_empty(),
        "the re-drained row is removed",
    );
}

// ----- the completion cancel is durable: it survives a failure and a retry -----

/// A re-upload whose inline tombstone cancel fails does not leave the tombstone to
/// doom the blob: a durable `cancel` row is queued (atomically with removing the
/// upload row), and the tombstone-cancel drain retries it until the tombstone is
/// gone. After the retry a GC past the grace keeps the live re-uploaded blob.
#[tokio::test]
async fn a_failed_completion_cancel_is_retried_until_the_tombstone_is_gone() {
    use crate::blob::upload::drain_uploads;

    let (storage, founder, member) = storage_with_chain().await;
    let owner = pubkey_hex(&founder);
    let cipher = plaintext_cipher();
    let tmp = tempfile::tempdir().unwrap();

    // A prior cycle tombstoned the blob.
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = BlobTombstoneJson::signed(
        "test-lib",
        "blob-key".to_string(),
        deleted_at.to_string(),
        &member,
    );
    plant_tombstone(&storage, &tombstone).await;

    // The host re-stages the blob and queues the re-upload.
    let src = tmp.path().join("blob.bin");
    std::fs::write(&src, b"fresh contents").unwrap();
    let db = open_outbox_db();
    db.enqueue_upload(
        "blob-file",
        "blob-key",
        Some(&src.to_string_lossy()),
        BlobScope::Master,
        false,
        T0,
    )
    .await
    .expect("enqueue upload");

    // The cloud fails the cancel (a `delete` of the tombstone key) the first time.
    // The drain writes the blob, the inline cancel fails, and a durable cancel row
    // is queued in its place.
    let failing = FailCloudOpOnKey::new(
        &storage,
        FailingCloudOp::Delete,
        "blob_tombstones/blob-key",
        1,
    );
    let clock = FixedClock(at("2024-06-01T01:00:00Z"));
    let n = drain_uploads(
        &db,
        &failing,
        &cipher,
        &LibraryDir::new(tmp.path()),
        &clock,
        &crate::sync::hlc::Hlc::new("test-device".to_string()),
        None,
    )
    .await
    .expect("drain")
    .uploaded;
    assert_eq!(n, 1, "the blob is re-uploaded despite the cancel failing");
    assert!(
        storage.read("blob-key").await.is_ok(),
        "the re-uploaded blob is in the cloud",
    );
    assert!(
        storage.read("blob_tombstones/blob-key").await.is_ok(),
        "the tombstone still exists — the inline cancel failed",
    );
    assert!(
        db.get_pending_cloud_uploads().await.unwrap().is_empty(),
        "the upload row is removed once the blob is written",
    );
    let cancels = db.get_pending_cloud_cancels().await.unwrap();
    assert_eq!(cancels.len(), 1, "a durable cancel row is queued for retry");
    assert_eq!(cancels[0].cloud_key, "blob-key");

    // The tombstone-cancel drain retries (the injected failure is spent), removing
    // the tombstone and clearing the cancel row.
    let done = drain_tombstone_cancels(&db, &failing, &cipher)
        .await
        .expect("cancel drain");
    assert_eq!(done, 1, "the retried cancel completes");
    assert!(
        storage.read("blob_tombstones/blob-key").await.is_err(),
        "the retry removed the tombstone",
    );
    assert!(
        db.get_pending_cloud_cancels().await.unwrap().is_empty(),
        "the cancel row is cleared once the tombstone is gone",
    );

    // A GC long past the grace now finds no tombstone and keeps the live blob.
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let gc = gc_tombstones(&storage, &storage, &cipher, "test-lib", Some(&owner), &past)
        .await
        .expect("gc");
    assert_eq!(
        gc, 0,
        "no tombstone remains to reclaim the re-uploaded blob"
    );
    assert!(
        storage.read("blob-key").await.is_ok(),
        "the re-uploaded blob survives across the cancel failure and its retry",
    );
}

// ----- the GC tolerates a tombstone left over by a failed tombstone delete -----

/// When the GC reclaims a blob but the follow-up tombstone delete fails, the blob
/// is gone and the tombstone is left for a retry. The leftover must be harmless: a
/// later GC finds the blob already gone, cleans up the tombstone, and reports no
/// reclaim — it does not re-count the already-gone blob, and (paired with the
/// durable cancel) a blob re-uploaded to the key is never deleted by the leftover.
#[tokio::test]
async fn a_tombstone_left_by_a_failed_delete_is_harmless() {
    let (storage, founder, member) = storage_with_chain().await;
    let owner = pubkey_hex(&founder);
    let cipher = plaintext_cipher();

    storage
        .write(
            "blob-key",
            crate::storage::cloud::BlobBody::from_bytes(b"contents".to_vec()),
            &no_progress(),
        )
        .await
        .unwrap();
    let deleted_at = "2024-06-01T00:00:00+00:00";
    let tombstone = BlobTombstoneJson::signed(
        "test-lib",
        "blob-key".to_string(),
        deleted_at.to_string(),
        &member,
    );
    plant_tombstone(&storage, &tombstone).await;

    // First GC past the grace: the blob delete succeeds, but the tombstone delete
    // fails (injected). The blob is reclaimed; the tombstone is left behind.
    let failing = FailCloudOpOnKey::new(
        &storage,
        FailingCloudOp::Delete,
        "blob_tombstones/blob-key",
        1,
    );
    let past = FixedClock(at(&past_grace_instant(deleted_at)));
    let n = gc_tombstones(&storage, &failing, &cipher, "test-lib", Some(&owner), &past)
        .await
        .expect("first gc");
    assert_eq!(n, 1, "the blob is reclaimed");
    assert!(
        storage.read("blob-key").await.is_err(),
        "the blob is deleted",
    );
    assert!(
        storage.read("blob_tombstones/blob-key").await.is_ok(),
        "the tombstone is left behind because its delete failed",
    );

    // Second GC: the blob is already gone, so the leftover tombstone is a no-op
    // cleanup — it is removed and reports zero reclaims (no phantom re-count).
    let n = gc_tombstones(&storage, &storage, &cipher, "test-lib", Some(&owner), &past)
        .await
        .expect("second gc");
    assert_eq!(
        n, 0,
        "the leftover tombstone reclaims nothing the second pass"
    );
    assert!(
        storage.read("blob_tombstones/blob-key").await.is_err(),
        "the leftover tombstone is cleaned up",
    );

    // The data-loss case the leftover could cause: a blob re-uploaded to the same
    // key before the leftover is cleaned. The re-upload's durable cancel removes the
    // tombstone, so a GC past the grace keeps the re-uploaded blob. Reconstruct the
    // leftover (tombstone present, blob now re-uploaded) and drive a re-upload
    // through the real drain.
    let tmp = tempfile::tempdir().unwrap();
    plant_tombstone(&storage, &tombstone).await; // the leftover, again
    let src = tmp.path().join("blob.bin");
    std::fs::write(&src, b"re-uploaded contents").unwrap();
    let db = open_outbox_db();
    db.enqueue_upload(
        "blob-file",
        "blob-key",
        Some(&src.to_string_lossy()),
        BlobScope::Master,
        false,
        T0,
    )
    .await
    .expect("enqueue upload");
    let clock = FixedClock(at("2024-06-01T01:00:00Z"));
    crate::blob::upload::drain_uploads(
        &db,
        &storage,
        &cipher,
        &LibraryDir::new(tmp.path()),
        &clock,
        &crate::sync::hlc::Hlc::new("test-device".to_string()),
        None,
    )
    .await
    .expect("re-upload drain");
    assert!(
        storage.read("blob_tombstones/blob-key").await.is_err(),
        "the re-upload's cancel removed the leftover tombstone",
    );
    let n = gc_tombstones(&storage, &storage, &cipher, "test-lib", Some(&owner), &past)
        .await
        .expect("gc after re-upload");
    assert_eq!(n, 0, "no tombstone remains to delete the re-uploaded blob");
    assert!(
        storage.read("blob-key").await.is_ok(),
        "the re-uploaded blob survives the leftover-tombstone window",
    );
}
