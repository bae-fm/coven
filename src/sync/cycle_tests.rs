//! Blob-before-row ordering is enforced per row by the gate column: a blob-bearing
//! row's gate column stays off until its blobs upload, then coven flips it on (the
//! manage completion in the upload drain), so the changeset gate — and the snapshot,
//! which runs the same gate — only ever carry rows whose blobs are in the cloud. The
//! sync cycle does not hold the whole changeset back on a global "any upload
//! pending" flag.
//!
//! These tests pin that contract: a pending upload does not hold back an
//! already-shareable (gated-true) changeset or snapshot, and a gated-false row is
//! withheld until its gate flips. The completion flip + its mid-batch publish
//! (`resume_drain_promptly`) are covered in `blob::transition_tests`.

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use crate::blob::BlobScope;
use crate::clock::SystemClock;
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::push::SCHEMA_VERSION;
use crate::sync::signed_control::AckJson;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

const T0: &str = "2024-01-01T00:00:00Z";

/// Run one sync cycle for device "M" with no cloud home (no outbox drain).
async fn run_cycle_m(
    storage: &MockSyncStorage,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    keypair: &UserKeypair,
    hlc: &Hlc,
    ld: &LibraryDir,
) {
    run_single_sync_cycle(
        storage,
        "test-lib",
        "M",
        hlc,
        &SystemClock,
        db,
        cipher,
        keypair,
        ld,
        None,
        None,
    )
    .await
    .expect("cycle");
}

/// Queue a pending upload whose source file doesn't exist, so the cycle's drain
/// can't clear it — the entry stays pending, modeling a slow or stuck upload
/// while we assert the changeset/snapshot aren't held back by it.
async fn seed_pending_upload(db: &Database) {
    db.enqueue_upload(
        "f1",
        "storage/aa/bb/f1",
        Some("/nonexistent/f1"),
        BlobScope::Master,
        false,
        T0,
    )
    .await
    .expect("seed pending upload");
}

/// A pending cloud upload does not hold back a gated-true changeset: the gate
/// column decides per-row visibility, so a row that is shareable now reaches
/// peers without waiting for unrelated uploads to finish. The gate still cuts a
/// gated-false row, which is what withholds a not-yet-uploaded unit.
#[tokio::test]
async fn pending_upload_does_not_hold_back_a_gated_true_changeset() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::new_with_key(
        &[5u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    // A slow/stuck upload for some OTHER unit is pending the whole time.
    seed_pending_upload(&db).await;

    // The cycle below pushes a changeset and then snapshots it, and changeset
    // reclamation runs after the snapshot. Seed a peer that has not acked so the
    // library is multi-device with an un-acked member: its missing ack pins the
    // reclaim floor at 0, so the freshly pushed changeset is kept (a peer might
    // still need it), exactly as a real fleet behaves until everyone acks. Without
    // this peer M would be the only device and the snapshot-covered changeset would
    // be reclaimed, which is correct but not what this gate-focused test asserts.
    storage
        .put_head("peer-lagging", 0, None, T0)
        .await
        .expect("seed an un-acked peer head");

    // One shareable note (its blobs are up → gate on) and one still-private note
    // (its blobs aren't up yet → gate off; the host hasn't flipped it).
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('pub', 'Shareable', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('priv', 'NotYet', NULL, 0, '0000000002000-0000-M', '2026-01-01')",
    )
    .await;

    // The changeset pushes despite the pending upload — no global deferral.
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        storage.get_changeset("M", 1).await.is_ok(),
        "a gated-true changeset must push even while an unrelated upload is pending",
    );

    // A fresh peer pulls: it gets the shareable row, never the gated-false one.
    let db_b = open_test_db();
    pull_into(&db_b, &storage, "B", &HashMap::new(), &ld).await;
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'pub'").await,
        "Shareable",
        "the shareable note reaches the peer",
    );
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'priv'").await,
        "a gated-false row is still withheld — that is what holds a not-yet-uploaded unit",
    );
}

/// A gated-false row is withheld until its gate flips on, then it propagates: the
/// per-row gate, not a global flag, is what holds a not-yet-uploaded unit. (coven
/// flips the gate when a manage's blobs land; here the flip is written directly.)
#[tokio::test]
async fn gated_false_row_propagates_once_its_gate_flips() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::new_with_key(
        &[8u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    // A note whose blobs aren't up yet: gate off.
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', NULL, 0, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;

    let db_b = open_test_db();
    pull_into(&db_b, &storage, "B", &HashMap::new(), &ld).await;
    assert!(
        !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a gated-false row must not reach a peer",
    );

    // The blobs land; the host flips the gate on. The next cycle re-emits the
    // now-shareable row.
    exec(
        &db,
        "UPDATE notes SET shared = 1, _updated_at = '0000000003000-0000-M' WHERE id = 'n1'",
    )
    .await;
    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;

    // n1 was gated-false in cycle 1 (cut → no changeset pushed), so the flip
    // re-emits it at seq 1. Re-pull from empty cursors to pick it up wherever it
    // landed.
    pull_into(&db_b, &storage, "B", &HashMap::new(), &ld).await;
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "Album Title",
        "once its gate flips on, the row reaches the peer",
    );
}

/// The snapshot is the second propagation channel and runs the same row-level
/// gate (`delete_gated_false`), so a pending upload does not withhold it: the
/// snapshot carries the gated-true rows and excludes the gated-false ones, which
/// is the blob-before-row guarantee at snapshot granularity.
#[tokio::test]
async fn snapshot_is_not_withheld_by_pending_uploads() {
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::new_with_key(
        &[9u8; 32],
    )));
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());

    // local_seq past 0 with no snapshot yet → the snapshot policy fires this cycle.
    db.set_sync_state("local_seq", "1")
        .await
        .expect("seed local_seq");
    seed_pending_upload(&db).await;

    run_cycle_m(&storage, &db, &enc, &keypair, &hlc, &ld).await;
    assert!(
        SyncStorage::get_snapshot_pointer(&storage).await.is_ok(),
        "the snapshot must publish even while an upload is pending — the gate, not a \
         global flag, decides what it carries",
    );
}

// The drain's break-to-publish is now driven by a manage *completion* (coven flips
// the gate the moment the last blob lands), not by an observer signal. It is covered
// end-to-end in `blob::transition_tests` — `resume_drain_promptly` after a manage
// completes, with another root's blob left queued.

/// Founder-at-creation + owner anchoring (issue #102): the first cloud connect of
/// a created library writes the founder Owner entry and pins the owner; later
/// connects anchor the chain to that pinned owner; and a wiped or refounded chain
/// is refused as a takeover attempt.
#[tokio::test]
async fn ensure_owner_anchored_chain_founds_pins_and_refuses_tampering() {
    use crate::sync::cycle::ensure_owner_anchored_chain;
    use crate::sync::membership::founder_entry;
    use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    let hlc = Hlc::new("owner-dev".to_string());
    let db = open_test_db();

    // First connect: empty storage, no pinned owner → found + pin.
    let storage = MockSyncStorage::new();
    let chain = ensure_owner_anchored_chain(&storage, &db, &owner, &hlc)
        .await
        .expect("first connect founds the library");
    assert!(chain.is_founded_by(&owner_pk));
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk.clone()),
        "the owner is pinned in sync_state",
    );
    assert_eq!(
        storage.list_membership_entries().await.unwrap().len(),
        1,
        "the founder entry is written to storage",
    );

    // Second connect on the same storage + db: anchors fine (founder == owner).
    let again = ensure_owner_anchored_chain(&storage, &db, &owner, &hlc)
        .await
        .expect("re-connect anchors to the pinned owner");
    assert!(again.is_founded_by(&owner_pk));

    // Wiped membership/* with the owner still pinned → refuse (do not re-found).
    let wiped = MockSyncStorage::new();
    assert!(
        ensure_owner_anchored_chain(&wiped, &db, &owner, &hlc)
            .await
            .is_err(),
        "an empty chain with a pinned owner is tampering, not a fresh library",
    );

    // Refounded under an attacker's key with the owner pinned → refuse.
    let attacker = UserKeypair::generate();
    let forged = MockSyncStorage::new();
    let forged_founder = founder_entry(&attacker, "2026-03-01T00:00:00Z");
    forged
        .put_membership_entry(
            &hex::encode(attacker.public_key),
            1,
            serde_json::to_vec(&forged_founder).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        ensure_owner_anchored_chain(&forged, &db, &owner, &hlc)
            .await
            .is_err(),
        "a chain refounded under a different key is a takeover attempt",
    );
}

/// Founding writes the cloud founder entry before pinning the owner, so a crash
/// between the two leaves a chain founded by our own key with no pin. The next
/// connect completes the pin (the founder is provably ours). A chain founded by a
/// DIFFERENT key with no pin is a first-connect takeover seed and is refused — the
/// branch that previously adopted any founder on trust.
#[tokio::test]
async fn ensure_owner_anchored_chain_completes_own_founding_but_refuses_foreign() {
    use crate::sync::cycle::ensure_owner_anchored_chain;
    use crate::sync::membership_ops::{write_founder_entry, OWNER_PUBKEY_STATE_KEY};

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key);
    let hlc = Hlc::new("owner-dev".to_string());

    // Cloud-first crash: our founder is in storage, but the pin never landed. The
    // next connect completes it (founder == our key) and anchors.
    let db = open_test_db();
    let storage = MockSyncStorage::new();
    write_founder_entry(&storage, &owner, "0000000001000-0000-owner")
        .await
        .unwrap();
    let chain = ensure_owner_anchored_chain(&storage, &db, &owner, &hlc)
        .await
        .expect("completes our own half-done founding");
    assert!(chain.is_founded_by(&owner_pk));
    assert_eq!(
        db.get_sync_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
        Some(owner_pk),
        "the pin is completed from our own founder",
    );

    // Foreign chain, no pin: an attacker seeded a chain under their own key before
    // we ever connected. We neither founded it nor pinned an owner → refuse.
    let attacker = UserKeypair::generate();
    let fresh_db = open_test_db();
    let seeded = MockSyncStorage::new();
    write_founder_entry(&seeded, &attacker, "0000000001000-0000-attacker")
        .await
        .unwrap();
    assert!(
        ensure_owner_anchored_chain(&seeded, &fresh_db, &owner, &hlc)
            .await
            .is_err(),
        "a foreign chain with no pinned owner must be refused, not adopted on trust",
    );
}

// ---- Issue #92: the capture window is just the apply, not the whole cycle ----

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;

use crate::sync::storage::{DeviceHead, MinSchemaVersion, StorageError};

/// A [`SyncStorage`] that injects a host write at a cycle `await` point — the
/// moment the cycle fetches an incoming changeset to apply — by running a host
/// INSERT through the same `Database` the cycle holds, once, before delegating
/// `get_changeset` to the inner mock.
///
/// This models the real hazard in issue #92: a host edit committed while the
/// cycle is in its network phase. The write goes through the actor's one
/// connection (the only door) at an `await` the cycle is parked on, while capture
/// is live. If the cycle suspended capture for the whole span (the bug), the write
/// would not be recorded into the next outgoing changeset and would be lost; with
/// capture live across push/pull (the fix), it is recorded.
struct HostWriteInjector {
    inner: MockSyncStorage,
    db: Database,
    /// The INSERT to run, once, the first time the cycle fetches a changeset.
    write_sql: String,
    fired: AtomicBool,
}

impl HostWriteInjector {
    fn new(inner: MockSyncStorage, db: Database, write_sql: &str) -> Self {
        Self {
            inner,
            db,
            write_sql: write_sql.to_string(),
            fired: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl SyncStorage for HostWriteInjector {
    async fn get_changeset(&self, device_id: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        // Fire the host write exactly once, at this `await` inside the pull's
        // network phase — capture is live here, and the apply (which disables it)
        // has not started for this changeset yet.
        if !self.fired.swap(true, Ordering::SeqCst) {
            exec(&self.db, &self.write_sql).await;
        }
        self.inner.get_changeset(device_id, seq).await
    }

    async fn list_heads(&self) -> Result<Vec<DeviceHead>, StorageError> {
        self.inner.list_heads().await
    }
    async fn put_changeset(
        &self,
        device_id: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner.put_changeset(device_id, seq, data).await
    }
    async fn put_head(
        &self,
        device_id: &str,
        seq: u64,
        snapshot_seq: Option<u64>,
        timestamp: &str,
    ) -> Result<(), StorageError> {
        self.inner
            .put_head(device_id, seq, snapshot_seq, timestamp)
            .await
    }
    async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
        cloud_path: Option<&str>,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner
            .put_blob(namespace, id, scope, cloud_path, data)
            .await
    }
    async fn get_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
        cloud_path: Option<&str>,
    ) -> Result<Vec<u8>, StorageError> {
        self.inner.get_blob(namespace, id, scope, cloud_path).await
    }
    async fn read_blob_range(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
        cloud_path: Option<&str>,
        source_size: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, StorageError> {
        self.inner
            .read_blob_range(namespace, id, scope, cloud_path, source_size, offset, len)
            .await
    }
    async fn put_snapshot(
        &self,
        author: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner.put_snapshot(author, seq, data).await
    }
    async fn get_snapshot(&self, author: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        self.inner.get_snapshot(author, seq).await
    }
    async fn delete_changeset(&self, device_id: &str, seq: u64) -> Result<(), StorageError> {
        self.inner.delete_changeset(device_id, seq).await
    }
    async fn list_changesets(&self, device_id: &str) -> Result<Vec<u64>, StorageError> {
        self.inner.list_changesets(device_id).await
    }
    async fn put_ack(&self, device_id: &str, data: Vec<u8>) -> Result<(), StorageError> {
        self.inner.put_ack(device_id, data).await
    }
    async fn get_ack(&self, device_id: &str) -> Result<Vec<u8>, StorageError> {
        self.inner.get_ack(device_id).await
    }
    async fn get_min_schema_version(&self) -> Result<Option<MinSchemaVersion>, StorageError> {
        self.inner.get_min_schema_version().await
    }
    async fn set_min_schema_version(&self, version: u32) -> Result<(), StorageError> {
        self.inner.set_min_schema_version(version).await
    }
    async fn put_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner
            .put_membership_entry(author_pubkey, seq, data)
            .await
    }
    async fn get_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
    ) -> Result<Vec<u8>, StorageError> {
        self.inner.get_membership_entry(author_pubkey, seq).await
    }
    async fn list_membership_entries(&self) -> Result<Vec<(String, u64)>, StorageError> {
        self.inner.list_membership_entries().await
    }
    async fn put_wrapped_key(&self, user_pubkey: &str, data: Vec<u8>) -> Result<(), StorageError> {
        self.inner.put_wrapped_key(user_pubkey, data).await
    }
    async fn get_wrapped_key(&self, user_pubkey: &str) -> Result<Vec<u8>, StorageError> {
        self.inner.get_wrapped_key(user_pubkey).await
    }
    async fn delete_wrapped_key(&self, user_pubkey: &str) -> Result<(), StorageError> {
        self.inner.delete_wrapped_key(user_pubkey).await
    }
    async fn put_snapshot_meta(
        &self,
        author: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.inner.put_snapshot_meta(author, seq, data).await
    }
    async fn get_snapshot_meta(&self, author: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        self.inner.get_snapshot_meta(author, seq).await
    }
    async fn put_snapshot_pointer(&self, data: Vec<u8>) -> Result<(), StorageError> {
        self.inner.put_snapshot_pointer(data).await
    }
    async fn get_snapshot_pointer(&self) -> Result<Vec<u8>, StorageError> {
        self.inner.get_snapshot_pointer().await
    }
    async fn list_own_snapshot_generations(&self, author: &str) -> Result<Vec<u64>, StorageError> {
        self.inner.list_own_snapshot_generations(author).await
    }
    async fn delete_snapshot_generation(&self, author: &str, seq: u64) -> Result<(), StorageError> {
        self.inner.delete_snapshot_generation(author, seq).await
    }
}

/// Issue #92: a host write made WHILE a cycle is in its push/pull network phase
/// must land in the device's NEXT outgoing changeset. It is captured because the
/// cycle keeps the capture session enabled across push/pull — disabling it only
/// around the apply of incoming rows.
///
/// Setup: a peer "A" has a changeset in shared storage. Device "M" runs a cycle
/// that pulls it; the storage wrapper injects a host INSERT into M at the
/// `get_changeset` await (inside the pull, capture live). We then assert the
/// injected row is (a) present locally on M and (b) carried in M's next outgoing
/// changeset — proven by pulling that changeset into a fresh peer.
///
/// Mutation proof: revert the cycle to suspending capture across the whole span
/// (drop the per-apply disable and instead suspend at the top / resume at the
/// bottom). The injected write then lands while capture is off, so it is absent
/// from M's next changeset and assertion (b) fails.
#[tokio::test]
async fn host_write_during_pull_lands_in_next_outgoing_changeset() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_library_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::new_with_key(
        &[4u8; 32],
    )));

    // A peer A has published one changeset (an insert of note 'a1') to shared
    // storage, so M's cycle has something to fetch — the await we inject at.
    let inner = MockSyncStorage::new();
    let a_src = open_test_db();
    let a_cs = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    inner.store_changeset("A", 1, &a_cs, SCHEMA_VERSION);

    // M's database. The injector runs this INSERT into M at the get_changeset
    // await, mid-pull, while capture is live.
    let db_m = open_test_db();
    let storage = HostWriteInjector::new(
        inner,
        db_m.clone(),
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('m_mid', 'WrittenMidCycle', NULL, 1, '0000000002000-0000-M', '2026-01-01')",
    );

    // Cycle 1: M pulls A's changeset; the host write fires mid-pull.
    run_cycle_m_storage(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

    // (a) The injected row is present locally on M.
    assert_eq!(
        query_text(&db_m, "SELECT title FROM notes WHERE id = 'm_mid'").await,
        "WrittenMidCycle",
        "the mid-cycle host write committed to M's local db",
    );

    // (b) The injected row is in M's NEXT outgoing changeset. Cycle 2 captures the
    // batch recorded since cycle 1's capture — which includes the mid-pull write —
    // and pushes it. A fresh peer C pulls M's output and must receive 'm_mid'.
    run_cycle_m_storage(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

    let db_c = open_test_db();
    pull_into(&db_c, &storage, "C", &HashMap::new(), &ld).await;
    assert_eq!(
        query_text(&db_c, "SELECT title FROM notes WHERE id = 'm_mid'").await,
        "WrittenMidCycle",
        "the mid-cycle host write reached a peer via M's next outgoing changeset — \
         it was NOT lost to a capture-off window during push/pull",
    );
}

/// Issue #92, the other half of the invariant: an APPLIED row must NOT echo. After
/// M applies a peer's changeset, M's own next outgoing changeset must not carry the
/// applied rows — capture is disabled around the apply, so the applied rows are not
/// recorded as M's own writes.
///
/// Mutation proof: drop the capture-disable around the apply (apply on the normal
/// enabled `db.call` path). The applied rows are then recorded into M's session and
/// re-shipped on M's next changeset, so device C receives note 'a1' attributed to
/// M and the assertion fails.
#[tokio::test]
async fn applied_rows_do_not_echo_into_next_outgoing_changeset() {
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new("M".to_string());
    let (_tmp, ld) = temp_library_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::new_with_key(
        &[6u8; 32],
    )));

    // Peer A publishes a changeset; M pulls and applies it in cycle 1.
    let storage = MockSyncStorage::new();
    let a_src = open_test_db();
    let a_cs = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("A", 1, &a_cs, SCHEMA_VERSION);

    let db_m = open_test_db();
    run_cycle_m(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;
    assert_eq!(
        query_text(&db_m, "SELECT title FROM notes WHERE id = 'a1'").await,
        "FromA",
        "M applied A's changeset",
    );

    // Cycle 2: M pushes whatever it captured since. The applied row must not be in
    // it. A third device C pulls ONLY M's changesets (skip A's by pre-seeding C's
    // cursor for A) and must not receive 'a1' from M.
    run_cycle_m(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

    let db_c = open_test_db();
    let mut c_cursors = HashMap::new();
    c_cursors.insert("A".to_string(), 1); // C already has A's seq 1; only pull M.
    pull_into(&db_c, &storage, "C", &c_cursors, &ld).await;
    assert!(
        !row_exists(&db_c, "SELECT 1 FROM notes WHERE id = 'a1'").await,
        "the row M applied from A must NOT echo back through M's own changeset \
         (capture is disabled around the apply)",
    );
}

/// Like [`run_cycle_m`] but over an arbitrary `&dyn SyncStorage` (e.g. the
/// host-write injector), still with no cloud home (no outbox drain, no auth
/// refresh).
async fn run_cycle_m_storage(
    storage: &dyn SyncStorage,
    db: &Database,
    cipher: &RwLock<CloudCipher>,
    keypair: &UserKeypair,
    hlc: &Hlc,
    ld: &LibraryDir,
) {
    run_single_sync_cycle(
        storage,
        "test-lib",
        "M",
        hlc,
        &SystemClock,
        db,
        cipher,
        keypair,
        ld,
        None,
        None,
    )
    .await
    .expect("cycle");
}

// ---- changeset reclamation through a real cycle ----

/// A changeset that becomes both snapshot-covered and acked by every current
/// device is reclaimed by the cycle that publishes the snapshot. Peer A has pushed
/// `changes/A/1`; M runs one cycle that pulls it (so M acks A->1), snapshots
/// (covering A->1), and then reclaims — so `changes/A/1` is gone afterward.
///
/// The mock is built with M's keypair so the head it signs for M and the ack M
/// publishes share an author, the same identity a real device's storage and ack
/// share — which is what lets reclamation honor M's ack against M's head.
#[tokio::test]
async fn cycle_reclaims_a_fully_acked_changeset() {
    let keypair = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(keypair.clone());
    let db_m = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::new_with_key(
        &[11u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    // Peer A's changeset 1 (a shareable note).
    let a_src = open_test_db();
    let a_cs = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("A", 1, &a_cs, SCHEMA_VERSION);

    // M's cycle pulls A->1, acks A->1, snapshots covering A->1, then reclaims.
    run_cycle_m(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

    assert!(
        storage.get_changeset("A", 1).await.is_err(),
        "a snapshot-covered, fully-acked changeset is reclaimed by the cycle",
    );
}

/// A changeset a behind peer still needs is NOT reclaimed, even after a snapshot
/// covers it. Peer A has pushed `changes/A/1` and `changes/A/2`; a peer B is parked
/// at A->1 (its ack reports only A->1). M pulls both, snapshots covering A->2, and
/// reclaims — but B's ack pins the floor at 1, so `changes/A/2` survives and B pulls
/// it forward.
#[tokio::test]
async fn cycle_keeps_a_behind_peers_changeset() {
    let keypair = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(keypair.clone());
    let db_m = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let enc = RwLock::new(CloudCipher::Encrypted(EncryptionService::new_with_key(
        &[12u8; 32],
    )));
    let hlc = Hlc::new("M".to_string());

    // Peer A's two changesets (two independent shareable notes).
    let a_src = open_test_db();
    let cs1 = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'One', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("A", 1, &cs1, SCHEMA_VERSION);
    let cs2 = capture_bytes(
        &a_src,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a2', 'Two', NULL, 1, '0000000002000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("A", 2, &cs2, SCHEMA_VERSION);

    // A behind peer B: a head (so it counts as a current device) and an ack that
    // reports it has pulled A only through seq 1. Both are signed by the mock's
    // keypair so B's ack author matches B's head author.
    storage
        .put_head("B", 0, None, T0)
        .await
        .expect("seed behind peer head");
    let b_ack = AckJson::signed("B", BTreeMap::from([("A".to_string(), 1u64)]), &keypair);
    storage
        .put_ack("B", serde_json::to_vec(&b_ack).expect("serialize ack"))
        .await
        .expect("seed behind peer ack");

    // M's cycle pulls A->2, acks A->2, snapshots covering A->2, then reclaims. The
    // floor is min(snapshot A->2, min(M ack A->2, B ack A->1)) = 1.
    run_cycle_m(&storage, &db_m, &enc, &keypair, &hlc, &ld).await;

    assert!(
        storage.get_changeset("A", 1).await.is_err(),
        "the changeset below the floor (everyone has it) is reclaimed",
    );
    assert!(
        storage.get_changeset("A", 2).await.is_ok(),
        "the changeset the behind peer still needs is kept",
    );

    // And the behind peer pulls the kept changeset forward.
    let db_b = open_test_db();
    let mut b_cursors = HashMap::new();
    b_cursors.insert("A".to_string(), 1);
    pull_into(&db_b, &storage, "B", &b_cursors, &ld).await;
    assert!(
        row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'a2'").await,
        "the behind peer pulls the kept changeset forward",
    );
}
