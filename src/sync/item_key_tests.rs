//! End-to-end tests for coven-managed item keys and the public→internal scope
//! resolution.
//!
//! These drive a real [`crate::database::Database`] (so `mint_item_key`'s INSERT
//! enters the capture session's changeset and `item_keys` rides the synced set
//! `Database::open` injects) and a real [`EncryptedSyncStorage`] over a shared
//! [`InMemoryCloudHome`] (so a blob actually round-trips through encryption). The
//! load-bearing property throughout: an `Item`-scoped blob is encrypted under the
//! per-item key, which a joining device recovers — by changeset replay or by
//! snapshot bootstrap — while the library master key (which every member holds)
//! cannot read it.

use crate::blob::{BlobScope, ResolvedScope};
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::apply::apply_changeset_lww;
use crate::sync::encrypted_storage::EncryptedSyncStorage;
use crate::sync::snapshot::{bootstrap_from_snapshot, create_snapshot, push_snapshot};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{create_synced_schema, test_synced_tables};

/// A fixed master key, distinct from any minted item key, so "the master cannot
/// read item content" is a real assertion.
const MASTER_KEY: [u8; 32] = [7u8; 32];

/// Open a real `Database` over a fresh in-memory connection with the synthetic
/// host schema. `Database::open` injects coven's `item_keys` table into the
/// synced set, so a minted key is captured and snapshotted like any synced row.
fn open_db(device_id: &str) -> Database {
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        device_id.to_string(),
        create_synced_schema,
    )
    .expect("open item-key test database");
    db
}

/// An `EncryptedSyncStorage` over `home`, keyed by the library master key. A
/// blob's actual encryption key is selected by the resolved scope, not this.
fn storage_over(home: InMemoryCloudHome) -> EncryptedSyncStorage {
    EncryptedSyncStorage::new(Box::new(home), EncryptionService::new_with_key(&MASTER_KEY))
}

/// Capture the outgoing changeset from `db` (the rows written since the last
/// capture), re-attaching the session so the db stays usable.
async fn capture(db: &Database) -> Vec<u8> {
    let bytes = db
        .take_changeset_and_suspend()
        .await
        .expect("capture changeset");
    db.resume_session().await.expect("resume session");
    bytes
}

/// Apply `bytes` to `db` over its full synced set (host tables plus coven's
/// injected `item_keys`), suspending the session around the apply as the cycle
/// does.
async fn apply(db: &Database, bytes: &[u8]) {
    db.take_changeset_and_suspend()
        .await
        .expect("suspend before apply");
    let bytes = bytes.to_vec();
    let tables = db.synced_tables().to_vec();
    db.call(move |conn| apply_changeset_lww(conn, &bytes, &tables).map(|_| ()))
        .await
        .expect("apply changeset");
    db.resume_session().await.expect("resume after apply");
}

/// An `Item`-scoped blob is encrypted under the minted item key: it round-trips
/// when the scope resolves to that key, and the master key cannot read it. This
/// is the per-item-key property at the storage boundary, exercised through the
/// real public→internal resolution rather than a hand-built `ResolvedScope::Key`.
#[tokio::test]
async fn item_scoped_blob_round_trips_master_cannot_read() {
    let db = open_db("dev-a");
    let storage = storage_over(InMemoryCloudHome::new());

    let item_key = db.mint_item_key("item-1").await.expect("mint item key");
    assert_ne!(
        item_key, MASTER_KEY,
        "a minted item key is independent of the master key"
    );

    let plaintext = b"per-item cover bytes".to_vec();
    let resolved = db
        .resolve_blob_scope(BlobScope::Item("item-1".to_string()))
        .await
        .expect("resolve Item scope");
    assert_eq!(
        resolved,
        ResolvedScope::Key(item_key),
        "Item(id) resolves to the minted key"
    );

    storage
        .put_blob("images", "item-1", resolved.clone(), plaintext.clone())
        .await
        .expect("put item-scoped blob");

    // The item key reads it back.
    let got = storage
        .get_blob("images", "item-1", resolved)
        .await
        .expect("get item-scoped blob");
    assert_eq!(got, plaintext);

    // The master key — held by every member — cannot decrypt it.
    assert!(
        storage
            .get_blob("images", "item-1", ResolvedScope::Master)
            .await
            .is_err(),
        "the master key must not decrypt an item-scoped blob"
    );
}

/// A missing `item_keys` row at resolution time is a host bug, surfaced as an
/// error — coven must NOT silently fall back to the master key (that would
/// encrypt the blob so no share recipient could read it).
#[tokio::test]
async fn resolving_unminted_item_errors() {
    let db = open_db("dev-a");
    let err = db
        .resolve_blob_scope(BlobScope::Item("never-minted".to_string()))
        .await
        .expect_err("resolving an item with no minted key must error");
    let DbError(msg) = err;
    assert!(
        msg.contains("never-minted"),
        "the error names the offending item: {msg}"
    );
}

/// `mint_item_key` is idempotent: a re-mint returns the original key and does not
/// rotate it out from under blobs already encrypted under it.
#[tokio::test]
async fn mint_item_key_is_idempotent() {
    let db = open_db("dev-a");
    let first = db.mint_item_key("item-1").await.expect("first mint");
    let second = db.mint_item_key("item-1").await.expect("re-mint");
    assert_eq!(first, second, "re-minting keeps the original key");
    assert_eq!(
        db.item_key("item-1").await.expect("read back"),
        Some(first),
        "the stored key matches the first mint"
    );
}

/// Changeset-replay multi-device join: device A mints an item key and creates an
/// `Item`-scoped blob, then syncs (the item key rides the changeset). Device B
/// pulls CHANGESETS ONLY (no snapshot), resolves `Item(id)` from the replayed
/// `item_keys` row, and decrypts the blob. This is the path a member that joined
/// before any snapshot exists takes.
#[tokio::test]
async fn changeset_replay_join_resolves_item_and_decrypts() {
    let home = InMemoryCloudHome::new();
    let storage = storage_over(home);

    // --- Device A: mint the item key, upload an Item-scoped blob, capture the
    // changeset carrying the item_keys row. ---
    let db_a = open_db("dev-a");
    let item_key = db_a.mint_item_key("item-1").await.expect("mint on A");

    let plaintext = b"AUDIO-BYTES-for-item-1".to_vec();
    let resolved_a = db_a
        .resolve_blob_scope(BlobScope::Item("item-1".to_string()))
        .await
        .expect("resolve on A");
    storage
        .put_blob("audio", "item-1", resolved_a, plaintext.clone())
        .await
        .expect("A uploads the item-scoped blob");

    let changeset = capture(&db_a).await;
    assert!(
        !changeset.is_empty(),
        "minting an item key must enter the changeset (it is a synced table)"
    );

    // --- Device B: a brand-new device that has NOT bootstrapped a snapshot.
    // It applies A's changeset only, then must resolve and decrypt. ---
    let db_b = open_db("dev-b");
    assert_eq!(
        db_b.item_key("item-1").await.expect("B pre-apply read"),
        None,
        "device B has no item key before replaying A's changeset"
    );

    apply(&db_b, &changeset).await;

    assert_eq!(
        db_b.item_key("item-1").await.expect("B post-apply read"),
        Some(item_key),
        "the item key replayed to device B via the changeset"
    );

    let resolved_b = db_b
        .resolve_blob_scope(BlobScope::Item("item-1".to_string()))
        .await
        .expect("resolve on B");
    let recovered = storage
        .get_blob("audio", "item-1", resolved_b)
        .await
        .expect("B downloads + decrypts the item-scoped blob");
    assert_eq!(
        recovered, plaintext,
        "device B recovers the original audio via the replayed item key"
    );

    // The master key alone (membership) does not unlock it.
    assert!(
        storage
            .get_blob("audio", "item-1", ResolvedScope::Master)
            .await
            .is_err(),
        "membership alone (the master key) does not unlock item content"
    );
}

/// Snapshot-bootstrap join: device A mints an item key and creates an
/// `Item`-scoped blob, then snapshots. Device B bootstraps from the snapshot (no
/// changeset replay) and still resolves `Item(id)` and decrypts — i.e. the
/// `item_keys` row SURVIVES `bootstrap_from_snapshot`. This is the opposite of
/// the bookkeeping-stripped assertion: bookkeeping tables end empty after a
/// bootstrap, but `item_keys` (a synced table) must be preserved.
#[tokio::test]
async fn snapshot_bootstrap_join_resolves_item_and_decrypts() {
    let home = InMemoryCloudHome::new();
    let storage = storage_over(home);
    let snapshot_enc = EncryptionService::new_with_key(&MASTER_KEY);

    // --- Device A: mint + upload an Item-scoped blob, then snapshot its live DB. ---
    let db_a = open_db("dev-a");
    let item_key = db_a.mint_item_key("item-1").await.expect("mint on A");

    let plaintext = b"AUDIO-BYTES-for-item-1".to_vec();
    let resolved_a = db_a
        .resolve_blob_scope(BlobScope::Item("item-1".to_string()))
        .await
        .expect("resolve on A");
    storage
        .put_blob("audio", "item-1", resolved_a, plaintext.clone())
        .await
        .expect("A uploads the item-scoped blob");

    // Snapshot over A's full synced set (including the injected `item_keys`), the
    // same set production passes via `db.synced_tables()`.
    let temp = tempfile::tempdir().unwrap();
    let snap_dir = temp.path().to_path_buf();
    let tables = db_a.synced_tables().to_vec();
    let enc = snapshot_enc.clone();
    let encrypted = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables, &enc)
                .map_err(|e| DbError(format!("snapshot: {e}")))
        })
        .await
        .expect("create snapshot on A");

    push_snapshot(
        &storage,
        encrypted,
        "dev-a",
        std::collections::HashMap::new(),
        1,
        &crate::clock::SystemClock,
    )
    .await
    .expect("push snapshot");

    // --- Device B: bootstrap from the snapshot bytes (no changeset replay). ---
    let target = temp.path().join("device_b.db");
    bootstrap_from_snapshot(&storage, &snapshot_enc, &target)
        .await
        .expect("device B bootstraps from snapshot");

    // Open the bootstrapped file as a real Database so resolution runs over the
    // injected synced set, exactly as a joined device would.
    let (db_b, _stamper) = Database::open(
        &target,
        test_synced_tables(),
        "dev-b".to_string(),
        // The snapshot bytes already carry the schema; migrate is a no-op.
        |_conn| Ok(()),
    )
    .expect("open bootstrapped database");

    assert_eq!(
        db_b.item_key("item-1").await.expect("B reads item key"),
        Some(item_key),
        "the item key SURVIVES the snapshot bootstrap (item_keys is a synced table)"
    );

    let resolved_b = db_b
        .resolve_blob_scope(BlobScope::Item("item-1".to_string()))
        .await
        .expect("resolve on bootstrapped B");
    let recovered = storage
        .get_blob("audio", "item-1", resolved_b)
        .await
        .expect("B decrypts the item-scoped blob after bootstrap");
    assert_eq!(
        recovered, plaintext,
        "device B recovers the audio via the snapshot-preserved item key"
    );
}
