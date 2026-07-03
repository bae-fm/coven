//! End-to-end tests for coven-managed item keys and the public→internal scope
//! resolution.
//!
//! These drive a real [`crate::database::Database`] (so `mint_item_key`'s INSERT
//! enters the capture session's changeset and `item_keys` rides the synced set
//! `Database::open` injects) and a real [`CloudSyncStorage`] over a shared
//! [`InMemoryCloudHome`] (so a blob actually round-trips through encryption). The
//! load-bearing property throughout: an `Item`-scoped blob is encrypted under the
//! per-item key, which a joining device recovers — by changeset replay or by
//! snapshot bootstrap — while the library master key (which every member holds)
//! cannot read it.

use std::collections::HashMap;

use crate::blob::{BlobScope, CacheFill, Provenance, ResolvedScope};
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::cycle::push_changeset;
use crate::sync::envelope::{self, ChangesetEnvelope};
use crate::sync::pull::pull_changes;
use crate::sync::session::{BlobDecl, BlobScopeSpec};
use crate::sync::snapshot::{bootstrap_from_snapshot, create_snapshot, push_snapshot};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{
    capture_bytes, exec, temp_library_dir, test_migrations, test_synced_tables,
    test_synced_tables_with_blob,
};

/// The synthetic test db opens with a single migration, so its
/// [`Database::schema_version`] is 1. Changesets are stored at that version.
const SCHEMA_VERSION: u32 = 1;

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
        &test_migrations(),
    )
    .expect("open item-key test database");
    db
}

/// A `CloudSyncStorage` over `home`, keyed by the library master key. A blob's
/// actual encryption key is selected by the resolved scope, not this.
fn storage_over(home: InMemoryCloudHome) -> CloudSyncStorage {
    CloudSyncStorage::new(
        std::sync::Arc::new(home),
        CloudCipher::Encrypted(EncryptionService::from_key(MASTER_KEY)),
        BlobPathScheme::Hashed,
        UserKeypair::generate(),
    )
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
        .put_blob(
            "images",
            "item-1",
            resolved.clone(),
            None,
            plaintext.clone(),
        )
        .await
        .expect("put item-scoped blob");

    // The item key reads it back.
    let got = storage
        .get_blob("images", "item-1", resolved, None)
        .await
        .expect("get item-scoped blob");
    assert_eq!(got, plaintext);

    // The master key — held by every member — cannot decrypt it.
    assert!(
        storage
            .get_blob("images", "item-1", ResolvedScope::Master, None)
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

/// A stored `item_keys.key` that is not 32 bytes is a corrupt DB. Reading it
/// surfaces a [`DbError`] naming the item and the wrong length — not a panic, and
/// not an opaque "actor dropped" error from a panicked db thread. (`mint_item_key`
/// only ever writes 32 bytes, so a short key can arise only from corruption.)
#[tokio::test]
async fn reading_a_wrong_length_item_key_errors() {
    let db = open_db("dev-a");
    db.call(|conn| {
        conn.execute(
            "INSERT INTO item_keys (item_id, key, _updated_at) \
             VALUES ('item-1', ?1, '0000000001000-0000-dev-a')",
            [vec![0u8; 16]],
        )
        .map(|_| ())
        .map_err(DbError::from)
    })
    .await
    .expect("plant a 16-byte key");

    let DbError(msg) = db
        .item_key("item-1")
        .await
        .expect_err("a 16-byte stored key must error, not panic");
    assert!(
        msg.contains("item-1") && msg.contains("16") && msg.contains("32"),
        "the error names the item and both lengths: {msg}"
    );
}

/// A `note_photos` blob declaration mapping each row to an `Item`-scoped,
/// `CacheEager` blob keyed by the row's `note_id` — the public scope a sharing host
/// emits, the one whose resolution `pull_changes` performs before applying the
/// changeset. The namespace is `audio` (the bulk share payload bae moves this way).
/// `CacheEager` so the pull downloads it: this exercise is about resolving the
/// item key during download-before-apply, not the on-demand skip.
fn item_photo_decl() -> BlobDecl {
    BlobDecl::new("audio", Provenance::HostProvided, CacheFill::CacheEager)
        .with_scope(BlobScopeSpec::ItemColumn("note_id".to_string()))
}

/// Capture `db_a`'s pending writes and publish them to `storage` as device A's
/// changeset seq 1: pack an unsigned envelope (no membership chain in this test,
/// so unsigned is accepted) and push it, which also advances A's head. This is
/// what the cycle's push does, minus the gate (the rows here are already
/// shareable) — enough for a real `pull_changes` to fetch and apply.
async fn publish_changeset(db_a: &Database, storage: &dyn SyncStorage) {
    let changeset = capture_bytes(db_a, &[]).await;
    assert!(
        !changeset.is_empty(),
        "device A's writes (note rows + the item_keys row) must enter the changeset"
    );
    let env = ChangesetEnvelope {
        device_id: "dev-a".to_string(),
        seq: 1,
        schema_version: SCHEMA_VERSION,
        message: String::new(),
        timestamp: "2026-01-01T00:00:00Z".to_string(),
        changeset_size: changeset.len(),
        author_pubkey: None,
        membership_grant: None,
        signature: None,
    };
    let packed = envelope::pack(&env, &changeset);
    push_changeset(storage, "dev-a", 1, packed, None, "2026-01-01T00:00:00Z")
        .await
        .expect("publish device A's changeset");
}

/// Changeset-replay multi-device join, through the REAL pull path: device A mints
/// an item key, writes a shareable note + its blob-bearing child row, and uploads
/// the `Item`-scoped blob; then publishes the changeset (carrying the `item_keys`
/// row). Device B runs the production [`pull_changes`] — which now resolves
/// `Item(id)` from the `item_keys` row carried IN this changeset and downloads +
/// fsyncs the blob BEFORE applying the changeset (issue #111: a row is never
/// applied before its blob is durable). The key comes from the walked changeset,
/// not a freshly-applied DB row, so the blob lands on B's disk already decrypted
/// without the apply having to precede the download. This catches a regression
/// where the pull-side resolution is dropped: B would fail to resolve the key and
/// the blob would not land. Members that join before any snapshot exists take
/// this path.
#[tokio::test]
async fn changeset_replay_join_resolves_item_and_decrypts() {
    let storage = CloudSyncStorage::new(
        std::sync::Arc::new(InMemoryCloudHome::new()),
        CloudCipher::Encrypted(EncryptionService::from_key(MASTER_KEY)),
        BlobPathScheme::Hashed,
        UserKeypair::generate(),
    );

    // --- Device A: mint the item key, write a shareable note + a blob-bearing
    // child row, upload the Item-scoped blob, publish the changeset. ---
    let db_a = open_db("dev-a");
    let item_key = db_a.mint_item_key("note-1").await.expect("mint on A");

    exec(
        &db_a,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('note-1', 'Shared', NULL, 1, '0000000001000-0000-dev-a', '2026-01-01')",
    )
    .await;
    exec(
        &db_a,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('blob-1', 'note-1', 'cover', '0000000001000-0000-dev-a', '2026-01-01')",
    )
    .await;

    let plaintext = b"AUDIO-BYTES-for-note-1".to_vec();
    let resolved_a = db_a
        .resolve_blob_scope(BlobScope::Item("note-1".to_string()))
        .await
        .expect("resolve on A");
    storage
        .put_blob("audio", "blob-1", resolved_a, None, plaintext.clone())
        .await
        .expect("A uploads the item-scoped blob");

    publish_changeset(&db_a, &storage).await;

    // --- Device B: a brand-new device that has NOT bootstrapped a snapshot. It
    // pulls A's changeset through the real pull path; resolution + download run
    // inside `pull_changes`. ---
    let (db_b, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        test_synced_tables_with_blob(item_photo_decl()),
        "dev-b".to_string(),
        &test_migrations(),
    )
    .expect("open device B with the item-scoped blob declaration");
    assert_eq!(
        db_b.item_key("note-1").await.expect("B pre-pull read"),
        None,
        "device B has no item key before pulling A's changeset"
    );

    let (_tmp_lib, ld) = temp_library_dir();

    let (cursors, result) = pull_changes(
        &db_b,
        db_b.synced_tables(),
        &storage,
        "dev-b",
        &HashMap::new(),
        &ld,
    )
    .await
    .expect("device B pulls A's changeset");

    assert_eq!(result.changesets_applied, 1, "B applied A's changeset");
    assert!(
        !result.asset_downloads_failed,
        "the Item-scoped blob resolved and downloaded inside pull_changes"
    );
    assert_eq!(
        cursors.get("dev-a"),
        Some(&1),
        "B's cursor advanced past the changeset whose blob downloaded"
    );

    // The item key replayed via the changeset, and the production pull download
    // path resolved it and wrote the decrypted blob to B. A `CacheEager` blob
    // lands in B's evictable cache (`storage/cache/<id>`) on pull.
    assert_eq!(
        db_b.item_key("note-1").await.expect("B post-pull read"),
        Some(item_key),
        "the item key replayed to device B via the changeset"
    );
    let landed = std::fs::read(ld.cache_blob_path("audio", "blob-1").expect("cache path"))
        .expect("the pull wrote the blob to B's disk");
    assert_eq!(
        landed, plaintext,
        "device B recovers the original audio — the pull resolved Item(id) and decrypted with the replayed item key"
    );

    // The master key alone (membership) does not unlock it.
    assert!(
        storage
            .get_blob("audio", "blob-1", ResolvedScope::Master, None)
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
    let snapshot_enc = CloudCipher::Encrypted(EncryptionService::from_key(MASTER_KEY));

    // --- Device A: mint + upload an Item-scoped blob, then snapshot its live DB. ---
    let db_a = open_db("dev-a");
    let item_key = db_a.mint_item_key("item-1").await.expect("mint on A");

    let plaintext = b"AUDIO-BYTES-for-item-1".to_vec();
    let resolved_a = db_a
        .resolve_blob_scope(BlobScope::Item("item-1".to_string()))
        .await
        .expect("resolve on A");
    storage
        .put_blob("audio", "item-1", resolved_a, None, plaintext.clone())
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
        "test-lib",
        encrypted,
        "dev-a",
        std::collections::HashMap::new(),
        1,
        db_a.schema_version(),
        &UserKeypair::generate(),
        &crate::clock::SystemClock,
    )
    .await
    .expect("push snapshot");

    // --- Device B: bootstrap from the snapshot bytes (no changeset replay). ---
    // This library has no membership chain (a bare storage with no founder entry),
    // so the snapshot is authorized on its signature alone — the open-library path.
    // The bootstrapping binary supports the same schema version A wrote (1).
    let target = temp.path().join("device_b.db");
    bootstrap_from_snapshot(&storage, "test-lib", &snapshot_enc, None, 1, &target)
        .await
        .expect("device B bootstraps from snapshot");

    // Open the bootstrapped file as a real Database so resolution runs over the
    // injected synced set, exactly as a joined device would. The snapshot bytes
    // already carry the schema at `user_version` 1, so the ladder is a no-op here.
    let (db_b, _stamper) = Database::open(
        &target,
        test_synced_tables(),
        "dev-b".to_string(),
        &test_migrations(),
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
        .get_blob("audio", "item-1", resolved_b, None)
        .await
        .expect("B decrypts the item-scoped blob after bootstrap");
    assert_eq!(
        recovered, plaintext,
        "device B recovers the audio via the snapshot-preserved item key"
    );
}
