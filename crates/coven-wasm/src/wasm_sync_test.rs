//! Headless wasm proof that coven's sync engine runs on wasm32: a row written on
//! one `Database` crosses to a second `Database` through the real engine —
//! capture, Store commit publication, pull, and apply — over a shared in-memory cloud.
//!
//! Two `:memory:` `Database`s stand in for two devices; each wraps the SAME
//! backing `InMemoryCloudHome` in its own `CloudSyncStorage`, so they read
//! and write one cloud bucket. The cipher is `Plaintext` and blob paths are plain, so
//! the engine exercises the production `CloudSyncStorage` path end to end with no
//! key setup. Each drive uses the initialized sync session's cycle method, the
//! same entry the browser runtime calls.
//!
//! `:memory:` (not OPFS) because this exercises the sync engine, not durability:
//! the two DBs only need to be independent, which a fresh `:memory:` connection
//! already is. So no `install_browser_storage` and no Worker-only OPFS handles are
//! needed — but the test still runs in a dedicated Worker to match the rest of the
//! wasm suite. Drive it with `wasm-pack test --headless --firefox`.

use rusqlite::OptionalExtension;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

use crate::clock::SystemClock;
use crate::database::{Database, DbError};
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::SequentialCopyIdGenerator;
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::cycle::StoreInitialization;
use crate::sync::test_helpers::{test_migrations, test_synced_tables};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Open a `Database` over a fresh `:memory:` connection with the synthetic synced
/// schema (`notes` gated by `shared`, plus FK children) and `device_id`. Each call
/// is an independent in-memory database — the two devices share nothing locally,
/// only the cloud.
fn open_device(device_id: &str) -> Database {
    crate::install_platform();
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        device_id.to_string(),
        &test_migrations(),
    )
    .expect("open in-memory Database");
    db
}

/// A `CloudSyncStorage` over a clone of the shared cloud handle, plaintext at rest
/// with plain blob paths. Two of these built over clones of one
/// `InMemoryCloudHome` are two devices on one bucket.
fn storage_for(cloud: &InMemoryCloudHome, identity: &UserKeypair) -> CloudSyncStorage {
    let copy_source = crate::keys::public_key_hex(identity);
    CloudSyncStorage::new(
        std::sync::Arc::new(cloud.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "wasm-sync-test",
        identity.clone(),
    )
    .with_copy_ids(std::sync::Arc::new(SequentialCopyIdGenerator::new(
        &copy_source,
    )))
}

/// Run one full sync cycle for `device_id` against `storage`. No cloud home and no
/// observer: this engine test publishes and pulls Store commits without blobs.
async fn run_cycle(storage: CloudSyncStorage, db: &Database, initialization: StoreInitialization) {
    // No blobs or snapshot images are created, so the cycle does not access this
    // browser-only placeholder directory.
    let store_dir = StoreDir::new(std::path::Path::new("/coven-wasm-sync-test"));

    let components = crate::sync::cycle::init_sync_over_storage(db, storage, initialization)
        .await
        .expect("initialize sync session");
    components
        .run_cycle(&SystemClock, None, &store_dir, None)
        .await
        .expect("sync cycle");
}

/// Whether `db` holds a `notes` row with the given id.
async fn has_note(db: &Database, id: &str) -> bool {
    let id = id.to_string();
    db.call(move |conn| {
        conn.query_row("SELECT 1 FROM notes WHERE id = ?1", [id], |_| Ok(()))
            .optional()
            .map(|o| o.is_some())
            .map_err(DbError::from)
    })
    .await
    .expect("query notes")
}

/// Device A writes a shared row and pushes; device B pulls and ends up with the
/// row. This is the whole engine on wasm: A's `Database` captures the INSERT into a
/// changeset, `CloudSyncStorage` publishes an immutable Store package and commit,
/// and B's cycle verifies and applies that commit to B's `Database`.
#[wasm_bindgen_test]
async fn row_syncs_from_one_database_to_another_through_the_engine() {
    console_error_panic_hook::set_once();

    let cloud = InMemoryCloudHome::new();
    assert!(cloud.is_empty(), "the shared cloud starts empty");

    let db_a = open_device("device-a");
    let db_b = open_device("device-b");
    let identity = UserKeypair::generate();

    // Device A writes a SHARED note (gate column `shared = 1`, so the gate keeps
    // it in the outgoing changeset) through coven's journaled write path, the same
    // path a host write takes; the write lands in the pending-changeset journal for
    // A's cycle to push.
    crate::wasm_test_support::journaled_exec(
        &db_a,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('note-1', 'hello from A', 'body', 1, '0000000001000-0000-device-a', '2026-01-01')",
    )
    .await
    .expect("device A insert");

    // Device A publishes its package, commit, and head to the shared cloud.
    let storage_a = storage_for(&cloud, &identity);
    run_cycle(storage_a, &db_a, StoreInitialization::CreateStore).await;
    let genesis_hash = db_a
        .get_protocol_state(crate::database::PROTOCOL_GENESIS_HASH_STATE_KEY)
        .await
        .expect("read A's protocol genesis")
        .expect("A initialized a protocol genesis")
        .parse()
        .expect("parse A's protocol genesis hash");

    assert!(
        cloud
            .appended_keys()
            .iter()
            .any(|key| key.starts_with("store-v1/packages/device-a/1/")),
        "device A's Store package must land in the shared cloud at seq 1",
    );
    assert!(
        cloud
            .appended_keys()
            .iter()
            .any(|key| key.starts_with("store-v1/heads/device-a/1/")),
        "device A must publish its Store head",
    );

    // The row exists on A, not yet on B.
    assert!(has_note(&db_a, "note-1").await, "A holds its own row");
    assert!(
        !has_note(&db_b, "note-1").await,
        "B has not pulled yet, so it must not hold A's row",
    );

    // Device B verifies A's head, commit, package, and signature, then applies
    // the captured INSERT.
    let storage_b = storage_for(&cloud, &identity);
    run_cycle(
        storage_b,
        &db_b,
        StoreInitialization::OpenStore {
            expected_genesis_hash: genesis_hash,
            expected_founder: crate::keys::public_key_hex(&identity),
        },
    )
    .await;

    assert!(
        has_note(&db_b, "note-1").await,
        "device B did not receive device A's row — capture → push → pull → apply \
         failed on wasm",
    );

    // The applied row carries A's content, not a placeholder: the changeset moved
    // the actual column values across.
    let title: Option<String> = db_b
        .call(|conn| {
            conn.query_row("SELECT title FROM notes WHERE id = 'note-1'", [], |r| {
                r.get::<_, String>(0)
            })
            .optional()
            .map_err(DbError::from)
        })
        .await
        .expect("read title on B");
    assert_eq!(
        title.as_deref(),
        Some("hello from A"),
        "the pulled row must carry A's column values",
    );
}
