//! Headless wasm proof that coven's sync engine runs on wasm32: a row written on
//! one `Database` crosses to a second `Database` through the real engine —
//! capture, changeset push, pull, and apply — over a shared in-memory cloud.
//!
//! Two `:memory:` `Database`s stand in for two devices; each wraps the SAME
//! backing `InMemoryCloudHome` in its own `CloudSyncStorage`, so they read
//! and write one cloud bucket. The cipher is `Plaintext` and blobs are hashed, so
//! the engine exercises the production `CloudSyncStorage` path end to end with no
//! key setup. The drive entry is [`run_single_sync_cycle`] — the same single-cycle
//! function the native sync loop calls — so this proves the engine itself, not a
//! test shim, runs on the browser's one thread.
//!
//! `:memory:` (not OPFS) because this exercises the sync engine, not durability:
//! the two DBs only need to be independent, which a fresh `:memory:` connection
//! already is. So no `install_browser_storage` and no Worker-only OPFS handles are
//! needed — but the test still runs in a dedicated Worker to match the rest of the
//! wasm suite. Drive it with `wasm-pack test --headless --firefox`.

use std::sync::RwLock;

use rusqlite::OptionalExtension;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

use crate::clock::SystemClock;
use crate::database::{Database, DbError};
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
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
        device_id.to_string(),
        &test_migrations(),
    )
    .expect("open in-memory Database");
    db
}

/// A `CloudSyncStorage` over a clone of the shared cloud handle, plaintext at rest
/// with hashed blob paths. Two of these built over clones of one
/// `InMemoryCloudHome` are two devices on one bucket.
fn storage_for(cloud: &InMemoryCloudHome) -> CloudSyncStorage {
    CloudSyncStorage::new(
        std::sync::Arc::new(cloud.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Hashed,
        UserKeypair::generate(),
    )
}

/// Run one full sync cycle for `device_id` against `storage`. No cloud home and no
/// observer: this engine test pushes/pulls changesets only, not the blob outbox.
async fn run_cycle(storage: &CloudSyncStorage, db: &Database, device_id: &str) {
    let cipher = RwLock::new(CloudCipher::Plaintext);
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new(device_id.to_string());
    db.set_sync_state("snapshot_seq", "0")
        .await
        .expect("seed snapshot floor for changeset-only wasm test");
    // A library dir that never touches disk: this test pushes/pulls changesets, so
    // the only fs touch the cycle attempts is best-effort changeset staging, which
    // logs and continues on failure. No blobs, no snapshot bytes are read back.
    let library_dir = LibraryDir::new(std::path::Path::new("/coven-wasm-sync-test"));

    run_single_sync_cycle(
        storage,
        "test-lib",
        device_id,
        &hlc,
        &SystemClock,
        db,
        &cipher,
        &keypair,
        &library_dir,
        None,
        None,
    )
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
/// changeset, `CloudSyncStorage` writes it to the shared cloud, B's cycle pulls the
/// changeset out of that same cloud and applies it to B's `Database`.
#[wasm_bindgen_test]
async fn row_syncs_from_one_database_to_another_through_the_engine() {
    console_error_panic_hook::set_once();

    let cloud = InMemoryCloudHome::new();
    assert!(cloud.is_empty(), "the shared cloud starts empty");

    let db_a = open_device("device-a");
    let db_b = open_device("device-b");

    // Device A writes a SHARED note (gate column `shared = 1`, so the gate keeps
    // it in the outgoing changeset) through `Database::call`, the same path a host
    // uses. The attached capture session records it.
    db_a.call(|conn| {
        conn.execute(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('note-1', 'hello from A', 'body', 1, ?1, '2026-01-01')",
            ["0000000001000-0000-device-a"],
        )
        .map(|_| ())
        .map_err(DbError::from)
    })
    .await
    .expect("device A insert");

    // Device A: one cycle pushes its changeset (and head) to the shared cloud.
    let storage_a = storage_for(&cloud);
    run_cycle(&storage_a, &db_a, "device-a").await;

    assert!(
        cloud.get("changes/device-a/1").is_some(),
        "device A's changeset must land in the shared cloud at seq 1",
    );
    assert!(
        cloud.get("heads/device-a.json").is_some(),
        "device A must publish its head",
    );

    // The row exists on A, not yet on B.
    assert!(has_note(&db_a, "note-1").await, "A holds its own row");
    assert!(
        !has_note(&db_b, "note-1").await,
        "B has not pulled yet, so it must not hold A's row",
    );

    // Device B: one cycle pulls A's changeset out of the shared cloud and applies
    // it. B sees A's head, fetches `changes/device-a/1`, verifies the signature,
    // and applies the captured INSERT.
    let storage_b = storage_for(&cloud);
    run_cycle(&storage_b, &db_b, "device-b").await;

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
