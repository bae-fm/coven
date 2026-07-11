//! Headless wasm proof that the facade ([`CovenStore`]) correctly assembles the
//! whole browser stack: two instances over one shared in-memory cloud converge a
//! row **through the public facade API** — `sql`, `sql_read`, `start_sync`,
//! `sync_now` — with no live S3.
//!
//! [`CovenStore::open`] builds an [`S3WasmCloudHome`] and hands it to the
//! internal [`from_home`](CovenStore::from_home) seam. A live S3 round-trip needs
//! a CORS-configured bucket, which is not available headlessly, so this test drives
//! the same seam with a shared [`InMemoryCloudHome`] instead. That exercises
//! everything `open` does except the S3 wire format itself (covered by
//! `s3_wasm`'s own tests): the OPFS database, the cipher + blob-path choices, the
//! [`CloudSyncStorage`](crate::sync::cloud_storage::CloudSyncStorage) layer, and
//! the event-loop [`WasmSyncRuntime`](crate::sync::wasm_runtime::WasmSyncRuntime).
//!
//! Tab A writes a note and triggers a sync; the test waits (bounded) for tab B's
//! `sql_read` to return the row. That the row crosses with the test only ever calling
//! the facade's own methods is the proof: the facade assembles a working store,
//! and two of them converge. Drive it with `wasm-pack test --headless --firefox`.
//!
//! [`S3WasmCloudHome`]: crate::storage::cloud::s3_wasm::S3WasmCloudHome

use gloo_timers::future::TimeoutFuture;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

use crate::migration::Migration;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::CloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher};
use crate::sync::hlc::Timestamp;
use crate::sync::session::SyncedTable;
use crate::sync::wasm_runtime::WasmSyncSchedule;
use crate::wasm_facade::CovenStore;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Build a started [`CovenStore`] for `device_id` over a clone of the shared
/// cloud, plaintext at rest with readable blob paths — the simplest home, and the
/// one the harness README recommends as a first config. Each `store_id` is
/// distinct so the two devices get independent SQLite paths, which the browser
/// VFS hashes into separate OPFS storage names; they converge only through the
/// cloud.
async fn open_store(store_id: &str, device_id: &str, cloud: &InMemoryCloudHome) -> CovenStore {
    let home: Box<dyn CloudHome> = Box::new(cloud.clone());
    let lib = CovenStore::from_home(
        home,
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        store_id,
        device_id.to_string(),
        demo_migrations(),
        demo_synced_tables(),
        // A fresh identity per device: `open` would load one shared identity from the
        // origin keystore, but these two simulated devices must sign as distinct
        // members to model two tabs/devices converging.
        crate::keys::UserKeypair::generate(),
        // Short cadence so the test converges quickly: a 10 ms startup grace and a
        // 50 ms idle interval, versus the facade's 3 s / 30 s production timings.
        WasmSyncSchedule {
            initial_delay_ms: 10,
            idle_interval_ms: 50,
            backoff_cap_secs: 1,
        },
    )
    .await
    .expect("assemble CovenStore over the shared in-memory cloud");
    lib.start_sync();
    lib
}

async fn assemble_store(
    store_id: &str,
    device_id: &str,
    cloud: &InMemoryCloudHome,
) -> Result<CovenStore, String> {
    CovenStore::from_home(
        Box::new(cloud.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        store_id,
        device_id.to_string(),
        demo_migrations(),
        demo_synced_tables(),
        crate::keys::UserKeypair::generate(),
        WasmSyncSchedule {
            initial_delay_ms: 10,
            idle_interval_ms: 50,
            backoff_cap_secs: 1,
        },
    )
    .await
}

fn demo_migrations() -> Vec<Migration> {
    vec![Migration::sql(1, DEMO_MIGRATION_NAME, DEMO_NOTES_SQL)]
}

fn demo_synced_tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new(DEMO_TABLE_NAME)]
}

const DEMO_MIGRATION_NAME: &str = "demo-schema";
const DEMO_TABLE_NAME: &str = "notes";
const DEMO_NOTES_SQL: &str = "CREATE TABLE IF NOT EXISTS notes (
    id TEXT PRIMARY KEY,
    body TEXT NOT NULL,
    _updated_at TEXT NOT NULL,
    created_at TEXT NOT NULL
) STRICT;";

fn to_js_value(value: serde_json::Value) -> JsValue {
    serde::Serialize::serialize(&value, &serde_wasm_bindgen::Serializer::json_compatible())
        .expect("serialize JS value")
}

fn public_open_config(store_id: &str, device_id: &str) -> JsValue {
    to_js_value(serde_json::json!({
        "bucket": "test-bucket",
        "region": "us-east-1",
        "access_key": "test-access-key",
        "secret_key": "test-secret-key",
        "store_id": store_id,
        "storage": "browsable",
        "device_id": device_id,
    }))
}

fn public_open_migrations() -> JsValue {
    to_js_value(serde_json::json!([
        {
            "version": 1,
            "name": DEMO_MIGRATION_NAME,
            "sql": DEMO_NOTES_SQL,
        }
    ]))
}

fn public_open_synced_tables() -> JsValue {
    to_js_value(serde_json::json!([
        { "name": DEMO_TABLE_NAME }
    ]))
}

async fn public_open(store_id: &str, device_id: &str) -> Result<CovenStore, JsValue> {
    CovenStore::open(
        public_open_config(store_id, device_id),
        public_open_migrations(),
        public_open_synced_tables(),
    )
    .await
}

fn js_error_string(value: &JsValue) -> String {
    value
        .as_string()
        .expect("CovenStore::open errors are strings")
}

async fn assert_public_open_rejects(
    store_id: &str,
    device_id: &str,
    expected: &[&str],
    label: &str,
) {
    let result = public_open(store_id, device_id).await;
    match result {
        Ok(lib) => {
            drop(lib);
            panic!("{label} must be rejected");
        }
        Err(error) => {
            let error = js_error_string(&error);
            for expected in expected {
                assert!(
                    error.contains(expected),
                    "error {error:?} contains {expected:?}",
                );
            }
        }
    }
}

/// Whether `lib`'s `notes` table holds a row with `id`, read through the facade's
/// public `sql_read` (which returns the rows as a JSON value the test parses
/// back).
async fn has_note(lib: &CovenStore, id: &str) -> bool {
    let rows_js = lib
        .sql_read(format!("SELECT id FROM notes WHERE id = '{id}'"))
        .await
        .expect("sql_read notes through the facade");
    let rows: Vec<serde_json::Value> =
        serde_wasm_bindgen::from_value(rows_js).expect("sql_read returns a JSON array of rows");
    !rows.is_empty()
}

/// Two stores over one cloud, both syncing: tab A writes a note and triggers a
/// sync, and the note converges to tab B — observed entirely through the facade's
/// public API. No `run_single_sync_cycle`, no direct `Database`: only `from_home`
/// (the seam `open` uses), `sql`, `sql_read`, `start_sync`, and `sync_now`.
#[wasm_bindgen_test]
async fn two_stores_converge_a_note_through_the_facade() {
    console_error_panic_hook::set_once();

    // A unique suffix per run so the OPFS databases are fresh (OPFS outlives the
    // test process, and each store_id feeds a persistent SQLite path that is
    // hashed into an OPFS storage name).
    let run = uuid::Uuid::new_v4().simple().to_string();
    let cloud = InMemoryCloudHome::new();

    let lib_a = open_store(&format!("tab-a-{run}"), "device-a", &cloud).await;
    let lib_b = open_store(&format!("tab-b-{run}"), "device-b", &cloud).await;

    // Tab A writes a note through `sql`. The synced `notes` row carries the
    // `_updated_at` register the contract requires (here a literal HLC-shaped stamp
    // ending in the device id; a real app mints it with the `UpdatedAtStamper`).
    lib_a
        .sql(
            "INSERT INTO notes (id, body, _updated_at, created_at) \
             VALUES ('note-1', 'hello from tab A', '0000000001000-0000-device-a', '2026-01-01')"
                .to_string(),
        )
        .expect("tab A insert through the facade");

    assert!(lib_a.is_syncing(), "tab A's sync loop is running");
    assert!(lib_b.is_syncing(), "tab B's sync loop is running");
    assert!(has_note(&lib_a, "note-1").await, "tab A holds its own note");

    // Bounded convergence wait: each iteration nudges both tabs to sync now and
    // then checks B for the row, sleeping a short interval between checks. Nudging
    // each pass (the same `sync_now` a real app calls on demand) keeps the test
    // from depending on idle-tick timing, which a headless worker throttles. A
    // healthy pair converges well inside the budget; the bound turns a stuck
    // runtime into a clear failure rather than a hang.
    let mut converged = false;
    for _ in 0..200 {
        lib_a.sync_now();
        lib_b.sync_now();
        if has_note(&lib_b, "note-1").await {
            converged = true;
            break;
        }
        TimeoutFuture::new(50).await;
    }

    lib_a.stop_sync();
    lib_b.stop_sync();

    assert!(
        converged,
        "tab B never received tab A's note through the facade — the facade did not \
         assemble a working Database + storage + sync runtime, or two instances do \
         not converge over the shared cloud",
    );

    // The converged row carries A's content, read back through the facade's
    // sql_read.
    let rows_js = lib_b
        .sql_read("SELECT body FROM notes WHERE id = 'note-1'".to_string())
        .await
        .expect("read body on tab B");
    let rows: Vec<serde_json::Value> =
        serde_wasm_bindgen::from_value(rows_js).expect("sql_read returns a JSON array of rows");
    assert_eq!(
        rows.first()
            .and_then(|r| r.get("body"))
            .and_then(|b| b.as_str()),
        Some("hello from tab A"),
        "the converged row must carry tab A's column values",
    );
}

#[wasm_bindgen_test]
async fn second_open_of_one_store_id_is_refused_until_the_first_handle_drops() {
    console_error_panic_hook::set_once();

    let run = uuid::Uuid::new_v4().simple().to_string();
    let store_id = format!("double-open-{run}");
    let cloud = InMemoryCloudHome::new();

    let first = assemble_store(&store_id, "device-a", &cloud)
        .await
        .expect("first open succeeds");
    let second = assemble_store(&store_id, "device-b", &cloud).await;

    match second {
        Ok(_) => panic!("second open of the same store id must fail"),
        Err(error) => assert!(
            error.contains("already open"),
            "second open error names the open store: {error}",
        ),
    }

    drop(first);

    assemble_store(&store_id, "device-c", &cloud)
        .await
        .expect("open succeeds after the first handle drops");
}

#[wasm_bindgen_test]
async fn open_rejects_empty_store_id() {
    console_error_panic_hook::set_once();

    assert_public_open_rejects("", "device-a", &["store_id", "empty"], "empty store_id").await;
}

#[wasm_bindgen_test]
async fn open_rejects_empty_device_id() {
    console_error_panic_hook::set_once();

    let run = uuid::Uuid::new_v4().simple().to_string();
    assert_public_open_rejects(
        &format!("empty-device-{run}"),
        "",
        &["device_id", "empty"],
        "empty device_id",
    )
    .await;
}

#[wasm_bindgen_test]
async fn from_home_rejects_empty_store_id() {
    console_error_panic_hook::set_once();

    let cloud = InMemoryCloudHome::new();
    let result = assemble_store("", "device-a", &cloud).await;
    match result {
        Ok(lib) => {
            drop(lib);
            panic!("empty store_id must be rejected");
        }
        Err(error) => {
            assert!(
                error.contains("store_id") && error.contains("empty"),
                "error names the empty store id: {error}",
            );
        }
    }
}

#[wasm_bindgen_test]
async fn runtime_and_open_share_one_clock() {
    console_error_panic_hook::set_once();

    let run = uuid::Uuid::new_v4().simple().to_string();
    let cloud = InMemoryCloudHome::new();
    let lib = assemble_store(&format!("shared-clock-{run}"), "device-a", &cloud)
        .await
        .expect("assemble store");

    assert!(
        lib.runtime_hlc_is_database_hlc_for_test(),
        "runtime must hold the database HLC instance opened and seeded by Database::open",
    );
}

#[wasm_bindgen_test]
async fn store_stamp_uses_pull_advanced_clock() {
    console_error_panic_hook::set_once();

    let run = uuid::Uuid::new_v4().simple().to_string();
    let cloud = InMemoryCloudHome::new();
    let lib = assemble_store(&format!("pull-advanced-stamp-{run}"), "device-a", &cloud)
        .await
        .expect("assemble store");

    let pulled = Timestamp::new(9_000_000_000_000, 7, "device-b".to_string());
    lib.db_hlc_for_test().advance_past(&pulled);

    let stamp = lib.stamp();
    assert!(
        stamp > pulled.to_string(),
        "store stamp {stamp} must sort after pulled stamp {pulled}",
    );
}

#[wasm_bindgen_test]
async fn free_without_stop_sync_halts_syncing() {
    console_error_panic_hook::set_once();

    let run = uuid::Uuid::new_v4().simple().to_string();
    let cloud = InMemoryCloudHome::new();
    let lib = assemble_store(&format!("free-stops-sync-{run}"), "device-a", &cloud)
        .await
        .expect("assemble store");

    lib.start_sync();
    let token = lib
        .runtime_active_token_for_test()
        .expect("store runtime has active token after start_sync");
    assert!(token.get(), "start_sync marks the loop token running");

    drop(lib);

    assert!(!token.get(), "dropping the store stops the sync loop token");
}
