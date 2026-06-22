//! Headless wasm proof that the sync RUNTIME drives convergence on the browser
//! event loop — not a manual [`run_single_sync_cycle`] call.
//!
//! Two `:memory:` `Database`s stand in for two devices; each gets a
//! [`WasmSyncRuntime`] over its own `CloudSyncStorage` wrapping a clone of one
//! shared `InMemoryCloudHome`, so both read and write one cloud bucket. Both
//! runtimes `start()` and tick on `spawn_local` + gloo-timers with a short idle
//! interval. Device A writes a shared row; the test triggers both runtimes and
//! then waits (bounded) for the row to appear on device B. That the row crosses
//! without a single hand-called cycle is the proof: the event-loop-driven runtime,
//! not just the engine, moves the data.
//!
//! `:memory:` (not OPFS) because this exercises the runtime's scheduling, not
//! durability: the two DBs only need to be independent, which a fresh `:memory:`
//! connection already is. It still runs in a dedicated Worker to match the rest of
//! the wasm suite. Drive it with `wasm-pack test --headless --firefox`.
//!
//! [`run_single_sync_cycle`]: crate::sync::cycle::run_single_sync_cycle

use std::rc::Rc;
use std::sync::Arc;

use gloo_timers::future::TimeoutFuture;
use rusqlite::OptionalExtension;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

use crate::clock::{ClockRef, SystemClock};
use crate::database::{Database, DbError};
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::hlc::Hlc;
use crate::sync::test_helpers::{create_synced_schema, test_synced_tables, NoopBlobSource};
use crate::sync::wasm_runtime::{WasmSyncRuntime, WasmSyncSchedule};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Open a `Database` over a fresh `:memory:` connection with the synthetic synced
/// schema (`notes` gated by `shared`, plus FK children). Each call is an
/// independent in-memory database — the two devices share nothing locally, only
/// the cloud.
fn open_device(device_id: &str) -> Database {
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        device_id.to_string(),
        create_synced_schema,
    )
    .expect("open in-memory Database");
    db
}

/// Build a started [`WasmSyncRuntime`] for `device_id` over a clone of the shared
/// cloud. The schedule is fast (short startup grace and idle interval) so the test
/// converges quickly; everything else mirrors a real device's wiring.
fn start_runtime(device_id: &str, db: Database, cloud: &InMemoryCloudHome) -> WasmSyncRuntime {
    // One identity for both the storage (signs the head it writes) and the
    // runtime's cycle, as a real device has a single signing keypair.
    let keypair = UserKeypair::generate();
    let storage = CloudSyncStorage::new(
        Arc::new(cloud.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Hashed,
        keypair.clone(),
    );
    // The runtime shares the storage's cipher lock (the same instance the storage
    // seals/opens with), as the facade and native init do.
    let cipher = storage.shared_cipher();
    // A library dir that never touches disk: this runs the changeset path, whose
    // only fs touch is best-effort changeset staging (logs and continues on
    // failure). No blobs, no snapshot bytes are read back.
    let library_dir = LibraryDir::new(std::path::Path::new("/coven-wasm-runtime-test"));

    let runtime = WasmSyncRuntime::new(
        storage,
        "test-lib".to_string(),
        device_id.to_string(),
        Rc::new(Hlc::new(device_id.to_string())),
        cipher,
        db,
        keypair,
        Arc::new(SystemClock) as ClockRef,
        library_dir,
        Rc::new(NoopBlobSource),
        None,
        // Short cadence: a 10 ms startup grace and a 50 ms idle interval keep the
        // test fast while still exercising the timer-driven wait between cycles.
        WasmSyncSchedule {
            initial_delay_ms: 10,
            idle_interval_ms: 50,
            backoff_cap_secs: 1,
        },
    );
    runtime.start();
    runtime
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

/// Device A writes a shared row; both runtimes tick on the event loop; the row
/// converges to device B. No `run_single_sync_cycle` is called from the test — the
/// runtimes alone drive capture → push → pull → apply.
#[wasm_bindgen_test]
async fn runtime_drives_two_devices_to_convergence() {
    console_error_panic_hook::set_once();

    let cloud = InMemoryCloudHome::new();
    let db_a = open_device("device-a");
    let db_b = open_device("device-b");

    // Device A writes a SHARED note (gate column `shared = 1`, so the gate keeps it
    // in the outgoing changeset) through `Database::call`, the same path a host
    // uses. A's attached capture session records it; A's runtime will push it.
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

    // Start both runtimes; clone db_b so the test keeps a handle to read from while
    // the runtime owns its own clone (both share the one `:memory:` connection).
    let runtime_a = start_runtime("device-a", db_a, &cloud);
    let runtime_b = start_runtime("device-b", db_b.clone(), &cloud);

    // Nudge A to push now and B to pull now, so we do not wait out the initial
    // delay for the first cycles. The bounded wait below tolerates any ordering:
    // if B pulls before A has pushed, B's next idle tick pulls the row.
    runtime_a.trigger();
    runtime_b.trigger();

    // Bounded convergence wait: poll B for the row, sleeping a short interval
    // between checks, up to a few seconds total. Several idle ticks (50 ms each)
    // fit in this budget, so a healthy runtime converges well inside it; the bound
    // turns a stuck runtime into a clear failure instead of a hang.
    let mut converged = false;
    for _ in 0..100 {
        if has_note(&db_b, "note-1").await {
            converged = true;
            break;
        }
        TimeoutFuture::new(50).await;
    }

    runtime_a.stop();
    runtime_b.stop();

    assert!(
        converged,
        "device B never received device A's row within the wait budget — the wasm \
         sync runtime did not drive capture → push → pull → apply to convergence",
    );

    // The converged row carries A's content, not a placeholder: the runtime moved
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
        "the converged row must carry A's column values",
    );
}
