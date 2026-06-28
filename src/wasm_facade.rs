//! The JS-callable browser facade: one object that assembles coven's whole
//! browser stack and exposes it to JavaScript.
//!
//! Everything below this layer is already in place — the OPFS-backed
//! [`Database`](crate::database::Database), the [`CloudCipher`] / [`BlobPathScheme`]
//! at-rest and path choices, the fetch-based [`S3WasmCloudHome`](crate::storage::cloud::s3_wasm::S3WasmCloudHome),
//! the [`CloudSyncStorage`] layer, and the event-loop [`WasmSyncRuntime`]. This
//! file is the single entry that wires them together for a web page:
//! [`CovenLibrary::open`] takes a config object, installs the browser storage VFS,
//! opens the database, builds the storage + sync runtime, and hands back a handle
//! the page drives through [`exec`](CovenLibrary::exec) /
//! [`query`](CovenLibrary::query) and [`start_sync`](CovenLibrary::start_sync) /
//! [`stop_sync`](CovenLibrary::stop_sync) / [`sync_now`](CovenLibrary::sync_now).
//!
//! ## Where the app's schema goes
//!
//! coven owns the sync layer, not the domain. A real app declares its own SQLite
//! schema and its own [`SyncedTable`] set and passes them in here. This facade
//! hard-codes a small `notes` schema purely to demonstrate row sync end to end;
//! see [`demo_schema`] and [`demo_synced_tables`]. To build a real browser app on
//! coven, replace those two functions with the app's schema and synced set — the
//! rest of the assembly is unchanged.
//!
//! ## Worker-only
//!
//! Every method here runs on the dedicated Worker that owns the database: the
//! OPFS sync access handles the VFS uses exist only off the main thread, and the
//! wasm [`Database`](crate::database::Database) is `!Send`. The page talks to this
//! object across the Worker boundary (e.g. via `wasm-bindgen`'s worker glue).

use std::rc::Rc;

use futures_util::FutureExt;
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

use crate::clock::{ClockRef, SystemClock};
use crate::config::HomeStorage;
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::s3_wasm::S3WasmCloudHome;
use crate::storage::cloud::CloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::hlc::Hlc;
use crate::sync::session::SyncedTable;
use crate::sync::wasm_runtime::{WasmSyncRuntime, WasmSyncSchedule};
use crate::wasm::install_browser_storage;
use crate::wasm_keystore::BrowserKeystore;

/// The config object JavaScript passes to [`CovenLibrary::open`].
///
/// The S3 fields name the bucket the library lives in; `endpoint` is set for an
/// S3-compatible service (MinIO, Backblaze, R2, Google Cloud Storage's S3 API)
/// and left unset for AWS. `library_id` names the library (it derives the OPFS
/// filename and must be distinct per library on one origin). `storage` is
/// `"opaque"` (end-to-end-encrypted with obfuscated, content-addressed blob keys
/// — then `encryption_key_hex`, 64 hex chars = a 32-byte key, is required) or
/// `"browsable"` (a plaintext bucket at readable blob paths). `device_id`
/// identifies this writer in the sync protocol (each tab/device must use a
/// distinct one).
#[derive(Deserialize)]
struct OpenConfig {
    bucket: String,
    region: String,
    #[serde(default)]
    endpoint: Option<String>,
    access_key: String,
    secret_key: String,
    #[serde(default)]
    key_prefix: Option<String>,
    library_id: String,
    storage: HomeStorage,
    #[serde(default)]
    encryption_key_hex: Option<String>,
    device_id: String,
}

/// The browser-facing coven handle: an open database plus the sync runtime that
/// keeps it converged with the cloud. Construct with [`open`](Self::open), then
/// drive app SQL through [`exec`](Self::exec) / [`query`](Self::query) and the
/// sync loop through [`start_sync`](Self::start_sync) and friends.
#[wasm_bindgen]
pub struct CovenLibrary {
    db: Database,
    runtime: WasmSyncRuntime,
}

#[wasm_bindgen]
impl CovenLibrary {
    /// Open a library against an S3 (or S3-compatible) bucket and return a handle
    /// to it.
    ///
    /// Assembles the whole browser stack from `config` (see [`OpenConfig`]):
    /// installs the OPFS storage VFS, opens the [`Database`] at an OPFS file
    /// derived from `library_id`, builds the at-rest [`CloudCipher`] and the
    /// [`BlobPathScheme`], constructs the fetch-based [`S3WasmCloudHome`], wraps it
    /// in [`CloudSyncStorage`], and starts a [`WasmSyncRuntime`] over it. The sync
    /// loop is not running yet — call [`start_sync`](Self::start_sync).
    ///
    /// Every failure (bad config, storage install, database open, a missing or
    /// malformed encryption key) is returned as a `JsValue` string rather than
    /// silently defaulted, so the page sees exactly what went wrong.
    pub async fn open(config: JsValue) -> Result<CovenLibrary, JsValue> {
        // Surface a Rust panic as a console error with its message + stack, rather
        // than an opaque `unreachable` trap, for anyone running this in a browser.
        console_error_panic_hook::set_once();

        let config: OpenConfig = serde_wasm_bindgen::from_value(config)
            .map_err(|e| JsValue::from_str(&format!("invalid open config: {e}")))?;

        // Build the cipher first so a bad/missing encryption key fails before any
        // storage or database side effects.
        let cipher = build_cipher(config.storage, config.encryption_key_hex.as_deref())
            .map_err(|e| JsValue::from_str(&e))?;
        let blob_paths = BlobPathScheme::for_storage(config.storage);

        let home: Box<dyn CloudHome> = Box::new(S3WasmCloudHome::new(
            config.bucket,
            config.region,
            config.endpoint,
            config.access_key,
            config.secret_key,
            config.key_prefix,
        ));

        // Load (or, on first use, mint) this device's persistent signing identity
        // from the browser keystore, so a reopened tab keeps the identity other
        // members already trust rather than appearing as a new device each time.
        let user_keypair = BrowserKeystore::open()
            .await
            .map_err(|e| JsValue::from_str(&format!("open keystore: {e}")))?
            .get_or_create_user_keypair()
            .await
            .map_err(|e| JsValue::from_str(&format!("load device identity: {e}")))?;

        Self::from_home(
            home,
            cipher,
            blob_paths,
            &config.library_id,
            config.device_id,
            user_keypair,
            production_schedule(),
        )
        .await
        .map_err(|e| JsValue::from_str(&e))
    }

    /// Run a write statement (or several, separated by `;`) against the database.
    ///
    /// This is the host's write path: the attached capture session records these
    /// writes into the outgoing changeset, so [`start_sync`](Self::start_sync)
    /// publishes them. Synced rows must carry the `_updated_at` register stamp the
    /// synced-table contract requires; the demo schema's `notes` table uses
    /// SQLite defaults for the facade's demo writes, while native hosts mint
    /// stamps with [`crate::SqlContext::stamp`].
    pub fn exec(&self, sql: String) -> Result<(), JsValue> {
        // The wasm `Database::call` runs the closure synchronously on the borrowed
        // connection and returns an already-complete future, so `now_or_never`
        // drives it to its value without awaiting. It returns `None` only if a
        // wasm `Database` method ever gained a real suspension point — a bug to
        // surface, not to paper over.
        self.db
            .call(move |conn| conn.execute_batch(&sql).map_err(DbError::from))
            .now_or_never()
            .expect("wasm Database::call returns a ready future")
            .map_err(|e| JsValue::from_str(&format!("exec failed: {e}")))
    }

    /// Run a read query and return its rows as a JSON value: an array of objects,
    /// one per row, keyed by column name. SQLite NULL maps to JSON `null`, integers
    /// and reals to JSON numbers, and text to JSON strings; a non-UTF-8 text or a
    /// BLOB column is an error rather than a silently corrupted string (see
    /// [`value_ref_to_json`]).
    pub async fn query(&self, sql: String) -> Result<JsValue, JsValue> {
        let rows = self
            .db
            .call(move |conn| query_rows(conn, &sql))
            .await
            .map_err(|e| JsValue::from_str(&format!("query failed: {e}")))?;
        // `json_compatible` serializes each row's map as a plain JS object, so the
        // page can read `row.body`; the default serializer would emit JS `Map`s,
        // which have no property access.
        rows.serialize(&serde_wasm_bindgen::Serializer::json_compatible())
            .map_err(|e| JsValue::from_str(&format!("serialize rows: {e}")))
    }

    /// Start the background sync loop. No-op if already running. The loop runs an
    /// initial cycle after a short startup grace, then on a steady cadence, with a
    /// [`sync_now`](Self::sync_now) wake in between.
    pub fn start_sync(&self) {
        self.runtime.start();
    }

    /// Stop the background sync loop after the in-flight cycle. Idempotent.
    pub fn stop_sync(&self) {
        self.runtime.stop();
    }

    /// Wake the sync loop to run a cycle immediately (push local writes / pull
    /// remote ones now, rather than at the next idle tick). Collapses with any
    /// pending wake; harmless if the loop is stopped.
    pub fn sync_now(&self) {
        self.runtime.sync_now();
    }

    /// Whether the sync loop is running.
    pub fn is_syncing(&self) -> bool {
        self.runtime.is_running()
    }
}

impl CovenLibrary {
    /// Assemble a [`CovenLibrary`] over an already-built [`CloudHome`], the at-rest
    /// cipher, and the blob-path scheme.
    ///
    /// This is the seam [`open`](Self::open) and the headless facade test share:
    /// `open` builds an [`S3WasmCloudHome`] and passes it here; the test passes an
    /// in-memory home so the full Database + storage + runtime assembly is exercised
    /// without a live, CORS-configured S3 bucket. It installs the OPFS VFS, opens
    /// the database at the `library_id`-derived path with the demo schema + synced
    /// set, and builds (but does not start) the [`WasmSyncRuntime`] on `schedule`.
    /// `open` passes [`production_schedule`]; the headless test passes a short one
    /// so it converges quickly.
    ///
    /// The caller supplies `user_keypair` — `open` loads the device's persistent
    /// identity from the [`BrowserKeystore`](crate::wasm_keystore::BrowserKeystore),
    /// while the test passes a fresh one per device so two simulated devices have
    /// distinct identities (a single origin keystore would hand both the same one).
    pub(crate) async fn from_home(
        home: Box<dyn CloudHome>,
        cipher: CloudCipher,
        blob_paths: BlobPathScheme,
        library_id: &str,
        device_id: String,
        user_keypair: UserKeypair,
        schedule: WasmSyncSchedule,
    ) -> Result<CovenLibrary, String> {
        install_browser_storage()
            .await
            .map_err(|e| format!("install browser storage: {e}"))?;

        // The OPFS VFS keys files by name, so the library_id is the filename. A
        // page that opens two libraries on one origin must give them distinct ids.
        let path = format!("{library_id}.db");
        let (db, _stamper) = Database::open(
            std::path::Path::new(&path),
            demo_synced_tables(),
            device_id.clone(),
            demo_schema,
        )
        .map_err(|e| format!("open database: {e}"))?;

        let storage = CloudSyncStorage::new(
            std::sync::Arc::from(home),
            cipher,
            blob_paths,
            user_keypair.clone(),
        );
        // The cycle and the storage must seal/open under the *same* cipher lock so a
        // key rotation through one is seen by the other; the storage owns it, so the
        // runtime borrows that one rather than a separate copy.
        let cipher = storage.shared_cipher();

        // A library dir that never touches disk: the browser has none. The
        // changeset path's only fs touch is best-effort changeset staging, which
        // logs and continues on failure. This demo syncs rows only — its notes
        // schema carries no blobs — so the dir is never read or written.
        let library_dir = LibraryDir::new(std::path::Path::new("/coven-browser"));

        let runtime = WasmSyncRuntime::new(
            storage,
            library_id.to_string(),
            device_id.clone(),
            Rc::new(Hlc::new(device_id)),
            cipher,
            db.clone(),
            // The device's signing identity. `open` loads it from the persistent
            // browser keystore so a reopened tab keeps the identity other members
            // trust; the headless test passes a fresh one per device.
            user_keypair,
            // `Clock` is a plain `Send + Sync` sync trait, so its handle is an
            // `Arc` (`ClockRef`) even on the single-threaded browser runtime.
            std::sync::Arc::new(SystemClock) as ClockRef,
            library_dir,
            // The demo schema declares no blob-bearing tables, so coven derives an
            // empty blob set. A real app declares its blob columns via
            // `SyncedTable::carries_blob` (see `crate::blob::decl::BlobDecls`).
            None,
            schedule,
        );

        Ok(CovenLibrary { db, runtime })
    }
}

/// The sync loop's real cadence: a 3 s startup grace so the loop does not race
/// page load, a 30 s steady interval between cycles, and a 300 s cap on the
/// exponential failure backoff. (A [`sync_now`](CovenLibrary::sync_now) wake runs
/// a cycle without waiting out the interval.)
fn production_schedule() -> WasmSyncSchedule {
    WasmSyncSchedule {
        initial_delay_ms: 3_000,
        idle_interval_ms: 30_000,
        backoff_cap_secs: 300,
    }
}

/// Build the at-rest [`CloudCipher`] from the home's [`HomeStorage`].
///
/// An opaque home requires a 64-hex-char (32-byte) key; its absence or a parse
/// failure is an error, never a silent fall back to plaintext (which would write
/// the library's data to the bucket in the clear). A browsable home takes no key.
fn build_cipher(storage: HomeStorage, key_hex: Option<&str>) -> Result<CloudCipher, String> {
    if storage.is_opaque() {
        let hex = key_hex.ok_or("opaque home requires `encryption_key_hex`")?;
        let service =
            EncryptionService::new(hex).map_err(|e| format!("invalid encryption_key_hex: {e}"))?;
        Ok(CloudCipher::Encrypted(service))
    } else {
        Ok(CloudCipher::Plaintext)
    }
}

/// The demo app's SQLite schema. **A real app supplies its own here** — coven owns
/// the sync layer, not the domain. This `notes` table is the minimum that
/// demonstrates row sync: a text primary key, a body, and the `_updated_at`
/// register the synced-table contract requires (`created_at` is ordinary app
/// data). `IF NOT EXISTS` because coven re-runs the migration on every open,
/// including a reopen that already has the table in OPFS.
fn demo_schema(conn: &Connection) -> Result<(), DbError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS notes (
            id TEXT PRIMARY KEY,
            body TEXT NOT NULL,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        );",
    )
    .map_err(DbError::from)
}

/// The demo app's synced-table set. **A real app supplies its own here.** One
/// unconditionally-synced table (`notes`) is enough to demonstrate convergence;
/// coven appends its own reserved tables (e.g. `item_keys`) on top.
fn demo_synced_tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes")]
}

/// Run a `SELECT` and collect its rows as `serde_json` values, keyed by column
/// name, so the facade can hand them to JavaScript. Shared by [`CovenLibrary::query`]
/// and the headless test.
fn query_rows(conn: &Connection, sql: &str) -> Result<Vec<serde_json::Value>, DbError> {
    let mut stmt = conn.prepare(sql).map_err(DbError::from)?;
    let column_count = stmt.column_count();
    let column_names: Vec<String> = stmt
        .column_names()
        .into_iter()
        .map(str::to_string)
        .collect();

    let mut rows = stmt.query([]).map_err(DbError::from)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(DbError::from)? {
        let mut obj = serde_json::Map::with_capacity(column_count);
        for (i, name) in column_names.iter().enumerate() {
            obj.insert(
                name.clone(),
                value_ref_to_json(name, row.get_ref(i).map_err(DbError::from)?)?,
            );
        }
        out.push(serde_json::Value::Object(obj));
    }
    Ok(out)
}

/// Render one column value as JSON. SQLite's storage classes map directly: NULL →
/// null, INTEGER/REAL → number, TEXT → string. A BLOB is surfaced as an error —
/// this facade's `query` is for the demo's text columns, and lossily stringifying
/// arbitrary bytes would corrupt them silently; a real app that stores blobs adds
/// an explicit (e.g. base64) encoding here.
fn value_ref_to_json(column: &str, value: ValueRef<'_>) -> Result<serde_json::Value, DbError> {
    Ok(match value {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(i) => serde_json::Value::from(i),
        ValueRef::Real(f) => serde_json::Value::from(f),
        ValueRef::Text(bytes) => serde_json::Value::from(
            std::str::from_utf8(bytes)
                .map_err(|e| DbError(format!("column {column} is not valid UTF-8 text: {e}")))?
                .to_owned(),
        ),
        ValueRef::Blob(_) => {
            return Err(DbError(format!(
                "column {column} is a BLOB; query() renders only text/number columns as JSON"
            )))
        }
    })
}
