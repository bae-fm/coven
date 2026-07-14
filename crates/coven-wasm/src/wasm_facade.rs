//! The JS-callable browser facade: one object that assembles coven's whole
//! browser stack and exposes it to JavaScript.
//!
//! Everything below this layer is already in place — the OPFS-backed
//! [`Database`](crate::database::Database), the [`CloudCipher`] / [`BlobPathScheme`]
//! at-rest and path choices, the fetch-based [`S3WasmCloudHome`](crate::storage::cloud::s3_wasm::S3WasmCloudHome),
//! the [`CloudSyncStorage`] layer, and the event-loop [`WasmSyncRuntime`]. This
//! file is the single entry that wires them together for a web page:
//! [`CovenStore::open`] takes a config object, installs the browser storage VFS,
//! opens the database, builds the storage + sync runtime, and hands back a handle
//! the page drives through [`sql`](CovenStore::sql) /
//! [`sql_read`](CovenStore::sql_read) and [`start_sync`](CovenStore::start_sync) /
//! [`stop_sync`](CovenStore::stop_sync) / [`sync_now`](CovenStore::sync_now).
//!
//! ## Worker-only
//!
//! Every method here runs on the dedicated Worker that owns the database: the
//! OPFS sync access handles the VFS uses exist only off the main thread, and the
//! wasm [`Database`](crate::database::Database) is `!Send`. The page talks to this
//! object across the Worker boundary (e.g. via `wasm-bindgen`'s worker glue).

use std::cell::RefCell;
use std::collections::HashSet;

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
use crate::migration::Migration;
use crate::storage::cloud::s3_wasm::S3WasmCloudHome;
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::session::SyncedTable;
use crate::sync::wasm_runtime::{WasmSyncRuntime, WasmSyncSchedule};
use crate::wasm::install_browser_storage;
use crate::wasm_keystore::BrowserKeystore;

thread_local! {
    static OPEN_STORES: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}

/// The config object JavaScript passes to [`CovenStore::open`].
///
/// The S3 fields name the bucket the store lives in; `endpoint` is set for an
/// S3-compatible service (MinIO, Backblaze, R2, Google Cloud Storage's S3 API)
/// and left unset for AWS. `store_id` names the store (it feeds the stable
/// SQLite path that the browser VFS hashes into an OPFS storage filename, and
/// must be distinct per store on one origin). `storage` is
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
    store_id: String,
    storage: HomeStorage,
    #[serde(default)]
    encryption_key_hex: Option<String>,
    device_id: String,
    initialization: JsStoreInitialization,
}

#[derive(Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum JsStoreInitialization {
    CreateStore,
    OpenStore {
        genesis_hash: String,
        founder_pubkey: String,
    },
}

impl JsStoreInitialization {
    fn into_core(self) -> Result<crate::sync::cycle::StoreInitialization, String> {
        match self {
            Self::CreateStore => Ok(crate::sync::cycle::StoreInitialization::CreateStore),
            Self::OpenStore {
                genesis_hash,
                founder_pubkey,
            } => Ok(crate::sync::cycle::StoreInitialization::OpenStore {
                expected_genesis_hash: genesis_hash
                    .parse()
                    .map_err(|error| format!("invalid genesis_hash: {error}"))?,
                expected_founder: founder_pubkey,
            }),
        }
    }
}

#[derive(Deserialize)]
struct JsMigration {
    version: u32,
    name: String,
    sql: String,
}

#[derive(Deserialize)]
struct JsSyncedTable {
    name: String,
    #[serde(default)]
    gated_by: Option<String>,
    #[serde(default)]
    gated_by_descendants: bool,
    #[serde(default)]
    asset: bool,
}

/// The browser-facing coven handle: an open database plus the sync runtime that
/// keeps it converged with the cloud. Construct with [`open`](Self::open), then
/// drive app SQL through [`sql`](Self::sql) / [`sql_read`](Self::sql_read) and the
/// sync loop through [`start_sync`](Self::start_sync) and friends.
#[wasm_bindgen]
pub struct CovenStore {
    db: Database,
    runtime: WasmSyncRuntime,
    _open_guard: WasmStoreOpenGuard,
}

struct WasmStoreOpenGuard {
    store_id: String,
}

impl WasmStoreOpenGuard {
    fn acquire(store_id: &str) -> Result<Self, String> {
        OPEN_STORES.with(|stores| {
            let mut stores = stores.borrow_mut();
            if !stores.insert(store_id.to_string()) {
                return Err(format!("store {store_id} is already open in this worker"));
            }
            Ok(Self {
                store_id: store_id.to_string(),
            })
        })
    }
}

impl Drop for WasmStoreOpenGuard {
    fn drop(&mut self) {
        OPEN_STORES.with(|stores| {
            stores.borrow_mut().remove(&self.store_id);
        });
    }
}

#[wasm_bindgen]
impl CovenStore {
    /// Open a store against an S3 (or S3-compatible) bucket and return a handle
    /// to it.
    ///
    /// Assembles the whole browser stack from `config` (see [`OpenConfig`]):
    /// installs the OPFS storage VFS, opens the [`Database`] at the
    /// `store_id`-derived SQLite path that the browser VFS hashes into an OPFS
    /// storage name, builds the at-rest [`CloudCipher`] and the
    /// [`BlobPathScheme`], constructs the fetch-based [`S3WasmCloudHome`], wraps it in
    /// [`CloudSyncStorage`], and starts a [`WasmSyncRuntime`] over it. The sync loop
    /// is not running yet — call [`start_sync`](Self::start_sync).
    ///
    /// Every failure (bad config, storage install, database open, a missing or
    /// malformed encryption key) is returned as a `JsValue` string rather than
    /// silently defaulted, so the page sees exactly what went wrong.
    pub async fn open(
        config: JsValue,
        migrations: JsValue,
        synced_tables: JsValue,
    ) -> Result<CovenStore, JsValue> {
        // Surface a Rust panic as a console error with its message + stack, rather
        // than an opaque `unreachable` trap, for anyone running this in a browser.
        console_error_panic_hook::set_once();

        let config: OpenConfig = serde_wasm_bindgen::from_value(config)
            .map_err(|e| JsValue::from_str(&format!("invalid open config: {e}")))?;
        validate_store_id(&config.store_id)
            .map_err(|e| JsValue::from_str(&format!("invalid browser config: {e}")))?;
        let migrations = parse_migrations(migrations)
            .map_err(|e| JsValue::from_str(&format!("invalid migrations: {e}")))?;
        let synced_tables = parse_synced_tables(synced_tables)
            .map_err(|e| JsValue::from_str(&format!("invalid synced tables: {e}")))?;
        let initialization = config
            .initialization
            .into_core()
            .map_err(|e| JsValue::from_str(&format!("invalid initialization: {e}")))?;

        // Build the cipher first so a bad/missing encryption key fails before any
        // storage or database side effects.
        let cipher = build_cipher(config.storage, config.encryption_key_hex.as_deref())
            .map_err(|e| JsValue::from_str(&e))?;
        let home: Box<dyn CloudHome> = Box::new(S3WasmCloudHome::new(
            config.bucket,
            config.region,
            config.endpoint,
            config.access_key,
            config.secret_key,
            config.key_prefix,
        ));

        // Load (or, on first use, mint) this store's persistent signing identity
        // from the browser keystore, so a reopened tab keeps the identity other
        // members already trust rather than appearing as a new device each time.
        // Scoped to `store_id`: a second store opened from the same origin gets
        // its own independent identity, never this one.
        let user_keypair = BrowserKeystore::open()
            .await
            .map_err(|e| JsValue::from_str(&format!("open keystore: {e}")))?
            .get_or_create_user_keypair(&config.store_id)
            .await
            .map_err(|e| JsValue::from_str(&format!("load this store's identity: {e}")))?;

        Self::from_home(
            home,
            cipher,
            config.storage,
            &config.store_id,
            &config.store_id,
            config.device_id,
            migrations,
            synced_tables,
            user_keypair,
            initialization,
            production_schedule(),
        )
        .await
        .map_err(|e| JsValue::from_str(&e))
    }

    /// Run a write statement (or several, separated by `;`) against the database.
    ///
    /// This is the host's write path: the write runs inside a journaled
    /// transaction whose capture session records it into the pending-changeset
    /// journal, so [`start_sync`](Self::start_sync) publishes it. Synced rows must
    /// carry the `_updated_at` register stamp the synced-table contract requires;
    /// wasm hosts with an open store mint those stamps with [`stamp`](Self::stamp).
    pub fn sql(&self, sql: String) -> Result<(), JsValue> {
        // The wasm `Database::call` runs the closure synchronously on the borrowed
        // connection and returns an already-complete future, so `now_or_never`
        // drives it to its value without awaiting. It returns `None` only if a
        // wasm `Database` method ever gained a real suspension point — a bug to
        // surface, not to paper over.
        let tables = self.db.synced_tables().to_vec();
        self.db
            .call(move |conn| {
                Database::run_pending_journaled_transaction_on(conn, &tables, |tx| {
                    tx.execute_batch(&sql).map(|_| ()).map_err(DbError::from)
                })
            })
            .now_or_never()
            .expect("wasm Database::call returns a ready future")
            .map_err(|e| JsValue::from_str(&format!("sql failed: {e}")))
    }

    /// Run a pure read query and return its rows as a JSON value: an array of
    /// objects, one per row, keyed by column name. SQLite NULL maps to JSON
    /// `null`, integers and reals to JSON numbers, and text to JSON strings; a
    /// non-UTF-8 text or a BLOB column is an error rather than a silently
    /// corrupted string (see [`value_ref_to_json`]).
    #[wasm_bindgen(js_name = sqlRead)]
    pub async fn sql_read(&self, sql: String) -> Result<JsValue, JsValue> {
        let rows = self
            .db
            .call(move |conn| query_rows(conn, &sql))
            .await
            .map_err(|e| JsValue::from_str(&format!("sql_read failed: {e}")))?;
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

    /// Mint the next `_updated_at` register stamp for a synced-row write through
    /// this open store. This stamp uses the database's seeded clock, the same
    /// clock the sync runtime records pulled rows on.
    pub fn stamp(&self) -> String {
        self.db.stamper().stamp()
    }
}

impl CovenStore {
    /// Assemble a [`CovenStore`] over an already-built [`CloudHome`], the at-rest
    /// cipher, and the blob-path scheme.
    ///
    /// This is the seam [`open`](Self::open) and the headless facade test share:
    /// `open` builds an [`S3WasmCloudHome`] and passes it here; the test passes an
    /// in-memory home so the full Database + storage + runtime assembly is exercised
    /// without a live, CORS-configured S3 bucket. It installs the OPFS VFS, opens
    /// the database at the `database_id`-derived SQLite path that the browser VFS
    /// hashes into an OPFS storage name, and
    /// builds (but does not start) the [`WasmSyncRuntime`] on `schedule`.
    /// `open` passes [`production_schedule`]; the headless test passes a short one
    /// so it converges quickly.
    ///
    /// The caller supplies `user_keypair` — `open` loads the store's persistent
    /// identity from the [`BrowserKeystore`](crate::wasm_keystore::BrowserKeystore),
    /// while tests pass the same identity to every simulated device belonging to
    /// one store.
    pub(crate) async fn from_home(
        home: Box<dyn CloudHome>,
        cipher: CloudCipher,
        storage_mode: HomeStorage,
        store_id: &str,
        database_id: &str,
        device_id: String,
        migrations: Vec<Migration>,
        synced_tables: Vec<SyncedTable>,
        user_keypair: UserKeypair,
        initialization: crate::sync::cycle::StoreInitialization,
        schedule: WasmSyncSchedule,
    ) -> Result<CovenStore, String> {
        validate_store_id(store_id)?;
        validate_store_id(database_id)?;
        let open_guard = WasmStoreOpenGuard::acquire(database_id)?;

        install_browser_storage()
            .await
            .map_err(|e| format!("install browser storage: {e}"))?;

        // The browser VFS hashes this SQLite path into a flat OPFS filename
        // (`coven-<hash>.db`), so distinct store ids produce distinct storage
        // files on one origin.
        let path = format!("{database_id}.db");
        let (db, _stamper) = Database::open(
            std::path::Path::new(&path),
            synced_tables,
            // The browser facade does not yet expose a grace knob; it takes the
            // default convergence window.
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            device_id.clone(),
            &migrations,
        )
        .map_err(|e| format!("open database: {e}"))?;

        let storage = CloudSyncStorage::new(
            std::sync::Arc::from(home),
            cipher,
            BlobPathScheme::for_storage(storage_mode),
            store_id,
            user_keypair.clone(),
        )
        .with_copy_ids(std::sync::Arc::new(
            crate::storage::cloud::RandomCopyIdGenerator,
        ));
        let components = crate::sync::cycle::init_sync_over_storage(&db, storage, initialization)
            .await
            .map_err(|error| format!("initialize sync: {error}"))?;

        // This row-only facade schema has no blobs and creates no snapshot image,
        // so the runtime does not access this browser placeholder directory.
        let store_dir = StoreDir::new(std::path::Path::new("/coven-browser"));

        let runtime = WasmSyncRuntime::new(
            components,
            // `Clock` is a plain `Send + Sync` sync trait, so its handle is an
            // `Arc` (`ClockRef`) even on the single-threaded browser runtime.
            std::sync::Arc::new(SystemClock) as ClockRef,
            store_dir,
            // The demo schema declares no blob-bearing tables, so coven derives an
            // empty blob set. A real app declares its blob columns via
            // `SyncedTable::carries_blob` (see `crate::blob::decl::BlobDecls`).
            None,
            schedule,
        );

        Ok(CovenStore {
            db,
            runtime,
            _open_guard: open_guard,
        })
    }

    #[cfg(all(test, target_arch = "wasm32"))]
    pub(crate) fn runtime_hlc_is_database_hlc_for_test(&self) -> bool {
        self.runtime.hlc_is_for_test(&self.db.hlc())
    }

    #[cfg(all(test, target_arch = "wasm32"))]
    pub(crate) fn db_hlc_for_test(&self) -> std::sync::Arc<crate::sync::hlc::Hlc> {
        self.db.hlc()
    }

    #[cfg(all(test, target_arch = "wasm32"))]
    pub(crate) fn runtime_active_token_for_test(
        &self,
    ) -> Option<std::rc::Rc<std::cell::Cell<bool>>> {
        self.runtime.active_token_for_test()
    }

    #[cfg(all(test, target_arch = "wasm32"))]
    pub(crate) async fn protocol_genesis_hash_for_test(
        &self,
    ) -> crate::sync::store_commit::ObjectHash {
        self.db
            .get_protocol_state(crate::database::PROTOCOL_GENESIS_HASH_STATE_KEY)
            .await
            .expect("read protocol genesis state")
            .expect("initialized store has protocol genesis state")
            .parse()
            .expect("protocol genesis state is a valid object hash")
    }
}

fn validate_store_id(store_id: &str) -> Result<(), String> {
    crate::store_dir::validate_path_token(store_id).map_err(|e| format!("store_id {e}"))
}

/// The sync loop's real cadence: a 3 s startup grace so the loop does not race
/// page load, a 30 s steady interval between cycles, and a 300 s cap on the
/// exponential failure backoff. (A [`sync_now`](CovenStore::sync_now) wake runs
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
/// the store's data to the bucket in the clear). A browsable home takes no key.
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

fn parse_migrations(value: JsValue) -> Result<Vec<Migration>, String> {
    let defs: Vec<JsMigration> =
        serde_wasm_bindgen::from_value(value).map_err(|e| e.to_string())?;
    defs.into_iter()
        .map(|m| {
            if m.name.is_empty() {
                return Err(format!("migration {} has empty name", m.version));
            }
            if m.sql.is_empty() {
                return Err(format!(
                    "migration {} ({}) has empty SQL",
                    m.version, m.name
                ));
            }
            Ok(Migration::sql_owned(m.version, m.name, m.sql))
        })
        .collect()
}

fn parse_synced_tables(value: JsValue) -> Result<Vec<SyncedTable>, String> {
    let defs: Vec<JsSyncedTable> =
        serde_wasm_bindgen::from_value(value).map_err(|e| e.to_string())?;
    defs.into_iter()
        .map(|t| {
            if t.gated_by.is_some() && t.gated_by_descendants {
                return Err(format!(
                    "synced table {} cannot be both gated_by and gated_by_descendants",
                    t.name
                ));
            }
            let mut table = SyncedTable::new(t.name);
            if let Some(column) = t.gated_by {
                table = table.gated_by(column);
            }
            if t.gated_by_descendants {
                table = table.gated_by_descendants();
            }
            if t.asset {
                table = table.asset();
            }
            Ok(table)
        })
        .collect()
}

/// Run a `SELECT` and collect its rows as `serde_json` values, keyed by column
/// name, so the facade can hand them to JavaScript. Shared by
/// [`CovenStore::sql_read`] and the headless test.
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
/// this facade's `sql_read` is for the demo's text columns, and lossily
/// stringifying arbitrary bytes would corrupt them silently; a real app that
/// stores blobs adds an explicit (e.g. base64) encoding here.
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
                "column {column} is a BLOB; sql_read() renders only text/number columns as JSON"
            )))
        }
    })
}
