//! The owned SQLite connection.
//!
//! coven owns one `rusqlite::Connection` together with a persistent session
//! attached to the synced tables. Every database access — the host's app SQL,
//! coven's bookkeeping, changeset capture and apply — runs against that one
//! connection, so access is serialized and the session sees every write.
//!
//! Native hosts open coven with [`crate::Coven::builder`] and run app SQL through
//! [`crate::CovenHandle::sql`] or [`crate::CovenHandle::write`]. Platform crates
//! choose how a SQLite connection is opened and held.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};
use tracing::error;

use crate::db::{ExternalBlob, OutboxEntry, OutboxOperation, MIGRATION_SQL};
use crate::migration::{run_migrations, Migration};
use crate::sync::hlc::{Hlc, Timestamp, UpdatedAtStamper, HIGHWATER_STATE_KEY};
use crate::sync::session::SyncedTable;

/// An error from the owned database.
#[derive(Debug, thiserror::Error)]
#[error("database error: {0}")]
pub struct DbError(pub String);

impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        DbError(e.to_string())
    }
}

#[derive(Clone)]
struct DatabaseState {
    hlc: Arc<Hlc>,
    synced_tables: Arc<Vec<SyncedTable>>,
    schema_version: u32,
}

/// The SQLite connection plus the capture state that must stay beside it.
///
/// `Session<'conn>` borrows the `Connection`; its `'conn` is a borrow-checker
/// fiction over a raw `sqlite3_session*` that internally holds the connection's
/// `sqlite3*`. Keeping both in one struct, with the session declared before the
/// connection, means the session is dropped first (Rust drops fields
/// top-to-bottom) and the connection it points at outlives it. The lifetime is
/// erased to `'static` at construction ([`DatabaseCore::open`]); that erasure is
/// sound precisely because of this co-ownership and drop order.
///
/// `session` is `Some` for the connection's whole life, except a transient window
/// inside [`DatabaseCore::reset_session`] where it is dropped and immediately
/// recreated to reset the recorded batch (SQLite cannot clear a session in
/// place). Capture is never suspended across an `await`: an apply disables
/// recording on the live session rather than detaching it. `None` otherwise means
/// a prior attach failed and is reported loudly, not relied upon.
struct DatabaseCore {
    session: Option<rusqlite::session::Session<'static>>,
    conn: Connection,
    hlc: Arc<Hlc>,
    synced_tables: Arc<Vec<SyncedTable>>,
    schema_version: u32,
}

// Native constructs the core before spawning the actor, then moves the whole
// owned connection/session pair into that thread and never shares it. The erased
// session lifetime is valid because `DatabaseCore` drops the session before the
// connection, and no table filter closure is installed.
#[cfg(not(target_arch = "wasm32"))]
unsafe impl Send for DatabaseCore {}

impl DatabaseCore {
    fn open(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
    ) -> Result<(Self, DatabaseState, UpdatedAtStamper), DbError> {
        let conn = open_connection(path)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::from)?;

        // coven's bookkeeping tables and the coven-owned `item_keys` synced table
        // first (declarative, idempotent, run every open), then the host's synced
        // schema ladder over `PRAGMA user_version`. The two are kept separate by
        // sync-visibility: coven's bookkeeping rows are stripped on snapshot, so a
        // versioned ledger can't track them; the host's synced ladder version rides
        // inside the snapshot's DB header.
        conn.execute_batch(MIGRATION_SQL).map_err(DbError::from)?;
        migrate_bookkeeping_schema(&conn)?;
        let schema_version = run_migrations(&conn, migrations)?;

        // `item_keys` is coven-owned but is library-global content every member
        // needs, so coven — not the host — declares it synced. Injecting it here
        // is the single point that puts it on every path that reads the set: the
        // capture session attaches it, the snapshot preserves it, and the gate
        // and apply operate over it. The host never sees or declares it. A
        // coven-reserved table name the host cannot declare, so this appends
        // unconditionally.
        let mut synced_tables = synced_tables;
        synced_tables.push(SyncedTable::new(crate::db::ITEM_KEYS_TABLE));

        // Seed the register clock so a restart cannot mint a stamp behind a value
        // already on disk. Floor = max(persisted high-water, max synced-row
        // `_updated_at`).
        let persisted = conn
            .query_row(
                "SELECT value FROM sync_state WHERE key = ?1",
                [HIGHWATER_STATE_KEY],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?;
        seed_from(&hlc, persisted, "HLC high-water mark in sync_state")?;
        let on_disk = scan_max_updated_at(&conn, &synced_tables)?;
        seed_from(&hlc, on_disk, "`_updated_at` in synced tables")?;

        let stamper = UpdatedAtStamper::new(hlc.clone());
        let synced_tables = Arc::new(synced_tables);
        let session = Some(attach_erased_session(&conn, &synced_tables)?);
        let core = DatabaseCore {
            session,
            conn,
            hlc,
            synced_tables,
            schema_version,
        };
        let state = core.state();

        Ok((core, state, stamper))
    }

    fn state(&self) -> DatabaseState {
        DatabaseState {
            hlc: self.hlc.clone(),
            synced_tables: self.synced_tables.clone(),
            schema_version: self.schema_version,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn connection(&self) -> &Connection {
        &self.conn
    }

    #[cfg(target_arch = "wasm32")]
    fn with_connection<R>(
        &self,
        f: impl FnOnce(&Connection) -> Result<R, DbError>,
    ) -> Result<R, DbError> {
        f(&self.conn)
    }

    fn capture_changeset(&mut self) -> Result<Vec<u8>, DbError> {
        capture_changeset(self.session.as_mut())
    }

    fn with_capture_disabled<R>(&mut self, f: impl FnOnce(&Connection) -> R) -> R {
        let DatabaseCore { session, conn, .. } = self;
        if let Some(s) = session.as_mut() {
            s.set_enabled(false);
        } else {
            error!("capture session absent at apply; applying with capture off (already not recording)");
        }
        let _reenable = CaptureReenable { session };
        f(conn)
    }

    fn reset_session(&mut self) {
        self.session = None;
        match attach_erased_session(&self.conn, &self.synced_tables) {
            Ok(s) => self.session = Some(s),
            Err(e) => error!("failed to recreate capture session after changeset capture: {e}"),
        }
    }
}

struct CaptureReenable<'a> {
    session: &'a mut Option<rusqlite::session::Session<'static>>,
}

impl Drop for CaptureReenable<'_> {
    fn drop(&mut self) {
        if let Some(s) = self.session.as_mut() {
            s.set_enabled(true);
        }
    }
}

/// A handle to the owned database. Cloneable; every clone serializes access to
/// the same connection and capture session.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct Database {
    core: Arc<tokio::sync::Mutex<DatabaseCore>>,
    state: DatabaseState,
}

/// A handle to the owned database. Cloneable; every clone shares the one
/// connection and capture session through the same `Rc`.
///
/// The browser is single-threaded (the connection lives on one Worker), so this
/// holds the core directly behind `Rc<RefCell<…>>` instead of talking to a
/// thread.
#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
pub struct Database {
    core: std::rc::Rc<std::cell::RefCell<DatabaseCore>>,
    state: DatabaseState,
}

/// Erase a capture session's connection-borrow lifetime to `'static`.
///
/// The lifetime is `PhantomData` over a raw pointer; the cast changes no bytes.
/// The caller guarantees the borrowed connection outlives the returned session
/// (here, by co-owning both in [`DatabaseCore`] and dropping the session first).
fn erase_session_lifetime(
    session: rusqlite::session::Session<'_>,
) -> rusqlite::session::Session<'static> {
    // Same type but for the lifetime parameter, which is pure `PhantomData`.
    unsafe {
        std::mem::transmute::<rusqlite::session::Session<'_>, rusqlite::session::Session<'static>>(
            session,
        )
    }
}

/// Attach a capture session and erase its connection-borrow lifetime to `'static`
/// for storage in [`DatabaseCore`]. The caller must keep `conn` alive at least as
/// long as the returned session (see [`erase_session_lifetime`]).
fn attach_erased_session(
    conn: &Connection,
    synced_tables: &[SyncedTable],
) -> Result<rusqlite::session::Session<'static>, DbError> {
    Ok(erase_session_lifetime(attach_session(conn, synced_tables)?))
}

impl Database {
    /// Open and own the connection at `path`.
    ///
    /// Runs coven's bookkeeping migration, then the host's synced-schema migration
    /// ladder (`migrations`) over `PRAGMA user_version`, seeds the register clock
    /// off the on-disk rows, and attaches the capture session to `synced_tables`.
    /// Returns the handle plus the non-optional `_updated_at` stamper the host
    /// binds into every synced-row write.
    ///
    pub fn open(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        device_id: String,
        migrations: &[Migration],
    ) -> Result<(Database, UpdatedAtStamper), DbError> {
        Self::open_with_hlc(
            path,
            synced_tables,
            Arc::new(Hlc::new(device_id)),
            migrations,
        )
    }

    /// Open with a caller-supplied register clock instead of a fresh
    /// system-wall-clock one. Lets a test inject an [`Hlc`] over a controlled
    /// wall clock to exercise the skew/restart-seeding guarantees, sharing the
    /// production open path (migration, seed, session) so the test drives the
    /// real unit.
    ///
    pub(crate) fn open_with_hlc(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        hlc: Arc<Hlc>,
        migrations: &[Migration],
    ) -> Result<(Database, UpdatedAtStamper), DbError> {
        let (core, state, stamper) = DatabaseCore::open(path, synced_tables, hlc, migrations)?;

        #[cfg(not(target_arch = "wasm32"))]
        let database = Database {
            core: Arc::new(tokio::sync::Mutex::new(core)),
            state,
        };

        #[cfg(target_arch = "wasm32")]
        let database = Database {
            core: std::rc::Rc::new(std::cell::RefCell::new(core)),
            state,
        };

        Ok((database, stamper))
    }

    /// The host's declared synced-table set, the single owner of which tables
    /// participate in changeset sync. The capture session attaches exactly these,
    /// the register-clock seed scanned these, and the gate/apply operate over
    /// these — so the sync layer reads the set from here instead of carrying a
    /// separately-passed copy that could silently diverge.
    pub fn synced_tables(&self) -> &[SyncedTable] {
        &self.state.synced_tables
    }

    /// The applied synced-schema version — `PRAGMA user_version` after the
    /// migration ladder ran at open. This is the single source of the wire
    /// `schema_version`: every outgoing changeset is stamped with it, the pull
    /// gates compare incoming changesets and the min-floor against it, and the
    /// snapshot meta carries it. A device cannot stamp a version it has not
    /// migrated to. Cached because migrations run only at open, so the value is
    /// fixed for the handle's life.
    pub fn schema_version(&self) -> u32 {
        self.state.schema_version
    }

    /// The shared register clock. coven's sync layer advances it past pulled rows
    /// and stamps envelopes off it; it is the same `Arc<Hlc>` the stamper wraps.
    ///
    /// Native-only: the sole consumer is the native-only [`crate::sync::sync_manager::SyncManager`].
    #[cfg(not(target_arch = "wasm32"))]
    pub fn hlc(&self) -> Arc<Hlc> {
        self.state.hlc.clone()
    }

    pub fn stamper(&self) -> UpdatedAtStamper {
        UpdatedAtStamper::new(self.state.hlc.clone())
    }

    /// The receiver's current wall-clock millis, read from this database's
    /// register clock. The pull reads it once and passes it down to bound an
    /// incoming `_updated_at`'s physical component (a grossly-future stamp must not
    /// win last-writer-wins or ratchet the clock). Available on both targets — pull
    /// runs on wasm too — unlike [`Self::hlc`], whose only caller is native.
    pub(crate) fn receive_wall_ms(&self) -> u64 {
        self.state.hlc.wall_now_ms()
    }

    /// Run `f` against the connection and await the result.
    ///
    /// This is how the host runs its app SQL and how coven runs bookkeeping,
    /// gating, and apply — anything that needs `&Connection`. Writes issued here
    /// are captured by the attached session.
    ///
    /// Native serializes access behind a mutex; wasm runs `f` directly on the
    /// borrowed connection on this Worker and returns a ready future, so it needs
    /// no `Send` — the closure never leaves the thread.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn call<F, R>(&self, f: F) -> Result<R, DbError>
    where
        F: FnOnce(&Connection) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        let core = self.core.lock().await;
        f(core.connection())
    }

    /// See the native `call`. wasm borrows the connection, runs `f`, drops the
    /// borrow, and returns a ready future — the borrow never spans an `.await`.
    #[cfg(target_arch = "wasm32")]
    pub async fn call<F, R>(&self, f: F) -> Result<R, DbError>
    where
        F: FnOnce(&Connection) -> Result<R, DbError> + 'static,
        R: 'static,
    {
        let core = self.core.borrow();
        core.with_connection(f)
    }

    /// Test-only raw capture path for tests that inspect changeset bytes directly.
    #[cfg(all(any(test, feature = "test-utils"), not(target_arch = "wasm32")))]
    pub async fn take_changeset(&self) -> Result<Vec<u8>, DbError> {
        let mut core = self.core.lock().await;
        let bytes = core.capture_changeset();
        core.reset_session();
        bytes
    }

    /// Capture the outgoing changeset and stage non-empty bytes at `path` before
    /// resetting the recorded batch. If staging fails, the session is left
    /// un-reset so the same batch can be captured again on the next cycle.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn take_changeset_staged(&self, path: PathBuf) -> Result<Vec<u8>, DbError> {
        let mut core = self.core.lock().await;
        let bytes = core.capture_changeset()?;
        if !bytes.is_empty() {
            crate::local_blob::write_atomic(&path, &bytes)
                .await
                .map_err(DbError)?;
        }
        core.reset_session();
        Ok(bytes)
    }

    /// Test-only raw capture path for tests that inspect changeset bytes directly.
    #[cfg(all(any(test, feature = "test-utils"), target_arch = "wasm32"))]
    pub async fn take_changeset(&self) -> Result<Vec<u8>, DbError> {
        let mut core = self.core.borrow_mut();
        let bytes = core.capture_changeset();
        core.reset_session();
        bytes
    }

    /// See the native `take_changeset_staged`. wasm runs the same
    /// stage-before-reset ordering inline on the owned connection.
    #[cfg(target_arch = "wasm32")]
    pub(crate) async fn take_changeset_staged(&self, path: PathBuf) -> Result<Vec<u8>, DbError> {
        let _ = path;
        let mut core = self.core.borrow_mut();
        let bytes = core.capture_changeset()?;
        if !bytes.is_empty() {
            drop(core);
            crate::local_blob::write_atomic(&path, &bytes)
                .await
                .map_err(DbError)?;
            core = self.core.borrow_mut();
        }
        core.reset_session();
        Ok(bytes)
    }

    /// Apply an incoming changeset with capture disabled around just the apply, so
    /// the applied rows are not re-recorded as this device's own writes. `f`
    /// performs the apply on the connection and is SYNCHRONOUS (no `await`): on a
    /// single thread nothing else runs while it executes, so no host write can
    /// interleave inside the capture-off window. Capture is the live session's —
    /// disabled with `set_enabled(false)` and re-enabled the instant `f` returns —
    /// so host writes OUTSIDE this call (during push/pull) are still recorded.
    ///
    /// This is the ONLY thing the capture session is ever blind to: every other
    /// `call` runs on the normal enabled path.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn apply_changeset<F, R>(&self, f: F) -> Result<R, DbError>
    where
        F: FnOnce(&Connection) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        let mut core = self.core.lock().await;
        core.with_capture_disabled(f)
    }

    /// See the native `apply_changeset`. wasm borrows the connection, disables the
    /// live session, runs `f`, re-enables it, and returns a ready future — the
    /// borrow (and the capture-off window) never spans an `.await`.
    #[cfg(target_arch = "wasm32")]
    pub(crate) async fn apply_changeset<F, R>(&self, f: F) -> Result<R, DbError>
    where
        F: FnOnce(&Connection) -> Result<R, DbError> + 'static,
        R: 'static,
    {
        let mut core = self.core.borrow_mut();
        core.with_capture_disabled(f)
    }

    // ---- Bookkeeping: sync_state ----

    pub(crate) async fn get_sync_state(&self, key: &str) -> Result<Option<String>, DbError> {
        let key = key.to_string();
        self.call(move |conn| {
            conn.query_row("SELECT value FROM sync_state WHERE key = ?1", [key], |r| {
                r.get::<_, String>(0)
            })
            .optional()
            .map_err(DbError::from)
        })
        .await
    }

    pub async fn set_sync_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        let (key, value) = (key.to_string(), value.to_string());
        self.call(move |conn| {
            conn.execute(
                "INSERT INTO sync_state (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    /// `namespace`'s device-local cache-size budget in bytes, or `None` if the host
    /// has not set one for it. `None` means unlimited — eviction is off for that
    /// namespace and its cache grows without bound; the host opts a namespace into a
    /// budget by calling [`Self::set_cache_budget`]. Budgets are per namespace so a
    /// small namespace (`covers`) is never wiped by pressure from a big one
    /// (`release_files`): each evicts against its own budget. Stored as a single
    /// decimal value under [`crate::blob::cache::cache_budget_state_key`] in
    /// `sync_state` (config, not per-blob accounting — the cache's truth is still the
    /// folder on disk).
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError> {
        let key = crate::blob::cache::cache_budget_state_key(namespace);
        match self.get_sync_state(&key).await? {
            Some(raw) => raw.parse::<u64>().map(Some).map_err(|e| {
                DbError(format!(
                    "cache budget for {namespace:?} in sync_state is not a byte count: {e}"
                ))
            }),
            None => Ok(None),
        }
    }

    /// Set `namespace`'s device-local cache-size budget in bytes. Once set, a populate
    /// into that namespace's cache that pushes `storage/cache/<namespace>/` over this
    /// total evicts its oldest files (by mtime) back under it; `pinned/` is never
    /// counted or touched, and another namespace's files are never walked. Stored
    /// under [`crate::blob::cache::cache_budget_state_key`] in `sync_state`.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError> {
        let key = crate::blob::cache::cache_budget_state_key(namespace);
        self.set_sync_state(&key, &max_bytes.to_string()).await
    }

    // ---- Bookkeeping: sync_cursors ----

    pub(crate) async fn get_all_sync_cursors(&self) -> Result<HashMap<String, u64>, DbError> {
        self.call(move |conn| {
            let mut stmt = conn
                .prepare("SELECT device_id, last_seq FROM sync_cursors")
                .map_err(DbError::from)?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64))
                })
                .map_err(DbError::from)?;
            let mut out = HashMap::new();
            for row in rows {
                let (id, seq) = row.map_err(DbError::from)?;
                out.insert(id, seq);
            }
            Ok(out)
        })
        .await
    }

    pub async fn set_sync_cursor(&self, device_id: &str, seq: u64) -> Result<(), DbError> {
        let device_id = device_id.to_string();
        self.call(move |conn| {
            conn.execute(
                "INSERT INTO sync_cursors (device_id, last_seq) VALUES (?1, ?2) \
                 ON CONFLICT(device_id) DO UPDATE SET last_seq = excluded.last_seq",
                (device_id, seq as i64),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    // ---- Item keys ----

    /// Mint a random per-item content key and store it in the synced `item_keys`
    /// table, keyed by `item_id`. Idempotent: a no-op if the item already has a
    /// key (a re-mint must not rotate it out from under blobs already encrypted
    /// under it).
    ///
    /// The INSERT runs through [`Self::call`], so the attached capture session
    /// records it into the outgoing changeset and it replays to every member; the
    /// `_updated_at` HLC stamp satisfies the synced-table contract. Because
    /// `item_keys` is in the synced set, the row also survives a snapshot
    /// bootstrap. Returns the item's key (the freshly minted one, or the existing
    /// one if it was already present).
    ///
    /// `item_id` must be unique at creation. coven does not coordinate two devices
    /// independently minting the SAME `item_id` while offline: they would generate
    /// different random keys, last-writer-wins on the `item_keys` row keeps one,
    /// and any blobs the loser already encrypted under its key become
    /// undecryptable. Hosts avoid this by minting at item-creation time, where the
    /// id is freshly allocated and the item does not yet exist on another device.
    pub async fn mint_item_key(&self, item_id: &str) -> Result<[u8; 32], DbError> {
        let item_id = item_id.to_string();
        let new_key = crate::encryption::generate_random_key();
        let updated_at = self.state.hlc.now().to_string();
        self.call(move |conn| Self::mint_item_key_on(conn, &item_id, new_key, &updated_at))
            .await
    }

    /// Transaction-composable form of [`mint_item_key`](Self::mint_item_key):
    /// runs on a connection the host already holds inside a
    /// [`call`](Self::call) closure, so the host can mint an item's key
    /// atomically with the item's own rows. The caller supplies the random
    /// key ([`crate::encryption::generate_random_key`]) and the `_updated_at`
    /// stamp (the host's [`crate::sync::hlc::UpdatedAtStamper`], which shares
    /// this database's HLC register).
    pub fn mint_item_key_on(
        conn: &Connection,
        item_id: &str,
        new_key: [u8; 32],
        updated_at: &str,
    ) -> Result<[u8; 32], DbError> {
        conn.execute(
            "INSERT OR IGNORE INTO item_keys (item_id, key, _updated_at) \
             VALUES (?1, ?2, ?3)",
            (item_id, new_key.to_vec(), updated_at),
        )
        .map_err(DbError::from)?;
        // Read back the stored key — `INSERT OR IGNORE` keeps the existing
        // row, so on a re-mint this returns the original key, not `new_key`.
        read_item_key(conn, item_id)?
            .ok_or_else(|| DbError(format!("item_keys row absent after mint for {item_id}")))
    }

    /// The content key for `item_id` from the synced `item_keys` table, or `None`
    /// if no key has been minted for it. A local SELECT — the row arrives via the
    /// changeset (for members) or the snapshot (for a bootstrapped joiner).
    pub async fn item_key(&self, item_id: &str) -> Result<Option<[u8; 32]>, DbError> {
        let item_id = item_id.to_string();
        self.call(move |conn| read_item_key(conn, &item_id)).await
    }

    /// Resolve a public [`crate::blob::BlobScope`] to the internal
    /// [`crate::blob::ResolvedScope`] storage and encryption consume. `Master`
    /// and `Derived` pass through unchanged; `Item(id)` looks up the item's key
    /// in `item_keys`. This is the single resolution point shared by the three
    /// blob paths (changeset push, changeset pull, outbox drain).
    ///
    /// A missing `item_keys` row is a host bug — the host must mint the key (and
    /// let it sync) before tagging a blob with that item. coven surfaces it as an
    /// error rather than silently falling back to the master key, which would
    /// encrypt the blob so no share recipient could ever read it.
    pub async fn resolve_blob_scope(
        &self,
        scope: crate::blob::BlobScope,
    ) -> Result<crate::blob::ResolvedScope, DbError> {
        use crate::blob::{BlobScope, ResolvedScope};
        match scope {
            BlobScope::Master => Ok(ResolvedScope::Master),
            BlobScope::Derived(s) => Ok(ResolvedScope::Derived(s)),
            BlobScope::Item(item_id) => match self.item_key(&item_id).await? {
                Some(key) => Ok(ResolvedScope::Key(key)),
                None => Err(DbError(format!(
                    "no item key for {item_id:?}: a blob was scoped to an item with no minted key \
                     (mint_item_key must run and sync before tagging the blob)"
                ))),
            },
        }
    }

    // ---- Cloud outbox ----

    /// Enqueue a blob upload. `scope` names which key the blob is encrypted
    /// under (master, a derived scope, or a coven-managed item); coven persists
    /// it on the row and resolves it to a key at drain — looking up the
    /// `item_keys` row for an [`crate::blob::BlobScope::Item`] scope — long after
    /// the enqueue site is gone. Idempotent on `(operation, cloud_key)`. Queuing an
    /// upload also cancels any pending delete of the same key — latest intent wins,
    /// so a re-upload isn't tombstoned in the same cycle.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn enqueue_upload(
        &self,
        file_id: &str,
        cloud_key: &str,
        source_path: Option<&str>,
        scope: crate::blob::BlobScope,
        retain_pinned: bool,
        created_at: &str,
    ) -> Result<(), DbError> {
        let (file_id, cloud_key, source_path, created_at) = (
            file_id.to_string(),
            cloud_key.to_string(),
            source_path.map(str::to_string),
            created_at.to_string(),
        );
        self.call(move |conn| {
            Self::enqueue_upload_on(
                conn,
                &file_id,
                &cloud_key,
                source_path.as_deref(),
                scope,
                retain_pinned,
                &created_at,
            )
        })
        .await
    }

    /// Transaction-composable form of [`enqueue_upload`](Self::enqueue_upload):
    /// runs on a connection the host already holds inside a
    /// [`call`](Self::call) closure, so the host can commit a row's upload
    /// intent atomically with the row itself (e.g. an import that must either
    /// land with its uploads queued or not land at all).
    pub fn enqueue_upload_on(
        conn: &Connection,
        file_id: &str,
        cloud_key: &str,
        source_path: Option<&str>,
        scope: crate::blob::BlobScope,
        retain_pinned: bool,
        created_at: &str,
    ) -> Result<(), DbError> {
        // Latest intent wins: queuing an upload for a key cancels a pending delete
        // of the same key, so a re-upload and a stale delete can't both be live in
        // one cycle (which the upload-then-delete phase split would otherwise
        // resolve by deleting the freshly re-uploaded blob). Runs on the caller's
        // connection so a transactional import stays atomic with this cancel.
        conn.execute(
            "DELETE FROM cloud_outbox WHERE operation = 'delete' AND cloud_key = ?1",
            [cloud_key],
        )
        .map_err(DbError::from)?;
        conn.execute(
            "INSERT OR IGNORE INTO cloud_outbox \
             (operation, file_id, cloud_key, source_path, scope, retain_pinned, created_at) \
             VALUES ('upload', ?1, ?2, ?3, ?4, ?5, ?6)",
            (
                file_id,
                cloud_key,
                source_path,
                scope.to_outbox_str(),
                retain_pinned,
                created_at,
            ),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Enqueue a blob delete. The next sync cycle's drain turns it into a signed
    /// cloud tombstone, and a later GC reclaims the blob once the convergence grace
    /// has passed (see [`crate::blob::delete`]). Idempotent on `(operation,
    /// cloud_key)`. Queuing a delete also cancels any pending upload of the same key
    /// — latest intent wins.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn enqueue_delete(&self, cloud_key: &str, created_at: &str) -> Result<(), DbError> {
        let (cloud_key, created_at) = (cloud_key.to_string(), created_at.to_string());
        self.call(move |conn| Self::enqueue_delete_on(conn, &cloud_key, &created_at))
            .await
    }

    /// Transaction-composable form of [`enqueue_delete`](Self::enqueue_delete):
    /// runs on a connection the host already holds inside a [`call`](Self::call)
    /// closure, so coven's `make_local` can flip a root's gate false and enqueue its
    /// blob deletes in one transaction — the tombstone can't be lost to a crash
    /// between the flip and a separate enqueue. A delete touches no key, so it
    /// carries no scope (the column is NULL for a delete row).
    pub fn enqueue_delete_on(
        conn: &Connection,
        cloud_key: &str,
        created_at: &str,
    ) -> Result<(), DbError> {
        // Latest intent wins: queuing a delete cancels a pending upload of the same
        // key (the mirror of `enqueue_upload_on`), so an enqueued-then-deleted blob
        // isn't uploaded only to be tombstoned in the same cycle. It also drops a
        // pending tombstone-cancel for the key: a fresh delete wants the blob
        // tombstoned, so a leftover cancel (which would remove that tombstone) must
        // not survive to undo it.
        conn.execute(
            "DELETE FROM cloud_outbox \
             WHERE operation IN ('upload', 'cancel') AND cloud_key = ?1",
            [cloud_key],
        )
        .map_err(DbError::from)?;
        conn.execute(
            "INSERT OR IGNORE INTO cloud_outbox \
             (operation, cloud_key, scope, created_at) \
             VALUES ('delete', ?1, NULL, ?2)",
            (cloud_key, created_at),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Pending upload entries, oldest first. The host reads these to drive its
    /// own upload-status UI; coven's sync loop reads them to do the uploads.
    pub async fn get_pending_cloud_uploads(&self) -> Result<Vec<OutboxEntry>, DbError> {
        self.pending_outbox("upload").await
    }

    /// Pending delete entries, oldest first.
    pub async fn get_pending_cloud_deletes(&self) -> Result<Vec<OutboxEntry>, DbError> {
        self.pending_outbox("delete").await
    }

    /// Pending tombstone-cancel entries, oldest first. Each names a `cloud_key`
    /// whose tombstone must be removed because the blob was re-uploaded; the
    /// tombstone-cancel drain reads these and retries the removal until it lands
    /// (see [`crate::blob::delete::drain_tombstone_cancels`]).
    pub async fn get_pending_cloud_cancels(&self) -> Result<Vec<OutboxEntry>, DbError> {
        self.pending_outbox("cancel").await
    }

    async fn pending_outbox(&self, op_str: &'static str) -> Result<Vec<OutboxEntry>, DbError> {
        self.call(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT id, operation, file_id, cloud_key, source_path, scope, \
                            retain_pinned, attempt_count, last_attempt_at \
                     FROM cloud_outbox WHERE operation = ?1 ORDER BY id",
                )
                .map_err(DbError::from)?;
            let rows = stmt
                .query_map([op_str], row_to_outbox_entry)
                .map_err(DbError::from)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(DbError::from)?);
            }
            Ok(out)
        })
        .await
    }

    /// Remove an outbox entry by id (after the upload or delete completed).
    pub async fn remove_cloud_outbox_entry(&self, id: i64) -> Result<(), DbError> {
        self.call(move |conn| {
            conn.execute("DELETE FROM cloud_outbox WHERE id = ?1", [id])
                .map(|_| ())
                .map_err(DbError::from)
        })
        .await
    }

    /// Clear the retry-backoff timestamp on failed uploads so the next cycle
    /// retries them immediately. Backs a host "retry now" action.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn reset_cloud_outbox_backoff(&self) -> Result<(), DbError> {
        self.call(move |conn| {
            conn.execute(
                "UPDATE cloud_outbox SET last_attempt_at = NULL \
                 WHERE operation = 'upload' AND attempt_count > 0",
                [],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    /// Record a failed upload attempt (bumps `attempt_count`, stores the error
    /// and the time). The entry stays queued for retry.
    pub async fn record_cloud_upload_failure(
        &self,
        id: i64,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        self.record_cloud_outbox_failure(id, "upload", error, attempted_at)
            .await
    }

    /// Record a failed delete/tombstone attempt (bumps `attempt_count`, stores the
    /// error and the time). Scoped to delete rows so an id collision with another
    /// operation cannot mutate the wrong kind of outbox entry.
    pub async fn record_cloud_delete_failure(
        &self,
        id: i64,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        self.record_cloud_outbox_failure(id, "delete", error, attempted_at)
            .await
    }

    /// Record a failed tombstone-cancel attempt (bumps `attempt_count`, stores
    /// the error and the time). Scoped to cancel rows so an id collision with
    /// another operation cannot mutate the wrong kind of outbox entry.
    pub async fn record_cloud_cancel_failure(
        &self,
        id: i64,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        self.record_cloud_outbox_failure(id, "cancel", error, attempted_at)
            .await
    }

    async fn record_cloud_outbox_failure(
        &self,
        id: i64,
        operation: &str,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), DbError> {
        let (operation, error, attempted_at) = (
            operation.to_string(),
            error.to_string(),
            attempted_at.to_string(),
        );
        self.call(move |conn| {
            conn.execute(
                "UPDATE cloud_outbox \
                 SET attempt_count = attempt_count + 1, last_error = ?1, last_attempt_at = ?2 \
                 WHERE id = ?3 AND operation = ?4",
                (error, attempted_at, id, operation),
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    // ---- Local blob refs (external user files) ----

    /// Register an external blob ref: map `blob_id` to the user-owned file at
    /// `path`, whose plaintext length is `size`. Insert-or-replace on `blob_id`, so
    /// a relocate (the user picks a new folder; the host recomputes each path and
    /// re-registers) overwrites the prior row. coven reads this file but does not
    /// own it; a read validates it by presence + size (see
    /// [`crate::db`]'s `local_blob_refs` and [`crate::blob::cache::read_blob`]).
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn register_external_blob(
        &self,
        blob_id: &str,
        namespace: &str,
        path: &Path,
        size: u64,
    ) -> Result<(), DbError> {
        let (blob_id, namespace, path) = (
            blob_id.to_string(),
            namespace.to_string(),
            path.to_path_buf(),
        );
        self.call(move |conn| {
            Self::register_external_blob_on(conn, &blob_id, &namespace, &path, size)
        })
        .await
    }

    /// Transaction-composable form of
    /// [`register_external_blob`](Self::register_external_blob): runs on a
    /// connection the host already holds inside a [`call`](Self::call) closure, so
    /// coven's `make_local` can flip a root's gate false and register the external
    /// refs for its now-local user files in one transaction. Insert-or-replace on
    /// `blob_id`, so a relocate overwrites the prior row.
    pub fn register_external_blob_on(
        conn: &Connection,
        blob_id: &str,
        namespace: &str,
        path: &Path,
        size: u64,
    ) -> Result<(), DbError> {
        let path = path.to_str().ok_or_else(|| {
            DbError(format!(
                "external blob path for {blob_id} is not valid UTF-8: {path:?}"
            ))
        })?;
        conn.execute(
            "INSERT INTO local_blob_refs (blob_id, namespace, path, size) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(blob_id) DO UPDATE SET \
                 namespace = excluded.namespace, \
                 path = excluded.path, \
                 size = excluded.size",
            (blob_id, namespace, path, size as i64),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Remove the external blob ref for `blob_id`, so the blob resolves through the
    /// normal cache/cloud path again. A no-op if no ref is registered.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn clear_external_blob(&self, blob_id: &str) -> Result<(), DbError> {
        let blob_id = blob_id.to_string();
        self.call(move |conn| Self::clear_external_blob_on(conn, &blob_id))
            .await
    }

    /// Transaction-composable form of
    /// [`clear_external_blob`](Self::clear_external_blob): runs on a connection the
    /// host already holds inside a [`call`](Self::call) closure, so coven's
    /// `make_remote` completion can flip a root's gate true and drop the external
    /// refs for its now-cloud-backed blobs in one transaction. A no-op if no ref is
    /// registered.
    pub fn clear_external_blob_on(conn: &Connection, blob_id: &str) -> Result<(), DbError> {
        conn.execute("DELETE FROM local_blob_refs WHERE blob_id = ?1", [blob_id])
            .map(|_| ())
            .map_err(DbError::from)
    }

    // ---- Blob make-Remote intents (device-local in-flight make_remote markers) ----

    /// Record an in-flight make_remote of `(root_table, root_id)` as a durable
    /// marker. Transaction-composable: coven's `make_remote` inserts this in the same
    /// transaction that enqueues the root's user-provided blob uploads or flips a
    /// host-provided-only root, so an in-flight make_remote is a durable, atomic fact.
    /// `retain_pinned` is consumed by inline host-provided uploads, which have no
    /// upload outbox row to carry the pin choice.
    pub fn insert_make_remote_intent_on(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
        retain_pinned: bool,
    ) -> Result<(), DbError> {
        conn.execute(
            "INSERT OR REPLACE INTO blob_make_remote_intents \
             (root_table, root_id, retain_pinned) VALUES (?1, ?2, ?3)",
            (root_table, root_id, retain_pinned),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Remove the make_remote intent for `(root_table, root_id)`.
    /// Transaction-composable so it commits with the gate flip (on completion) or
    /// with the cancel cleanup.
    pub fn delete_make_remote_intent_on(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), DbError> {
        conn.execute(
            "DELETE FROM blob_make_remote_intents WHERE root_table = ?1 AND root_id = ?2",
            (root_table, root_id),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Whether a make_remote is in flight for `(root_table, root_id)`. Read inside
    /// the upload drain's completion check to tell a make_remote to finish (`true`)
    /// from an orphan upload of a cancelled make_remote to tombstone (`false`).
    /// Synchronous (runs on a connection the caller already holds).
    pub fn make_remote_intent_exists(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
    ) -> Result<bool, DbError> {
        conn.query_row(
            "SELECT 1 FROM blob_make_remote_intents \
             WHERE root_table = ?1 AND root_id = ?2",
            (root_table, root_id),
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(DbError::from)
    }

    pub fn make_remote_intent_retain_pinned(
        conn: &Connection,
        root_table: &str,
        root_id: &str,
    ) -> Result<Option<bool>, DbError> {
        conn.query_row(
            "SELECT retain_pinned FROM blob_make_remote_intents \
             WHERE root_table = ?1 AND root_id = ?2",
            (root_table, root_id),
            |r| r.get::<_, bool>(0),
        )
        .optional()
        .map_err(DbError::from)
    }

    /// The external user-owned file `blob_id` resolves to, or `None` when no ref is
    /// registered (the blob is host-provided local-store, cache, or cloud — not a
    /// user-provided external file). The locality-aware read
    /// ([`crate::blob::cache::read_blob`]) consults this before dispatching on the
    /// blob's locality.
    pub async fn external_blob(&self, blob_id: &str) -> Result<Option<ExternalBlob>, DbError> {
        let blob_id = blob_id.to_string();
        self.call(move |conn| {
            conn.query_row(
                "SELECT path, size FROM local_blob_refs WHERE blob_id = ?1",
                [blob_id],
                |r| {
                    Ok(ExternalBlob {
                        path: std::path::PathBuf::from(r.get::<_, String>(0)?),
                        size: r.get::<_, i64>(1)? as u64,
                    })
                },
            )
            .optional()
            .map_err(DbError::from)
        })
        .await
    }
}

/// Read the 32-byte content key for `item_id` from `item_keys`, or `None` if no
/// row exists. A stored key of the wrong length is a corrupt DB and surfaces as a
/// [`DbError`] — `mint_item_key` only ever writes 32 bytes, so this names the
/// offending row rather than failing later as an opaque decrypt error.
fn read_item_key(conn: &Connection, item_id: &str) -> Result<Option<[u8; 32]>, DbError> {
    let stored: Option<Vec<u8>> = conn
        .query_row(
            "SELECT key FROM item_keys WHERE item_id = ?1",
            [item_id],
            |r| r.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(DbError::from)?;
    match stored {
        None => Ok(None),
        Some(bytes) => {
            let len = bytes.len();
            bytes.try_into().map(Some).map_err(|_| {
                DbError(format!(
                    "item_keys.key for {item_id} is {len} bytes, not 32"
                ))
            })
        }
    }
}

fn migrate_bookkeeping_schema(conn: &Connection) -> Result<(), DbError> {
    let columns = crate::blob::decl::table_columns(conn, "blob_make_remote_intents")
        .map_err(|e| DbError(e.to_string()))?;
    let has_retain_pinned = columns.iter().any(|column| column == "retain_pinned");
    if !has_retain_pinned {
        conn.execute(
            "ALTER TABLE blob_make_remote_intents \
             ADD COLUMN retain_pinned INTEGER",
            [],
        )
        .map_err(DbError::from)?;
    }
    Ok(())
}

/// Map a `cloud_outbox` row to an [`OutboxEntry`]. Column order matches the
/// SELECT in [`Database::pending_outbox`]. The flat row reads back as one
/// [`OutboxOperation`] variant or the other, built from the columns that belong
/// to that operation — the rest are NULL and unread.
fn row_to_outbox_entry(r: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxEntry> {
    let op_tag: String = r.get(1)?;
    let operation = match op_tag.as_str() {
        "upload" => {
            // A present scope must parse — a present-but-unparseable scope is a
            // corrupt row. An upload always wrote one (the column is non-NULL
            // for an upload), so its absence is also corruption.
            let scope_str: String = r.get(5)?;
            let scope = crate::blob::BlobScope::from_outbox_str(&scope_str)
                .unwrap_or_else(|| panic!("invalid cloud_outbox.scope: {scope_str:?}"));
            OutboxOperation::Upload {
                file_id: r.get(2)?,
                source_path: r.get(4)?,
                scope,
                retain_pinned: r.get(6)?,
            }
        }
        "delete" => OutboxOperation::Delete,
        "cancel" => OutboxOperation::Cancel,
        other => panic!("invalid cloud_outbox.operation: {other:?}"),
    };
    Ok(OutboxEntry {
        id: r.get(0)?,
        cloud_key: r.get(3)?,
        attempt_count: r.get(7)?,
        last_attempt_at: r.get(8)?,
        operation,
    })
}

/// Seed the register clock from one candidate floor, if present. A present but
/// unparseable value is a corrupt register and aborts; `None` contributes no
/// floor.
fn seed_from(hlc: &Hlc, value: Option<String>, context: &str) -> Result<(), DbError> {
    if let Some(stamp) = value {
        let floor = Timestamp::parse(&stamp)
            .ok_or_else(|| DbError(format!("corrupt {context}: {stamp:?}")))?;
        hlc.seed(&floor);
    }
    Ok(())
}

/// `SELECT MAX(_updated_at)` over every synced table, taking the overall
/// lexicographic max. A registered table that does not exist is a host
/// integration error and surfaces as `Err`.
fn scan_max_updated_at(
    conn: &Connection,
    synced_tables: &[SyncedTable],
) -> Result<Option<String>, DbError> {
    let mut overall: Option<String> = None;
    for t in synced_tables {
        let sql = format!(
            "SELECT MAX(_updated_at) FROM {}",
            crate::sync::session::quote_ident(t.name())
        );
        let value: Option<String> = conn
            .query_row(&sql, [], |r| r.get::<_, Option<String>>(0))
            .map_err(|e| {
                DbError(format!(
                    "register-floor scan over synced table {}: {e}",
                    t.name()
                ))
            })?;
        if let Some(v) = value {
            overall = Some(match overall {
                Some(cur) if cur >= v => cur,
                _ => v,
            });
        }
    }
    Ok(overall)
}

/// Create a session and attach every synced table. Returns `None` (logging) on
/// failure so the loop can keep serving non-capturing requests.
fn attach_session<'c>(
    conn: &'c Connection,
    synced_tables: &[SyncedTable],
) -> Result<rusqlite::session::Session<'c>, DbError> {
    let mut session = rusqlite::session::Session::new(conn)
        .map_err(|e| DbError(format!("failed to create capture session: {e}")))?;
    for t in synced_tables {
        session.attach(Some(t.name())).map_err(|e| {
            DbError(format!(
                "failed to attach synced table {} to session: {e}",
                t.name()
            ))
        })?;
    }
    Ok(session)
}

/// Drain the session's recorded changes into a changeset, WITHOUT touching the
/// session afterward (the caller resets it — see [`reset_session`]). An absent
/// session is exceptional — a prior attach failed, so host writes since then were
/// NOT recorded — and is returned as an error so the caller surfaces the lost
/// capture loudly rather than silently reporting "no local changes" while writes
/// went uncaptured.
fn capture_changeset(
    session: Option<&mut rusqlite::session::Session<'_>>,
) -> Result<Vec<u8>, DbError> {
    match session {
        Some(s) => {
            let mut buf = Vec::new();
            s.changeset_strm(&mut buf)
                .map(|()| buf)
                .map_err(DbError::from)
        }
        None => Err(DbError(
            "capture session absent at changeset capture; host writes since the last \
             cycle were not recorded (a prior session attach failed)"
                .to_string(),
        )),
    }
}

pub type PlatformConnectionOpener = fn(&Path) -> Result<Connection, DbError>;

#[cfg(not(target_arch = "wasm32"))]
static PLATFORM_CONNECTION_OPENER: std::sync::OnceLock<PlatformConnectionOpener> =
    std::sync::OnceLock::new();

#[cfg(target_arch = "wasm32")]
thread_local! {
    static PLATFORM_CONNECTION_OPENER: std::cell::Cell<Option<PlatformConnectionOpener>> =
        std::cell::Cell::new(None);
}

#[cfg(not(target_arch = "wasm32"))]
pub fn register_platform_connection_opener(opener: PlatformConnectionOpener) {
    let _ = PLATFORM_CONNECTION_OPENER.set(opener);
}

#[cfg(target_arch = "wasm32")]
pub fn register_platform_connection_opener(opener: PlatformConnectionOpener) {
    PLATFORM_CONNECTION_OPENER.with(|slot| slot.set(Some(opener)));
}

#[cfg(not(target_arch = "wasm32"))]
fn open_connection(path: &Path) -> Result<Connection, DbError> {
    if let Some(opener) = PLATFORM_CONNECTION_OPENER.get() {
        opener(path)
    } else {
        #[cfg(any(test, feature = "test-utils"))]
        {
            let conn = Connection::open(path).map_err(DbError::from)?;
            conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
                .map_err(DbError::from)?;
            Ok(conn)
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        {
            Err(DbError(
                "platform SQLite connection opener is not registered".to_string(),
            ))
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn open_connection(path: &Path) -> Result<Connection, DbError> {
    PLATFORM_CONNECTION_OPENER
        .with(|slot| slot.get())
        .ok_or_else(|| DbError("platform SQLite connection opener is not registered".to_string()))?(
        path,
    )
}
