//! The owned SQLite connection.
//!
//! coven owns one `rusqlite::Connection` together with a persistent session
//! attached to the synced tables. Every database access — the host's app SQL,
//! coven's bookkeeping, changeset capture and apply — runs against that one
//! connection, so access is serialized and the session sees every write.
//!
//! The host no longer opens a connection, lends a raw pointer, or implements
//! bookkeeping. It hands coven a path and a schema migration, then runs its own
//! SQL through [`Database::call`].
//!
//! How the connection is held is target-split, because the platforms differ on
//! what "one owner" can mean:
//!
//! - Native runs the connection on a dedicated OS thread (the actor). Every
//!   method sends a request over an mpsc channel and awaits a oneshot reply, so
//!   the connection never crosses a thread boundary and the `Send` bounds the
//!   native sync loop needs are satisfiable.
//! - wasm has no threads in the browser without cross-origin isolation, and the
//!   OPFS VFS's sync access handles work only on the Worker the connection lives
//!   on. So wasm holds the connection and capture session directly behind
//!   `Rc<RefCell<…>>` on that one Worker; every method runs the same logic inline
//!   and returns a ready future, keeping the `async fn` signatures identical.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::oneshot;
#[cfg(not(target_arch = "wasm32"))]
use tracing::debug;
use tracing::error;

use crate::db::{OutboxEntry, OutboxOperation, MIGRATION_SQL};
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

/// A unit of work for the actor thread. Each carries its own reply channel.
#[cfg(not(target_arch = "wasm32"))]
enum Request {
    /// Run an arbitrary closure against the connection. Host app SQL and coven
    /// bookkeeping both arrive this way; writes are captured by the attached
    /// session. The closure boxes its own typed reply.
    Call(Box<dyn FnOnce(&Connection) + Send>),
    /// Capture the outgoing changeset bytes from the session, then reset the
    /// recorded batch so the next capture starts empty. Capture stays ENABLED —
    /// this does NOT suspend; host writes after it are recorded normally.
    TakeChangeset(oneshot::Sender<Result<Vec<u8>, DbError>>),
    /// Run a changeset apply on the connection with capture disabled around just
    /// the apply, so the applied rows are not re-recorded as this device's own
    /// writes. The closure (which performs the apply) boxes its own typed reply;
    /// capture is enabled again the instant it returns.
    ApplyChangeset(Box<dyn FnOnce(&Connection) + Send>),
}

/// A handle to the owned database. Cloneable; every clone talks to the same
/// connection thread.
///
/// Field drop order is load-bearing: `tx` must drop before `_thread`. The actor
/// loop exits only once every `Request` sender is gone, and `JoinOnDrop` joins
/// the thread; if `_thread` dropped first it would block forever waiting on a
/// loop this handle's `tx` still keeps alive. Rust drops fields top-to-bottom,
/// so `tx` precedes `_thread` here deliberately.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct Database {
    tx: std::sync::mpsc::Sender<Request>,
    hlc: Arc<Hlc>,
    /// The host's declared synced-table set, owned here so the sync layer reads
    /// it from one place (the same set the capture session attaches and the
    /// register-clock seed scanned). See [`Database::synced_tables`].
    synced_tables: Arc<Vec<SyncedTable>>,
    _thread: Arc<JoinOnDrop>,
}

/// A handle to the owned database. Cloneable; every clone shares the one
/// connection and capture session through the same `Rc`.
///
/// The browser is single-threaded (the connection lives on one Worker), so this
/// holds the connection directly behind `Rc<RefCell<…>>` instead of talking to a
/// thread. The capture session borrows the connection; the inner `Owned` holds
/// both so the session's pointer stays valid for the connection's whole life, and
/// its field order drops the session before the connection.
#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
pub struct Database {
    owned: std::rc::Rc<std::cell::RefCell<Owned>>,
    hlc: Arc<Hlc>,
    /// The host's declared synced-table set, owned here so the sync layer reads
    /// it from one place (the same set the capture session attaches and the
    /// register-clock seed scanned). See [`Database::synced_tables`].
    synced_tables: Arc<Vec<SyncedTable>>,
}

/// The wasm connection and its capture session, kept together.
///
/// `Session<'conn>` borrows the `Connection`; its `'conn` is a borrow-checker
/// fiction over a raw `sqlite3_session*` that internally holds the connection's
/// `sqlite3*`. Keeping both in one struct, with the session declared before the
/// connection, means the session is dropped first (Rust drops fields
/// top-to-bottom) and the connection it points at outlives it. The lifetime is
/// erased to `'static` at construction ([`Owned::new`]); that erasure is sound
/// precisely because of this co-ownership and drop order.
///
/// `session` is `Some` for the connection's whole life, except a transient window
/// inside [`Database::take_changeset`] where it is dropped and immediately
/// recreated to reset the recorded batch (SQLite cannot clear a session in
/// place). Capture is never suspended across an `await`: an apply disables
/// recording on the live session ([`Database::apply_changeset`]) rather than
/// detaching it. `None` otherwise means a prior attach failed and is reported
/// loudly, not relied upon.
#[cfg(target_arch = "wasm32")]
struct Owned {
    session: Option<rusqlite::session::Session<'static>>,
    conn: Connection,
}

#[cfg(target_arch = "wasm32")]
impl Owned {
    /// Hold `conn` and attach a capture session over `synced_tables`.
    ///
    /// The session is created against `&conn` and its lifetime erased to
    /// `'static`. Sound because `Owned` owns the connection alongside the session
    /// and drops the session first — the C session object never outlives the
    /// `sqlite3*` it points at.
    fn new(conn: Connection, synced_tables: &[SyncedTable]) -> Result<Self, DbError> {
        let session = Some(attach_erased_session(&conn, synced_tables)?);
        Ok(Owned { session, conn })
    }
}

/// Erase a capture session's connection-borrow lifetime to `'static`.
///
/// The lifetime is `PhantomData` over a raw pointer; the cast changes no bytes.
/// The caller guarantees the borrowed connection outlives the returned session
/// (here, by co-owning both in [`Owned`] and dropping the session first).
#[cfg(target_arch = "wasm32")]
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
/// for storage in [`Owned`]. The caller must keep `conn` alive at least as long as
/// the returned session (see [`erase_session_lifetime`]).
#[cfg(target_arch = "wasm32")]
fn attach_erased_session(
    conn: &Connection,
    synced_tables: &[SyncedTable],
) -> Result<rusqlite::session::Session<'static>, DbError> {
    Ok(erase_session_lifetime(attach_session(conn, synced_tables)?))
}

/// Joins the actor thread when the last `Database` handle drops, so the
/// connection is closed cleanly.
#[cfg(not(target_arch = "wasm32"))]
struct JoinOnDrop {
    handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for JoinOnDrop {
    fn drop(&mut self) {
        if let Some(h) = self.handle.lock().unwrap().take() {
            // The actor loop exits when every Sender is dropped; the Database's
            // tx is gone by the time this runs, so the join returns promptly.
            let _ = h.join();
        }
    }
}

impl Database {
    /// Open and own the connection at `path`.
    ///
    /// Runs coven's bookkeeping migration, then the host's `migrate` for the
    /// app's own tables, seeds the register clock off the on-disk rows, and
    /// attaches the capture session to `synced_tables`. Natively it then spawns
    /// the connection thread; on wasm the connection and session are held
    /// directly on this Worker. Returns the handle plus the non-optional
    /// `_updated_at` stamper the host binds into every synced-row write.
    ///
    /// On wasm, `path` is mapped to a flat OPFS filename (see `open_with_hlc`),
    /// and `coven::wasm::install_browser_storage` must have run once before this
    /// is called.
    pub fn open(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        device_id: String,
        migrate: impl FnOnce(&Connection) -> Result<(), DbError>,
    ) -> Result<(Database, UpdatedAtStamper), DbError> {
        Self::open_with_hlc(path, synced_tables, Arc::new(Hlc::new(device_id)), migrate)
    }

    /// Open with a caller-supplied register clock instead of a fresh
    /// system-wall-clock one. Lets a test inject an [`Hlc`] over a controlled
    /// wall clock to exercise the skew/restart-seeding guarantees, sharing the
    /// production open path (migration, seed, session, actor) so the test drives
    /// the real unit.
    ///
    /// On wasm, OPFS filenames are flat — the installed opfs-sahpool VFS has no
    /// directories. So the connection opens against the path's file-name
    /// component (`name.db` from `/any/dir/name.db`), which is the stable,
    /// deterministic key a reopen must match. A host that opens two libraries
    /// must therefore give them distinct file names, not just distinct
    /// directories.
    pub(crate) fn open_with_hlc(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        hlc: Arc<Hlc>,
        migrate: impl FnOnce(&Connection) -> Result<(), DbError>,
    ) -> Result<(Database, UpdatedAtStamper), DbError> {
        #[cfg(not(target_arch = "wasm32"))]
        let conn = Connection::open(path).map_err(DbError::from)?;
        #[cfg(target_arch = "wasm32")]
        let conn = {
            let name = opfs_filename(path)?;
            tracing::debug!(opfs_name = %name, "wasm: opening DB against opfs-sahpool VFS");
            Connection::open(&name).map_err(DbError::from)?
        };
        // WAL is a no-op on an in-memory db (the pragma reports "memory"); use a
        // query that tolerates the returned row rather than `pragma_update`,
        // which errors when a pragma yields rows.
        //
        // The browser's OPFS-backed SQLite VFS forbids WAL — leave the default
        // rollback journal there. (sqlite-wasm-rs's VFS has no shared-memory WAL
        // index.) Skipped, not silently defaulted: the log records the choice.
        #[cfg(not(target_arch = "wasm32"))]
        conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
            .map_err(DbError::from)?;
        #[cfg(target_arch = "wasm32")]
        tracing::debug!(
            "wasm: skipping PRAGMA journal_mode=WAL (OPFS VFS uses the rollback journal)"
        );
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::from)?;

        // coven's bookkeeping tables first, then the host's schema.
        conn.execute_batch(MIGRATION_SQL).map_err(DbError::from)?;
        migrate(&conn)?;

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

        #[cfg(not(target_arch = "wasm32"))]
        let database = {
            let (tx, rx) = std::sync::mpsc::channel::<Request>();
            let actor_tables = synced_tables.clone();
            let handle = std::thread::Builder::new()
                .name("coven-db".to_string())
                .spawn(move || run_actor(conn, &actor_tables, rx))
                .map_err(|e| DbError(format!("failed to spawn db thread: {e}")))?;
            Database {
                tx,
                hlc,
                synced_tables,
                _thread: Arc::new(JoinOnDrop {
                    handle: std::sync::Mutex::new(Some(handle)),
                }),
            }
        };

        // wasm holds the connection and its capture session directly on this
        // Worker — no thread, no channel. `Owned::new` attaches the session over
        // the same `synced_tables` the native actor does.
        #[cfg(target_arch = "wasm32")]
        let database = Database {
            owned: std::rc::Rc::new(std::cell::RefCell::new(Owned::new(conn, &synced_tables)?)),
            hlc,
            synced_tables,
        };

        Ok((database, stamper))
    }

    /// The host's declared synced-table set, the single owner of which tables
    /// participate in changeset sync. The capture session attaches exactly these,
    /// the register-clock seed scanned these, and the gate/apply operate over
    /// these — so the sync layer reads the set from here instead of carrying a
    /// separately-passed copy that could silently diverge.
    pub fn synced_tables(&self) -> &[SyncedTable] {
        &self.synced_tables
    }

    /// The shared register clock. coven's sync layer advances it past pulled rows
    /// and stamps envelopes off it; it is the same `Arc<Hlc>` the stamper wraps.
    ///
    /// Native-only: the sole consumer is the native-only [`crate::sync::sync_manager::SyncManager`].
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn hlc(&self) -> Arc<Hlc> {
        self.hlc.clone()
    }

    /// The receiver's current wall-clock millis, read from this database's
    /// register clock. The pull reads it once and passes it down to bound an
    /// incoming `_updated_at`'s physical component (a grossly-future stamp must not
    /// win last-writer-wins or ratchet the clock). Available on both targets — pull
    /// runs on wasm too — unlike [`Self::hlc`], whose only caller is native.
    pub(crate) fn receive_wall_ms(&self) -> u64 {
        self.hlc.wall_now_ms()
    }

    /// Run `f` against the connection and await the result.
    ///
    /// This is how the host runs its app SQL and how coven runs bookkeeping,
    /// gating, and apply — anything that needs `&Connection`. Writes issued here
    /// are captured by the attached session.
    ///
    /// Native runs `f` on the connection thread; the `Send` bounds let the closure
    /// and its reply cross to that thread. wasm runs `f` directly on the borrowed
    /// connection on this Worker and returns a ready future, so it needs no `Send`
    /// — the closure never leaves the thread.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn call<F, R>(&self, f: F) -> Result<R, DbError>
    where
        F: FnOnce(&Connection) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let boxed: Box<dyn FnOnce(&Connection) + Send> = Box::new(move |conn| {
            let _ = reply_tx.send(f(conn));
        });
        self.tx
            .send(Request::Call(boxed))
            .map_err(|_| DbError("db thread is gone".to_string()))?;
        reply_rx
            .await
            .map_err(|_| DbError("db thread dropped the reply".to_string()))?
    }

    /// See the native `call`. wasm borrows the connection, runs `f`, drops the
    /// borrow, and returns a ready future — the borrow never spans an `.await`.
    #[cfg(target_arch = "wasm32")]
    pub async fn call<F, R>(&self, f: F) -> Result<R, DbError>
    where
        F: FnOnce(&Connection) -> Result<R, DbError> + 'static,
        R: 'static,
    {
        let owned = self.owned.borrow();
        f(&owned.conn)
    }

    /// Capture the outgoing changeset bytes and reset the recorded batch, leaving
    /// capture ENABLED. The returned bytes are the local changes recorded since the
    /// last capture; empty bytes mean none. Capture is NOT suspended — a host write
    /// after this is recorded into the next batch. (SQLite cannot clear a session
    /// in place, so the reset drops and immediately recreates the session, all
    /// within this one actor request, leaving a fresh enabled session attached.)
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn take_changeset(&self) -> Result<Vec<u8>, DbError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::TakeChangeset(reply_tx))
            .map_err(|_| DbError("db thread is gone".to_string()))?;
        reply_rx
            .await
            .map_err(|_| DbError("db thread dropped the reply".to_string()))?
    }

    /// See the native `take_changeset`. Same logic the actor runs for
    /// `Request::TakeChangeset`, inline on the borrowed session: capture the batch,
    /// then reset it unconditionally so the result (Ok or Err) leaves a fresh
    /// enabled session attached.
    #[cfg(target_arch = "wasm32")]
    pub(crate) async fn take_changeset(&self) -> Result<Vec<u8>, DbError> {
        let mut owned = self.owned.borrow_mut();
        let bytes = capture_changeset(owned.session.as_mut());
        reset_session(&mut owned, &self.synced_tables);
        bytes
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
        let (reply_tx, reply_rx) = oneshot::channel();
        let boxed: Box<dyn FnOnce(&Connection) + Send> = Box::new(move |conn| {
            let _ = reply_tx.send(f(conn));
        });
        self.tx
            .send(Request::ApplyChangeset(boxed))
            .map_err(|_| DbError("db thread is gone".to_string()))?;
        reply_rx
            .await
            .map_err(|_| DbError("db thread dropped the reply".to_string()))?
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
        let mut owned = self.owned.borrow_mut();
        let Owned { session, conn } = &mut *owned;
        if let Some(s) = session.as_mut() {
            s.set_enabled(false);
        } else {
            error!("capture session absent at apply; applying with capture off (already not recording)");
        }
        let result = f(conn);
        if let Some(s) = session.as_mut() {
            s.set_enabled(true);
        }
        result
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

    pub(crate) async fn set_sync_state(&self, key: &str, value: &str) -> Result<(), DbError> {
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

    /// The device-local cache-size budget in bytes, or `None` if the host has not
    /// set one. `None` means unlimited — eviction is off and the cache grows
    /// without bound; the host opts into a budget by calling
    /// [`Self::set_max_cache_size`]. Stored as a single decimal value under
    /// [`crate::blob::cache::MAX_CACHE_SIZE_STATE_KEY`] in `sync_state` (config, not
    /// per-blob accounting — the cache's truth is still the folder on disk).
    pub async fn get_max_cache_size(&self) -> Result<Option<u64>, DbError> {
        match self
            .get_sync_state(crate::blob::cache::MAX_CACHE_SIZE_STATE_KEY)
            .await?
        {
            Some(raw) => raw.parse::<u64>().map(Some).map_err(|e| {
                DbError(format!(
                    "max_cache_size in sync_state is not a byte count: {e}"
                ))
            }),
            None => Ok(None),
        }
    }

    /// Set the device-local cache-size budget in bytes. Once set, a populate that
    /// pushes `storage/cache/` over this total evicts its oldest files (by mtime)
    /// back under it; `pinned/` is never counted or touched. Stored under
    /// [`crate::blob::cache::MAX_CACHE_SIZE_STATE_KEY`] in `sync_state`.
    pub async fn set_max_cache_size(&self, max_bytes: u64) -> Result<(), DbError> {
        self.set_sync_state(
            crate::blob::cache::MAX_CACHE_SIZE_STATE_KEY,
            &max_bytes.to_string(),
        )
        .await
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

    pub(crate) async fn set_sync_cursor(&self, device_id: &str, seq: u64) -> Result<(), DbError> {
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
        let updated_at = self.hlc.now().to_string();
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
    pub async fn enqueue_delete(&self, cloud_key: &str, created_at: &str) -> Result<(), DbError> {
        let (cloud_key, created_at) = (cloud_key.to_string(), created_at.to_string());
        // A delete touches no key, so it carries no scope — the column is
        // nullable and a delete row leaves it NULL.
        self.call(move |conn| {
            // Latest intent wins: queuing a delete cancels a pending upload of the
            // same key (the mirror of `enqueue_upload_on`), so an enqueued-then-
            // deleted blob isn't uploaded only to be tombstoned in the same cycle.
            // It also drops a pending tombstone-cancel for the key: a fresh delete
            // wants the blob tombstoned, so a leftover cancel (which would remove
            // that tombstone) must not survive to undo it.
            conn.execute(
                "DELETE FROM cloud_outbox \
                 WHERE operation IN ('upload', 'cancel') AND cloud_key = ?1",
                [&cloud_key],
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
        })
        .await
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
                            retain_pinned, created_at, attempt_count, last_error, last_attempt_at \
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

    /// Atomically complete an upload whose inline tombstone-cancel failed: remove
    /// the upload row `id` and enqueue a `cancel` row for `cloud_key` in one
    /// transaction. A re-uploaded blob must have its tombstone removed or a later
    /// GC deletes the live blob; doing both writes together means the durable
    /// cancel intent exists the instant the upload row is gone, so a crash can
    /// never drop the upload (it is no longer queued) while leaving the tombstone
    /// (no cancel queued). The tombstone-cancel drain retries the removal until it
    /// lands. Used only on the inline-cancel-failed path — a successful inline
    /// cancel needs no row and just calls [`Self::remove_cloud_outbox_entry`].
    pub async fn complete_upload_canceling_tombstone(
        &self,
        id: i64,
        cloud_key: &str,
        created_at: &str,
    ) -> Result<(), DbError> {
        let (cloud_key, created_at) = (cloud_key.to_string(), created_at.to_string());
        self.call(move |conn| {
            // One transaction so the row-removal and the cancel-insert commit
            // together: a crash mid-pair must never leave the upload row gone
            // while no cancel is queued (the tombstone would then outlive every
            // retry). `unchecked_transaction` takes `&Connection`, which is what
            // `call` hands the closure.
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            tx.execute("DELETE FROM cloud_outbox WHERE id = ?1", [id])
                .map_err(DbError::from)?;
            tx.execute(
                "INSERT OR IGNORE INTO cloud_outbox \
                 (operation, cloud_key, scope, created_at) \
                 VALUES ('cancel', ?1, NULL, ?2)",
                (cloud_key, created_at),
            )
            .map_err(DbError::from)?;
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    /// Drop any queued uploads for `cloud_key`. The host calls this when a file
    /// is deleted before its upload ran — no point uploading what's about to go.
    pub async fn remove_cloud_outbox_uploads_for_key(
        &self,
        cloud_key: &str,
    ) -> Result<(), DbError> {
        let cloud_key = cloud_key.to_string();
        self.call(move |conn| {
            conn.execute(
                "DELETE FROM cloud_outbox WHERE operation = 'upload' AND cloud_key = ?1",
                [cloud_key],
            )
            .map(|_| ())
            .map_err(DbError::from)
        })
        .await
    }

    /// Clear the retry-backoff timestamp on failed uploads so the next cycle
    /// retries them immediately. Backs a host "retry now" action.
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
        let (error, attempted_at) = (error.to_string(), attempted_at.to_string());
        self.call(move |conn| {
            conn.execute(
                "UPDATE cloud_outbox \
                 SET attempt_count = attempt_count + 1, last_error = ?1, last_attempt_at = ?2 \
                 WHERE id = ?3",
                (error, attempted_at, id),
            )
            .map(|_| ())
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
        created_at: r.get(7)?,
        attempt_count: r.get(8)?,
        last_error: r.get(9)?,
        last_attempt_at: r.get(10)?,
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

/// The actor loop: owns the connection and its capture session, processing one
/// request at a time on this thread.
#[cfg(not(target_arch = "wasm32"))]
fn run_actor(
    conn: Connection,
    synced_tables: &[SyncedTable],
    rx: std::sync::mpsc::Receiver<Request>,
) {
    // A failure here leaves capture off; the connection still serves reads/writes,
    // so log it loudly rather than aborting the actor. The next `TakeChangeset`
    // re-attempts the attach (its reset recreates the session) and surfaces its own
    // error to the caller.
    let mut session = match attach_session(&conn, synced_tables) {
        Ok(s) => Some(s),
        Err(e) => {
            error!("capture session not attached at startup: {e}");
            None
        }
    };

    while let Ok(req) = rx.recv() {
        match req {
            Request::Call(f) => {
                // A host/coven closure must not be able to kill the actor: every
                // SQL the app runs goes through here, so a panic in one closure
                // would leave every later `Database::call` returning "db thread is
                // gone". Catch it, log it, keep serving. The closure owns its own
                // reply oneshot; a panic drops it un-sent, so the caller already
                // observes a "dropped the reply" `DbError` — we just don't let the
                // panic propagate past this thread.
                if let Err(panic) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&conn)))
                {
                    error!(
                        panic = %panic_message(&panic),
                        "db closure panicked; the caller's reply was dropped, actor stays alive"
                    );
                }
            }
            Request::TakeChangeset(reply) => {
                // Capture the recorded batch, then reset it by dropping and
                // recreating the session — synchronously, here in the one actor
                // request — so the loop continues with a fresh ENABLED session and
                // capture is never suspended across a caller's `await`.
                let bytes = capture_changeset(session.as_mut());
                reset_session(&mut session, &conn, synced_tables);
                if reply.send(bytes).is_err() {
                    debug!("db actor: changeset-capture caller dropped its reply receiver");
                }
            }
            Request::ApplyChangeset(f) => {
                // Disable recording on the live session around just the apply, so
                // the applied rows aren't echoed, then re-enable it. The session
                // stays attached — host writes before/after (other `Call`s) are
                // captured. Caught like `Call`: an apply closure panic must not
                // brick the actor, and must leave capture re-enabled.
                if let Some(s) = session.as_mut() {
                    s.set_enabled(false);
                } else {
                    error!("capture session absent at apply; applying with capture off (already not recording)");
                }
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&conn)));
                if let Some(s) = session.as_mut() {
                    s.set_enabled(true);
                }
                if let Err(panic) = outcome {
                    error!(
                        panic = %panic_message(&panic),
                        "apply closure panicked; the caller's reply was dropped, actor stays alive (capture re-enabled)"
                    );
                }
            }
        }
    }
}

/// Best-effort string from a `catch_unwind` panic payload (the common
/// `&str`/`String` cases; otherwise a placeholder).
#[cfg(not(target_arch = "wasm32"))]
fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
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
/// went uncaptured. Shared by the native actor and the wasm inline
/// path so both drain identically.
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

/// Reset the recorded batch by dropping the session and immediately recreating it,
/// since SQLite cannot clear a session in place. Leaves a fresh ENABLED session
/// attached so capture continues uninterrupted — this does NOT suspend. A
/// recreate failure leaves `session = None`, logged loudly; the next capture then
/// surfaces an error (host writes went uncaptured) and re-attempts the attach.
#[cfg(not(target_arch = "wasm32"))]
fn reset_session<'c>(
    session: &mut Option<rusqlite::session::Session<'c>>,
    conn: &'c Connection,
    synced_tables: &[SyncedTable],
) {
    *session = None;
    match attach_session(conn, synced_tables) {
        Ok(s) => *session = Some(s),
        Err(e) => error!("failed to recreate capture session after changeset capture: {e}"),
    }
}

/// wasm form of [`reset_session`]: recreates the session against the `Owned`
/// connection with its lifetime erased to `'static` for storage. Same semantics —
/// a fresh enabled session, no suspend; a failure leaves `None`, logged loudly.
#[cfg(target_arch = "wasm32")]
fn reset_session(owned: &mut Owned, synced_tables: &[SyncedTable]) {
    owned.session = None;
    match attach_erased_session(&owned.conn, synced_tables) {
        Ok(s) => owned.session = Some(s),
        Err(e) => error!("failed to recreate capture session after changeset capture: {e}"),
    }
}

/// Map a library DB `path` to the flat OPFS filename the connection opens
/// against, since the opfs-sahpool VFS has no directories.
///
/// The mapping is a hash of the full path, not its file-name component: coven
/// names every library's database the same way (`.../{library_id}/library.db`),
/// so the file name alone collapses distinct libraries onto one OPFS file. The
/// full-path hash is unique per library and deterministic, so a reopen on the
/// same `path` resolves the same OPFS file and reads back what the first open
/// wrote. `:memory:` passes through unchanged so an in-memory open stays in
/// memory (each such connection is its own private database, never touching OPFS).
#[cfg(target_arch = "wasm32")]
fn opfs_filename(path: &Path) -> Result<String, DbError> {
    use sha2::{Digest, Sha256};
    if path.as_os_str() == ":memory:" {
        return Ok(":memory:".to_string());
    }
    let path_str = path.to_str().ok_or_else(|| {
        DbError(format!(
            "wasm: non-UTF-8 db path cannot map to an OPFS file: {path:?}"
        ))
    })?;
    let digest = Sha256::digest(path_str.as_bytes());
    Ok(format!("coven-{}.db", hex::encode(&digest[..16])))
}
