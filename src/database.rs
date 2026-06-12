//! The owned SQLite connection.
//!
//! coven owns one `rusqlite::Connection` on a dedicated OS thread (the actor),
//! together with a persistent session attached to the synced tables. Every
//! database access — the host's app SQL, coven's bookkeeping, changeset capture
//! and apply — is a request run on that one thread, so the connection never
//! crosses a thread boundary and access is serialized without a lock.
//!
//! The host no longer opens a connection, lends a raw pointer, or implements
//! bookkeeping. It hands coven a path and a schema migration, then runs its own
//! SQL through [`Database::call`].

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};
use tokio::sync::oneshot;
use tracing::{error, warn};

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
enum Request {
    /// Run an arbitrary closure against the connection. Host app SQL and coven
    /// bookkeeping both arrive this way; writes are captured by the attached
    /// session. The closure boxes its own typed reply.
    Call(Box<dyn FnOnce(&Connection) + Send>),
    /// Capture the outgoing changeset bytes from the live session, then drop the
    /// session so a subsequent apply is not re-recorded as a local change.
    TakeChangesetAndSuspend(oneshot::Sender<Result<Vec<u8>, DbError>>),
    /// Recreate and re-attach the session after an apply/snapshot, resuming
    /// capture of host writes.
    ResumeSession(oneshot::Sender<Result<(), DbError>>),
}

/// A handle to the owned database. Cloneable; every clone talks to the same
/// connection thread.
///
/// Field drop order is load-bearing: `tx` must drop before `_thread`. The actor
/// loop exits only once every `Request` sender is gone, and `JoinOnDrop` joins
/// the thread; if `_thread` dropped first it would block forever waiting on a
/// loop this handle's `tx` still keeps alive. Rust drops fields top-to-bottom,
/// so `tx` precedes `_thread` here deliberately.
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

/// Joins the actor thread when the last `Database` handle drops, so the
/// connection is closed cleanly.
struct JoinOnDrop {
    handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

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
    /// app's own tables, seeds the register clock off the on-disk rows, attaches
    /// the capture session to `synced_tables`, and spawns the connection thread.
    /// Returns the handle plus the non-optional `_updated_at` stamper the host
    /// binds into every synced-row write.
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
    pub(crate) fn open_with_hlc(
        path: &Path,
        synced_tables: Vec<SyncedTable>,
        hlc: Arc<Hlc>,
        migrate: impl FnOnce(&Connection) -> Result<(), DbError>,
    ) -> Result<(Database, UpdatedAtStamper), DbError> {
        let conn = Connection::open(path).map_err(DbError::from)?;
        // WAL is a no-op on an in-memory db (the pragma reports "memory"); use a
        // query that tolerates the returned row rather than `pragma_update`,
        // which errors when a pragma yields rows.
        conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
            .map_err(DbError::from)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(DbError::from)?;

        // coven's bookkeeping tables first, then the host's schema.
        conn.execute_batch(MIGRATION_SQL).map_err(DbError::from)?;
        migrate(&conn)?;

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

        let (tx, rx) = std::sync::mpsc::channel::<Request>();
        let actor_tables = synced_tables.clone();
        let handle = std::thread::Builder::new()
            .name("coven-db".to_string())
            .spawn(move || run_actor(conn, &actor_tables, rx))
            .map_err(|e| DbError(format!("failed to spawn db thread: {e}")))?;

        Ok((
            Database {
                tx,
                hlc,
                synced_tables,
                _thread: Arc::new(JoinOnDrop {
                    handle: std::sync::Mutex::new(Some(handle)),
                }),
            },
            stamper,
        ))
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
    pub(crate) fn hlc(&self) -> Arc<Hlc> {
        self.hlc.clone()
    }

    /// Run `f` against the connection on its owning thread and await the result.
    ///
    /// This is how the host runs its app SQL and how coven runs bookkeeping,
    /// gating, and apply — anything that needs `&Connection`. Writes issued here
    /// are captured by the attached session.
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

    /// Capture the outgoing changeset and suspend the session. The returned bytes
    /// are the recorded local changes since the last capture; after this, the
    /// session is dropped so a subsequent [`Database::apply`] does not re-record
    /// the applied rows. Empty bytes mean no local changes.
    pub(crate) async fn take_changeset_and_suspend(&self) -> Result<Vec<u8>, DbError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::TakeChangesetAndSuspend(reply_tx))
            .map_err(|_| DbError("db thread is gone".to_string()))?;
        reply_rx
            .await
            .map_err(|_| DbError("db thread dropped the reply".to_string()))?
    }

    /// Recreate and re-attach the session, resuming capture of host writes. Call
    /// after the apply phase of a cycle.
    pub(crate) async fn resume_session(&self) -> Result<(), DbError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::ResumeSession(reply_tx))
            .map_err(|_| DbError("db thread is gone".to_string()))?;
        reply_rx
            .await
            .map_err(|_| DbError("db thread dropped the reply".to_string()))?
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

    // ---- Cloud outbox ----

    /// Enqueue a blob upload. `scope` names which key the blob is encrypted
    /// under (master or a derived scope); coven persists it on the row and
    /// resolves it to a key at drain, long after the enqueue site is gone.
    /// Idempotent on `(operation, cloud_key)`.
    pub async fn enqueue_upload(
        &self,
        file_id: &str,
        cloud_key: &str,
        source_path: Option<&str>,
        scope: crate::blob::BlobScope,
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
        created_at: &str,
    ) -> Result<(), DbError> {
        conn.execute(
            "INSERT OR IGNORE INTO cloud_outbox \
             (operation, file_id, cloud_key, source_path, scope, created_at) \
             VALUES ('upload', ?1, ?2, ?3, ?4, ?5)",
            (
                file_id,
                cloud_key,
                source_path,
                scope.to_outbox_str(),
                created_at,
            ),
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Enqueue a blob delete. The drain removes it from the cloud on the next
    /// sync cycle once the cloud is reachable. Idempotent on `(operation, cloud_key)`.
    pub async fn enqueue_delete(&self, cloud_key: &str, created_at: &str) -> Result<(), DbError> {
        let (cloud_key, created_at) = (cloud_key.to_string(), created_at.to_string());
        // A delete touches no key, so it carries no scope — the column is
        // nullable and a delete row leaves it NULL.
        self.call(move |conn| {
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

    async fn pending_outbox(&self, op_str: &'static str) -> Result<Vec<OutboxEntry>, DbError> {
        self.call(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT id, operation, file_id, cloud_key, source_path, scope, \
                            created_at, attempt_count, last_error, last_attempt_at \
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

    /// Whether any upload is still queued. The host's changeset push gates on
    /// this so peers never learn of a row whose blob isn't in the cloud yet.
    pub async fn has_pending_cloud_uploads(&self) -> Result<bool, DbError> {
        self.call(move |conn| {
            conn.query_row(
                "SELECT 1 FROM cloud_outbox WHERE operation = 'upload' LIMIT 1",
                [],
                |_| Ok(()),
            )
            .optional()
            .map(|o| o.is_some())
            .map_err(DbError::from)
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
            }
        }
        "delete" => OutboxOperation::Delete,
        other => panic!("invalid cloud_outbox.operation: {other:?}"),
    };
    Ok(OutboxEntry {
        id: r.get(0)?,
        cloud_key: r.get(3)?,
        created_at: r.get(6)?,
        attempt_count: r.get(7)?,
        last_error: r.get(8)?,
        last_attempt_at: r.get(9)?,
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
fn run_actor(
    conn: Connection,
    synced_tables: &[SyncedTable],
    rx: std::sync::mpsc::Receiver<Request>,
) {
    let mut session = attach_session(&conn, synced_tables);

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
            Request::TakeChangesetAndSuspend(reply) => {
                let result = match session.as_mut() {
                    Some(s) => {
                        let mut buf = Vec::new();
                        s.changeset_strm(&mut buf)
                            .map(|()| buf)
                            .map_err(DbError::from)
                    }
                    // An absent session is exceptional: it means a prior resume
                    // failed to re-attach. Reporting "no local changes" would mask
                    // that the device has silently stopped capturing host writes,
                    // so make the absence loud. The cycle's unconditional resume
                    // (below, in the sync cycle) re-attaches afterward.
                    None => {
                        warn!(
                            "capture session unexpectedly absent at changeset capture; \
                             reporting no local changes (a prior resume must have failed)"
                        );
                        Ok(Vec::new())
                    }
                };
                session = None;
                let _ = reply.send(result);
            }
            Request::ResumeSession(reply) => {
                session = attach_session(&conn, synced_tables);
                let ok = if session.is_some() {
                    Ok(())
                } else {
                    Err(DbError("failed to re-attach capture session".to_string()))
                };
                let _ = reply.send(ok);
            }
        }
    }
}

/// Best-effort string from a `catch_unwind` panic payload (the common
/// `&str`/`String` cases; otherwise a placeholder).
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
) -> Option<rusqlite::session::Session<'c>> {
    let mut session = match rusqlite::session::Session::new(conn) {
        Ok(s) => s,
        Err(e) => {
            warn!("failed to create capture session: {e}");
            return None;
        }
    };
    for t in synced_tables {
        if let Err(e) = session.attach(Some(t.name())) {
            warn!(
                table = t.name(),
                "failed to attach synced table to session: {e}"
            );
            return None;
        }
    }
    Some(session)
}
