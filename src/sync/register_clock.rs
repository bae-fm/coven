//! coven's `_updated_at` register clock, built and seeded independently of the
//! cloud/encryption sync machinery.
//!
//! The register is just a [`Hlc`] plus its restart seeding: it needs a device id
//! and the database (to read its persisted floor), and nothing about encryption,
//! keys, or a cloud provider. A host opening a brand-new local-only library can
//! therefore obtain a stamper for its write path without minting an encryption
//! key or constructing a [`SyncManager`](crate::sync::sync_manager::SyncManager) —
//! the manager is built lazily, only once a provider is connected, and borrows
//! this same clock so its envelope/membership stamps and advance-on-pull share
//! the instance the host stamps rows from.

use std::ffi::{c_char, c_int, CStr, CString};
use std::ptr;
use std::sync::Arc;

use libsqlite3_sys as ffi;

use crate::db::SyncDb;
use crate::sync::hlc::{Hlc, Timestamp, UpdatedAtStamper, HIGHWATER_STATE_KEY};
use crate::sync::session::synced_tables;

/// coven's `_updated_at` register: a seeded [`Hlc`] shared between the host's
/// write path (via [`RegisterClock::updated_at_stamper`]) and, once connected,
/// the sync layer (via [`RegisterClock::hlc`]).
pub struct RegisterClock {
    hlc: Arc<Hlc>,
}

impl RegisterClock {
    /// Build the register and seed it so it cannot mint a stamp behind a value
    /// already on disk across a restart.
    ///
    /// The seed floor is `max(persisted high-water, max synced-row
    /// `_updated_at`)`. The flushed high-water mark covers envelope/membership
    /// stamps but lags any local row stamp minted between cycles (it's flushed
    /// only at cycle end); the on-disk row scan is the register's own ground
    /// truth, immune to flush timing. [`Hlc::seed`] is monotonic, so seeding
    /// from each candidate in turn lands on their max.
    ///
    /// coven derives the on-disk floor itself rather than asking the host for it:
    /// it already knows the synced tables ([`synced_tables`], registered before
    /// this call), requires each to carry `_updated_at`, and drives the raw write
    /// handle directly. It scans `SELECT MAX(_updated_at)` over each registered
    /// table on the raw handle and takes the overall lexicographic max — no
    /// host-implemented query to get wrong (a host that omits a table or fumbles
    /// NULL would silently break restart seeding).
    ///
    /// Every read runs here; a read or parse error is surfaced rather than
    /// swallowed — starting with an unseeded clock could mint stamps behind
    /// existing rows and silently lose merges. An absent value (fresh library, or
    /// a synced table with no rows) is distinct from a failure and simply
    /// contributes no floor. A registered synced table that does not exist on the
    /// raw handle is a host integration error and surfaces as `Err`, not a skip.
    ///
    /// Precondition: [`crate::sync::session::set_synced_tables`] must already have
    /// run (a documented startup step) so the scan sees the host's tables.
    pub async fn open(device_id: String, db: &dyn SyncDb) -> Result<Self, String> {
        let hlc = Arc::new(Hlc::new(device_id));

        let persisted_high_water = db
            .get_sync_state(HIGHWATER_STATE_KEY)
            .await
            .map_err(|e| format!("Failed to read HLC high-water mark: {e}"))?;
        seed_from_stamp(
            &hlc,
            persisted_high_water,
            "HLC high-water mark in sync_state",
        )?;

        let on_disk_row_floor = scan_max_synced_updated_at(db).await?;
        seed_from_stamp(&hlc, on_disk_row_floor, "`_updated_at` in synced tables")?;

        Ok(Self { hlc })
    }

    /// A cloneable handle over the register, for the host's database write path.
    /// The host obtains this once after [`RegisterClock::open`] and injects it
    /// into the write path; every synced-row `_updated_at` write then uses
    /// [`UpdatedAtStamper::stamp`], so the host's writes and coven's sync layer
    /// share one logical clock (every clone wraps the same `Arc<Hlc>`, so coven's
    /// seeding and advance-on-pull reach every stamp the host mints).
    ///
    /// Ordering requirement: obtain and inject the stamper before any synced-row
    /// write occurs, or an early write stamps off a clock the host isn't sharing.
    pub fn updated_at_stamper(&self) -> UpdatedAtStamper {
        UpdatedAtStamper::new(self.hlc.clone())
    }

    /// The shared `Arc<Hlc>` the sync layer drives: it advances the clock past
    /// every pulled row and stamps envelope/membership entries off it, so those
    /// and the host's stamps all order against one register.
    pub(crate) fn hlc(&self) -> Arc<Hlc> {
        self.hlc.clone()
    }
}

/// Seed the register from one candidate floor, if present. `value` is a resolved
/// register stamp (the persisted high-water mark, or the on-disk row max); a
/// `None` simply contributes no floor. A present-but-unparseable value is a
/// corrupt register and aborts — `context` names which source it came from so the
/// error is actionable. [`Hlc::seed`] is monotonic, so seeding from each
/// candidate in turn lands the clock on their max.
fn seed_from_stamp(hlc: &Hlc, value: Option<String>, context: &str) -> Result<(), String> {
    if let Some(stamp) = value {
        let floor =
            Timestamp::parse(&stamp).ok_or_else(|| format!("Corrupt {context}: {stamp:?}"))?;
        hlc.seed(&floor);
    }
    Ok(())
}

/// Scan `SELECT MAX(_updated_at)` over every registered synced table on the raw
/// write handle and return the overall lexicographic max, or `None` when no
/// synced row exists anywhere.
///
/// This is coven's own ground truth for the register floor — derived from the
/// tables coven already knows ([`synced_tables`]) against the handle it already
/// drives, rather than a host-implemented query the host could get wrong. The
/// returned string is an opaque HLC stamp; the caller parses and seeds with it.
///
/// Errors (surfaced, never swallowed):
/// - acquiring the raw handle fails;
/// - a prepare/step fails — including a registered table that does not exist on
///   the handle (`SELECT MAX(_updated_at) FROM "missing"` errors at prepare).
///   That is a host integration bug (a synced table was registered but never
///   created), not a benign empty result, so it must abort rather than silently
///   drop a table from the floor.
///
/// A table with no rows (or all-`NULL` `_updated_at`) yields SQL `NULL` and
/// contributes nothing — distinct from an error.
async fn scan_max_synced_updated_at(db: &dyn SyncDb) -> Result<Option<String>, String> {
    let raw = db
        .raw_write_handle()
        .await
        .map_err(|e| format!("Failed to acquire raw write handle to seed register floor: {e}"))?;

    // SAFETY: `raw` is the host's open write connection (the same one the session
    // extension attaches to); we only read from it synchronously here, holding it
    // across no await points. The `!Send` pointer never crosses a yield.
    unsafe { max_synced_updated_at_on(raw) }
}

/// Synchronous half of [`scan_max_synced_updated_at`]: the FFI loop over the
/// registered tables on an already-acquired connection.
///
/// # Safety
/// `db` must be a valid, open sqlite3 connection.
unsafe fn max_synced_updated_at_on(db: *mut ffi::sqlite3) -> Result<Option<String>, String> {
    let mut overall: Option<String> = None;

    for table in synced_tables() {
        // `table` comes from `set_synced_tables` (trusted host input) and cannot
        // be bound as a parameter; quote it as an identifier (doubling any inner
        // quote) so it interpolates safely.
        let quoted = format!("\"{}\"", table.replace('"', "\"\""));
        let sql = format!("SELECT MAX(_updated_at) FROM {quoted}");
        let c_sql = CString::new(sql.as_str())
            .map_err(|e| format!("synced table name not representable as SQL: {table}: {e}"))?;

        let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
        let rc = ffi::sqlite3_prepare_v2(db, c_sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
        if rc != ffi::SQLITE_OK as c_int {
            let msg = sqlite_errmsg(db);
            return Err(format!(
                "prepare of register-floor scan over synced table {table} failed (rc={rc}): {msg}"
            ));
        }

        let step = ffi::sqlite3_step(stmt);
        if step != ffi::SQLITE_ROW as c_int {
            // A `SELECT MAX(...)` always returns exactly one row; anything else is
            // an error, not a benign empty result.
            let msg = sqlite_errmsg(db);
            ffi::sqlite3_finalize(stmt);
            return Err(format!(
                "register-floor scan over synced table {table} did not return a row \
                 (rc={step}): {msg}"
            ));
        }

        // SQL `NULL` (no rows / all-NULL column) → column type NULL → no
        // contribution; only a real text value seeds the floor.
        let value = if ffi::sqlite3_column_type(stmt, 0) == ffi::SQLITE_NULL as c_int {
            None
        } else {
            let text = ffi::sqlite3_column_text(stmt, 0);
            if text.is_null() {
                None
            } else {
                Some(
                    CStr::from_ptr(text as *const c_char)
                        .to_string_lossy()
                        .into_owned(),
                )
            }
        };
        ffi::sqlite3_finalize(stmt);

        if let Some(v) = value {
            overall = Some(match overall {
                Some(cur) if cur >= v => cur,
                _ => v,
            });
        }
    }

    Ok(overall)
}

/// The latest sqlite error message on `db` as an owned string.
///
/// # Safety
/// `db` must be a valid, open sqlite3 connection.
unsafe fn sqlite_errmsg(db: *mut ffi::sqlite3) -> String {
    let ptr = ffi::sqlite3_errmsg(db);
    if ptr.is_null() {
        return "(no sqlite error message)".to_string();
    }
    CStr::from_ptr(ptr).to_string_lossy().into_owned()
}
