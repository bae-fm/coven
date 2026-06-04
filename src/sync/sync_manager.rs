//! High-level sync manager: lifecycle, membership, status.
//!
//! Owns the sync lifecycle — cloud home + sync loop — and starts/stops it when
//! a provider is connected/disconnected, no app restart required. The host
//! supplies the config snapshot, keys, encryption, database, clock, and blob
//! handling; coven drives the rest.

use std::ffi::{c_char, c_int, CStr, CString};
use std::ptr;
use std::sync::{Arc, RwLock};

use libsqlite3_sys as ffi;
use tracing::info;

use crate::blob::{BlobPlan, BlobUploadObserver};
use crate::clock::ClockRef;
use crate::config::Config;
use crate::db::SyncDb;
use crate::encryption::EncryptionService;
use crate::keys::KeyService;
use crate::storage::cloud::CloudHome;
use crate::sync::hlc::{Hlc, Timestamp, HIGHWATER_STATE_KEY};
use crate::sync::membership::MemberRole;
use crate::sync::session::synced_tables;
use crate::sync::storage::SyncStorage;
use crate::sync::sync_loop::SyncLoopHandle;

/// Supplies the host's current config on demand. coven reads it fresh each call
/// — never snapshotting or writing it — so a host with reactive config sees
/// changes without rebuilding the manager.
pub type ConfigProvider = Arc<dyn Fn() -> Config + Send + Sync>;

/// High-level sync manager.
///
/// Always has a valid EncryptionService — if no encryption key exists,
/// don't create a SyncManager at all.
pub struct SyncManager {
    config_provider: ConfigProvider,
    key_service: KeyService,
    encryption_service: EncryptionService,
    db: Arc<dyn SyncDb>,
    clock: ClockRef,
    blob_plan: Arc<dyn BlobPlan>,
    observer: Option<Arc<dyn BlobUploadObserver>>,

    /// coven's `_updated_at` register. Built and seeded once at construction so
    /// the host can stamp rows before `start_sync()`; the sync loop borrows this
    /// instance rather than minting its own.
    hlc: Arc<Hlc>,

    // Mutable sync state — updated when providers are connected/disconnected
    sync_loop_handle: RwLock<Option<Arc<SyncLoopHandle>>>,
    cloud_home: RwLock<Option<Arc<dyn CloudHome>>>,
}

/// A member as returned by get_members.
#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub pubkey: String,
    pub role: MemberRole,
    pub is_self: bool,
}

/// Sync status snapshot.
#[derive(Debug, Clone)]
pub struct SyncStatus {
    pub configured: bool,
    pub syncing: bool,
    pub last_sync_time: Option<String>,
    pub error: Option<String>,
    pub device_count: u32,
}

impl SyncManager {
    /// Build the manager, seeding the `_updated_at` register so it cannot mint a
    /// stamp behind a value already on disk across a restart.
    ///
    /// The seed floor is `max(persisted high-water, max synced-row
    /// `_updated_at`)`. The flushed high-water mark covers envelope/membership
    /// stamps but lags any local row stamp minted between cycles (it's flushed
    /// only at cycle end); the on-disk row scan is the register's own ground
    /// truth, immune to flush timing. [`Hlc::seed`] is monotonic, so seeding
    /// from each candidate in turn lands on their max.
    ///
    /// coven derives the on-disk floor itself rather than asking the host for it:
    /// it already knows the synced tables ([`crate::sync::session::synced_tables`],
    /// registered before construction), requires each to carry `_updated_at`, and
    /// already drives the raw write handle for the session extension. It scans
    /// `SELECT MAX(_updated_at)` over each registered table on the raw handle and
    /// takes the overall lexicographic max — no host-implemented query to get
    /// wrong (a host that omits a table or fumbles NULL would silently break
    /// restart seeding).
    ///
    /// Every read runs on construction; a read or parse error is surfaced rather
    /// than swallowed — starting with an unseeded clock could mint stamps behind
    /// existing rows and silently lose merges. An absent value (fresh library, or
    /// a synced table with no rows) is distinct from a failure and simply
    /// contributes no floor. A registered synced table that does not exist on the
    /// raw handle is a host integration error and surfaces as `Err`, not a skip.
    ///
    /// Precondition: [`crate::sync::session::set_synced_tables`] must already have
    /// run (a documented startup step) so the scan sees the host's tables.
    pub async fn new(
        config_provider: ConfigProvider,
        key_service: KeyService,
        encryption_service: EncryptionService,
        db: Arc<dyn SyncDb>,
        clock: ClockRef,
        blob_plan: Arc<dyn BlobPlan>,
        observer: Option<Arc<dyn BlobUploadObserver>>,
    ) -> Result<Self, String> {
        let device_id = (config_provider)().device_id;
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

        let on_disk_row_floor = scan_max_synced_updated_at(db.as_ref()).await?;
        seed_from_stamp(&hlc, on_disk_row_floor, "`_updated_at` in synced tables")?;

        Ok(Self {
            config_provider,
            key_service,
            encryption_service,
            db,
            clock,
            blob_plan,
            observer,
            hlc,
            sync_loop_handle: RwLock::new(None),
            cloud_home: RwLock::new(None),
        })
    }

    /// Stamp a synced row's `_updated_at`. The host binds this opaque string
    /// into every synced-row write; it must not parse or compare it as a
    /// wall-clock time. Advancing the clock persists nothing here (the value is
    /// in-memory until a sync cycle flushes the high-water mark), but the clock
    /// is already seeded past existing rows, so the stamp never regresses.
    pub fn stamp_updated_at(&self) -> String {
        self.hlc.now().to_string()
    }

    pub fn encryption_service(&self) -> &EncryptionService {
        &self.encryption_service
    }

    pub fn cloud_home(&self) -> Option<Arc<dyn CloudHome>> {
        self.cloud_home.read().unwrap().clone()
    }

    pub fn sync_loop_handle(&self) -> Option<Arc<SyncLoopHandle>> {
        self.sync_loop_handle.read().unwrap().clone()
    }

    // =========================================================================
    // Sync lifecycle
    // =========================================================================

    /// Initialize cloud home and sync loop from current config.
    /// Called at startup (if already configured) and after connecting a provider.
    pub async fn start_sync(&self) {
        let config = (self.config_provider)();

        // Create cloud home
        let cloud_home: Option<Arc<dyn CloudHome>> = match crate::storage::cloud::create_cloud_home(
            &config,
            &self.key_service,
            self.clock.clone(),
        )
        .await
        {
            Ok(ch) => Some(Arc::from(ch)),
            Err(e) => {
                info!("Cloud home not available: {e}");
                return;
            }
        };

        *self.cloud_home.write().unwrap() = cloud_home;

        if !config.sync_enabled(&self.key_service) {
            return;
        }

        // Initialize sync loop
        let sync_loop = crate::sync::cycle::init_sync(
            &config,
            &self.key_service,
            self.db.as_ref(),
            self.clock.clone(),
            &self.encryption_service,
            self.hlc.clone(),
        )
        .await;

        if let Some(components) = sync_loop {
            let library_dir = config.library_dir.clone();
            let handle = Arc::new(SyncLoopHandle::new(
                components,
                self.db.clone(),
                self.clock.clone(),
                library_dir,
                self.blob_plan.clone(),
                self.observer.clone(),
            ));
            handle.start();

            info!("Sync loop started");
            *self.sync_loop_handle.write().unwrap() = Some(handle);
        }
    }

    /// Tear down the sync loop and cloud home.
    pub fn stop_sync(&self) {
        *self.sync_loop_handle.write().unwrap() = None;
        *self.cloud_home.write().unwrap() = None;

        info!("Sync loop stopped");
    }

    // =========================================================================
    // Status / config queries
    // =========================================================================

    pub fn is_sync_ready(&self) -> bool {
        self.sync_loop_handle
            .read()
            .unwrap()
            .as_ref()
            .is_some_and(|h| h.is_running())
    }

    pub fn trigger_sync(&self) {
        if let Some(ref sync_loop) = *self.sync_loop_handle.read().unwrap() {
            sync_loop.trigger();
        }
    }

    // =========================================================================
    // Keys / codes
    // =========================================================================

    pub fn get_user_pubkey(&self) -> Result<Option<String>, String> {
        self.key_service
            .get_user_public_key()
            .map(|opt| opt.map(hex::encode))
            .map_err(|e| format!("Failed to read user public key: {e}"))
    }

    pub fn generate_restore_code(&self) -> Result<String, String> {
        let config = (self.config_provider)();
        crate::storage::cloud::setup::generate_restore_code(&config, &self.key_service)
            .map_err(|e| e.to_string())
    }

    // =========================================================================
    // Membership
    // =========================================================================

    pub async fn get_members(&self) -> Result<Vec<MemberInfo>, String> {
        let config = (self.config_provider)();
        if !config.sync_enabled(&self.key_service) {
            return Ok(Vec::new());
        }

        let storage = crate::storage::cloud::setup::create_sync_storage(
            &config,
            &self.key_service,
            &Some(self.encryption_service.clone()),
            self.clock.clone(),
        )
        .await
        .map_err(|e| format!("Failed to create storage client: {e}"))?;

        let user_pubkey = self
            .key_service
            .get_user_public_key()
            .map_err(|e| format!("Failed to read user public key: {e}"))?;
        let members = crate::sync::membership_ops::get_members(
            &storage,
            user_pubkey.as_ref().map(|k| k.as_slice()),
        )
        .await
        .map_err(|e| e.0)?;

        Ok(members
            .into_iter()
            .map(|m| MemberInfo {
                pubkey: m.pubkey,
                role: m.role,
                is_self: m.is_self,
            })
            .collect())
    }

    pub async fn invite_member(
        &self,
        public_key_hex: &str,
        role: MemberRole,
    ) -> Result<String, String> {
        let sync_loop = self
            .sync_loop_handle
            .read()
            .unwrap()
            .clone()
            .ok_or("Sync is not configured")?;

        let encryption_key_hex = self
            .key_service
            .get_encryption_key()
            .map_err(|e| format!("Failed to read encryption key: {e}"))?
            .ok_or("Encryption key not configured")?;

        let key_bytes: [u8; 32] = hex::decode(&encryption_key_hex)
            .map_err(|e| format!("Invalid encryption key hex: {e}"))?
            .try_into()
            .map_err(|_| "Encryption key wrong length".to_string())?;

        let (library_id, library_name) = {
            let config = (self.config_provider)();
            (config.library_id.clone(), config.library_name.clone())
        };

        let storage: &dyn SyncStorage = &**sync_loop.storage();
        let cloud_home = sync_loop.storage().cloud_home();

        let invite_code = crate::sync::membership_ops::invite_member(
            storage,
            cloud_home,
            sync_loop.user_keypair(),
            sync_loop.hlc(),
            public_key_hex,
            role,
            &key_bytes,
            &library_id,
            &library_name,
        )
        .await
        .map_err(|e| e.0)?;

        Ok(crate::join_code::encode(&invite_code))
    }

    pub async fn remove_member(&self, public_key_hex: &str) -> Result<String, String> {
        let sync_loop = self
            .sync_loop_handle
            .read()
            .unwrap()
            .clone()
            .ok_or("Sync is not configured")?;

        let storage: &dyn SyncStorage = &**sync_loop.storage();
        let cloud_home = sync_loop.storage().cloud_home();

        let new_key = crate::sync::membership_ops::remove_member(
            storage,
            cloud_home,
            sync_loop.user_keypair(),
            sync_loop.hlc(),
            public_key_hex,
        )
        .await
        .map_err(|e| e.0)?;

        // Rotate the in-use key; the host records the returned fingerprint and
        // that a key is stored in its own config.
        let fingerprint = crate::sync::membership_ops::apply_key_rotation(
            new_key,
            &self.key_service,
            sync_loop.encryption(),
        )
        .map_err(|e| e.0)?;

        Ok(fingerprint)
    }
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
///   created), not a benign empty result, so it must abort construction rather
///   than silently drop a table from the floor.
///
/// A table with no rows (or all-`NULL` `_updated_at`) yields SQL `NULL` and
/// contributes nothing — distinct from an error.
/// Seed the register from one candidate floor, if present. `value` is a resolved
/// register stamp (the persisted high-water mark, or the on-disk row max); a
/// `None` simply contributes no floor. A present-but-unparseable value is a
/// corrupt register and aborts construction — `context` names which source it
/// came from so the error is actionable. [`Hlc::seed`] is monotonic, so seeding
/// from each candidate in turn lands the clock on their max.
fn seed_from_stamp(hlc: &Hlc, value: Option<String>, context: &str) -> Result<(), String> {
    if let Some(stamp) = value {
        let floor =
            Timestamp::parse(&stamp).ok_or_else(|| format!("Corrupt {context}: {stamp:?}"))?;
        hlc.seed(&floor);
    }
    Ok(())
}

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
