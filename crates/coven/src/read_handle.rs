//! The read-only native handle: a same-store secondary reader.
//!
//! Where [`CovenHandle`](crate::CovenHandle) is the one full handle a host opens to
//! drive rows, blobs, and sync, [`CovenReadHandle`] is the deliberately narrow
//! counterpart for a second reader of the *same* store — a separate process (the
//! macOS File Provider extension) or a second in-process handle — that must read
//! while another handle holds the writer open.
//!
//! It is opened with [`Coven::builder(cfg).open_read_only()`](crate::CovenBuilder::open_read_only)
//! and exposes reads only: SQL queries against the connection coven owns, and blob
//! reads (local store, pinned/evictable cache, or a cloud fetch into the cache).
//! There is no write, sync-manager, migration, or stamp API on it — those are absent
//! by construction, so a reader cannot mutate the synced state a concurrent writer
//! owns. Its connection is `SQLITE_OPEN_READONLY`, so even the raw
//! [`sql_read`](CovenReadHandle::sql_read) closure's `&Connection` refuses DML at
//! the SQLite layer.
//!
//! It takes no store lock: it coexists with the writer that holds the exclusive
//! `.coven-lock`, and with any number of other read-only opens (a read-only
//! connection cannot write, so the single-writer lock does not apply to it).
//! Cross-process reads are safe because the writer opens the db in WAL mode.

use std::sync::Arc;

use crate::blob::cache::BlobCacheError;
use crate::blob::BlobRef;
use crate::clock::ClockRef;
use crate::config::Config;
use crate::coven::{CovenError, CovenResult};
use crate::database::Database;
use crate::encryption::SealError;
use crate::keys::{DeviceIdentityCustody, MasterKeyCustody, StoreKeys};
use crate::store_dir::StoreDir;
use crate::sync::storage::SyncStorage;
use crate::sync::sync_manager::ConfigProvider;

/// A read-only handle over one coven store, for a same-store secondary reader.
///
/// Open it with
/// [`Coven::builder(cfg).open_read_only()`](crate::CovenBuilder::open_read_only).
/// Cheap to [`clone`](Clone) — every field is shared (an `Arc` or a `Clone` handle),
/// so a clone reads the same database and storage as the original.
///
/// # What it can do
///
/// - **Rows** — read via [`sql_read`](Self::sql_read). The closure receives the
///   `&Connection` coven owns; because the connection is read-only, any write
///   statement fails at the SQLite layer.
/// - **Blobs** — [`read_blob`](Self::read_blob) and
///   [`open_blob_stream`](Self::open_blob_stream) resolve a blob's locality and serve
///   it from the local store, the cache, or a cloud fetch into the per-device cache.
///   [`is_pinned`](Self::is_pinned) reports whether a set is kept offline.
///
/// It builds read storage from the current [`Config`] on a cloud miss, exactly as a
/// home-less full handle does — there is no sync loop to reuse.
#[derive(Clone)]
pub struct CovenReadHandle {
    db: Database,
    store_dir: StoreDir,
    config_provider: ConfigProvider,
    key_service: StoreKeys,
    key_custody: Arc<dyn MasterKeyCustody>,
    identity_custody: Arc<dyn DeviceIdentityCustody>,
    clock: ClockRef,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
}

impl CovenReadHandle {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        db: Database,
        store_dir: StoreDir,
        config_provider: ConfigProvider,
        key_service: StoreKeys,
        key_custody: Arc<dyn MasterKeyCustody>,
        identity_custody: Arc<dyn DeviceIdentityCustody>,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Self {
        Self {
            db,
            store_dir,
            config_provider,
            key_service,
            key_custody,
            identity_custody,
            clock,
            cloudkit_ops,
        }
    }

    fn config(&self) -> Config {
        (self.config_provider)()
    }

    /// Run a pure read against the connection coven owns and await the result.
    ///
    /// This is the read handle's form of
    /// [`CovenHandle::sql_read`](crate::CovenHandle::sql_read): the closure receives
    /// the `&Connection` directly (coven serializes access on its connection
    /// thread, so this never races the writer's process), and a host closure
    /// written against `CovenHandle::sql_read` ports unchanged here. The connection
    /// is `SQLITE_OPEN_READONLY`: a `SELECT`/`PRAGMA` reads normally, and any
    /// `INSERT`/`UPDATE`/`DELETE`/DDL is refused by SQLite — the read-only
    /// guarantee is enforced at the connection, not left to the caller.
    pub async fn sql_read<F, R>(&self, f: F) -> CovenResult<R>
    where
        F: FnOnce(&rusqlite::Connection) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let outcome = self
            .db
            .call(move |conn| Ok(f(conn)))
            .await
            .map_err(CovenError::from)?;
        outcome
    }

    /// The read [`SyncStorage`] for a cloud miss, or `None` for a home-less store.
    ///
    /// A read-only handle holds no sync manager, so — unlike the full handle — it
    /// always builds storage from the current [`Config`]: `None` when no provider is
    /// configured (a home-less store holds only Local blobs, which never reach the
    /// cloud-miss path), `Some` built from config otherwise. A provider configured
    /// but unbuildable (missing credentials) surfaces its setup error.
    async fn blob_storage(
        &self,
    ) -> Result<Option<Arc<dyn SyncStorage>>, crate::storage::cloud::setup::StorageSetupError> {
        let config = self.config();
        if config.cloud_home.provider.is_none() {
            return Ok(None);
        }
        let storage = crate::storage::cloud::setup::create_sync_storage_with_cloudkit(
            &config,
            &self.key_service,
            self.key_custody.as_ref(),
            self.identity_custody.as_ref(),
            None,
            self.clock.clone(),
            self.cloudkit_ops.clone(),
        )
        .await?;
        Ok(Some(Arc::new(storage)))
    }

    /// Read a blob's whole plaintext through coven's locality-aware read: served from
    /// the user's file (Local user-provided), coven's local store (Local
    /// host-provided), the pinned/evictable cache on a Remote hit, or fetched from
    /// the cloud into the cache on a Remote miss. The read counterpart of
    /// [`CovenHandle::read_blob`](crate::CovenHandle::read_blob).
    ///
    /// A cloud fetch writes the fetched bytes into the per-device cache
    /// (`storage/cache/`) with an atomic temp-then-rename — device scratch, no synced
    /// state touched — so a File Provider materializing remote content works through a
    /// read-only handle. The exact immutable cloud location comes from the database
    /// snapshot; the read performs no cloud listing.
    pub async fn read_blob(&self, blob: &BlobRef) -> Result<Vec<u8>, BlobCacheError> {
        let storage = self.blob_storage().await?;
        crate::blob::cache::read_blob(&self.db, &self.store_dir, storage.as_deref(), blob).await
    }

    /// Serve `len` plaintext bytes of a blob starting at `offset`, for streaming or
    /// seeking without loading the whole file. The ranged sibling of
    /// [`read_blob`](Self::read_blob); a Remote-miss range read fetches from the cloud
    /// but writes no cache file (only a whole-file read populates).
    pub async fn open_blob_stream(
        &self,
        blob: &BlobRef,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, BlobCacheError> {
        let storage = self.blob_storage().await?;
        crate::blob::cache::open_blob_stream(
            &self.db,
            &self.store_dir,
            storage.as_deref(),
            blob,
            offset,
            len,
        )
        .await
    }

    /// Open a payload
    /// [`CovenHandle::seal_app_data`](crate::CovenHandle::seal_app_data) produced,
    /// resolving the store's master keyring through this handle's custody. The read
    /// side of app-data sealing: a secondary reader opens what the writer sealed,
    /// under whichever generation the payload names.
    ///
    /// There is no seal counterpart here — sealing writes new ciphertext, which is
    /// the writer's job; this handle only reads.
    ///
    /// [`SealError::Locked`] if the store is locked; a wrong `aad`, a tampered
    /// payload, an unreadable version, or a generation this store's keyring lacks
    /// each surface their own typed error.
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        crate::handle::app_data_cipher(self.key_custody.as_ref())?.open_app_data(sealed, aad)
    }

    /// Whether every blob in `blobs` is pinned for offline — present in coven's kept
    /// cache folder (`storage/pinned/`). An empty set is vacuously pinned. A read; it
    /// stats the folder, never writes.
    pub async fn is_pinned(&self, blobs: &[BlobRef]) -> Result<bool, BlobCacheError> {
        for blob in blobs {
            if !crate::blob::cache::is_pinned(&self.db, &self.store_dir, blob).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
