//! The read-only handle: a same-store secondary reader.
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
//! There is no write, sync-connection, migration, or stamp API on it — those are absent
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

use crate::blob::RowBlobRef;
use crate::clock::ClockRef;
use crate::coven::CovenResult;
use crate::database::Database;
use crate::database::StoreDatabase;
use crate::encryption::SealError;
use crate::keys::{DeviceIdentityCustody, MasterKeyCustody, StoreKeys};
use crate::read_store_rows::ReadStoreRows;
use crate::store_blobs::{ReadOnlyBlobStorage, ReadStoreBlobs, StoreBlobReads};
use crate::store_dir::StoreDir;
use crate::store_security::StoreSecurity;
use crate::store_sync::ConfigProvider;
use crate::sync::store::blob::{LocalStoreBlobAccess, StoreBlobCache};
use crate::sync::{BlobCacheError, BlobStream};

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
    rows: ReadStoreRows,
    blobs: ReadStoreBlobs,
    security: StoreSecurity,
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
        oauth_clients: crate::oauth::OAuthClients,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        blob_chunking: crate::storage::BlobChunking,
    ) -> Self {
        let database = StoreDatabase::from_database(db);
        let security =
            StoreSecurity::new(key_service, key_custody, identity_custody, oauth_clients);
        let rows = ReadStoreRows::new(database.clone());
        let blob_cache = StoreBlobCache::new(database.clone(), store_dir.clone());
        let local_blob_access =
            LocalStoreBlobAccess::new(database.clone(), store_dir, blob_cache.clone());
        let blob_storage = ReadOnlyBlobStorage::new(
            config_provider,
            security.clone(),
            clock,
            cloudkit_ops,
            blob_chunking,
        );
        let blob_reads = StoreBlobReads::new(local_blob_access, blob_cache, blob_storage);
        let blobs = ReadStoreBlobs::new(database.clone(), blob_reads);
        Self {
            rows,
            blobs,
            security,
        }
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
        self.rows.read(f).await
    }

    /// Capture the exact current blob-bearing row version from this reader's
    /// database snapshot.
    pub async fn row_blob_ref(
        &self,
        table: &str,
        row_id: &str,
    ) -> Result<RowBlobRef, crate::database::DbError> {
        self.blobs.row_blob_ref(table, row_id).await
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
    /// read-only handle. The supplied [`RowBlobRef`] already carries the exact stored
    /// object and authority, so the read performs no database write or cloud listing.
    pub async fn read_blob(&self, blob: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError> {
        self.blobs.read(blob).await
    }

    /// Open an exact row blob's plaintext for ranged reading, for streaming or
    /// seeking without loading the whole file. The ranged sibling of
    /// [`read_blob`](Self::read_blob); the read counterpart of
    /// [`CovenHandle::open_blob_stream`](crate::CovenHandle::open_blob_stream).
    ///
    /// Opening resolves the blob's locality, proves the plaintext's size and content
    /// hash against the row, and holds the open file, so every
    /// [`BlobStream::read_at`] costs only the bytes it returns. A Remote miss fetches
    /// the exact cloud object once and populates the per-device cache
    /// (`storage/cache/`) with an atomic temp-then-link — device scratch, no synced
    /// state touched — so a File Provider serving ranges through a read-only handle
    /// downloads the object once per opened stream, not once per range.
    pub async fn open_blob_stream(&self, blob: &RowBlobRef) -> Result<BlobStream, BlobCacheError> {
        self.blobs.open_stream(blob).await
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
        self.security.open_app_data(sealed, aad)
    }

    /// Whether every blob in `blobs` is pinned for offline — present in coven's kept
    /// cache folder (`storage/pinned/`). An empty set is vacuously pinned. A read; it
    /// stats the folder, never writes.
    pub async fn is_pinned(&self, blobs: &[RowBlobRef]) -> Result<bool, BlobCacheError> {
        self.blobs.all_pinned(blobs).await
    }
}
