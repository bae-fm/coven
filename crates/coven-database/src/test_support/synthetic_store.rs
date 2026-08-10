//! The synthetic store every test database opens over: its synced schema, its
//! migration ladder, and the `Database` constructors that combine the two.
//!
//! Domain-free on purpose — three tables exercising the engine's generic
//! mechanics rather than any host's real shape.

use crate::Migration;
use crate::{Database, DbError};
use coven_protocol::synced_schema::{BlobDecl, SyncedTable};

/// The database and Store directory produced by one synthetic Store fixture.
///
/// These are sibling construction outputs: tests name the dependency they use
/// instead of treating the bundle as a database or recovering the directory
/// from a database owner.
#[derive(Clone)]
pub struct SyntheticStoreFixture {
    pub database: Database,
    pub store_dir: coven_foundation::store_dir::StoreDir,
}

impl SyntheticStoreFixture {
    pub fn open(
        path: &std::path::Path,
        tables: Vec<SyncedTable>,
        grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        device_id: String,
        clock: coven_foundation::clock::ClockRef,
        migrations: &[Migration],
    ) -> Result<Self, crate::OpenError> {
        let store_dir = store_dir_for_test_database(path);
        let database = Database::open_in_store_dir_for_test(
            path,
            store_dir.clone(),
            tables,
            grace,
            transfer_limits,
            device_id,
            clock,
            migrations,
        )?;
        Ok(Self {
            database,
            store_dir,
        })
    }

    pub fn open_with_hlc(
        path: &std::path::Path,
        tables: Vec<SyncedTable>,
        grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        hlc: std::sync::Arc<coven_protocol::hlc::Hlc>,
        migrations: &[Migration],
    ) -> Result<Self, crate::OpenError> {
        let store_dir = store_dir_for_test_database(path);
        let database = Database::open_with_hlc_in_store_dir_for_test(
            path,
            store_dir.clone(),
            tables,
            grace,
            transfer_limits,
            hlc,
            migrations,
        )?;
        Ok(Self {
            database,
            store_dir,
        })
    }
}

fn store_dir_for_test_database(path: &std::path::Path) -> coven_foundation::store_dir::StoreDir {
    if path == std::path::Path::new(":memory:") {
        coven_foundation::store_dir::StoreDir::new_ephemeral(
            std::env::temp_dir().join(format!("coven-test-store-{}", uuid::Uuid::new_v4())),
        )
    } else {
        coven_foundation::store_dir::StoreDir::new(
            path.parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| std::path::Path::new(".")),
        )
    }
}

/// The synthetic, domain-free schema the sync tests run against. Three synced
/// tables exercising the engine's generic mechanics: a *gated root* (`notes`,
/// gated by its `shared` boolean), a child with a foreign key (`note_tags`,
/// which inherits the gate and exercises FK-violation retry), and a child that
/// CAN carry a blob (`note_photos`, also FK-to-`notes`, so it inherits the gate).
/// `note_photos` carries no blob here; blob tests declare one with
/// [`test_synced_tables_with_blob`].
pub fn test_synced_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new(
            "notes",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "note_tags",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "note_photos",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        ),
    ]
}

/// [`test_synced_tables`] with `note_photos` declared blob-bearing per `decl`, for
/// tests exercising the blob push/pull/backfill paths. The blob id defaults to the
/// `note_photos` primary key; `note_photos.cloud_path` holds a readable key for
/// plain-scheme tests, and `note_photos.blob_id` is there for a decl that names a
/// blob id apart from the PK — the shape a row repointed at a new blob needs, since
/// the row keeps its primary key.
pub fn test_synced_tables_with_blob(decl: BlobDecl) -> Vec<SyncedTable> {
    vec![
        SyncedTable::new(
            "notes",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "note_tags",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "note_photos",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .carries_blob(decl),
    ]
}

/// [`test_synced_tables_with_blob`] with an ungated `notes` root: the rows are
/// remote from the start rather than waiting on a gate, which is what a snapshot
/// test needs to have something to publish before any gate is opened.
pub fn test_synced_tables_remote_root_with_blob(decl: BlobDecl) -> Vec<SyncedTable> {
    vec![
        SyncedTable::new(
            "notes",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .remote_root(),
        SyncedTable::new(
            "note_tags",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "note_photos",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .carries_blob(decl),
    ]
}

/// [`test_synced_tables`] with TWO blob-bearing children of the gated `notes` root:
/// `note_photos` per `photo_decl` (a release file, user-provided) and `note_covers`
/// per `cover_decl` (a host-provided asset). Both inherit the `notes` gate, so a
/// make_remote of a note carries both — the user-provided file through the durable
/// outbox and the host-provided cover through the inline push — exercising the
/// per-provenance split in one subtree.
pub fn test_synced_tables_with_user_and_host_blobs(
    photo_decl: BlobDecl,
    cover_decl: BlobDecl,
) -> Vec<SyncedTable> {
    vec![
        SyncedTable::new(
            "notes",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "note_tags",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "note_photos",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .carries_blob(photo_decl),
        SyncedTable::new(
            "note_covers",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .carries_blob(cover_decl),
    ]
}

/// Open a test [`Database`] over the synthetic schema with `note_photos` declared
/// blob-bearing per `decl`.
pub fn open_test_db_with_blob(decl: BlobDecl) -> SyntheticStoreFixture {
    open_test_db_schema(test_synced_tables_with_blob(decl), test_migrations())
}

/// Open a read-test [`Database`] whose `note_photos` child carries a blob in
/// `namespace`, so `read_blob`'s locality dispatch can resolve a
/// blob in that namespace up to its gated `notes` root. The decl's namespace MUST
/// match the blobs the test reads (the read path resolves the carrying table from the
/// blob's namespace); its provenance/fill don't matter to that resolution (the read
/// reads the row → root → gate, and takes provenance off the `BlobRef`), so this fixes
/// them. Pair with [`Database::plant_blob_row_for_test`].
pub fn read_test_db(namespace: &str) -> SyntheticStoreFixture {
    open_test_db_with_blob(BlobDecl::new(
        namespace,
        coven_protocol::blob::Provenance::UserProvided,
        coven_protocol::blob::CacheFill::CacheLazy,
    ))
}

/// Like [`read_test_db`] but with a chosen `max_concurrent_downloads`, so a pin test
/// can drive the download loop concurrently. Uploads run one at a time (not exercised here).
pub fn read_test_db_with_download_limit(
    namespace: &str,
    downloads: usize,
) -> SyntheticStoreFixture {
    let tables = test_synced_tables_with_blob(BlobDecl::new(
        namespace,
        coven_protocol::blob::Provenance::UserProvided,
        coven_protocol::blob::CacheFill::CacheLazy,
    ));
    let limits = coven_protocol::blob::TransferLimits {
        uploads: std::num::NonZeroUsize::MIN,
        downloads: std::num::NonZeroUsize::new(downloads).expect("downloads limit is nonzero"),
    };
    open_synthetic_database(
        tables,
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        limits,
        std::sync::Arc::new(
            coven_protocol::hlc::Hlc::try_new(
                "test-device".to_string(),
                std::sync::Arc::new(coven_foundation::clock::SystemClock),
            )
            .expect("create test register clock"),
        ),
        test_migrations(),
    )
}

/// Open a test [`Database`] with both `note_photos` (per `photo_decl`) and
/// `note_covers` (per `cover_decl`) declared blob-bearing — the schema for the
/// per-provenance transition tests.
pub fn open_test_db_with_user_and_host_blobs(
    photo_decl: BlobDecl,
    cover_decl: BlobDecl,
) -> SyntheticStoreFixture {
    open_test_db_schema(
        test_synced_tables_with_user_and_host_blobs(photo_decl, cover_decl),
        test_migrations(),
    )
}

/// The synthetic test schema as a single-migration ladder, so a test db opens at
/// `schema_version() == 1`. The host-schema ladder for every `open_test_db*`
/// helper.
pub fn test_migrations() -> Vec<Migration> {
    vec![Migration::run(1, "test-schema", create_synced_schema)]
}

/// Create the synthetic test schema on a connection. Run as the host migration
/// step for [`open_test_db`] (see [`test_migrations`]).
pub fn create_synced_schema(conn: &crate::MigrationContext<'_>) -> Result<(), DbError> {
    conn.execute_batch(
        "CREATE TABLE notes (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT,
            shared INTEGER NOT NULL DEFAULT 0,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        ) STRICT;
        CREATE TABLE note_tags (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        ) STRICT;
        CREATE TABLE note_photos (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            hash TEXT,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            cloud_path TEXT,
            blob_id TEXT,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        ) STRICT;
        CREATE TABLE note_covers (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            hash TEXT,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            cloud_path TEXT,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        ) STRICT;",
    )
    .map_err(DbError::from)
}

/// Open a [`Database`] over a fresh in-memory connection with the synthetic test
/// schema and the [`test_synced_tables`] synced set.
pub fn open_test_db() -> SyntheticStoreFixture {
    open_test_db_schema(test_synced_tables(), test_migrations())
}

pub fn open_test_db_with_tombstone_grace(grace: chrono::Duration) -> SyntheticStoreFixture {
    open_test_db_schema_with_tombstone_grace(test_synced_tables(), test_migrations(), grace)
}

/// Like [`open_test_db`] but with an explicit synced set and migration ladder, for
/// tests that exercise a different schema (gate tests).
pub fn open_test_db_schema(
    tables: Vec<SyncedTable>,
    migrations: Vec<Migration>,
) -> SyntheticStoreFixture {
    open_test_db_schema_with_tombstone_grace(
        tables,
        migrations,
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
    )
}

fn open_test_db_schema_with_tombstone_grace(
    tables: Vec<SyncedTable>,
    migrations: Vec<Migration>,
    grace: chrono::Duration,
) -> SyntheticStoreFixture {
    let hlc = std::sync::Arc::new(
        coven_protocol::hlc::Hlc::try_new(
            "test-device".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
        )
        .expect("create test register clock"),
    );
    open_synthetic_database(
        tables,
        grace,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        hlc,
        migrations,
    )
}

fn open_synthetic_database(
    tables: Vec<SyncedTable>,
    grace: chrono::Duration,
    transfer_limits: coven_protocol::blob::TransferLimits,
    hlc: std::sync::Arc<coven_protocol::hlc::Hlc>,
    migrations: Vec<Migration>,
) -> SyntheticStoreFixture {
    let store_dir = coven_foundation::store_dir::StoreDir::new_ephemeral(
        std::env::temp_dir().join(format!("coven-test-store-{}", uuid::Uuid::new_v4())),
    );
    let database = Database::open_with_hlc_in_store_dir_for_test(
        std::path::Path::new(":memory:"),
        store_dir.clone(),
        tables,
        grace,
        transfer_limits,
        hlc,
        &migrations,
    )
    .expect("open test database");
    SyntheticStoreFixture {
        database,
        store_dir,
    }
}

/// Open a test [`Database`] over the synthetic schema with a caller-supplied
/// register clock (so a test can control the wall clock), plus an extra `seed`
/// step run after the host schema is created to plant host rows before
/// `Database::open` reads its floor.
///
/// Used only by the register-clock tests (`hlc_register_tests`).
pub fn open_test_db_with_hlc(
    hlc: std::sync::Arc<coven_protocol::hlc::Hlc>,
    seed: impl for<'connection> Fn(&crate::MigrationContext<'connection>) -> Result<(), DbError>
        + Send
        + Sync
        + 'static,
) -> SyntheticStoreFixture {
    let migrations = vec![Migration::run(1, "test-schema", move |conn| {
        create_synced_schema(conn)?;
        seed(conn)
    })];
    open_synthetic_database(
        test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        hlc,
        migrations,
    )
}
/// The Store view of a test database. Every sync test builds one; naming it here
/// keeps the three test modules that used to declare it from drifting apart.
pub fn store_database(db: &Database) -> crate::StoreDatabase {
    crate::StoreDatabase::new(db)
}

/// The host-provided, eagerly-cached photo blob declaration most blob tests use.
pub fn photo_decl() -> BlobDecl {
    BlobDecl::new(
        "photos",
        coven_protocol::blob::Provenance::HostProvided,
        coven_protocol::blob::CacheFill::CacheEager,
    )
}

/// The notes schema with a remote-root parent, carrying `decl` on `note_photos`.
pub fn remote_root_db(decl: BlobDecl) -> SyntheticStoreFixture {
    open_test_db_schema(
        vec![
            SyncedTable::new(
                "notes",
                coven_protocol::synced_schema::RowIdentity::SharedKey,
            )
            .remote_root(),
            SyncedTable::new(
                "note_tags",
                coven_protocol::synced_schema::RowIdentity::SharedKey,
            ),
            SyncedTable::new(
                "note_photos",
                coven_protocol::synced_schema::RowIdentity::SharedKey,
            )
            .carries_blob(decl),
        ],
        test_migrations(),
    )
}
