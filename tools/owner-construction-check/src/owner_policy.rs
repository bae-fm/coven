//! The policy file of Appendix F.3. `owner_policy_tests.rs` holds the guard
//! tests that fail when a row names a crate, file, type or method that is not
//! in the workspace.

use crate::policy::{Gate, Policy};

#[path = "owner_policy_capabilities.rs"]
mod capabilities;
use capabilities::CAPABILITIES;

const PROVIDER_CONNECTOR: &str = "crates/coven-storage/src/providers/connector.rs";
const CONNECTION: &str = "crates/coven/src/storage.rs";
const MEMORY_STORAGE: &str = "crates/coven-storage/src/test_utils.rs";
const MEMORY_CONNECTOR: &str = "crates/coven-storage/src/memory_connector.rs";
const CONNECTION_CONTROL: &str = "crates/coven/src/storage_control.rs";
const SAVED_RESTORE: &str = "crates/coven/src/saved_restore.rs";
const TEST_COVEN: &str = "crates/coven/src/test_utils.rs";
const TEST_KEYCHAIN: &str = "crates/coven-crypto/src/custody/keychain_test_utils.rs";
pub(crate) const POLICY: Policy = Policy {
    crate_order: &[
        "coven-foundation",
        "coven-crypto",
        "coven-merge",
        "coven-format",
        "coven-database",
        "coven-storage",
        "coven-sync",
        "coven",
    ],
    separated_crates: &[("coven-database", "coven-storage")],
    capabilities: CAPABILITIES,
    database_schema: Some((
        "crates/coven-database/src/internal_schema.rs",
        &[
            "coven_tables",
            "coven_ownership_tables",
            "coven_input_tables",
            "coven_sequence_tables",
        ],
    )),
    // One list for owner construction, runtime acquisition and task starts.
    // Owner constructors include their private representations; names grant no exemption.
    composition_roots: &[
        (PROVIDER_CONNECTOR, "ProviderConnector", "with_folders"),
        (
            "crates/coven-storage/src/providers/oauth_folders.rs",
            "OAuthFolders",
            "new",
        ),
        (MEMORY_CONNECTOR, "MemoryStorage", "with_transfers"),
        (
            "crates/coven-crypto/src/custody/key_session.rs",
            "KeySession",
            "open",
        ),
        (
            "crates/coven-crypto/src/custody/key_session.rs",
            "KeySession",
            "store",
        ),
        (
            "crates/coven-crypto/src/custody/key_session.rs",
            "KeySession",
            "member",
        ),
        (
            "crates/coven-crypto/src/custody/passphrase.rs",
            "PassphraseCustody",
            "new",
        ),
        (
            "crates/coven-crypto/src/custody/store_custody.rs",
            "StoreCustody",
            "new",
        ),
        ("crates/coven-database/src/database.rs", "<free>", "process"),
        (
            "crates/coven-database/src/database.rs",
            "<free>",
            "start_call",
        ),
        ("crates/coven-database/src/database.rs", "Database", "close"),
        (
            "crates/coven-database/src/database.rs",
            "Database",
            "write_with_files",
        ),
        (
            "crates/coven-database/src/database.rs",
            "DatabaseReadHandle",
            "close",
        ),
        (
            "crates/coven-database/src/database_builder.rs",
            "DatabaseBuilder",
            "new",
        ),
        (
            "crates/coven-database/src/database_builder.rs",
            "DatabaseBuilder",
            "open",
        ),
        (
            "crates/coven-database/src/database_builder.rs",
            "DatabaseBuilder",
            "open_graph",
        ),
        (
            "crates/coven-database/src/database_builder.rs",
            "DatabaseBuilder",
            "open_locked",
        ),
        (
            "crates/coven-database/src/database_builder.rs",
            "DatabaseBuilder",
            "open_read_graph",
        ),
        (
            "crates/coven-database/src/database_builder.rs",
            "DatabaseBuilder",
            "open_read_only",
        ),
        (
            "crates/coven-database/src/database_builder.rs",
            "<free>",
            "discarded_operations",
        ),
        // Name reads open temporary read-only connections at the database boundary.
        (
            "crates/coven-database/src/database_builder.rs",
            "<free>",
            "read_store_name",
        ),
        (
            "crates/coven-database/src/database_builder.rs",
            "<free>",
            "discarded_store_name",
        ),
        (
            "crates/coven-database/src/file_database.rs",
            "CacheReservation",
            "publish",
        ),
        // Consumes the upload's cache reservation on its own blocking task,
        // releasing it after the database guard as `publish` does.
        (
            "crates/coven-database/src/file_database.rs",
            "FileDatabase",
            "complete_upload",
        ),
        (
            "crates/coven-database/src/file_database.rs",
            "FileDatabase",
            "new",
        ),
        (
            "crates/coven-database/src/file_database.rs",
            "FileDatabase",
            "read_only",
        ),
        (
            "crates/coven-database/src/file_staging.rs",
            "FileStaging",
            "write",
        ),
        (
            "crates/coven-database/src/local_file.rs",
            "LocalFileStream",
            "read_at",
        ),
        ("crates/coven-database/src/read_pool.rs", "ReadPool", "new"),
        (
            "crates/coven-database/src/input_database.rs",
            "InputDatabase",
            "new",
        ),
        // Removes an abandoned snapshot staging's records on a blocking task
        // once its last owner drops; closing the store waits for it (§16.2).
        (
            "crates/coven-database/src/snapshot_staging.rs",
            "StagedRows",
            "drop",
        ),
        // Starts a database's checkpointer task, which the store stops as it
        // closes (§4).
        (
            "crates/coven-database/src/wal_checkpoints.rs",
            "WalCheckpointer",
            "start",
        ),
        (
            "crates/coven-foundation/src/files/atomic_file.rs",
            "FileWriter",
            "run",
        ),
        // A batch's flush syncs its folders and the drive's cache on a
        // blocking task, which the write waits for before its commit.
        (
            "crates/coven-foundation/src/files/flush_batch.rs",
            "FlushBatch",
            "flush",
        ),
        // Copies into a folder the person picked do their file work on a
        // blocking task, which finishes even if the call is dropped (§17.3).
        (
            "crates/coven-foundation/src/files/folder_copy.rs",
            "<free>",
            "blocking",
        ),
        // Runs its caller's work on a blocking task and waits for it, so the
        // work's panic comes back to the caller as a value (api.md E15).
        ("crates/coven-foundation/src/task.rs", "<free>", "blocking"),
        (
            "crates/coven-foundation/src/files/bootstrap.rs",
            "<free>",
            "reserve_bootstrap_directory",
        ),
        // Both store listings read the directory on a blocking task.
        (
            "crates/coven-foundation/src/files/layout.rs",
            "StoreDirectories",
            "listed",
        ),
        (
            "crates/coven-foundation/src/files/lock_registry.rs",
            "LockRegistration",
            "open_on_worker",
        ),
        (
            "crates/coven-storage/src/conformance.rs",
            "Conformance",
            "new",
        ),
        (MEMORY_CONNECTOR, "MemoryStorage", "connect"),
        (
            "crates/coven-storage/src/memory_storage_builder.rs",
            "MemoryStorageBuilder",
            "new",
        ),
        (
            "crates/coven-storage/src/memory_storage_builder.rs",
            "MemoryStorageBuilder",
            "build",
        ),
        (
            "crates/coven-storage/src/providers/cloudkit.rs",
            "CloudKitStorage",
            "new",
        ),
        (PROVIDER_CONNECTOR, "ProviderConnector", "connect"),
        (PROVIDER_CONNECTOR, "ProviderConnector", "new"),
        (
            "crates/coven-storage/src/providers/dropbox.rs",
            "DropboxStorage",
            "new",
        ),
        (
            "crates/coven-storage/src/providers/oauth.rs",
            "OAuthFlow",
            "new",
        ),
        (
            "crates/coven-storage/src/providers/onedrive.rs",
            "OneDriveStorage",
            "new",
        ),
        (
            "crates/coven-storage/src/providers/s3.rs",
            "S3Storage",
            "new",
        ),
        (
            "crates/coven-storage/src/providers/s3.rs",
            "S3Storage",
            "with_settings",
        ),
        // The S3 client's signing clock and its refusal handling (§20.7).
        (
            "crates/coven-storage/src/providers/s3_clock.rs",
            "SigningClock",
            "new",
        ),
        (
            "crates/coven-storage/src/providers/s3_clock.rs",
            "ClockSkew",
            "new",
        ),
        // Detection connects one candidate per signer, then its probe runner.
        (
            "crates/coven-storage/src/providers/s3_detect.rs",
            "<free>",
            "detect",
        ),
        // A new store's own-bucket check connects one candidate per signer.
        (
            "crates/coven-storage/src/providers/s3_own_bucket.rs",
            "<free>",
            "check_new_store",
        ),
        // Emptying an S3 prefix, a development tool, connects to the location
        // with its own settings (api.md E14).
        (
            "crates/coven-storage/src/providers/s3_empty.rs",
            "<free>",
            "empty_prefix",
        ),
        // The development tool's entry point builds the production connector
        // with the system clock and random ids, as the builder does (api.md E14).
        ("crates/coven/src/dev_tools.rs", "<free>", "empty_s3_prefix"),
        (
            "crates/coven-storage/src/settings.rs",
            "StorageSettings",
            "new",
        ),
        (MEMORY_STORAGE, "MemoryStorage", "builder"),
        (MEMORY_STORAGE, "MemoryStorage", "from_memory"),
        (MEMORY_STORAGE, "MemoryStorage", "clone"),
        (MEMORY_STORAGE, "MemoryStorage", "connect_device"),
        (MEMORY_CONNECTOR, "MemoryStorage", "shared_connection"),
        (MEMORY_CONNECTOR, "MemoryStorage", "with_publication_gate"),
        (MEMORY_CONNECTOR, "MemoryStorage", "with_cooldown"),
        (MEMORY_STORAGE, "MemoryStorage", "for_device"),
        (MEMORY_STORAGE, "MemoryStorage", "for_recipient"),
        (
            "crates/coven-sync/src/file_read.rs",
            "FileStream",
            "read_at",
        ),
        // Creates a reserved cache file for a pin or an upload's own copy.
        (
            "crates/coven-sync/src/file_cache.rs",
            "FilesInner",
            "create_cache_file",
        ),
        ("crates/coven-sync/src/file_read.rs", "UploadedFile", "open"),
        ("crates/coven-sync/src/files.rs", "Files", "new"),
        // One retry schedule per opened installation, injected into every
        // worker and connection; a read-only handle keeps its own in memory.
        ("crates/coven-sync/src/retry_waits.rs", "RetryWaits", "open"),
        (
            "crates/coven-sync/src/retry_waits.rs",
            "RetryWaits",
            "in_memory",
        ),
        ("crates/coven-sync/src/sync_loop.rs", "SyncLoop", "new"),
        // Credential changes run as owned tasks, so a dropped app future
        // cannot leave custody and the live connection disagreeing.
        (
            "crates/coven-sync/src/restore_codes.rs",
            "RestoreCodes",
            "new",
        ),
        (
            "crates/coven-sync/src/restore_codes.rs",
            "RestoreCodes",
            "replace_member_keys",
        ),
        (
            "crates/coven-sync/src/restore_codes.rs",
            "RestoreCodes",
            "replace_access_key",
        ),
        (
            "crates/coven-sync/src/restore_codes.rs",
            "RestoreCodes",
            "update_credentials",
        ),
        // A sign-in's custody: device custody once per opened installation,
        // session custody per completed sign-in until publication (§20.2).
        (
            "crates/coven-sync/src/sign_in.rs",
            "DeviceCredentials",
            "new",
        ),
        ("crates/coven-sync/src/sign_in.rs", "SessionSignIn", "new"),
        (
            "crates/coven-sync/src/credential_custody.rs",
            "CredentialKeychains",
            "new",
        ),
        // The sequence owner composes its file transfers and pass state.
        (
            "crates/coven-sync/src/sequence_sync.rs",
            "SequenceSync",
            "open",
        ),
        (
            "crates/coven-sync/src/sequence_sync.rs",
            "SequenceSync",
            "reconnect",
        ),
        // An operation's call runs as its own task, so a dropped app future
        // cannot abandon a provider request already sent (§11).
        (
            "crates/coven-sync/src/sequence_operations.rs",
            "SequenceSync",
            "submit",
        ),
        (
            "crates/coven-sync/src/sequence_keys.rs",
            "SequenceSync",
            "change_key",
        ),
        (
            "crates/coven/src/bootstrap.rs",
            "<free>",
            "bootstrap_device",
        ),
        (
            "crates/coven/src/bootstrap.rs",
            "<free>",
            "join_with_invite",
        ),
        ("crates/coven/src/bootstrap.rs", "<free>", "resume_join"),
        // Listing reads each unfinished installation's saved code.
        (
            "crates/coven/src/bootstrap.rs",
            "<free>",
            "unfinished_joins",
        ),
        (
            "crates/coven/src/bootstrap.rs",
            "<free>",
            "prepare_and_load",
        ),
        (
            "crates/coven/src/bootstrap.rs",
            "<free>",
            "restore_from_code",
        ),
        (
            "crates/coven/src/bootstrap.rs",
            "<free>",
            "restore_from_saved_code",
        ),
        (
            "crates/coven/src/bootstrap_commit.rs",
            "<free>",
            "publish_bootstrap",
        ),
        // Committing custody reads and restores the installation's settings.
        ("crates/coven/src/bootstrap_commit.rs", "<free>", "commit"),
        (
            "crates/coven/src/bootstrap_join.rs",
            "<free>",
            "prepare_request",
        ),
        // Restore connects through the builder's held sign-in.
        (
            "crates/coven/src/restore_credentials.rs",
            "<free>",
            "restore_connection",
        ),
        // A join keeps its sign-in in the device custody it composes for the
        // store, beside its saved code (§13.2).
        (
            "crates/coven/src/bootstrap_authentication.rs",
            "<free>",
            "join_sign_in",
        ),
        // A replacement composes the new installation beside the old one,
        // holding the store's writer lock until it is in place.
        (
            "crates/coven-foundation/src/files/replacement.rs",
            "<free>",
            "begin",
        ),
        (
            "crates/coven/src/bootstrap_replace.rs",
            "<free>",
            "replace_installation",
        ),
        (
            "crates/coven/src/bootstrap_replace.rs",
            "<free>",
            "open_swapped",
        ),
        ("crates/coven/src/bootstrap_replace.rs", "<free>", "publish"),
        (
            "crates/coven/src/bootstrap_replace.rs",
            "<free>",
            "restore_device",
        ),
        (
            "crates/coven/src/bootstrap_reset.rs",
            "<free>",
            "reset_device",
        ),
        (
            "crates/coven/src/bootstrap_reset.rs",
            "<free>",
            "reset_unconnected",
        ),
        (
            "crates/coven/src/builder.rs",
            "CovenBuilder",
            "authenticate",
        ),
        ("crates/coven/src/builder.rs", "CovenBuilder", "connector"),
        ("crates/coven/src/builder.rs", "CovenBuilder", "database"),
        ("crates/coven/src/builder.rs", "CovenBuilder", "keychain"),
        // Builder setters replace a configured or already-unlocked custody choice.
        ("crates/coven/src/builder.rs", "CovenBuilder", "key_custody"),
        (
            "crates/coven/src/builder.rs",
            "CovenBuilder",
            "identity_custody",
        ),
        (
            "crates/coven/src/builder.rs",
            "CovenBuilder",
            "make_identity",
        ),
        ("crates/coven/src/builder.rs", "CovenBuilder", "make_keys"),
        ("crates/coven/src/builder.rs", "CovenBuilder", "new"),
        ("crates/coven/src/builder.rs", "CovenBuilder", "oauth_flow"),
        ("crates/coven/src/builder.rs", "CovenBuilder", "open"),
        ("crates/coven/src/builder.rs", "CovenBuilder", "open_graph"),
        (
            "crates/coven/src/builder.rs",
            "CovenBuilder",
            "reset_device",
        ),
        (
            "crates/coven/src/builder.rs",
            "CovenBuilder",
            "restore_device",
        ),
        ("crates/coven/src/builder.rs", "CovenBuilder", "resume_join"),
        (
            "crates/coven/src/builder.rs",
            "CovenBuilder",
            "open_read_only",
        ),
        ("crates/coven/src/builder.rs", "CovenBuilder", "owners"),
        ("crates/coven/src/builder.rs", "OpeningOwners", "handle"),
        (
            "crates/coven/src/builder.rs",
            "OpeningOwners",
            "into_handle",
        ),
        ("crates/coven/src/builder.rs", "OpeningStore", "open"),
        ("crates/coven/src/coven.rs", "<free>", "blocking"),
        ("crates/coven/src/coven.rs", "Coven", "builder"),
        ("crates/coven/src/store_layout.rs", "StoreLayout", "new"),
        ("crates/coven/src/coven.rs", "Coven", "storage_config"),
        (
            "crates/coven/src/builder.rs",
            "CovenBuilder",
            "create_store",
        ),
        ("crates/coven/src/builder_creation.rs", "<free>", "create"),
        ("crates/coven/src/builder_creation.rs", "<free>", "prepare"),
        ("crates/coven/src/coven.rs", "Coven", "discard_store"),
        // Recovery discovery and forgetting acquire the registered keychain and
        // scope its entries.
        (SAVED_RESTORE, "<free>", "saved_restore_codes"),
        (SAVED_RESTORE, "<free>", "list"),
        (SAVED_RESTORE, "<free>", "forget_saved_restore_code"),
        (SAVED_RESTORE, "<free>", "forget"),
        ("crates/coven/src/handle.rs", "CovenHandle", "close"),
        ("crates/coven/src/handle.rs", "CovenHandle", "new"),
        (
            "crates/coven/src/read_handle.rs",
            "CovenReadHandle",
            "close",
        ),
        ("crates/coven/src/read_handle.rs", "CovenReadHandle", "new"),
        // A completed sign-in becomes this session's custody.
        (CONNECTION, "StorageConnections", "authenticate"),
        (CONNECTION, "StorageConnections", "call"),
        (CONNECTION, "StorageConnections", "new"),
        (CONNECTION, "StorageConnections", "release"),
        // Setup, unlocking, starting and loading compose a connection's
        // sequence owner, loop and credential owner at runtime.
        (CONNECTION, "StorageConnections", "attach"),
        (CONNECTION, "StorageConnections", "sequence"),
        (CONNECTION, "StorageConnections", "activate"),
        (CONNECTION, "StorageConnections", "setup_connection"),
        (CONNECTION, "StorageConnections", "forget_store_keys"),
        // A started loop's connection attempts outlive the start call; close
        // stops and awaits them. Stopping releases the client after the pass.
        (CONNECTION_CONTROL, "StorageConnections", "start"),
        (CONNECTION_CONTROL, "StorageConnections", "stop"),
        (TEST_COVEN, "TestCoven", "builder"),
        (TEST_COVEN, "TestCoven", "create_closed_store"),
        (TEST_COVEN, "TestCoven", "discard_store"),
        (TEST_COVEN, "TestCoven", "keep_store_keys"),
        (TEST_COVEN, "TestCoven", "new"),
        // Another of the person's devices shares only the person entries.
        (TEST_COVEN, "TestCoven", "another_device"),
        (TEST_KEYCHAIN, "Keychain", "another_device"),
        // An Android phone keeps the person entries in Block Store, and a new
        // phone set up from it receives a copy of them.
        (TEST_COVEN, "TestCoven", "with_block_store"),
        (TEST_COVEN, "TestCoven", "set_up_new_device"),
        (TEST_COVEN, "TestCoven", "restore_new_device_from_cloud"),
        (TEST_KEYCHAIN, "Keychain", "in_memory_block_store"),
        (TEST_KEYCHAIN, "Keychain", "set_up_new_device"),
        (TEST_KEYCHAIN, "Keychain", "restore_new_device_from_cloud"),
    ],
    task_starts: &[
        Gate {
            kind: "runtime construction",
            crates: &[],
            path_patterns: &[
                &["Runtime", "new"],
                &["Builder", "new_current_thread"],
                &["Builder", "new_multi_thread"],
            ],
            method_patterns: &[],
        },
        Gate {
            kind: "ambient runtime acquisition",
            crates: &[],
            path_patterns: &[&["Handle", "current"], &["Handle", "try_current"]],
            method_patterns: &[],
        },
        Gate {
            kind: "thread or task spawn",
            crates: &[],
            path_patterns: &[
                &["tokio", "spawn"],
                &["task", "spawn"],
                &["task", "spawn_blocking"],
                &["task", "spawn_local"],
                &["thread", "spawn"],
            ],
            method_patterns: &["spawn", "spawn_blocking", "spawn_local"],
        },
    ],
    // These methods derive a scoped capability from their injected directory.
    // They do not acquire an unrelated directory or file for another owner.
    capability_factories: &[
        (
            "crates/coven-foundation/src/files/asset_files.rs",
            "AssetFiles",
            "stage",
            "AssetFile",
        ),
        (
            "crates/coven-foundation/src/files/layout.rs",
            "StoreDirectories",
            "begin_bootstrap",
            "BootstrapStore",
        ),
        (
            "crates/coven-foundation/src/files/bootstrap.rs",
            "BootstrapStore",
            "directory",
            "StoreDir",
        ),
        (
            "crates/coven-foundation/src/files/layout.rs",
            "StoreDirectories",
            "begin_replacement",
            "ReplacementStore",
        ),
        (
            "crates/coven-foundation/src/files/replacement.rs",
            "ReplacementStore",
            "directory",
            "StoreDir",
        ),
        (
            "crates/coven-foundation/src/files/directory.rs",
            "StoreDir",
            "lock_for_deletion",
            "StoreDeletionLock",
        ),
        (
            "crates/coven-foundation/src/files/directory.rs",
            "StoreDir",
            "lock_read_only",
            "StoreReadLock",
        ),
        (
            "crates/coven-foundation/src/files/atomic_file.rs",
            "AtomicFile",
            "create_writer",
            "FileWriter",
        ),
        (
            "crates/coven-foundation/src/files/directory.rs",
            "StoreDir",
            "lock_exclusive",
            "StoreLock",
        ),
        (
            "crates/coven-foundation/src/files/directory.rs",
            "StoreDir",
            "lock_for_open",
            "StoreLock",
        ),
        (
            "crates/coven-foundation/src/files/layout.rs",
            "StoreDirectories",
            "store_dir",
            "StoreDir",
        ),
        (
            "crates/coven-foundation/src/files/layout.rs",
            "StoreDirectories",
            "create_store_dir",
            "StoreDir",
        ),
        (
            "crates/coven-foundation/src/files/directory.rs",
            "StoreDir",
            "owned_file",
            "AtomicFile",
        ),
        (
            "crates/coven-foundation/src/files/directory.rs",
            "StoreDir",
            "file",
            "AtomicFile",
        ),
    ],
    // These are the leaf capabilities. Owners retaining them receive them
    // through constructors; their implementations keep raw OS handles private.
    capability_types: &[
        "Clock",
        "ClockRef",
        "SystemClock",
        "IdSource",
        "IdSourceRef",
        "UuidIds",
        "StoreLayout",
        "StoreDirectories",
        "BootstrapStore",
        "ReplacementStore",
        "StoreDir",
        "AtomicFile",
        "FileWriter",
        "FileReader",
        "AssetFiles",
        "AssetFile",
        "StoreLock",
        "StoreReadLock",
        "StoreDeletionLock",
        "Keychain",
        "StoreKeychain",
        "PersonEntries",
        "KeyringCustody",
        "StoreKeyCustody",
        "MemberKeyCustody",
        "CustodyPersistence",
        "StorageConnector",
        "Storage",
        "ProviderOps",
        "OAuthClients",
        "OAuthPresenter",
        "OAuthSession",
        "DatabaseConnection",
    ],
    capability_traits: &[
        "Clock",
        "IdSource",
        "StoreKeyCustody",
        "MemberKeyCustody",
        "CustodyPersistence",
        "StorageConnector",
        "Storage",
        "ProviderOps",
        "OAuthPresenter",
    ],
    construction_only_capability_types: &[
        "AssetFiles",
        "AssetFile",
        "BootstrapStore",
        "ReplacementStore",
        "StoreDir",
        "StoreLock",
        "StoreReadLock",
        "StoreDeletionLock",
        "AtomicFile",
        "FileWriter",
        "ClockRef",
        "IdSourceRef",
        "Keychain",
        "StoreKeychain",
        "PersonEntries",
        "OAuthClients",
        "OAuthSession",
        "DatabaseConnection",
    ],
    non_owner_types: &[
        // A cursor over retained provider bytes names the input connection,
        // whose lifetime the database owns: closing it invalidates the cursor.
        "RetainedBytes",
        // Failure transport may carry a published handle; it does not own store lifetime.
        "BootstrapError",
        "StoreCreationError",
        "KeyCustody",
        "IdentityCustody",
        "Command",
        "Request",
        // A handle on the providers' one HTTP client, cloned into each
        // connection and admin call as the reqwest client inside it is. It
        // holds the connector's clock only to time the stall window (§6.7).
        "HttpClient",
        // When one HTTP exchange last moved bytes; it lives for that exchange.
        "ExchangeProgress",
        // A connection's request totals. It holds the connection's clock
        // only to time each request, and lives with that connection.
        "RequestTally",
        // One long operation's progress reports. It holds the operation's
        // clock only to pace them, and lives for that operation.
        "ProgressReport",
    ],
    // Writes borrow the database's retained connection and id source; app
    // SQL receives none of those dependencies.
    borrowed_facade_types: &[
        "SnapshotMetadata",
        // A run of slot reads borrows the sequence owner and hands each slot's
        // bytes to its reader in order.
        "SlotReads",
        // These readers borrow a connection for one committed read or transaction.
        "DatabaseMetadata",
        "PredicateContext",
        "PreparedBytes",
        "PreparedInputs",
        "PreparedRecords",
        "PreparedWrite",
        "PreparedPart",
        "PreparedSnapshot",
        // One batch of a snapshot's staging writes its records through the
        // input connection's transaction for that batch.
        "RecordStaging",
        "ClaimBytes",
        "Circles",
        "FileWrite",
        // A write's batch borrows the store's id source while the app
        // stages its files, giving each a new id.
        "WriteBatch",
        "SqlReadContext",
        "Read",
        "ReadOwner",
        "ChangeCapture",
        "StreamCapture",
        "MigrationContext",
        "SqlContext",
        // A bulk write's SQL borrows the writer connection for its one
        // transaction, as a write's does.
        "BulkSqlContext",
        "SqlTransaction",
        "ReaderLease",
        "AppView",
        "MergeStore",
        "WriteMetadata",
        "DatabaseRemovalView",
        "WriteApply",
        // A local write's operation hook, its phases and their capture borrow
        // the writer connection for one transaction.
        "EventCapture",
        "LocalPhase",
        "Tracker",
        "PreWrite",
    ],
    task_types: &[
        "OpenFile",
        "LocalFileReader",
        "LockedWriter",
        // The files one commit records, and each folder they were made in,
        // until the flush before that commit (Appendix C.3).
        "FlushBatch",
        "BatchFolder",
        "Circles",
        "CacheReservation",
        // The copies one call making files device-only is writing (§17.3).
        "CopyReservation",
        "UploadedFile",
        "RetainedInput",
        "ObjectInput",
        "InputStream",
        // One run of consecutive slot reads.
        "SlotReads",
        // One floor observation, snapshot read or upload source.
        "FloorObject",
        "SnapshotObject",
        "ReadSnapshot",
        "SelectedSnapshot",
        // One snapshot's records, staged while its body streams until the
        // load or file check that reads them.
        "SnapshotStaging",
        "StagedRows",
        "StagedSnapshot",
        "StagedRecords",
        "ReloadStart",
        // One reload's selected streams and installation target.
        "ReloadInputs",
        "ClaimSource",
        "SnapshotUploadSource",
        "FloorUploadSource",
        "FileTransfer",
        "FileRangeStream",
        "FileWorker",
        "LocalFileStream",
        "FileStream",
        "FileStaging",
        "FileWrite",
        "SqlReadContext",
        "Read",
        "ReadOwner",
        "ChangeCapture",
        "StreamCapture",
        "LiveQuery",
        "ReconfigurableLiveQuery",
        "SqlTransaction",
        "ReaderLease",
        "AppView",
        "MergeStore",
        "WriteMetadata",
        "DatabaseRemovalView",
        "WriteApply",
        // A local write's operation hook, its phases and their capture borrow
        // the writer connection for one transaction.
        "EventCapture",
        "LocalPhase",
        "Tracker",
        "PreWrite",
    ],
    internal_dependency_types: &["DatabaseConnection"],
    closed_session_types: &[
        "FileWrite",
        "SqlReadContext",
        "MigrationContext",
        "SqlContext",
        "BulkSqlContext",
        "AppView",
        "MergeStore",
        "WriteMetadata",
        "DatabaseRemovalView",
        "WriteApply",
        // A local write's operation hook, its phases and their capture borrow
        // the writer connection for one transaction.
        "EventCapture",
        "LocalPhase",
        "Tracker",
        "PreWrite",
    ],
};

#[cfg(test)]
#[path = "owner_policy_tests.rs"]
mod tests;
