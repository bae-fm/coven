//! The policy file of §20.2. `owner_policy_tests.rs` holds the guard
//! tests that fail when a row names a crate, file, type or method that is not
//! in the workspace.

use crate::policy::{Capabilities, Capability, Gate, Policy};

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
        ],
    )),
    // One list for owner construction, runtime acquisition and task starts.
    // Owner constructors include their private representations; names grant no exemption.
    composition_roots: &[
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
        (
            "crates/coven-database/src/file_database.rs",
            "CacheReservation",
            "publish",
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
        (
            "crates/coven-foundation/src/files/atomic_file.rs",
            "FileWriter",
            "run",
        ),
        (
            "crates/coven-foundation/src/files/bootstrap.rs",
            "<free>",
            "reserve_bootstrap_directory",
        ),
        (
            "crates/coven-foundation/src/files/layout.rs",
            "StoreLayout",
            "stores",
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
        (
            "crates/coven-storage/src/memory_connector.rs",
            "MemoryStorage",
            "connect",
        ),
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
        (
            "crates/coven-storage/src/providers/connector.rs",
            "ProviderConnector",
            "connect",
        ),
        (
            "crates/coven-storage/src/providers/connector.rs",
            "ProviderConnector",
            "new",
        ),
        (
            "crates/coven-storage/src/providers/dropbox.rs",
            "DropboxStorage",
            "new",
        ),
        (
            "crates/coven-storage/src/providers/google_drive.rs",
            "GoogleDriveStorage",
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
        // The S3 client's signing clock and its refusal handling (§4).
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
        // Admin access becomes one location's key administration (§4).
        (
            "crates/coven-storage/src/providers/s3_minting.rs",
            "<free>",
            "key_admin",
        ),
        // The member-side check connects with the minted key itself.
        (
            "crates/coven-storage/src/providers/s3_minting.rs",
            "ProfileKeyAdmin",
            "probe",
        ),
        (
            "crates/coven-storage/src/settings.rs",
            "StorageSettings",
            "new",
        ),
        (
            "crates/coven-storage/src/test_utils.rs",
            "MemoryStorage",
            "builder",
        ),
        (
            "crates/coven-storage/src/test_utils.rs",
            "MemoryStorage",
            "clone",
        ),
        (
            "crates/coven-storage/src/test_utils.rs",
            "MemoryStorage",
            "connect_device",
        ),
        (
            "crates/coven-storage/src/test_utils.rs",
            "MemoryStorage",
            "shared_connection",
        ),
        (
            "crates/coven-storage/src/test_utils.rs",
            "MemoryStorage",
            "with_publication_gate",
        ),
        (
            "crates/coven-storage/src/test_utils.rs",
            "MemoryStorage",
            "with_cooldown",
        ),
        (
            "crates/coven-storage/src/test_utils.rs",
            "MemoryStorage",
            "for_device",
        ),
        (
            "crates/coven-storage/src/test_utils.rs",
            "MemoryStorage",
            "for_recipient",
        ),
        (
            "crates/coven-sync/src/device_log_sync.rs",
            "DeviceLogSync",
            "disconnected",
        ),
        (
            "crates/coven-sync/src/device_log_sync.rs",
            "DeviceLogSync",
            "new",
        ),
        (
            "crates/coven-sync/src/file_read.rs",
            "FileStream",
            "read_at",
        ),
        (
            "crates/coven-sync/src/file_read.rs",
            "UploadedFile",
            "keep_whole",
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
        // One sending gate per opened installation, injected into every sender.
        ("crates/coven-sync/src/send_gate.rs", "SendGate", "new"),
        ("crates/coven-sync/src/operations.rs", "Operations", "new"),
        // Setup stages its candidate connection's keys without changing custody.
        (
            "crates/coven-sync/src/storage_setup.rs",
            "StoreLogSync",
            "catch_up_setup",
        ),
        (
            "crates/coven-sync/src/restore_codes.rs",
            "RestoreCodes",
            "new",
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
        (
            "crates/coven-sync/src/restore_codes.rs",
            "RestoreCodes",
            "install_oauth",
        ),
        (
            "crates/coven-sync/src/restore_codes.rs",
            "RestoreCodes",
            "adopt_delivered_key",
        ),
        (
            "crates/coven-sync/src/store_log_sync.rs",
            "StoreLogSync",
            "disconnected",
        ),
        (
            "crates/coven-sync/src/store_log_sync.rs",
            "StoreLogSync",
            "new",
        ),
        // A sign-in's custody: device custody once per opened installation,
        // session custody per completed sign-in until publication (§4).
        (
            "crates/coven-sync/src/sign_in.rs",
            "DeviceCredentials",
            "new",
        ),
        ("crates/coven-sync/src/sign_in.rs", "SessionSignIn", "new"),
        ("crates/coven-sync/src/sync_loop.rs", "SyncLoop", "new"),
        ("crates/coven-sync/src/sync_loop.rs", "SyncLoop", "setup"),
        ("crates/coven-sync/src/sync_loop.rs", "SyncLoop", "unlock"),
        ("crates/coven-sync/src/sync_loop.rs", "SyncRun", "connect"),
        ("crates/coven-sync/src/sync_loop.rs", "SyncRun", "setup"),
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
            "restore_from_keychain",
        ),
        (
            "crates/coven/src/bootstrap_commit.rs",
            "<free>",
            "publish_bootstrap",
        ),
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
        // Finishing a recorded reset composes the new installation.
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
            "open_read_only",
        ),
        ("crates/coven/src/builder.rs", "CovenBuilder", "owners"),
        ("crates/coven/src/builder.rs", "OpeningOwners", "handle"),
        ("crates/coven/src/builder.rs", "OpeningOwners", "sync"),
        ("crates/coven/src/builder.rs", "OpeningStore", "open"),
        ("crates/coven/src/coven.rs", "<free>", "blocking"),
        ("crates/coven/src/coven.rs", "Coven", "builder"),
        ("crates/coven/src/coven.rs", "Coven", "create_store"),
        ("crates/coven/src/coven.rs", "Coven", "delete_store"),
        ("crates/coven/src/handle.rs", "CovenHandle", "close"),
        ("crates/coven/src/handle.rs", "CovenHandle", "new"),
        (
            "crates/coven/src/read_handle.rs",
            "CovenReadHandle",
            "close",
        ),
        ("crates/coven/src/read_handle.rs", "CovenReadHandle", "new"),
        // A completed sign-in becomes this session's custody; a Drive upgrade
        // is its own until committed.
        (
            "crates/coven/src/storage.rs",
            "StorageConnections",
            "authenticate",
        ),
        (
            "crates/coven/src/storage.rs",
            "StorageConnections",
            "prepare_invite",
        ),
        ("crates/coven/src/storage.rs", "StorageConnections", "call"),
        ("crates/coven/src/storage.rs", "StorageConnections", "new"),
        ("crates/coven/src/test_utils.rs", "TestCoven", "builder"),
        (
            "crates/coven/src/test_utils.rs",
            "TestCoven",
            "create_store",
        ),
        (
            "crates/coven/src/test_utils.rs",
            "TestCoven",
            "delete_store",
        ),
        (
            "crates/coven/src/test_utils.rs",
            "TestCoven",
            "keep_store_keys",
        ),
        ("crates/coven/src/test_utils.rs", "TestCoven", "new"),
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
            "StoreLayout",
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
            "crates/coven-foundation/src/files/layout.rs",
            "StoreLayout",
            "create_store_dir_with",
            "StoreDir",
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
            "StoreLayout",
            "store_dir",
            "StoreDir",
        ),
        (
            "crates/coven-foundation/src/files/layout.rs",
            "StoreLayout",
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
        "BootstrapStore",
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
        "OAuthClients",
        "OAuthSession",
        "DatabaseConnection",
    ],
    non_owner_types: &[
        // Failure transport may carry a published handle; it does not own store lifetime.
        "BootstrapError",
        "KeyCustody",
        "IdentityCustody",
        "Command",
        "SyncCommand",
        "Request",
    ],
    // Writes borrow the database's retained connection and id source; app
    // SQL receives none of those dependencies.
    borrowed_facade_types: &[
        // A setup or admission exception borrows the gate for exact paths.
        "PublicationScope",
        "SnapshotMetadata",
        "Circles",
        "FileWrite",
        "SqlReadContext",
        "Read",
        "ReadOwner",
        "ChangeCapture",
        "StreamCapture",
        "MigrationContext",
        "SqlContext",
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
        "PublicationScope",
        "OpenFile",
        "LocalFileReader",
        "LockedWriter",
        "SyncRun",
        "OperationRun",
        "RunningOperations",
        "Circles",
        "CacheReservation",
        "UploadedFile",
        "SnapshotInput",
        "SealedPlaintext",
        "RetainedInput",
        "ObjectInput",
        "InputStream",
        "SnapshotWriteParts",
        "SnapshotTransfer",
        "ReloadReads",
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

/// §20.2's table. Each capability's homes are the paths the spec assigns it;
/// a home inside a crate that has not landed yet covers nothing until it does.
const CAPABILITIES: Capabilities = Capabilities {
    network: Capability {
        name: "network",
        homes: &["crates/coven-storage/src/providers/"],
        gates: &[
            Gate {
                kind: "CloudKit framework",
                crates: &["objc2_cloud_kit"],
                path_patterns: &[],
                method_patterns: &[],
            },
            Gate {
                kind: "HTTP client (reqwest)",
                crates: &["reqwest"],
                path_patterns: &[],
                method_patterns: &[],
            },
            Gate {
                kind: "HTTP server (axum)",
                crates: &["axum"],
                path_patterns: &[],
                method_patterns: &[],
            },
            Gate {
                kind: "AWS SDK",
                crates: &[
                    "aws_config",
                    "aws_sdk_s3",
                    "aws_sdk_sts",
                    "aws_credential_types",
                    "aws_smithy_http_client",
                    "aws_smithy_runtime_api",
                    "aws_smithy_types",
                    "smithy_transport_reqwest",
                ],
                path_patterns: &[],
                method_patterns: &[],
            },
            Gate {
                kind: "browser opener (open)",
                crates: &["open"],
                path_patterns: &[],
                method_patterns: &[],
            },
            Gate {
                kind: "sockets",
                crates: &[],
                path_patterns: &[&["std", "net"], &["tokio", "net"]],
                method_patterns: &[],
            },
        ],
    },
    cryptography: Capability {
        name: "cryptography",
        homes: &["crates/coven-crypto/src/"],
        gates: &[
            Gate {
                kind: "signing keys (ed25519-dalek)",
                crates: &["ed25519_dalek"],
                path_patterns: &[],
                method_patterns: &[],
            },
            Gate {
                kind: "sealed-box keys (crypto_box / x25519-dalek)",
                crates: &["crypto_box", "x25519_dalek"],
                path_patterns: &[],
                method_patterns: &[],
            },
            Gate {
                kind: "AEAD cipher (chacha20poly1305)",
                crates: &["chacha20poly1305"],
                path_patterns: &[],
                method_patterns: &[],
            },
            Gate {
                kind: "key derivation (hkdf / argon2)",
                crates: &["hkdf", "argon2"],
                path_patterns: &[],
                method_patterns: &[],
            },
            Gate {
                kind: "keyed hashes (hmac / sha2)",
                crates: &["hmac", "sha2"],
                path_patterns: &[],
                method_patterns: &[],
            },
            Gate {
                kind: "randomness",
                crates: &["rand", "rand_core", "getrandom"],
                path_patterns: &[&["OsRng"], &["thread_rng"], &["ThreadRng"]],
                method_patterns: &[],
            },
        ],
    },
    // The raw handles and coven's own SQL are the database boundary's subject
    // (`database_boundary.rs`); this row names the database's home and the
    // SQLite library beneath rusqlite.
    sqlite: Capability {
        name: "SQLite",
        homes: &["crates/coven-database/src/"],
        gates: &[Gate {
            kind: "SQLite library (rusqlite / libsqlite3-sys)",
            crates: &["rusqlite", "libsqlite3_sys"],
            path_patterns: &[],
            method_patterns: &[],
        }],
    },
    keychain: Capability {
        name: "OS keychain",
        homes: &["crates/coven-crypto/src/custody/"],
        gates: &[Gate {
            kind: "platform keychain",
            crates: &[
                "keyring",
                "keyring_core",
                "apple_native_keyring_store",
                "android_native_keyring_store",
                "windows_native_keyring_store",
                "zbus_secret_service_keyring_store",
                "security_framework",
            ],
            path_patterns: &[],
            method_patterns: &[],
        }],
    },
    time: Capability {
        name: "current time",
        homes: &["crates/coven-foundation/src/clock.rs"],
        gates: &[Gate {
            kind: "system clock",
            crates: &[],
            path_patterns: &[
                &["SystemTime", "now"],
                &["Instant", "now"],
                &["Utc", "now"],
                &["Local", "now"],
                &["OffsetDateTime", "now_utc"],
                &["OffsetDateTime", "now_local"],
                &["tokio", "time"],
                &["thread", "sleep"],
                &["thread", "sleep_until"],
            ],
            method_patterns: &[],
        }],
    },
    // Only generation is gated: parsing and validating an id is a
    // deterministic transformation any module may perform.
    ids: Capability {
        name: "new ids",
        homes: &["crates/coven-foundation/src/id_source.rs"],
        gates: &[Gate {
            kind: "id generation (uuid)",
            crates: &[],
            path_patterns: &[
                &["Uuid", "new_v4"],
                &["Uuid", "new_v7"],
                &["Uuid", "now_v7"],
            ],
            method_patterns: &[],
        }],
    },
    files: Capability {
        name: "files on disk",
        homes: &["crates/coven-foundation/src/files/"],
        gates: &[Gate {
            kind: "filesystem",
            crates: &["tempfile"],
            path_patterns: &[
                &["std", "fs"],
                &["tokio", "fs"],
                &["unix", "fs"],
                &["windows", "fs"],
                &["rustix", "fs"],
                &["windows_sys", "Win32", "Storage", "FileSystem"],
            ],
            method_patterns: &[],
        }],
    },
};

#[cfg(test)]
#[path = "owner_policy_tests.rs"]
mod tests;
