//! The policy file of §21.2. It names only what exists; each crate's PR adds
//! the rows for what it introduces. `owner_policy_tests.rs` holds the guard
//! tests that fail when a row names a crate, file, type or method that is not
//! in the workspace.

use crate::policy::{Capabilities, Capability, Gate, Policy};

pub(crate) const POLICY: Policy = Policy {
    // Rows are added as crates land, in §21.1's order: coven-foundation,
    // coven-crypto, coven-merge, coven-format, coven-database, coven-storage,
    // coven-sync, coven.
    crate_order: &[
        "coven-foundation",
        "coven-crypto",
        "coven-merge",
        "coven-format",
        "coven-database",
        "coven-storage",
    ],
    separated_crates: &[("coven-database", "coven-storage")],
    capabilities: CAPABILITIES,
    database_schema: Some((
        "crates/coven-database/src/internal_schema.rs",
        &["coven_tables"],
    )),
    composition_roots: &[
        (
            "crates/coven-storage/src/providers/s3.rs",
            "S3Storage",
            "new",
        ),
        (
            "crates/coven-database/src/database.rs",
            "DatabaseBuilder",
            "open_graph",
        ),
        (
            "crates/coven-database/src/database.rs",
            "DatabaseBuilder",
            "open_read_graph",
        ),
    ],
    lifetime_authorities: &[
        ("ReconfigurableLiveQuery", "Database"),
        ("LiveQuery", "Database"),
    ],
    // These methods derive a scoped capability from their injected directory.
    // They do not acquire an unrelated directory or file for another owner.
    capability_factories: &[
        (
            "crates/coven-foundation/src/files/directory.rs",
            "StoreDir",
            "lock_exclusive",
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
        "StoreDir",
        "AtomicFile",
        "StoreLock",
        "Keychain",
        "StoreKeychain",
        "KeyringCustody",
        "StoreKeyCustody",
        "MemberKeyCustody",
        "Storage",
        "CloudKitOps",
        "OAuthClients",
        "OAuthSession",
        "DatabaseConnection",
    ],
    capability_traits: &[
        "Clock",
        "IdSource",
        "StoreKeyCustody",
        "MemberKeyCustody",
        "Storage",
        "CloudKitOps",
    ],
    construction_only_capability_types: &[
        "StoreDir",
        "StoreLock",
        "AtomicFile",
        "ClockRef",
        "IdSourceRef",
        "Keychain",
        "StoreKeychain",
        "OAuthClients",
        "OAuthSession",
        "DatabaseConnection",
    ],
    non_owner_types: &["KeyCustody", "IdentityCustody"],
    // A write borrows the database's retained connection and StoreDir; app SQL
    // uses that write as its capability and never receives either dependency.
    borrowed_facade_types: &[
        "FileWrite",
        "SqlReadContext",
        "Read",
        "ReadOwner",
        "ChangeCapture",
        "MigrationContext",
        "SqlContext",
        "SqlTransaction",
        "ReaderLease",
        "AppView",
        "MergeStore",
        "WriteMetadata",
        "DatabaseRemovalView",
        "WriteApply",
    ],
    root_owner_types: &["Database", "CovenReadHandle"],
    task_types: &[
        "FileWrite",
        "SqlReadContext",
        "Read",
        "ReadOwner",
        "ChangeCapture",
        "LiveQuery",
        "ReconfigurableLiveQuery",
        "SqlTransaction",
        "ReaderLease",
        "AppView",
        "MergeStore",
        "WriteMetadata",
        "DatabaseRemovalView",
        "WriteApply",
    ],
    internal_dependency_types: &["DatabaseConnection"],
    always_forbidden_returns: &[],
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
    ],
    field_capability_types: &[],
    raw_provider_operations: &[],
    derived_services: &[],
    unexported_capability_types: &[],
    exportable_capability_outputs: &[],
};

/// §21.2's table. Each capability's homes are the paths the spec assigns it;
/// a home inside a crate that has not landed yet covers nothing until it does.
const CAPABILITIES: Capabilities = Capabilities {
    network: Capability {
        name: "network",
        homes: &["crates/coven-storage/src/providers/"],
        gates: &[
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
        homes: &[
            "crates/coven-foundation/src/files/",
            "crates/coven-sync/src/file_cache/",
        ],
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
    // Database calls await blocking work while retaining the database owner.
    // There is no long-lived task or runtime construction in this graph.
    runtimes: Capability {
        name: "runtimes and spawned work",
        homes: &["crates/coven-database/src/database.rs"],
        gates: &[
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
    },
};

#[cfg(test)]
#[path = "owner_policy_tests.rs"]
mod tests;
