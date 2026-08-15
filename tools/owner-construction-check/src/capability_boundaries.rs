//! Capability boundaries: each raw capability — an HTTP client, a signing
//! primitive, the platform keyring, runtime construction, ambient time and
//! entropy, the filesystem — has a declared set of implementation homes.
//! Production code outside those homes cannot name the capability's crates or
//! call paths; it composes the owner that retains the capability instead.
//!
//! The check is syntactic, like the database boundary: naming the crate or the
//! construction path is the violation, so a new leak fails before compilation
//! and review. Test sources and `cfg(test)` items are exempt — fixtures may
//! assemble raw material — except where a boundary's homes already cover the
//! test's subject.

use std::collections::BTreeSet;

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::{is_test_only, is_test_source, RustFile};

/// One gated capability: naming any of `crates`, or writing a path that
/// contains any of `path_patterns` as adjacent segments, is forbidden outside
/// the `allowed` path prefixes.
pub(crate) struct GatedCapability {
    pub(crate) kind: &'static str,
    pub(crate) crates: &'static [&'static str],
    pub(crate) path_patterns: &'static [&'static [&'static str]],
    pub(crate) allowed: &'static [&'static str],
}

const NETWORK_HOMES: &[&str] = &[
    "crates/coven-storage/src/cloud/",
    "crates/coven-storage/src/oauth/",
    "crates/coven-storage/src/oauth.rs",
];

/// Only cloud-provider implementations and the OAuth flow speak HTTP. Every
/// other module composes storage and OAuth capabilities.
pub(crate) const NETWORK_BOUNDARY: &[GatedCapability] = &[
    GatedCapability {
        kind: "HTTP client (reqwest)",
        crates: &["reqwest"],
        path_patterns: &[],
        allowed: NETWORK_HOMES,
    },
    GatedCapability {
        kind: "HTTP server (axum)",
        crates: &["axum"],
        path_patterns: &[],
        allowed: NETWORK_HOMES,
    },
    GatedCapability {
        kind: "AWS SDK",
        crates: &[
            "aws_config",
            "aws_sdk_s3",
            "aws_sdk_sts",
            "aws_credential_types",
            "aws_smithy_http_client",
            "aws_smithy_runtime_api",
            "aws_smithy_types",
        ],
        path_patterns: &[],
        allowed: &["crates/coven-storage/src/cloud/"],
    },
    GatedCapability {
        kind: "browser opener (open)",
        crates: &["open"],
        path_patterns: &[],
        allowed: &["crates/coven-storage/src/oauth.rs"],
    },
];

/// Key-bearing primitives live with the custody, encryption, and protocol
/// owners that enforce how key material is created, wrapped, and used.
pub(crate) const CRYPTO_BOUNDARY: &[GatedCapability] = &[
    GatedCapability {
        kind: "signing keys (ed25519-dalek)",
        crates: &["ed25519_dalek"],
        path_patterns: &[],
        allowed: &[
            "crates/coven-keys/src/keys/",
            "crates/coven-keys/src/identity_custody.rs",
        ],
    },
    GatedCapability {
        kind: "sealed-box keys (crypto_box / x25519-dalek)",
        crates: &["crypto_box", "x25519_dalek"],
        path_patterns: &[],
        allowed: &["crates/coven-keys/src/keys/"],
    },
    GatedCapability {
        kind: "AEAD cipher (chacha20poly1305)",
        crates: &["chacha20poly1305"],
        path_patterns: &[],
        allowed: &[
            "crates/coven-keys/src/encryption.rs",
            "crates/coven-keys/src/envelope.rs",
        ],
    },
    GatedCapability {
        kind: "passphrase KDF (argon2)",
        crates: &["argon2"],
        path_patterns: &[],
        allowed: &[
            "crates/coven-keys/src/custody.rs",
            "crates/coven-keys/src/envelope.rs",
            "crates/coven-keys/src/identity_custody.rs",
        ],
    },
    GatedCapability {
        kind: "key derivation (hkdf)",
        crates: &["hkdf"],
        path_patterns: &[],
        allowed: &[
            "crates/coven-keys/src/encryption.rs",
            "crates/coven-protocol/src/circle.rs",
        ],
    },
    GatedCapability {
        kind: "keyed MAC (hmac)",
        crates: &["hmac"],
        path_patterns: &[],
        allowed: &[
            "crates/coven-protocol/src/circle.rs",
            "crates/coven-protocol/src/circle_control.rs",
            // Google XML API authentication is provider wire signing, owned by
            // the storage adapter that retains the provider credentials.
            "crates/coven-storage/src/cloud/s3/google_cloud_storage.rs",
        ],
    },
];

/// The OS keyring is device-key custody. Only the key service and the
/// keyring-backend installer touch it; everything else holds `StoreKeys`.
pub(crate) const KEYRING_BOUNDARY: &[GatedCapability] = &[GatedCapability {
    kind: "platform keyring",
    crates: &[
        "keyring_core",
        "apple_native_keyring_store",
        "android_native_keyring_store",
        "windows_native_keyring_store",
        "security_framework",
    ],
    path_patterns: &[],
    allowed: &[
        "crates/coven-keys/src/keys/",
        "crates/coven-keys/src/keyring_backend.rs",
    ],
}];

/// Runtimes are constructed by their declared lifetime owners: the sync loop's
/// dedicated thread, the cloud runtime, and resumable-upload drop cancellation.
/// Retaining or passing a `tokio::runtime::Handle` is injection and is not
/// gated; spawning onto the caller's current runtime is not gated either — the
/// identity of that runtime is coherent by construction. Ambient acquisition
/// (`Handle::current`) is a process-state read and has its own reviewed homes:
/// the host API edge that donates the host's runtime to blob staging, and the
/// connection thread's drop path that must detect whether joining would stall
/// an executor worker.
pub(crate) const RUNTIME_BOUNDARY: &[GatedCapability] = &[
    GatedCapability {
        kind: "tokio runtime construction",
        crates: &[],
        path_patterns: &[
            &["Runtime", "new"],
            &["Builder", "new_current_thread"],
            &["Builder", "new_multi_thread"],
        ],
        allowed: &[
            "crates/coven-replication/src/sync/sync_loop.rs",
            "crates/coven-replication/src/sync/sync_loop/",
            "crates/coven-storage/src/cloud/runtime.rs",
            "crates/coven-storage/src/cloud/resumable.rs",
        ],
    },
    GatedCapability {
        kind: "ambient runtime acquisition",
        crates: &[],
        path_patterns: &[&["Handle", "current"], &["Handle", "try_current"]],
        allowed: &[
            "crates/coven/src/store_sync/blobs.rs",
            "crates/coven-database/src/database_connection.rs",
        ],
    },
];

/// Ambient time, entropy, and identity come from the injected `ClockRef`,
/// `IdProvider`, and the key/encryption owners. Reading them anywhere else
/// hides a dependency the owner graph should carry.
pub(crate) const AMBIENT_BOUNDARY: &[GatedCapability] = &[
    GatedCapability {
        kind: "ambient wall clock",
        crates: &[],
        path_patterns: &[&["SystemTime", "now"], &["Instant", "now"], &["Utc", "now"]],
        allowed: &["crates/coven-foundation/src/clock.rs"],
    },
    GatedCapability {
        kind: "ambient randomness",
        crates: &["rand"],
        path_patterns: &[&["OsRng"], &["thread_rng"], &["ThreadRng"]],
        allowed: &[
            "crates/coven-keys/src/keys/",
            "crates/coven-keys/src/encryption.rs",
            "crates/coven-keys/src/envelope.rs",
            "crates/coven-storage/src/oauth.rs",
            "crates/coven-storage/src/cloud/oauth_session.rs",
        ],
    },
    // Only generation is gated: parsing and validating a UUID value is a
    // deterministic transformation any module may perform.
    GatedCapability {
        kind: "ambient identifier generation (uuid)",
        crates: &[],
        path_patterns: &[&["Uuid", "new_v4"]],
        allowed: &["crates/coven-foundation/src/id_provider.rs"],
    },
];

/// Files and directories are owned by the staging, store-directory, database,
/// and provider owners that carry their rollback and durability obligations.
pub(crate) const FILESYSTEM_BOUNDARY: &[GatedCapability] = &[GatedCapability {
    kind: "raw filesystem access",
    crates: &["tempfile"],
    path_patterns: &[&["std", "fs"], &["tokio", "fs"]],
    allowed: FILESYSTEM_HOMES,
}];

/// Reviewed filesystem owners. Each entry either owns a filesystem lifetime
/// (staging, rollback, cache, spool, lock, config, sealed files, database
/// images) or is the provider/local-storage implementation whose subject is
/// the file:
///
/// - `atomic_file.rs` / `store_dir.rs`: the staged-write and store-directory
///   owners, including the single-writer open guard.
/// - `config.rs`, `custody.rs`, `envelope.rs`, `identity_custody.rs`: config
///   and sealed-secret files.
/// - `database/`: SQLite files, staged database images, the device-join
///   journal.
/// - `storage/`: provider implementations, local files, and spools.
/// - `blob/transition.rs`: `ExactPlaintextFile` and blob locality moves.
/// - `sync/store/blob.rs`: local blob access and cache materialization.
/// - `host_write.rs`, `blob_preparation.rs`, `snapshot.rs`,
///   `snapshot/image.rs`: blob and snapshot staging with rollback state.
const FILESYSTEM_HOMES: &[&str] = &[
    "crates/coven-foundation/src/atomic_file.rs",
    "crates/coven-foundation/src/local_file.rs",
    "crates/coven-foundation/src/store_dir.rs",
    "crates/coven-foundation/src/config.rs",
    "crates/coven-keys/src/custody.rs",
    "crates/coven-keys/src/envelope.rs",
    "crates/coven-keys/src/identity_custody.rs",
    "crates/coven-database/src/",
    "crates/coven-storage/src/",
    "crates/coven-replication/src/blob/transition.rs",
    "crates/coven-replication/src/sync/store/blob.rs",
    "crates/coven-replication/src/sync/store/host_write.rs",
    "crates/coven-replication/src/sync/store/commit_publication/operation/blob_preparation.rs",
    "crates/coven-replication/src/sync/store/snapshots/mod.rs",
    "crates/coven-replication/src/sync/store/snapshots/image.rs",
];

pub(crate) struct CapabilityBoundaryViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) kind: &'static str,
    pub(crate) homes: &'static [&'static str],
}

impl CapabilityBoundaryViolation {
    fn key(&self) -> (String, usize, &'static str) {
        (self.path.clone(), self.line, self.kind)
    }
}

pub(crate) fn find_capability_boundary_violations(
    files: &[RustFile],
    boundary: &'static [GatedCapability],
) -> Vec<CapabilityBoundaryViolation> {
    let mut violations: Vec<CapabilityBoundaryViolation> = Vec::new();
    let mut seen = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        let gated: Vec<&GatedCapability> = boundary
            .iter()
            .filter(|capability| {
                !capability
                    .allowed
                    .iter()
                    .any(|home| file.relative_path.starts_with(home))
            })
            .collect();
        if gated.is_empty() {
            continue;
        }
        let mut visitor = CapabilityBoundaryVisitor {
            path: &file.relative_path,
            gated: &gated,
            violations: &mut violations,
            seen: &mut seen,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.sort_by(|a, b| a.key().cmp(&b.key()));
    violations
}

struct CapabilityBoundaryVisitor<'a> {
    path: &'a str,
    gated: &'a [&'static GatedCapability],
    violations: &'a mut Vec<CapabilityBoundaryViolation>,
    seen: &'a mut BTreeSet<(String, usize, &'static str)>,
}

impl CapabilityBoundaryVisitor<'_> {
    fn record(&mut self, capability: &'static GatedCapability, span: Span) {
        let violation = CapabilityBoundaryViolation {
            path: self.path.to_string(),
            line: span.start().line,
            kind: capability.kind,
            homes: capability.allowed,
        };
        if self.seen.insert(violation.key()) {
            self.violations.push(violation);
        }
    }

    /// `is_import` distinguishes `use` trees from expression, type, and macro
    /// paths. In an import, a bare crate name (`use open;`) references the
    /// crate; in an expression, a single-segment path (`open(...)`) is a local
    /// item and must not match a gated crate of the same name.
    fn check_segments(&mut self, segments: &[String], is_import: bool, span: Span) {
        for capability in self.gated {
            let first_is_gated_crate = (is_import || segments.len() >= 2)
                && segments
                    .first()
                    .is_some_and(|first| capability.crates.iter().any(|name| first == name));
            let contains_pattern = capability.path_patterns.iter().any(|pattern| {
                segments
                    .windows(pattern.len())
                    .any(|window| window.iter().zip(pattern.iter()).all(|(a, b)| a == b))
            });
            if first_is_gated_crate || contains_pattern {
                self.record(capability, span);
            }
        }
    }
}

impl<'ast> Visit<'ast> for CapabilityBoundaryVisitor<'_> {
    fn visit_attribute(&mut self, node: &'ast syn::Attribute) {
        if node.path().is_ident("doc") {
            return;
        }
        visit::visit_attribute(self, node);
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_item_mod(self, node);
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_item_impl(self, node);
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_item_fn(self, node);
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_impl_item_fn(self, node);
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        let mut paths = Vec::new();
        flatten_use_tree(&node.tree, &mut Vec::new(), &mut paths);
        for segments in paths {
            self.check_segments(&segments, true, node.span());
        }
    }

    fn visit_path(&mut self, node: &'ast syn::Path) {
        let segments = node
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        self.check_segments(&segments, false, node.span());
        visit::visit_path(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        let segments = node
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        self.check_segments(&segments, false, node.span());
        visit::visit_macro(self, node);
    }
}

pub(crate) fn flatten_use_tree(
    tree: &syn::UseTree,
    prefix: &mut Vec<String>,
    output: &mut Vec<Vec<String>>,
) {
    match tree {
        syn::UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            flatten_use_tree(&path.tree, prefix, output);
            prefix.pop();
        }
        syn::UseTree::Name(name) => {
            let mut segments = prefix.clone();
            segments.push(name.ident.to_string());
            output.push(segments);
        }
        syn::UseTree::Rename(rename) => {
            let mut segments = prefix.clone();
            segments.push(rename.ident.to_string());
            output.push(segments);
        }
        syn::UseTree::Glob(_) => {
            output.push(prefix.clone());
        }
        syn::UseTree::Group(group) => {
            for item in &group.items {
                flatten_use_tree(item, prefix, output);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, source: &str) -> RustFile {
        RustFile {
            relative_path: path.to_string(),
            syntax: syn::parse_file(source).expect("parse fixture"),
        }
    }

    #[test]
    fn network_crates_are_rejected_outside_their_homes() {
        let files = vec![file(
            "crates/coven-replication/src/sync/leak.rs",
            r#"
            use reqwest::Client;
            fn serve() { let router = axum::Router::new(); }
            async fn fetch() { let _ = reqwest::get("https://example.com").await; }
            "#,
        )];
        let violations = find_capability_boundary_violations(&files, NETWORK_BOUNDARY);
        let kinds = violations
            .iter()
            .map(|violation| violation.kind)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            kinds,
            BTreeSet::from(["HTTP client (reqwest)", "HTTP server (axum)"]),
        );
    }

    #[test]
    fn network_crates_are_allowed_in_provider_and_oauth_homes() {
        let files = vec![
            file(
                "crates/coven-storage/src/cloud/google_drive.rs",
                "use reqwest::Client;",
            ),
            file("crates/coven-storage/src/oauth.rs", "use axum::Router;"),
            file(
                "crates/coven-storage/src/cloud/s3.rs",
                "use aws_sdk_s3::Client;",
            ),
        ];
        assert!(find_capability_boundary_violations(&files, NETWORK_BOUNDARY).is_empty());
    }

    #[test]
    fn cfg_test_items_and_test_sources_are_exempt() {
        let files = vec![
            file(
                "crates/coven-replication/src/sync/workflow.rs",
                r#"
                #[cfg(test)]
                mod tests {
                    use axum::Router;
                    fn helper() { let _ = reqwest::Client::new(); }
                }
                "#,
            ),
            file(
                "crates/coven-replication/src/sync/workflow_tests.rs",
                "use reqwest::Client;",
            ),
        ];
        assert!(find_capability_boundary_violations(&files, NETWORK_BOUNDARY).is_empty());
    }

    #[test]
    fn signing_primitives_are_rejected_outside_key_custody() {
        let files = vec![file(
            "crates/coven-replication/src/sync/leak.rs",
            r#"
            use ed25519_dalek::SigningKey;
            fn forge() { let _ = ed25519_dalek::Signature::from_bytes(&[0; 64]); }
            "#,
        )];
        let violations = find_capability_boundary_violations(&files, CRYPTO_BOUNDARY);
        assert_eq!(violations.len(), 2);
        assert_eq!(violations[0].kind, "signing keys (ed25519-dalek)");
    }

    #[test]
    fn protocol_circle_keys_keep_their_kdf_and_mac_homes() {
        let files = vec![
            file("crates/coven-protocol/src/circle.rs", "use hkdf::Hkdf;"),
            file(
                "crates/coven-protocol/src/circle_control.rs",
                "use hmac::Mac;",
            ),
            file("crates/coven-keys/src/encryption.rs", "use hkdf::Hkdf;"),
        ];
        assert!(find_capability_boundary_violations(&files, CRYPTO_BOUNDARY).is_empty());
    }

    #[test]
    fn platform_keyring_is_rejected_outside_key_service() {
        let files = vec![file(
            "crates/coven-storage/src/cloud/oauth_session.rs",
            "use keyring_core::Entry;",
        )];
        let violations = find_capability_boundary_violations(&files, KEYRING_BOUNDARY);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].kind, "platform keyring");
    }

    #[test]
    fn runtime_construction_is_rejected_outside_declared_owners() {
        let files = vec![file(
            "crates/coven-replication/src/blob/transfer.rs",
            r#"
            fn build() {
                let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            }
            fn build_imported() {
                use tokio::runtime::Runtime;
                let _ = Runtime::new();
            }
            "#,
        )];
        let violations = find_capability_boundary_violations(&files, RUNTIME_BOUNDARY);
        assert!(!violations.is_empty());
        assert!(violations
            .iter()
            .all(|violation| violation.kind == "tokio runtime construction"));
    }

    #[test]
    fn declared_runtime_owners_construct_runtimes() {
        let files = vec![file(
            "crates/coven-storage/src/cloud/runtime.rs",
            r#"
            fn build() {
                let _ = tokio::runtime::Builder::new_multi_thread().build();
            }
            "#,
        )];
        assert!(find_capability_boundary_violations(&files, RUNTIME_BOUNDARY).is_empty());
    }

    #[test]
    fn ambient_clock_entropy_and_ids_are_rejected_outside_their_providers() {
        let files = vec![file(
            "crates/coven-replication/src/sync/leak.rs",
            r#"
            fn stamp() -> std::time::SystemTime { std::time::SystemTime::now() }
            fn when() -> chrono::DateTime<chrono::Utc> { chrono::Utc::now() }
            fn token() -> String { uuid::Uuid::new_v4().to_string() }
            fn entropy() { use rand::RngCore; rand::rng().fill_bytes(&mut [0u8; 8]); }
            "#,
        )];
        let violations = find_capability_boundary_violations(&files, AMBIENT_BOUNDARY);
        let kinds = violations
            .iter()
            .map(|violation| violation.kind)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            kinds,
            BTreeSet::from([
                "ambient wall clock",
                "ambient identifier generation (uuid)",
                "ambient randomness",
            ]),
        );
    }

    #[test]
    fn clock_and_id_providers_own_their_ambient_reads() {
        let files = vec![
            file(
                "crates/coven-foundation/src/clock.rs",
                "fn now() -> chrono::DateTime<chrono::Utc> { chrono::Utc::now() }",
            ),
            file(
                "crates/coven-foundation/src/id_provider.rs",
                "fn new_id() -> String { uuid::Uuid::new_v4().to_string() }",
            ),
        ];
        assert!(find_capability_boundary_violations(&files, AMBIENT_BOUNDARY).is_empty());
    }

    #[test]
    fn raw_filesystem_access_is_rejected_outside_declared_owners() {
        let files = vec![file(
            "crates/coven-replication/src/sync/leak.rs",
            r#"
            use std::fs;
            fn stage() { let _ = tempfile::NamedTempFile::new(); }
            async fn read() { let _ = tokio::fs::read("path").await; }
            "#,
        )];
        let violations = find_capability_boundary_violations(&files, FILESYSTEM_BOUNDARY);
        assert!(!violations.is_empty());
    }

    #[test]
    fn a_local_item_sharing_a_gated_crate_name_is_not_a_crate_reference() {
        let files = vec![file(
            "crates/coven-keys/src/envelope.rs",
            r#"
            fn open(key: &[u8], nonce: &[u8], ciphertext: &[u8]) -> Vec<u8> { Vec::new() }
            fn unlock() { let _ = open(&[], &[], &[]); }
            "#,
        )];
        assert!(find_capability_boundary_violations(&files, NETWORK_BOUNDARY).is_empty());

        let import = vec![file("crates/coven-keys/src/envelope.rs", "use open;")];
        assert_eq!(
            find_capability_boundary_violations(&import, NETWORK_BOUNDARY).len(),
            1
        );
    }

    #[test]
    fn runtime_handles_are_injectable_but_not_ambiently_acquired() {
        let retained = vec![file(
            "crates/coven-replication/src/sync/store/host_write.rs",
            r#"
            struct HostWriteBlobStaging { runtime: tokio::runtime::Handle }
            impl HostWriteBlobStaging {
                fn new(runtime: tokio::runtime::Handle) -> Self { Self { runtime } }
            }
            "#,
        )];
        assert!(find_capability_boundary_violations(&retained, RUNTIME_BOUNDARY).is_empty());

        let acquired = vec![file(
            "crates/coven-replication/src/sync/workflow.rs",
            "fn grab() { let _ = tokio::runtime::Handle::current(); }",
        )];
        let violations = find_capability_boundary_violations(&acquired, RUNTIME_BOUNDARY);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].kind, "ambient runtime acquisition");
    }

    #[test]
    fn uuid_validation_is_not_gated_generation() {
        let files = vec![file(
            "crates/coven-replication/src/sync/session.rs",
            r#"
            fn validate(value: &str) -> bool { uuid::Uuid::parse_str(value).is_ok() }
            "#,
        )];
        assert!(find_capability_boundary_violations(&files, AMBIENT_BOUNDARY).is_empty());
    }

    #[test]
    fn doc_comments_do_not_trip_capability_boundaries() {
        let files = vec![file(
            "crates/coven-replication/src/sync/workflow.rs",
            r#"
            /// Uses reqwest internally via the storage owner; see tokio::runtime docs.
            fn documented() {}
            "#,
        )];
        assert!(find_capability_boundary_violations(&files, NETWORK_BOUNDARY).is_empty());
        assert!(find_capability_boundary_violations(&files, RUNTIME_BOUNDARY).is_empty());
    }
}
