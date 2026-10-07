use super::*;
use crate::owner_policy::POLICY;

fn violations(path: &str, source: &str) -> Vec<CapabilityBoundaryViolation> {
    find_capability_boundary_violations(&[RustFile::fixture(path, source)], &POLICY)
}

fn kinds(path: &str, source: &str) -> BTreeSet<&'static str> {
    violations(path, source)
        .into_iter()
        .map(|violation| violation.kind)
        .collect()
}

#[test]
fn network_crates_are_rejected_outside_the_providers() {
    assert_eq!(
        kinds(
            "crates/coven-sync/src/leak.rs",
            r#"
            use reqwest::Client;
            fn serve() { let router = axum::Router::new(); }
            async fn fetch() { let _ = reqwest::get("https://example.com").await; }
            async fn connect() { let _ = tokio::net::TcpStream::connect("host:443").await; }
            "#,
        ),
        BTreeSet::from(["HTTP client (reqwest)", "HTTP server (axum)", "sockets"]),
    );
}

#[test]
fn network_crates_are_allowed_in_the_providers() {
    let files = [
        RustFile::fixture(
            "crates/coven-storage/src/providers/google_drive.rs",
            "use reqwest::Client;",
        ),
        RustFile::fixture(
            "crates/coven-storage/src/providers/oauth.rs",
            "use axum::Router; use open;",
        ),
        RustFile::fixture(
            "crates/coven-storage/src/providers/s3.rs",
            "use aws_sdk_s3::Client;",
        ),
    ];
    assert!(find_capability_boundary_violations(&files, &POLICY).is_empty());
}

#[test]
fn network_crates_are_rejected_in_storage_outside_the_providers() {
    assert_eq!(
        kinds("crates/coven-storage/src/upload.rs", "use reqwest::Client;"),
        BTreeSet::from(["HTTP client (reqwest)"]),
    );
}

#[test]
fn cfg_test_items_and_test_sources_are_exempt() {
    let files = [
        RustFile::fixture(
            "crates/coven-sync/src/workflow.rs",
            r#"
            #[cfg(test)]
            fn helper() { let _ = reqwest::Client::new(); }
            "#,
        ),
        RustFile::fixture(
            "crates/coven-sync/src/workflow_tests.rs",
            "use reqwest::Client;",
        ),
    ];
    assert!(find_capability_boundary_violations(&files, &POLICY).is_empty());
}

#[test]
fn signing_primitives_are_rejected_outside_coven_crypto() {
    let violations = violations(
        "crates/coven-sync/src/leak.rs",
        r#"
        use ed25519_dalek::SigningKey;
        fn forge() { let _ = ed25519_dalek::Signature::from_bytes(&[0; 64]); }
        "#,
    );
    assert_eq!(violations.len(), 2);
    assert!(violations
        .iter()
        .all(|violation| violation.kind == "signing keys (ed25519-dalek)"
            && violation.capability == "cryptography"));
}

#[test]
fn coven_crypto_owns_every_primitive() {
    let files = [
        RustFile::fixture("crates/coven-crypto/src/derive.rs", "use hkdf::Hkdf; use sha2::Sha256;"),
        RustFile::fixture("crates/coven-crypto/src/naming.rs", "use hmac::Mac;"),
        RustFile::fixture(
            "crates/coven-crypto/src/sealing.rs",
            "use crypto_box::SalsaBox; use chacha20poly1305::XChaCha20Poly1305; use rand::rngs::OsRng;",
        ),
    ];
    assert!(find_capability_boundary_violations(&files, &POLICY).is_empty());
}

#[test]
fn the_keychain_is_rejected_outside_custody_even_inside_coven_crypto() {
    let violations = violations(
        "crates/coven-crypto/src/keys.rs",
        "use keyring_core::Entry;",
    );
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].kind, "platform keychain");
    assert!(kinds(
        "crates/coven-crypto/src/custody/keychain.rs",
        "use keyring_core::Entry;"
    )
    .is_empty());
}

#[test]
fn sqlite_beneath_rusqlite_is_rejected_outside_coven_database() {
    assert_eq!(
        kinds(
            "crates/coven-sync/src/leak.rs",
            "fn open() { unsafe { libsqlite3_sys::sqlite3_initialize(); } }",
        ),
        BTreeSet::from(["SQLite library (rusqlite / libsqlite3-sys)"]),
    );
}

#[test]
fn runtime_construction_is_rejected_outside_lifetime_authorities() {
    let violations = violations(
        "crates/coven-sync/src/transfer.rs",
        r#"
        fn build() {
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        }
        fn build_imported() {
            use tokio::runtime::Runtime;
            let _ = Runtime::new();
        }
        "#,
    );
    assert!(!violations.is_empty());
    assert!(violations
        .iter()
        .all(|violation| violation.kind == "runtime construction"));
}

#[test]
fn a_lifetime_authority_starts_its_work() {
    const AUTHORITY: Policy = Policy {
        capabilities: crate::policy::Capabilities {
            runtimes: Capability {
                homes: &["crates/coven-sync/src/sync_loop.rs"],
                ..POLICY.capabilities.runtimes
            },
            ..POLICY.capabilities
        },
        ..POLICY
    };
    let source = r#"
        fn start(handle: tokio::runtime::Handle) {
            let _ = tokio::runtime::Builder::new_multi_thread().build();
            handle.spawn(async {});
            std::thread::spawn(|| {});
        }
    "#;
    let file = |path| [RustFile::fixture(path, source)];
    assert!(find_capability_boundary_violations(
        &file("crates/coven-sync/src/sync_loop.rs"),
        &AUTHORITY
    )
    .is_empty());
    let elsewhere =
        find_capability_boundary_violations(&file("crates/coven-sync/src/upload.rs"), &AUTHORITY)
            .into_iter()
            .map(|violation| violation.kind)
            .collect::<BTreeSet<_>>();
    assert_eq!(
        elsewhere,
        BTreeSet::from(["runtime construction", "thread or task spawn"])
    );
}

#[test]
fn spawning_work_is_rejected_outside_lifetime_authorities() {
    let violations = violations(
        "crates/coven-sync/src/upload.rs",
        r#"
        async fn upload(handle: tokio::runtime::Handle) {
            tokio::spawn(async {});
            tokio::task::spawn_blocking(|| {});
            handle.spawn(async {});
        }
        "#,
    );
    assert_eq!(violations.len(), 3);
    assert!(violations
        .iter()
        .all(|violation| violation.kind == "thread or task spawn"));
}

#[test]
fn runtime_handles_are_injectable_but_not_ambiently_acquired() {
    assert!(kinds(
        "crates/coven-sync/src/staging.rs",
        r#"
        struct Staging { runtime: tokio::runtime::Handle }
        impl Staging {
            fn new(runtime: tokio::runtime::Handle) -> Self { Self { runtime } }
        }
        "#,
    )
    .is_empty());

    let violations = violations(
        "crates/coven-sync/src/workflow.rs",
        "fn grab() { let _ = tokio::runtime::Handle::current(); }",
    );
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].kind, "ambient runtime acquisition");
}

#[test]
fn time_ids_and_randomness_are_rejected_outside_their_homes() {
    assert_eq!(
        kinds(
            "crates/coven-merge/src/leak.rs",
            r#"
            fn stamp() -> std::time::SystemTime { std::time::SystemTime::now() }
            fn when() -> chrono::DateTime<chrono::Utc> { chrono::Utc::now() }
            fn token() -> String { uuid::Uuid::new_v4().to_string() }
            fn entropy() { use rand::RngCore; rand::rng().fill_bytes(&mut [0u8; 8]); }
            "#,
        ),
        BTreeSet::from(["system clock", "id generation (uuid)", "randomness"]),
    );
}

#[test]
fn the_clock_and_the_id_source_own_their_reads() {
    let files = [
        RustFile::fixture(
            "crates/coven-foundation/src/clock.rs",
            "fn now() -> chrono::DateTime<chrono::Utc> { chrono::Utc::now() }",
        ),
        RustFile::fixture(
            "crates/coven-foundation/src/id_source.rs",
            "fn new_id() -> String { uuid::Uuid::new_v4().to_string() }",
        ),
    ];
    assert!(find_capability_boundary_violations(&files, &POLICY).is_empty());
}

#[test]
fn timers_belong_to_the_injected_clock() {
    for source in [
        "async fn wait() { tokio::time::sleep(duration).await; }",
        "async fn wait() { tokio::time::sleep_until(deadline).await; }",
        "fn retry() { tokio::time::interval(duration); }",
        "fn retry() { tokio::time::interval_at(deadline, duration); }",
        "use tokio::time::sleep as pause; async fn wait() { pause(duration).await; }",
        "use tokio::time as timers; async fn wait() { timers::sleep(duration).await; }",
        "async fn wait() { tokio::select! { _ = tokio::time::sleep(duration) => {} } }",
        "fn wait() { std::thread::sleep(duration); }",
        "fn wait() { std::thread::sleep_until(deadline); }",
    ] {
        assert_eq!(
            kinds("crates/coven-sync/src/operations.rs", source),
            BTreeSet::from(["system clock"]),
            "{source}",
        );
        assert!(kinds("crates/coven-foundation/src/clock.rs", source).is_empty());
        assert!(kinds("crates/coven-sync/src/operations_tests.rs", source).is_empty());
    }
    assert!(kinds(
        "crates/coven-sync/src/operations.rs",
        "async fn wait(clock: ClockRef) { clock.sleep(std::time::Duration::from_secs(1)).await; }",
    )
    .is_empty());
}

#[test]
fn files_are_rejected_outside_foundations_file_boundary() {
    assert_eq!(
        kinds(
            "crates/coven-sync/src/leak.rs",
            r#"
            use std::fs;
            fn stage() { let _ = tempfile::NamedTempFile::new(); }
            async fn read() { let _ = tokio::fs::read("path").await; }
            "#,
        ),
        BTreeSet::from(["filesystem"]),
    );
    assert!(kinds(
        "crates/coven-foundation/src/files/atomic_file.rs",
        "use std::fs::File;"
    )
    .is_empty());
}

#[test]
fn platform_file_operations_obey_the_same_filesystem_boundary() {
    for source in [
        "use rustix::fs::{renameat_with, RenameFlags};",
        "use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, SetFileAttributesW};",
    ] {
        assert_eq!(
            kinds("crates/coven-sync/src/leak.rs", source),
            BTreeSet::from(["filesystem"]),
        );
        assert!(kinds("crates/coven-foundation/src/files/atomic_file.rs", source).is_empty());
    }
}

#[test]
fn a_local_item_sharing_a_gated_crate_name_is_not_a_crate_reference() {
    assert!(kinds(
        "crates/coven-crypto/src/sealing.rs",
        r#"
        fn open(key: &[u8], nonce: &[u8], ciphertext: &[u8]) -> Vec<u8> { Vec::new() }
        fn unlock() { let _ = open(&[], &[], &[]); }
        "#,
    )
    .is_empty());

    assert_eq!(
        kinds("crates/coven-crypto/src/sealing.rs", "use open;"),
        BTreeSet::from(["browser opener (open)"])
    );
}

#[test]
fn id_validation_is_not_gated_generation() {
    assert!(kinds(
        "crates/coven-sync/src/session.rs",
        "fn validate(value: &str) -> bool { uuid::Uuid::parse_str(value).is_ok() }",
    )
    .is_empty());
}

#[test]
fn doc_comments_do_not_trip_capability_boundaries() {
    assert!(kinds(
        "crates/coven-sync/src/workflow.rs",
        r#"
        /// Uses reqwest internally via the storage owner; see tokio::runtime docs.
        fn documented() {}
        "#,
    )
    .is_empty());
}

#[test]
fn a_capability_used_inside_a_macro_call_is_a_direct_use() {
    let source = r#"fn label() -> String { format!("{}", uuid::Uuid::new_v4()) }"#;
    assert_eq!(
        kinds("crates/coven-sync/src/label.rs", source),
        BTreeSet::from(["id generation (uuid)"])
    );
    assert!(kinds("crates/coven-foundation/src/id_source.rs", source).is_empty());
}

#[test]
fn nested_macro_calls_and_method_calls_inside_them_are_read() {
    assert_eq!(
        kinds(
            "crates/coven-sync/src/upload.rs",
            r#"
            fn start(handle: tokio::runtime::Handle) {
                assert!(vec![handle.spawn(async {})].len() == 1);
                debug_assert_eq!(std::time::SystemTime::now(), std::time::UNIX_EPOCH);
            }
            "#,
        ),
        BTreeSet::from(["system clock", "thread or task spawn"])
    );
}

#[test]
fn a_macro_rules_body_is_read_as_tokens() {
    assert_eq!(
        kinds(
            "crates/coven-sync/src/macros.rs",
            r#"
            macro_rules! stamp {
                ($handle:expr) => {
                    $handle.spawn(async move { ::std::fs::write("stamp", ::std::time::SystemTime::now()) })
                };
            }
            "#,
        ),
        BTreeSet::from(["filesystem", "system clock", "thread or task spawn"])
    );
}

#[test]
fn rusqlite_is_used_only_by_the_database_crate() {
    let source = "use rusqlite::Connection; fn open() { let _ = Connection::open_in_memory(); }";
    assert_eq!(
        kinds("crates/coven-storage/src/leak.rs", source),
        BTreeSet::from(["SQLite library (rusqlite / libsqlite3-sys)"])
    );
    assert!(kinds("crates/coven-database/src/sqlite.rs", source).is_empty());
}
