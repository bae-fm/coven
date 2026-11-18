use super::*;
use crate::crate_code::CrateCode;

fn assert_findings(
    actual: &[Finding],
    path: &str,
    lines: &[usize],
    message: &str,
    remedy: &'static str,
) {
    assert_eq!(
        actual,
        lines.iter().map(|line| Finding::new(path, *line, message, remedy)).collect::<Vec<_>>()
    );
}

/// The rule's findings in these crate files, read as the checker reads them.
fn rule(files: &[RustFile], policy: &Policy) -> Vec<Finding> {
    find_capability_boundary_violations(&CrateCode::fixture(files, &[], policy), policy)
}

fn violations(path: &str, source: &str) -> Vec<Finding> {
    files_violations(&[RustFile::fixture(path, source)])
}

fn files_violations(files: &[RustFile]) -> Vec<Finding> {
    let policy = Policy::gated_fixture();
    rule(files, &policy)
}

fn kind(finding: &Finding) -> &str {
    finding
        .message
        .rsplit_once(" (")
        .map_or(finding.message.as_str(), |(message, _)| message)
        .split(" outside")
        .next()
        .unwrap()
}

fn kinds(path: &str, source: &str) -> BTreeSet<String> {
    violations(path, source).into_iter().map(|violation| kind(&violation).to_string()).collect()
}

#[test]
fn network_crates_are_rejected_outside_the_providers() {
    assert_eq!(
        kinds(
            "crates/app-sync/src/leak.rs",
            r#"
            use reqwest::Client;
            fn serve() { let router = axum::Router::new(); }
            async fn fetch() { let _ = reqwest::get("https://example.com").await; }
            async fn connect() { let _ = tokio::net::TcpStream::connect("host:443").await; }
            "#,
        ),
        BTreeSet::from(
            ["HTTP client (reqwest)", "HTTP server (axum)", "sockets"].map(str::to_string)
        ),
    );
}

#[test]
fn network_crates_are_allowed_in_the_providers() {
    let files = [
        RustFile::fixture("crates/app-storage/src/providers/dropbox.rs", "use reqwest::Client;"),
        RustFile::fixture(
            "crates/app-storage/src/providers/oauth.rs",
            "use axum::Router; use open;",
        ),
        RustFile::fixture("crates/app-storage/src/providers/s3.rs", "use aws_sdk_s3::Client;"),
    ];
    assert!(files_violations(&files).is_empty());
}

#[test]
fn network_crates_are_rejected_in_storage_outside_the_providers() {
    assert_eq!(
        kinds("crates/app-storage/src/upload.rs", "use reqwest::Client;"),
        BTreeSet::from(["HTTP client (reqwest)"].map(str::to_string)),
    );
}

#[test]
fn cfg_test_items_and_test_sources_are_exempt() {
    let files = [
        RustFile::fixture(
            "crates/app-sync/src/workflow.rs",
            r#"
            #[cfg(test)]
            fn helper() { let _ = reqwest::Client::new(); }
            "#,
        ),
        RustFile::fixture("crates/app-sync/src/workflow_tests.rs", "use reqwest::Client;"),
    ];
    assert!(files_violations(&files).is_empty());
}

#[test]
fn signing_primitives_are_rejected_outside_app_crypto() {
    let violations = violations(
        "crates/app-sync/src/leak.rs",
        r#"
        use ed25519_dalek::SigningKey;
        fn forge() { let _ = ed25519_dalek::Signature::from_bytes(&[0; 64]); }
        "#,
    );
    assert_findings(&violations, "crates/app-sync/src/leak.rs", &[2, 3],
        "signing keys (ed25519-dalek) (cryptography) is used directly only in crates/app-crypto/src/",
        "reach a capability through the owner that holds it, given to you when you are built");
}

#[test]
fn app_crypto_owns_every_primitive() {
    let files = [
        RustFile::fixture("crates/app-crypto/src/derive.rs", "use hkdf::Hkdf; use sha2::Sha256;"),
        RustFile::fixture("crates/app-crypto/src/naming.rs", "use hmac::Mac;"),
        RustFile::fixture(
            "crates/app-crypto/src/sealing.rs",
            "use crypto_box::SalsaBox; use chacha20poly1305::XChaCha20Poly1305; use rand::rngs::OsRng;",
        ),
    ];
    assert!(files_violations(&files).is_empty());
}

#[test]
fn the_keychain_is_rejected_outside_custody_even_inside_app_crypto() {
    let violations = violations("crates/app-crypto/src/keys.rs", "use keyring_core::Entry;");
    assert_findings(
        &violations,
        "crates/app-crypto/src/keys.rs",
        &[1],
        "platform keychain (OS keychain) is used directly only in crates/app-crypto/src/custody/",
        "reach a capability through the owner that holds it, given to you when you are built",
    );
    assert!(
        kinds("crates/app-crypto/src/custody/keychain.rs", "use keyring_core::Entry;").is_empty()
    );
}

#[test]
fn sqlite_beneath_rusqlite_is_rejected_outside_app_database() {
    assert_eq!(
        kinds(
            "crates/app-sync/src/leak.rs",
            "fn open() { unsafe { libsqlite3_sys::sqlite3_initialize(); } }",
        ),
        BTreeSet::from(["SQLite library (rusqlite / libsqlite3-sys)"].map(str::to_string)),
    );
}

#[test]
fn runtime_construction_is_rejected_outside_composition_roots() {
    let violations = violations(
        "crates/app-sync/src/transfer.rs",
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
    assert_findings(
        &violations,
        "crates/app-sync/src/transfer.rs",
        &[3, 7],
        "runtime construction outside a composition root",
        "start background work only at a listed root, which must keep and stop it",
    );
}

#[test]
fn a_lifetime_authority_starts_its_work() {
    let mut authority = Policy::gated_fixture();
    authority.composition_roots.push(crate::policy::CompositionRoot {
        path: "crates/app-sync/src/sync_loop.rs".into(),
        owner: "<free>".into(),
        method: "start".into(),
    });
    let source = r#"
        fn start(handle: tokio::runtime::Handle) {
            let _ = tokio::runtime::Builder::new_multi_thread().build();
            handle.spawn(async {});
            std::thread::spawn(|| {});
        }
    "#;
    let file = |path| [RustFile::fixture(path, source)];
    assert!(rule(&file("crates/app-sync/src/sync_loop.rs"), &authority).is_empty());
    let elsewhere = rule(&file("crates/app-sync/src/upload.rs"), &authority)
        .into_iter()
        .map(|violation| kind(&violation).to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        elsewhere,
        BTreeSet::from(["runtime construction", "thread or future spawn"].map(str::to_string))
    );
}

/// A static listed as a root builds its value there, a runtime included.
#[test]
fn a_static_listed_as_a_root_may_start_its_work() {
    let mut authority = Policy::gated_fixture();
    authority.composition_roots.push(crate::policy::CompositionRoot {
        path: "crates/app-sync/src/runtime.rs".into(),
        owner: "<free>".into(),
        method: "RUNTIME".into(),
    });
    let source = "static RUNTIME: LazyLock<Runtime> = \
        LazyLock::new(|| tokio::runtime::Builder::new_multi_thread().build().unwrap()); \
        static OTHER: LazyLock<Runtime> = \
        LazyLock::new(|| tokio::runtime::Builder::new_multi_thread().build().unwrap());";
    let found = rule(&[RustFile::fixture("crates/app-sync/src/runtime.rs", source)], &authority);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(kind(&found[0]), "runtime construction");
}

#[test]
fn spawning_work_is_rejected_outside_composition_roots() {
    let violations = violations(
        "crates/app-sync/src/upload.rs",
        r#"
        async fn upload(handle: tokio::runtime::Handle) {
            tokio::spawn(async {});
            tokio::task::spawn_blocking(|| {});
            handle.spawn(async {});
        }
        "#,
    );
    assert_findings(
        &violations,
        "crates/app-sync/src/upload.rs",
        &[3, 4, 5],
        "thread or future spawn outside a composition root",
        "start background work only at a listed root, which must keep and stop it",
    );
}

#[test]
fn runtime_handles_are_injectable_but_not_ambiently_acquired() {
    assert!(kinds(
        "crates/app-sync/src/staging.rs",
        r#"
        struct Staging { runtime: tokio::runtime::Handle }
        impl Staging {
            fn new(runtime: tokio::runtime::Handle) -> Self { Self { runtime } }
        }
        "#,
    )
    .is_empty());

    let violations = violations(
        "crates/app-sync/src/workflow.rs",
        "fn grab() { let _ = tokio::runtime::Handle::current(); }",
    );
    assert_findings(
        &violations,
        "crates/app-sync/src/workflow.rs",
        &[1],
        "ambient runtime acquisition outside a composition root",
        "start background work only at a listed root, which must keep and stop it",
    );
}

#[test]
fn time_ids_and_randomness_are_rejected_outside_their_homes() {
    assert_eq!(
        kinds(
            "crates/app-merge/src/leak.rs",
            r#"
            fn stamp() -> std::time::SystemTime { std::time::SystemTime::now() }
            fn when() -> chrono::DateTime<chrono::Utc> { chrono::Utc::now() }
            fn token() -> String { uuid::Uuid::new_v4().to_string() }
            fn entropy() { use rand::RngCore; rand::rng().fill_bytes(&mut [0u8; 8]); }
            "#,
        ),
        BTreeSet::from(["system clock", "id generation (uuid)", "randomness"].map(str::to_string)),
    );
}

#[test]
fn the_clock_and_the_id_source_own_their_reads() {
    let files = [
        RustFile::fixture(
            "crates/app-foundation/src/clock.rs",
            "fn now() -> chrono::DateTime<chrono::Utc> { chrono::Utc::now() }",
        ),
        RustFile::fixture(
            "crates/app-foundation/src/id_source.rs",
            "fn new_id() -> String { uuid::Uuid::new_v4().to_string() }",
        ),
    ];
    assert!(files_violations(&files).is_empty());
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
            kinds("crates/app-sync/src/operations.rs", source),
            BTreeSet::from(["system clock"].map(str::to_string)),
            "{source}",
        );
        assert!(kinds("crates/app-foundation/src/clock.rs", source).is_empty());
        assert!(kinds("crates/app-sync/src/operations_tests.rs", source).is_empty());
    }
    assert!(kinds(
        "crates/app-sync/src/operations.rs",
        "async fn wait(clock: ClockRef) { clock.sleep(std::time::Duration::from_secs(1)).await; }",
    )
    .is_empty());
}

#[test]
fn files_are_rejected_outside_foundations_file_boundary() {
    assert_eq!(
        kinds(
            "crates/app-sync/src/leak.rs",
            r#"
            use std::fs;
            fn stage() { let _ = tempfile::NamedTempFile::new(); }
            async fn read() { let _ = tokio::fs::read("path").await; }
            "#,
        ),
        BTreeSet::from(["filesystem"].map(str::to_string)),
    );
    assert!(
        kinds("crates/app-foundation/src/files/atomic_file.rs", "use std::fs::File;").is_empty()
    );
}

#[test]
fn platform_file_operations_obey_the_same_filesystem_boundary() {
    for source in [
        "use rustix::fs::{renameat_with, RenameFlags};",
        "use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, SetFileAttributesW};",
    ] {
        assert_eq!(
            kinds("crates/app-sync/src/leak.rs", source),
            BTreeSet::from(["filesystem"].map(str::to_string)),
        );
        assert!(kinds("crates/app-foundation/src/files/atomic_file.rs", source).is_empty());
    }
}

#[test]
fn a_local_item_sharing_a_gated_crate_name_is_not_a_crate_reference() {
    assert!(kinds(
        "crates/app-crypto/src/sealing.rs",
        r#"
        fn open(key: &[u8], nonce: &[u8], ciphertext: &[u8]) -> Vec<u8> { Vec::new() }
        fn unlock() { let _ = open(&[], &[], &[]); }
        "#,
    )
    .is_empty());

    assert_eq!(
        kinds("crates/app-crypto/src/sealing.rs", "use open;"),
        BTreeSet::from(["browser opener (open)"].map(str::to_string))
    );
}

#[test]
fn id_validation_is_not_gated_generation() {
    assert!(kinds(
        "crates/app-sync/src/session.rs",
        "fn validate(value: &str) -> bool { uuid::Uuid::parse_str(value).is_ok() }",
    )
    .is_empty());
}

#[test]
fn doc_comments_do_not_trip_capability_boundaries() {
    assert!(kinds(
        "crates/app-sync/src/workflow.rs",
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
        kinds("crates/app-sync/src/label.rs", source),
        BTreeSet::from(["id generation (uuid)"].map(str::to_string))
    );
    assert!(kinds("crates/app-foundation/src/id_source.rs", source).is_empty());
}

#[test]
fn nested_macro_calls_and_method_calls_inside_them_are_read() {
    assert_eq!(
        kinds(
            "crates/app-sync/src/upload.rs",
            r#"
            fn start(handle: tokio::runtime::Handle) {
                assert!(vec![handle.spawn(async {})].len() == 1);
                debug_assert_eq!(std::time::SystemTime::now(), std::time::UNIX_EPOCH);
            }
            "#,
        ),
        BTreeSet::from(["system clock", "thread or future spawn"].map(str::to_string))
    );
}

/// A macro's body is read where it is used, as the code it writes there; a
/// definition no one invokes writes nothing.
#[test]
fn a_macro_rules_body_is_read_where_it_is_invoked() {
    let definition = r#"
        macro_rules! stamp {
            ($handle:expr) => {
                $handle.spawn(async move { ::std::fs::write("stamp", ::std::time::SystemTime::now()) })
            };
        }
    "#;
    assert!(kinds("crates/app-sync/src/macros.rs", definition).is_empty());
    let invoked = format!("{definition} fn record(handle: Handle) {{ stamp!(handle); }}");
    let found = violations("crates/app-sync/src/macros.rs", &invoked);
    assert_eq!(
        found.iter().map(kind).collect::<BTreeSet<_>>(),
        BTreeSet::from(["filesystem", "system clock", "thread or future spawn"])
    );
    assert!(found.iter().all(|finding| finding.line == 7), "{found:?}");
}

#[test]
fn rusqlite_is_used_only_by_the_database_crate() {
    let source = "use rusqlite::Connection; fn open() { let _ = Connection::open_in_memory(); }";
    assert_eq!(
        kinds("crates/app-storage/src/leak.rs", source),
        BTreeSet::from(["SQLite library (rusqlite / libsqlite3-sys)"].map(str::to_string))
    );
    assert!(kinds("crates/app-database/src/sqlite.rs", source).is_empty());
}

#[test]
fn sql_value_conversions_are_allowed_without_allowing_database_access() {
    let path = "crates/app-foundation/src/uuid_text.rs";
    assert!(kinds(
        path,
        r#"
        use rusqlite::ToSql;
        use rusqlite::types::{FromSql, FromSqlResult, FromSqlError, ToSqlOutput, Value, ValueRef};
        use rusqlite::types::{ToSql as Bind, Type};
        fn bind() -> rusqlite::Result<ToSqlOutput<'static>> { todo!() }
        impl rusqlite::types::ToSql for Key { fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> { todo!() } }
        fn refuse(index: usize) -> rusqlite::Error {
            rusqlite::Error::InvalidColumnType(index, String::new(), rusqlite::types::Type::Null)
        }
    "#
    )
    .is_empty());
    for source in [
        "use rusqlite::Connection;",
        "use rusqlite::ffi;",
        "use rusqlite::types::*;",
        "fn open() { rusqlite::Connection::open_in_memory(); }",
    ] {
        assert!(!kinds(path, source).is_empty(), "{source}");
    }
}

#[test]
fn an_unlisted_function_in_an_authoritys_file_cannot_spawn() {
    assert!(!violations(
        "crates/app-sync/src/sync_loop.rs",
        "fn unrelated() { tokio::spawn(async {}); }"
    )
    .is_empty());
}

/// A file system capability that gates calls: code anywhere may name a file
/// type, and only its home opens one.
const CALLS: &str = r#"
    rules = ["capability-boundaries"]
    [[capabilities]]
    name = "file system"
    homes = ["crates/app-files/src/"]
    [[capabilities.gates]]
    kind = "standard library file system"
    calls = ["std::fs"]
    [[capabilities.gates]]
    kind = "temporary files"
    calls = ["tempfile"]
"#;

/// The line and gate kind of each finding the `CALLS` policy makes in these
/// files, each of which depends on `tempfile`.
fn call_findings(files: &[(&str, &str)]) -> Vec<(String, usize, String)> {
    let policy = Policy::fixture(CALLS);
    let files = files
        .iter()
        .map(|(path, source)| {
            let mut file = RustFile::fixture(path, source);
            file.extern_crates.insert("tempfile".into(), "tempfile".into());
            file
        })
        .collect::<Vec<_>>();
    rule(&files, &policy)
        .iter()
        .map(|finding| (finding.path.clone(), finding.line, kind(finding).to_string()))
        .collect()
}

fn calls_in(source: &str) -> Vec<(usize, String)> {
    call_findings(&[("crates/app/src/leak.rs", source)])
        .into_iter()
        .map(|(_, line, kind)| (line, kind))
        .collect()
}

#[test]
fn a_call_gate_finds_the_call_through_its_import_and_not_the_import() {
    assert_eq!(
        calls_in(
            r#"
            use std::fs;
            fn load(path: &Path) -> Vec<u8> {
                fs::read(path).unwrap()
            }
            "#
        ),
        [(4, "standard library file system".to_string())]
    );
}

#[test]
fn a_call_gate_ignores_types_and_imports() {
    assert_eq!(
        calls_in(
            r#"
            use std::fs::File;
            struct Held { file: std::fs::File }
            fn len(file: &std::fs::File) -> std::fs::Metadata { todo!() }
            fn keep(file: File) -> Option<File> { Some(file) }
            "#
        ),
        []
    );
}

#[test]
fn a_call_gate_finds_a_type_s_function_through_its_import() {
    assert_eq!(
        calls_in(
            r#"
            use tempfile::TempDir;
            fn stage() -> TempDir {
                TempDir::new().unwrap()
            }
            "#
        ),
        [(4, "temporary files".to_string())]
    );
}

#[test]
fn a_call_gate_finds_a_call_inside_a_macro_call() {
    assert_eq!(
        calls_in(
            r#"
            fn label(path: &Path) -> String {
                format!("{:?}", std::fs::read_to_string(path))
            }
            fn tagged(path: &Path) {
                tracing::info!(key => std::fs::metadata(path), x: std::fs::File);
            }
            "#
        ),
        [
            (3, "standard library file system".to_string()),
            (6, "standard library file system".to_string())
        ]
    );
}

#[test]
fn a_call_gate_finds_a_function_used_as_a_value_and_one_a_module_re_exports() {
    let found = call_findings(&[
        ("crates/app/src/lib.rs", "mod files; mod leak;"),
        ("crates/app/src/files.rs", "pub(crate) use std::fs::read as load;"),
        (
            "crates/app/src/leak.rs",
            r#"
            fn all(paths: &[PathBuf]) -> Vec<Vec<u8>> {
                paths.iter().map(std::fs::read).map(Result::unwrap).collect()
            }
            fn one(path: &Path) { let _ = crate::files::load(path); }
            "#,
        ),
    ]);
    assert_eq!(
        found,
        [
            ("crates/app/src/leak.rs".to_string(), 3, "standard library file system".to_string()),
            ("crates/app/src/leak.rs".to_string(), 5, "standard library file system".to_string()),
        ]
    );
}

#[test]
fn a_call_gate_ignores_workspace_items_and_local_bindings_with_a_gated_name() {
    assert_eq!(
        calls_in(
            r#"
            mod fs { pub fn read(path: &Path) {} }
            fn load(path: &Path) { fs::read(path); }
            fn bound(tempfile: Staged) { drop(tempfile); }
            "#
        ),
        []
    );
}

/// A path whose first segment resolves to nothing, such as a crate the
/// manifest doesn't list, matches as written.
#[test]
fn a_call_gate_matches_an_unresolved_path_as_written() {
    let found = rule(
        &[RustFile::fixture("crates/app/src/leak.rs", "fn stage() { tempfile::tempdir(); }")],
        &Policy::fixture(CALLS),
    );
    assert_eq!(
        found.iter().map(|finding| (finding.line, kind(finding))).collect::<Vec<_>>(),
        [(1, "temporary files")]
    );
}

#[test]
fn a_call_gate_allows_calls_in_the_capability_s_homes() {
    assert_eq!(
        call_findings(&[(
            "crates/app-files/src/staging.rs",
            "fn stage() { let _ = tempfile::TempDir::new(); let _ = std::fs::read(\"a\"); }",
        )]),
        []
    );
}

/// The `CALLS` policy with these rows in its capability, before its gates.
fn listed_policy(rows: &str) -> Policy {
    let (capability, gates) = CALLS.split_once("[[capabilities.gates]]").expect("a gate");
    Policy::fixture(&format!("{capability}{rows}\n[[capabilities.gates]]{gates}"))
}

/// The findings `policy` makes in these files, each of which depends on
/// `tempfile`, as their path, line and message.
fn listed_findings(policy: &Policy, files: &[(&str, &str)]) -> Vec<(String, usize, String)> {
    let files = files
        .iter()
        .map(|(path, source)| {
            let mut file = RustFile::fixture(path, source);
            file.extern_crates.insert("tempfile".into(), "tempfile".into());
            file
        })
        .collect::<Vec<_>>();
    rule(&files, policy)
        .into_iter()
        .map(|finding| (finding.path, finding.line, finding.message))
        .collect()
}

const STAGING: (&str, &str) = (
    "crates/app/src/staging.rs",
    r#"
    struct Staged;
    impl Staged {
        fn stage(&self) {
            let _ = tempfile::tempdir();
            let _ = tempfile::tempdir();
        }
        fn length(&self) -> u64 { std::fs::metadata("a").unwrap().len() }
    }
    fn copy() { std::fs::copy("a", "b").unwrap(); }
    "#,
);

#[test]
fn a_frozen_row_covers_exactly_its_count_of_a_file_s_uses_of_a_gate() {
    let policy = listed_policy(
        r#"frozen = [
            { path = "crates/app/src/staging.rs", gate = "temporary files", count = 2, finding = "stages copies" },
            { path = "crates/app/src/staging.rs", gate = "standard library file system", count = 2, finding = "reads and copies" },
        ]"#,
    );
    assert_eq!(listed_findings(&policy, &[STAGING]), []);
}

#[test]
fn a_frozen_row_over_the_code_says_to_lower_or_remove_it() {
    let policy = listed_policy(
        r#"frozen = [
            { path = "crates/app/src/staging.rs", gate = "temporary files", count = 3, finding = "stages copies" },
            { path = "crates/app/src/staging.rs", gate = "standard library file system", count = 2, finding = "reads and copies" },
            { path = "crates/app/src/gone.rs", gate = "temporary files", count = 1, finding = "stages copies" },
        ]"#,
    );
    let found = listed_findings(&policy, &[STAGING, ("crates/app/src/gone.rs", "fn kept() {}")]);
    assert_eq!(
        found.iter().map(|(_, _, message)| message.as_str()).collect::<Vec<_>>(),
        [
            "capabilities.frozen: file system: crates/app/src/gone.rs (temporary files) lists 1 findings but the code has none: remove the row",
            "capabilities.frozen: file system: crates/app/src/staging.rs (temporary files) lists 3 findings but the code has 2: lower the count to 2",
        ]
    );
    assert!(found.iter().all(|(path, line, _)| path == crate::policy::FILE_NAME && *line == 1));
}

#[test]
fn a_frozen_row_under_the_code_prints_every_use_and_the_count() {
    let policy = listed_policy(
        r#"frozen = [
            { path = "crates/app/src/staging.rs", gate = "temporary files", count = 1, finding = "stages copies" },
            { path = "crates/app/src/staging.rs", gate = "standard library file system", count = 2, finding = "reads and copies" },
        ]"#,
    );
    let message = "temporary files (file system) is used directly only in crates/app-files/src/";
    assert_eq!(
        listed_findings(&policy, &[STAGING]),
        [
            (
                STAGING.0.to_string(),
                5,
                "crates/app/src/staging.rs (temporary files) has 2 findings, and its frozen row lists 1"
                    .to_string()
            ),
            (STAGING.0.to_string(), 5, message.to_string()),
            (STAGING.0.to_string(), 6, message.to_string()),
        ]
    );
}

#[test]
fn an_allowed_row_covers_its_item_s_uses_and_the_rest_need_a_frozen_row() {
    let policy = listed_policy(
        r#"allowed = [
            { path = "crates/app/src/staging.rs", item = "Staged::length", gate = "standard library file system", count = 1, reason = "the length of a file the caller opened" },
        ]
        frozen = [
            { path = "crates/app/src/staging.rs", gate = "temporary files", count = 2, finding = "stages copies" },
        ]"#,
    );
    assert_eq!(
        listed_findings(&policy, &[STAGING]),
        [(
            STAGING.0.to_string(),
            10,
            "standard library file system (file system) is used directly only in crates/app-files/src/".to_string()
        )]
    );
}

#[test]
fn an_allowed_row_names_its_item_and_count_exactly() {
    let policy = listed_policy(
        r#"allowed = [
            { path = "crates/app/src/staging.rs", item = "Staged::length", gate = "standard library file system", count = 2, reason = "the length of an open file" },
            { path = "crates/app/src/staging.rs", item = "Staged::stage", gate = "standard library file system", count = 1, reason = "none here" },
        ]
        frozen = [
            { path = "crates/app/src/staging.rs", gate = "temporary files", count = 2, finding = "stages copies" },
            { path = "crates/app/src/staging.rs", gate = "standard library file system", count = 1, finding = "copies" },
        ]"#,
    );
    assert_eq!(
        listed_findings(&policy, &[STAGING]).into_iter().map(|(_, _, message)| message).collect::<Vec<_>>(),
        [
            "capabilities.allowed: file system: crates/app/src/staging.rs: Staged::length (standard library file system) lists 2 findings but the code has 1: lower the count to 1",
            "capabilities.allowed: file system: crates/app/src/staging.rs: Staged::stage (standard library file system) lists 1 findings but the code has none: remove the row",
        ]
    );
}

/// The `CALLS` policy's capability reading test code, with these test homes.
fn tests_policy(test_homes: &str) -> Policy {
    listed_policy(&format!("tests = true\ntest_homes = [{test_homes}]"))
}

#[test]
fn a_capability_that_reads_tests_finds_test_code_outside_its_homes() {
    let leak = (
        "crates/app/src/leak.rs",
        r#"
        use std::fs;
        fn production() {}
        #[cfg(test)]
        mod tests {
            use super::*;
            fn inline() { fs::read("a").unwrap(); }
        }
        #[cfg(test)]
        #[path = "leak_tests.rs"]
        mod more;
        "#,
    );
    let tests = (
        "crates/app/src/leak_tests.rs",
        "use super::*;\nfn beside() { fs::write(\"a\", \"b\").unwrap(); }",
    );
    let found =
        listed_findings(&tests_policy(""), &[("crates/app/src/lib.rs", "mod leak;"), leak, tests]);
    let message =
        "standard library file system (file system) is used directly only in crates/app-files/src/";
    assert_eq!(
        found,
        [
            ("crates/app/src/leak.rs".to_string(), 7, message.to_string()),
            ("crates/app/src/leak_tests.rs".to_string(), 2, message.to_string()),
        ]
    );
    let without_tests =
        listed_findings(&listed_policy(""), &[("crates/app/src/lib.rs", "mod leak;"), leak, tests]);
    assert_eq!(without_tests, []);
}

#[test]
fn test_code_may_use_a_capability_in_its_homes_their_tests_and_its_test_homes() {
    let policy = Policy::fixture(&CALLS.replace(
        r#"homes = ["crates/app-files/src/"]"#,
        r#"homes = ["crates/app-files/src/", "crates/app/src/staging.rs"]
        tests = true
        test_homes = ["crates/app/src/fixtures/", "crates/app/src/seeds.rs"]"#,
    ));
    let call = "#[cfg(test)] fn t() { let _ = tempfile::tempdir(); }";
    let whole = "fn t() { let _ = tempfile::tempdir(); }";
    let files = [
        ("crates/app-files/src/stage_tests.rs", whole),
        ("crates/app/src/staging.rs", call),
        ("crates/app/src/staging_tests.rs", whole),
        ("crates/app/src/staging_tests/more.rs", &format!("#![cfg(test)]\n{whole}")),
        ("crates/app/src/fixtures/folders.rs", call),
        ("crates/app/src/seeds.rs", call),
        ("crates/app/src/other_tests.rs", whole),
    ];
    assert_eq!(
        listed_findings(&policy, &files),
        [(
            "crates/app/src/other_tests.rs".to_string(),
            1,
            "temporary files (file system) is used directly only in crates/app-files/src/, crates/app/src/staging.rs".to_string()
        )]
    );
}

#[test]
fn test_code_starts_background_work_anywhere() {
    let (capability, gates) = CALLS.split_once("[[capabilities.gates]]").expect("a gate");
    let policy = Policy::fixture(&format!(
        "{capability}tests = true\n[[capabilities.gates]]{gates}\n[[background_starts]]\nkind = \"thread or future spawn\"\npaths = [\"tokio::spawn\"]"
    ));
    let test = "#[cfg(test)] fn t() { tokio::spawn(async {}); }";
    assert_eq!(listed_findings(&policy, &[("crates/app/src/leak.rs", test)]), []);
    let production = "fn t() { tokio::spawn(async {}); }";
    assert_eq!(listed_findings(&policy, &[("crates/app/src/leak.rs", production)]).len(), 1);
}

/// A call the resolver finds two definitions for, or only through imports
/// that name each other, may use the capability: it is a finding wherever a
/// gate matches calls, and only there.
#[test]
fn a_call_that_resolves_ambiguously_is_a_finding_where_a_gate_matches_calls() {
    let source = r#"
        mod a { pub fn read() {} }
        mod b { pub fn read() {} }
        use a::*;
        use b::*;
        fn load() { read(); }
    "#;
    let found =
        rule(&[RustFile::fixture("crates/app/src/leak.rs", source)], &Policy::fixture(CALLS));
    assert_eq!(
        found.iter().map(|finding| (finding.line, finding.message.as_str())).collect::<Vec<_>>(),
        [(6, "cannot resolve call read: ambiguous paths: app::leak::a::read, app::leak::b::read")]
    );
    assert!(violations("crates/app/src/leak.rs", source).is_empty());
}

/// The line and message of each finding the `CALLS` policy makes in a crate
/// root holding `source`, whose crate depends on `lofty` and `tempfile`; a
/// gate's finding by its kind.
fn root_findings(source: &str) -> Vec<(usize, String)> {
    let mut file = RustFile::fixture("crates/app/src/lib.rs", source);
    for name in ["lofty", "tempfile"] {
        file.extern_crates.insert(name.into(), name.into());
    }
    rule(&[file], &Policy::fixture(CALLS))
        .iter()
        .map(|finding| (finding.line, kind(finding).to_string()))
        .collect()
}

/// A glob of an outside crate's module brings in the names that crate
/// exports, which the checker doesn't read: none that the workspace's
/// globs, the scopes around it or the dependencies name, and its own path
/// names the crate. A name nothing else names is that crate's, the nearest
/// such glob's.
#[test]
fn an_outside_crate_s_glob_names_only_what_nothing_else_names() {
    let source = r#"
        mod tags { pub fn read_tags() {} }
        mod tests {
            use super::tags::*;
            use lofty::prelude::*;
            use std::path::Path;
            fn load(path: &Path) {
                read_tags();
                let _ = Path::new("a");
                lofty::read_from_path(path);
                std::fs::read(path);
            }
            fn stage() { use tempfile::*; let _ = tempdir(); read_tags(); }
        }
    "#;
    assert_eq!(
        root_findings(source),
        [(11, "standard library file system".to_string()), (13, "temporary files".to_string())]
    );
}

/// `use name;` imports what `name` names without the import, there: a
/// module re-exports its `macro_rules!` so, which names no value, and a
/// local of the same name stays a local.
#[test]
fn a_macro_s_re_export_names_no_value() {
    let source = r#"
        mod members {
            use super::*;
            macro_rules! self_is_admin { () => { "SELECT 1" } }
            pub(crate) use self_is_admin;
        }
        pub(crate) use members::self_is_admin;
        fn removable(self_is_admin: bool, is_self: bool) -> bool { self_is_admin && !is_self }
    "#;
    assert_eq!(root_findings(source), []);
}

/// `use Name;` in a module whose glob brings `Name` in names what the glob
/// brings.
#[test]
fn an_import_of_a_name_a_glob_brings_names_the_glob_s_item() {
    let source = r#"
        mod fill { pub enum Fill { Shuffled } pub fn read() {} }
        use fill::*;
        mod bench {
            use super::*;
            use Fill;
            use read;
            fn run() { take(Fill::Shuffled); read(); std::fs::read("a"); }
        }
    "#;
    assert_eq!(root_findings(source), [(8, "standard library file system".to_string())]);
}

/// A glob brings in only the names visible where it is: a module's private
/// imports, its globs among them, stay its own, and so do its private items.
#[test]
fn a_glob_brings_only_the_names_visible_where_it_is() {
    let source = r#"
        mod queue { pub(super) fn admit() {} }
        mod identification {
            use super::queue::*;
            fn settle() {}
            pub(crate) mod tests { use super::*; pub(crate) fn seed() {} }
        }
        mod handle {
            pub(crate) mod tests { pub(crate) fn admit() {} pub(crate) fn settle() {} }
            mod import {
                mod tests {
                    use crate::handle::tests::*;
                    use crate::identification::*;
                    use crate::identification::tests::*;
                    fn run() { admit(); settle(); seed(); }
                }
            }
        }
    "#;
    assert_eq!(root_findings(source), []);
}
