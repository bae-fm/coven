use super::*;
use crate::crate_code::CrateCode;
use crate::sources::Manifest;

fn workspace(files: Vec<RustFile>, members: &[(&str, &str)]) -> Workspace {
    Workspace {
        files,
        manifests: members
            .iter()
            .map(|(directory, package)| Manifest {
                relative_path: format!("{directory}/Cargo.toml"),
                table: format!("[package]\nname = {package:?}").parse().expect("fixture manifest"),
            })
            .collect(),
        workspace_dependencies: toml::Table::new(),
    }
}

fn stale(files: Vec<RustFile>, rows: &str) -> Vec<String> {
    let members = [("crates/app-foundation", "app-foundation"), ("crates/app", "app")];
    let workspace = workspace(files, &members);
    let policy = Policy::fixture(&format!("rules = [\"conventions\"]\n{rows}"));
    stale_rows(&workspace, &CrateCode::read(&workspace, &policy), &policy)
}

#[test]
fn a_policy_naming_only_what_exists_is_current() {
    let files = vec![
        RustFile::fixture(
            "crates/app-foundation/src/clock.rs",
            "pub trait Clock {} pub struct SystemClock; impl Clock for SystemClock {}",
        ),
        RustFile::fixture(
            "crates/app/src/builder.rs",
            "struct Builder; impl Builder { fn open() {} } fn restore() {}",
        ),
    ];
    let rows = r#"
        crate_order = ["app-foundation", "app"]
        capability_types = ["Clock", "SystemClock"]
        capability_traits = ["Clock"]
        [composition_roots]
        "crates/app/src/builder.rs" = ["Builder::open", "restore"]
        [[capabilities]]
        name = "current time"
        homes = ["crates/app-foundation/src/clock.rs", "crates/app-foundation/src/"]
    "#;
    assert_eq!(stale(files, rows), Vec::<String>::new());
}

#[test]
fn a_policy_must_select_rules() {
    let (workspace, policy) = (workspace(Vec::new(), &[]), Policy::fixture(""));
    let rows = stale_rows(&workspace, &CrateCode::read(&workspace, &policy), &policy);
    assert_eq!(rows, ["rules: the policy selects no rules"]);
}

#[test]
fn members_named_by_the_policy_exist_and_crates_are_not_tools() {
    let rows = r#"
        tools = ["app-foundation", "checker"]
        api_crate = "app-foundation"
        crate_order = ["app", "app-foundation", "app-missing"]
        separated_crates = [["app", "app-other"]]
    "#;
    assert_eq!(
        stale(Vec::new(), rows),
        [
            "tools: checker is not a workspace member",
            "api_crate: app-foundation is not a crate of the workspace",
            "crate_order: app-foundation is not a crate of the workspace",
            "crate_order: app-missing is not a crate of the workspace",
            "separated_crates: app-other has no row in crate_order",
        ]
    );
}

/// A home that moved would cover nothing, so it must hold a source.
#[test]
fn library_names_name_a_dependency() {
    let mut workspace = workspace(Vec::new(), &[("crates/app", "app")]);
    workspace.manifests[0].table =
        "[package]\nname = \"app\"\n[dependencies]\ncoreaudio-rs.workspace = true"
            .parse()
            .expect("fixture manifest");
    let policy = Policy::fixture(
        r#"rules = ["conventions"]
        library_names = { "coreaudio-rs" = "coreaudio", "objc2-rs" = "objc2" }"#,
    );
    assert_eq!(
        stale_rows(&workspace, &CrateCode::read(&workspace, &policy), &policy),
        ["library_names: no member depends on objc2-rs"]
    );
}

#[test]
fn every_capability_home_holds_a_source() {
    let files = vec![RustFile::fixture("crates/app-foundation/src/files/atomic.rs", "")];
    let rows = r#"
        [[capabilities]]
        name = "files on disk"
        homes = [
            "crates/app-foundation/src/files/",
            "crates/app-foundation/src/clock.rs",
            "crates/app/src/",
        ]
    "#;
    assert_eq!(
        stale(files, rows),
        [
            "capabilities: files on disk has home crates/app-foundation/src/clock.rs, which holds no source",
            "capabilities: files on disk has home crates/app/src/, which holds no source",
        ]
    );
}

#[test]
fn composition_roots_resolve_methods_trait_methods_free_functions_and_statics_outside_tests() {
    let path = "crates/app/src/bootstrap.rs";
    let files = vec![
        RustFile::fixture(
            path,
            "struct Builder; impl Builder { fn open() {} } fn restore() {} #[cfg(test)] fn fixture() {} trait Connector { fn connect(&self) -> Builder; } static REGISTRY: u8 = 0; #[cfg(test)] static FIXTURE: u8 = 0;",
        ),
        RustFile::fixture("crates/app/src/bootstrap_tests.rs", "fn helper() {}"),
    ];
    let rows = format!(
        r#"
        [composition_roots]
        "{path}" = ["Builder::open", "restore", "fixture", "Builder::close", "Other::open", "Connector::connect", "REGISTRY", "FIXTURE"]
        "crates/app/src/bootstrap_tests.rs" = ["helper"]
        "#
    );
    assert_eq!(
        stale(files, &rows),
        [
            "composition_roots: crates/app/src/bootstrap.rs: <free>::fixture names no function or static there",
            "composition_roots: crates/app/src/bootstrap.rs: Builder::close names no function or static there",
            "composition_roots: crates/app/src/bootstrap.rs: Other::open names no function or static there",
            "composition_roots: crates/app/src/bootstrap.rs: <free>::FIXTURE names no function or static there",
            "composition_roots: crates/app/src/bootstrap_tests.rs: <free>::helper names no function or static there",
            "composition_roots: no crate declares Other",
        ]
    );
}

#[test]
fn capability_factories_name_a_receiver_and_its_capability_product() {
    let factories = r#"
        capability_types = ["StoreDir"]
        construction_only_capability_types = ["AtomicFile"]
        [capability_factories."crates/app-foundation/src/directory.rs"]
        "StoreDir::file" = "AtomicFile"
    "#;
    let types = "struct StoreDir; struct AtomicFile; struct Other;";
    for (path, declaration, valid) in [
        (
            "crates/app-foundation/src/directory.rs",
            "impl StoreDir { fn file(&self) -> AtomicFile {} }",
            true,
        ),
        (
            "crates/app-foundation/src/renamed.rs",
            "impl StoreDir { fn file(&self) -> AtomicFile {} }",
            false,
        ),
        (
            "crates/app-foundation/src/directory.rs",
            "impl Other { fn file(&self) -> AtomicFile {} }",
            false,
        ),
        (
            "crates/app-foundation/src/directory.rs",
            "impl StoreDir { fn renamed(&self) -> AtomicFile {} }",
            false,
        ),
        (
            "crates/app-foundation/src/directory.rs",
            "impl StoreDir { fn file() -> AtomicFile {} }",
            false,
        ),
        (
            "crates/app-foundation/src/directory.rs",
            "impl StoreDir { fn file(&self) -> Other {} }",
            false,
        ),
        (
            "crates/app-foundation/src/directory.rs",
            "impl StoreDir { #[cfg(test)] fn file(&self) -> AtomicFile {} }",
            false,
        ),
    ] {
        let rows =
            stale(vec![RustFile::fixture(path, &format!("{types} {declaration}"))], factories);
        let expected: &[&str] = if valid {
            &[]
        } else {
            &["capability_factories: crates/app-foundation/src/directory.rs: StoreDir::file -> AtomicFile is not a receiver method of a capability type returning that construction-only capability"]
        };
        assert_eq!(rows, expected, "{path}: {declaration}");
    }
}

#[test]
fn every_type_the_policy_names_is_declared_by_a_crate() {
    let members = [("crates/app", "app"), ("tools/checker", "checker")];
    let files = vec![
        RustFile::fixture("crates/app/src/lib.rs", "struct Clock; #[cfg(test)] struct Fixture;"),
        RustFile::fixture("crates/app/src/lib_tests.rs", "struct Tested;"),
        RustFile::fixture("tools/checker/src/main.rs", "struct Tool;"),
    ];
    let policy = Policy::fixture(
        r#"
        rules = ["conventions"]
        tools = ["checker"]
        capability_types = ["Clock", "Fixture"]
        task_types = ["Tested", "Tool"]
        "#,
    );
    assert_eq!(
        {
            let workspace = workspace(files, &members);
            stale_rows(&workspace, &CrateCode::read(&workspace, &policy), &policy)
        },
        [
            "capability_types: no crate declares Fixture",
            "task_types: no crate declares Tested",
            "task_types: no crate declares Tool",
        ]
    );
}

#[test]
fn phantom_type_arguments_name_a_type() {
    let files =
        vec![RustFile::fixture("crates/app/src/server.rs", "fn mount(router: Router<()>) {}")];
    assert_eq!(
        stale(
            files.clone(),
            r#"phantom_type_arguments = ["Router", "Missing"]
            external_types = ["Router"]"#
        ),
        ["phantom_type_arguments: no crate declares Missing"]
    );
}

#[test]
fn capability_traits_are_declared_traits() {
    let rows = r#"
        capability_types = ["Clock", "IdSource"]
        capability_traits = ["Clock", "IdSource", "Missing"]
    "#;
    let files = vec![RustFile::fixture(
        "crates/app-foundation/src/lib.rs",
        "mod clock { trait Clock {} } struct IdSource;",
    )];
    assert_eq!(
        stale(files, rows),
        [
            "capability_traits: no crate declares Missing",
            "capability_traits: no crate declares trait IdSource",
            "capability_traits: no crate declares trait Missing",
        ]
    );
}

/// The table macros the policy names are exactly the schema module's table
/// lists, each declared once.
#[test]
fn the_database_schema_module_declares_exactly_its_table_macros() {
    let schema = RustFile::fixture(
        "crates/app-foundation/src/schema.rs",
        r#"
        macro_rules! app_tables { ($visit:ident) => { $visit!(_app_writes, "key TEXT"); }; }
        macro_rules! unlisted_tables { ($visit:ident) => { $visit!(_app_other, "key TEXT"); }; }
        macro_rules! helper { () => {}; }
        "#,
    );
    let rows = r#"
        [database]
        capability = "SQLite"
        schema = "crates/app-foundation/src/schema.rs"
        table_macros = ["app_tables", "missing_tables"]
        rusqlite_reexport = "app-missing"
        [[capabilities]]
        name = "SQLite"
        homes = ["crates/app-foundation/src/"]
    "#;
    assert_eq!(
        stale(vec![schema], rows),
        [
            "database: rusqlite_reexport app-missing is not a workspace member",
            "database: the schema module under crates/app-foundation/src/schema.rs declares macro_rules! missing_tables 0 times, not once",
            "database: schema table macro unlisted_tables is missing from table_macros",
        ]
    );
    let rows = r#"
        [database]
        capability = "Missing"
        schema = "crates/app-foundation/src/moved.rs"
    "#;
    assert_eq!(
        stale(Vec::new(), rows),
        [
            "database: capability Missing is not in capabilities",
            "database: schema module root crates/app-foundation/src/moved.rs does not exist",
        ]
    );
}

/// Domains place each schema table once, in homes that hold a source.
#[test]
fn domains_place_each_schema_table_once() {
    let schema = RustFile::fixture(
        "crates/app-foundation/src/schema.rs",
        r#"macro_rules! app_tables {
            ($visit:ident) => { $visit!(_app_writes, "key TEXT"); $visit!(_app_rows, "key TEXT"); };
        }"#,
    );
    let rows = r#"
        [database]
        capability = "SQLite"
        schema = "crates/app-foundation/src/schema.rs"
        table_macros = ["app_tables"]
        [[database.domains]]
        name = "writes"
        homes = ["crates/app-foundation/src/schema.rs", "crates/app-foundation/src/moved/"]
        tables = ["_app_writes", "_app_gone"]
        [[database.domains]]
        name = "writes"
        homes = ["crates/app-foundation/src/"]
        tables = ["_APP_WRITES"]
        [[capabilities]]
        name = "SQLite"
        homes = ["crates/app-foundation/src/"]
    "#;
    assert_eq!(
        stale(vec![schema], rows),
        [
            "database: domain writes has home crates/app-foundation/src/moved/, which holds no source",
            "database: domain writes is named twice",
            "database: table _APP_WRITES is in domains writes and writes",
            "database: schema table _app_rows is in no domain",
            "database: domain table _app_gone is not a schema table",
        ]
    );
}

#[test]
fn external_types_are_named_by_a_crate_and_declared_by_none() {
    let files = vec![
        RustFile::fixture(
            "crates/app/src/lib.rs",
            "struct Worker { runtime: tokio::runtime::Runtime, lock: Mutex<()> } struct Local;",
        ),
        RustFile::fixture("crates/app/src/lib_tests.rs", "struct Fixture { handle: Handle }"),
    ];
    let rows = r#"
        capability_types = ["Runtime", "Mutex", "Local", "Handle", "Gone"]
        external_types = ["Runtime", "Mutex", "Local", "Handle", "Gone", "Unused"]
    "#;
    assert_eq!(
        stale(files, rows),
        [
            "external_types: no crate names Gone",
            "external_types: no crate names Handle",
            "external_types: a crate declares Local",
            "external_types: no crate names Local",
            "external_types: no crate names Unused",
            "external_types: no type row names Unused",
        ]
    );
}

/// A handle's owner is another declared type with a method returning it, and
/// the handle is no capability or shared state row besides; nor is shared
/// state a capability row.
#[test]
fn a_handle_names_an_owner_that_returns_it() {
    let files = vec![RustFile::fixture(
        "crates/app/src/query.rs",
        "struct Requests; struct Query; struct Other; struct Watch; struct Lock; \
        impl Query { fn requests(&self) -> Option<Requests> { todo!() } fn watch(&self) -> Watch { todo!() } } \
        impl Other { fn other(&self) {} }",
    )];
    let rows = r#"
        capability_types = ["Requests", "Lock"]
        shared_state_types = ["Watch", "Lock"]
        handles = { Requests = "Query", Other = "Other", Query = "Other", Missing = "Query", Watch = "Query" }
    "#;
    assert_eq!(
        stale(files, rows),
        [
            "handles: Query has no method returning Missing",
            "handles: Other names itself as its owner",
            "handles: Other has no method returning Query",
            "handles: Requests is also in capability_types",
            "handles: Watch is also in shared_state_types",
            "shared_state_types: Lock is also in capability_types",
            "handles: no crate declares Missing",
        ]
    );
}
