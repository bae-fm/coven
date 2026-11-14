use super::*;
use crate::crate_code::CrateCode;
use crate::sources::{Manifest, SqlFile};

fn workspace(files: Vec<RustFile>, members: &[(&str, &str)]) -> Workspace {
    Workspace {
        files,
        sql_files: Vec::new(),
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
            "struct Builder; impl Builder { fn open() {} } fn restore() {} #[cfg(test)] fn fixture() {} trait Connector { fn connect(&self) -> Builder; } static REGISTRY: u8 = 0; #[cfg(test)] static FIXTURE: u8 = 0; thread_local! { static CURRENT: u8 = 0; }",
        ),
        RustFile::fixture("crates/app/src/bootstrap_tests.rs", "fn helper() {}"),
    ];
    let rows = format!(
        r#"
        [composition_roots]
        "{path}" = ["Builder::open", "restore", "fixture", "Builder::close", "Other::open", "Connector::connect", "REGISTRY", "FIXTURE", "CURRENT"]
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

/// Domains place each schema table once, in homes that hold a source, and
/// each names an owner the checker infers.
#[test]
fn domains_place_each_schema_table_once() {
    let schema = RustFile::fixture(
        "crates/app-foundation/src/schema.rs",
        r#"macro_rules! app_tables {
            ($visit:ident) => { $visit!(_app_writes, "key TEXT"); $visit!(_app_rows, "key TEXT"); };
        }
        struct Store { connection: Connection }
        struct Rows;"#,
    );
    let rows = r#"
        capability_types = ["Connection"]
        external_types = ["Connection"]
        [database]
        capability = "SQLite"
        schema = "crates/app-foundation/src/schema.rs"
        table_macros = ["app_tables"]
        [[database.domains]]
        name = "writes"
        homes = ["crates/app-foundation/src/schema.rs", "crates/app-foundation/src/moved/"]
        tables = ["_app_writes", "_app_gone"]
        owner = "Store"
        [[database.domains]]
        name = "writes"
        homes = ["crates/app-foundation/src/"]
        tables = ["_APP_WRITES"]
        owner = "Rows"
        [[capabilities]]
        name = "SQLite"
        homes = ["crates/app-foundation/src/"]
    "#;
    assert_eq!(
        stale(vec![schema], rows),
        [
            "database: domain writes has home crates/app-foundation/src/moved/, which holds no source",
            "database: domain writes is named twice",
            "database: domain writes names owner Rows, which holds no capability, shared state or owner",
            "database: table _APP_WRITES is in domains writes and writes",
            "database: schema table _app_rows is in no domain",
            "database: domain table _app_gone is not a schema table",
        ]
    );
}

/// Without a schema module, the schema's tables are what the crates' SQL
/// files create: a domain may have a SQL file as its home, and temporary
/// tables and tests' SQL make no table of the schema.
#[test]
fn sql_files_create_the_schemas_tables() {
    let mut workspace = workspace(
        vec![RustFile::fixture(
            "crates/app-foundation/src/listing.rs",
            "struct Listing { connection: Connection }",
        )],
        &[("crates/app-foundation", "app-foundation")],
    );
    workspace.sql_files = [
        (
            "crates/app-foundation/src/listing.sql",
            "CREATE TABLE IF NOT EXISTS listing (id TEXT);
             CREATE TEMP TABLE staged (id TEXT);
             CREATE VIRTUAL TABLE listing_search USING fts5(id);",
        ),
        ("crates/app-foundation/tests/fixture.sql", "CREATE TABLE fixture (id TEXT);"),
    ]
    .map(|(path, text)| SqlFile {
        relative_path: path.into(),
        package_root: "crates/app-foundation/".into(),
        package: "app-foundation".into(),
        text: text.into(),
    })
    .into();
    let policy = Policy::fixture(
        r#"rules = ["database-boundary"]
        capability_types = ["Connection"]
        external_types = ["Connection"]
        [database]
        capability = "SQLite"
        [[database.domains]]
        name = "listing"
        homes = ["crates/app-foundation/src/listing.sql"]
        tables = ["listing", "gone"]
        owner = "Listing"
        [[capabilities]]
        name = "SQLite"
        homes = ["crates/app-foundation/src/"]
        "#,
    );
    assert_eq!(
        stale_rows(&workspace, &CrateCode::read(&workspace, &policy), &policy),
        [
            "database: schema table listing_search is in no domain",
            "database: domain table gone is not a schema table",
        ]
    );
}

/// A domain's views are views the crates' production SQL creates, each in
/// one domain.
#[test]
fn domains_name_views_the_sql_creates() {
    let mut workspace = workspace(
        vec![RustFile::fixture(
            "crates/app-foundation/src/listing.rs",
            r#"struct Listing { connection: Connection }
            const VIEW: &str = "CREATE VIEW listing_moves AS SELECT id FROM listing";"#,
        )],
        &[("crates/app-foundation", "app-foundation")],
    );
    workspace.sql_files = [
        ("crates/app-foundation/src/listing.sql", "CREATE TABLE listing (id TEXT);"),
        ("crates/app-foundation/tests/fixture.sql", "CREATE VIEW fixture AS SELECT 1;"),
    ]
    .map(|(path, text)| SqlFile {
        relative_path: path.into(),
        package_root: "crates/app-foundation/".into(),
        package: "app-foundation".into(),
        text: text.into(),
    })
    .into();
    let policy = Policy::fixture(
        r#"rules = ["database-boundary"]
        capability_types = ["Connection"]
        external_types = ["Connection"]
        [database]
        capability = "SQLite"
        [[database.domains]]
        name = "listing"
        homes = ["crates/app-foundation/src/"]
        tables = ["listing"]
        views = ["listing_moves", "listing", "fixture"]
        owner = "Listing"
        [[database.domains]]
        name = "moves"
        homes = ["crates/app-foundation/src/"]
        tables = []
        views = ["Listing_Moves"]
        owner = "Listing"
        [[capabilities]]
        name = "SQLite"
        homes = ["crates/app-foundation/src/"]
        "#,
    );
    assert_eq!(
        stale_rows(&workspace, &CrateCode::read(&workspace, &policy), &policy),
        [
            "database: domain listing names view listing, which is a schema table",
            "database: domain listing names view fixture, which the SQL never creates",
            "database: view Listing_Moves is in domains listing and moves",
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

/// Private state is held one way: no capability, shared state or handle row
/// names it too.
#[test]
fn private_state_is_no_other_row() {
    let files = vec![RustFile::fixture(
        "crates/app/src/lib.rs",
        "struct Worker { lock: Mutex<()>, ready: Condvar, gate: Gate, shared: Arc<()> } \
        struct Gate; struct Query; impl Query { fn gate(&self) -> Gate { todo!() } }",
    )];
    let rows = r#"
        capability_types = ["Mutex"]
        shared_state_types = ["Condvar"]
        private_state_types = ["Mutex", "Condvar", "Gate"]
        shared_pointer_types = ["Arc"]
        handles = { Gate = "Query" }
        external_types = ["Mutex", "Condvar", "Arc"]
    "#;
    assert_eq!(
        stale(files, rows),
        [
            "private_state_types: Condvar is also in shared_state_types",
            "private_state_types: Gate is also a handle",
            "private_state_types: Mutex is also in capability_types",
        ]
    );
}

fn read_shapes_rows(rows: &str) -> String {
    format!(
        r#"
        [read_shapes]
        read_types = ["SqlReadContext"]
        rule = "the rule"
        {rows}
        "#
    )
}

fn read_shape_files() -> Vec<RustFile> {
    vec![
        RustFile::fixture(
            "crates/app/src/reads.rs",
            "pub struct SqlReadContext; struct Screen; impl Screen { fn read(&self) {} } fn free() {}",
        ),
        RustFile::fixture("crates/app/src/plumbing.rs", ""),
    ]
}

#[test]
fn the_read_shapes_rows_naming_what_exists_are_current() {
    let rows = read_shapes_rows(
        r#"plumbing = ["crates/app/src/plumbing.rs"]
        frozen = [{ path = "crates/app/src/reads.rs", item = "Screen::read", shape = "sort", count = 1, finding = "sorts" }]
        allowed = [{ path = "crates/app/src/reads.rs", item = "free", shape = "pick", count = 2, reason = "a constant table" }]"#,
    );
    assert_eq!(stale(read_shape_files(), &rows), Vec::<String>::new());
}

#[test]
fn a_read_type_is_declared_by_a_crate_or_external() {
    let rows = read_shapes_rows("").replace("SqlReadContext", "NoSuchType");
    assert_eq!(
        stale(read_shape_files(), &rows),
        ["read_shapes.read_types: no crate declares NoSuchType"]
    );
}

#[test]
fn plumbing_names_a_file_or_directory_holding_source() {
    let rows = read_shapes_rows(
        r#"plumbing = ["crates/app/src/plumbing.rs", "crates/app/src/moved/", "crates/app/src/gone.rs"]"#,
    );
    assert_eq!(
        stale(read_shape_files(), &rows),
        [
            "read_shapes.plumbing: crates/app/src/moved/ holds no source",
            "read_shapes.plumbing: crates/app/src/gone.rs holds no source",
        ]
    );
}

#[test]
fn a_list_row_names_a_file_and_an_item_there() {
    let rows = read_shapes_rows(
        r#"frozen = [
            { path = "crates/app/src/moved.rs", item = "free", shape = "sort", count = 1, finding = "x" },
            { path = "crates/app/src/reads.rs", item = "Screen::gone", shape = "sort", count = 1, finding = "x" },
            { path = "crates/app/src/reads.rs", item = "gone", shape = "sort", count = 1, finding = "x" },
            { path = "crates/app/src/reads.rs", item = "not an item", shape = "sort", count = 1, finding = "x" },
        ]"#,
    );
    assert_eq!(
        stale(read_shape_files(), &rows),
        [
            "read_shapes.frozen: crates/app/src/moved.rs holds no source",
            "read_shapes.frozen: crates/app/src/reads.rs: Screen::gone names no function there",
            "read_shapes.frozen: crates/app/src/reads.rs: gone names no function there",
            "read_shapes.frozen: crates/app/src/reads.rs: not an item is neither `Type::method` nor a function name",
        ]
    );
}

#[test]
fn a_list_row_has_a_count_of_at_least_one_and_a_note() {
    let rows = read_shapes_rows(
        r#"frozen = [{ path = "crates/app/src/reads.rs", item = "free", shape = "sort", count = 0, finding = "" }]
        allowed = [{ path = "crates/app/src/reads.rs", item = "free", shape = "pick", count = 1, reason = "" }]"#,
    );
    assert_eq!(
        stale(read_shape_files(), &rows),
        [
            "read_shapes.frozen: crates/app/src/reads.rs: free (sort) has a count of 0, not at least 1",
            "read_shapes.frozen: crates/app/src/reads.rs: free (sort) states no finding",
            "read_shapes.allowed: crates/app/src/reads.rs: free (pick) states no reason",
        ]
    );
}

#[test]
fn a_key_is_in_at_most_one_list_and_once() {
    let rows = read_shapes_rows(
        r#"frozen = [
            { path = "crates/app/src/reads.rs", item = "free", shape = "sort", count = 1, finding = "x" },
            { path = "crates/app/src/reads.rs", item = "free", shape = "sort", count = 2, finding = "x" },
        ]
        allowed = [{ path = "crates/app/src/reads.rs", item = "free", shape = "sort", count = 1, reason = "x" }]"#,
    );
    assert_eq!(
        stale(read_shape_files(), &rows),
        [
            "read_shapes.frozen: crates/app/src/reads.rs: free (sort) is listed twice",
            "read_shapes.allowed: crates/app/src/reads.rs: free (sort) is also in frozen",
        ]
    );
}

#[test]
fn the_live_query_value_stream_and_kept_state_types_are_declared_by_a_crate_or_external() {
    let rows = r#"
        external_types = ["Mutex"]
        [read_shapes]
        read_types = ["SqlReadContext"]
        live_query_types = ["LiveQuery"]
        value_stream_types = ["Watch"]
        kept_state_types = ["Mutex", "Cell"]
        rule = "the rule"
    "#;
    let files = vec![RustFile::fixture(
        "crates/app/src/reads.rs",
        "pub struct SqlReadContext; pub struct LiveQuery; struct Holds(Mutex<u8>);",
    )];
    assert_eq!(
        stale(files, rows),
        [
            "read_shapes.value_stream_types: no crate declares Watch",
            "read_shapes.kept_state_types: no crate declares Cell",
        ]
    );
}

#[test]
fn a_kept_state_row_names_a_type_or_a_function_and_a_counter_row_a_sql_column() {
    let rows = read_shapes_rows(
        r#"frozen = [
            { path = "crates/app/src/reads.rs", item = "Screen", shape = "kept-state", count = 1, finding = "x" },
            { path = "crates/app/src/reads.rs", item = "Screen::read", shape = "kept-state", count = 1, finding = "x" },
            { path = "crates/app/src/reads.rs", item = "Gone", shape = "kept-state", count = 1, finding = "x" },
            { path = "crates/app/src/schema.sql", item = "v.version", shape = "counter", count = 1, finding = "x" },
            { path = "crates/app/src/schema.sql", item = "v.gone", shape = "counter", count = 1, finding = "x" },
        ]"#,
    );
    let members = [("crates/app-foundation", "app-foundation"), ("crates/app", "app")];
    let mut workspace = workspace(read_shape_files(), &members);
    workspace.sql_files = vec![SqlFile {
        relative_path: "crates/app/src/schema.sql".to_string(),
        package_root: "crates/app/".to_string(),
        package: "app".to_string(),
        text: "UPDATE v SET version = version + 1;".to_string(),
    }];
    let policy = Policy::fixture(&format!("rules = [\"conventions\"]\n{rows}"));
    assert_eq!(
        stale_rows(&workspace, &CrateCode::read(&workspace, &policy), &policy),
        [
            "read_shapes.frozen: crates/app/src/reads.rs: Gone names no type or function there",
            "read_shapes.frozen: crates/app/src/schema.sql: v.gone is no column the SQL there increases",
        ]
    );
}
