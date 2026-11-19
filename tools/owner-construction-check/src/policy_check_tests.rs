use super::*;
use crate::crate_code::CrateCode;
use crate::sources::{Manifest, SqlFile};

/// The rows of `policy` that name what `workspace` does not hold, read as
/// the checker reads them.
fn current_stale_rows(workspace: &Workspace, policy: Policy) -> Vec<String> {
    let rows = NamedTypes::of(&policy);
    let mut policy = policy;
    crate::type_rows::read_by_name(&mut policy);
    let code = CrateCode::read(workspace, &policy);
    let graph = OwnerGraph::collect(&code.expanded, &policy);
    stale_rows(workspace, &code, &graph, &policy, &rows)
}

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
    current_stale_rows(&workspace, policy)
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
    let rows = current_stale_rows(&workspace, policy);
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
        current_stale_rows(&workspace, policy),
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
            current_stale_rows(&workspace, policy)
        },
        [
            "capability_types: no crate declares Fixture",
            "task_types: no crate declares Tested",
            "task_types: no crate declares Tool",
        ]
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
        [[capabilities]]
        name = "SQLite"
        homes = ["crates/app-foundation/src/"]
    "#;
    assert_eq!(
        stale(vec![schema], rows),
        [
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

/// A domain's home holds source when it holds code the database rule reads:
/// production or test code, Rust or SQL. Test-support code, which only a
/// test-only module declaration reaches, is source too.
#[test]
fn a_domain_home_holding_only_test_code_holds_source() {
    let policy = Policy::fixture(
        r#"rules = ["database-boundary"]
        test_support_features = ["test-utils"]
        capability_types = ["Connection"]
        external_types = ["Connection"]
        [database]
        capability = "SQLite"
        [[database.domains]]
        name = "listing"
        homes = [
            "crates/app-foundation/src/listing.sql",
            "crates/app-foundation/src/test_fixtures.rs",
            "crates/app-foundation/src/test_fixtures/",
            "crates/app-foundation/src/listing_tests.rs",
            "crates/app-foundation/tests/",
            "crates/app-foundation/src/gone/",
        ]
        tables = ["listing"]
        owner = "Listing"
        [[capabilities]]
        name = "SQLite"
        homes = ["crates/app-foundation/src/"]
        "#,
    );
    let mut files = [
        (
            "crates/app-foundation/src/lib.rs",
            r#"struct Listing { connection: Connection }
            #[cfg(any(test, feature = "test-utils"))]
            pub mod test_fixtures;"#,
        ),
        ("crates/app-foundation/src/test_fixtures.rs", "pub mod rows;"),
        ("crates/app-foundation/src/test_fixtures/rows.rs", "pub fn seed() {}"),
        ("crates/app-foundation/src/listing_tests.rs", "fn listed() {}"),
    ]
    .map(|(path, source)| RustFile::fixture(path, source));
    crate::module_sources::mark_test_modules(&mut files, policy.test_support_features());
    let mut workspace = workspace(files.into(), &[("crates/app-foundation", "app-foundation")]);
    workspace.sql_files = [
        ("crates/app-foundation/src/listing.sql", "CREATE TABLE IF NOT EXISTS listing (id TEXT);"),
        ("crates/app-foundation/tests/fixture.sql", "INSERT INTO listing VALUES ('a');"),
    ]
    .map(|(path, text)| SqlFile {
        relative_path: path.into(),
        package_root: "crates/app-foundation/".into(),
        package: "app-foundation".into(),
        text: text.into(),
    })
    .into();
    assert_eq!(
        current_stale_rows(&workspace, policy),
        ["database: domain listing has home crates/app-foundation/src/gone/, which holds no source"]
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
        current_stale_rows(&workspace, policy),
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
        current_stale_rows(&workspace, policy),
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
            "struct Worker { runtime: tokio::runtime::Runtime, lock: Mutex<()> } struct Local;
             fn read(sql: &impl coven::SqlRead) {}",
        ),
        RustFile::fixture("crates/app/src/lib_tests.rs", "struct Fixture { handle: Handle }"),
    ];
    let rows = r#"
        capability_types = ["Runtime", "Mutex", "Local", "Handle", "Gone", "SqlRead"]
        external_types = ["Runtime", "Mutex", "Local", "Handle", "Gone", "Unused", "SqlRead"]
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

/// A row naming its type by path names a dependency's type when the path's
/// crate is no crate of the workspace: a crate names it, and declares no
/// type of its name, without an external_types row.
#[test]
fn a_row_naming_its_type_by_path_tells_a_dependency_s_type_by_its_crate() {
    let files = vec![
        RustFile::fixture("crates/app-foundation/src/clock.rs", "pub struct Clock; struct Local;"),
        RustFile::fixture(
            "crates/app/src/lib.rs",
            "struct Worker { runtime: tokio::runtime::Runtime, local: tokio::sync::Local }",
        ),
    ];
    let rows = r#"
        capability_types = [
            "app_foundation::clock::Clock",
            "app::Missing",
            "tokio::runtime::Runtime",
            "tokio::sync::Local",
            "tokio::Gone",
        ]
    "#;
    assert_eq!(
        stale(files, rows),
        [
            "capability_types: no crate declares app::Missing",
            "capability_types: no crate names tokio::Gone",
            "capability_types: tokio::sync::Local shares its name with a type a crate declares",
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
        current_stale_rows(&workspace, policy),
        [
            "read_shapes.frozen: crates/app/src/reads.rs: Gone names no type or function there",
            "read_shapes.frozen: crates/app/src/schema.sql: v.gone is no column the SQL there increases",
        ]
    );
}

fn capability_rows(rows: &str) -> String {
    format!(
        r#"
        [[capabilities]]
        name = "file system"
        homes = ["crates/app/src/files/"]
        {rows}
        [[capabilities.gates]]
        kind = "temporary files"
        calls = ["tempfile"]
        [[capabilities.gates]]
        kind = "path queries"
        methods = ["metadata"]
        "#
    )
}

fn capability_files() -> Vec<RustFile> {
    vec![
        RustFile::fixture("crates/app/src/files/stage.rs", ""),
        RustFile::fixture(
            "crates/app/src/reads.rs",
            "struct Info; impl Info { fn size(&self) {} const LIMIT: u8 = 0; } fn free() {} static SHARED: u8 = 0; const MAX: u8 = 0;",
        ),
        RustFile::fixture("crates/app/src/reads_tests.rs", "fn helper() {}"),
    ]
}

#[test]
fn capability_rows_naming_what_exists_are_current() {
    let rows = capability_rows(
        r#"frozen = [
            { path = "crates/app/src/reads.rs", gate = "temporary files", count = 1, finding = "stages" },
            { path = "crates/app/src/reads_tests.rs", gate = "temporary files", count = 1, finding = "stages" },
        ]
        allowed = [
            { path = "crates/app/src/reads.rs", item = "Info::size", gate = "path queries", count = 1, reason = "an info's metadata" },
            { path = "crates/app/src/reads.rs", item = "free", gate = "path queries", count = 1, reason = "x" },
            { path = "crates/app/src/reads.rs", item = "SHARED", gate = "path queries", count = 1, reason = "x" },
            { path = "crates/app/src/reads.rs", item = "MAX", gate = "path queries", count = 1, reason = "x" },
            { path = "crates/app/src/reads.rs", item = "Info::LIMIT", gate = "path queries", count = 1, reason = "x" },
            { path = "crates/app/src/reads_tests.rs", item = "helper", gate = "path queries", count = 1, reason = "x" },
        ]"#,
    );
    assert_eq!(stale(capability_files(), &rows), Vec::<String>::new());
}

#[test]
fn a_capability_row_names_a_source_its_gate_and_an_item_there() {
    let rows = capability_rows(
        r#"frozen = [
            { path = "crates/app/src/moved.rs", gate = "temporary files", count = 1, finding = "x" },
            { path = "crates/app/src/reads.rs", gate = "sockets", count = 1, finding = "x" },
        ]
        allowed = [
            { path = "crates/app/src/reads.rs", item = "Info::gone", gate = "path queries", count = 1, reason = "x" },
            { path = "crates/app/src/reads.rs", item = "Gone", gate = "path queries", count = 1, reason = "x" },
        ]"#,
    );
    assert_eq!(
        stale(capability_files(), &rows),
        [
            "capabilities.frozen: file system: crates/app/src/moved.rs holds no source",
            "capabilities.frozen: file system: crates/app/src/reads.rs names gate sockets, which the capability lacks",
            "capabilities.allowed: file system: crates/app/src/reads.rs: Info::gone names no function, method, static or const there",
            "capabilities.allowed: file system: crates/app/src/reads.rs: Gone names no function, method, static or const there",
        ]
    );
}

#[test]
fn a_capability_row_has_a_count_a_note_and_a_key_of_its_own() {
    let rows = capability_rows(
        r#"frozen = [
            { path = "crates/app/src/reads.rs", gate = "temporary files", count = 0, finding = "" },
            { path = "crates/app/src/reads.rs", gate = "temporary files", count = 1, finding = "x" },
        ]
        allowed = [
            { path = "crates/app/src/reads.rs", item = "free", gate = "path queries", count = 1, reason = " " },
            { path = "crates/app/src/reads.rs", item = "free", gate = "path queries", count = 2, reason = "x" },
        ]"#,
    );
    assert_eq!(
        stale(capability_files(), &rows),
        [
            "capabilities.frozen: file system: crates/app/src/reads.rs (temporary files) has a count of 0, not at least 1",
            "capabilities.frozen: file system: crates/app/src/reads.rs (temporary files) states no finding",
            "capabilities.frozen: file system: crates/app/src/reads.rs (temporary files) is listed twice",
            "capabilities.allowed: file system: crates/app/src/reads.rs: free (path queries) states no reason",
            "capabilities.allowed: file system: crates/app/src/reads.rs: free (path queries) is listed twice",
        ]
    );
}

#[test]
fn a_capability_names_each_gate_kind_once() {
    let rows = capability_rows(
        r#"
        [[capabilities.gates]]
        kind = "temporary files"
        crates = ["tempfile"]"#,
    );
    assert_eq!(
        stale(capability_files(), &rows),
        ["capabilities: file system has gate kind temporary files twice"]
    );
}

#[test]
fn test_homes_hold_source_and_belong_to_a_capability_that_reads_tests() {
    let rows = capability_rows(
        r#"tests = true
        test_homes = ["crates/app/src/reads_tests.rs", "crates/app/src/fixtures/", "crates/app/src/gone.rs"]"#,
    );
    assert_eq!(
        stale(capability_files(), &rows),
        [
            "capabilities: file system has test home crates/app/src/fixtures/, which holds no source",
            "capabilities: file system has test home crates/app/src/gone.rs, which holds no source",
        ]
    );
    let rows = capability_rows(r#"test_homes = ["crates/app/src/reads_tests.rs"]"#);
    assert_eq!(
        stale(capability_files(), &rows),
        ["capabilities: file system has test_homes but reads no test code"]
    );
}
