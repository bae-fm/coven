use super::*;
use crate::crate_code::CrateCode;
use crate::policy::Policy;
use crate::sources::SqlFile;
use crate::syntax::RustFile;

fn policy() -> Policy {
    Policy::fixture(
        r#"
        rules = ["read-shapes"]
        [database]
        capability = "SQLite"
        test_support_features = ["test-utils"]
        [read_shapes]
        read_types = ["SqlReadContext"]
        request_query_types = ["LiveRead", "ReconfigurableLiveQuery"]
        plumbing = ["crates/app/src/plumbing.rs"]
        rule = "the read rule"
        "#,
    )
}

fn sql_file(path: &str, text: &str) -> SqlFile {
    SqlFile {
        relative_path: path.to_string(),
        package_root: "crates/app/".to_string(),
        package: "app".to_string(),
        text: text.to_string(),
    }
}

/// The counters in these SQL files and Rust files, as `path:line item`.
fn counters_in(sql: &[(&str, &str)], rust: &[(&str, &str)]) -> Vec<String> {
    let sql = sql.iter().map(|(path, text)| sql_file(path, text)).collect::<Vec<_>>();
    let files = rust.iter().map(|(path, text)| RustFile::fixture(path, text)).collect::<Vec<_>>();
    let mut found = counters(&CrateCode::of(&files, &sql))
        .into_iter()
        .map(|hit| {
            assert_eq!(hit.shape.id(), "counter");
            format!("{}:{} {}", hit.path, hit.line, hit.item)
        })
        .collect::<Vec<_>>();
    found.sort();
    found
}

const SQL: &str = "crates/app/src/schema.sql";

#[test]
fn a_column_only_counted_up_in_a_trigger_is_a_counter() {
    let sql = "CREATE TRIGGER bump AFTER INSERT ON rows BEGIN\n    UPDATE v SET version = version + 1;\nEND;";
    assert_eq!(counters_in(&[(SQL, sql)], &[]), ["crates/app/src/schema.sql:2 v.version"]);
}

#[test]
fn another_value_set_anywhere_makes_it_no_counter() {
    let bump = "UPDATE v SET version = version + 1;";
    assert_eq!(
        counters_in(&[(SQL, bump), ("crates/app/src/other.sql", "UPDATE v SET version = 0;")], &[]),
        Vec::<String>::new()
    );
    assert_eq!(
        counters_in(
            &[(SQL, "UPDATE c SET count = count + 1; UPDATE c SET count = count - 1;")],
            &[]
        ),
        Vec::<String>::new()
    );
    assert_eq!(
        counters_in(&[(SQL, "UPDATE v SET version = version + 1; UPDATE v SET version = ?;")], &[]),
        Vec::<String>::new()
    );
}

#[test]
fn an_upserts_increment_is_a_counter_until_it_copies_the_inserted_value() {
    let upsert =
        "INSERT INTO c (id, count) VALUES (?1, 1) ON CONFLICT (id) DO UPDATE SET count = count + 1;";
    assert_eq!(counters_in(&[(SQL, upsert)], &[]), ["crates/app/src/schema.sql:1 c.count"]);
    let copy = "INSERT INTO c (id, count) VALUES (?1, 1) ON CONFLICT (id) DO UPDATE SET count = excluded.count;";
    assert_eq!(
        counters_in(&[(SQL, upsert), ("crates/app/src/other.sql", copy)], &[]),
        Vec::<String>::new()
    );
}

#[test]
fn a_rust_literal_counts_like_a_sql_file() {
    let rust = "fn bump() {\n    let _ = \"UPDATE v SET version = version + 1\";\n}";
    assert_eq!(
        counters_in(&[], &[("crates/app/src/a.rs", rust)]),
        ["crates/app/src/a.rs:2 v.version"]
    );
    assert_eq!(
        counters_in(&[(SQL, "UPDATE v SET version = 0;")], &[("crates/app/src/a.rs", rust)]),
        Vec::<String>::new()
    );
}

#[test]
fn a_bound_value_is_no_increment_and_inserts_set_nothing() {
    assert_eq!(counters_in(&[(SQL, "UPDATE v SET x = ?;")], &[]), Vec::<String>::new());
    assert_eq!(
        counters_in(&[(SQL, "INSERT INTO v (version) VALUES (0);")], &[]),
        Vec::<String>::new()
    );
}

#[test]
fn the_finding_is_at_the_first_increment() {
    let sql = "UPDATE v SET version = version + 1;\nUPDATE v SET version = version + 1;";
    assert_eq!(counters_in(&[(SQL, sql)], &[]), ["crates/app/src/schema.sql:1 v.version"]);
}

#[test]
fn increment_sites_list_every_increment_whether_a_counter_or_not() {
    let sql = sql_file(SQL, "UPDATE v SET version = version + 1; UPDATE v SET version = 0;");
    let sites = increment_sites(&CrateCode::of(&[], &[sql]));
    assert_eq!(sites.into_iter().collect::<Vec<_>>(), [(SQL.to_string(), "v.version".to_string())]);
}

const REQUESTS: &str = r#"
    struct Query { revision: u64, offset: u64, name: String }
    struct Shared { revision: u64 }
    struct Own { epoch: u64, offset: u64 }
    struct Window { limit: u64 }
    struct Screen { query: ReconfigurableLiveQuery<Own, u8>, other: LiveRead<Option<Vec<Query>>, u8> }
"#;

fn bumps(body: &str) -> Vec<(String, String)> {
    let source = format!("{REQUESTS}\nimpl Screen {{ fn work(&mut self, x: &mut Own, q: &mut Query, limit: u64) {{ {body} }} }}");
    let files = [RustFile::fixture("crates/app/src/a.rs", &source)];
    let files = crate::crate_code::checked(&files);
    let policy = policy();
    let shapes = policy.read_shapes.as_ref().expect("rows");
    let read = ReadCode::collect(&files, &policy, shapes);
    let mut found = request_bumps(&read, &files, shapes)
        .into_iter()
        .map(|hit| {
            assert_eq!(hit.shape.id(), "request-bump");
            (hit.item, hit.what)
        })
        .collect::<Vec<_>>();
    found.sort();
    found
}

#[test]
fn a_request_fields_increase_by_one_is_found_in_every_form() {
    assert_eq!(
        bumps(
            "x.epoch += 1; x.epoch = x.epoch + 1; let y = Own { epoch: x.epoch + 1, offset: 0 };
             x.epoch = x.epoch.wrapping_add(1); x.epoch = x.epoch.saturating_add(1);
             let z = x.epoch.checked_add(1);"
        )
        .len(),
        6
    );
    assert_eq!(bumps("x.epoch += 1;")[0].0, "Screen::work");
}

#[test]
fn a_request_field_moved_another_way_is_not_found() {
    assert_eq!(
        bumps("x.offset += limit; x.offset = x.offset + limit; x.epoch += 2; x.epoch -= 1; x.epoch = 0;"),
        Vec::<(String, String)>::new()
    );
}

#[test]
fn a_field_name_another_struct_shares_is_skipped_unless_the_literal_names_its_type() {
    // `revision` is a field of `Query`, a request, and of `Shared`, which is none.
    assert_eq!(bumps("q.revision += 1;"), Vec::<(String, String)>::new());
    assert_eq!(bumps("let y = Query { revision: q.revision + 1, offset: 0, name: n };").len(), 1);
}

#[test]
fn a_struct_that_is_no_request_has_no_bump() {
    assert_eq!(bumps("w.limit += 1;"), Vec::<(String, String)>::new());
}

#[test]
fn request_types_are_the_first_type_argument_through_options_and_collections() {
    // `Query` is named inside `Option<Vec<..>>`: a request.
    assert_eq!(bumps("let y = Query { revision: q.revision + 1, offset: 0, name: n };").len(), 1);
}
