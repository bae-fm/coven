use super::*;
use crate::crate_code::CrateCode;
use crate::sources::SqlFile;
use crate::syntax::RustFile;
use owner_construction_policy::Policy;

fn policy() -> Policy {
    Policy::fixture(
        r#"
        rules = ["database-boundary"]
        test_support_features = ["test-utils"]
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
    let mut found = counters(&CrateCode::fixture(&files, &sql, &policy()))
        .into_iter()
        .map(|counter| format!("{}:{} {}", counter.path, counter.line, counter.column))
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
        counters_in(&[(SQL, bump), ("crates/app/src/other.sql", "UPDATE v SET version = 5;")], &[]),
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
        counters_in(&[(SQL, "UPDATE v SET version = 5;")], &[("crates/app/src/a.rs", rust)]),
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
fn a_column_counted_up_and_reset_to_zero_is_a_counter() {
    // queue.rs: `additions = additions + 1`, and `additions = 0` on start.
    let sql = "UPDATE q SET additions = additions + 1; UPDATE q SET additions = 0, added = NULL;";
    assert_eq!(counters_in(&[(SQL, sql)], &[]), ["crates/app/src/schema.sql:1 q.additions"]);
    let files = [sql_file(SQL, sql)];
    let hits = counters(&CrateCode::fixture(&[], &files, &policy()));
    assert!(hits[0].reset);
    let plain = [sql_file(SQL, "UPDATE q SET additions = additions + 1;")];
    assert!(!counters(&CrateCode::fixture(&[], &plain, &policy()))[0].reset);
    assert_eq!(
        counters_in(
            &[(SQL, "UPDATE q SET additions = additions + 1; UPDATE q SET additions = ?1;")],
            &[]
        ),
        Vec::<String>::new()
    );
}
