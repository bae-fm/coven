use crate::compiler::crate_compile::tests::{code_of, compile_lib, messages};

#[test]
fn a_rowid_is_no_option_and_tells_a_missing_row_from_a_row() {
    let output = compile_lib(
        r##"
sqlive_compose::fragment!(Verdict, r#"SELECT v.verdict_id FROM verdicts AS v"#);
"##,
    )
    .unwrap();
    let code = code_of(&output, "Verdict.rs");
    assert!(code.contains("pub (crate) verdict_id : i64"), "{code}");
    let manifest: serde_json::Value = serde_json::from_str(&output.manifest).unwrap();
    assert_eq!(manifest["fragments"][0]["presence"], 0);
}

#[test]
fn a_left_join_gives_a_rowid_null() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::read!(Judged { verdict_id: i64 }, r#"
    SELECT v.verdict_id FROM issues AS i LEFT JOIN verdicts AS v ON v.kind = i.state
"#);
"##,
    ));
    assert_eq!(
        errors,
        ["src/lib.rs:2:1: verdict_id can be NULL, and the header types it i64, which holds no NULL"]
    );
}

#[test]
fn a_json_column_that_can_be_null_needs_an_option() {
    for sql in [
        // A column that the table lets hold NULL.
        "SELECT d.body AS value FROM drafts AS d",
        // A NOT NULL column that a LEFT JOIN gives NULL.
        "SELECT p.name AS value FROM issues AS i LEFT JOIN people AS p ON p.id = i.assignee_id",
        // A scalar subquery that can return no row.
        "SELECT (SELECT json_array(p.name) FROM people AS p) AS value",
    ] {
        let lib = format!(
            "sqlive_compose::fragment!(Case {{ value: json<Vec<String>> }}, r#\"{sql}\"#);"
        );
        assert_eq!(
            messages(compile_lib(&lib)),
            ["src/lib.rs:1:1: value can be NULL, and the header types it \
              json<Vec<String>>, which holds no NULL"],
            "{sql}"
        );
        let optional = lib.replace("json<Vec<String>>", "json<Option<Vec<String>>>");
        let output = compile_lib(&optional).unwrap();
        let code = code_of(&output, "Case.rs");
        assert!(code.contains("optional_json_column (row , offset) ?"), "{code}");
        let manifest: serde_json::Value = serde_json::from_str(&output.manifest).unwrap();
        // The column can be NULL in a row, so it tells no missing row.
        assert_eq!(manifest["fragments"][0]["presence"], serde_json::Value::Null, "{sql}");
    }
}

#[test]
fn a_column_read_through_a_query_in_from_is_null_where_that_query_gives_null() {
    let left = "FROM issues AS i LEFT JOIN people AS p ON p.id = i.assignee_id";
    for sql in [
        // A LEFT JOIN of the inner query.
        format!("SELECT s.name AS value FROM (SELECT p.name {left}) AS s"),
        format!("WITH s AS (SELECT p.name {left}) SELECT s.name AS value FROM s"),
        "SELECT s.name AS value FROM assignees AS s".to_owned(),
        format!("SELECT s.name AS value FROM (SELECT * FROM (SELECT p.name {left})) AS s"),
        // A scalar subquery that can return no row.
        "SELECT s.name AS value FROM (SELECT (SELECT p.name FROM people AS p) AS name) AS s"
            .to_owned(),
        // A later SELECT of a compound.
        "SELECT s.name AS value FROM (SELECT NULL AS name UNION ALL SELECT name FROM people) AS s"
            .to_owned(),
        "SELECT s.name AS value FROM \
         (SELECT assignee_id AS name FROM issues UNION ALL SELECT name FROM people) AS s"
            .to_owned(),
        // An aggregate over no rows gives its one row NULL in each column.
        "SELECT s.name AS value FROM (SELECT p.name, count(*) FROM people AS p) AS s".to_owned(),
        // A bare name, a `*` and a join in parentheses.
        "SELECT name AS value FROM issues AS i \
         LEFT JOIN (SELECT id, name FROM people) AS s ON s.id = i.assignee_id"
            .to_owned(),
        "SELECT p.name AS value FROM issues AS i \
         LEFT JOIN (people AS p LEFT JOIN projects AS j ON j.id = p.id) ON p.id = i.assignee_id"
            .to_owned(),
    ] {
        let lib = format!("sqlive_compose::fragment!(Case {{ value: String }}, r#\"{sql}\"#);");
        assert_eq!(
            messages(compile_lib(&lib)),
            ["src/lib.rs:1:1: value can be NULL, and the header types it String, which holds no NULL"],
            "{sql}"
        );
    }
}
