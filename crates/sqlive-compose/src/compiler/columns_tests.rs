use crate::compiler::core::{every_item, outline};
use crate::compiler::crate_compile::tests::{code_of, compile_lib, messages};
use crate::compiler::lex::tokens;

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
    assert_eq!(manifest["fragments"][0]["required"], serde_json::json!([0]));
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
        assert_eq!(manifest["fragments"][0]["required"], serde_json::json!([]), "{sql}");
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

#[test]
fn a_spread_in_a_join_in_parentheses_is_an_option_where_a_join_gives_it_null() {
    for (from, ty) in [
        // The LEFT JOIN of the parentheses pads each item in them.
        (
            "issues AS i LEFT JOIN (@crate::Person AS a LEFT JOIN projects AS j \
             ON j.id = a.person_id) ON a.person_id = i.assignee_id",
            "Option < crate :: Person >",
        ),
        (
            "issues AS i LEFT JOIN (projects AS j LEFT JOIN @crate::Person AS a \
             ON a.person_id = j.id) ON j.id = i.project_id",
            "Option < crate :: Person >",
        ),
        // A LEFT JOIN between the items in the parentheses.
        (
            "issues AS i JOIN (projects AS j LEFT JOIN @crate::Person AS a \
             ON a.person_id = j.id) ON j.id = i.project_id",
            "Option < crate :: Person >",
        ),
        (
            "issues AS i JOIN (projects AS j JOIN (projects AS k \
             LEFT JOIN @crate::Person AS a ON a.person_id = k.id) ON k.id = j.id) \
             ON j.id = i.project_id",
            "Option < crate :: Person >",
        ),
        // No join gives the spread NULL.
        (
            "issues AS i JOIN (projects AS j JOIN @crate::Person AS a ON a.person_id = j.id) \
             ON j.id = i.project_id",
            "crate :: Person",
        ),
        (
            "issues AS i JOIN (@crate::Person AS a LEFT JOIN projects AS j \
             ON j.id = a.person_id) ON a.person_id = i.assignee_id",
            "crate :: Person",
        ),
    ] {
        let lib = format!(
            "sqlive_compose::fragment!(Person, r#\"SELECT p.id AS person_id FROM people AS p\"#);\n\
             sqlive_compose::fragment!(Case, r#\"SELECT a.* FROM {from}\"#);"
        );
        let output = compile_lib(&lib).unwrap_or_else(|errors| panic!("{from}: {errors:?}"));
        let code = code_of(&output, "Case.rs");
        assert!(code.contains(&format!("pub (crate) a : {ty} ,")), "{from}: {code}");
    }
}

#[test]
fn sql_in_a_join_in_parentheses_is_read_as_the_rest_of_from_is() {
    // A CTE the call declares, and the parent row of a value spread.
    let lib = r##"
sqlive_compose::fragment!(
    Said on comments { said: String },
    r#"SELECT json_array(c.body) AS said FROM comments AS c WHERE c.id = @parent.id"#
);
sqlive_compose::read!(Case { said: Option<String> }, r#"
    WITH mine AS (SELECT id, issue_id FROM comments)
    SELECT i.title, @crate::Said(parent: c) AS said
    FROM issues AS i JOIN (mine AS m JOIN comments AS c ON c.id = m.id) ON m.issue_id = i.id
"#);
"##;
    let output = compile_lib(lib).unwrap_or_else(|errors| panic!("{errors:?}"));
    let case = output.calls.iter().find(|call| call.name == "Case").unwrap();
    assert_eq!(case.reads.iter().collect::<Vec<_>>(), ["comments", "issues"]);
}

#[test]
fn a_table_functions_column_takes_a_declared_type_that_holds_null() {
    for sql in [
        "SELECT value FROM json_each(:list)",
        "SELECT s.value FROM (SELECT j.value FROM json_each(:list) AS j) AS s",
    ] {
        let lib = format!(
            "sqlive_compose::read!(Case(list: String) {{ value: Option<String> }}, r#\"{sql}\"#);"
        );
        let output = compile_lib(&lib).unwrap_or_else(|errors| panic!("{sql}: {errors:?}"));
        let code = code_of(&output, "Case.rs");
        assert!(code.contains("pub value : Option < String >"), "{sql}: {code}");
        assert_eq!(
            messages(compile_lib(&lib.replace("Option<String> }", "String }"))),
            ["src/lib.rs:1:1: value can be NULL, and the header types it String, which holds no NULL"],
            "{sql}"
        );
    }
}

#[test]
fn is_distinct_from_in_a_result_column_starts_no_from_clause() {
    for operator in ["IS DISTINCT FROM", "IS NOT DISTINCT FROM"] {
        let lib = format!(
            "sqlive_compose::fragment!(Person, r#\"SELECT p.id AS person_id FROM people AS p\"#);\n\
             sqlive_compose::fragment!(Case {{ other: i64 }}, r#\"SELECT a.*, \
             j.name {operator} 'x' AS other \
             FROM projects AS j LEFT JOIN @crate::Person AS a ON a.person_id = j.id\"#);\n\
             sqlive_compose::fragment!(First {{ other: i64 }}, r#\"SELECT a.*, \
             a.person_id {operator} 'x' AS other FROM @crate::Person AS a\"#);"
        );
        let output = compile_lib(&lib).unwrap_or_else(|errors| panic!("{operator}: {errors:?}"));
        assert!(code_of(&output, "Case.rs").contains("pub (crate) a : Option < crate :: Person >"));
        assert!(code_of(&output, "First.rs").contains("pub (crate) a : crate :: Person ,"));
        let sql = format!("SELECT j.name {operator} 'x' FROM projects AS j");
        let core = outline(&sql, &tokens(&sql));
        let from: Vec<_> =
            every_item(&core.from).into_iter().map(|item| item.qualifier.as_deref()).collect();
        assert_eq!(from, [Some("j")], "{operator}");
    }
}

#[test]
fn a_spread_beside_an_aggregate_with_no_group_by_is_an_option() {
    for (rest, count, ty) in [
        ("count(*) AS n FROM @crate::Person AS a", "i64", "Option < crate :: Person >"),
        (
            "max(a.name) AS n FROM @crate::Person AS a",
            "Option<String>",
            "Option < crate :: Person >",
        ),
        // SQLite gives this aggregate to the query around its subquery.
        (
            "(SELECT count(a.person_id)) AS n FROM @crate::Person AS a",
            "Option<i64>",
            "Option < crate :: Person >",
        ),
        // No row comes of no rows.
        ("count(*) AS n FROM @crate::Person AS a GROUP BY a.name", "i64", "crate :: Person"),
        ("count(*) OVER () AS n FROM @crate::Person AS a", "i64", "crate :: Person"),
        ("(SELECT count(*) FROM issues) AS n FROM @crate::Person AS a", "i64", "crate :: Person"),
    ] {
        let lib = format!(
            "sqlive_compose::fragment!(Person, \
             r#\"SELECT p.id AS person_id, p.name FROM people AS p\"#);\n\
             sqlive_compose::fragment!(Case {{ n: {count} }}, r#\"SELECT a.*, {rest}\"#);"
        );
        let output = compile_lib(&lib).unwrap_or_else(|errors| panic!("{rest}: {errors:?}"));
        let code = code_of(&output, "Case.rs");
        assert!(code.contains(&format!("pub (crate) a : {ty} ,")), "{rest}: {code}");
    }
}

#[test]
fn a_spread_is_an_option_where_a_later_select_gives_it_no_row() {
    for (later, ty) in [
        ("SELECT NULL, NULL", "Option < crate :: Person >"),
        ("VALUES (NULL, NULL)", "Option < crate :: Person >"),
        ("SELECT i.assignee_id, i.assignee_id FROM issues AS i", "Option < crate :: Person >"),
        (
            "SELECT p.id, p.name FROM issues AS i LEFT JOIN people AS p ON p.id = i.assignee_id",
            "Option < crate :: Person >",
        ),
        // Each later row is a row of the fragment.
        ("SELECT p.id, NULL FROM people AS p", "crate :: Person"),
        ("SELECT b.* FROM @crate::Person AS b", "crate :: Person"),
    ] {
        let lib = format!(
            "sqlive_compose::fragment!(Person {{ note: Option<String> }}, \
             r#\"SELECT p.id AS person_id, nullif(p.name, '') AS note FROM people AS p\"#);\n\
             sqlive_compose::fragment!(Case, \
             r#\"SELECT a.* FROM @crate::Person AS a UNION ALL {later}\"#);"
        );
        let output = compile_lib(&lib).unwrap_or_else(|errors| panic!("{later}: {errors:?}"));
        let code = code_of(&output, "Case.rs");
        assert!(code.contains(&format!("pub (crate) a : {ty} ,")), "{later}: {code}");
    }
}

/// The errors of the fragment `Case`, whose header declares `declarations`,
/// after the fragment `Person`, or none when it builds.
fn compound_errors(declarations: &str, sql: &str) -> Vec<String> {
    let lib = format!(
        "sqlive_compose::fragment!(Person {{ note: Option<String> }}, r#\"\
         SELECT p.id AS person_id, p.name, nullif(p.name, '') AS note FROM people AS p\"#);\n\
         sqlive_compose::fragment!(Case {declarations}, r#\"{sql}\"#);"
    );
    compile_lib(&lib).map_or_else(|errors| messages(Err(errors)), |_| Vec::new())
}

#[test]
fn a_compound_column_that_a_select_can_give_null_needs_an_option() {
    for (sql, select) in [
        ("SELECT p.name AS value FROM people AS p UNION ALL SELECT NULL", 2),
        ("SELECT d.body AS value FROM drafts AS d UNION ALL SELECT 'x'", 1),
        (
            "SELECT p.name AS value FROM people AS p UNION ALL SELECT 'x' \
             UNION ALL SELECT i.assignee_id FROM issues AS i",
            3,
        ),
        (
            "SELECT p.name AS value FROM people AS p \
             UNION ALL SELECT s.name FROM issues AS i LEFT JOIN people AS s ON 1",
            2,
        ),
        ("SELECT p.name AS value FROM people AS p UNION ALL VALUES ('x'), (NULL)", 2),
        (
            "SELECT p.name AS value FROM people AS p \
             UNION ALL SELECT (SELECT s.name FROM people AS s)",
            2,
        ),
    ] {
        assert_eq!(
            compound_errors("{ value: String }", sql),
            [format!(
                "src/lib.rs:2:1: value can be NULL in SELECT {select} of the compound, \
                 and the header types it String, which holds no NULL"
            )],
            "{sql}"
        );
        assert_eq!(compound_errors("{ value: Option<String> }", sql), [""; 0], "{sql}");
    }
    // No SELECT gives NULL, or the header's type says what a computed one gives.
    for sql in [
        "SELECT p.name AS value FROM people AS p UNION ALL SELECT i.title FROM issues AS i",
        "SELECT upper(p.name) AS value FROM people AS p UNION ALL SELECT 'x' UNION VALUES (:v)",
    ] {
        let declarations =
            if sql.contains(":v") { "(v: String) { value: String }" } else { "{ value: String }" };
        assert_eq!(compound_errors(declarations, sql), [""; 0], "{sql}");
    }
}

#[test]
fn a_compound_gives_a_spread_a_row_or_no_row() {
    let first = "SELECT a.* FROM @crate::Person AS a UNION ALL";
    for (later, column) in [
        ("SELECT 'x', NULL, 'y'", "name"),
        ("SELECT p.id, d.body, 'y' FROM people AS p, drafts AS d", "name"),
        // The id and the name come from rows that can be missing apart.
        (
            "SELECT p.id, s.name, 'y' FROM issues AS i LEFT JOIN people AS p ON 1 \
             LEFT JOIN people AS s ON 1",
            "name",
        ),
        ("SELECT i.assignee_id, NULL, 'y' FROM issues AS i", "name"),
    ] {
        assert_eq!(
            compound_errors("", &format!("{first} {later}")),
            [format!(
                "src/lib.rs:2:1: Case: SELECT 2 of the compound can give NULL in a.{column}, \
                 where each row of tracker::Person holds a value"
            )],
            "{later}"
        );
    }
    // Each later row is a row of the fragment, or none.
    for later in [
        "SELECT NULL, NULL, NULL",
        // The reader tells no row by the id alone.
        "SELECT NULL, d.body, 'y' FROM drafts AS d",
        "SELECT p.id, p.name, NULL FROM people AS p",
        "SELECT p.id, p.name, 'y' FROM issues AS i LEFT JOIN people AS p ON 1",
        "SELECT b.* FROM issues AS i LEFT JOIN @crate::Person AS b ON 1",
        "SELECT b.* FROM @crate::Person AS b",
    ] {
        assert_eq!(compound_errors("", &format!("{first} {later}")), [""; 0], "{later}");
    }
}

#[test]
fn a_column_of_a_statement_the_probe_cant_read_can_be_null() {
    // SQLite reads a comment that never ends, and the probe's parser doesn't.
    // No live query runs a write, so nothing else refuses one.
    let lib = r##"sqlive_compose::write!(Redraft(id: String) { value: String }, r#"
    UPDATE drafts SET body = 'x' WHERE id = :id RETURNING upper(id) AS value /* never ends"#);"##;
    assert_eq!(
        messages(compile_lib(lib)),
        ["src/lib.rs:1:1: value can be NULL, and the header types it String, which holds no NULL"]
    );
    compile_lib(&lib.replace("value: String", "value: Option<String>"))
        .unwrap_or_else(|errors| panic!("{errors:?}"));
}

#[test]
fn a_subquery_of_a_compound_is_never_null_where_it_is_on_its_own() {
    for sql in [
        "SELECT (SELECT count(*) FROM issues) AS value UNION ALL SELECT 0",
        "SELECT 0 AS value UNION ALL SELECT (SELECT count(*) FROM issues)",
        // A query the column passes through, in a later SELECT and in a plain one.
        "SELECT 0 AS value UNION ALL SELECT s.n FROM (SELECT (SELECT count(*) FROM issues) AS n) AS s",
        "SELECT s.n AS value FROM (SELECT (SELECT count(*) FROM issues) AS n) AS s",
    ] {
        let errors = compound_errors("{ value: i64 }", sql);
        assert!(errors.is_empty(), "{sql}: {errors:?}");
    }
    // One that can be NULL on its own names the SELECT that holds it.
    assert_eq!(
        compound_errors(
            "{ value: i64 }",
            "SELECT 0 AS value UNION ALL SELECT (SELECT max(number) FROM issues)"
        ),
        ["src/lib.rs:2:1: value can be NULL in SELECT 2 of the compound, and the header types \
          it i64, which holds no NULL"]
    );
}

#[test]
fn a_spread_in_a_later_select_keeps_its_never_null_subqueries() {
    let lib = r##"
sqlive_compose::fragment!(Counted { n: i64 }, r#"
    SELECT i.id, (SELECT count(*) FROM comments AS c WHERE c.issue_id = i.id) AS n FROM issues AS i
"#);
sqlive_compose::fragment!(Both, r#"
    SELECT a.* FROM @crate::Counted AS a UNION ALL SELECT b.* FROM @crate::Counted AS b
"#);
"##;
    let output = compile_lib(lib).unwrap_or_else(|errors| panic!("{errors:?}"));
    let code = code_of(&output, "Both.rs");
    assert!(code.contains("pub (crate) a : crate :: Counted ,"), "{code}");
}

#[test]
fn a_value_spread_that_adds_a_with_clause_types_its_alias_as_the_fragment() {
    let output = compile_lib(
        r##"
sqlive_compose::fragment!(Counted on projects with wanted(id TEXT) { n: i64 }, r#"
    SELECT count(*) AS n FROM wanted AS w WHERE w.id = @parent.id
"#);
sqlive_compose::read!(Counts, r#"
    SELECT @crate::Counted(parent: j, wanted: (SELECT i.project_id FROM issues AS i)) AS counted
    FROM projects AS j
"#);
"##,
    )
    .unwrap();
    let code = code_of(&output, "Counts.rs");
    assert!(code.contains("pub counted : crate :: Counted"), "{code}");
}

#[test]
fn a_fragment_whose_limit_or_compound_can_drop_its_row_is_not_one_row() {
    let output = compile_lib(
        r##"
sqlive_compose::fragment!(Counted { n: i64 }, r#"SELECT count(*) AS n FROM issues"#);
sqlive_compose::fragment!(Skipped { n: i64 }, r#"SELECT count(*) AS n FROM issues LIMIT 1 OFFSET 1"#);
sqlive_compose::fragment!(Crossed { n: i64 }, r#"SELECT count(*) AS n FROM issues INTERSECT SELECT 1"#);
"##,
    )
    .unwrap();
    let manifest: serde_json::Value = serde_json::from_str(&output.manifest).unwrap();
    let one_row: Vec<_> = (0..3).map(|at| manifest["fragments"][at]["one_row"].clone()).collect();
    assert_eq!(one_row, [true, false, false]);
}
