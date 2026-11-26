use crate::compiler::crate_compile::tests::{code_of, compile_lib, messages};

/// The compile of a read with the declarations `declarations` and the SQL
/// `sql`.
fn read(declarations: &str, sql: &str) -> Result<String, Vec<String>> {
    let lib = format!("sqlive_compose::read!(Case {{ {declarations} }}, r#\"{sql}\"#);");
    match compile_lib(&lib) {
        Ok(output) => Ok(code_of(&output, "Case.rs")),
        Err(errors) => Err(messages(Err(errors))),
    }
}

/// Asserts that the header may type the column `value` of each statement
/// `SELECT <expression> AS value` as an `i64`, which holds no NULL.
fn assert_never_null(expressions: &[&str]) {
    for expression in expressions {
        let sql = format!("SELECT {expression} AS value");
        if let Err(errors) = read("value: i64", &sql) {
            panic!("{expression}: {errors:?}");
        }
    }
}

/// Asserts that the compiler refuses to type the column `value` of each
/// statement as an `i64` because it can be NULL.
fn assert_can_be_null(statements: &[&str]) {
    for sql in statements {
        assert_eq!(
            read("value: i64", sql).err(),
            Some(vec![
                "src/lib.rs:1:1: value can be NULL, and the header types it i64, which holds no NULL"
                    .to_owned()
            ]),
            "{sql}"
        );
    }
}

/// `SELECT <expression> AS value` for each expression.
fn assert_expressions_can_be_null(expressions: &[&str]) {
    for expression in expressions {
        assert_can_be_null(&[&format!("SELECT {expression} AS value")]);
    }
}

#[test]
fn a_subquery_over_an_aggregate_that_is_never_null_is_never_null() {
    assert_never_null(&[
        "(SELECT count(*) FROM issues)",
        "(SELECT count(i.assignee_id) FROM issues AS i WHERE i.id = '')",
        "(SELECT total(i.number) FROM issues AS i)",
        "(SELECT json_group_array(i.title ORDER BY i.title) FROM issues AS i)",
        "(SELECT jsonb_group_array(i.title) FROM issues AS i)",
        "(SELECT json_group_object(i.id, i.title) FROM issues AS i)",
        "(SELECT jsonb_group_object(i.id, i.title) FROM issues AS i)",
        "(SELECT count(*) FILTER (WHERE i.number > 1) FROM issues AS i)",
        "(SELECT DISTINCT count(*) AS n FROM issues AS i ORDER BY 1)",
        "(WITH few AS (SELECT * FROM issues LIMIT 2) SELECT count(*) FROM few)",
        "((SELECT count(*) FROM issues))",
    ]);
}

#[test]
fn a_scalar_subquerys_expression_on_never_null_values_is_never_null() {
    assert_never_null(&[
        "(SELECT json(json_group_array(i.title)) FROM issues AS i)",
        "(SELECT coalesce(max(i.number), 0) FROM issues AS i)",
        "(SELECT ifnull(sum(i.number), count(*)) FROM issues AS i)",
        "(SELECT json_object('top', max(i.number)) FROM issues AS i)",
        "(SELECT json_array(max(i.number)) FROM issues AS i)",
        "(SELECT count(*) > 0 FROM issues AS i)",
        "(SELECT max(i.number) IS NULL FROM issues AS i)",
        "(SELECT max(i.number) IS NOT NULL AND count(*) = 1 FROM issues AS i)",
        "(SELECT NOT count(*) FROM issues AS i)",
        "(SELECT count(*) + count(i.assignee_id) * 2 - 1 FROM issues AS i)",
        "(SELECT 'n ' || count(*) FROM issues AS i)",
        "(SELECT iif(count(*) > 1, 'many', 'few') FROM issues AS i)",
        "(SELECT CASE WHEN count(*) > 1 THEN 'many' ELSE 'few' END FROM issues AS i)",
        "(SELECT CASE count(*) WHEN 0 THEN '[]' ELSE json_group_array(i.id) END FROM issues AS i)",
        "(SELECT (count(*)) FROM issues AS i)",
        "(SELECT count(*) > 0 AND EXISTS (SELECT 1 FROM people) FROM issues AS i)",
        "(SELECT CASE WHEN count(*) THEN NOT EXISTS (SELECT 1) ELSE 0 END FROM issues AS i)",
        "(SELECT coalesce((SELECT count(*) FROM people), max(i.number)) FROM issues AS i)",
    ]);
}

#[test]
fn a_subquery_over_an_aggregate_that_can_be_null_can_be_null() {
    assert_expressions_can_be_null(&[
        "(SELECT max(i.number) FROM issues AS i)",
        "(SELECT sum(i.number) FROM issues AS i)",
        "(SELECT group_concat(i.title) FROM issues AS i)",
        "(SELECT json(max(i.title)) FROM issues AS i)",
        "(SELECT coalesce(max(i.number), min(i.number)) FROM issues AS i)",
        "(SELECT count(*) / count(i.assignee_id) FROM issues AS i)",
        "(SELECT count(*) + max(i.number) FROM issues AS i)",
        "(SELECT total(i.number) - total(i.opened_at) FROM issues AS i)",
        "(SELECT nullif(count(*), 0) FROM issues AS i)",
        "(SELECT CASE WHEN count(*) > 1 THEN 'many' END FROM issues AS i)",
        "(SELECT CASE WHEN count(*) > 1 THEN max(i.title) ELSE 'few' END FROM issues AS i)",
        "(SELECT iif(count(*) > 1, 'many') FROM issues AS i)",
        "(SELECT i.title || count(*) FROM issues AS i)",
        "(SELECT count(*) BETWEEN max(i.number) AND 2 FROM issues AS i)",
        "(SELECT count(*) > max(i.number) FROM issues AS i)",
        "(SELECT json_object('n', count(*)) -> 'missing' FROM issues)",
        "(SELECT json_group_array(i.id) ->> '$[0]' FROM issues AS i)",
        "(SELECT max(i.number) IS NULL = max(i.number) FROM issues AS i)",
        "(SELECT count(*) IS NULL OR max(i.number) FROM issues AS i)",
        "(SELECT CASE WHEN count(*) THEN 1 ELSE 2 END || CASE WHEN 0 THEN 1 END FROM issues)",
        "(SELECT count(*) IN (SELECT max(number) FROM issues) FROM issues)",
    ]);
}

#[test]
fn a_subquery_that_can_return_no_row_can_be_null() {
    assert_expressions_can_be_null(&[
        "(SELECT count(*) FROM issues GROUP BY state)",
        "(SELECT count(*) FROM issues HAVING count(*) > 0)",
        "(SELECT count(*) FROM issues LIMIT 1 OFFSET 1)",
        "(SELECT count(*) FROM issues LIMIT 0)",
        "(SELECT count(*) FROM issues INTERSECT SELECT 1)",
        "(SELECT count(*) OVER () FROM issues)",
        "(SELECT i.number FROM issues AS i)",
        "(WITH none AS (SELECT * FROM issues WHERE 0) SELECT number FROM none)",
        "(VALUES (NULL))",
        "(SELECT count(*) FROM issues UNION ALL SELECT NULL)",
        "((SELECT i.number FROM issues AS i))",
    ]);
    assert_can_be_null(&[
        "WITH w AS (SELECT 1) SELECT (WITH none AS (SELECT * FROM verdicts WHERE 0) \
         SELECT verdict_id FROM none) AS value",
    ]);
}

#[test]
fn an_aggregate_of_the_outer_rows_does_not_give_the_subquery_a_row() {
    assert_can_be_null(&[
        "SELECT (SELECT count(i.id) FROM people AS p WHERE 0) AS value FROM issues AS i",
    ]);
}

#[test]
fn a_compound_of_a_subquery_can_be_null() {
    assert_can_be_null(&["SELECT (SELECT count(*) FROM issues) AS value UNION ALL SELECT NULL"]);
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
