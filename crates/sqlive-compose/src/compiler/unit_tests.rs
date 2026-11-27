//! What the compiler finds of a unit's scope: the warning for a read of
//! every row, the scope plan a read ships, and the shapes sqlive can't
//! analyze.

use crate::compiler::crate_compile::tests::{code_of, compile_lib, messages, setup};
use crate::compiler::{compile_crate, Input, Setup};

#[test]
fn warnings_name_the_table_and_sqlives_reason() {
    let output = compile_lib(
        r##"
sqlive_compose::read!(AllIssues, r#"SELECT i.title FROM issues AS i"#);
"##,
    )
    .unwrap();
    assert_eq!(
        output.warnings.iter().map(ToString::to_string).collect::<Vec<_>>(),
        ["src/lib.rs:2:1: AllIssues reads every row of issues: nothing narrows `i`"]
    );
}

/// The scope plan a read's code holds, as sqlive reports what it narrows.
fn shipped_plan(code: &str) -> sqlive_reads::ReadReport {
    let file = syn::parse_file(code).unwrap();
    let plan = file
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Impl(item) => Some(&item.items),
            _ => None,
        })
        .flatten()
        .find_map(|item| match item {
            syn::ImplItem::Const(constant) if constant.ident == "PLAN" => match &constant.expr {
                syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(text), .. }) => Some(text.value()),
                _ => None,
            },
            _ => None,
        })
        .expect("a read's code holds its plan");
    sqlive_reads::CompiledPlan::decode(&plan).unwrap().report()
}

/// A read's code holds the scope plan sqlive builds of it against the schema,
/// with the views and tables the host's live queries read whole, so a read
/// of one of those reads it whole.
#[test]
fn a_read_ships_its_scope_plan_built_with_the_hosts_whole_views() {
    let lib = r##"
sqlive_compose::read!(Assignee(issue: String), r#"
    SELECT a.name FROM assignees AS a WHERE a.issue_id = :issue
"#);
"##;
    let compile = |whole_views: Vec<String>| {
        compile_crate(Input {
            crate_name: "tracker".to_owned(),
            source_reader: &|path: &str| {
                (path == "src/lib.rs")
                    .then(|| lib.to_owned())
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))
            },
            manifests: Vec::new(),
            setup: Setup { whole_views, ..setup() },
        })
        .unwrap()
    };
    let output = compile(Vec::new());
    assert!(output.warnings.is_empty());
    assert_eq!(
        shipped_plan(&code_of(&output, "Assignee.rs")).to_string(),
        "issues: keyed by id\npeople: looked up"
    );
    let output = compile(vec!["assignees".to_owned()]);
    assert_eq!(
        shipped_plan(&code_of(&output, "Assignee.rs")).to_string(),
        "assignees: whole: a view the live query reads whole"
    );
}

#[test]
fn a_fragment_that_reads_every_row_is_no_warning() {
    let output = compile_lib(
        r##"
sqlive_compose::fragment!(Person, r#"SELECT p.id, p.name FROM people AS p"#);
"##,
    )
    .unwrap();
    assert!(output.warnings.is_empty());
}

#[test]
fn sqlive_refuses_shapes_it_cannot_analyze() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::read!(Rows, r#"
    SELECT i.title FROM issues AS i RIGHT JOIN people AS p ON p.id = i.author_id
"#);
"##,
    ));
    assert_eq!(errors, ["src/lib.rs:2:1: sqlive can't analyze Rows: a NATURAL or RIGHT join"]);
}

/// The messages of compiling `sql`, under `header`, as a fragment and as a
/// read.
fn shape_messages(header: &str, sql: &str) -> Vec<String> {
    let lib = format!(
        "sqlive_compose::fragment!(F {{ {header} }}, r#\"{sql}\"#);\n\
         sqlive_compose::read!(R {{ {header} }}, r#\"{sql}\"#);\n"
    );
    messages(compile_lib(&lib))
}

#[test]
fn each_shape_sqlive_cannot_analyze_fails_the_build_in_fragments_and_reads() {
    let shapes: &[(&str, &str, &str)] = &[
        (
            "n: i64",
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) \
             SELECT n + 0 AS n FROM c",
            "a recursive CTE",
        ),
        (
            "t: String",
            "SELECT i.title AS t FROM issues AS i NATURAL JOIN people AS p",
            "a NATURAL or RIGHT join",
        ),
        (
            "t: String",
            "SELECT i.title AS t FROM issues AS i RIGHT JOIN people AS p ON p.id = i.author_id",
            "a NATURAL or RIGHT join",
        ),
        (
            "t: String",
            "SELECT i.title AS t FROM (issues AS i JOIN people AS p ON p.id = i.author_id) AS x",
            "a parenthesized join under an alias, starting with another, or joined by USING",
        ),
        (
            "t: String",
            "SELECT i.title AS t FROM ((issues AS i JOIN people AS p ON p.id = i.author_id) \
             JOIN projects AS j ON j.id = i.project_id)",
            "a parenthesized join under an alias, starting with another, or joined by USING",
        ),
        (
            "t: String",
            "SELECT i.title AS t FROM (issues AS i JOIN comments AS c USING (id))",
            "USING inside a parenthesized join",
        ),
        ("t: String", "SELECT i.title AS t FROM issues AS i WHERE i.id IN docs", "`x IN table`"),
        (
            "t: Option<String>",
            "SELECT d.body AS t FROM docs AS d WHERE docs MATCH 'x'",
            "an FTS5 table's hidden column",
        ),
        (
            "t: Option<String>, rank: Option<f64>",
            "SELECT d.body AS t, d.rank FROM docs AS d WHERE docs MATCH 'x'",
            "an FTS5 table's hidden column",
        ),
        (
            "t: String",
            "SELECT i.name AS t FROM temp.sqlite_master AS i",
            "a table outside the main schema",
        ),
        (
            "t: Option<String>",
            "SELECT i.name AS t FROM sqlite_master AS i",
            "a name that is no CTE, ordinary table, view or contentful FTS5 table",
        ),
    ];
    for (header, sql, why) in shapes {
        assert_eq!(
            shape_messages(header, sql),
            [
                format!("src/lib.rs:1:1: sqlive can't analyze F: {why}"),
                format!("src/lib.rs:2:1: sqlive can't analyze R: {why}"),
            ],
            "{sql}"
        );
    }
}

#[test]
fn sqlite_refuses_a_cte_column_list_of_the_wrong_length_and_a_second_statement() {
    assert_eq!(
        shape_messages("t: i64", "WITH c(a, b, d) AS (SELECT 1, 2) SELECT a + 0 AS t FROM c"),
        [
            "src/lib.rs:1:1: table tracker__F__c has 2 values for 3 columns",
            "src/lib.rs:2:1: table tracker__R__c has 2 values for 3 columns",
        ]
    );
    assert_eq!(
        shape_messages("t: i64", "SELECT 1 AS t; SELECT 2 AS t"),
        [
            "src/lib.rs:1:1: Multiple statements provided",
            "src/lib.rs:2:1: Multiple statements provided",
        ]
    );
}

#[test]
fn a_view_with_such_a_shape_is_a_warning_naming_the_view() {
    let output = compile_lib(
        r##"
sqlive_compose::read!(Weird { t: Option<String> }, r#"SELECT w.title AS t FROM weird AS w"#);
sqlive_compose::read!(Counting { n: Option<i64> }, r#"SELECT w.n AS n FROM counting AS w"#);
"##,
    )
    .unwrap();
    assert_eq!(
        output.warnings.iter().map(ToString::to_string).collect::<Vec<_>>(),
        [
            "src/lib.rs:2:1: Weird reads every row of weird: a NATURAL or RIGHT join",
            "src/lib.rs:3:1: Counting reads every row of counting: a recursive CTE",
        ]
    );
}
