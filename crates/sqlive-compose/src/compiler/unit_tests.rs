//! What the compiler finds of a unit's scope: the warning for a read of
//! every row, the scope plan a read ships, the shapes sqlive can't analyze,
//! and which handle runs a read.

use crate::compiler::crate_compile::tests::{code_of, compile_lib, messages, setup};
use crate::compiler::{compile_crate, CompileError, Input, Output, Setup};

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
        ["src/lib.rs:2:1: AllIssues reads every row of issues at line 1, column 21 of its SQL: nothing narrows `i`"]
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

/// A read's code holds the scope plan sqlive builds of it against the schema.
#[test]
fn a_read_ships_its_scope_plan() {
    let lib = r##"
sqlive_compose::read!(Assignee(issue: String), r#"
    SELECT a.name FROM assignees AS a WHERE a.issue_id = :issue
"#);
"##;
    let output = compile_lib(lib).unwrap();
    assert!(output.warnings.is_empty());
    assert_eq!(
        shipped_plan(&code_of(&output, "Assignee.rs")).to_string(),
        "issues in `assignees`: keyed by id\npeople in `assignees`: looked up"
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

/// A recursive CTE is read whole, as a view sqlive can't analyze is: a read
/// warns once for each table the CTE reads, at the place that names the CTE,
/// and the rest of its SQL narrows. A fragment compiles with no warning.
#[test]
fn a_recursive_cte_is_a_warning_for_each_table_it_reads() {
    let sql = "
    WITH RECURSIVE later(number) AS (
        SELECT i.number FROM issues AS i WHERE i.id = :issue
        UNION SELECT n.number FROM issues AS n JOIN later ON n.number = later.number + 1)
    SELECT p.name, (SELECT count(*) FROM later) AS later FROM people AS p WHERE p.id = :person";
    let output = compile_lib(&format!(
        "sqlive_compose::fragment!(F(issue: String, person: String) {{ later: i64 }}, r#\"{sql}\"#);\n\
         sqlive_compose::read!(R(issue: String, person: String) {{ later: i64 }}, r#\"{sql}\"#);\n"
    ))
    .unwrap();
    assert_eq!(
        output.warnings.iter().map(ToString::to_string).collect::<Vec<_>>(),
        ["src/lib.rs:6:1: R reads every row of issues at line 5, column 42 of its SQL: a recursive CTE"]
    );
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

/// A view sqlive can't analyze is a warning that names the view, and a view
/// with a recursive CTE one for each table the CTE reads, in the view.
#[test]
fn a_view_with_such_a_shape_or_a_recursive_cte_is_a_warning() {
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
            "src/lib.rs:2:1: Weird reads every row of weird at line 1, column 26 of its SQL: a NATURAL or RIGHT join",
            "src/lib.rs:3:1: Counting reads every row of issues in `counting`: a recursive CTE",
        ]
    );
}

/// Each place a statement reads a table whole is a warning that says where
/// the call's own SQL writes it, by line and column, or which spread brings
/// it. A place of the same table that the statement narrows gets none.
#[test]
fn each_place_read_whole_is_a_warning_that_says_where_it_is() {
    let output = compile_lib(
        r##"
sqlive_compose::fragment!(Total { total: i64 }, r#"SELECT count(*) AS total FROM issues AS counted"#);
sqlive_compose::read!(IssueAndTotal(issue: String) { total: i64 }, r#"
    SELECT i.title,
           (SELECT count(*) FROM issues AS every) AS total
    FROM issues AS i WHERE i.id = :issue
"#);
sqlive_compose::read!(Twice { first: i64, second: i64 }, r#"
    SELECT (SELECT count(*) FROM issues AS i) AS first,
      (SELECT count(*) FROM issues AS i) AS second
"#);
sqlive_compose::read!(Spreading, r#"SELECT t.* FROM @crate::Total AS t"#);
sqlive_compose::read!(After { total: i64, titles: i64 }, r#"
    WITH spread AS (SELECT t.total FROM @crate::Total AS t)
    SELECT spread.total, (SELECT count(*) FROM issues AS late) AS titles FROM spread
"#);
"##,
    )
    .unwrap();
    assert_eq!(
        output.warnings.iter().map(ToString::to_string).collect::<Vec<_>>(),
        [
            "src/lib.rs:3:1: IssueAndTotal reads every row of issues at line 3, column 34 of its \
             SQL: nothing narrows `every`",
            "src/lib.rs:8:1: Twice reads every row of issues at line 2, column 34 of its SQL: \
             nothing narrows `i`",
            "src/lib.rs:8:1: Twice reads every row of issues at line 3, column 29 of its SQL: \
             nothing narrows `i`",
            "src/lib.rs:12:1: Spreading reads every row of issues in @crate::Total, spread at \
             line 1, column 17 of its SQL: nothing narrows `counted`",
            "src/lib.rs:13:1: After reads every row of issues in @crate::Total, spread at \
             line 2, column 41 of its SQL: nothing narrows `counted`",
            "src/lib.rs:13:1: After reads every row of issues at line 3, column 48 of its SQL: \
             nothing narrows `late`",
        ]
    );
}

/// A call whose own SQL reads a table function qualifies each column it
/// names, wherever it names it, its spreads' arguments included. A name of
/// a result column that is a whole ORDER BY term, which nothing can qualify,
/// and `true` and `false` are no column's name.
#[test]
fn sql_that_reads_a_table_function_qualifies_every_column() {
    let failures = |sql: &str| {
        messages(compile_lib(&format!(
            "sqlive_compose::read!(Picked {{ n: Option<i64> }}, r#\"{sql}\"#);"
        )))
    };
    for sql in [
        "SELECT 1 AS n FROM issues AS i, json_each('[1]') AS w WHERE title = w.value",
        "SELECT (SELECT count(*) FROM json_each('[1]') AS w WHERE w.value = 1) AS n \
         FROM issues AS i WHERE title = ''",
        "SELECT title AS n FROM issues AS i JOIN json_tree('[1]') AS w ON w.value = i.id",
        "SELECT 1 AS n FROM issues AS i, json_each('[1]') AS w ORDER BY title",
        "SELECT 1 AS n FROM issues AS i, json_each('[1]') AS w ORDER BY lower(title)",
    ] {
        assert_eq!(
            failures(sql),
            ["src/lib.rs:1:1: Picked reads a table function, so each column it names \
              needs its table: `title` has none"],
            "{sql}"
        );
    }
    for sql in [
        "SELECT w.value + 0 AS n FROM json_each('[1]') AS w WHERE true ORDER BY n, w.key",
        "SELECT w.value + 0 AS n FROM json_each('[1]') AS w ORDER BY (n)",
        "SELECT w.value + 0 AS n FROM json_each('[1]') AS w \
         UNION SELECT i.rowid FROM issues AS i ORDER BY n",
        "SELECT i.rowid AS n FROM issues AS i WHERE title = ''",
    ] {
        compile_lib(&format!("sqlive_compose::read!(Picked {{ n: Option<i64> }}, r#\"{sql}\"#);"))
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
}

/// A fragment is checked by its own SQL, so a read that spreads one that
/// reads no table function leaves its bare names alone, and so does a
/// `@parent` marker.
#[test]
fn each_call_is_checked_by_the_sql_it_writes() {
    compile_lib(
        r##"
sqlive_compose::fragment!(Titles { title: String }, r#"SELECT title FROM issues"#);
sqlive_compose::read!(Picked, r#"
    SELECT t.* FROM json_each('[1]') AS w JOIN @crate::Titles AS t ON t.title = w.value
"#);
"##,
    )
    .unwrap();
}

/// A relation's argument is SQL the call writes, in the spread though it is.
#[test]
fn a_relations_argument_qualifies_every_column_beside_a_table_function() {
    let lib = r##"
sqlive_compose::fragment!(Chosen with chosen(issue_id TEXT) { issue_id: String },
    r#"SELECT chosen.issue_id FROM chosen"#);
sqlive_compose::read!(Picked, r#"
    SELECT c.* FROM @crate::Chosen(chosen: (SELECT value FROM json_each('["a"]'))) AS c
"#);
"##;
    assert_eq!(
        messages(compile_lib(lib)),
        ["src/lib.rs:4:1: Picked reads a table function, so each column it names needs its \
          table: `value` has none"]
    );
}

/// Objects only the writer reads, as a host lists them: a view, and the table
/// it reads. `shown` is an app's view over the view.
const WRITERS: &str = "
CREATE TABLE sync_row (state TEXT NOT NULL) STRICT;
CREATE VIEW sync_state AS SELECT state FROM sync_row;
CREATE VIEW shown AS SELECT state FROM sync_state;";

fn compile_for_a_writer(lib: &str) -> Result<Output, Vec<CompileError>> {
    let mut setup = setup();
    setup.sql.push(WRITERS.to_owned());
    compile_crate(Input {
        crate_name: "tracker".to_owned(),
        macros_crate: "sqlive_compose".into(),
        source_reader: &|path: &str| {
            (path == "src/lib.rs")
                .then(|| lib.to_owned())
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))
        },
        manifests: Vec::new(),
        setup: Setup { writer_only: vec!["sync_state".into(), "Sync_Row".into()], ..setup },
    })
}

/// A read that the writer doesn't run fails when it reads what only the
/// writer reads, and the error names it: the first the host lists of those
/// the read reaches, through a view of the app's or a fragment too.
#[test]
fn a_read_not_marked_writer_reads_nothing_only_the_writer_reads() {
    for (sql, object) in [
        ("SELECT s.state FROM sync_state AS s", "sync_state"),
        ("SELECT count(*) AS state FROM sync_state", "sync_state"),
        ("SELECT s.state FROM shown AS s", "sync_state"),
        ("SELECT (SELECT count(*) FROM shown) AS state", "sync_state"),
        ("SELECT r.state FROM sync_row AS r", "Sync_Row"),
        ("SELECT s.* FROM @crate::State AS s", "sync_state"),
    ] {
        let lib = format!(
            "sqlive_compose::fragment!(State, r#\"SELECT s.state FROM sync_state AS s\"#);\n\
             sqlive_compose::read!(Picked {{ state: String }}, r#\"{sql}\"#);"
        );
        assert_eq!(
            messages(compile_for_a_writer(&lib)),
            [format!(
                "src/lib.rs:2:1: Picked reads {object}, which only the writer reads: a read the \
                 writer runs is `read!(writer Picked …)`"
            )],
            "{sql}"
        );
    }
}

/// `read!(writer …)` is a read the writer runs, and every other read one the
/// read-only handle runs; its code's type says which. A fragment reads what
/// its reads may.
#[test]
fn a_reads_code_says_which_handle_runs_it() {
    let output = compile_for_a_writer(
        r##"
sqlive_compose::fragment!(State, r#"SELECT s.state FROM sync_state AS s"#);
sqlive_compose::read!(writer Status, r#"SELECT s.* FROM @crate::State AS s"#);
sqlive_compose::read!(Titles, r#"SELECT i.title FROM issues AS i"#);
"##,
    )
    .unwrap();
    for (file, handle) in [("Status.rs", "Writer"), ("Titles.rs", "ReadOnly")] {
        let code = code_of(&output, file);
        let associated =
            format!("type Handle = :: sqlive_compose :: generated :: sqlive_reads :: {handle} ;");
        assert!(code.contains(&associated), "{file}: {code}");
    }
}

#[test]
fn only_a_read_is_marked_writer() {
    for (call, noun) in [("fragment!", "fragment"), ("write!", "write")] {
        let lib = format!("sqlive_compose::{call}(writer Marked, r#\"SELECT 1\"#);");
        assert_eq!(
            messages(compile_for_a_writer(&lib)),
            [format!("src/lib.rs:1:1: a {noun} isn't marked writer: only a read is")],
        );
    }
}

/// A read may be named `writer`.
#[test]
fn writer_alone_is_a_reads_name() {
    compile_for_a_writer(
        r##"sqlive_compose::read!(writer, r#"SELECT i.title FROM issues AS i"#);"##,
    )
    .unwrap();
}
