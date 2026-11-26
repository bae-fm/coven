use crate::compiler::{compile_crate, CompileError, Function, Input, Output, Setup};
use std::collections::BTreeMap;

/// The tables of the tracker.
const SCHEMA: &str = "
CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, photo BLOB NOT NULL) STRICT;
CREATE TABLE projects (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL) STRICT;
CREATE TABLE issues (
    id TEXT NOT NULL PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects (id),
    number INTEGER NOT NULL,
    title TEXT NOT NULL,
    state TEXT NOT NULL,
    author_id TEXT NOT NULL REFERENCES people (id),
    assignee_id TEXT REFERENCES people (id),
    opened_at INTEGER NOT NULL
) STRICT;
CREATE TABLE labels (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, hidden INTEGER NOT NULL) STRICT;
CREATE TABLE issue_labels (
    issue_id TEXT NOT NULL REFERENCES issues (id),
    label_id TEXT NOT NULL REFERENCES labels (id),
    PRIMARY KEY (issue_id, label_id)
) STRICT;
CREATE TABLE comments (
    id TEXT NOT NULL PRIMARY KEY,
    issue_id TEXT NOT NULL REFERENCES issues (id),
    author_id TEXT NOT NULL REFERENCES people (id),
    body TEXT NOT NULL,
    written_at INTEGER NOT NULL
) STRICT;
CREATE TABLE comment_reads (
    issue_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    read_at INTEGER NOT NULL,
    PRIMARY KEY (issue_id, person_id)
) STRICT;
CREATE TABLE drafts (id TEXT NOT NULL PRIMARY KEY, body TEXT) STRICT;
CREATE TRIGGER drafts_broken AFTER INSERT ON drafts BEGIN INSERT INTO nowhere VALUES (1); END;
CREATE VIRTUAL TABLE docs USING fts5(body);
CREATE TABLE verdicts (verdict_id INTEGER PRIMARY KEY, kind TEXT NOT NULL) STRICT;
CREATE VIEW weird AS SELECT i.title FROM issues AS i RIGHT JOIN people AS p ON p.id = i.author_id;
CREATE VIEW counting AS WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT n FROM c;";

/// The tracker's setup step.
fn setup() -> Setup {
    Setup {
        sql: vec![SCHEMA.to_owned()],
        functions: vec![Function { name: "fold_case".into(), arguments: 1, deterministic: true }],
        collations: vec![],
    }
}

/// Compile the crate `name` whose files are `files`, given the manifests of
/// the crates it depends on.
fn compile(
    name: &str,
    files: &[(&str, &str)],
    manifests: &[&str],
) -> Result<Output, Vec<CompileError>> {
    let files: BTreeMap<String, String> =
        files.iter().map(|(path, text)| ((*path).to_owned(), (*text).to_owned())).collect();
    let reader = move |path: &str| {
        files.get(path).cloned().ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))
    };
    compile_crate(Input {
        crate_name: name.to_owned(),
        source_reader: &reader,
        manifests: manifests.iter().map(|manifest| (*manifest).to_owned()).collect(),
        setup: setup(),
    })
}

/// The compile of a crate whose `src/lib.rs` is `lib`.
pub(crate) fn compile_lib(lib: &str) -> Result<Output, Vec<CompileError>> {
    compile("tracker", &[("src/lib.rs", lib)], &[])
}

/// The error messages of a failed compile.
pub(crate) fn messages(result: Result<Output, Vec<CompileError>>) -> Vec<String> {
    match result {
        Ok(_) => panic!("the compile passes"),
        Err(errors) => errors.iter().map(ToString::to_string).collect(),
    }
}

pub(crate) fn code_of(output: &super::Output, name: &str) -> String {
    let (_, text) = output.files.iter().find(|(file, _)| file == name).unwrap();
    // The generated code is Rust.
    syn::parse_file(text).unwrap();
    text.clone()
}

#[test]
fn a_fragment_types_its_columns_from_the_schema_and_its_declarations() {
    let output = compile_lib(
        r##"
sqlive_compose::fragment!(Person { person_id: PersonId }, r#"
    SELECT p.id AS person_id, p.name, p.photo FROM people AS p
"#);
"##,
    )
    .unwrap();
    let code = code_of(&output, "Person.rs");
    for field in [
        "pub (crate) person_id : PersonId",
        "pub (crate) name : String",
        "pub (crate) photo : Vec < u8 >",
    ] {
        assert!(code.contains(field), "{field} in {code}");
    }
    let manifest: serde_json::Value = serde_json::from_str(&output.manifest).unwrap();
    let record = &manifest["fragments"][0];
    assert_eq!(record["path"], "tracker::Person");
    assert_eq!(record["width"], 3);
    assert_eq!(record["presence"], 0);
    assert_eq!(output.read, ["src/lib.rs"]);
}

#[test]
fn a_read_has_its_statement_request_and_row() {
    let output = compile_lib(
        r##"
sqlive_compose::read!(IssueTitle(issue: IssueId) { opened_at: Timestamp }, r#"
    SELECT i.title, i.assignee_id, i.opened_at FROM issues AS i WHERE i.id = :issue
"#);
"##,
    )
    .unwrap();
    let code = code_of(&output, "IssueTitle.rs");
    for part in [
        "pub struct IssueTitleParams { # [allow (missing_docs)] pub issue : IssueId",
        "(\":issue\" , & self . issue as & dyn :: sqlive_compose :: generated :: sqlive_reads :: ToSql)",
        "pub assignee_id : Option < String >",
        "pub opened_at : Timestamp",
        "pub const SQL",
    ] {
        assert!(code.contains(part), "{part} in {code}");
    }
    assert!(output.warnings.is_empty());
}

#[test]
fn a_read_of_every_row_of_a_table_warns() {
    let output = compile_lib(
        r##"
sqlive_compose::read!(AllIssues, r#"SELECT i.title FROM issues AS i"#);
sqlive_compose::read!(Counted, r#"SELECT (SELECT count(*) FROM issues AS every) AS total"#);
"##,
    )
    .err()
    .unwrap();
    // `total` computes a value with no declaration.
    assert_eq!(
        output[0].to_string(),
        "src/lib.rs:3:1: Counted computes the column total, and its header gives total no type"
    );
    let output = compile_lib(
        r##"
sqlive_compose::read!(AllIssues, r#"SELECT i.title FROM issues AS i"#);
sqlive_compose::read!(Counted { total: i64 }, r#"SELECT (SELECT max(every.number) FROM issues AS every) AS total"#);
"##,
    );
    assert_eq!(
        messages(output),
        ["src/lib.rs:3:1: total can be NULL, and the header types it i64, which holds no NULL"]
    );
}

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
fn sqlites_own_error_is_the_message() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::fragment!(Titles, r#"SELECT i.id AS issue_id, i.titel FROM issues AS i"#);
"##,
    ));
    assert_eq!(errors, ["src/lib.rs:2:1: no such column: i.titel"]);
}

#[test]
fn parameters_must_match_the_header() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::read!(IssuePage(issue: IssueId), r#"SELECT i.title FROM issues AS i WHERE i.id = :isue"#);
sqlive_compose::read!(Other(issue: IssueId), r#"SELECT i.title FROM issues AS i"#);
"##,
    ));
    assert_eq!(
        errors,
        [
            "src/lib.rs:2:1: IssuePage reads :isue, which its header does not declare",
            "src/lib.rs:3:1: Other declares issue, which its SQL does not read",
        ]
    );
}

#[test]
fn computed_nullable_and_unmapped_columns_need_the_right_declaration() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::fragment!(Titles(project: ProjectId), r#"
    SELECT i.id AS issue_id, upper(i.title) AS title FROM issues AS i WHERE i.project_id = :project
"#);
sqlive_compose::fragment!(Assignee { assignee_id: PersonId }, r#"
    SELECT i.id AS issue_id, i.assignee_id FROM issues AS i
"#);
sqlive_compose::fragment!(Unreturned { one: i64, nothing: i64 }, r#"SELECT 1 AS one"#);
"##,
    ));
    assert_eq!(
        errors,
        [
            "src/lib.rs:2:1: Titles computes the column title, and its header gives title no type",
            "src/lib.rs:5:1: assignee_id can be NULL, and the header types it PersonId, which holds no NULL",
            "src/lib.rs:8:1: Unreturned declares the column nothing, which its SQL does not return",
        ]
    );
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

#[test]
fn a_stray_at_sign_a_bad_header_and_a_repeated_name_fail() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::read!(Marked(x: i64), r#"SELECT 1 AS one WHERE @x"#);
sqlive_compose::read!(Same { one: i64 }, r#"SELECT 1 AS one"#);
sqlive_compose::fragment!(Same { one: i64 }, r#"SELECT 1 AS one"#);
sqlive_compose::read!(Broken);
"##,
    ));
    assert_eq!(errors.len(), 3);
    assert!(
        errors[0].contains("Marked writes @x: `@` marks a spread or a parent column"),
        "{}",
        errors[0]
    );
    assert_eq!(errors[1], "src/lib.rs:4:1: Same is also the name of the call at src/lib.rs:3:1");
    assert!(errors[2].starts_with("src/lib.rs:5:1: "), "{}", errors[2]);
}

const PEOPLE: &str = r##"
sqlive_compose::fragment!(Person { person_id: PersonId }, r#"
    SELECT p.id AS person_id, p.name, p.photo FROM people AS p
"#);
sqlive_compose::fragment!(
    CommentAuthor on comments,
    r#"SELECT p.name FROM people AS p WHERE p.id = @parent.author_id"#
);
"##;

const ISSUES: &str = r##"
sqlive_compose::fragment!(
    Issue { issue_id: IssueId, project_id: ProjectId, state: IssueState, author_id: PersonId,
            assignee_id: Option<PersonId>, opened_at: Timestamp },
    r#"SELECT i.id AS issue_id, i.project_id, i.number, i.title, i.state,
              i.author_id, i.assignee_id, i.opened_at
       FROM issues AS i"#
);
sqlive_compose::fragment!(Project { project_id: ProjectId }, r#"
    SELECT j.id AS project_id, j.name FROM projects AS j
"#);
"##;

/// The manifest of the crate `name` whose `src/lib.rs` is `lib`.
fn manifest_of(name: &str, lib: &str, manifests: &[&str]) -> String {
    compile(name, &[("src/lib.rs", lib)], manifests).unwrap().manifest
}

#[test]
fn a_read_spreads_the_fragments_of_other_crates() {
    let people = manifest_of("people", PEOPLE, &[]);
    let issues = manifest_of("issues", ISSUES, &[]);
    let output = compile(
        "tracker",
        &[(
            "src/lib.rs",
            r##"
sqlive_compose::read!(IssuePage(issue: Option<IssueId>), r#"
    SELECT issue.*, author.*, project.*
    FROM @issues::Issue AS issue
    JOIN @people::Person AS author ON author.person_id = issue.author_id
    JOIN @issues::Project AS project ON project.project_id = issue.project_id
    WHERE issue.issue_id = :issue
"#);
sqlive_compose::read!(IssueAssignee(issue: IssueId), r#"
    SELECT issue.*, assignee.*
    FROM @issues::Issue AS issue
    LEFT JOIN @people::Person AS assignee ON assignee.person_id = issue.assignee_id
    WHERE issue.issue_id = :issue
"#);
"##,
        )],
        &[&people, &issues],
    )
    .unwrap();
    let page = code_of(&output, "IssuePage.rs");
    assert!(page.contains("pub issue : issues :: Issue"), "{page}");
    assert!(page.contains("pub author : people :: Person"), "{page}");
    assert!(page.contains("pub project : issues :: Project"), "{page}");
    assert!(page.contains("issues :: Issue :: read (row , offset) ?"), "{page}");
    assert!(page.contains("people :: Person :: read (row , offset + 8usize) ?"), "{page}");
    let assignee = code_of(&output, "IssueAssignee.rs");
    assert!(assignee.contains("pub assignee : Option < people :: Person >"), "{assignee}");
    assert!(assignee.contains("is_null (row , offset + 8usize) ?"), "{assignee}");
    // The statement joins the fragments' SQL as subqueries.
    let sql = &assignee[assignee.find("pub const SQL").unwrap()..];
    assert!(sql.contains("FROM (SELECT i.id AS issue_id"), "{sql}");
    assert!(output.warnings.is_empty(), "{:?}", output.warnings);
}

#[test]
fn a_value_spread_is_a_field_of_the_fragments_type() {
    let people = manifest_of("people", PEOPLE, &[]);
    let output = compile(
        "tracker",
        &[(
            "src/lib.rs",
            r##"
sqlive_compose::read!(IssueComments(issue: IssueId) { id: CommentId, written_at: Timestamp }, r#"
    SELECT c.id, c.body, c.written_at, @people::CommentAuthor(parent: c) AS author
    FROM comments AS c
    WHERE c.issue_id = :issue
    ORDER BY c.written_at, c.id
"#);
"##,
        )],
        &[&people],
    )
    .unwrap();
    let code = code_of(&output, "IssueComments.rs");
    assert!(code.contains("pub author : Option < people :: CommentAuthor >"), "{code}");
    assert!(code.contains("WHERE p.id = c.author_id"), "{code}");
}

#[test]
fn fragments_of_one_crate_spread_each_other_in_any_order() {
    let output = compile_lib(
        r##"
sqlive_compose::fragment!(Outer, r#"SELECT inner_one.* FROM @crate::Inner AS inner_one"#);
sqlive_compose::fragment!(Inner, r#"SELECT p.id AS person_id, p.name FROM people AS p"#);
"##,
    )
    .unwrap();
    let outer = code_of(&output, "Outer.rs");
    assert!(outer.contains("pub (crate) inner_one : crate :: Inner"), "{outer}");
    let manifest: serde_json::Value = serde_json::from_str(&output.manifest).unwrap();
    let outer = manifest["fragments"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["path"] == "tracker::Outer")
        .unwrap();
    assert_eq!(
        outer["sql"],
        "SELECT inner_one.* FROM (SELECT p.id AS person_id, p.name FROM people AS p) AS inner_one"
    );
    assert_eq!(outer["width"], 2);
}

#[test]
fn a_circle_of_fragments_names_its_members() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::fragment!(A, r#"SELECT b.* FROM @crate::B AS b"#);
sqlive_compose::fragment!(B, r#"SELECT a.* FROM @crate::A AS a"#);
sqlive_compose::fragment!(C, r#"SELECT a.* FROM @crate::A AS a"#);
"##,
    ));
    assert_eq!(
        errors,
        ["src/lib.rs:2:1: tracker::A spreads itself: tracker::A, tracker::B, tracker::A"]
    );
}

#[test]
fn spreads_are_checked_where_they_stand() {
    let people = manifest_of("people", PEOPLE, &[]);
    let issues = manifest_of("issues", ISSUES, &[]);
    let errors = messages(compile(
        "tracker",
        &[(
            "src/lib.rs",
            r##"
sqlive_compose::read!(InFrom, r#"
    SELECT c.id, a.* FROM comments AS c, @people::CommentAuthor(parent: c) AS a
"#);
sqlive_compose::read!(WrongTable, r#"
    SELECT i.id, @people::CommentAuthor(parent: i) AS author FROM issues AS i
"#);
sqlive_compose::read!(OneColumn, r#"
    SELECT issue.issue_id FROM @issues::Issue AS issue
"#);
sqlive_compose::read!(ByColumn, r#"
    SELECT p.person_id, p.name FROM @people::Person AS p
"#);
sqlive_compose::read!(Missing, r#"SELECT p.* FROM @people::Person AS p WHERE p.person_id = :wanted"#);
"##,
        )],
        &[&people, &issues],
    ));
    assert_eq!(
        errors,
        [
            "src/lib.rs:2:1: people::CommentAuthor reads a parent row, so its spread is a value, not a table in FROM",
            "src/lib.rs:5:1: the spread of people::CommentAuthor takes its parent row from i, which is no FROM item that reads comments",
            "src/lib.rs:8:1: OneColumn selects issue.issue_id, one column of a spread: select the spread with its alias and `.*`",
            "src/lib.rs:11:1: ByColumn selects p.person_id, one column of a spread: select the spread with its alias and `.*`",
            "src/lib.rs:14:1: Missing reads :wanted, which its header does not declare",
        ]
    );
}

#[test]
fn a_parent_fragment_returns_one_column() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::fragment!(Two on issues { a: String, b: String }, r#"SELECT 'x' AS a, 'y' AS b WHERE @parent.id IS NOT NULL"#);
"##,
    ));
    assert_eq!(
        errors,
        ["src/lib.rs:2:1: Two reads a parent row and returns 2 columns, and a value is one column"]
    );
}

const LABELS: &str = r##"
sqlive_compose::fragment!(
    LabelUse with chosen(issue_id TEXT) { label_id: LabelId, uses: i64 },
    r#"
    SELECT l.id AS label_id, l.name, count(*) AS uses
    FROM chosen
    JOIN issue_labels AS il ON il.issue_id = chosen.issue_id
    JOIN labels AS l ON l.id = il.label_id
    GROUP BY l.id
"#);
sqlive_compose::fragment!(
    Named,
    r#"
    WITH visible AS (SELECT l.id, l.name FROM labels AS l WHERE l.hidden = 0)
    SELECT visible.id, visible.name FROM visible
"#);
"##;

#[test]
fn a_relation_and_a_cte_are_named_for_their_fragment() {
    let labels = manifest_of("labels", LABELS, &[]);
    let issues = manifest_of(
        "issues",
        r##"
sqlive_compose::fragment!(
    OpenIssueIds(project: ProjectId) { issue_id: IssueId },
    r#"SELECT i.id AS issue_id FROM issues AS i WHERE i.project_id = :project AND i.state = 'open'"#
);
"##,
        &[],
    );
    let output = compile(
        "tracker",
        &[(
            "src/lib.rs",
            r##"
sqlive_compose::read!(OpenLabelUse(project: ProjectId), r#"
    SELECT used.*, named.*
    FROM @labels::LabelUse(chosen: @issues::OpenIssueIds(project: :project)) AS used
    JOIN @labels::Named AS named ON named.id = used.label_id
    ORDER BY used.uses DESC, used.label_id
"#);
"##,
        )],
        &[&labels, &issues],
    )
    .unwrap();
    let code = code_of(&output, "OpenLabelUse.rs");
    assert!(code.contains("WITH labels__LabelUse__chosen (issue_id) AS (SELECT i.id AS issue_id FROM issues AS i WHERE i.project_id = :project AND i.state = 'open')"), "{code}");
    assert!(code.contains("FROM labels__LabelUse__chosen AS chosen"), "{code}");
    assert!(code.contains("WITH labels__Named__visible AS"), "{code}");
    assert!(code.contains("FROM labels__Named__visible"), "{code}");
    assert!(output.warnings.is_empty(), "{:?}", output.warnings);
    assert!(code.contains("pub used : labels :: LabelUse"), "{code}");
}

#[test]
fn a_relation_that_is_not_bound_or_not_declared_fails() {
    let labels = manifest_of("labels", LABELS, &[]);
    let errors = messages(compile(
        "tracker",
        &[(
            "src/lib.rs",
            r##"
sqlive_compose::read!(NoRows, r#"SELECT used.* FROM @labels::LabelUse AS used"#);
sqlive_compose::fragment!(Undeclared, r#"SELECT chosen.issue_id FROM chosen"#);
"##,
        )],
        &[&labels],
    ));
    assert_eq!(
        errors,
        [
            "src/lib.rs:2:1: the spread of labels::LabelUse binds no rows to its relation chosen",
            "src/lib.rs:3:1: no such table: chosen",
        ]
    );
}

#[test]
fn fragments_pass_their_spreads_parameters_on() {
    let people = manifest_of("people", PEOPLE, &[]);
    let comments = manifest_of(
        "comments",
        r##"
sqlive_compose::fragment!(
    UnreadCount(viewer: PersonId) on issues { unread: i64 },
    r#"
    SELECT count(*) AS unread
    FROM comments AS c
    WHERE c.issue_id = @parent.id
      AND c.written_at > COALESCE((SELECT r.read_at FROM comment_reads AS r
                                   WHERE r.issue_id = c.issue_id
                                     AND r.person_id = :viewer), 0)
"#);
"##,
        &[],
    );
    let issues = manifest_of(
        "issues",
        r##"
sqlive_compose::fragment!(
    IssueListItem(viewer: PersonId) { issue_id: IssueId, project_id: ProjectId, state: IssueState },
    r#"
    SELECT i.id AS issue_id, i.project_id, i.number, i.title, i.state,
           assignee.*,
           @comments::UnreadCount(parent: i, viewer: :viewer) AS unread
    FROM issues AS i
    LEFT JOIN @people::Person AS assignee ON assignee.person_id = i.assignee_id
"#);
"##,
        &[&people, &comments],
    );
    let output = compile(
        "tracker",
        &[(
            "src/lib.rs",
            r##"
sqlive_compose::read!(
    IssueList(project: ProjectId, state: IssueState, me: PersonId),
    r#"
    SELECT item.*
    FROM @issues::IssueListItem(viewer: :me) AS item
    WHERE item.project_id = :project AND item.state = :state
    ORDER BY item.number DESC
"#);
"##,
        )],
        &[&issues],
    )
    .unwrap();
    let code = code_of(&output, "IssueList.rs");
    assert!(code.contains("AND r.person_id = :me), 0)) AS unread"), "{code}");
    assert!(!code.contains(":viewer"), "{code}");
    assert!(code.contains("pub item : issues :: IssueListItem"), "{code}");
    // The statement's one parameter is the request's.
    assert!(code.contains("pub me : PersonId"), "{code}");
    assert!(output.warnings.is_empty(), "{:?}", output.warnings);
}

#[test]
fn a_parameter_that_is_not_written_with_a_name_fails() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::read!(Numbered, r#"SELECT i.title FROM issues AS i WHERE i.id = ?1"#);
sqlive_compose::read!(Bare, r#"SELECT i.title FROM issues AS i WHERE i.id = ?"#);
"##,
    ));
    assert_eq!(
        errors,
        [
            "src/lib.rs:2:1: Numbered reads the parameter ?1, and a parameter is written :name",
            "src/lib.rs:3:1: Bare reads the parameter ?, and a parameter is written :name",
        ]
    );
}

fn types_of(lib: &str, name: &str) -> Vec<String> {
    let output = compile_lib(lib).unwrap();
    let (_, code) = output.files.iter().find(|(file, _)| file == &format!("{name}.rs")).unwrap();
    let file = syn::parse_file(code).unwrap();
    let syn::Item::Struct(item) = &file.items[0] else { panic!("a struct comes first") };
    item.fields
        .iter()
        .map(|field| {
            let ty = &field.ty;
            format!("{}: {}", field.ident.as_ref().unwrap(), quote::quote!(#ty))
        })
        .collect()
}

#[test]
fn a_column_on_the_right_of_a_left_join_can_be_null() {
    let lib = r##"
sqlive_compose::fragment!(Assigned, r#"
    SELECT i.id, a.name, a.photo
    FROM issues AS i LEFT JOIN people AS a ON a.id = i.assignee_id
"#);
"##;
    assert_eq!(
        types_of(lib, "Assigned"),
        ["id: String", "name: Option < String >", "photo: Option < Vec < u8 > >"]
    );
}

#[test]
fn a_bare_column_is_placed_by_its_table() {
    let lib = r##"
sqlive_compose::fragment!(Assigned, r#"
    SELECT title, name
    FROM issues LEFT JOIN people ON people.id = issues.assignee_id
"#);
"##;
    assert_eq!(types_of(lib, "Assigned"), ["title: String", "name: Option < String >"]);
}

#[test]
fn a_star_takes_the_columns_the_other_items_leave() {
    let lib = r##"
sqlive_compose::fragment!(Starred, r#"
    SELECT j.*, i.title FROM projects AS j JOIN issues AS i ON i.project_id = j.id
"#);
"##;
    assert_eq!(types_of(lib, "Starred"), ["id: String", "name: String", "title: String"]);
    let errors = messages(compile_lib(
        r##"sqlive_compose::fragment!(Starred, r#"SELECT j.*, i.* FROM projects AS j JOIN issues AS i ON i.project_id = j.id"#);"##,
    ));
    assert_eq!(errors, ["src/lib.rs:1:1: Starred selects more than one `*`, so name the columns"]);
}

#[test]
fn a_scalar_subquery_can_be_null_and_a_declaration_may_say_so() {
    let lib = r##"
sqlive_compose::fragment!(Latest { latest: Option<i64> }, r#"
    SELECT i.id, (SELECT max(c.written_at) FROM comments AS c WHERE c.issue_id = i.id) AS latest
    FROM issues AS i
"#);
"##;
    assert_eq!(types_of(lib, "Latest"), ["id: String", "latest: Option < i64 >"]);
}

#[test]
fn two_columns_with_one_name_fail() {
    let output =
        compile_lib(r##"sqlive_compose::fragment!(Same { a: i64 }, r#"SELECT 1 AS a, 2 AS a"#);"##);
    assert_eq!(messages(output), ["src/lib.rs:1:1: Same returns the column a twice"]);
}

#[test]
fn a_bare_column_that_two_items_could_give_needs_a_declaration() {
    let lib = |declaration: &str| {
        format!(
            r##"sqlive_compose::fragment!(Twice {declaration}, r#"
    SELECT id FROM people AS a LEFT JOIN people AS b USING (id)
"#);"##
        )
    };
    assert_eq!(
        messages(compile_lib(&lib(""))),
        ["src/lib.rs:1:1: Twice names the column id with no table, and more than one FROM item could give it, so the header must declare id"]
    );
    assert_eq!(types_of(&lib("{ id: String }"), "Twice"), ["id: String"]);
}

const JSON_FRAGMENTS: &str = r##"
sqlive_compose::fragment!(
    IssueLabels on issues {
        struct Label { name: String, hidden: bool }
        labels: json<Vec<Label>>
    },
    r#"
    SELECT json_group_array(json_object(
               'name', l.name,
               'hidden', json(iif(l.hidden, 'true', 'false')))
               ORDER BY l.name) AS labels
    FROM issue_labels AS il
    JOIN labels AS l ON l.id = il.label_id
    WHERE il.issue_id = @parent.id
"#);
sqlive_compose::fragment!(
    CommentAuthor on comments {
        struct Author { id: PersonId, name: String, photo: hex }
        author: json<Author>
    },
    r#"
    SELECT json_object('id', p.id, 'name', p.name, 'photo', hex(p.photo)) AS author
    FROM people AS p
    WHERE p.id = @parent.author_id
"#);
"##;

#[test]
fn a_document_that_matches_its_shape_compiles_to_a_struct_and_a_decoder() {
    let output = compile_lib(JSON_FRAGMENTS).unwrap();
    let labels = output.files.iter().find(|(name, _)| name == "IssueLabels.rs").unwrap().1.clone();
    syn::parse_file(&labels).unwrap();
    assert!(labels.contains("pub struct Label"), "{labels}");
    assert!(labels.contains("pub (crate) labels : Vec < Label >"), "{labels}");
    assert!(labels.contains("json_column (row , offset) ?"), "{labels}");
    let author =
        output.files.iter().find(|(name, _)| name == "CommentAuthor.rs").unwrap().1.clone();
    syn::parse_file(&author).unwrap();
    assert!(
        author.contains("deserialize_with = \"::sqlive_compose::generated::hex_bytes\""),
        "{author}"
    );
    assert!(author.contains("pub (crate) photo : Vec < u8 >"), "{author}");
    let manifest: serde_json::Value = serde_json::from_str(&output.manifest).unwrap();
    // An aggregate with no GROUP BY gives one row for any parent row.
    assert_eq!(manifest["fragments"][0]["one_row"], true);
    assert_eq!(manifest["fragments"][1]["one_row"], false);
}

#[test]
fn keys_that_differ_from_the_shape_fail_with_both_lists() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::fragment!(
    IssueLabels on issues {
        struct Label { name: String, hidden: bool }
        labels: json<Vec<Label>>
    },
    r#"
    SELECT COALESCE((SELECT json_group_array(json_object('name', l.name, 'color', l.hidden))
                     FROM issue_labels AS il JOIN labels AS l ON l.id = il.label_id
                     WHERE il.issue_id = @parent.id), '[]') AS labels
"#);
"##,
    ));
    assert_eq!(
        errors,
        ["src/lib.rs:2:1: labels: the document has the key color, which Label lacks, and lacks Label's key hidden"]
    );
}

#[test]
fn a_nested_document_and_the_kind_of_a_document_are_checked() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::read!(
    Nested {
        struct Inner { a: i64 }
        struct Outer { name: String, inner: Inner }
        doc: json<Outer>,
        other: json<Vec<Inner>>
    },
    r#"SELECT json_object('name', 'x', 'inner', json_object('a', 1, 'b', 2)) AS doc,
              json_object('a', 1) AS other"#
);
"##,
    ));
    assert_eq!(errors, ["src/lib.rs:2:1: doc: the document has the key b, which Inner lacks"]);
    let errors = messages(compile_lib(
        r##"
sqlive_compose::read!(
    Kind { struct Inner { a: i64 } other: json<Vec<Inner>> },
    r#"SELECT json_object('a', 1) AS other"#
);
"##,
    ));
    assert_eq!(
        errors,
        ["src/lib.rs:2:1: other: the SQL builds an object, and the header declares an array"]
    );
}

#[test]
fn an_opaque_document_is_not_checked() {
    compile_lib(
        r##"
sqlive_compose::read!(Opaque { doc: json<Vec<Elsewhere>> }, r#"SELECT json_group_array(json_object('x', 1)) AS doc"#);
"##,
    )
    .unwrap();
}

#[test]
fn a_column_that_cannot_name_a_field_fails() {
    let errors = messages(compile_lib(
        r##"
sqlive_compose::read!(Odd, r#"SELECT p.name AS "my col" FROM people AS p"#);
sqlive_compose::read!(Selfish, r#"SELECT p.name AS "self" FROM people AS p"#);
"##,
    ));
    assert_eq!(
        errors,
        [
            "src/lib.rs:2:1: Odd returns my col, which can't name a Rust field: give the column another name with AS",
            "src/lib.rs:3:1: Selfish returns self, which can't name a Rust field: give the column another name with AS",
        ]
    );
}

#[test]
fn a_parent_row_that_is_no_alias_is_a_bad_argument() {
    let people = manifest_of("people", PEOPLE, &[]);
    let errors = messages(compile(
        "tracker",
        &[(
            "src/lib.rs",
            r##"
sqlive_compose::read!(Numbered, r#"SELECT c.id, @people::CommentAuthor(parent: 1) AS a FROM comments AS c"#);
"##,
        )],
        &[&people],
    ));
    assert_eq!(
        errors,
        ["src/lib.rs:2:1: the spread of people::CommentAuthor binds parent to a value that is not the form it takes"]
    );
}

#[test]
fn a_manifest_path_that_is_no_rust_path_is_refused_where_the_manifest_is_read() {
    let manifest = r#"{"fragments":[{"path":"people::a b","parameters":[],"parent":null,
        "relations":[],"sql":"SELECT 1","width":1,"presence":null,"one_row":false}]}"#;
    let errors = messages(compile("app", &[("src/lib.rs", "")], &[manifest]));
    assert_eq!(errors, ["a manifest names a fragment at people::a b, which is no Rust path"]);
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
