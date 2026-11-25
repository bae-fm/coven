use crate::compiler::test_tracker::{compile_lib, messages};

fn code_of(output: &super::Output, name: &str) -> String {
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
        "(\":issue\" , & self . issue as & dyn :: sqlive_compose :: generated :: sqlive :: ToSql)",
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
sqlive_compose::read!(Counted { total: i64 }, r#"SELECT (SELECT count(*) FROM issues AS every) AS total"#);
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
