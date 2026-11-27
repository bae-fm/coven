use crate::compiler::crate_compile::tests::{code_of, compile_lib, messages};
use crate::compiler::CallKind;

/// The messages of the compile of one write over the tracker's schema, with
/// the fragment `OpenIssueIds` beside it.
fn failure(write: &str) -> String {
    let lib = format!("sqlive_compose::write!({write});");
    let mut found = messages(compile_lib(&lib));
    assert_eq!(found.len(), 1, "{found:?}");
    found.remove(0)
}

#[test]
fn an_upsert_binds_its_parameters_and_returns_no_rows() {
    let output = compile_lib(
        r##"
sqlive_compose::write!(
    MarkRead(issue: IssueId, person: PersonId, at: Timestamp),
    r#"
    INSERT INTO comment_reads (issue_id, person_id, read_at)
    VALUES (:issue, :person, :at)
    ON CONFLICT (issue_id, person_id) DO UPDATE SET read_at = excluded.read_at
"#);
"##,
    )
    .unwrap();
    let code = code_of(&output, "MarkRead.rs");
    for part in [
        "pub (crate) struct MarkRead ;",
        "pub (crate) struct MarkReadParams {",
        "pub (crate) issue : IssueId",
        "impl :: sqlive_compose :: generated :: Statement for MarkReadParams",
        "const SQL : & 'static str = MarkRead :: SQL",
        "(\":person\" , & self . person as & dyn :: sqlive_compose :: generated :: sqlive_reads :: ToSql)",
        "impl :: sqlive_compose :: generated :: Write for MarkReadParams { }",
    ] {
        assert!(code.contains(part), "{part} in {code}");
    }
    assert!(!code.contains("MarkReadRow") && !code.contains("WriteReturning"), "{code}");
    assert!(output.warnings.is_empty());
}

#[test]
fn a_write_that_returns_columns_has_a_row_typed_by_the_schema() {
    let output = compile_lib(
        r##"
sqlive_compose::write!(
    OpenIssue(id: IssueId, project: ProjectId, title: &'p str, author: PersonId, at: Timestamp),
    r#"
    INSERT INTO issues (id, project_id, number, title, state, author_id, opened_at)
    SELECT :id, :project, COALESCE(MAX(i.number), 0) + 1, :title, 'open', :author, :at
    FROM issues AS i
    WHERE i.project_id = :project
    RETURNING number, title, assignee_id
"#);
"##,
    )
    .unwrap();
    let code = code_of(&output, "OpenIssue.rs");
    for part in [
        "pub (crate) struct OpenIssueParams < 'p > {",
        "pub (crate) title : &'p str",
        "impl < 'p > :: sqlive_compose :: generated :: Statement for OpenIssueParams < 'p >",
        "pub (crate) struct OpenIssueRow {",
        "pub (crate) number : i64",
        "pub (crate) title : String",
        "pub (crate) assignee_id : Option < String >",
        "impl < 'p > :: sqlive_compose :: generated :: WriteReturning for OpenIssueParams < 'p >",
        "type Row = OpenIssueRow",
    ] {
        assert!(code.contains(part), "{part} in {code}");
    }
    assert!(!code.contains("generated :: Write for"), "{code}");
    let call = &output.calls[0];
    assert_eq!(call.kind, CallKind::Write);
    assert_eq!(call.reads.iter().map(String::as_str).collect::<Vec<_>>(), ["issues"]);
}

#[test]
fn a_write_without_borrowed_parameters_has_no_lifetime() {
    let output = compile_lib(
        r##"
sqlive_compose::write!(Retitle(id: IssueId, title: String), r#"
    UPDATE issues SET title = :title WHERE id = :id RETURNING id
"#);
"##,
    )
    .unwrap();
    let code = code_of(&output, "Retitle.rs");
    assert!(code.contains("pub (crate) struct RetitleParams {"), "{code}");
    assert!(!code.contains("'p"), "{code}");
    assert!(code.contains("pub (crate) id : String"), "{code}");
}

#[test]
fn a_delete_reads_nothing_by_naming_its_table() {
    let output = compile_lib(
        r##"
sqlive_compose::write!(DropLabel(label: LabelId), r#"
    DELETE FROM issue_labels WHERE label_id = :label RETURNING issue_id
"#);
sqlive_compose::write!(DropOpen(project: ProjectId), r#"
    DELETE FROM issues AS i WHERE i.project_id = :project AND i.id IN (SELECT issue_id FROM issue_labels)
"#);
"##,
    )
    .unwrap();
    let reads: Vec<_> = output
        .calls
        .iter()
        .map(|call| (call.name.as_str(), call.reads.iter().map(String::as_str).collect::<Vec<_>>()))
        .collect();
    assert_eq!(reads, [("DropLabel", vec![]), ("DropOpen", vec!["issue_labels"])]);
    let code = code_of(&output, "DropLabel.rs");
    assert!(code.contains("pub (crate) issue_id : String"), "{code}");
}

const OPEN_ISSUE_IDS: &str = r##"
sqlive_compose::fragment!(OpenIssueIds(project: ProjectId), r#"
    SELECT i.id AS issue_id FROM issues AS i WHERE i.project_id = :project AND i.state = 'open'
"#);
sqlive_compose::fragment!(LabelCount on issues { n: i64 }, r#"
    SELECT count(*) AS n FROM issue_labels AS il WHERE il.issue_id = @parent.id
"#);
"##;

#[test]
fn a_write_spreads_a_fragment_and_may_come_before_it() {
    let lib = format!(
        "{}{OPEN_ISSUE_IDS}",
        r##"
sqlive_compose::write!(LabelOpenIssues(project: ProjectId, label: LabelId), r#"
    INSERT OR IGNORE INTO issue_labels (issue_id, label_id)
    SELECT open_issue.issue_id, :label
    FROM @crate::OpenIssueIds(project: :project) AS open_issue
"#);
"##
    );
    let output = compile_lib(&lib).unwrap();
    let code = code_of(&output, "LabelOpenIssues.rs");
    assert!(code.contains("FROM (SELECT i.id AS issue_id FROM issues AS i"), "{code}");
    assert!(code.contains("(\":project\""), "{code}");
    let call = output.calls.iter().find(|call| call.name == "LabelOpenIssues").unwrap();
    assert!(call.reads.is_empty(), "{:?}", call.reads);
}

#[test]
fn an_update_or_a_delete_names_its_target_as_a_spreads_parent_row() {
    let lib = format!(
        "{}{OPEN_ISSUE_IDS}",
        r##"
sqlive_compose::write!(CountLabels(id: IssueId), r#"
    UPDATE issues SET number = @crate::LabelCount(parent: issues) WHERE id = :id
"#);
sqlive_compose::write!(ForgetUnlabelled, r#"
    DELETE FROM issues AS gone WHERE @crate::LabelCount(parent: gone) = 0
"#);
sqlive_compose::write!(CountLabelsBy(id: IssueId), r#"
    UPDATE issues AS target SET number = @crate::LabelCount(parent: target) WHERE id = :id
"#);
"##
    );
    let output = compile_lib(&lib).unwrap();
    assert!(code_of(&output, "CountLabels.rs").contains("WHERE il.issue_id = issues.id"));
    assert!(code_of(&output, "ForgetUnlabelled.rs").contains("il.issue_id = gone.id"));
    assert!(code_of(&output, "CountLabelsBy.rs").contains("il.issue_id = target.id"));
}

#[test]
fn an_upserts_target_is_a_parent_row_for_the_value_it_sets() {
    let lib = format!(
        "{}{OPEN_ISSUE_IDS}",
        r##"
sqlive_compose::write!(Recount(id: IssueId, project: ProjectId), r#"
    INSERT INTO issues (id, project_id, number, title, state, author_id, opened_at)
    VALUES (:id, :project, 1, '', 'open', 'a', 0)
    ON CONFLICT (id) DO UPDATE SET number = @crate::LabelCount(parent: issues)
"#);
"##
    );
    let output = compile_lib(&lib).unwrap();
    assert!(code_of(&output, "Recount.rs").contains("il.issue_id = issues.id"));
}

#[test]
fn a_parent_row_that_the_write_does_not_read_fails() {
    let lib = format!(
        "{}{OPEN_ISSUE_IDS}",
        r##"
sqlive_compose::write!(Wrong(id: IssueId), r#"
    UPDATE comments SET written_at = @crate::LabelCount(parent: comments) WHERE id = :id
"#);
"##
    );
    let found = messages(compile_lib(&lib));
    assert!(found[0].contains("is no FROM item that reads issues"), "{found:?}");
}

#[test]
fn the_sql_must_be_one_insert_replace_update_or_delete() {
    for sql in [
        "SELECT 1",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "CREATE TABLE made (a TEXT)",
        "DROP TABLE people",
    ] {
        let message = failure(&format!("Bad, r#\"{sql}\"#"));
        assert!(message.contains("not one INSERT, REPLACE, UPDATE or DELETE"), "{sql}: {message}");
    }
    let message = failure("Bad, r#\"DELETE FROM comments; DELETE FROM labels\"#");
    assert!(message.contains("Multiple statements"), "{message}");
}

#[test]
fn a_with_clause_may_come_before_the_write() {
    let output = compile_lib(
        r##"
sqlive_compose::write!(Prune(project: ProjectId), r#"
    WITH stale AS (SELECT i.id FROM issues AS i WHERE i.project_id = :project)
    DELETE FROM issue_labels WHERE issue_id IN (SELECT id FROM stale)
"#);
"##,
    )
    .unwrap();
    let code = code_of(&output, "Prune.rs");
    assert!(code.contains("tracker__Prune__stale"), "{code}");
    assert!(output.calls[0].reads.iter().all(|read| read != "stale"));
}

#[test]
fn a_read_cannot_write() {
    let lib = "sqlive_compose::read!(Bad, r#\"DELETE FROM comments RETURNING id\"#);";
    let found = messages(compile_lib(lib));
    assert!(found[0].contains("changes the database, and only a write! may"), "{found:?}");
    let lib = "sqlive_compose::fragment!(Bad, r#\"UPDATE comments SET body = '' RETURNING id\"#);";
    let found = messages(compile_lib(lib));
    assert!(found[0].contains("only a write! may"), "{found:?}");
}

#[test]
fn sqlite_refuses_what_the_schema_does_not_hold() {
    let message =
        failure("Bad(a: String), r#\"INSERT INTO people (id, nme, photo) VALUES (:a, '', x'')\"#");
    assert!(message.contains("nme"), "{message}");
    let message = failure("Bad(a: String), r#\"UPDATE nobody SET name = :a\"#");
    assert!(message.contains("nobody"), "{message}");
    let message = failure(
        "Bad(a: String), r#\"INSERT INTO people (id, name, photo) VALUES (:a, '', x'') ON CONFLICT (name) DO NOTHING\"#",
    );
    assert!(message.contains("ON CONFLICT"), "{message}");
}

#[test]
fn a_trigger_of_the_schema_that_does_not_prepare_fails_the_write_that_fires_it() {
    let message = failure("Bad(a: String), r#\"INSERT INTO drafts (id) VALUES (:a)\"#");
    assert!(message.contains("nowhere"), "{message}");
}

#[test]
fn parameters_must_match_the_header() {
    let message = failure("Bad, r#\"DELETE FROM comments WHERE id = :id\"#");
    assert!(message.contains("reads :id, which its header does not declare"), "{message}");
    let message = failure("Bad(id: String), r#\"DELETE FROM comments\"#");
    assert!(message.contains("declares id, which its SQL does not read"), "{message}");
    let message = failure("Bad, r#\"DELETE FROM comments WHERE id = ?\"#");
    assert!(message.contains("parameter is written :name"), "{message}");
}

#[test]
fn a_parameter_that_can_be_null_may_not_fill_a_not_null_column_on_its_own() {
    for sql in [
        "INSERT INTO people (id, name, photo) VALUES ('1', :a, x'')",
        "INSERT INTO people (id, name, photo) VALUES ('1', '', x''), ('2', :a, x'')",
        "INSERT INTO people VALUES ('1', :a, x'')",
        "INSERT INTO people (id, name, photo) SELECT 'x', :a, x''",
        "INSERT INTO people (id, name, photo) SELECT 'x', :a AS who, x''",
        "UPDATE people SET photo = x'', name = :a",
        "INSERT INTO people (id, name, photo) VALUES ('1', '', x'') ON CONFLICT (id) DO UPDATE SET name = :a",
    ] {
        let message = failure(&format!("Bad(a: Option<String>), r#\"{sql}\"#"));
        assert!(message.contains("binds :a, which can be NULL, to name, which is NOT NULL"), "{sql}: {message}");
    }
}

#[test]
fn a_parameter_that_can_be_null_fills_a_column_that_can_be() {
    let output = compile_lib(
        r##"
sqlive_compose::write!(
    Assign(id: IssueId, who: Option<PersonId>, title: String),
    r#"
    UPDATE issues SET assignee_id = :who, title = :title WHERE id = :id
"#);
sqlive_compose::write!(Describe(id: DraftId, body: Option<String>), r#"
    INSERT INTO drafts (id, body) VALUES ('d', :body) ON CONFLICT (id) DO UPDATE SET body = :body
"#);
"##,
    );
    // The trigger of `drafts` keeps the second write from preparing, and the
    // first shows that a nullable column takes an Option.
    let message = messages(output).remove(0);
    assert!(message.contains("nowhere"), "{message}");
    let output = compile_lib(
        r##"
sqlive_compose::write!(
    Assign(id: IssueId, who: Option<PersonId>, title: String),
    r#"
    UPDATE issues SET assignee_id = :who, title = :title WHERE id = :id
"#);
sqlive_compose::write!(Place(a: Option<String>, b: String), r#"
    INSERT INTO people (id, name, photo) VALUES (:b, :b, x'') ON CONFLICT (id) DO UPDATE SET name = :b || :a
"#);
"##,
    );
    output.unwrap();
}

#[test]
fn a_borrowed_parameter_names_the_lifetime_p() {
    for (ty, expected) in [
        ("&str", "names its lifetime 'p"),
        ("Vec<&str>", "names its lifetime 'p"),
        ("&'a str", "borrow for 'p, not 'a"),
    ] {
        let message = failure(&format!("Bad(a: {ty}), r#\"DELETE FROM comments WHERE id = :a\"#"));
        assert!(message.contains(expected), "{ty}: {message}");
    }
    compile_lib(
        "sqlive_compose::write!(Fine(a: &'p str, b: &'static str, c: Vec<&'p str>), r#\"DELETE FROM comments WHERE id = :a OR id = :b OR id = :c\"#);",
    )
    .unwrap();
}

#[test]
fn a_write_has_no_parent_row_and_no_relations() {
    let message = failure("Bad on issues, r#\"DELETE FROM comments\"#");
    assert!(message.contains("a write has no parent row"), "{message}");
    let message = failure("Bad with rows(a TEXT), r#\"DELETE FROM comments\"#");
    assert!(message.contains("a write has no relations"), "{message}");
}

#[test]
fn a_declaration_types_a_returned_column_the_schema_does_not() {
    let output = compile_lib(
        r##"
sqlive_compose::write!(Rename(id: IssueId, title: String) { loud: Option<String> }, r#"
    UPDATE issues SET title = :title WHERE id = :id RETURNING upper(title) AS loud, id
"#);
"##,
    )
    .unwrap();
    let code = code_of(&output, "Rename.rs");
    assert!(code.contains("pub (crate) loud : Option < String >"), "{code}");
    assert!(code.contains("pub (crate) id : String"), "{code}");
    let message =
        failure("Bad(id: String) { gone: String }, r#\"DELETE FROM comments WHERE id = :id\"#");
    assert!(
        message.contains("declares the column gone, which its SQL does not return"),
        "{message}"
    );
}

#[test]
fn a_returned_column_with_no_declaration_needs_one() {
    let message = failure(
        "Bad(id: String), r#\"UPDATE issues SET title = upper(title) WHERE id = :id RETURNING upper(title) AS loud\"#",
    );
    assert!(message.contains("loud"), "{message}");
}

#[test]
fn a_write_and_a_read_may_not_share_a_name() {
    let lib = "sqlive_compose::write!(Same, r#\"DELETE FROM comments\"#);\nsqlive_compose::read!(Same, r#\"SELECT 1 AS one\"#);";
    let found = messages(compile_lib(lib));
    assert!(
        found.iter().any(|message| message.contains("is also the name of the call")),
        "{found:?}"
    );
}
