use crate::compiler::crate_compile::tests::compile_lib;
use crate::compiler::CallKind;

#[test]
fn each_call_is_reported_with_its_place_sql_and_own_reads() {
    let output = compile_lib(
        r##"
sqlive_compose::fragment!(Person, r#"SELECT p.id AS person_id, p.name FROM people AS p"#,);
sqlive_compose::read!(
    Titles,
    r#"
    WITH recent AS (SELECT i.id, i.author_id FROM main.Issues AS i)
    SELECT t.title, author.*
    FROM recent AS r
    JOIN issues AS t ON t.id = r.id
    JOIN @tracker::Person AS author ON author.person_id = r.author_id
    WHERE t.project_id IN (SELECT id FROM "Projects")
"#
);
"##,
    )
    .unwrap();
    let found: Vec<_> = output
        .calls
        .iter()
        .map(|call| {
            (
                call.kind,
                call.name.as_str(),
                call.location.to_string(),
                call.reads.iter().map(String::as_str).collect::<Vec<_>>(),
            )
        })
        .collect();
    assert_eq!(
        found,
        [
            (CallKind::Fragment, "Person", "src/lib.rs:2:1".to_owned(), vec!["people"]),
            (CallKind::Read, "Titles", "src/lib.rs:3:1".to_owned(), vec!["issues", "projects"]),
        ]
    );
    assert_eq!(output.calls[0].sql, "SELECT p.id AS person_id, p.name FROM people AS p");
    assert!(output.calls[1].sql.contains("JOIN @tracker::Person AS author"));
}
