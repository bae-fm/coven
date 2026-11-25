use crate::compiler::test_tracker::{compile_lib, messages};

const LABELS: &str = r##"
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
    let output = compile_lib(LABELS).unwrap();
    let labels = output.files.iter().find(|(name, _)| name == "IssueLabels.rs").unwrap().1.clone();
    syn::parse_file(&labels).unwrap();
    assert!(labels.contains("pub struct Label"), "{labels}");
    assert!(labels.contains("pub (crate) labels : Vec < Label >"), "{labels}");
    assert!(labels.contains("json_column (row , offset + 0usize) ?"), "{labels}");
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
