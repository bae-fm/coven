use super::{parse, CallKind, Declared, FieldType, RustType};
use quote::quote;

#[test]
fn a_fragment_with_only_a_name_and_sql() {
    let header = parse(CallKind::Fragment, quote!(Person, r#"SELECT 1"#)).unwrap();
    assert_eq!(header.name, "Person");
    assert_eq!(header.sql, "SELECT 1");
    assert!(header.parameters.is_empty() && header.declarations.is_empty());
}

#[test]
fn parameters_declarations_and_a_trailing_comma() {
    let header = parse(
        CallKind::Fragment,
        quote!(ProjectsWith(state: IssueState) { project_id: ProjectId, opened: Option<Timestamp>, }, r#"SELECT 1"#,),
    )
    .unwrap();
    assert_eq!(header.parameters[0].name, "state");
    assert_eq!(header.parameters[0].ty, RustType("IssueState".into()));
    assert_eq!(header.declarations.len(), 2);
    assert_eq!(
        header.declarations[1].declared,
        Declared::Type(RustType("Option<Timestamp>".into()))
    );
    assert!(header.declarations[1].declared != Declared::Json(RustType("x".into())));
}

#[test]
fn a_parent_row_with_a_json_shape() {
    let header = parse(
        CallKind::Fragment,
        quote!(
            IssueLabels on issues {
                struct Label { name: String, photo: hex }
                labels: json<Vec<Label>>
            },
            r#"SELECT 1"#
        ),
    )
    .unwrap();
    assert_eq!(header.parent.as_deref(), Some("issues"));
    assert_eq!(header.declarations[0].declared, Declared::Json(RustType("Vec<Label>".into())));
    assert_eq!(header.shapes[0].name, "Label");
    assert_eq!(header.shapes[0].fields[1], ("photo".into(), FieldType::Hex));
}

#[test]
fn relations_list_each_column_with_its_strict_type() {
    let header = parse(
        CallKind::Fragment,
        quote!(LabelUse with chosen(issue_id TEXT), other(a INTEGER, b BLOB) { uses: i64 }, r#"SELECT 1"#),
    )
    .unwrap();
    assert_eq!(header.relations.len(), 2);
    assert_eq!(
        header.relations[1].columns,
        [("a".into(), "INTEGER".into()), ("b".into(), "BLOB".into())]
    );
    let without_braces =
        parse(CallKind::Fragment, quote!(Rows with chosen(issue_id TEXT), r#"SELECT 1"#)).unwrap();
    assert_eq!(without_braces.relations.len(), 1);
}

#[test]
fn a_read_takes_neither_a_parent_row_nor_relations() {
    let error = parse(CallKind::Read, quote!(Page on issues, r#"SELECT 1"#)).unwrap_err();
    assert_eq!(error.to_string(), "a read has no parent row");
    let error = parse(CallKind::Read, quote!(Page with a(b TEXT), r#"SELECT 1"#)).unwrap_err();
    assert_eq!(error.to_string(), "a read has no relations");
}

#[test]
fn a_header_without_sql_or_with_extra_tokens_is_refused() {
    assert!(parse(CallKind::Read, quote!(Page(issue: IssueId))).is_err());
    assert!(parse(CallKind::Read, quote!(Page, r#"SELECT 1"#, extra)).is_err());
}

#[test]
fn option_types_are_recognized() {
    assert_eq!(
        RustType("Option<PersonId>".into()).option_inner(),
        Some(RustType("PersonId".into()))
    );
    assert_eq!(RustType("PersonId".into()).option_inner(), None);
    assert!(RustType("Option<Vec<u8>>".into()).is_option());
}
