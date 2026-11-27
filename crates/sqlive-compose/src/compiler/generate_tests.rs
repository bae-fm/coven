use super::{fragment, read};
use crate::compiler::header::{parse, CallKind, RustType};
use crate::compiler::model::{Field, FieldName, Reading};
use quote::quote;

fn field(name: &str, ty: &str, reading: Reading, column: usize) -> Field {
    Field { name: FieldName::new(name).unwrap(), ty: RustType(ty.into()), reading, column }
}

#[test]
fn a_fragment_reads_each_column_at_its_offset() {
    let header = parse(CallKind::Fragment, quote!(Thing, r#"SELECT 1"#)).unwrap();
    let code = fragment(
        &header,
        &[field("a", "i64", Reading::Value, 0), field("type", "Vec<Label>", Reading::Json, 1)],
        true,
    );
    assert!(code.contains("a : row . get :: < _ , i64 > (offset) ?"), "{code}");
    assert!(
        code.contains(
            "r#type : :: sqlive_compose :: generated :: json_column (row , offset + 1usize) ?"
        ),
        "{code}"
    );
    syn::parse_file(&code).unwrap();
}

#[test]
fn a_read_binds_its_request_by_name() {
    let header =
        parse(CallKind::Read, quote!(Page(a: i64, b: Option<String>), r#"SELECT 1"#)).unwrap();
    let code = read(&header, "SELECT 1", "{}", &[field("n", "i64", Reading::Value, 0)], true);
    assert!(code.contains("(\":a\" , & self . a as"), "{code}");
    assert!(code.contains("(\":b\" , & self . b as"), "{code}");
    syn::parse_file(&code).unwrap();
}

#[test]
fn a_read_implements_the_statement_traits_a_store_takes() {
    let header = parse(CallKind::Read, quote!(Page(a: i64), r#"SELECT 1"#)).unwrap();
    let code = read(&header, "SELECT 1", "the plan", &[field("n", "i64", Reading::Value, 0)], true);
    assert!(code.contains("impl :: sqlive_compose :: Statement for PageParams"), "{code}");
    assert!(code.contains("impl :: sqlive_compose :: Read for PageParams"), "{code}");
    assert!(code.contains("const PLAN : & 'static str = \"the plan\""), "{code}");
    assert!(code.contains("type Row = PageRow"), "{code}");
    syn::parse_file(&code).unwrap();
}

#[test]
fn a_reads_request_and_row_have_no_functions_beside_the_traits() {
    let header = parse(CallKind::Read, quote!(Page(a: i64), r#"SELECT 1"#)).unwrap();
    let code = read(&header, "SELECT 1", "{}", &[field("n", "i64", Reading::Value, 0)], true);
    let file = syn::parse_file(&code).unwrap();
    let inherent: Vec<String> = file
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Impl(item) if item.trait_.is_none() => {
                let ty = &item.self_ty;
                Some(quote!(#ty).to_string())
            }
            _ => None,
        })
        .collect();
    // The one inherent impl holds the statement's `SQL`.
    assert_eq!(inherent, ["Page"], "{code}");
}

#[test]
fn a_name_that_cannot_be_an_identifier_is_refused() {
    for name in ["my col", "self", "", "1st", "a-b", "\u{0301}a", "\u{00b7}"] {
        assert_eq!(FieldName::new(name), None, "{name}");
    }
    assert!(FieldName::new("type").is_some() && FieldName::new("r#type").is_some());
    assert!(FieldName::new("a\u{0301}\u{00b7}").is_some());
}

#[test]
fn a_read_in_a_private_module_has_no_pub_item() {
    let header = parse(CallKind::Read, quote!(Page(a: i64), r#"SELECT 1"#)).unwrap();
    let fields = [field("n", "i64", Reading::Value, 0)];
    let private = read(&header, "SELECT 1", "{}", &fields, false);
    assert_eq!(private.matches("pub").count(), private.matches("pub (crate)").count(), "{private}");
    let exported = read(&header, "SELECT 1", "{}", &fields, true);
    assert!(!exported.contains("pub (crate)"), "{exported}");
    syn::parse_file(&private).unwrap();
}

#[test]
fn a_row_derives_clone_partial_eq_and_debug() {
    let header = parse(CallKind::Read, quote!(Page, r#"SELECT 1"#)).unwrap();
    let code = read(&header, "SELECT 1", "{}", &[field("n", "i64", Reading::Value, 0)], true);
    assert!(code.contains("# [derive (Clone , Debug , PartialEq)] pub struct PageRow"), "{code}");
}

#[test]
fn a_fragment_in_a_private_module_has_no_pub_item() {
    let header = parse(
        CallKind::Fragment,
        quote!(
            Thing on things {
                struct Label { name: String }
                labels: json<Vec<Label>>
            },
            r#"SELECT 1"#
        ),
    )
    .unwrap();
    let fields = [field("n", "i64", Reading::Value, 0)];
    let private = fragment(&header, &fields, false);
    assert_eq!(private.matches("pub").count(), private.matches("pub (crate)").count(), "{private}");
    let exported = fragment(&header, &fields, true);
    assert!(
        exported.contains("pub struct Thing") && exported.contains("pub fn read"),
        "{exported}"
    );
    syn::parse_file(&private).unwrap();
}
