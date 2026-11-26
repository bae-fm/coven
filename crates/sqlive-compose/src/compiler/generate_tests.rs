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
    let code = read(&header, "SELECT 1", &[field("n", "i64", Reading::Value, 0)]);
    assert!(code.contains("(\":a\" , & self . a as"), "{code}");
    assert!(code.contains("(\":b\" , & self . b as"), "{code}");
    syn::parse_file(&code).unwrap();
}

#[test]
fn a_name_that_cannot_be_an_identifier_is_refused() {
    for name in ["my col", "self", "", "1st", "a-b", "\u{0301}a", "\u{00b7}"] {
        assert_eq!(FieldName::new(name), None, "{name}");
    }
    assert!(FieldName::new("type").is_some() && FieldName::new("r#type").is_some());
    assert!(FieldName::new("a\u{0301}\u{00b7}").is_some());
}
