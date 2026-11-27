use super::include_generated;
use quote::quote;

#[test]
fn a_call_includes_the_file_named_for_it() {
    let expansion = include_generated(quote!(IssuePage(issue: IssueId), r#"SELECT 1"#), "read!");
    assert_eq!(
        expansion.to_string(),
        quote!(include!(concat!(env!("OUT_DIR"), "/sqlive_compose/IssuePage.rs"));).to_string()
    );
}

#[test]
fn a_call_without_a_name_is_a_compile_error() {
    let expansion = include_generated(quote!("SELECT 1"), "fragment!");
    assert!(expansion.to_string().contains("compile_error"), "{expansion}");
    let expansion = include_generated(quote!(), "read!");
    assert!(expansion.to_string().contains("read! starts with the name of its call"));
}

#[test]
fn a_writer_read_includes_the_file_named_for_the_read() {
    let expansion = include_generated(quote!(writer SyncBar, r#"SELECT 1"#), "read!");
    assert_eq!(
        expansion.to_string(),
        quote!(include!(concat!(env!("OUT_DIR"), "/sqlive_compose/SyncBar.rs"));).to_string()
    );
}

/// `writer` alone, or before a fragment's clause, is the call's name.
#[test]
fn writer_without_a_name_after_it_names_the_call() {
    for call in [
        quote!(writer, r#"SELECT 1"#),
        quote!(writer on books { n: i64 }, r#"SELECT 1"#),
        quote!(writer with held(id TEXT), r#"SELECT 1"#),
    ] {
        let expansion = include_generated(call, "fragment!");
        assert!(expansion.to_string().contains("/sqlive_compose/writer.rs"), "{expansion}");
    }
}
