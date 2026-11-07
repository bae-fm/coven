use super::*;

fn found(source: &str) -> Vec<String> {
    find_string_error_violations(&[RustFile::fixture("crates/app/src/fixture.rs", source)])
        .into_iter()
        .map(|finding| finding.message)
        .collect()
}

#[test]
fn a_function_returning_a_string_error_is_refused() {
    for source in [
        "fn run() -> Result<(), String> { Ok(()) }",
        "fn run() -> std::result::Result<u8, String> { Ok(1) }",
        "struct S; impl S { fn run(&self) -> Result<u8, String> { Ok(1) } }",
        "trait T { fn run(&self) -> Result<u8, String>; }",
    ] {
        assert_eq!(found(source), ["run returns Result<_, String>"], "{source}");
    }
}

#[test]
fn a_map_err_that_makes_text_is_refused() {
    for source in [
        "fn f() { let _ = g().map_err(|e| e.to_string()); }",
        r#"fn f() { let _ = g().map_err(|e| format!("failed: {e}")); }"#,
        "fn f() { let _ = g().map_err(|e| { e.to_string() }); }",
        "fn f() { let _ = g().map_err(|e| (e.to_string())); }",
    ] {
        assert_eq!(found(source), ["map_err turns an error into a string"], "{source}");
    }
}

#[test]
fn typed_errors_pass() {
    for source in [
        "fn run() -> Result<(), Error> { Ok(()) }",
        "fn run() -> Result<String, Error> { Ok(String::new()) }",
        "fn run() -> io::Result<String> { Ok(String::new()) }",
        "fn f() { let _ = g().map_err(|e| Error::Failed(e)); }",
        "fn f() { let _ = g().map_err(Error::Failed); }",
        "fn f() { let _ = g().map_err(|e| Error::Failed(e.to_string())); }",
    ] {
        assert!(found(source).is_empty(), "{source}");
    }
}
