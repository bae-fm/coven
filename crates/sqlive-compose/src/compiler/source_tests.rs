use super::walk;
use crate::compiler::model::MacrosCrate;
use std::collections::BTreeMap;
use std::io;

fn reader(files: &[(&str, &str)]) -> impl Fn(&str) -> io::Result<String> {
    let files: BTreeMap<String, String> =
        files.iter().map(|(path, text)| ((*path).to_owned(), (*text).to_owned())).collect();
    move |path| files.get(path).cloned().ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
}

fn macros(name: &str) -> MacrosCrate {
    MacrosCrate::new(name).unwrap()
}

#[test]
fn calls_are_found_in_every_module_with_their_place() {
    let reader = reader(&[
        (
            "src/lib.rs",
            "mod page;\nmod nested;\n#[cfg(test)] mod page_tests;\nmod inline { sqlive_compose::fragment!(C, r#\"SELECT 1\"#); }\n",
        ),
        ("src/page.rs", "\n  sqlive_compose::read!(Page, r#\"SELECT 1\"#);\nfn not_an_item() { sqlive_compose::read!(Hidden, r#\"SELECT 1\"#); }\n"),
        ("src/nested/mod.rs", "mod deep;\n"),
        ("src/nested/deep.rs", "sqlive_compose::fragment!(Deep, r#\"SELECT 1\"#);\nother::fragment!(Other, r#\"SELECT 1\"#);\n"),
    ]);
    let walked = walk(&reader, &macros("sqlive_compose")).unwrap();
    let found: Vec<_> = walked
        .calls
        .iter()
        .map(|call| {
            (
                call.name.as_str(),
                call.module.join("::"),
                call.location.to_string(),
                call.header.as_ref().unwrap().is_read(),
            )
        })
        .collect();
    assert_eq!(
        found,
        [
            ("Page", "page".to_owned(), "src/page.rs:2:3".to_owned(), true),
            ("Deep", "nested::deep".to_owned(), "src/nested/deep.rs:1:1".to_owned(), false),
            ("C", "inline".to_owned(), "src/lib.rs:4:14".to_owned(), false),
        ]
    );
    assert_eq!(
        walked.read,
        ["src/lib.rs", "src/page.rs", "src/nested/mod.rs", "src/nested/deep.rs"]
    );
}

#[test]
fn a_path_attribute_names_the_modules_file() {
    let reader = reader(&[
        ("src/lib.rs", "#[path = \"shared/one.rs\"] mod one;\n"),
        ("src/shared/one.rs", "sqlive_compose::read!(One, r#\"SELECT 1\"#);\n"),
    ]);
    let walked = walk(&reader, &macros("sqlive_compose")).unwrap();
    assert_eq!(walked.calls[0].name, "One");
}

#[test]
fn only_calls_named_through_the_macros_crate_are_found() {
    let reader = reader(&[(
        "src/lib.rs",
        "coven::read!(Named, r#\"SELECT 1\"#);\ncoven::subscribe!(NoMacro);\n\
         sqlive_compose::read!(Other, r#\"SELECT 1\"#);\n",
    )]);
    let walked = walk(&reader, &macros("coven")).unwrap();
    let names: Vec<&str> = walked.calls.iter().map(|call| call.name.as_str()).collect();
    assert_eq!(names, ["Named"]);
}

#[test]
fn a_missing_or_unparsable_file_fails() {
    let errors = walk(&reader(&[("src/lib.rs", "mod missing;\n")]), &macros("sqlive_compose"))
        .err()
        .unwrap();
    assert!(errors[0].to_string().starts_with("can't read src/missing/mod.rs"), "{}", errors[0]);
    let errors =
        walk(&reader(&[("src/lib.rs", "fn (\n")]), &macros("sqlive_compose")).err().unwrap();
    assert!(errors[0].to_string().starts_with("can't parse src/lib.rs"), "{}", errors[0]);
}

#[test]
fn a_call_whose_tokens_are_no_header_keeps_its_error() {
    let walked = walk(
        &reader(&[("src/lib.rs", "sqlive_compose::read!(Page);\n")]),
        &macros("sqlive_compose"),
    )
    .unwrap();
    assert_eq!(walked.calls[0].name, "Page");
    assert!(walked.calls[0].header.is_err());
}

#[test]
fn a_call_is_exported_when_every_module_around_it_is_pub() {
    let reader = reader(&[
        (
            "src/lib.rs",
            "sqlive_compose::read!(Root, r#\"SELECT 1\"#);\npub mod open;\nmod closed;\npub(crate) mod crate_wide;\n",
        ),
        ("src/open.rs", "sqlive_compose::read!(Open, r#\"SELECT 1\"#);\npub mod inner { sqlive_compose::read!(Inner, r#\"SELECT 1\"#); }\nmod hidden { sqlive_compose::read!(Hidden, r#\"SELECT 1\"#); }\nsqlive_compose::read!(After, r#\"SELECT 1\"#);\n"),
        ("src/closed.rs", "pub mod inner { sqlive_compose::read!(Closed, r#\"SELECT 1\"#); }\n"),
        ("src/crate_wide.rs", "sqlive_compose::read!(CrateWide, r#\"SELECT 1\"#);\n"),
    ]);
    let walked = walk(&reader, &macros("sqlive_compose")).unwrap();
    let found: Vec<_> =
        walked.calls.iter().map(|call| (call.name.as_str(), call.exported)).collect();
    assert_eq!(
        found,
        [
            ("Root", true),
            ("Open", true),
            ("Inner", true),
            ("Hidden", false),
            ("After", true),
            ("Closed", false),
            ("CrateWide", false),
        ]
    );
}
