use std::collections::BTreeSet;

use syn::visit::Visit;

use crate::crate_code::CrateCode;
use crate::syntax::RustFile;

/// Every identifier a file's syntax tree holds, and every macro still invoked.
fn contents(file: &RustFile) -> (BTreeSet<String>, BTreeSet<String>) {
    #[derive(Default)]
    struct Contents(BTreeSet<String>, BTreeSet<String>);
    impl Visit<'_> for Contents {
        fn visit_ident(&mut self, node: &syn::Ident) {
            self.0.insert(node.to_string());
        }

        fn visit_macro(&mut self, node: &syn::Macro) {
            self.1.insert(node.path.segments.last().unwrap().ident.to_string());
            syn::visit::visit_macro(self, node);
        }
    }
    let mut contents = Contents::default();
    contents.visit_file(&file.syntax);
    (contents.0, contents.1)
}

fn code(files: &[(&str, &str)]) -> CrateCode {
    CrateCode::of(
        &files.iter().map(|(path, source)| RustFile::fixture(path, source)).collect::<Vec<_>>(),
        &[],
    )
}

fn file<'a>(code: &'a CrateCode, path: &str) -> &'a RustFile {
    code.expanded.iter().find(|file| file.relative_path == path).expect("expanded file")
}

#[test]
fn exported_macros_expand_where_another_crate_uses_them() {
    let code = code(&[
        (
            "crates/app-mirror/src/lib.rs",
            r#"
            #[macro_export]
            macro_rules! mirror { ($($name:ident),*) => { $(pub struct $name;)* $crate::touch!(); }; }
            #[macro_export]
            macro_rules! touch { () => { fn touched() {} }; }
            "#,
        ),
        ("crates/app/src/lib.rs", "use app_mirror::mirror; mirror!(One, Two);"),
        ("crates/app-bridge/src/lib.rs", "#[macro_use] extern crate app_mirror; mirror!(Three);"),
    ]);
    assert!(code.unexpanded.is_empty(), "{:?}", code.unexpanded);
    let (names, macros) = contents(file(&code, "crates/app/src/lib.rs"));
    assert!(["One", "Two", "touched"].iter().all(|name| names.contains(*name)), "{names:?}");
    assert!(macros.is_empty(), "{macros:?}");
    let (names, _) = contents(file(&code, "crates/app-bridge/src/lib.rs"));
    assert!(names.contains("Three") && names.contains("touched"));
    // A definition is no code until it is used.
    assert!(file(&code, "crates/app-mirror/src/lib.rs").syntax.items.is_empty());
}

#[test]
fn textual_scope_reaches_child_modules_and_macro_use_modules() {
    let code = code(&[
        (
            "crates/app/src/lib.rs",
            r#"
            macro_rules! make { ($name:ident) => { struct $name; }; }
            mod child;
            #[macro_use] mod defs;
            fn run() { local!(); }
            "#,
        ),
        ("crates/app/src/child.rs", "make!(Child);"),
        ("crates/app/src/defs.rs", "macro_rules! local { () => { let from_defs = 1; }; }"),
    ]);
    assert!(code.unexpanded.is_empty(), "{:?}", code.unexpanded);
    assert!(contents(file(&code, "crates/app/src/child.rs")).0.contains("Child"));
    assert!(contents(file(&code, "crates/app/src/lib.rs")).0.contains("from_defs"));
}

#[test]
fn every_position_expands() {
    let code = code(&[(
        "crates/app/src/lib.rs",
        r#"
        macro_rules! methods { () => { fn generated(&self) {} }; }
        macro_rules! value { () => { 41 + 1 }; }
        macro_rules! ty { () => { Generated }; }
        macro_rules! bind { () => { let bound = 1; }; }
        struct Subject;
        impl Subject { methods!(); }
        type Alias = ty!();
        fn run() -> u8 {
            bind!();
            println!("{}", value!());
            value!()
        }
        "#,
    )]);
    assert!(code.unexpanded.is_empty(), "{:?}", code.unexpanded);
    let (names, macros) = contents(file(&code, "crates/app/src/lib.rs"));
    for name in ["generated", "Generated", "bound"] {
        assert!(names.contains(name), "{name}: {names:?}");
    }
    assert_eq!(macros, BTreeSet::from(["println".to_string()]));
    let printed =
        quote::ToTokens::to_token_stream(&file(&code, "crates/app/src/lib.rs").syntax).to_string();
    assert!(printed.contains("println ! (\"{}\" , 41 + 1)"), "{printed}");
}

#[test]
fn what_the_checker_cannot_expand_is_a_finding() {
    let code = code(&[(
        "crates/app/src/lib.rs",
        r#"
            mod hidden { macro_rules! secret { () => {}; } }
            macro_rules! only_a { (a) => {}; }
            macro_rules! value { () => { 1 }; }
            fn run() {
                secret!();
                only_a!(b);
                tokio::select! { _ = value!() => {} }
            }
            "#,
    )]);
    let found = code
        .unexpanded
        .iter()
        .map(|finding| (finding.line, finding.message.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        found,
        [
            (6, "cannot expand secret!: no definition answers its name: no definition or import in the caller's scope"),
            (7, "cannot expand only_a!: no arm matches the invocation"),
            (8, "cannot expand value!: it is inside the arguments of select!, which do not parse as code"),
        ]
    );
}
