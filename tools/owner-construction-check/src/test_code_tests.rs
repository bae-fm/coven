use std::collections::BTreeSet;

use syn::visit::Visit;

use super::*;

/// Every identifier a file's syntax tree holds.
fn identifiers(file: &RustFile) -> BTreeSet<String> {
    struct Identifiers(BTreeSet<String>);
    impl Visit<'_> for Identifiers {
        fn visit_ident(&mut self, node: &syn::Ident) {
            self.0.insert(node.to_string());
        }
    }
    let mut identifiers = Identifiers(BTreeSet::new());
    identifiers.visit_file(&file.syntax);
    identifiers.0
}

#[test]
fn test_sources_are_dropped_whole() {
    let mut reached = RustFile::fixture("crates/app/src/helpers.rs", "fn helper() {}");
    reached.test_module = true;
    let files = [
        RustFile::fixture("crates/app/src/lib.rs", "fn kept() {}"),
        RustFile::fixture("crates/app/src/lib_tests.rs", "fn case() {}"),
        RustFile::fixture("crates/app/tests/flow.rs", "fn flow() {}"),
        RustFile::fixture("crates/app/src/only.rs", "#![cfg(test)] fn only() {}"),
        reached,
    ];
    let kept =
        production_code(&files).into_iter().map(|file| file.relative_path).collect::<Vec<_>>();
    assert_eq!(kept, ["crates/app/src/lib.rs"]);
}

#[test]
fn test_only_parts_are_removed_and_the_rest_kept() {
    let file = RustFile::fixture(
        "crates/app/src/lib.rs",
        r#"
        #[cfg(test)] use fixtures::Fake;
        #[cfg(test)] mod checks { fn check() {} }
        #[cfg(any(test, feature = "test-utils"))] fn shared_fake() {}
        mod work {
            #[test] fn case() {}
            fn run() {}
        }
        struct Worker { kept: u8, #[cfg(test)] probe: u8 }
        struct Pair(u8, #[cfg(test)] Probe);
        enum State { Running, #[cfg(test)] Probed }
        impl Worker {
            #[cfg(test)] fn probe(&self) {}
            fn run(&self) {
                #[cfg(test)] let probe = 1;
                #[cfg(test)] check();
                #[cfg(test)] { more(); }
                let worker = Worker { kept: 1, #[cfg(test)] probe: 2 };
                match state { State::Running => {} #[cfg(test)] State::Probed => {} }
            }
        }
        trait Work { #[cfg(test)] fn probe(&self); fn run(&self); }
        "#,
    );
    let code = production_code(&[file]);
    let identifiers = identifiers(&code[0]);
    for absent in ["Fake", "checks", "case", "probe", "Probe", "Probed", "check", "more"] {
        assert!(!identifiers.contains(absent), "kept {absent}");
    }
    for present in ["shared_fake", "run", "kept", "Running", "Work", "worker"] {
        assert!(identifiers.contains(present), "removed {present}");
    }
}
