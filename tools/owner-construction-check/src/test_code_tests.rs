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
        production_code(&files, &[]).into_iter().map(|file| file.relative_path).collect::<Vec<_>>();
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
    let code = production_code(&[file], &[]);
    let identifiers = identifiers(&code[0]);
    for absent in ["Fake", "checks", "case", "probe", "Probe", "Probed", "check", "more"] {
        assert!(!identifiers.contains(absent), "kept {absent}");
    }
    for present in ["shared_fake", "run", "kept", "Running", "Work", "worker"] {
        assert!(identifiers.contains(present), "removed {present}");
    }
}

/// Test code is each test file whole, and the test-only items of every
/// other file, inside inline modules and impls too.
#[test]
fn test_code_is_test_files_and_test_only_items() {
    let files = [
        RustFile::fixture(
            "crates/app/src/lib.rs",
            r#"
            fn kept() {}
            #[cfg(test)] fn fixture() {}
            #[cfg(any(test, feature = "test-utils"))] fn shared_fake() {}
            mod work { #[test] fn case() {} fn run() {} }
            impl Worker { #[cfg(test)] fn probe(&self) {} fn run(&self) {} }
            "#,
        ),
        RustFile::fixture("crates/app/src/lib_tests.rs", "fn whole() {}"),
    ];
    let code = test_code(&files, &[]);
    let production = identifiers(&code[0]);
    for absent in ["kept", "shared_fake", "run"] {
        assert!(!production.contains(absent), "kept {absent}");
    }
    for present in ["fixture", "case", "probe", "Worker"] {
        assert!(production.contains(present), "removed {present}");
    }
    assert!(identifiers(&code[1]).contains("whole"));
}

/// Code that compiles only under a feature the policy lists as test support
/// is test code, as `cfg(test)` code is; a feature it doesn't list compiles
/// into production.
#[test]
fn code_gated_on_a_listed_test_support_feature_is_test_code() {
    let files = [
        RustFile::fixture(
            "crates/app/src/lib.rs",
            r#"
            #[cfg(feature = "test-utils")] fn fake() {}
            #[cfg(any(test, feature = "test-utils"))] fn shared_fake() {}
            #[cfg(feature = "cloud")] fn upload() {}
            #[cfg(any(feature = "test-utils", feature = "cloud"))] fn either() {}
            struct Worker { kept: u8, #[cfg(feature = "test-utils")] probe: u8 }
            "#,
        ),
        RustFile::fixture(
            "crates/app/src/fakes.rs",
            r#"#![cfg(feature = "test-utils")] fn fake() {}"#,
        ),
    ];
    let features = ["test-utils".to_string()];
    let production = production_code(&files, &features);
    assert_eq!(
        production.iter().map(|file| file.relative_path.as_str()).collect::<Vec<_>>(),
        ["crates/app/src/lib.rs"]
    );
    let kept = identifiers(&production[0]);
    for absent in ["fake", "shared_fake", "probe"] {
        assert!(!kept.contains(absent), "kept {absent}");
    }
    for present in ["upload", "either", "kept"] {
        assert!(kept.contains(present), "removed {present}");
    }
    let tests = test_code(&files, &features);
    let gated = identifiers(&tests[0]);
    for present in ["fake", "shared_fake"] {
        assert!(gated.contains(present), "left out {present}");
    }
    for absent in ["upload", "either"] {
        assert!(!gated.contains(absent), "took {absent}");
    }
    assert!(identifiers(&tests[1]).contains("fake"));
    let unlisted = identifiers(&production_code(&files, &[])[0]);
    assert!(unlisted.contains("fake") && unlisted.contains("probe"));
}

#[test]
fn attributes_a_cfg_attr_adds_only_in_tests_are_test_code() {
    let file = RustFile::fixture(
        "crates/app/src/lib.rs",
        r#"
        #[cfg_attr(feature = "test-utils", derive(Default))] struct Fake(u8);
        #[cfg_attr(test, derive(Hash))] struct Probe { #[cfg_attr(test, serde(skip))] kept: u8 }
        #[cfg_attr(feature = "cloud", derive(Clone))] struct Upload;
        impl Upload { #[cfg_attr(any(test, feature = "test-utils"), inline)] fn send() {} }
        "#,
    );
    // Attribute arguments are tokens, which a syntax tree's identifiers
    // leave out, so the test prints the kept syntax back as text and
    // searches that.
    let text = |features: &[String]| {
        quote::ToTokens::to_token_stream(
            &production_code(std::slice::from_ref(&file), features)[0].syntax,
        )
        .to_string()
    };
    let kept = text(&["test-utils".to_string()]);
    for absent in ["Default", "Hash", "serde", "inline"] {
        assert!(!kept.contains(absent), "kept {absent}: {kept}");
    }
    for present in ["Fake", "Probe", "kept", "Clone", "send"] {
        assert!(kept.contains(present), "removed {present}: {kept}");
    }
    let unlisted = text(&[]);
    assert!(unlisted.contains("Default") && !unlisted.contains("Hash"), "{unlisted}");
}
