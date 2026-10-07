use super::*;

const FIRST: &str = "crates/first/src/lib.rs";
const SECOND: &str = "crates/second/src/lib.rs";

fn violations(first: &str, second: &str, policy: &Policy) -> Vec<UniqueTypeNameViolation> {
    find_unique_type_name_violations(
        &[
            RustFile::fixture(FIRST, first),
            RustFile::fixture(SECOND, second),
        ],
        policy,
    )
}

fn duplicate(name: &str) -> Vec<UniqueTypeNameViolation> {
    vec![UniqueTypeNameViolation {
        name: name.to_string(),
        paths: vec![FIRST.to_string(), SECOND.to_string()],
    }]
}

#[test]
fn every_type_bearing_policy_row_requires_unique_names() {
    let policies = [
        Policy {
            capability_types: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            capability_traits: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            construction_only_capability_types: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            non_owner_types: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            borrowed_facade_types: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            root_owner_types: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            task_types: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            internal_dependency_types: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            always_forbidden_returns: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            closed_session_types: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            field_capability_types: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            unexported_capability_types: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            exportable_capability_outputs: &["Subject"],
            ..Policy::EMPTY
        },
        Policy {
            composition_roots: &[(FIRST, "Subject", "open")],
            ..Policy::EMPTY
        },
        Policy {
            lifetime_authorities: &[("Subject", "Authority")],
            ..Policy::EMPTY
        },
        Policy {
            lifetime_authorities: &[("Service", "Subject")],
            ..Policy::EMPTY
        },
        Policy {
            capability_factories: &[(FIRST, "Subject", "build", "Product")],
            ..Policy::EMPTY
        },
        Policy {
            capability_factories: &[(FIRST, "Factory", "build", "Subject")],
            ..Policy::EMPTY
        },
        Policy {
            raw_provider_operations: &[("Subject", &["read"])],
            ..Policy::EMPTY
        },
        Policy {
            derived_services: &[("Subject", &["Source"])],
            ..Policy::EMPTY
        },
        Policy {
            derived_services: &[("Product", &["Source", "Subject"])],
            ..Policy::EMPTY
        },
    ];
    for policy in policies {
        assert_eq!(
            violations("struct Subject;", "struct Subject;", &policy),
            duplicate("Subject"),
            "policy rows: {:?}",
            policy.named_types(),
        );
    }
}

#[test]
fn all_declaration_kinds_count_including_nested_modules() {
    const POLICY: Policy = Policy {
        capability_types: &["Subject"],
        ..Policy::EMPTY
    };
    let declarations = [
        "struct Subject;",
        "enum Subject { Value }",
        "union Subject { value: u64 }",
        "trait Subject {}",
        "type Subject = u64;",
    ];
    for first in declarations {
        for second in declarations {
            assert_eq!(
                violations(
                    first,
                    &format!("mod outer {{ mod inner {{ {second} }} }}"),
                    &POLICY
                ),
                duplicate("Subject"),
                "{first} / {second}",
            );
        }
    }
}

#[test]
fn capability_trait_implementations_need_no_explicit_type_row() {
    const POLICY: Policy = Policy {
        capability_traits: &["Clock"],
        ..Policy::EMPTY
    };
    let source = r#"
        trait Clock {}
        #[cfg(feature = "test-utils")]
        mod fakes {
            struct SuppliedClock<T>(T);
            impl<T> foundation::Clock for SuppliedClock<T> {}
        }
    "#;
    assert_eq!(
        violations(source, "struct SuppliedClock;", &POLICY),
        duplicate("SuppliedClock"),
    );
}

#[test]
fn inferred_owners_include_aliases_enums_unions_and_transitive_holders() {
    const POLICY: Policy = Policy {
        capability_types: &["Clock"],
        ..Policy::EMPTY
    };
    for owner in [
        "struct Worker { clock: Box<dyn Clock> }",
        "enum Worker { Active(Box<dyn Clock>) }",
        "union Worker { clock: *const dyn Clock }",
        "type Worker = Box<dyn Clock>;",
        "struct Inner { clock: Box<dyn Clock> } struct Worker { inner: Inner }",
    ] {
        let source = format!("trait Clock {{}} {owner}");
        assert_eq!(
            violations(&source, "struct Worker;", &POLICY),
            duplicate("Worker"),
            "{owner}",
        );
        // Input order cannot let a plain value overwrite an owner's fields.
        assert_eq!(
            violations("struct Worker;", &source, &POLICY),
            duplicate("Worker"),
            "{owner}",
        );
    }
}

#[test]
fn test_only_items_and_their_nested_declarations_do_not_count() {
    const POLICY: Policy = Policy {
        capability_types: &["Subject"],
        ..Policy::EMPTY
    };
    for source in [
        "#[cfg(test)] struct Subject;",
        "#[cfg(test)] enum Subject {}",
        "#[cfg(test)] union Subject { value: u64 }",
        "#[cfg(test)] trait Subject {}",
        "#[cfg(test)] type Subject = u64;",
        "#[cfg(all(test, unix))] mod nested { struct Subject; }",
        "#[test] fn fixture() { struct Subject; }",
        "#[tokio::test] async fn fixture() { struct Subject; }",
        "#[cfg(test)] impl Fixture { fn fixture() { struct Subject; } }",
        "impl Fixture { #[cfg(test)] fn fixture() { struct Subject; } }",
        "impl Fixture { #[cfg(test)] const VALUE: () = { struct Subject; }; }",
        "trait Fixture { #[cfg(test)] fn fixture() { struct Subject; } }",
        "trait Fixture { #[cfg(test)] const VALUE: () = { struct Subject; }; }",
        "#[cfg(test)] const VALUE: () = { struct Subject; };",
        "#[cfg(test)] static VALUE: () = { struct Subject; };",
        "#[cfg(test)] declare! { struct Subject; }",
        "impl Fixture { #[cfg(test)] declare! { fn fixture() { struct Subject; } } }",
        "trait Fixture { #[cfg(test)] declare! { fn fixture() { struct Subject; } } }",
        "#![cfg(test)]\nstruct Subject;",
    ] {
        assert!(
            violations("struct Subject;", source, &POLICY).is_empty(),
            "{source}"
        );
    }
}

#[test]
fn test_sources_do_not_count() {
    const POLICY: Policy = Policy {
        capability_types: &["Subject"],
        ..Policy::EMPTY
    };
    for path in [
        "crates/second/src/lib_tests.rs",
        "crates/second/tests/integration.rs",
        "crates/second/tests/support/fixture.rs",
    ] {
        assert!(
            find_unique_type_name_violations(
                &[
                    RustFile::fixture(FIRST, "struct Subject;"),
                    RustFile::fixture(path, "struct Subject;"),
                ],
                &POLICY,
            )
            .is_empty(),
            "{path}"
        );
    }
}

#[test]
fn feature_gates_that_allow_production_count() {
    const POLICY: Policy = Policy {
        capability_types: &["Subject"],
        ..Policy::EMPTY
    };
    for attribute in [
        "#[cfg(feature = \"test-utils\")]",
        "#[cfg(any(test, feature = \"test-utils\"))]",
        "#[cfg(not(test))]",
        "#[cfg(unix)]",
    ] {
        for source in [
            format!("{attribute} struct Subject;"),
            format!("{attribute} mod nested {{ struct Subject; }}"),
        ] {
            assert_eq!(
                violations("struct Subject;", &source, &POLICY),
                duplicate("Subject"),
                "{source}"
            );
        }
    }
}

#[test]
fn declarations_inside_functions_and_macro_arguments_count() {
    const POLICY: Policy = Policy {
        capability_types: &["Subject"],
        ..Policy::EMPTY
    };
    for source in [
        "fn run() { struct Subject; }",
        "declare! { struct Subject; }",
    ] {
        assert_eq!(
            violations("struct Subject;", source, &POLICY),
            duplicate("Subject"),
            "{source}"
        );
    }
}

#[test]
fn repeated_declarations_in_one_file_are_not_collapsed_before_counting() {
    const POLICY: Policy = Policy {
        capability_types: &["Subject"],
        ..Policy::EMPTY
    };
    assert_eq!(
        violations(
            "mod first { struct Subject; } mod second { struct Subject; }",
            "",
            &POLICY
        ),
        vec![UniqueTypeNameViolation {
            name: "Subject".into(),
            paths: vec![FIRST.into()]
        }],
    );
}

#[test]
fn every_declaring_file_is_reported_in_path_order() {
    const POLICY: Policy = Policy {
        capability_types: &["Subject"],
        ..Policy::EMPTY
    };
    let third = "crates/third/src/lib.rs";
    assert_eq!(
        find_unique_type_name_violations(
            &[
                RustFile::fixture(third, "struct Subject;"),
                RustFile::fixture(SECOND, "struct Subject;"),
                RustFile::fixture(FIRST, "struct Subject;"),
            ],
            &POLICY,
        ),
        vec![UniqueTypeNameViolation {
            name: "Subject".into(),
            paths: vec![FIRST.into(), SECOND.into(), third.into()]
        }],
    );
}

#[test]
fn imports_and_reexports_are_not_declarations() {
    const POLICY: Policy = Policy {
        capability_types: &["Subject"],
        ..Policy::EMPTY
    };
    assert!(violations("struct Subject;", "pub use first::Subject;", &POLICY).is_empty());
}
