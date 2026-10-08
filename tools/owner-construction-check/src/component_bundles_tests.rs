use super::*;

#[test]
fn component_bundle_constructed_only_to_be_destructured_is_rejected() {
    let files = vec![RustFile::fixture(
        "crates/coven/src/handle.rs",
        r#"
        struct ComponentBundle {
            pub(crate) first: First,
            pub(crate) second: Second,
        }

        impl ComponentBundle {
            fn new(first: First, second: Second) -> Self { Self { first, second } }
        }

        fn compose(first: First, second: Second) {
            let ComponentBundle { first, second } = ComponentBundle::new(first, second);
            use_components(first, second);
        }
        "#,
    )];

    let violations = find_component_bundle_violations(&files);
    assert_eq!(violations.len(), 1);
    assert!(violations[0].message.starts_with("ComponentBundle "));
}

#[test]
fn component_value_with_behavior_is_allowed() {
    let files = vec![RustFile::fixture(
        "crates/coven/src/handle.rs",
        r#"
        struct PreparedComponents {
            pub(crate) first: First,
            pub(crate) second: Second,
        }

        impl PreparedComponents {
            fn new(first: First, second: Second) -> Self { Self { first, second } }
            fn install(self) { use_components(self.first, self.second); }
        }
        "#,
    )];

    assert!(find_component_bundle_violations(&files).is_empty());
}

#[test]
fn a_bundle_destructured_inside_a_macro_call_is_rejected() {
    let files = vec![RustFile::fixture(
        "crates/coven/src/handle.rs",
        r#"
        struct ComponentBundle {
            pub(crate) first: First,
            pub(crate) second: Second,
        }

        impl ComponentBundle {
            fn new(first: First, second: Second) -> Self { Self { first, second } }
        }

        fn compose(first: First, second: Second) {
            run! {
                let ComponentBundle { first, second } = ComponentBundle::new(first, second);
                use_components(first, second);
            }
        }
        "#,
    )];

    assert_eq!(find_component_bundle_violations(&files).len(), 1);
}
