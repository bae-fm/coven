use super::*;

const POLICY: Policy = Policy {
    construction_only_capability_types: &["SystemClock", "UuidIds"],
    composition_roots: &[("crates/coven/src/builder.rs", "Builder", "open")],
    ..Policy::EMPTY
};

fn check(path: &str, source: &str) -> Vec<CapabilityConstructionViolation> {
    find_capability_construction_violations(&[RustFile::fixture(path, source)], &POLICY)
}

#[test]
fn unit_value_paths_and_empty_literals_are_construction() {
    for (capability, expressions) in [
        (
            "SystemClock",
            [
                "SystemClock",
                "coven_foundation::clock::SystemClock.now()",
                "SystemClock {}",
                "Clock::now(&SystemClock)",
                "Arc::new(SystemClock)",
            ],
        ),
        (
            "UuidIds",
            [
                "UuidIds",
                "coven_foundation::id_source::UuidIds.new_id()",
                "UuidIds {}",
                "IdSource::new_id(&UuidIds)",
                "Arc::new(UuidIds)",
            ],
        ),
    ] {
        for expression in expressions {
            let source = format!("fn acquire() {{ let _ = {expression}; }}");
            let violations = check("crates/coven/src/runtime.rs", &source);
            assert_eq!(violations.len(), 1, "{expression}: {violations:?}");
            assert_eq!(violations[0].capability, capability);
        }
    }
}

#[test]
fn imports_types_and_injected_capabilities_do_not_construct_values() {
    let violations = check(
        "crates/coven/src/runtime.rs",
        r#"
        use coven_foundation::clock::SystemClock;
        use coven_foundation::id_source::UuidIds;
        struct Runtime { clock: SystemClock, ids: UuidIds }
        impl Runtime {
            fn run(&self) { self.clock.now(); self.ids.new_id(); }
        }
        fn run(clock: &dyn Clock, ids: &dyn IdSource) {
            clock.now(); ids.new_id();
        }
        "#,
    );
    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn composition_authority_matches_the_file_type_and_method() {
    for (path, owner, method, allowed) in [
        ("crates/coven/src/builder.rs", "Builder", "open", true),
        ("crates/coven/src/runtime.rs", "Builder", "open", false),
        ("crates/coven/src/builder.rs", "Runtime", "open", false),
        ("crates/coven/src/builder.rs", "Builder", "run", false),
    ] {
        let source = format!(
            "impl {owner} {{ fn {method}() {{ let _ = SystemClock.now(); let _ = UuidIds.new_id(); }} }}"
        );
        let violations = check(path, &source);
        assert_eq!(violations.is_empty(), allowed, "{path}: {source}");
    }
}

#[test]
fn nested_items_do_not_inherit_composition_authority() {
    let violations = check(
        "crates/coven/src/builder.rs",
        r#"
        impl Builder {
            fn open() {
                let _ = || SystemClock.now();
                fn acquire() { let _ = SystemClock.now(); }
                const IDS: UuidIds = UuidIds;
                let _ = UuidIds.new_id();
            }
        }
        "#,
    );
    assert_eq!(violations.len(), 2, "{violations:?}");
}

#[test]
fn self_does_not_hide_a_unit_constructor() {
    let violations = check(
        "crates/coven-foundation/src/clock.rs",
        "impl SystemClock { fn acquire() -> Self { Self } }",
    );
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].capability, "SystemClock");
}

#[test]
fn macro_arguments_and_bodies_are_checked() {
    for source in [
        "fn acquire() { let _ = wrap!(SystemClock.now()); }",
        "macro_rules! acquire { () => { SystemClock.now() }; }",
        "fn acquire() { let _ = wrap!(UuidIds.new_id()); }",
        "macro_rules! acquire { () => { UuidIds.new_id() }; }",
    ] {
        let violations = check("crates/coven/src/runtime.rs", source);
        assert_eq!(violations.len(), 1, "{source}: {violations:?}");
    }
}

#[test]
fn test_fixtures_may_construct_capabilities() {
    assert!(check(
        "crates/coven/src/runtime_tests.rs",
        "fn fixture() { let _ = SystemClock.now(); let _ = UuidIds.new_id(); }",
    )
    .is_empty());
    let violations = check(
        "crates/coven/src/runtime.rs",
        r#"
        #[cfg(test)] fn fixture() { let _ = SystemClock.now(); }
        #[cfg(test)] const IDS: UuidIds = UuidIds;
        #[cfg(test)] static CLOCK: SystemClock = SystemClock;
        #[cfg(test)] impl Fixture { fn new() { let _ = UuidIds.new_id(); } }
        impl Runtime {
            #[cfg(test)] fn fixture() { let _ = UuidIds.new_id(); }
        }
        #[cfg(test)] mod fixtures {
            fn fixture() { let _ = SystemClock.now(); }
        }
        "#,
    );
    assert!(violations.is_empty(), "{violations:?}");
}
