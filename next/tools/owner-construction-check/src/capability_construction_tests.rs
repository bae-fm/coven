use super::*;

const POLICY: Policy = Policy {
    capability_traits: &["Clock", "IdSource"],
    construction_only_capability_types: &["StoreDir", "AtomicFile", "ClockRef", "IdSourceRef"],
    composition_roots: &[("crates/coven/src/builder.rs", "Builder", "open")],
    ..Policy::EMPTY
};

fn check(path: &str, source: &str) -> Vec<CapabilityConstructionViolation> {
    find_capability_construction_violations(
        &[
            RustFile::fixture("crates/coven-foundation/src/capabilities.rs", CAPABILITIES),
            RustFile::fixture(path, source),
        ],
        &POLICY,
    )
}

const CAPABILITIES: &str = r#"
    trait Clock {}
    trait IdSource {}
    struct SystemClock;
    impl Clock for SystemClock {}
    struct UuidIds;
    impl IdSource for UuidIds {}
    #[cfg(feature = "test-utils")]
    mod fakes {
        struct FixedClock(u64);
        impl Clock for FixedClock {}
        struct SequentialIds { next: u64 }
        impl IdSource for SequentialIds {}
        struct ClosureClock<F>(F);
        impl<F: Fn() -> u64> Clock for ClosureClock<F> {}
    }
"#;

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
fn nested_callables_do_not_inherit_composition_authority() {
    let violations = check(
        "crates/coven/src/builder.rs",
        r#"
        impl Builder {
            fn open() {
                let _ = || SystemClock.now();
                fn acquire() { let _ = SystemClock.now(); }
                fn ids() { let _ = UuidIds; }
                let _ = UuidIds.new_id();
            }
        }
        "#,
    );
    assert_eq!(violations.len(), 2, "{violations:?}");
}

#[test]
fn self_does_not_hide_construction_in_a_runtime_method() {
    let violations = check(
        "crates/coven-foundation/src/clock.rs",
        "impl SystemClock { fn run(&self) { let _ = Self; } }",
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

fn assert_root_only(statement: &str, capability: &str) {
    for (method, allowed) in [("run", false), ("open", true)] {
        let source = format!("impl Builder {{ fn {method}() {{ {statement} }} }}");
        let violations = check("crates/coven/src/builder.rs", &source);
        if allowed {
            assert!(violations.is_empty(), "{source}: {violations:?}");
        } else {
            assert_eq!(violations.len(), 1, "{source}: {violations:?}");
            assert_eq!(violations[0].capability, capability, "{source}");
        }
    }
}

#[test]
fn every_value_form_requires_a_composition_root() {
    for (capability, expression) in [
        ("SystemClock", "SystemClock"),
        ("SystemClock", "SystemClock {}"),
        ("FixedClock", "FixedClock(17)"),
        ("FixedClock", "coven_foundation::fakes::FixedClock(17)"),
        ("ClosureClock", "ClosureClock::<F>(f)"),
        ("SequentialIds", "SequentialIds { next: 17 }"),
        ("SequentialIds", "SequentialIds { next: 17, ..other }"),
        ("SequentialIds", "SequentialIds { ..other }"),
        (
            "SequentialIds",
            "SequentialIds { next: 17, ..Default::default() }",
        ),
        ("FixedClock", "FixedClock::new(17)"),
        ("FixedClock", "FixedClock::default()"),
        ("FixedClock", "<FixedClock as Default>::default()"),
        (
            "FixedClock",
            "<FixedClock as std::default::Default>::default()",
        ),
        ("StoreDir", "StoreDir { path, id }"),
        ("AtomicFile", "AtomicFile::new(path)"),
        ("ClockRef", "ClockRef::new(clock)"),
        ("IdSourceRef", "IdSourceRef::new(ids)"),
    ] {
        for wrapped in [
            expression.to_string(),
            format!("wrap!({expression})"),
            format!("wrap!(label => {expression})"),
        ] {
            assert_root_only(&format!("let _ = {wrapped};"), capability);
        }
    }
}

#[test]
fn static_and_const_items_require_a_composition_root() {
    for keyword in ["const", "static"] {
        for (ty, initializer) in [
            ("SystemClock", "SystemClock"),
            ("SystemClock", "supplied"),
            ("Option<SystemClock>", "None"),
            ("Wrapper", "Wrapper(SystemClock)"),
        ] {
            assert_root_only(
                &format!("{keyword} CLOCK: {ty} = {initializer};"),
                "SystemClock",
            );
            let violations = check(
                "crates/coven/src/builder.rs",
                &format!("{keyword} CLOCK: {ty} = {initializer};"),
            );
            assert_eq!(
                violations.len(),
                1,
                "{keyword} {ty} = {initializer}: {violations:?}"
            );
        }
    }
}

#[test]
fn arbitrary_associated_factories_are_checked_at_the_use_site() {
    let definitions = r#"
        impl FixedClock {
            fn frozen(value: u64) -> Self { Self(value) }
            fn at(value: u64) -> FixedClock { FixedClock(value) }
            fn try_at(value: u64) -> Result<Self, Error> { Ok(Self(value)) }
            fn resolution() -> u64 { 1 }
        }
    "#;
    for expression in [
        "FixedClock::frozen(17)",
        "FixedClock::at(17)",
        "FixedClock::try_at(17)",
        "<FixedClock>::at(17)",
        "wrap!(FixedClock::at(17))",
        "wrap!(value => FixedClock::at(17))",
    ] {
        for (method, allowed) in [("open", true), ("run", false)] {
            let source = format!(
                "{definitions} impl Builder {{ fn {method}() {{ let _ = {expression}; }} }}"
            );
            let violations = check("crates/coven/src/builder.rs", &source);
            assert_eq!(
                violations.len(),
                usize::from(!allowed),
                "{source}: {violations:?}"
            );
        }
    }
    assert!(check(
        "crates/coven/src/runtime.rs",
        &format!("{definitions} fn run() {{ FixedClock::resolution(); }}")
    )
    .is_empty());
}

#[test]
fn trait_provided_associated_factories_are_checked() {
    for expression in [
        "FixedClock::frozen(17)",
        "<FixedClock as ClockFactory>::frozen(17)",
        "wrap!(FixedClock::frozen(17))",
    ] {
        for (method, allowed) in [("open", true), ("run", false)] {
            let source = format!(
                r#"
                trait ClockFactory {{ fn frozen(value: u64) -> Self {{ todo!() }} }}
                impl ClockFactory for FixedClock {{}}
                impl Builder {{ fn {method}() {{ let _ = {expression}; }} }}
            "#
            );
            let violations = check("crates/coven/src/builder.rs", &source);
            assert_eq!(
                violations.len(),
                usize::from(!allowed),
                "{source}: {violations:?}"
            );
        }
    }
}

#[test]
fn factory_authority_is_limited_to_its_result_and_body() {
    let source = r#"
        impl FixedClock {
            fn new(value: u64) -> Self {
                let _ = SystemClock;
                fn run() { let _ = FixedClock(17); }
                const CLOCK: FixedClock = FixedClock(17);
                Self(value)
            }
            fn replace(&self) -> Self { Self(17) }
        }
    "#;
    let violations = check("crates/coven-foundation/src/fakes.rs", source);
    assert_eq!(violations.len(), 4, "{violations:?}");
}

#[test]
fn factory_helpers_do_not_hide_construction_from_callers() {
    for expression in ["ClockFactory::frozen(17)", "factory::fixed(17)"] {
        for (method, allowed) in [("open", true), ("run", false)] {
            let source = format!(
                r#"
                impl ClockFactory {{ fn frozen(value: u64) -> FixedClock {{ FixedClock(value) }} }}
                mod factory {{ fn fixed(value: u64) -> FixedClock {{ FixedClock(value) }} }}
                impl Builder {{ fn {method}() {{ let _ = {expression}; }} }}
            "#
            );
            let violations = check("crates/coven/src/builder.rs", &source);
            assert_eq!(
                violations.len(),
                usize::from(!allowed),
                "{source}: {violations:?}"
            );
        }
    }
}

#[test]
fn default_cannot_make_capability_construction_implicit() {
    for definition in [
        "impl Default for FixedClock { fn default() -> Self { Self(17) } }",
        "impl std::default::Default for FixedClock { fn default() -> Self { todo!() } }",
        "impl<T> Default for ClosureClock<T> { fn default() -> Self { todo!() } }",
        "#[derive(Default)] struct FixedClock(u64);",
        "#[derive(Debug, std::default::Default)] struct FixedClock(u64);",
        "#[cfg_attr(feature = \"test-utils\", derive(Default))] struct FixedClock(u64);",
    ] {
        let violations = check("crates/coven-foundation/src/fakes.rs", definition);
        assert!(!violations.is_empty(), "accepted {definition}");
        assert!(violations
            .iter()
            .all(|violation| violation.kind == ConstructionKind::DefaultImplementation));
    }
    assert!(check(
        "crates/coven-foundation/src/values.rs",
        "#[derive(Default)] struct Value(u64);"
    )
    .is_empty());
}

#[test]
fn default_is_forbidden_even_when_declared_inside_a_root_but_test_items_are_exempt() {
    let declaration = "impl Default for FixedClock { fn default() -> Self { Self(17) } }";
    let violations = check(
        "crates/coven/src/builder.rs",
        &format!("impl Builder {{ fn open() {{ {declaration} }} }}"),
    );
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert_eq!(violations[0].kind, ConstructionKind::DefaultImplementation);
    for source in [
        format!("#[cfg(test)] {declaration}"),
        "#[cfg(test)] #[derive(Default)] struct FixedClock(u64);".to_string(),
    ] {
        assert!(check("crates/coven-foundation/src/fakes.rs", &source).is_empty());
    }
    assert!(check("crates/coven-foundation/src/clock_tests.rs", declaration).is_empty());
}

#[test]
fn associated_constants_are_not_composition_roots() {
    for source in [
        "impl FixedClock { const CLOCK: Self = Self(17); }",
        "impl Factory { const CLOCK: FixedClock = supplied; }",
        "trait Factory { const CLOCK: FixedClock = supplied; }",
    ] {
        let violations = check("crates/coven-foundation/src/fakes.rs", source);
        assert_eq!(violations.len(), 1, "{source}: {violations:?}");
        assert_eq!(violations[0].capability, "FixedClock");
    }
}

#[test]
fn trait_implementations_are_collected_across_crates_modules_and_features() {
    assert!(POLICY.capability_traits.contains(&"Clock"));
    let files = [
        RustFile::fixture(
            "crates/coven/src/builder.rs",
            "impl Builder { fn run() { let _ = other::Provider::<u64>(17); } }",
        ),
        RustFile::fixture(
            "crates/other/src/provider.rs",
            r#"
            mod nested {
                struct Provider<T>(T);
                #[cfg(feature = "test-utils")]
                impl<T: Send + Sync> coven_foundation::clock::Clock for Provider<T> {}
            }
        "#,
        ),
    ];
    let violations = find_capability_construction_violations(&files, &POLICY);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert_eq!(violations[0].capability, "Provider");
}

#[test]
fn trait_fakes_are_allowed_only_in_roots_and_test_code() {
    let source = "fn fixture() { let _ = FixedClock(17); let _ = SequentialIds { next: 1 }; }";
    for path in [
        "crates/coven/src/runtime_tests.rs",
        "crates/coven/tests/runtime.rs",
        "crates/coven/tests/support/fixture.rs",
    ] {
        assert!(check(path, source).is_empty(), "{path}");
    }
    for attribute in ["#[cfg(test)]", "#[test]"] {
        assert!(check(
            "crates/coven/src/runtime.rs",
            &format!("{attribute} {source}")
        )
        .is_empty());
    }
    for attribute in [
        "",
        "#[cfg(feature = \"test-utils\")]",
        "#[cfg(any(test, feature = \"test-utils\"))]",
    ] {
        let violations = check(
            "crates/coven/src/runtime.rs",
            &format!("{attribute} {source}"),
        );
        assert_eq!(violations.len(), 2, "{attribute}: {violations:?}");
    }
}
