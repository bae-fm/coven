use super::*;

fn find_capability_construction_violations(files: &[RustFile], policy: &Policy) -> Vec<Finding> {
    super::find_capability_construction_violations(
        files,
        policy,
        &OwnerGraph::collect(files, policy),
    )
}

const REMEDY: &str = "construct owners and capabilities explicitly at the listed roots; inject them elsewhere; do not implement or derive Default";

fn findings(path: &str, sites: &[(usize, &str, &str)]) -> Vec<Finding> {
    sites
        .iter()
        .map(|(line, caller, capability)| {
            Finding::new(
                path,
                *line,
                format!("{caller} constructs capability {capability} outside a composition root"),
                REMEDY,
            )
        })
        .collect()
}

const POLICY: Policy = Policy {
    capability_traits: &["Clock", "IdSource"],
    construction_only_capability_types: &["StoreDir", "AtomicFile", "ClockRef", "IdSourceRef"],
    composition_roots: &[("crates/coven/src/builder.rs", "Builder", "open")],
    ..Policy::EMPTY
};

fn check(path: &str, source: &str) -> Vec<Finding> {
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
fn test_factories_do_not_assign_capability_types_to_production_values() {
    let source = "fn work(file: File, directory: Path) {\n    consume(file);\n    consume(directory);\n    let _ = AtomicFile::new(path);\n}";
    let files = [
        RustFile::fixture("crates/coven/src/work.rs", source),
        RustFile::fixture(
            "crates/coven/src/values.rs",
            "fn file() -> Value { todo!() } fn read() { file(); }",
        ),
        RustFile::fixture(
            "crates/coven/src/work_tests.rs",
            "fn file() -> AtomicFile { todo!() } fn directory() -> StoreDir { todo!() }",
        ),
    ];
    let violations = find_capability_construction_violations(&files, &POLICY);
    assert_eq!(
        violations,
        findings(
            "crates/coven/src/work.rs",
            &[(4, "<free>::work", "AtomicFile")]
        )
    );
}

#[test]
fn test_only_factories_do_not_change_production_factory_results() {
    let source = r#"
        #[cfg(test)] fn file() -> AtomicFile { todo!() }
        #[cfg(not(test))] fn file() -> Value { todo!() }
        fn read() { file(); }
        #[cfg(test)] mod fixtures {
            fn directory() -> StoreDir { todo!() }
            impl Factory { fn build() -> AtomicFile { todo!() } }
        }
        impl Factory {
            #[cfg(test)] fn build() -> AtomicFile { todo!() }
            #[cfg(not(test))]
            fn build() -> Value { todo!() }
        }
        fn work(file: File, directory: Path) {
            consume(file);
            consume(directory);
            Factory::build();
        }
    "#;
    assert!(check("crates/coven/src/work.rs", source).is_empty());
}

#[test]
fn local_bindings_shadow_factory_names_but_factory_references_still_count() {
    let source = r#"
        fn file() -> AtomicFile { AtomicFile::new(path) }
        fn supplied(file: File) { consume(file); }
        fn work() {
            { let file = supplied_value; consume(file); }
            let acquire = file;
            let file = supplied_value;
            consume(file);
            let _ = |file| consume(file);
            for file in files { consume(file); }
            match input { Some(file) => consume(file), None => {} }
        }
    "#;
    let violations = check("crates/coven/src/work.rs", source);
    assert_eq!(
        violations,
        findings(
            "crates/coven/src/work.rs",
            &[(6, "<free>::work", "AtomicFile")]
        )
    );
}

#[test]
fn binding_scopes_do_not_hide_factory_references_after_they_end() {
    for statement in [
        "{ let file = supplied; consume(file); }",
        "let _ = |file| consume(file);",
        "for file in files { consume(file); }",
        "match input { Some(file) => consume(file), None => {} }",
        "if let Some(file) = input { consume(file); }",
        "while let Some(file) = input { consume(file); }",
    ] {
        let source = format!("fn file() -> AtomicFile {{ AtomicFile::new(path) }}\nfn work() {{\n{statement}\nlet factory = file;\n}}");
        let violations = check("crates/coven/src/work.rs", &source);
        assert_eq!(
            violations,
            findings(
                "crates/coven/src/work.rs",
                &[(4, "<free>::work", "AtomicFile")]
            ),
            "{source}"
        );
    }
    let source = "fn file() -> AtomicFile { AtomicFile::new(path) }\nfn work() {\nlet file = file;\nconsume(file);\n}";
    let violations = check("crates/coven/src/work.rs", source);
    assert_eq!(
        violations,
        findings(
            "crates/coven/src/work.rs",
            &[(3, "<free>::work", "AtomicFile")]
        )
    );
}

#[test]
fn injected_owner_factory_authority_is_explicit_and_limited_to_its_product() {
    const DERIVED: Policy = Policy {
        capability_types: &["StoreDir"],
        capability_factories: &[(
            "crates/coven-foundation/src/directory.rs",
            "StoreDir",
            "file",
            "AtomicFile",
        )],
        ..POLICY
    };
    let source = r#"
        impl StoreDir {
            fn file(&self) -> AtomicFile { AtomicFile::new(self.path()) }
            fn unrelated(&self) -> AtomicFile { AtomicFile::new(path) }
        }
        fn use_injected(dir: &StoreDir) {
            let _ = dir.file();
            let _ = StoreDir::file(dir);
        }
    "#;
    for (path, expected_lines) in [
        ("crates/coven-foundation/src/directory.rs", vec![4]),
        ("crates/coven-foundation/src/other.rs", vec![3, 4]),
    ] {
        let violations =
            find_capability_construction_violations(&[RustFile::fixture(path, source)], &DERIVED);
        assert_eq!(
            violations.iter().map(|v| v.line).collect::<Vec<_>>(),
            expected_lines,
            "{violations:?}"
        );
    }
    let source = r#"
        impl StoreDir {
            fn file(&self) -> AtomicFile {
                let _ = StoreDir::new(path);
                fn nested() { let _ = AtomicFile::new(path); }
                AtomicFile::new(self.path())
            }
        }
    "#;
    let violations = find_capability_construction_violations(
        &[RustFile::fixture(
            "crates/coven-foundation/src/directory.rs",
            source,
        )],
        &DERIVED,
    );
    assert_eq!(
        violations,
        findings(
            "crates/coven-foundation/src/directory.rs",
            &[
                (4, "StoreDir::file", "StoreDir"),
                (5, "<free>::nested", "AtomicFile"),
            ]
        )
    );
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
            assert_eq!(
                violations,
                findings(
                    "crates/coven/src/runtime.rs",
                    &[(1, "<free>::acquire", capability)]
                ),
                "{expression}"
            );
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
    assert_eq!(
        violations,
        findings(
            "crates/coven/src/builder.rs",
            &[
                (5, "<free>::acquire", "SystemClock"),
                (6, "<free>::ids", "UuidIds")
            ]
        )
    );
}

#[test]
fn self_does_not_hide_construction_in_a_runtime_method() {
    let violations = check(
        "crates/coven-foundation/src/clock.rs",
        "impl SystemClock { fn run(&self) { let _ = Self; } }",
    );
    assert_eq!(
        violations,
        findings(
            "crates/coven-foundation/src/clock.rs",
            &[(1, "SystemClock::run", "SystemClock")]
        )
    );
}

#[test]
fn macro_arguments_and_bodies_are_checked() {
    for (caller, capability, source) in [
        (
            "<free>::acquire",
            "SystemClock",
            "fn acquire() { let _ = wrap!(SystemClock.now()); }",
        ),
        (
            "<free>::<item>",
            "SystemClock",
            "macro_rules! acquire { () => { SystemClock.now() }; }",
        ),
        (
            "<free>::acquire",
            "UuidIds",
            "fn acquire() { let _ = wrap!(UuidIds.new_id()); }",
        ),
        (
            "<free>::<item>",
            "UuidIds",
            "macro_rules! acquire { () => { UuidIds.new_id() }; }",
        ),
    ] {
        let violations = check("crates/coven/src/runtime.rs", source);
        assert_eq!(
            violations,
            findings("crates/coven/src/runtime.rs", &[(1, caller, capability)]),
            "{source}"
        );
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
            assert_eq!(
                violations,
                findings(
                    "crates/coven/src/builder.rs",
                    &[(1, "Builder::run", capability)]
                ),
                "{source}"
            );
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
                violations,
                findings(
                    "crates/coven/src/builder.rs",
                    &[(1, "<free>::<item>", "SystemClock")]
                ),
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
                violations,
                findings(
                    "crates/coven/src/builder.rs",
                    if allowed {
                        &[]
                    } else {
                        &[(8, "Builder::run", "FixedClock")]
                    }
                ),
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
                violations,
                findings(
                    "crates/coven/src/builder.rs",
                    if allowed {
                        &[]
                    } else {
                        &[(4, "Builder::run", "FixedClock")]
                    }
                ),
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
    assert_eq!(
        violations,
        findings(
            "crates/coven-foundation/src/fakes.rs",
            &[
                (4, "FixedClock::new", "SystemClock"),
                (5, "<free>::run", "FixedClock"),
                (6, "FixedClock::new", "FixedClock"),
                (9, "FixedClock::replace", "FixedClock")
            ]
        )
    );
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
                violations,
                findings(
                    "crates/coven/src/builder.rs",
                    if allowed {
                        &[]
                    } else {
                        &[(4, "Builder::run", "FixedClock")]
                    }
                ),
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
        assert!(violations.iter().all(|violation| violation
            .message
            .starts_with("implements Default for capability")));
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
    assert_eq!(
        violations,
        [Finding::new("crates/coven/src/builder.rs", 1,
            "implements Default for capability FixedClock, which permits implicit construction through Default::default()", REMEDY)]
    );
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
    for (caller, source) in [
        (
            "FixedClock::<item>",
            "impl FixedClock { const CLOCK: Self = Self(17); }",
        ),
        (
            "Factory::<item>",
            "impl Factory { const CLOCK: FixedClock = supplied; }",
        ),
        (
            "Factory::<item>",
            "trait Factory { const CLOCK: FixedClock = supplied; }",
        ),
    ] {
        let violations = check("crates/coven-foundation/src/fakes.rs", source);
        assert_eq!(
            violations,
            findings(
                "crates/coven-foundation/src/fakes.rs",
                &[(1, caller, "FixedClock")]
            ),
            "{source}"
        );
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
    assert_eq!(
        violations,
        findings(
            "crates/coven/src/builder.rs",
            &[(1, "Builder::run", "Provider")]
        )
    );
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
        assert_eq!(
            violations,
            findings(
                "crates/coven/src/runtime.rs",
                &[
                    (1, "<free>::fixture", "FixedClock"),
                    (1, "<free>::fixture", "SequentialIds")
                ]
            ),
            "{attribute}"
        );
    }
}

#[test]
fn named_free_roots_construct_capabilities_without_authorizing_other_functions() {
    let policy = Policy {
        composition_roots: &[("crates/coven/src/bootstrap.rs", "<free>", "restore")],
        construction_only_capability_types: &["Keychain"],
        ..Policy::EMPTY
    };
    let files = [RustFile::fixture(
        "crates/coven/src/bootstrap.rs",
        r#"
        fn restore() { let _ = Keychain::new(); }
        fn unrelated() { let _ = Keychain::new(); }
    "#,
    )];
    let violations = find_capability_construction_violations(&files, &policy);
    assert_eq!(
        violations,
        findings(
            "crates/coven/src/bootstrap.rs",
            &[(3, "<free>::unrelated", "Keychain")]
        )
    );
}
