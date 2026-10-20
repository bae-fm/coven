use super::*;
use crate::capability_construction::find_capability_construction_violations;
use crate::finding::Finding;

const PATH: &str = "crates/coven-sync/src/fixture.rs";
const POLICY: Policy = Policy {
    capability_types: &["Clock"],
    task_types: &["Pass"],
    composition_roots: &[(PATH, "Worker", "new"), (PATH, "<free>", "open")],
    ..Policy::EMPTY
};
fn constructions(source: &str, policy: &Policy) -> Vec<Finding> {
    let files = [RustFile::fixture(PATH, source)];
    find_capability_construction_violations(&files, policy, &OwnerGraph::collect(&files, policy))
}
const WORKER: &str = "struct Worker { clock: Clock } impl Worker { fn new(clock: Clock) -> Self { Self { clock } } }";

fn finding(caller: &str, owner: &str) -> Finding {
    Finding::new(PATH, 1,
        format!("{caller} constructs owner {owner} outside a composition root"),
        "construct owners and capabilities explicitly at the listed roots; inject them elsewhere; do not implement or derive Default")
}

#[test]
fn every_construction_site_requires_a_list_entry() {
    for expression in [
        "Worker { clock }",
        "Worker::new(clock)",
        "crate::fixture::Worker::new(clock)",
        "wrap!(Worker::new(clock))",
        "wrap!(label => Worker { clock })",
    ] {
        for (caller, callable) in [
            ("<free>::run", format!("fn run(clock: Clock) {{ let _ = {expression}; }}")),
            ("Unrelated::run", format!("struct Unrelated; impl Unrelated {{ fn run(clock: Clock) {{ let _ = {expression}; }} }}")),
            ("Parent::new", format!("struct Parent; impl Parent {{ fn new(clock: Clock) -> Self {{ let _ = {expression}; todo!() }} }}")),
        ] {
            let found = constructions(&format!("{WORKER} {callable}"), &POLICY);
            assert_eq!(found, [finding(caller, "Worker")], "{callable}");
        }
        assert!(constructions(
            &format!("{WORKER} fn open(clock: Clock) {{ let _ = {expression}; }}"),
            &POLICY
        )
        .is_empty());
    }
}

#[test]
fn constructors_themselves_require_entries_including_inner_types() {
    for name in ["Worker", "WorkerInner"] {
        let source = format!("struct {name} {{ clock: Clock }} impl {name} {{ fn new(clock: Clock) -> Self {{ Self {{ clock }} }} }}");
        let policy = Policy {
            composition_roots: &[],
            ..POLICY
        };
        let found = constructions(&source, &policy);
        assert_eq!(found, [finding(&format!("{name}::new"), name)]);
    }
}

#[test]
fn returning_an_owner_does_not_grant_factory_authority() {
    for (caller, source) in [
        (
            "<free>::build",
            "fn build(clock: Clock) -> Worker { Worker { clock } }",
        ),
        (
            "Factory::build",
            "struct Factory; impl Factory { fn build(clock: Clock) -> Worker { Worker { clock } } }",
        ),
        (
            "Factory::build",
            "struct Factory; impl Factory { fn build(&self, clock: Clock) -> Worker { Worker { clock } } }",
        ),
    ] {
        assert_eq!(
            constructions(&format!("{WORKER} {source}"), &POLICY),
            [finding(caller, "Worker")]
        );
    }
}

#[test]
fn free_and_associated_factories_cannot_hide_construction() {
    for expression in [
        "build(clock)",
        "factory::build(clock)",
        "Factory::build(clock)",
        "self.build(clock)",
        "wrap!(self.build(clock))",
    ] {
        let source = format!("{WORKER} fn build(clock: Clock) -> Worker {{ todo!() }} mod factory {{ pub use super::build; }} struct Factory; impl Factory {{ fn build(&self, clock: Clock) -> Worker {{ todo!() }} fn run(&self, clock: Clock) {{ let _ = {expression}; }} }}");
        assert_eq!(
            constructions(&source, &POLICY),
            [finding("Factory::run", "Worker")],
            "{source}"
        );
    }
}

#[test]
fn nested_functions_do_not_inherit_root_authority() {
    let source =
        format!("{WORKER} fn open() {{ fn hidden(clock: Clock) {{ Worker::new(clock); }} }}");
    assert_eq!(
        constructions(&source, &POLICY),
        [finding("<free>::hidden", "Worker")]
    );
}

#[test]
fn tasks_compose_but_do_not_construct_owners() {
    let source = format!("{WORKER} struct Pass {{ clock: Clock }} impl Pass {{ fn new(clock: Clock) -> Self {{ Self {{ clock }} }} fn run(&self) {{ Worker::new(clock); }} }}");
    let found = constructions(&source, &POLICY);
    assert_eq!(found, [finding("Pass::run", "Worker")]);
}

#[test]
fn tests_do_not_make_production_values_into_owners() {
    let files = [
        RustFile::fixture(
            PATH,
            "struct Value; #[cfg(test)] struct Hidden { clock: Clock }",
        ),
        RustFile::fixture(
            "crates/coven-sync/src/fixture_tests.rs",
            "struct Value { clock: Clock }",
        ),
    ];
    assert!(OwnerGraph::collect(&files, &POLICY).owners.is_empty());
}

#[test]
fn aliases_enums_unions_and_fields_share_one_inference() {
    let files = [RustFile::fixture(PATH, "struct A { clock: Clock } enum B { Some(A) } type C = B; union D { value: C } struct E(D);")];
    let graph = OwnerGraph::collect(&files, &POLICY);
    assert_eq!(
        graph.owners,
        ["A", "B", "C", "D", "E"].map(str::to_string).into()
    );
    assert!(graph.retained("E").contains("Clock"));
}

#[test]
fn enum_variants_named_like_owners_are_not_owner_construction() {
    let source =
        format!("{WORKER} enum Error {{ Worker(()) }} fn run() {{ Error::Worker(error); }}");
    assert!(constructions(&source, &POLICY).is_empty());
}

#[test]
fn enum_variants_and_aliases_are_owner_construction() {
    for name in ["Choice", "Alias"] {
        for value in ["Named { clock }", "Tuple(clock)", "Empty"] {
            let source = format!("enum Choice {{ Named {{ clock: Clock }}, Tuple(Clock), Empty }} type Alias = Choice; fn run(clock: Clock) {{ let _ = {name}::{value}; }}");
            let found = constructions(&source, &POLICY);
            assert_eq!(found, [finding("<free>::run", name)], "{source}");
        }
    }
}

#[test]
fn nested_and_macro_declared_owners_share_the_inference() {
    for source in [
        "fn run(clock: Clock) { struct Hidden { clock: Clock } let _ = Hidden { clock }; }",
        "declare! { struct Hidden { clock: Clock } } fn run(clock: Clock) { Hidden { clock }; }",
    ] {
        let found = constructions(source, &POLICY);
        assert_eq!(found, [finding("<free>::run", "Hidden")], "{source}");
    }
}

#[test]
fn same_named_free_functions_in_different_crates_do_not_share_results() {
    let files = [
        RustFile::fixture(
            "crates/other/src/lib.rs",
            "struct Joining { clock: Clock } fn prepare() -> Joining { todo!() }",
        ),
        RustFile::fixture(PATH, "fn prepare() {} fn replace() { prepare(); }"),
    ];
    let found = find_capability_construction_violations(
        &files,
        &POLICY,
        &OwnerGraph::collect(&files, &POLICY),
    );
    assert!(found.is_empty(), "{found:?}");
}

#[test]
fn imported_and_parent_module_factories_are_charged_to_the_caller() {
    for source in [
        "use other::prepare; fn run() { prepare(); }",
        "use other::prepare as acquire; fn run() { acquire(); }",
        "use other as factory; fn run() { factory::prepare(); }",
        "use other::*; fn run() { prepare(); }",
        "fn run() { other::prepare(); }",
    ] {
        let files = [
            RustFile::fixture(
                "crates/other/src/lib.rs",
                "struct Joining { clock: Clock } pub fn prepare() -> Joining { todo!() }",
            ),
            RustFile::fixture(PATH, source),
        ];
        let found = find_capability_construction_violations(
            &files,
            &POLICY,
            &OwnerGraph::collect(&files, &POLICY),
        );
        assert_eq!(found, [finding("<free>::run", "Joining")], "{source}");
    }
    let found = constructions("struct Joining { clock: Clock } fn prepare() -> Joining { todo!() } mod child { fn run() { super::prepare(); } }", &POLICY);
    assert_eq!(found, [finding("<free>::run", "Joining")]);
}

#[test]
fn sibling_paths_reexports_and_block_imports_select_the_definition() {
    for (expression, charged) in [
        ("right::prepare()", false),
        ("left::prepare()", true),
        ("self::left::prepare()", true),
        ("crate::fixture::left::prepare()", true),
        ("exports::acquire()", true),
        ("{ use left::prepare; prepare() }", true),
        ("{ use right::prepare; prepare() }", false),
        ("{ fn prepare() {} prepare() }", false),
    ] {
        let source = format!("struct Joining {{ clock: Clock }} mod left {{ use super::Joining; pub fn prepare() -> Joining {{ todo!() }} }} mod right {{ pub fn prepare() {{}} }} mod exports {{ pub use super::left::prepare as acquire; }} fn run() {{ {expression}; }}");
        let found = constructions(&source, &POLICY);
        let expected = if charged {
            vec![finding("<free>::run", "Joining")]
        } else {
            vec![]
        };
        assert_eq!(found, expected, "{expression}");
    }
}

#[test]
fn a_type_binding_does_not_shadow_a_function_in_an_outer_scope() {
    for declaration in ["type prepare = ();", "mod prepare {}"] {
        let source = format!("struct Joining {{ clock: Clock }} fn prepare() -> Joining {{ todo!() }} fn run() {{ {declaration} prepare(); }}");
        assert_eq!(
            constructions(&source, &POLICY),
            [finding("<free>::run", "Joining")],
            "{declaration}"
        );
    }
}

#[test]
fn associated_factories_keep_the_type_path_and_import_alias() {
    for (expression, charged) in [
        ("left::Factory::build()", true),
        ("right::Factory::build()", false),
        ("Imported::build()", true),
        ("<Imported>::build()", true),
        ("<crate::fixture::left::Factory>::build()", true),
    ] {
        let source = format!("struct Joining {{ clock: Clock }} mod left {{ use super::Joining; pub struct Factory; impl Factory {{ pub fn build() -> Joining {{ todo!() }} }} }} mod right {{ pub struct Factory; impl Factory {{ pub fn build() {{}} }} }} use left::Factory as Imported; fn run() {{ {expression}; }}");
        let found = constructions(&source, &POLICY);
        let expected = if charged {
            vec![finding("<free>::run", "Joining")]
        } else {
            vec![]
        };
        assert_eq!(found, expected, "{expression}");
    }
}

#[test]
fn self_resolves_to_the_impl_type_even_when_it_is_not_imported() {
    let source = "struct Joining { clock: Clock } mod factory { pub struct Factory; } impl factory::Factory { fn build(&self) -> Joining { todo!() } fn run(&self) { self.build(); } }";
    assert_eq!(
        constructions(source, &POLICY),
        [finding("Factory::run", "Joining")]
    );
}

#[test]
fn traits_with_the_same_name_keep_their_module_identity() {
    let source = "struct Joining { clock: Clock } mod left { use super::Joining; pub trait Make { fn build() -> Joining { todo!() } } } mod right { pub trait Make { fn build() {} } } use right::Make; struct Factory; impl right::Make for Factory {} fn run() { Factory::build(); }";
    assert!(constructions(source, &POLICY).is_empty());
}

#[test]
fn unresolved_or_ambiguous_factory_calls_report_the_call_site() {
    for call in [
        "prepare()",
        "crate::missing::prepare()",
        "missing::prepare()",
        "{ use left::*; use right::*; prepare() }",
    ] {
        let source = format!("struct Joining {{ clock: Clock }}\nmod left {{ use super::Joining; pub fn prepare() -> Joining {{ todo!() }} }}\nmod right {{ pub fn prepare() {{}} }}\nfn run() {{ {call}; }}");
        let found = constructions(&source, &POLICY);
        assert_eq!(found.len(), 1, "{call}: {found:?}");
        assert_eq!(found[0].path, PATH);
        assert_eq!(found[0].line, 4);
        assert!(
            found[0].message.starts_with("cannot resolve call"),
            "{found:?}"
        );
        assert!(found[0].message.contains("<free>::run"), "{found:?}");
    }
}

#[test]
fn module_declarations_determine_paths_including_path_attributes() {
    let files = [
        RustFile::fixture("crates/other/src/lib.rs", "mod parent; pub use parent::prepare;"),
        RustFile::fixture("crates/other/src/parent/mod.rs", "struct Joining { clock: Clock } pub fn prepare() -> Joining { todo!() } #[path = \"../implementation.rs\"] mod child;"),
        RustFile::fixture("crates/other/src/implementation.rs", "fn run() { super::prepare(); }"),
        RustFile::fixture(PATH, "fn run() { other::prepare(); }"),
    ];
    let found = find_capability_construction_violations(
        &files,
        &POLICY,
        &OwnerGraph::collect(&files, &POLICY),
    );
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(found.contains(&finding("<free>::run", "Joining")));
    assert!(found.iter().any(
        |finding| finding.path == "crates/other/src/implementation.rs"
            && finding.message.contains("constructs owner Joining")
    ));
}

#[test]
fn external_associated_factories_do_not_match_local_owner_names() {
    for expression in [
        "external::Worker::new()",
        "wrap!(label => external::Worker::new())",
    ] {
        let mut source = RustFile::fixture(PATH, &format!("{WORKER} fn run() {{ {expression}; }}"));
        source
            .extern_crates
            .insert("external".into(), "external".into());
        let files = [source];
        let found = find_capability_construction_violations(
            &files,
            &POLICY,
            &OwnerGraph::collect(&files, &POLICY),
        );
        assert!(found.is_empty(), "{expression}: {found:?}");
    }
}

#[test]
fn dependency_aliases_and_absolute_macro_paths_keep_crate_identity() {
    for call in [
        "renamed::prepare()",
        "::renamed::prepare()",
        "wrap!(tag => ::renamed::prepare())",
    ] {
        let mut caller = RustFile::fixture(
            PATH,
            &format!("mod renamed {{ pub fn prepare() {{}} }} fn run() {{ {call}; }}"),
        );
        caller
            .extern_crates
            .insert("renamed".into(), "other".into());
        let files = [
            RustFile::fixture(
                "crates/other/src/lib.rs",
                "struct Joining { clock: Clock } pub fn prepare() -> Joining { todo!() }",
            ),
            caller,
        ];
        let found = find_capability_construction_violations(
            &files,
            &POLICY,
            &OwnerGraph::collect(&files, &POLICY),
        );
        let expected = if call.starts_with("renamed") {
            vec![]
        } else {
            vec![finding("<free>::run", "Joining")]
        };
        assert_eq!(found, expected, "{call}");
    }
}

#[test]
fn module_and_function_names_use_separate_namespaces() {
    let source = "struct Joining { clock: Clock } mod prepare { use super::Joining; pub fn prepare() -> Joining { todo!() } } use prepare::prepare; fn run() { prepare(); }";
    assert_eq!(
        constructions(source, &POLICY),
        [finding("<free>::run", "Joining")]
    );
}

#[test]
fn prelude_bindings_do_not_borrow_other_crates_factory_results() {
    let files = [
        RustFile::fixture(
            "crates/other/src/lib.rs",
            "struct Joining { clock: Clock } fn drop() -> Joining { todo!() }",
        ),
        RustFile::fixture(
            PATH,
            "fn run() { drop(17); Some(1); Ok::<_, ()>(1); Vec::<u8>::new(); }",
        ),
    ];
    let found = find_capability_construction_violations(
        &files,
        &POLICY,
        &OwnerGraph::collect(&files, &POLICY),
    );
    assert!(found.is_empty(), "{found:?}");
}

#[test]
fn missing_calls_report_errors_even_without_a_same_named_factory() {
    for source in [
        "fn run() { absent(); }",
        "fn run() { (absent)(); }",
        "use missing::prepare as acquire; fn run() { acquire(); }",
    ] {
        let found = constructions(source, &POLICY);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].message.starts_with("cannot resolve call"),
            "{found:?}"
        );
        assert_eq!(found[0].line, 1);
    }
}

#[test]
fn macro_declared_functions_resolve_at_the_invocation() {
    let source = "struct Joining { clock: Clock } macro_rules! factory { ($name:ident, $result:ty) => { fn $name() -> $result { todo!() } }; } factory!(prepare, Joining); fn run() { prepare(); }";
    assert_eq!(
        constructions(source, &POLICY),
        [finding("<free>::run", "Joining")]
    );
}

#[test]
fn test_only_calls_and_cfg_predicates_are_not_production_calls() {
    let source = "fn run() { #[cfg(test)] absent(); #[cfg(test)] { another_absent(); } let _ = cfg!(any(unix, windows)); }";
    assert!(constructions(source, &POLICY).is_empty());
}

#[test]
fn modules_declared_in_separate_functions_do_not_share_factories() {
    let source = "struct Joining { clock: Clock } fn first() { mod factory { use super::Joining; pub fn prepare() -> Joining { todo!() } } factory::prepare(); } fn second() { mod factory { pub fn prepare() {} } factory::prepare(); }";
    assert_eq!(
        constructions(source, &POLICY),
        [finding("<free>::first", "Joining")]
    );
}

#[test]
fn local_functions_shadow_prelude_bindings_without_exemptions() {
    let source =
        "struct Joining { clock: Clock } fn drop() -> Joining { todo!() } fn run() { drop(); }";
    assert_eq!(
        constructions(source, &POLICY),
        [finding("<free>::run", "Joining")]
    );
}

#[test]
fn qualified_trait_calls_select_the_named_trait_and_inherent_methods_take_precedence() {
    for (call, charged) in [
        ("<Factory as Make>::build()", true),
        ("<Factory as Inspect>::build()", false),
        ("Factory::build()", false),
    ] {
        let source = format!("struct Joining {{ clock: Clock }} trait Make {{ fn build() -> Joining {{ todo!() }} }} trait Inspect {{ fn build() {{}} }} struct Factory; impl Make for Factory {{}} impl Inspect for Factory {{}} impl Factory {{ fn build() {{}} }} fn run() {{ {call}; }}");
        let expected = if charged {
            vec![finding("<free>::run", "Joining")]
        } else {
            vec![]
        };
        assert_eq!(constructions(&source, &POLICY), expected, "{call}");
    }
}

#[test]
fn imports_select_between_trait_factories_on_the_same_type() {
    for (import, charged) in [
        ("use left::Make;", true),
        ("use left::Make as Acquire;", true),
        ("use left::Make as _;", true),
        ("use left::*;", true),
        ("use right::Inspect;", false),
    ] {
        let source = format!("struct Joining {{ clock: Clock }} mod left {{ use super::Joining; pub trait Make {{ fn build() -> Joining {{ todo!() }} }} }} mod right {{ pub trait Inspect {{ fn build() {{}} }} }} struct Factory; impl left::Make for Factory {{}} impl right::Inspect for Factory {{}} {import} fn run() {{ Factory::build(); }}");
        let expected = if charged {
            vec![finding("<free>::run", "Joining")]
        } else {
            vec![]
        };
        assert_eq!(constructions(&source, &POLICY), expected, "{import}");
    }
}

#[test]
fn ambiguous_receiver_calls_report_the_call_site() {
    let source = "struct Joining { clock: Clock } trait Make { fn build(&self) -> Joining { todo!() } } trait Inspect { fn build(&self) {} } struct Factory; impl Make for Factory {} impl Inspect for Factory {}\nimpl Factory { fn run(&self) { self.build(); } }";
    assert_unresolved_factory_call(source);
}

#[test]
fn a_qualified_trait_impl_keeps_its_own_methods_in_scope() {
    let source = "struct Joining { clock: Clock } mod api { use super::Joining; pub trait Make { fn build(&self) -> Joining; fn run(&self); } } struct Factory; impl api::Make for Factory { fn build(&self) -> Joining { todo!() } fn run(&self) { self.build(); } }";
    assert_eq!(
        constructions(source, &POLICY),
        [finding("Factory::run", "Joining")]
    );
}

#[test]
fn unimplemented_qualified_trait_calls_report_the_call_site() {
    let source = "struct Joining { clock: Clock } trait Make { fn build() -> Joining { todo!() } } trait Unimplemented { fn build() {} } struct Factory; impl Make for Factory {}\nimpl Factory { fn run() { <Factory as Unimplemented>::build(); } }";
    assert_unresolved_factory_call(source);
}

fn assert_unresolved_factory_call(source: &str) {
    let found = constructions(source, &POLICY);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].path, PATH);
    assert_eq!(found[0].line, 2);
    assert!(
        found[0].message.starts_with("cannot resolve call"),
        "{found:?}"
    );
    assert!(found[0].message.contains("Factory::run"), "{found:?}");
}

/// A function pointer holds no value of its argument types, so a field that
/// takes an owner as an argument does not make its struct an owner. One that
/// returns an owner still does: calling it supplies one.
#[test]
fn function_pointer_fields_retain_only_their_outputs() {
    let source = format!(
        "{WORKER} struct Takes {{ run: fn(&Worker) }} struct Gives {{ make: fn() -> Worker }}"
    );
    let files = [RustFile::fixture(PATH, &source)];
    let graph = OwnerGraph::collect(&files, &POLICY);
    assert!(!graph.owners.contains("Takes"));
    assert!(graph.owners.contains("Gives"));
}
