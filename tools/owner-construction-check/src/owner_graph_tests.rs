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

#[test]
fn every_construction_site_requires_a_list_entry() {
    for expression in [
        "Worker { clock }",
        "Worker::new(clock)",
        "crate::Worker::new(clock)",
        "wrap!(Worker::new(clock))",
        "wrap!(label => Worker { clock })",
    ] {
        for callable in [
            format!("fn run(clock: Clock) {{ let _ = {expression}; }}"),
            format!("impl Unrelated {{ fn run(clock: Clock) {{ let _ = {expression}; }} }}"),
            format!("impl Parent {{ fn new(clock: Clock) -> Self {{ let _ = {expression}; todo!() }} }}"),
        ] {
            let found = constructions(&format!("{WORKER} {callable}"), &POLICY);
            assert_eq!(found.len(), 1, "{callable}: {found:?}");
            assert!(found[0].message.contains("constructs owner Worker"));
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
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0]
            .message
            .contains(&format!("constructs owner {name}")));
    }
}

#[test]
fn returning_an_owner_does_not_grant_factory_authority() {
    for source in [
        "fn build(clock: Clock) -> Worker { Worker { clock } }",
        "impl Factory { fn build(clock: Clock) -> Worker { Worker { clock } } }",
        "impl Factory { fn build(&self, clock: Clock) -> Worker { Worker { clock } } }",
    ] {
        assert_eq!(
            constructions(&format!("{WORKER} {source}"), &POLICY).len(),
            1
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
        let source = format!("{WORKER} fn build(clock: Clock) -> Worker {{ todo!() }} impl Factory {{ fn build(&self, clock: Clock) -> Worker {{ todo!() }} fn run(&self, clock: Clock) {{ let _ = {expression}; }} }}");
        assert_eq!(constructions(&source, &POLICY).len(), 1, "{source}");
    }
}

#[test]
fn nested_functions_do_not_inherit_root_authority() {
    let source =
        format!("{WORKER} fn open() {{ fn hidden(clock: Clock) {{ Worker::new(clock); }} }}");
    assert_eq!(constructions(&source, &POLICY).len(), 1);
}

#[test]
fn tasks_compose_but_do_not_construct_owners() {
    let source = format!("{WORKER} struct Pass {{ clock: Clock }} impl Pass {{ fn new(clock: Clock) -> Self {{ Self {{ clock }} }} fn run(&self) {{ Worker::new(clock); }} }}");
    let found = constructions(&source, &POLICY);
    assert_eq!(found.len(), 1);
    assert!(found[0].message.contains("constructs owner Worker"));
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
    let source = format!("{WORKER} fn run() {{ Error::Worker(error); }}");
    assert!(constructions(&source, &POLICY).is_empty());
}

#[test]
fn enum_variants_and_aliases_are_owner_construction() {
    for name in ["Choice", "Alias"] {
        for value in ["Named { clock }", "Tuple(clock)", "Empty"] {
            let source = format!("enum Choice {{ Named {{ clock: Clock }}, Tuple(Clock), Empty }} type Alias = Choice; fn run(clock: Clock) {{ let _ = {name}::{value}; }}");
            let found = constructions(&source, &POLICY);
            assert_eq!(found.len(), 1, "{source}: {found:?}");
            assert!(found[0]
                .message
                .contains(&format!("constructs owner {name}")));
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
        assert_eq!(found.len(), 1, "{source}: {found:?}");
        assert!(found[0].message.contains("constructs owner Hidden"));
    }
}
