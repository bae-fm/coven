use super::*;
use crate::policy::{Capabilities, Capability};

const PATH: &str = "crates/coven-database/src/work.rs";
const POLICY: Policy = Policy {
    capability_types: &["Clock", "DatabaseConnection", "Permit"],
    internal_dependency_types: &["DatabaseConnection"],
    non_owner_types: &["Transfer"],
    task_types: &["Plan", "Permit"],
    closed_session_types: &["Session"],
    composition_roots: &[(PATH, "Owner", "open")],
    capabilities: Capabilities {
        sqlite: Capability {
            name: "SQLite",
            homes: &["crates/coven-database/src/"],
            gates: &[],
        },
        ..Policy::EMPTY.capabilities
    },
    ..Policy::EMPTY
};

fn leaks(path: &str, source: &str) -> Vec<Finding> {
    let files = [RustFile::fixture(path, source)];
    find_owner_dependency_leaks(&files, &POLICY, &OwnerGraph::collect(&files, &POLICY))
}

#[test]
fn retained_capabilities_and_owners_cannot_be_returned() {
    let found = leaks(PATH, "struct Child { clock: Clock } struct Owner { child: Child } impl Owner { fn child(&self) -> &Child { todo!() } fn clock(&self) -> Clock { todo!() } }");
    assert_eq!(found.len(), 2);
    assert!(found
        .iter()
        .any(|f| f.message == "Owner::child returns retained dependency Child"));
    assert!(found
        .iter()
        .any(|f| f.message == "Owner::clock returns retained dependency Clock"));
}

#[test]
fn a_public_owner_method_cannot_return_another_service() {
    let found = leaks(PATH, "struct Child { clock: Clock } struct Owner { clock: Clock } impl Owner { pub fn child(&self) -> Child { todo!() } }");
    assert_eq!(found.len(), 1);
    assert!(found[0]
        .message
        .contains("returns retained dependency Child"));
}

#[test]
fn inner_names_do_not_allow_retained_getters() {
    let found = leaks(PATH, "struct ChildInner { clock: Clock } struct Owner { child: ChildInner } impl Owner { pub fn child(&self) -> ChildInner { todo!() } }");
    assert_eq!(found.len(), 1);
}

#[test]
fn public_fields_cannot_expose_owners_even_through_transfer_values() {
    let found = leaks(PATH, "struct Child { pub clock: Clock, pub count: usize } struct Transfer { child: Child } struct Envelope { pub transfer: Transfer }");
    assert_eq!(found.len(), 3);
    assert!(found
        .iter()
        .any(|f| f.message == "service owner Envelope exposes field transfer"));
}

#[test]
fn crate_root_sessions_cannot_expose_internal_dependencies_to_every_module() {
    let source = "struct Session { connection: DatabaseConnection }";
    assert_eq!(leaks("crates/coven-database/src/lib.rs", source).len(), 1);
    assert!(leaks(PATH, source).is_empty());
}

#[test]
fn wrappers_do_not_hide_raw_dependency_getters() {
    let found = leaks(PATH, "struct Records { connection: Connection } impl Records { fn connection(&self) -> &Connection { todo!() } } struct Wrapper { records: Records } impl Wrapper { fn records(&self) -> Records { todo!() } } struct Outer { wrapper: Wrapper } impl Outer { fn wrapper(&self) -> Wrapper { todo!() } }");
    assert_eq!(found.len(), 3);
    assert!(found
        .iter()
        .any(|f| f.message == "Outer::wrapper returns retained dependency Wrapper"));
}

#[test]
fn raw_database_handles_never_leave_functions_methods_or_traits() {
    for callable in [
        "fn leak() -> Connection { todo!() }",
        "impl Owner { fn leak(&self) -> Session { todo!() } }",
        "pub trait Owner { fn leak(&self) -> Transaction; }",
    ] {
        let found = leaks(PATH, callable);
        assert_eq!(found.len(), 1, "{callable}: {found:?}");
        assert!(found[0].message.contains("returns retained dependency"));
    }
}

#[test]
fn public_database_parameters_and_mutating_receiver_parameters_are_rejected() {
    for callable in [
        "pub fn leak(connection: &Connection) {}",
        "impl Owner { pub fn leak(connection: &Connection) {} }",
        "impl Owner { fn leak(&mut self, connection: &Connection) {} }",
        "pub trait Owner { fn leak(connection: &Connection); }",
    ] {
        let found = leaks(PATH, callable);
        assert_eq!(found.len(), 1, "{callable}: {found:?}");
        assert!(found[0]
            .message
            .contains("accepts raw dependency Connection"));
    }
    assert!(leaks(PATH, "fn leaf(connection: &Connection) {} impl Owner { fn leaf(&self, connection: &Connection) {} }").is_empty());
}

#[test]
fn consuming_tasks_can_transfer_products_but_cannot_lend_or_expose_raw_handles() {
    let found = leaks(PATH, "struct Plan { permit: Permit } impl Plan { fn into_permit(self) -> Permit { todo!() } fn borrow(&self) -> &Permit { todo!() } fn borrow_consumed(self) -> &'static Permit { todo!() } fn raw(self) -> Connection { todo!() } }");
    assert_eq!(found.len(), 3);
    assert!(found.iter().all(|f| !f.message.contains("into_permit")));
}

#[test]
fn a_listed_root_can_return_the_graph_it_constructs() {
    let source = "struct Child { clock: Clock } struct Owner { child: Child } impl Owner { pub fn open(&self) -> Child { todo!() } }";
    assert!(leaks(PATH, source).is_empty());
    assert_eq!(
        leaks("crates/coven-database/src/elsewhere.rs", source).len(),
        1
    );
}

#[test]
fn test_support_has_the_same_hand_out_boundary() {
    let source = "#[cfg(feature = \"test-utils\")] struct Owner { clock: Clock } impl Owner { pub fn clock(&self) -> Clock { todo!() } }";
    assert_eq!(leaks(PATH, source).len(), 1);
}

#[test]
fn associated_returns_and_outputs_alongside_self_obey_the_hand_out_rule() {
    for method in [
        "pub fn child() -> Child { todo!() }",
        "pub fn split(self) -> (Self, Child) { todo!() }",
    ] {
        let found = leaks(PATH, &format!("struct Child {{ clock: Clock }} struct Owner {{ child: Child }} impl Owner {{ {method} }}"));
        assert_eq!(found.len(), 1, "{method}: {found:?}");
        assert!(found[0]
            .message
            .contains("returns retained dependency Child"));
    }
}
