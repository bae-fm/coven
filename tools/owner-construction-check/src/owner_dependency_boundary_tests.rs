use super::*;

const PATH: &str = "crates/app-database/src/work.rs";
fn policy() -> Policy {
    Policy::fixture(&format!(
        r#"
        capability_types = ["Clock", "DatabaseConnection", "Permit"]
        internal_dependency_types = ["DatabaseConnection"]
        non_owner_types = ["Transfer"]
        task_types = ["Plan", "Permit"]
        closed_session_types = ["Session"]

        [composition_roots]
        "{PATH}" = ["Owner::open"]

        [database]
        capability = "SQLite"

        [[capabilities]]
        name = "SQLite"
        homes = ["crates/app-database/src/"]
        "#
    ))
}

fn leaks(path: &str, source: &str) -> Vec<Finding> {
    let files = [RustFile::fixture(path, source)];
    let policy = policy();
    find_owner_dependency_leaks(&files, &policy, &OwnerGraph::collect(&files, &policy))
}

fn findings(path: &str, messages: &[&str]) -> Vec<Finding> {
    messages.iter().map(|message| Finding::new(path, 1, *message, REMEDY)).collect()
}

#[test]
fn retained_capabilities_and_owners_cannot_be_returned() {
    let found = leaks(PATH, "struct Child { clock: Clock } struct Owner { child: Child } impl Owner { fn child(&self) -> &Child { todo!() } fn clock(&self) -> Clock { todo!() } }");
    assert_eq!(
        found,
        findings(
            PATH,
            &[
                "Owner::child returns retained dependency Child",
                "Owner::clock returns retained dependency Clock",
            ]
        )
    );
}

#[test]
fn a_public_owner_method_cannot_return_another_service() {
    let found = leaks(PATH, "struct Child { clock: Clock } struct Owner { clock: Clock } impl Owner { pub fn child(&self) -> Child { todo!() } }");
    assert_eq!(found, findings(PATH, &["Owner::child returns retained dependency Child"]));
}

#[test]
fn inner_names_do_not_allow_retained_getters() {
    let found = leaks(PATH, "struct ChildInner { clock: Clock } struct Owner { child: ChildInner } impl Owner { pub fn child(&self) -> ChildInner { todo!() } }");
    assert_eq!(found, findings(PATH, &["Owner::child returns retained dependency ChildInner"]));
}

#[test]
fn public_fields_cannot_expose_owners_even_through_transfer_values() {
    let found = leaks(PATH, "struct Child { pub clock: Clock, pub count: usize } struct Transfer { child: Child } struct Envelope { pub transfer: Transfer }");
    assert_eq!(
        found,
        findings(
            PATH,
            &[
                "service owner Child exposes field clock",
                "service owner Child exposes field count",
                "service owner Envelope exposes field transfer",
            ]
        )
    );
}

#[test]
fn crate_root_sessions_cannot_expose_internal_dependencies_to_every_module() {
    let source = "struct Session { connection: DatabaseConnection }";
    let path = "crates/app-database/src/lib.rs";
    assert_eq!(
        leaks(path, source),
        findings(
            path,
            &["crate-root Session exposes internal dependency DatabaseConnection to every module",]
        )
    );
    assert!(leaks(PATH, source).is_empty());
}

#[test]
fn wrappers_do_not_hide_raw_dependency_getters() {
    let found = leaks(PATH, "struct Records { connection: Connection } impl Records { fn connection(&self) -> &Connection { todo!() } } struct Wrapper { records: Records } impl Wrapper { fn records(&self) -> Records { todo!() } } struct Outer { wrapper: Wrapper } impl Outer { fn wrapper(&self) -> Wrapper { todo!() } }");
    assert_eq!(
        found,
        findings(
            PATH,
            &[
                "Outer::wrapper returns retained dependency Wrapper",
                "Records::connection returns retained dependency Connection",
                "Wrapper::records returns retained dependency Records",
            ]
        )
    );
}

#[test]
fn raw_database_handles_never_leave_functions_methods_or_traits() {
    for (callable, message) in [
        ("fn leak() -> Connection { todo!() }", "leak returns retained dependency Connection"),
        (
            "impl Owner { fn leak(&self) -> Session { todo!() } }",
            "Owner::leak returns retained dependency Session",
        ),
        (
            "pub trait Owner { fn leak(&self) -> Transaction; }",
            "Owner::leak returns retained dependency Transaction",
        ),
    ] {
        let found = leaks(PATH, callable);
        assert_eq!(found, findings(PATH, &[message]), "{callable}");
    }
}

#[test]
fn public_database_parameters_and_mutating_receiver_parameters_are_rejected() {
    for (callable, message) in [
        ("pub fn leak(connection: &Connection) {}", "leak accepts raw dependency Connection"),
        (
            "impl Owner { pub fn leak(connection: &Connection) {} }",
            "Owner::leak accepts raw dependency Connection",
        ),
        (
            "impl Owner { fn leak(&mut self, connection: &Connection) {} }",
            "Owner::leak accepts raw dependency Connection",
        ),
        (
            "pub trait Owner { fn leak(connection: &Connection); }",
            "Owner::leak accepts raw dependency Connection",
        ),
    ] {
        let found = leaks(PATH, callable);
        assert_eq!(found, findings(PATH, &[message]), "{callable}");
    }
    assert!(leaks(PATH, "fn leaf(connection: &Connection) {} impl Owner { fn leaf(&self, connection: &Connection) {} }").is_empty());
}

#[test]
fn consuming_tasks_can_transfer_products_but_cannot_lend_or_expose_raw_handles() {
    let found = leaks(PATH, "struct Plan { permit: Permit } impl Plan { fn into_permit(self) -> Permit { todo!() } fn borrow(&self) -> &Permit { todo!() } fn borrow_consumed(self) -> &'static Permit { todo!() } fn raw(self) -> Connection { todo!() } }");
    assert_eq!(
        found,
        findings(
            PATH,
            &[
                "Plan::borrow returns retained dependency Permit",
                "Plan::borrow_consumed returns retained dependency Permit",
                "Plan::raw returns retained dependency Connection",
            ]
        )
    );
}

#[test]
fn a_listed_root_can_return_the_graph_it_constructs() {
    let source = "struct Child { clock: Clock } struct Owner { child: Child } impl Owner { pub fn open(&self) -> Child { todo!() } }";
    assert!(leaks(PATH, source).is_empty());
    let path = "crates/app-database/src/elsewhere.rs";
    assert_eq!(
        leaks(path, source),
        findings(path, &["Owner::open returns retained dependency Child"])
    );
}

#[test]
fn test_support_has_the_same_hand_out_boundary() {
    let source = "#[cfg(feature = \"test-utils\")] struct Owner { clock: Clock } impl Owner { pub fn clock(&self) -> Clock { todo!() } }";
    assert_eq!(
        leaks(PATH, source),
        findings(PATH, &["Owner::clock returns retained dependency Clock"])
    );
}

#[test]
fn associated_returns_and_outputs_alongside_self_obey_the_hand_out_rule() {
    for (method, message) in [
        ("pub fn child() -> Child { todo!() }", "Owner::child returns retained dependency Child"),
        (
            "pub fn split(self) -> (Self, Child) { todo!() }",
            "Owner::split returns retained dependency Child",
        ),
    ] {
        let found = leaks(PATH, &format!("struct Child {{ clock: Clock }} struct Owner {{ child: Child }} impl Owner {{ {method} }}"));
        assert_eq!(found, findings(PATH, &[message]), "{method}");
    }
}

/// A handle's owner hands it out, and the handle clones itself. An owner
/// holding that owner, at any depth, passes up a fresh one it asked for; what
/// holds the handle itself, or holds no owner of it, hands out none.
#[test]
fn only_a_handles_owner_and_what_holds_it_hand_it_out() {
    let mut policy = policy();
    policy.handles.insert("Requests".into(), "Query".into());
    let source = "struct State { clock: Clock } \
        struct Requests { state: State } \
        impl Requests { fn clone(&self) -> Self { todo!() } } \
        struct Query { state: State } \
        impl Query { pub fn requests(&self) -> Requests { todo!() } } \
        struct Wrapper { query: Query } \
        impl Wrapper { pub fn requests(&self) -> Requests { todo!() } } \
        struct Outer { wrapper: std::sync::Arc<Wrapper> } \
        impl Outer { pub fn requests(&self) -> Requests { todo!() } } \
        struct Holder { requests: Requests } \
        impl Holder { pub fn requests(&self) -> Requests { todo!() } } \
        struct Keeper { query: Query, kept: Holder } \
        impl Keeper { pub fn requests(&self) -> Requests { todo!() } } \
        struct Owner { clock: Clock } \
        impl Owner { pub fn share(&self) -> Requests { todo!() } }";
    let files = [RustFile::fixture(PATH, source)];
    let found = find_owner_dependency_leaks(&files, &policy, &OwnerGraph::collect(&files, &policy));
    let finding = |message: &str| Finding::new(PATH, 1, message, HANDLE_REMEDY);
    assert_eq!(
        found,
        [
            finding("Holder::requests hands out handle Requests, which it holds; only Query makes one"),
            finding("Keeper::requests hands out handle Requests, which it holds; only Query makes one"),
            finding(
                "Owner::share hands out handle Requests, which only Query and what holds it hand out"
            ),
        ]
    );
}

/// What shares an owner's state without being declared its handle is no
/// handle: returning it hands out what the owner holds.
#[test]
fn undeclared_shares_of_an_owners_state_are_still_handed_out() {
    let mut policy = policy();
    policy.handles.insert("Requests".into(), "Query".into());
    let source = "struct State { clock: Clock } \
        struct Requests { state: State } \
        struct Watch { state: State } \
        struct Query { state: State } \
        impl Query { pub fn requests(&self) -> Requests { todo!() } pub fn watch(&self) -> Watch { todo!() } }";
    let files = [RustFile::fixture(PATH, source)];
    let found = find_owner_dependency_leaks(&files, &policy, &OwnerGraph::collect(&files, &policy));
    assert_eq!(found, findings(PATH, &["Query::watch returns retained dependency Watch"]));
}

/// A trait's method that makes owners, as a connector's `connect` makes a
/// connection, returns one when its declaration is a listed root.
#[test]
fn a_listed_trait_method_can_return_the_owner_it_makes() {
    let source =
        "struct Remote { clock: Clock } pub trait Connector { fn connect(&self) -> Remote; }";
    for (roots, expected) in [
        (
            r#"[composition_roots]
            "crates/app-storage/src/connector.rs" = ["Connector::connect"]"#,
            vec![],
        ),
        ("", findings(PATH, &["Connector::connect returns retained dependency Remote"])),
    ] {
        let policy =
            Policy::fixture(&format!("capability_types = [\"Clock\", \"Connector\"]\n{roots}"));
        let path = if roots.is_empty() { PATH } else { "crates/app-storage/src/connector.rs" };
        let files = [RustFile::fixture(path, source)];
        let found =
            find_owner_dependency_leaks(&files, &policy, &OwnerGraph::collect(&files, &policy));
        assert_eq!(found, expected, "{roots}");
    }
}
