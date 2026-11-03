use super::*;
use crate::crate_code::checked;

const PATH: &str = "crates/app-files/src/bootstrap.rs";

fn policy() -> Policy {
    Policy::fixture(
        r#"
        task_types = ["Bootstrap", "Deletion"]
        shared_pointer_types = ["Arc", "Weak"]
        private_state_types = ["Mutex"]
        capability_types = ["Clock"]
        handles = { Progress = "Bootstrap" }
        "#,
    )
}

fn leaks(source: &str) -> Vec<Finding> {
    let files = checked(&[RustFile::fixture(PATH, source)]);
    let policy = policy();
    find_task_state_leaks(&files, &policy, &OwnerGraph::collect(&files, &policy))
}

fn finding(line: usize, message: &str) -> Finding {
    Finding::new(PATH, line, message, REMEDY)
}

/// A bootstrap's lease, made where the bootstrap is built, reaches every
/// directory the bootstrap gives out: a directory outlives the bootstrap
/// and keeps its lock.
#[test]
fn a_tasks_new_shared_state_held_by_a_long_lived_type_is_a_finding() {
    let source = "struct Lease(Mutex<Option<u8>>);
        struct Bootstrap { lease: Arc<Lease> }
        struct Dir { bootstrap: Option<Arc<Lease>> }
        fn reserve() -> Bootstrap { let lease = Arc::new(Lease(Mutex::new(None))); Bootstrap { lease } }
        impl Bootstrap { fn directory(&self) -> Dir { Dir { bootstrap: Some(self.lease.clone()) } } }";
    assert_eq!(
        leaks(source),
        [finding(3, "Dir holds Lease, which task Bootstrap makes for its operation")]
    );
}

/// The same shape made in place, inside `Some`, through `?`, or in a tuple
/// task's constructor, as `Self` or by name.
#[test]
fn every_way_of_building_a_task_with_new_shared_state_counts() {
    for build in [
        "fn reserve() -> Bootstrap { Bootstrap { lease: Arc::new(lease()) } }",
        "fn reserve() -> Bootstrap { Bootstrap { lease: std::sync::Arc::new(lease()) } }",
        "fn reserve() -> Option<Bootstrap> { let lease = Some(Arc::new(lease())); Some(Bootstrap { lease: lease? }) }",
        "fn reserve() -> Deletion { Deletion(Arc::new(lease())) }",
        "impl Deletion { fn new() -> Self { Self(Arc::new(lease())) } }",
    ] {
        let source = format!(
            "struct Lease(Mutex<u8>); struct Bootstrap {{ lease: Arc<Lease> }} struct Deletion(Arc<Lease>);
            struct Dir {{ lease: std::sync::Weak<Lease> }} {build}"
        );
        let found = leaks(&source);
        assert_eq!(found.len(), 1, "{build}: {found:?}");
        assert!(found[0].message.starts_with("Dir holds Lease, which task"), "{found:?}");
    }
}

/// What a task holds that it did not make, such as an owner it was given,
/// is that owner's to share; tasks and the task's own handles may hold
/// what it made; and a reference borrows for no longer than the task.
#[test]
fn received_state_tasks_handles_and_borrows_are_no_finding() {
    let source = "struct Lease(Mutex<u8>); struct Database { clock: Clock }
        struct Bootstrap { lease: Arc<Lease>, database: Arc<Database> }
        struct Deletion { lease: Arc<Lease> }
        struct Progress { lease: Arc<Lease> }
        struct Owner { database: Arc<Database> }
        struct Scope<'a> { lease: &'a Lease }
        fn reserve(database: Arc<Database>) -> Bootstrap {
            Bootstrap { lease: Arc::new(lease()), database }
        }";
    assert!(leaks(source).is_empty(), "{:?}", leaks(source));
}

/// A task built from what its caller passes it marks nothing: where that
/// came from is the caller's.
#[test]
fn a_task_built_from_its_arguments_marks_nothing() {
    let source = "struct Lease(Mutex<u8>); struct Bootstrap { lease: Arc<Lease> }
        struct Dir { lease: Arc<Lease> }
        impl Bootstrap { fn new(lease: Arc<Lease>) -> Self { Self { lease } } }";
    assert!(leaks(source).is_empty());
}

/// A value no one changes may outlive the task that made it, and a pointer
/// to a type of the standard library's names no value of its own: two
/// fields of that type need not share one.
#[test]
fn values_and_unnamed_state_are_no_finding() {
    let source = "struct Changes(Vec<u8>);
        struct Bootstrap { changes: Arc<Changes>, list: Arc<Vec<u8>>, lock: Arc<Mutex<u8>> }
        struct Subscription { changes: Arc<Changes>, list: Arc<Vec<u8>>, copying: Arc<Mutex<u8>> }
        fn reserve() -> Bootstrap {
            Bootstrap { changes: Arc::new(changes()), list: Arc::new(list()), lock: Arc::new(lock()) }
        }";
    assert!(leaks(source).is_empty(), "{:?}", leaks(source));
}
