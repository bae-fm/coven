use super::*;
use crate::owner_construction::infer_owners;

fn files(source: &str) -> Vec<RustFile> {
    vec![RustFile::fixture("crates/coven/src/lib.rs", source)]
}

fn service_returns(source: &str, policy: &Policy) -> Vec<ServiceReturnViolation> {
    let files = files(source);
    let declared = collect_declared_types(&files);
    let owners = infer_owners(&declared, policy);
    let retained = collect_root_retained_types(&declared, &owners, policy.root_owner_types, policy);
    find_service_return_violations(&files, &retained, &owners, policy)
}

fn constructions(source: &str, policy: &Policy) -> Vec<RetainedServiceConstructionViolation> {
    let files = files(source);
    let declared = collect_declared_types(&files);
    let owners = infer_owners(&declared, policy);
    let retained = collect_root_retained_types(&declared, &owners, policy.root_owner_types, policy);
    find_retained_service_construction_violations(&files, &retained, policy)
}

#[test]
fn retained_services_are_constructed_only_by_roots_or_lifetime_authorities() {
    const POLICY: Policy = Policy {
        capability_types: &["Database"],
        root_owner_types: &["Root", "SessionOwner"],
        lifetime_authorities: &[("Session", "SessionOwner")],
        composition_roots: &[("crates/coven/src/lib.rs", "Root", "new")],
        ..Policy::EMPTY
    };
    let violations = constructions(
        r#"
        struct Database;
        struct Child { database: Database }
        impl Child { fn new(database: Database) -> Self { Self { database } } }

        struct Root { child: Child }
        struct Prepared;
        impl Prepared {
            fn initialize(self, database: Database) -> Child { Child::new(database) }
        }
        impl Root {
            fn new(database: Database) -> Self {
                Self { child: Prepared::initialize(Prepared, database) }
            }
        }

        struct Wrong { database: Database }
        impl Wrong { fn build(&self, database: Database) { Child::new(database); } }

        struct Session { database: Database }
        impl Session { fn new(database: Database) -> Self { Self { database } } }
        struct SessionOwner { session: Session }
        impl SessionOwner { fn replace(&self, database: Database) { Session::new(database); } }
        "#,
        &POLICY,
    );

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].service, "Child");
    assert_eq!(violations[0].owner, "Wrong");
    assert_eq!(violations[0].method, "build");
}

#[test]
fn lifetime_authority_must_be_retained_by_a_root() {
    const POLICY: Policy = Policy {
        capability_types: &["Database"],
        root_owner_types: &["Root"],
        lifetime_authorities: &[("Child", "DetachedAuthority")],
        ..Policy::EMPTY
    };
    let violations = constructions(
        r#"
        struct Database;
        struct Child { database: Database }
        impl Child { fn new(database: Database) -> Self { Self { database } } }

        struct Root { child: Child }
        struct DetachedAuthority { database: Database }
        impl DetachedAuthority {
            fn reconnect(&self, database: Database) { Child::new(database); }
        }
        "#,
        &POLICY,
    );

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].service, "Child");
    assert_eq!(violations[0].owner, "DetachedAuthority");
    assert_eq!(violations[0].authority, None);
}

const HANDLE_ROOT: Policy = Policy {
    capability_types: &["Database"],
    root_owner_types: &["Handle"],
    ..Policy::EMPTY
};

#[test]
fn owner_cannot_return_a_service_retained_by_a_composition_root() {
    let violations = service_returns(
        r#"
        struct Database;
        struct FileAccess { database: Database }
        struct SyncOwner { database: Database }
        struct Handle { sync: SyncOwner, files: FileAccess }

        impl SyncOwner {
            pub(crate) fn file_access(&self) -> Result<Option<Arc<FileAccess>>, Error> {
                todo!()
            }
        }
        "#,
        &HANDLE_ROOT,
    );

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].owner, "SyncOwner");
    assert_eq!(violations[0].returned, "FileAccess");
}

#[test]
fn owner_cannot_return_a_stateful_service_that_no_root_retains() {
    let violations = service_returns(
        r#"
        struct Database;
        struct DetachedFileAccess { database: Database }
        struct SyncOwner { database: Database }
        struct Handle { sync: SyncOwner }

        impl SyncOwner {
            pub(crate) fn file_access(&self) -> DetachedFileAccess { todo!() }
        }
        "#,
        &HANDLE_ROOT,
    );

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].returned, "DetachedFileAccess");
}

#[test]
fn task_values_and_a_services_own_constructor_are_returnable() {
    let violations = service_returns(
        r#"
        struct Database;
        struct FileAccess { database: Database }
        struct SyncOwner { database: Database }
        struct Handle { sync: SyncOwner, files: FileAccess }
        struct AuthorizedWrite;
        struct Prepared;

        impl FileAccess {
            fn new(database: Database) -> Self { Self { database } }
        }

        impl SyncOwner {
            fn authorize(&self) -> AuthorizedWrite { AuthorizedWrite }
        }

        impl Prepared {
            pub(crate) fn initialize(self) -> FileAccess { todo!() }
        }
        "#,
        &HANDLE_ROOT,
    );

    assert!(violations.is_empty());
}

#[test]
fn retained_owner_cannot_return_its_dependency_capability() {
    const POLICY: Policy = Policy {
        capability_types: &["Cipher", "Provider"],
        root_owner_types: &["Root"],
        ..Policy::EMPTY
    };
    let files = vec![
        RustFile::fixture(
            "crates/coven/src/lib.rs",
            r#"
            struct Cipher;
            struct Provider;
            struct Child { cipher: Cipher }
            struct Root { child: Child }
            struct Builder;

            impl Child {
                pub(crate) fn derived_cipher(&self) -> Cipher { Cipher }
                pub(crate) fn create_provider(&self) -> Provider { Provider }
            }
            impl Builder {
                fn open() -> Root { todo!() }
            }
            "#,
        ),
        RustFile::fixture(
            "crates/coven/tests/fixture.rs",
            "fn child_fixture() -> Child { todo!() }",
        ),
    ];
    let declared = collect_declared_types(&files);
    let owners = infer_owners(&declared, &POLICY);
    let retained = collect_root_retained_types(&declared, &owners, &["Root"], &POLICY);

    let violations = find_service_return_violations(&files, &retained, &owners, &POLICY);

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].owner, "Child");
    assert_eq!(violations[0].returned, "Cipher");
}

#[test]
fn closed_clock_key_and_image_tasks_are_not_retained_service_getters() {
    const POLICY: Policy = Policy {
        capability_types: &["Clock", "KeyAccess", "SnapshotImage"],
        root_owner_types: &["Database"],
        task_types: &[
            "ClockReading",
            "KeyAccess",
            "KeyPackage",
            "PreparedSnapshot",
        ],
        internal_dependency_types: &["Clock", "Database"],
        always_forbidden_returns: &["Database"],
        ..Policy::EMPTY
    };
    let source = r#"
        struct Clock;
        struct ClockReading<'a> { clock: &'a Clock }
        struct KeyAccess;
        enum KeyPackage { Exact(KeyAccess), Historical(String) }
        struct SnapshotImage;
        struct PreparedSnapshot { image: SnapshotImage }
        struct Database { clock: Clock, replies: Sender<PreparedSnapshot> }
        impl Clock {
            pub fn reading(&self) -> ClockReading<'_> { todo!() }
        }
        impl Database {
            pub async fn into_prepared(self) -> PreparedSnapshot { todo!() }
            pub fn key_package(&self) -> KeyPackage { todo!() }
        }
    "#;
    assert!(service_returns(source, &POLICY).is_empty());
    assert!(
        crate::owner_dependency_boundary::find_owner_dependency_leaks(
            &[RustFile::fixture("crates/coven/src/lib.rs", source)],
            &POLICY
        )
        .is_empty()
    );
}
