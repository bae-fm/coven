use super::*;

/// One policy for every fixture, as the workspace has one policy file.
const POLICY: Policy = Policy {
    capability_types: &[
        "Cipher",
        "CipherState",
        "Clock",
        "Database",
        "DatabaseHandle",
        "Encryption",
        "KeyCustody",
        "MemberKeys",
        "Provider",
        "SnapshotImage",
        "Storage",
        "StorageConnection",
        "StoreDir",
        "WritePermit",
    ],
    non_owner_types: &[
        "Cipher",
        "CreatedSnapshot",
        "CustodyChoice",
        "OpenedStore",
        "Records",
        "SnapshotCut",
        "SpoolProtection",
    ],
    task_types: &[
        "ClockReading",
        "CommitPlan",
        "PreparedSnapshot",
        "WritePermit",
    ],
    composition_roots: &[(
        "crates/coven-sync/src/test_helpers.rs",
        "TestStore",
        "bind_device_in",
    )],
    internal_dependency_types: &[
        "Clock",
        "Database",
        "DatabaseConnection",
        "DatabaseHandle",
        "DatabaseOwner",
        "Gates",
        "ProbeStorage",
        "Storage",
        "StoreDir",
    ],
    always_forbidden_returns: &[
        "Database",
        "DatabaseConnection",
        "DatabaseHandle",
        "DatabaseOwner",
        "ProbeStorage",
        "SlotStorage",
    ],
    closed_session_types: &["DatabaseSession", "WriteSession"],
    field_capability_types: &["Cipher", "CipherState", "SlotStorage"],
    raw_provider_operations: &[(
        "Storage",
        &[
            "delete_provider_object",
            "list_provider_objects",
            "provider_object_exists",
            "read_provider_object",
            "write_provider_object",
        ],
    )],
    derived_services: &[
        ("SpoolProtection", &["StorageConnection", "Storage"]),
        (
            "Cipher",
            &[
                "Cipher",
                "CipherState",
                "StorageConnection",
                "KeyCustody",
                "Security",
            ],
        ),
        (
            "Encryption",
            &[
                "Cipher",
                "CipherState",
                "StorageConnection",
                "KeyCustody",
                "Security",
            ],
        ),
    ],
    capabilities: crate::policy::Capabilities {
        sqlite: crate::policy::Capability {
            name: "SQLite",
            homes: &["crates/coven-database/src/"],
            gates: &[],
        },
        ..Policy::EMPTY.capabilities
    },
    ..Policy::EMPTY
};

fn leaks(path: &str, source: &str) -> Vec<OwnerDependencyLeak> {
    leaks_with(path, source, &POLICY)
}

fn leaks_with(path: &str, source: &str, policy: &Policy) -> Vec<OwnerDependencyLeak> {
    find_owner_dependency_leaks(&[RustFile::fixture(path, source)], policy)
}

const DATABASE_FILE: &str = "crates/coven-database/src/fixture.rs";

#[test]
fn crate_root_session_cannot_retain_a_raw_database_dependency() {
    let leaks = leaks(
        "crates/coven-database/src/lib.rs",
        r#"
        struct Connection;
        struct DatabaseSession<'a> { connection: &'a Connection }
        impl DatabaseSession<'_> {
            fn execute_domain_operation(&self) {}
        }
        "#,
    );

    assert_eq!(leaks.len(), 1);
}

#[test]
fn owner_methods_cannot_return_raw_dependencies() {
    let leaks = leaks(
        DATABASE_FILE,
        r#"
        struct Connection;
        struct Transaction<'a>(&'a Connection);
        struct StoreDir;
        struct Clock;
        struct DatabaseOwner {
            connection: Connection,
            store_dir: StoreDir,
            clock: Clock,
        }
        impl DatabaseOwner {
            fn connection(&self) -> &Connection { &self.connection }
            fn transaction(&self) -> Transaction<'_> { todo!() }
            fn store_dir(&self) -> &StoreDir { &self.store_dir }
            fn clock(&self) -> std::sync::Arc<Clock> { todo!() }
        }
        "#,
    );

    assert_eq!(leaks.len(), 4);
    assert!(leaks
        .iter()
        .all(|leak| matches!(leak, OwnerDependencyLeak::Return { .. })));
}

#[test]
fn returning_a_wrapper_that_exposes_a_dependency_is_rejected() {
    let leaks = leaks(
        DATABASE_FILE,
        r#"
        struct Connection;
        struct Records<'a> { connection: &'a Connection }
        impl Records<'_> {
            fn conn(&self) -> &Connection { self.connection }
        }
        struct MergeTransaction<'a> { records: Records<'a> }
        impl MergeTransaction<'_> {
            fn records(&self) -> Records<'_> { todo!() }
        }
        "#,
    );

    assert_eq!(leaks.len(), 2);
    assert!(leaks.iter().any(|leak| matches!(
        leak,
        OwnerDependencyLeak::Return { owner, dependency, .. }
            if owner == "MergeTransaction" && dependency == "Records"
    )));
}

#[test]
fn runtime_owner_methods_cannot_accept_raw_dependencies() {
    let file = RustFile::fixture(
        DATABASE_FILE,
        r#"
        struct Connection;
        struct Authority;
        impl Authority {
            fn new(connection: &Connection) -> Self { Self }
            fn required_root(&mut self, connection: &Connection) -> String { todo!() }
        }
        "#,
    );

    let methods = collect_receiver_methods(std::slice::from_ref(&file));
    let required_root = methods
        .iter()
        .find(|method| method.method == "required_root")
        .expect("required_root receiver method");
    assert!(required_root.mutates_owner);
    assert!(required_root.parameters.contains("Connection"));

    let leaks = find_owner_dependency_leaks(&[file], &POLICY);
    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Parameter { owner, method, dependency, .. }
            if owner == "Authority"
                && method == "required_root"
                && dependency == "Connection"
    ));
}

#[test]
fn owner_methods_cannot_install_retained_capabilities_through_wrappers() {
    const UNEXPORTED: Policy = Policy {
        unexported_capability_types: &["Database"],
        ..POLICY
    };
    let leaks = leaks_with(
        "crates/coven-sync/src/upload_observer.rs",
        r#"
        struct Database;
        struct UploadObserver {
            database: std::sync::OnceLock<std::sync::Arc<Database>>,
        }
        impl UploadObserver {
            fn set_database(&self, database: std::sync::Arc<Database>) {
                let _ = self.database.set(database);
            }
        }
        "#,
        &UNEXPORTED,
    );

    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Parameter { owner, method, dependency, .. }
            if owner == "UploadObserver"
                && method == "set_database"
                && dependency == "Database"
    ));
}

#[test]
fn retained_service_traits_cannot_return_child_services() {
    let leaks = leaks(
        DATABASE_FILE,
        r#"
        struct ProbeStorage;
        trait Storage {
            fn probes(&self) -> &ProbeStorage;
        }
        "#,
    );

    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "Storage" && method == "probes" && dependency == "ProbeStorage"
    ));
}

#[test]
fn storage_traits_cannot_expose_raw_provider_object_operations() {
    let leaks = leaks(
        "crates/coven-storage/src/storage.rs",
        r#"
        trait Storage {
            fn read_provider_object(&self, key: &str) -> Vec<u8>;
            fn write_provider_object(&self, key: &str, bytes: Vec<u8>);
            fn list_provider_objects(&self, prefix: &str) -> Vec<String>;
            fn delete_provider_object(&self, key: &str);
        }
        "#,
    );

    assert_eq!(leaks.len(), 4);
}

#[test]
fn retained_service_owners_cannot_expose_any_fields() {
    let leaks = leaks(
        "crates/coven-storage/src/range_reader.rs",
        r#"
        trait SlotStorage {}
        struct RangeReader {
            pub exact: std::sync::Arc<dyn SlotStorage>,
            pub plaintext_size: u64,
        }
        "#,
    );

    assert_eq!(leaks.len(), 2);
}

#[test]
fn configured_stateful_capabilities_define_owner_boundaries() {
    const UNEXPORTED: Policy = Policy {
        unexported_capability_types: &["AtomicBool"],
        ..POLICY
    };
    let leaks = leaks_with(
        "crates/coven-sync/src/service.rs",
        r#"
        struct AtomicBool;
        struct UploadState { paused: AtomicBool }
        struct App {
            state: UploadState,
            pub runtime_name: String,
        }
        impl App {
            pub(crate) fn state(&self) -> UploadState { todo!() }
        }
        "#,
        &UNEXPORTED,
    );

    assert_eq!(leaks.len(), 2);
    assert!(leaks.iter().any(|leak| matches!(
        leak,
        OwnerDependencyLeak::Field { owner, field, .. }
            if owner == "App" && field == "runtime_name"
    )));
    assert!(leaks.iter().any(|leak| matches!(
        leak,
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "App" && method == "state" && dependency == "UploadState"
    )));
}

#[test]
fn test_support_cannot_expose_retained_service_fields() {
    let leaks = leaks(
        "crates/coven-sync/src/test_helpers.rs",
        r#"
        trait Storage {}
        struct TestStoreFixture {
            pub storage: std::sync::Arc<dyn Storage>,
        }
        "#,
    );

    assert_eq!(leaks.len(), 1);
}

#[test]
fn transfer_objects_cannot_expose_service_fields() {
    let leaks = leaks(
        "crates/coven-sync/src/opening.rs",
        r#"
        struct DatabaseHandle;
        struct SyncOwner {
            database: DatabaseHandle,
        }
        struct OpenedStore {
            pub(crate) owner: SyncOwner,
            pub(crate) device_id: String,
        }
        "#,
    );

    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Field { owner, field, .. }
            if owner == "OpenedStore" && field == "owner"
    ));
}

#[test]
fn transfer_objects_cannot_be_publicly_exposed_through_another_transfer() {
    let leaks = leaks(
        "crates/coven-sync/src/circles.rs",
        r#"
        struct SnapshotImage;
        struct CreatedSnapshot {
            image: SnapshotImage,
        }
        struct SnapshotCut {
            snapshot: CreatedSnapshot,
        }
        struct AddMemberRequest {
            pub(super) bootstrap: SnapshotCut,
        }
        "#,
    );

    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Field { owner, field, .. }
            if owner == "AddMemberRequest" && field == "bootstrap"
    ));
}

#[test]
fn signing_and_resource_capabilities_cannot_be_exposed_as_fields() {
    let leaks = leaks(
        "crates/coven-sync/src/write_task.rs",
        r#"
        struct MemberKeys;
        struct SnapshotImage;
        struct WritePermit;
        struct WriteTaskState {
            pub(super) signer: MemberKeys,
            pub(crate) snapshot: SnapshotImage,
            pub permit: WritePermit,
        }
        "#,
    );

    assert_eq!(leaks.len(), 3);
}

#[test]
fn test_support_cannot_return_its_retained_database() {
    let leaks = leaks(
        "crates/coven-database/src/test_support/synthetic_store.rs",
        r#"
        struct Database;
        struct SyntheticStore {
            database: Database,
        }
        impl SyntheticStore {
            pub fn database(&self) -> &Database { &self.database }
        }
        "#,
    );

    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "SyntheticStore" && method == "database" && dependency == "Database"
    ));
}

#[test]
fn cfg_test_methods_cannot_return_retained_capabilities() {
    let leaks = leaks(
        "crates/coven/src/security.rs",
        r#"
        struct KeyCustody;
        struct Encryption;
        struct Security {
            custody: KeyCustody,
        }
        impl Security {
            #[cfg(test)]
            pub(crate) fn encryption_for_test(&self) -> Encryption { todo!() }
        }
        "#,
    );

    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "Security"
                && method == "encryption_for_test"
                && dependency == "Encryption"
    ));
}

#[test]
fn test_owner_graph_cannot_return_its_retained_services() {
    let leaks = leaks(
        "crates/coven-sync/src/test_owner_graph.rs",
        r#"
        struct DatabaseHandle;
        struct StoreDir;
        struct LocalFileAccess {
            database: DatabaseHandle,
            store_dir: StoreDir,
        }
        struct LocalFileMoves {
            database: DatabaseHandle,
            store_dir: StoreDir,
        }
        struct TestOwnerGraph {
            local_access: LocalFileAccess,
            local_moves: LocalFileMoves,
        }
        impl TestOwnerGraph {
            pub fn local_access(&self) -> LocalFileAccess { todo!() }
            pub fn local_moves(&self) -> LocalFileMoves { todo!() }
        }
        "#,
    );

    assert_eq!(leaks.len(), 2);
}

#[test]
fn composition_factory_can_return_a_new_service_but_not_a_retained_service() {
    let leaks = leaks(
        "crates/coven-sync/src/test_helpers.rs",
        r#"
        struct StorageConnection;
        struct TestDevice {
            storage: StorageConnection,
        }
        struct TestStore {
            founder: TestDevice,
        }
        impl TestStore {
            pub async fn bind_device_in(&self) -> TestDevice { todo!() }
            pub async fn founder_device(&self) -> TestDevice { todo!() }
        }
        "#,
    );

    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "TestStore" && method == "founder_device" && dependency == "TestDevice"
    ));
}

#[test]
fn owner_cannot_return_a_retained_capability() {
    let leaks = leaks(
        "crates/coven-sync/src/test_helpers.rs",
        r#"
        struct StorageConnection;
        struct TestOwnerGraph {
            storage: std::sync::Arc<StorageConnection>,
        }
        impl TestOwnerGraph {
            pub fn storage(&self) -> std::sync::Arc<StorageConnection> {
                self.storage.clone()
            }
        }
        "#,
    );

    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "TestOwnerGraph"
                && method == "storage"
                && dependency == "StorageConnection"
    ));
}

#[test]
fn owner_methods_cannot_build_unexported_capabilities_for_callers() {
    const UNEXPORTED: Policy = Policy {
        unexported_capability_types: &["Config", "Keys", "Library", "ProviderClient"],
        ..POLICY
    };
    let leaks = leaks_with(
        "crates/coven-sync/src/library.rs",
        r#"
        struct Config;
        struct Keys;
        struct ProviderClient;
        struct Library {
            config: Config,
            keys: Keys,
        }
        impl Library {
            fn provider_client(&self) -> ProviderClient { todo!() }
        }
        "#,
        &UNEXPORTED,
    );

    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "Library" && method == "provider_client" && dependency == "ProviderClient"
    ));
}

#[test]
fn an_exportable_task_output_does_not_allow_retained_getters() {
    const EXPORTABLE: Policy = Policy {
        unexported_capability_types: &["UploadSession", "UploadTarget"],
        exportable_capability_outputs: &["UploadSession"],
        ..POLICY
    };
    let leaks = leaks_with(
        "crates/coven-sync/src/uploads.rs",
        r#"
        struct UploadSession;
        struct UploadTarget;
        impl UploadTarget {
            fn start_session(&self) -> UploadSession { todo!() }
        }
        struct UploadService {
            session: UploadSession,
        }
        impl UploadService {
            fn session(&self) -> UploadSession { todo!() }
        }
        "#,
        &EXPORTABLE,
    );

    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "UploadService" && method == "session" && dependency == "UploadSession"
    ));
}

#[test]
fn private_owner_helpers_cannot_pass_retained_capabilities_between_owner_methods() {
    let leaks = leaks(
        "crates/coven/src/rows.rs",
        r#"
        struct Encryption;
        struct KeyCustody;
        struct Rows {
            keys: KeyCustody,
        }
        impl Rows {
            fn routing_encryption(&self) -> Encryption { todo!() }
            pub(crate) fn encryption(&self) -> Encryption { todo!() }
        }
        "#,
    );

    assert_eq!(leaks.len(), 2);
    assert!(leaks.iter().any(|leak| matches!(
        leak,
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "Rows" && method == "routing_encryption" && dependency == "Encryption"
    )));
}

#[test]
fn configuration_values_can_resolve_selected_capabilities() {
    let leaks = leaks(
        "crates/coven-crypto/src/custody.rs",
        r#"
        struct KeyCustody;
        enum CustodyChoice {
            Custom(KeyCustody),
        }
        impl CustodyChoice {
            pub fn resolve(self) -> KeyCustody { todo!() }
        }
        "#,
    );

    assert!(leaks.is_empty());
}

#[test]
fn methods_cannot_return_key_or_provider_capabilities() {
    let leaks = leaks(
        "crates/coven-storage/src/cipher.rs",
        r#"
        struct Cipher;
        struct SpoolProtection;
        trait SlotStorage {}
        trait CipherState {
            fn snapshot(&self) -> Cipher;
        }
        trait Storage {
            fn spool_protection(&self) -> SpoolProtection;
        }
        trait Provider {
            fn slot_storage(&self) -> std::sync::Arc<dyn SlotStorage>;
        }
        "#,
    );

    assert_eq!(leaks.len(), 3);
}

#[test]
fn closed_sessions_and_private_leaf_sql_are_allowed() {
    let leaks = leaks(
        DATABASE_FILE,
        r#"
        struct Connection;
        struct StoreDir;
        struct RootRef;
        struct WriteSession<'a> {
            connection: &'a Connection,
            store_dir: &'a StoreDir,
        }
        impl WriteSession<'_> {
            fn required_root(&mut self) -> RootRef { todo!() }
        }
        fn load_root_on(connection: &Connection) -> RootRef { todo!() }
        "#,
    );

    assert!(leaks.is_empty());
}

#[test]
fn public_free_functions_cannot_expose_raw_database_dependencies() {
    let leaks = leaks(
        DATABASE_FILE,
        r#"
        struct Connection;
        struct Transaction<'a>(&'a Connection);
        pub fn open_image() -> Connection { todo!() }
        pub fn persist_on(connection: &Connection) { todo!() }
        fn private_leaf(connection: &Connection) { todo!() }
        "#,
    );

    assert_eq!(leaks.len(), 2);
}

#[test]
fn production_callables_cannot_return_raw_database_dependencies() {
    let leaks = leaks(
        DATABASE_FILE,
        r#"
        struct Connection;
        struct Session;
        struct Image;
        pub(crate) fn open_image() -> Connection { todo!() }
        fn attach_capture() -> Session { todo!() }
        impl Image {
            fn connection(&self) -> Connection { todo!() }
        }
        "#,
    );

    assert_eq!(leaks.len(), 3);
}

#[test]
fn public_database_methods_cannot_expose_raw_database_dependencies() {
    let leaks = leaks(
        DATABASE_FILE,
        r#"
        struct Connection;
        pub struct Gates;
        impl Gates {
            pub fn from_connection(connection: &Connection) -> Self { todo!() }
            pub fn apply(&self, connection: &Connection) { todo!() }
            fn private_leaf(&self, connection: &Connection) { todo!() }
        }
        "#,
    );

    assert_eq!(leaks.len(), 2);
}

#[test]
fn public_database_traits_cannot_expose_raw_database_dependencies() {
    let leaks = leaks(
        DATABASE_FILE,
        r#"
        struct Connection;
        pub trait RawDatabaseWorkflow {
            fn open_image() -> Connection;
            fn persist_on(&self, connection: &Connection);
        }
        trait PrivateDatabaseWorkflow {
            fn persist_on(&self, connection: &Connection);
        }
        "#,
    );

    assert_eq!(leaks.len(), 2);
}

fn returning_methods(leaks: &[OwnerDependencyLeak]) -> BTreeSet<&str> {
    leaks
        .iter()
        .map(|leak| match leak {
            OwnerDependencyLeak::Return { method, .. } => method.as_str(),
            other => panic!("unexpected finding: {other:?}"),
        })
        .collect()
}

#[test]
fn consuming_tasks_transfer_permits_but_borrowed_getters_still_leak() {
    let leaks = leaks(
        DATABASE_FILE,
        r#"
        struct WritePermit;
        struct CommitPlan { permit: WritePermit }
        struct ResolutionPlan { plan: CommitPlan }
        impl CommitPlan {
            fn into_permit(self) -> WritePermit { self.permit }
            fn permit(&self) -> &WritePermit { &self.permit }
            fn borrowed_after_consuming(self) -> Box<&'static WritePermit> { todo!() }
        }
        impl ResolutionPlan {
            fn finish(self) -> CommitPlan { self.plan }
        }
        "#,
    );
    assert_eq!(
        returning_methods(&leaks),
        BTreeSet::from(["borrowed_after_consuming", "finish", "permit"])
    );
}

#[test]
fn consuming_prepared_images_does_not_allow_raw_database_returns_or_wrappers() {
    let leaks = leaks(
        DATABASE_FILE,
        r#"
        struct Connection;
        struct SnapshotImage;
        struct PreparedSnapshot { image: SnapshotImage, connection: Connection }
        impl PreparedSnapshot {
            fn connection(&self) -> &Connection { &self.connection }
        }
        struct Database { prepared: PreparedSnapshot, connection: Connection }
        impl Database {
            fn into_prepared(self) -> PreparedSnapshot { self.prepared }
            fn into_connection(self) -> Connection { self.connection }
        }
        "#,
    );
    assert_eq!(
        returning_methods(&leaks),
        BTreeSet::from(["connection", "into_connection", "into_prepared"])
    );
}

#[test]
fn consumed_worker_returns_owned_image_but_cannot_lend_retained_image() {
    let leaks = leaks(
        DATABASE_FILE,
        r#"
        struct SnapshotImage;
        struct PreparedSnapshot { image: SnapshotImage }
        struct Database { replies: Sender<PreparedSnapshot> }
        impl Database {
            async fn into_prepared(self) -> Result<PreparedSnapshot, Error> { todo!() }
            fn prepared(&self) -> &PreparedSnapshot { todo!() }
        }
        "#,
    );
    assert_eq!(leaks.len(), 1);
    assert!(
        matches!(&leaks[0], OwnerDependencyLeak::Return { method, .. } if method == "prepared")
    );
}

#[test]
fn consuming_tasks_transfer_closed_plans_and_permits() {
    let leaks = leaks(
        DATABASE_FILE,
        r#"
        struct WritePermit;
        struct CommitPlan { permit: WritePermit }
        struct ResolutionPlan { plan: CommitPlan }
        impl CommitPlan {
            fn into_permit(self) -> WritePermit { self.permit }
        }
        impl ResolutionPlan {
            fn finish(self) -> CommitPlan { self.plan }
        }
        "#,
    );
    assert!(leaks.is_empty());
}
