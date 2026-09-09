use super::*;

fn production_file(source: &str) -> RustFile {
    production_file_at("crates/coven-database/src/fixture.rs", source)
}

fn production_file_at(relative_path: &str, source: &str) -> RustFile {
    RustFile {
        relative_path: relative_path.to_string(),
        syntax: syn::parse_file(source).expect("parse fixture"),
    }
}

#[test]
fn crate_root_session_cannot_retain_a_raw_database_dependency() {
    let file = production_file_at(
        "crates/coven-database/src/lib.rs",
        r#"
        struct Connection;
        struct DatabaseSession<'a> { connection: &'a Connection }
        impl DatabaseSession<'_> {
            fn execute_domain_operation(&self) {}
        }
        "#,
    );

    assert_eq!(find_owner_dependency_leaks(&[file]).len(), 1);
}

#[test]
fn owner_methods_cannot_return_raw_dependencies() {
    let file = production_file(
        r#"
        struct Connection;
        struct Transaction<'a>(&'a Connection);
        struct StoreDir;
        struct Hlc;
        struct DatabaseCore {
            connection: Connection,
            store_dir: StoreDir,
            hlc: Hlc,
        }
        impl DatabaseCore {
            fn connection(&self) -> &Connection { &self.connection }
            fn transaction(&self) -> Transaction<'_> { todo!() }
            fn store_dir(&self) -> &StoreDir { &self.store_dir }
            fn hlc(&self) -> std::sync::Arc<Hlc> { todo!() }
        }
        "#,
    );

    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 4);
    assert!(leaks
        .iter()
        .all(|leak| matches!(leak, OwnerDependencyLeak::Return { .. })));
}

#[test]
fn returning_a_wrapper_that_exposes_a_dependency_is_rejected() {
    let file = production_file(
        r#"
        struct Connection;
        struct StoreRecords<'a> { connection: &'a Connection }
        impl StoreRecords<'_> {
            fn conn(&self) -> &Connection { self.connection }
        }
        struct MergeMaterializationTransaction<'a> { records: StoreRecords<'a> }
        impl MergeMaterializationTransaction<'_> {
            fn records(&self) -> StoreRecords<'_> { todo!() }
        }
        "#,
    );

    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 2);
    assert!(leaks.iter().any(|leak| matches!(
        leak,
        OwnerDependencyLeak::Return { owner, dependency, .. }
            if owner == "MergeMaterializationTransaction" && dependency == "StoreRecords"
    )));
}

#[test]
fn runtime_owner_methods_cannot_accept_raw_dependencies() {
    let file = production_file(
        r#"
        struct Connection;
        struct StoreAuthority;
        impl StoreAuthority {
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

    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Parameter { owner, method, dependency, .. }
            if owner == "StoreAuthority"
                && method == "required_root"
                && dependency == "Connection"
    ));
}

#[test]
fn owner_methods_cannot_install_retained_capabilities_through_wrappers() {
    let file = production_file_at(
        "bae-core/src/sync/upload_observer.rs",
        r#"
        struct Database;
        struct ReleaseUploadObserver {
            database: std::sync::OnceLock<std::sync::Arc<Database>>,
        }
        impl ReleaseUploadObserver {
            fn set_database(&self, database: std::sync::Arc<Database>) {
                let _ = self.database.set(database);
            }
        }
        "#,
    );

    let leaks = find_owner_dependency_leaks_with_capabilities(&[file], &["Database".to_string()]);
    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Parameter { owner, method, dependency, .. }
            if owner == "ReleaseUploadObserver"
                && method == "set_database"
                && dependency == "Database"
    ));
}

#[test]
fn retained_service_traits_cannot_return_child_services() {
    let file = production_file(
        r#"
        struct ProviderProbeStorage;
        trait CloudSyncObjectStorage {
            fn provider_probes(&self) -> &ProviderProbeStorage;
        }
        "#,
    );

    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "CloudSyncObjectStorage"
                && method == "provider_probes"
                && dependency == "ProviderProbeStorage"
    ));
}

#[test]
fn storage_traits_cannot_expose_raw_provider_object_operations() {
    let file = production_file_at(
        "crates/coven-storage/src/cloud_object_storage.rs",
        r#"
        trait CloudSyncObjectStorage {
            fn read_provider_object(&self, key: &str) -> Vec<u8>;
            fn write_provider_object(&self, key: &str, bytes: Vec<u8>);
            fn list_provider_objects(&self, prefix: &str) -> Vec<String>;
            fn delete_provider_object(&self, key: &str);
        }
        "#,
    );

    assert_eq!(find_owner_dependency_leaks(&[file]).len(), 4);
}

#[test]
fn retained_service_owners_cannot_expose_any_fields() {
    let file = production_file_at(
        "crates/coven-storage/src/remote/blob_io.rs",
        r#"
        trait ExactSlotStorage {}
        struct BlobRangeReader {
            pub exact: std::sync::Arc<dyn ExactSlotStorage>,
            pub plaintext_size: u64,
        }
        "#,
    );

    assert_eq!(find_owner_dependency_leaks(&[file]).len(), 2);
}

#[test]
fn configured_stateful_capabilities_define_owner_boundaries() {
    let file = production_file_at(
        "bae-core/src/service.rs",
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
    );

    let leaks = find_owner_dependency_leaks_with_capabilities(&[file], &["AtomicBool".to_string()]);
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
    let file = production_file_at(
        "crates/coven-replication/src/sync/test_helpers.rs",
        r#"
        trait CloudSyncObjectStorage {}
        struct TestStoreFixture {
            pub storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
        }
        "#,
    );

    assert_eq!(find_owner_dependency_leaks(&[file]).len(), 1);
}

#[test]
fn transfer_objects_cannot_expose_service_fields() {
    let file = production_file_at(
        "crates/coven-replication/src/sync/store/authorization.rs",
        r#"
        struct StoreDatabase;
        struct Store {
            database: StoreDatabase,
        }
        struct InitializedStore {
            pub(crate) store: Store,
            pub(crate) device_id: String,
        }
        "#,
    );

    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Field { owner, field, .. }
            if owner == "InitializedStore" && field == "store"
    ));
}

#[test]
fn transfer_objects_cannot_be_publicly_exposed_through_another_transfer() {
    let file = production_file_at(
        "crates/coven-replication/src/sync/store/circles/commands.rs",
        r#"
        struct SnapshotDatabaseImage;
        struct CreatedSnapshot {
            image: SnapshotDatabaseImage,
        }
        struct SnapshotCut {
            snapshot: CreatedSnapshot,
        }
        struct CircleAddMemberRequest {
            pub(super) bootstrap: SnapshotCut,
        }
        "#,
    );

    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Field { owner, field, .. }
            if owner == "CircleAddMemberRequest" && field == "bootstrap"
    ));
}

#[test]
fn signing_and_resource_capabilities_cannot_be_exposed_as_fields() {
    let file = production_file_at(
        "crates/coven-replication/src/sync/store/operation.rs",
        r#"
        struct UserKeypair;
        struct SnapshotDatabaseImage;
        struct OwnStreamAuthorship;
        struct OperationState {
            pub(super) signer: UserKeypair,
            pub(crate) snapshot: SnapshotDatabaseImage,
            pub permit: OwnStreamAuthorship,
        }
        "#,
    );

    assert_eq!(find_owner_dependency_leaks(&[file]).len(), 3);
}

#[test]
fn test_support_cannot_return_its_retained_database() {
    let file = production_file_at(
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

    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "SyntheticStore"
                && method == "database"
                && dependency == "Database"
    ));
}

#[test]
fn cfg_test_methods_cannot_return_retained_capabilities() {
    let file = production_file_at(
        "crates/coven/src/store_security.rs",
        r#"
        struct MasterKeyCustody;
        struct EncryptionService;
        struct StoreSecurity {
            custody: MasterKeyCustody,
        }
        impl StoreSecurity {
            #[cfg(test)]
            pub(crate) fn encryption_for_test(&self) -> EncryptionService { todo!() }
        }
        "#,
    );

    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "StoreSecurity"
                && method == "encryption_for_test"
                && dependency == "EncryptionService"
    ));
}

#[test]
fn test_owner_graph_cannot_return_its_retained_services() {
    let file = production_file_at(
        "crates/coven-replication/src/sync/test_owner_graph.rs",
        r#"
        struct StoreDatabase;
        struct StoreDir;
        struct LocalStoreBlobAccess {
            database: StoreDatabase,
            store_dir: StoreDir,
        }
        struct LocalBlobTransitions {
            database: StoreDatabase,
            store_dir: StoreDir,
        }
        struct TestOwnerGraph {
            local_access: LocalStoreBlobAccess,
            local_transitions: LocalBlobTransitions,
        }
        impl TestOwnerGraph {
            pub fn local_access(&self) -> LocalStoreBlobAccess { todo!() }
            pub fn local_transitions(&self) -> LocalBlobTransitions { todo!() }
        }
        "#,
    );

    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 2);
}

#[test]
fn composition_factory_can_return_a_new_service_but_not_a_retained_service() {
    let file = production_file_at(
        "crates/coven-replication/src/sync/test_helpers.rs",
        r#"
        struct CloudSyncConnection;
        struct TestDevice {
            storage: CloudSyncConnection,
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

    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "TestStore"
                && method == "founder_device"
                && dependency == "TestDevice"
    ));
}

#[test]
fn owner_cannot_return_a_retained_capability_outside_the_fixed_dependency_list() {
    let file = production_file_at(
        "crates/coven-replication/src/sync/test_helpers.rs",
        r#"
        struct CloudSyncConnection;
        struct TestOwnerGraph {
            storage: std::sync::Arc<CloudSyncConnection>,
        }
        impl TestOwnerGraph {
            pub fn storage(&self) -> std::sync::Arc<CloudSyncConnection> {
                self.storage.clone()
            }
        }
        "#,
    );

    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "TestOwnerGraph"
                && method == "storage"
                && dependency == "CloudSyncConnection"
    ));
}

#[test]
fn owner_methods_cannot_factory_configured_capabilities_for_callers() {
    let file = production_file_at(
        "bae-core/src/library/manager/config.rs",
        r#"
        struct ConfigHandle;
        struct KeyService;
        struct DiscogsClient;
        struct LibraryManager {
            config: ConfigHandle,
            keys: KeyService,
        }
        impl LibraryManager {
            fn discogs_client(&self) -> DiscogsClient { todo!() }
        }
        "#,
    );

    let leaks = find_owner_dependency_leaks_with_capabilities(
        &[file],
        &[
            "ConfigHandle".to_string(),
            "DiscogsClient".to_string(),
            "KeyService".to_string(),
            "LibraryManager".to_string(),
        ],
    );
    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "LibraryManager"
                && method == "discogs_client"
                && dependency == "DiscogsClient"
    ));
}

#[test]
fn operation_output_exception_does_not_allow_retained_getters() {
    let file = production_file_at(
        "bae-core/src/playback/output.rs",
        r#"
        struct AudioStream;
        struct AudioOutput;
        impl AudioOutput {
            fn create_stream(&self) -> AudioStream { todo!() }
        }
        struct PlaybackService {
            stream: AudioStream,
        }
        impl PlaybackService {
            fn stream(&self) -> AudioStream { todo!() }
        }
        "#,
    );
    let capabilities = &["AudioOutput".to_string(), "AudioStream".to_string()];
    let allowed = &["AudioStream".to_string()];

    let leaks = find_owner_dependency_leaks_with_policy(&[file], capabilities, allowed);
    assert_eq!(leaks.len(), 1);
    assert!(matches!(
        &leaks[0],
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "PlaybackService"
                && method == "stream"
                && dependency == "AudioStream"
    ));
}

#[test]
fn private_owner_helpers_cannot_pass_retained_capabilities_between_owner_methods() {
    let file = production_file_at(
        "crates/coven/src/store_rows.rs",
        r#"
        struct EncryptionService;
        struct MasterKeyCustody;
        struct StoreRows {
            master_keys: MasterKeyCustody,
        }
        impl StoreRows {
            fn routing_encryption(&self) -> EncryptionService { todo!() }
            pub(crate) fn encryption(&self) -> EncryptionService { todo!() }
        }
        "#,
    );

    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 2);
    assert!(leaks.iter().any(|leak| matches!(
        leak,
        OwnerDependencyLeak::Return { owner, method, dependency, .. }
            if owner == "StoreRows"
                && method == "routing_encryption"
                && dependency == "EncryptionService"
    )));
}

#[test]
fn configuration_values_can_resolve_selected_capabilities() {
    let file = production_file_at(
        "crates/coven-keys/src/custody.rs",
        r#"
        struct MasterKeyCustody;
        enum KeyCustody {
            Custom(MasterKeyCustody),
        }
        impl KeyCustody {
            pub fn resolve(self) -> MasterKeyCustody { todo!() }
        }
        "#,
    );

    assert!(find_owner_dependency_leaks(&[file]).is_empty());
}

#[test]
fn methods_cannot_return_key_or_provider_capabilities() {
    let file = production_file_at(
        "crates/coven-storage/src/remote/cipher.rs",
        r#"
        struct CloudCipher;
        struct BlobSpoolProtection;
        trait ExactSlotStorage {}
        trait CloudSyncCipherStateAccess {
            fn snapshot(&self) -> CloudCipher;
        }
        trait CloudSyncObjectStorage {
            fn store_blob_protection(&self) -> BlobSpoolProtection;
        }
        trait CloudHome {
            fn exact_slot_storage(&self) -> std::sync::Arc<dyn ExactSlotStorage>;
        }
        "#,
    );

    assert_eq!(find_owner_dependency_leaks(&[file]).len(), 3);
}

#[test]
fn closed_sessions_and_private_leaf_sql_are_allowed() {
    let file = production_file(
        r#"
        struct Connection;
        struct StoreDir;
        struct StoreRootRef;
        struct StoreSession<'a> {
            connection: &'a Connection,
            store_dir: &'a StoreDir,
        }
        impl StoreSession<'_> {
            fn required_root(&mut self) -> StoreRootRef { todo!() }
        }
        fn load_root_on(connection: &Connection) -> StoreRootRef { todo!() }
        "#,
    );

    assert!(find_owner_dependency_leaks(&[file]).is_empty());
}

#[test]
fn public_free_functions_cannot_expose_raw_database_dependencies() {
    let file = production_file(
        r#"
        struct Connection;
        struct Transaction<'a>(&'a Connection);
        pub fn open_image() -> Connection { todo!() }
        pub fn persist_on(connection: &Connection) { todo!() }
        fn private_leaf(connection: &Connection) { todo!() }
        "#,
    );

    assert_eq!(find_owner_dependency_leaks(&[file]).len(), 2);
}

#[test]
fn production_callables_cannot_return_raw_database_dependencies() {
    let file = production_file(
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

    assert_eq!(find_owner_dependency_leaks(&[file]).len(), 3);
}

#[test]
fn public_database_methods_cannot_expose_raw_database_dependencies() {
    let file = production_file(
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

    assert_eq!(find_owner_dependency_leaks(&[file]).len(), 2);
}

#[test]
fn public_database_traits_cannot_expose_raw_database_dependencies() {
    let file = production_file(
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

    assert_eq!(find_owner_dependency_leaks(&[file]).len(), 2);
}

#[test]
fn consuming_operations_transfer_permits_but_borrowed_getters_still_leak() {
    let file = production_file(
        r#"
        struct OwnStreamAuthorship;
        struct StoreOperationCommitPlan { authorship: OwnStreamAuthorship }
        struct MergeConflictResolutionCommitPlan { plan: StoreOperationCommitPlan }
        impl StoreOperationCommitPlan {
            fn into_authorship(self) -> OwnStreamAuthorship { self.authorship }
            fn authorship(&self) -> &OwnStreamAuthorship { &self.authorship }
            fn borrowed_after_consuming(self) -> Box<&'static OwnStreamAuthorship> { todo!() }
        }
        impl MergeConflictResolutionCommitPlan {
            fn finish(self) -> StoreOperationCommitPlan { self.plan }
        }
    "#,
    );
    let leaks = find_owner_dependency_leaks(&[file]);
    let methods = leaks
        .iter()
        .map(|leak| match leak {
            OwnerDependencyLeak::Return { method, .. } => method.as_str(),
            other => panic!("unexpected finding: {other:?}"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        methods,
        BTreeSet::from(["authorship", "borrowed_after_consuming", "finish"])
    );
}

#[test]
fn consuming_prepared_images_does_not_allow_raw_database_returns_or_wrappers() {
    let file = production_file(
        r#"
        struct Connection;
        struct SnapshotDatabaseImage;
        struct PreparedStoreSnapshot { image: SnapshotDatabaseImage, connection: Connection }
        impl PreparedStoreSnapshot {
            fn connection(&self) -> &Connection { &self.connection }
        }
        struct Database { prepared: PreparedStoreSnapshot, connection: Connection }
        impl Database {
            fn into_prepared(self) -> PreparedStoreSnapshot { self.prepared }
            fn into_connection(self) -> Connection { self.connection }
        }
    "#,
    );
    let leaks = find_owner_dependency_leaks(&[file]);
    let methods = leaks
        .iter()
        .map(|leak| match leak {
            OwnerDependencyLeak::Return { method, .. } => method.as_str(),
            other => panic!("unexpected finding: {other:?}"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        methods,
        BTreeSet::from(["connection", "into_prepared", "into_connection"])
    );
}

#[test]
fn consumed_worker_returns_owned_image_but_cannot_lend_retained_image() {
    let file = production_file(
        r#"
        struct SnapshotDatabaseImage;
        struct PreparedStoreSnapshot { image: SnapshotDatabaseImage }
        struct Database { replies: Sender<PreparedStoreSnapshot> }
        impl Database {
            async fn into_prepared(self) -> Result<PreparedStoreSnapshot, Error> { todo!() }
            fn prepared(&self) -> &PreparedStoreSnapshot { todo!() }
        }
    "#,
    );
    let leaks = find_owner_dependency_leaks(&[file]);
    assert_eq!(leaks.len(), 1);
    assert!(
        matches!(&leaks[0], OwnerDependencyLeak::Return { method, .. } if method == "prepared")
    );
}

#[test]
fn consuming_operations_transfer_closed_plans_and_permits() {
    let file = production_file(
        r#"
        struct OwnStreamAuthorship;
        struct StoreOperationCommitPlan { authorship: OwnStreamAuthorship }
        struct MergeConflictResolutionCommitPlan { plan: StoreOperationCommitPlan }
        impl StoreOperationCommitPlan {
            fn into_authorship(self) -> OwnStreamAuthorship { self.authorship }
        }
        impl MergeConflictResolutionCommitPlan {
            fn finish(self) -> StoreOperationCommitPlan { self.plan }
        }
    "#,
    );
    assert!(find_owner_dependency_leaks(&[file]).is_empty());
}
