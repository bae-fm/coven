use super::*;

/// Every gate in this tool keys on a raw repository path. A path key whose
/// file has been renamed or moved does not fail — it simply stops matching,
/// and the gate it belongs to silently covers nothing. That has happened
/// twice. Resolve every declared key against the working tree so a move
/// that strands one fails here instead of going quiet.
#[test]
fn every_declared_path_key_resolves_to_a_file_in_the_tree() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("tool lives two directories under the repository root")
        .to_path_buf();

    let mut keys: Vec<(String, String)> = Vec::new();
    for (flag, boundary) in CAPABILITY_BOUNDARY_FLAGS {
        for capability in *boundary {
            for home in capability.allowed {
                keys.push((format!("{flag} ({})", capability.kind), (*home).to_string()));
            }
        }
    }
    for (path, owner, method) in COMPOSITION_ROOTS {
        keys.push((
            format!("composition root {owner}::{method}"),
            (*path).to_string(),
        ));
    }
    for path in [DATABASE_MODULE_ROOT, DATABASE_MODULE_DIR, COVEN_SCHEMA_FILE] {
        keys.push(("--database-boundary".to_string(), path.to_string()));
    }

    let stranded: Vec<String> = keys
        .into_iter()
        .filter(|(_, key)| {
            let resolved = root.join(key);
            // A key ending in `/` names a directory prefix; every other key
            // names one file.
            if key.ends_with('/') {
                !resolved.is_dir()
            } else {
                !resolved.is_file()
            }
        })
        .map(|(gate, key)| format!("{gate}: {key}"))
        .collect();

    assert!(
        stranded.is_empty(),
        "path keys no longer resolve, so the gates keyed on them cover nothing:\n{}",
        stranded.join("\n")
    );
}

#[test]
fn every_composition_root_names_an_existing_method() {
    struct MethodCollector<'a> {
        path: &'a str,
        methods: &'a mut BTreeSet<(String, String, String)>,
    }

    impl Visit<'_> for MethodCollector<'_> {
        fn visit_item_impl(&mut self, node: &syn::ItemImpl) {
            let Some(owner) = type_name(&node.self_ty) else {
                return;
            };
            for item in &node.items {
                if let syn::ImplItem::Fn(method) = item {
                    self.methods.insert((
                        self.path.to_string(),
                        owner.clone(),
                        method.sig.ident.to_string(),
                    ));
                }
            }
            visit::visit_item_impl(self, node);
        }
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("tool lives two directories under the repository root");
    let files = rust_files(root).expect("parse repository Rust sources");
    let mut methods = BTreeSet::new();
    for file in &files {
        MethodCollector {
            path: &file.relative_path,
            methods: &mut methods,
        }
        .visit_file(&file.syntax);
    }

    let missing = COMPOSITION_ROOTS
        .iter()
        .filter(|(path, owner, method)| {
            !methods.contains(&(
                (*path).to_string(),
                (*owner).to_string(),
                (*method).to_string(),
            ))
        })
        .map(|(path, owner, method)| format!("{path}: {owner}::{method}"))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "composition roots name methods that do not exist:\n{}",
        missing.join("\n")
    );
}

#[test]
fn test_support_directories_are_test_sources() {
    assert!(is_test_source(
        "crates/coven-database/src/test_support/synthetic_store.rs"
    ));
}

#[test]
fn paths_cannot_skip_over_their_parent_module() {
    let source = syn::parse_file(
        r#"
        use super::super::Sibling;
        fn call() { super::super::run(); }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];

    assert_eq!(find_deep_parent_path_violations(&files).len(), 2);
}

#[test]
fn component_bundle_constructed_only_to_be_destructured_is_rejected() {
    let source = syn::parse_file(
        r#"
        struct ComponentBundle {
            pub(crate) first: First,
            pub(crate) second: Second,
        }

        impl ComponentBundle {
            fn new(first: First, second: Second) -> Self { Self { first, second } }
        }

        fn compose(first: First, second: Second) {
            let ComponentBundle { first, second } = ComponentBundle::new(first, second);
            use_components(first, second);
        }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "crates/coven/src/handle.rs".to_string(),
        syntax: source,
    }];

    let violations = find_transient_component_bundle_violations(&files);
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].bundle, "ComponentBundle");
}

#[test]
fn component_value_with_behavior_is_allowed() {
    let source = syn::parse_file(
        r#"
        struct PreparedComponents {
            pub(crate) first: First,
            pub(crate) second: Second,
        }

        impl PreparedComponents {
            fn new(first: First, second: Second) -> Self { Self { first, second } }
            fn install(self) { use_components(self.first, self.second); }
        }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "crates/coven/src/handle.rs".to_string(),
        syntax: source,
    }];

    assert!(find_transient_component_bundle_violations(&files).is_empty());
}

#[test]
fn operation_scope_exemptions_only_name_operation_authorities() {
    assert_eq!(
        OPERATION_SCOPED_OWNER_TYPES,
        &[
            "AuthorizedStore",
            "AuthorizedWriterOperation",
            "CircleEpochAccess",
            "CirclePackageAccess",
            "CloudOutboxLiveQuery",
            "ColdSnapshotDestination",
            "ColdSnapshotPreparation",
            "DestinationPayloadFiles",
            "DevicePairingHost",
            "HlcTransaction",
            "HostWriteBlobStaging",
            "LiveQuery",
            "OwnStreamAuthorship",
            "PreparedCloudHomeCredentials",
            "PreparedCloudHomeKey",
            "PreparedStoreSnapshot",
            "PreparedSyncLoopRuntime",
            "ReconfigurableLiveQuery",
            "RuntimeSlot",
            "RuntimeSlotState",
            "SnapshotPreparation",
            "StagedCloudHomeCredentials",
            "StagedMasterKeyCustody",
            "StoreOperationCommitPlan",
        ]
    );
    assert!(CAPABILITY_TYPES.contains(&"CircleEpochAccess"));
    for value in [
        "AdmittedStoreCloudConfig",
        "AdmittedStoreCloudHome",
        "BlobSpoolProtection",
        "CircleAckPublicationInput",
        "InitializedStore",
    ] {
        assert!(NON_OWNER_TYPES.contains(&value));
    }
    for facade in ["StoreCircleCommands", "StoreDeviceJoinTransport"] {
        assert!(BORROWED_FACADE_TYPES.contains(&facade));
    }
    for retained in [
        "CurrentRemoteBlobSource",
        "RemoteStoreBlobAccess",
        "Store",
        "StoreBlobCache",
    ] {
        assert!(!OPERATION_SCOPED_OWNER_TYPES.contains(&retained));
    }
}

#[test]
fn external_associated_factories_do_not_match_local_owner_names() {
    let external: syn::ExprCall =
        syn::parse_str("apple_native_keyring_store::protected::Store::new()")
            .expect("parse external factory");
    let syn::Expr::Path(external_path) = external.func.as_ref() else {
        panic!("external factory is a path");
    };
    let external_segments = external_path.path.segments.iter().collect::<Vec<_>>();
    assert!(!could_be_local_associated_function_path(&external_segments));

    for local in ["Store::new()", "crate::sync::store::Store::new()"] {
        let call: syn::ExprCall = syn::parse_str(local).expect("parse local factory");
        let syn::Expr::Path(path) = call.func.as_ref() else {
            panic!("local factory is a path");
        };
        let segments = path.path.segments.iter().collect::<Vec<_>>();
        assert!(could_be_local_associated_function_path(&segments));
    }
}

#[test]
fn retained_owner_runtime_method_cannot_accept_store_dir() {
    let source = syn::parse_file(
        r#"
        struct StoreDir;
        struct StoreDatabase;
        struct StoreRows { database: StoreDatabase, store_dir: StoreDir }

        impl StoreRows {
            fn new(database: StoreDatabase, store_dir: StoreDir) -> Self {
                Self { database, store_dir }
            }

            fn execute(&self, store_dir: &StoreDir) {}
        }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "crates/coven/src/store_rows.rs".to_string(),
        syntax: source,
    }];
    let structs = collect_structs(&files);
    let owners = infer_owners(&structs);
    let constructors = collect_constructors(&files, &owners);
    let violations = find_retained_capability_parameter_violations(&files, &owners, &constructors);

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].owner, "StoreRows");
    assert_eq!(violations[0].method, "execute");
    assert_eq!(violations[0].capability, "StoreDir");
}

#[test]
fn retained_services_are_constructed_only_by_roots_or_lifetime_authorities() {
    let production = syn::parse_file(
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
    )
    .expect("parse production fixture");
    let files = vec![RustFile {
        relative_path: "crates/coven/src/lib.rs".to_string(),
        syntax: production,
    }];
    let declared = collect_declared_types(&files);
    let owners = infer_owners(&declared);
    let retained = collect_root_retained_types(&declared, &owners, &["Root", "SessionOwner"]);
    let violations = find_retained_service_construction_violations(
        &files,
        &retained,
        &[("Session", "SessionOwner")],
        &[("crates/coven/src/lib.rs", "Root", "new")],
    );

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].service, "Child");
    assert_eq!(violations[0].owner, "Wrong");
    assert_eq!(violations[0].method, "build");
}

#[test]
fn lifetime_authority_must_be_retained_by_a_root() {
    let source = syn::parse_file(
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
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];
    let declared = collect_declared_types(&files);
    let owners = infer_owners(&declared);
    let retained = collect_root_retained_types(&declared, &owners, &["Root"]);
    assert!(LIFETIME_CONSTRUCTION_AUTHORITIES.contains(&("SyncLoopHandle", "StoreSync")));
    let violations = find_retained_service_construction_violations(
        &files,
        &retained,
        &[("Child", "DetachedAuthority")],
        &[],
    );

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].service, "Child");
    assert_eq!(violations[0].owner, "DetachedAuthority");
    assert_eq!(violations[0].authority, None);
}

#[test]
fn nested_owner_constructor_is_rejected() {
    let source = syn::parse_file(
        r#"
        struct StoreDatabase;
        struct Child { database: StoreDatabase }
        impl Child { fn new(database: StoreDatabase) -> Self { Self { database } } }
        struct Parent { child: Child }
        impl Parent { fn new(database: StoreDatabase) -> Self { Self { child: Child::new(database) } } }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];
    let structs = collect_structs(&files);
    let owners = infer_owners(&structs);
    let constructors = collect_constructors(&files, &owners);
    let free_constructors = collect_free_constructors(&files, &owners);
    let violations = find_violations(&files, &owners, &constructors, &free_constructors);
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].parent, "Parent::new");
    assert_eq!(violations[0].child, "Child");
}

#[test]
fn operation_scoped_owners_can_compose_operation_scoped_owners() {
    let source = syn::parse_file(
        r#"
        struct StoreDatabase;
        struct ReconfigurableLiveQuery { database: StoreDatabase }
        impl ReconfigurableLiveQuery {
            fn new(database: StoreDatabase) -> Self { Self { database } }
        }
        struct LiveQuery { inner: ReconfigurableLiveQuery }
        impl LiveQuery {
            fn new(database: StoreDatabase) -> Self {
                Self { inner: ReconfigurableLiveQuery::new(database) }
            }
        }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];
    let structs = collect_structs(&files);
    let owners = infer_owners(&structs);
    let constructors = collect_constructors(&files, &owners);
    let free_constructors = collect_free_constructors(&files, &owners);

    assert!(find_violations(&files, &owners, &constructors, &free_constructors).is_empty());
}

#[test]
fn injected_owner_is_accepted() {
    let source = syn::parse_file(
        r#"
        struct StoreDatabase;
        struct Child { database: StoreDatabase }
        impl Child { fn new(database: StoreDatabase) -> Self { Self { database } } }
        struct Parent { child: Child }
        impl Parent { fn new(child: Child) -> Self { Self { child } } }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];
    let structs = collect_structs(&files);
    let owners = infer_owners(&structs);
    let constructors = collect_constructors(&files, &owners);
    let free_constructors = collect_free_constructors(&files, &owners);
    assert!(find_violations(&files, &owners, &constructors, &free_constructors).is_empty());
}

#[test]
fn owner_cannot_return_a_service_retained_by_a_composition_root() {
    let source = syn::parse_file(
        r#"
        struct Database;
        struct StoreBlobAccess { database: Database }
        struct StoreSync { database: Database }
        struct CovenHandle { sync: StoreSync, blobs: StoreBlobAccess }

        impl StoreSync {
            pub(crate) fn blob_access(&self) -> Result<Option<Arc<StoreBlobAccess>>, Error> {
                todo!()
            }
        }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];
    let declared_types = collect_declared_types(&files);
    let owners = infer_owners(&declared_types);
    let retained_services = collect_root_retained_types(&declared_types, &owners, &["CovenHandle"]);
    let violations =
        find_service_return_violations(&files, &retained_services, &owners, &["CovenHandle"]);

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].owner, "StoreSync");
    assert_eq!(violations[0].returned, "StoreBlobAccess");
}

#[test]
fn owner_cannot_return_a_stateful_service_that_no_root_retains() {
    let source = syn::parse_file(
        r#"
        struct Database;
        struct DetachedBlobAccess { database: Database }
        struct StoreSync { database: Database }
        struct CovenHandle { sync: StoreSync }

        impl StoreSync {
            pub(crate) fn blob_access(&self) -> DetachedBlobAccess { todo!() }
        }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];
    let declared_types = collect_declared_types(&files);
    let owners = infer_owners(&declared_types);
    let retained_services = collect_root_retained_types(&declared_types, &owners, &["CovenHandle"]);
    let violations =
        find_service_return_violations(&files, &retained_services, &owners, &["CovenHandle"]);

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].returned, "DetachedBlobAccess");
}

#[test]
fn operation_scoped_values_and_a_services_own_constructor_are_returnable() {
    let source = syn::parse_file(
        r#"
        struct Database;
        struct StoreBlobAccess { database: Database }
        struct StoreSync { database: Database }
        struct CovenHandle { sync: StoreSync, blobs: StoreBlobAccess }
        struct AuthorizedWrite;
        struct Prepared;

        impl StoreBlobAccess {
            fn new(database: Database) -> Self { Self { database } }
        }

        impl StoreSync {
            fn authorize(&self) -> AuthorizedWrite { AuthorizedWrite }
        }

        impl Prepared {
            pub(crate) fn initialize(self) -> StoreBlobAccess { todo!() }
        }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];
    let declared_types = collect_declared_types(&files);
    let owners = infer_owners(&declared_types);
    let retained_services = collect_root_retained_types(&declared_types, &owners, &["CovenHandle"]);

    assert!(
        find_service_return_violations(&files, &retained_services, &owners, &["CovenHandle"],)
            .is_empty()
    );
}

#[test]
fn retained_owner_cannot_return_its_dependency_capability() {
    let production = syn::parse_file(
        r#"
        struct EncryptionService;
        struct CloudHome;
        struct Child { encryption: EncryptionService }
        struct Root { child: Child }
        struct Builder;

        impl Child {
            pub(crate) fn derived_encryption(&self) -> EncryptionService { EncryptionService }
            pub(crate) fn create_cloud_home(&self) -> CloudHome { CloudHome }
        }
        impl Builder {
            fn open() -> Root { todo!() }
        }
        "#,
    )
    .expect("parse production fixture");
    let tests = syn::parse_file(
        r#"
        fn child_fixture() -> Child { todo!() }
        "#,
    )
    .expect("parse test fixture");
    let files = vec![
        RustFile {
            relative_path: "crates/coven/src/lib.rs".to_string(),
            syntax: production,
        },
        RustFile {
            relative_path: "crates/coven/tests/fixture.rs".to_string(),
            syntax: tests,
        },
    ];
    let declared_types = collect_declared_types(&files);
    let owners = infer_owners(&declared_types);
    let retained_services = collect_root_retained_types(&declared_types, &owners, &["Root"]);

    let violations = find_service_return_violations(&files, &retained_services, &owners, &["Root"]);

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].owner, "Child");
    assert_eq!(violations[0].returned, "EncryptionService");
}

#[test]
fn private_inner_representation_is_accepted() {
    let source = syn::parse_file(
        r#"
        struct StoreDatabase;
        struct ParentInner { database: StoreDatabase }
        struct Parent { inner: ParentInner }
        impl Parent { fn new(database: StoreDatabase) -> Self { Self { inner: ParentInner { database } } } }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];
    let structs = collect_structs(&files);
    let owners = infer_owners(&structs);
    let constructors = collect_constructors(&files, &owners);
    let free_constructors = collect_free_constructors(&files, &owners);
    assert!(find_violations(&files, &owners, &constructors, &free_constructors).is_empty());
}

#[test]
fn owner_constructor_cannot_hide_child_construction_behind_a_free_function() {
    let source = syn::parse_file(
        r#"
        struct StoreDatabase;
        struct Child { database: StoreDatabase }
        fn build_child(database: StoreDatabase) -> Child { Child { database } }
        struct Parent { child: Child }
        impl Parent { fn new(database: StoreDatabase) -> Self { Self { child: build_child(database) } } }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];
    let structs = collect_structs(&files);
    let owners = infer_owners(&structs);
    let constructors = collect_constructors(&files, &owners);
    let free_constructors = collect_free_constructors(&files, &owners);
    let violations = find_violations(&files, &owners, &constructors, &free_constructors);
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].child, "Child");
}

#[test]
fn module_qualified_free_factory_is_rejected() {
    let source = syn::parse_file(
        r#"
        struct Database;
        struct Child { database: Database }
        mod factory {
            use super::*;
            pub(super) fn build_child(database: Database) -> Child { Child { database } }
        }
        struct Parent { child: Child }
        impl Parent {
            fn new(database: Database) -> Self {
                Self { child: crate::factory::build_child(database) }
            }
        }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];
    let structs = collect_structs(&files);
    let owners = infer_owners(&structs);
    let constructors = collect_constructors(&files, &owners);
    let free_constructors = collect_free_constructors(&files, &owners);
    let violations = find_violations(&files, &owners, &constructors, &free_constructors);

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].child, "Child");
}

#[test]
fn owner_constructor_cannot_hide_child_construction_behind_an_associated_factory() {
    let source = syn::parse_file(
        r#"
        struct Database;
        struct Child { database: Database }
        struct Parent { child: Child }
        struct ChildFactory;

        impl ChildFactory {
            fn build(database: Database) -> Child { Child { database } }
        }

        impl Parent {
            fn new(database: Database) -> Self {
                Self { child: ChildFactory::build(database) }
            }
        }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];
    let structs = collect_structs(&files);
    let owners = infer_owners(&structs);
    let constructors = collect_constructors(&files, &owners);
    let free_constructors = collect_free_constructors(&files, &owners);
    let violations = find_violations(&files, &owners, &constructors, &free_constructors);

    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].parent, "Parent::new");
    assert_eq!(violations[0].child, "Child");
}

#[test]
fn qualified_call_does_not_match_an_unrelated_free_constructor() {
    let source = syn::parse_file(
        r#"
        struct StoreDatabase;
        struct Database { database: StoreDatabase }
        fn open(database: StoreDatabase) -> Database { Database { database } }

        struct ParsedValue;
        impl ParsedValue { fn open() -> Self { Self } }

        struct Parent { database: StoreDatabase, value: ParsedValue }
        impl Parent {
            fn new(database: StoreDatabase) -> Self {
                Self { database, value: ParsedValue::open() }
            }
        }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "fixture.rs".to_string(),
        syntax: source,
    }];
    let structs = collect_structs(&files);
    let owners = infer_owners(&structs);
    let constructors = collect_constructors(&files, &owners);
    let free_constructors = collect_free_constructors(&files, &owners);

    assert!(find_violations(&files, &owners, &constructors, &free_constructors).is_empty());
}

#[test]
fn sqlite_connection_is_rejected_outside_the_database_module() {
    let source = syn::parse_file(
        r#"
        use rusqlite::Connection;
        fn leak(conn: &Connection) { conn.execute("DELETE FROM notes", []).unwrap(); }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "crates/coven-replication/src/sync/leak.rs".to_string(),
        syntax: source,
    }];
    let violations = find_database_boundary_violations(&files);
    assert_eq!(
        violations
            .iter()
            .map(|violation| violation.kind.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["raw SQLite connection"]),
    );
}

/// The host facade re-exports the database crate's `rusqlite` so a host can
/// name the connection types its own SQL uses. That path names the database
/// crate, not the SQLite crate, so it is not a raw import.
#[test]
fn the_facades_reexport_of_the_database_crates_rusqlite_is_allowed() {
    let source = syn::parse_file("pub use coven_database::rusqlite;").expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "crates/coven/src/lib.rs".to_string(),
        syntax: source,
    }];
    assert!(find_database_boundary_violations(&files).is_empty());
}

#[test]
fn every_raw_sqlite_ownership_path_is_rejected() {
    let source = syn::parse_file(
        r#"
        use rusqlite::{Connection as SqlConnection, Transaction};
        use rusqlite::session::Session;
        use rusqlite::*;
        use rusqlite as sqlite;
        fn qualified() -> rusqlite::Result<()> {
            let _ = rusqlite::Connection::open_in_memory()?;
            Ok(())
        }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "crates/coven-replication/src/sync/leak.rs".to_string(),
        syntax: source,
    }];
    let violations = find_database_boundary_violations(&files);
    assert_eq!(
        violations
            .iter()
            .map(|violation| violation.kind.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "raw SQLite connection",
            "raw SQLite crate import",
            "raw SQLite session",
            "raw SQLite transaction",
            "raw SQLite wildcard import",
        ]),
    );
}

#[test]
fn database_implementation_owns_raw_sqlite() {
    let source = syn::parse_file(
        r#"
        use rusqlite::{Connection, Transaction};
        fn transact(connection: &Connection, transaction: &Transaction<'_>) {}
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "crates/coven-database/src/transaction.rs".to_string(),
        syntax: source,
    }];

    assert!(find_database_boundary_violations(&files).is_empty());
}

#[test]
fn host_sql_and_query_values_are_allowed_outside_the_database_module() {
    let source = syn::parse_file(
        r#"
        fn read(context: SqlReadContext<'_>) -> rusqlite::Result<String> {
            context.query_row(
                "SELECT body FROM notes WHERE id = ?1",
                rusqlite::params!["note-id"],
                |row: &rusqlite::Row<'_>| row.get(0),
            )
        }
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "crates/coven/src/handle.rs".to_string(),
        syntax: source,
    }];

    assert!(find_database_boundary_violations(&files).is_empty());
}

#[test]
fn database_owned_sqlite_reexport_is_allowed() {
    let source = syn::parse_file(
        r#"
        pub use database::rusqlite;
        "#,
    )
    .expect("parse fixture");
    let files = vec![RustFile {
        relative_path: "crates/coven/src/lib.rs".to_string(),
        syntax: source,
    }];

    assert!(find_database_boundary_violations(&files).is_empty());
}

#[test]
fn coven_owned_sql_is_rejected_outside_database() {
    let schema = syn::parse_file(
        r#"
        macro_rules! coven_tables {
            ($visit:ident) => {
                $visit!(protocol_state, "key TEXT PRIMARY KEY");
            };
        }
        macro_rules! coven_routing_tables {
            ($visit:ident) => {
                $visit!(_coven_row_audiences, "table_name TEXT NOT NULL");
            };
        }
        "#,
    )
    .expect("parse schema fixture");
    let leak = syn::parse_file(
        r#"
        fn leak(database: DatabaseTestSql<'_>) {
            database.execute("DELETE FROM protocol_state", []).unwrap();
            database.query("SELECT * FROM _coven_row_audiences", [], |_| Ok(())).unwrap();
        }
        "#,
    )
    .expect("parse leak fixture");
    let files = vec![
        RustFile {
            relative_path: "crates/coven-database/src/coven_schema.rs".to_string(),
            syntax: schema,
        },
        RustFile {
            relative_path: "crates/coven-replication/src/sync/leak.rs".to_string(),
            syntax: leak,
        },
    ];

    let violations = find_database_boundary_violations(&files);
    assert_eq!(
        violations
            .iter()
            .map(|violation| violation.kind.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "Coven-owned SQL for table _coven_row_audiences",
            "Coven-owned SQL for table protocol_state",
        ]),
    );
}

#[test]
fn closed_clock_package_and_image_operations_are_not_retained_service_getters() {
    let syntax = syn::parse_file(
        r#"
        struct Hlc;
        struct HlcTransaction<'a> { clock: &'a Hlc }
        struct CircleEpochAccess;
        enum CirclePackageAccess { Exact(CircleEpochAccess), Historical(String) }
        struct SnapshotDatabaseImage;
        struct PreparedStoreSnapshot { image: SnapshotDatabaseImage }
        struct Database { clock: Hlc, replies: Sender<PreparedStoreSnapshot> }
        impl Hlc {
            pub fn transaction(&self) -> HlcTransaction<'_> { todo!() }
        }
        impl Database {
            pub async fn into_prepared(self) -> PreparedStoreSnapshot { todo!() }
            pub fn circle_package_access(&self) -> CirclePackageAccess { todo!() }
        }
    "#,
    )
    .expect("parse closed operation fixture");
    let files = [RustFile {
        relative_path: "fixture.rs".into(),
        syntax,
    }];
    let types = collect_declared_types(&files);
    let owners = infer_owners(&types);
    let retained = collect_root_retained_types(&types, &owners, &["Database"]);
    assert!(find_service_return_violations(&files, &retained, &owners, &["Database"]).is_empty());
    assert!(find_owner_dependency_leaks(&files).is_empty());
}
