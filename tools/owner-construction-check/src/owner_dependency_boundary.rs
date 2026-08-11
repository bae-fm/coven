use std::collections::{BTreeMap, BTreeSet};

use syn::spanned::Spanned;
use syn::visit::Visit;

use super::{
    collect_declared_types, is_test_only, is_test_source, type_name, RustFile, StructInfo,
    TypeNames, BORROWED_FACADE_TYPES, CAPABILITY_TYPES, COMPOSITION_ROOTS, NON_OWNER_TYPES,
};

const INTERNAL_DEPENDENCY_TYPES: &[&str] = &[
    "BlobDecls",
    "CloudSyncObjectStorage",
    "Connection",
    "Database",
    "DatabaseConnection",
    "DatabaseCore",
    "Gates",
    "Hlc",
    "ProviderProbeStorage",
    "Session",
    "StoreDir",
    "StoreDatabase",
    "Transaction",
];

const RAW_DATABASE_TYPES: &[&str] = &["Connection", "Session", "Transaction"];
const CLOSED_SESSION_TYPES: &[&str] = &["DatabaseSession", "StoreSession"];
const ALWAYS_FORBIDDEN_RETURNS: &[&str] = &[
    "Connection",
    "Database",
    "DatabaseConnection",
    "DatabaseCore",
    "ExactSlotStorage",
    "ProviderProbeStorage",
    "Session",
    "StoreDatabase",
    "Transaction",
];
const RAW_PROVIDER_OPERATIONS: &[&str] = &[
    "delete_provider_object",
    "list_provider_objects",
    "provider_object_exists",
    "read_provider_object",
    "write_provider_object",
];

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum OwnerDependencyLeak {
    Field {
        path: String,
        line: usize,
        owner: String,
        field: String,
    },
    CrateRootSessionField {
        path: String,
        line: usize,
        session: String,
        dependency: String,
    },
    Return {
        path: String,
        line: usize,
        owner: String,
        method: String,
        dependency: String,
    },
    Parameter {
        path: String,
        line: usize,
        owner: String,
        method: String,
        dependency: String,
    },
    RawProviderOperation {
        path: String,
        line: usize,
        owner: String,
        method: String,
    },
    FreeReturn {
        path: String,
        line: usize,
        function: String,
        dependency: String,
    },
    FreeParameter {
        path: String,
        line: usize,
        function: String,
        dependency: String,
    },
}

struct ReceiverMethod {
    path: String,
    line: usize,
    owner: String,
    method: String,
    output: BTreeSet<String>,
    parameters: BTreeSet<String>,
    returns_owner: bool,
    mutates_owner: bool,
    crosses_owner: bool,
}

pub(crate) fn find_owner_dependency_leaks(files: &[RustFile]) -> Vec<OwnerDependencyLeak> {
    let methods = collect_receiver_methods(files);
    let declared_types = collect_declared_types(files);
    let internal_dependencies = INTERNAL_DEPENDENCY_TYPES
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();
    let always_forbidden_returns = ALWAYS_FORBIDDEN_RETURNS
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();
    let retained_dependencies = declared_types
        .keys()
        .map(|owner| {
            (
                owner.clone(),
                transitive_field_types(owner, &declared_types),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let service_owners = infer_field_owners(&declared_types);
    let retained_service_types = service_owners
        .iter()
        .cloned()
        .chain(CAPABILITY_TYPES.iter().map(|name| (*name).to_string()))
        .collect::<BTreeSet<_>>();
    let mut exposed_dependencies = BTreeMap::<String, BTreeSet<String>>::new();
    loop {
        let before = exposed_dependencies.clone();
        for method in &methods {
            if method.returns_owner {
                continue;
            }
            let is_composition_root = composition_root_matches(method);
            let owner_is_service = service_owners.contains(&method.owner)
                || CAPABILITY_TYPES.contains(&method.owner.as_str());
            let retained = retained_dependencies
                .get(&method.owner)
                .cloned()
                .unwrap_or_default();
            let exposed = exposed_dependencies
                .entry(method.owner.clone())
                .or_default();
            for output in &method.output {
                if always_forbidden_returns.contains(output)
                    || (internal_dependencies.contains(output) && retained.contains(output))
                    || (owner_is_service
                        && method.crosses_owner
                        && !is_composition_root
                        && retained_service_types.contains(output)
                        && retained.contains(output))
                {
                    exposed.insert(output.clone());
                }
                if let Some(nested) = before.get(output) {
                    exposed.extend(nested.intersection(&retained).cloned());
                }
            }
        }
        if exposed_dependencies == before {
            break;
        }
    }

    let raw_database_types = RAW_DATABASE_TYPES
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();
    let mut leaks = BTreeSet::new();
    collect_retained_service_owner_fields(
        files,
        &declared_types,
        &service_owners,
        &retained_service_types,
        &mut leaks,
    );
    collect_crate_root_session_fields(files, &internal_dependencies, &mut leaks);
    collect_database_callables(files, &raw_database_types, &mut leaks);
    for method in methods {
        let is_composition_root = composition_root_matches(&method);
        let owner_is_service = service_owners.contains(&method.owner)
            || CAPABILITY_TYPES.contains(&method.owner.as_str());
        if method.owner == "CloudSyncObjectStorage"
            && RAW_PROVIDER_OPERATIONS.contains(&method.method.as_str())
        {
            leaks.insert(OwnerDependencyLeak::RawProviderOperation {
                path: method.path.clone(),
                line: method.line,
                owner: method.owner.clone(),
                method: method.method.clone(),
            });
        }
        if !method.returns_owner {
            let retained = retained_dependencies
                .get(&method.owner)
                .cloned()
                .unwrap_or_default();
            for output in &method.output {
                let returns_retained_dependency = always_forbidden_returns.contains(output)
                    || (internal_dependencies.contains(output) && retained.contains(output))
                    || (owner_is_service
                        && method.crosses_owner
                        && !is_composition_root
                        && retained_service_types.contains(output)
                        && retained.contains(output))
                    || (owner_is_service
                        && method.crosses_owner
                        && !is_composition_root
                        && returns_sensitive_derived_service(&method.owner, output, &retained));
                let returns_leaking_wrapper =
                    exposed_dependencies.get(output).is_some_and(|nested| {
                        let leaked = nested.intersection(&retained).collect::<BTreeSet<_>>();
                        leaked
                            .iter()
                            .any(|dependency| always_forbidden_returns.contains(*dependency))
                            || (method.crosses_owner && !is_composition_root && !leaked.is_empty())
                    });
                if returns_retained_dependency || returns_leaking_wrapper {
                    leaks.insert(OwnerDependencyLeak::Return {
                        path: method.path.clone(),
                        line: method.line,
                        owner: method.owner.clone(),
                        method: method.method.clone(),
                        dependency: output.clone(),
                    });
                }
            }
        }
        if method.mutates_owner {
            for dependency in method.parameters.intersection(&raw_database_types) {
                leaks.insert(OwnerDependencyLeak::Parameter {
                    path: method.path.clone(),
                    line: method.line,
                    owner: method.owner.clone(),
                    method: method.method.clone(),
                    dependency: dependency.clone(),
                });
            }
        }
    }
    leaks.into_iter().collect()
}

fn composition_root_matches(method: &ReceiverMethod) -> bool {
    COMPOSITION_ROOTS.iter().any(|(path, owner, name)| {
        method.path == *path && method.owner == *owner && method.method == *name
    })
}

fn infer_field_owners(types: &BTreeMap<String, StructInfo>) -> BTreeSet<String> {
    let capabilities = CAPABILITY_TYPES
        .iter()
        .chain([
            &"CloudCipher",
            &"CloudKitOps",
            &"CloudSyncCipherStateAccess",
            &"ExactSlotStorage",
            &"OAuthSession",
            &"SealedBlobOpener",
        ])
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();
    let mut owners = BTreeSet::new();
    loop {
        let before = owners.len();
        for (name, info) in types {
            if NON_OWNER_TYPES.contains(&name.as_str())
                || BORROWED_FACADE_TYPES.contains(&name.as_str())
            {
                continue;
            }
            if info
                .field_types
                .iter()
                .any(|field| capabilities.contains(field) || owners.contains(field))
            {
                owners.insert(name.clone());
            }
        }
        if owners.len() == before {
            return owners;
        }
    }
}

fn collect_retained_service_owner_fields(
    files: &[RustFile],
    declared_types: &BTreeMap<String, StructInfo>,
    service_owners: &BTreeSet<String>,
    retained_service_types: &BTreeSet<String>,
    leaks: &mut BTreeSet<OwnerDependencyLeak>,
) {
    for file in files {
        collect_retained_service_owner_fields_in_items(
            &file.relative_path,
            &file.syntax.items,
            declared_types,
            service_owners,
            retained_service_types,
            leaks,
        );
    }
}

fn collect_retained_service_owner_fields_in_items(
    path: &str,
    items: &[syn::Item],
    declared_types: &BTreeMap<String, StructInfo>,
    service_owners: &BTreeSet<String>,
    retained_service_types: &BTreeSet<String>,
    leaks: &mut BTreeSet<OwnerDependencyLeak>,
) {
    for item in items {
        match item {
            syn::Item::Struct(item) if !is_test_only(&item.attrs) => {
                let owner = item.ident.to_string();
                for (index, field) in item.fields.iter().enumerate() {
                    if !super::visibility_crosses_owner(&field.vis) {
                        continue;
                    }
                    let field_types = type_names(&field.ty);
                    let exposes_service = field_types.iter().any(|name| {
                        retained_service_types.contains(name)
                            || !transitive_field_types(name, declared_types)
                                .is_disjoint(retained_service_types)
                    });
                    if !service_owners.contains(&owner) && !exposes_service {
                        continue;
                    }
                    leaks.insert(OwnerDependencyLeak::Field {
                        path: path.to_string(),
                        line: field.span().start().line,
                        owner: owner.clone(),
                        field: field
                            .ident
                            .as_ref()
                            .map_or_else(|| index.to_string(), ToString::to_string),
                    });
                }
            }
            syn::Item::Mod(item) if !is_test_only(&item.attrs) => {
                if let Some((_, items)) = &item.content {
                    collect_retained_service_owner_fields_in_items(
                        path,
                        items,
                        declared_types,
                        service_owners,
                        retained_service_types,
                        leaks,
                    );
                }
            }
            _ => {}
        }
    }
}

fn returns_sensitive_derived_service(
    owner: &str,
    output: &str,
    retained: &BTreeSet<String>,
) -> bool {
    let owner_or_retained = |dependency: &str| owner == dependency || retained.contains(dependency);
    match output {
        "BlobSpoolProtection" => ["CloudSyncConnection", "CloudSyncObjectStorage"]
            .iter()
            .any(|dependency| owner_or_retained(dependency)),
        "CloudCipher" | "EncryptionService" => [
            "CloudCipher",
            "CloudSyncCipherStateAccess",
            "CloudSyncConnection",
            "MasterKeyCustody",
            "StoreSecurity",
        ]
        .iter()
        .any(|dependency| owner_or_retained(dependency)),
        _ => false,
    }
}

fn collect_database_callables(
    files: &[RustFile],
    raw_database_types: &BTreeSet<String>,
    leaks: &mut BTreeSet<OwnerDependencyLeak>,
) {
    for file in files {
        if is_test_source(&file.relative_path)
            || !file.relative_path.starts_with("crates/coven-database/src/")
        {
            continue;
        }
        collect_database_callables_in_items(
            &file.relative_path,
            &file.syntax.items,
            raw_database_types,
            leaks,
        );
    }
}

fn collect_database_callables_in_items(
    path: &str,
    items: &[syn::Item],
    raw_database_types: &BTreeSet<String>,
    leaks: &mut BTreeSet<OwnerDependencyLeak>,
) {
    for item in items {
        match item {
            syn::Item::Fn(function) if !is_test_only(&function.attrs) => {
                collect_database_signature(
                    path,
                    None,
                    &function.sig,
                    matches!(function.vis, syn::Visibility::Public(_)),
                    raw_database_types,
                    leaks,
                );
            }
            syn::Item::Impl(implementation) if !is_test_only(&implementation.attrs) => {
                let Some(owner) = type_name(&implementation.self_ty) else {
                    continue;
                };
                for item in &implementation.items {
                    let syn::ImplItem::Fn(method) = item else {
                        continue;
                    };
                    if is_test_only(&method.attrs) {
                        continue;
                    }
                    collect_database_signature(
                        path,
                        Some(&owner),
                        &method.sig,
                        matches!(method.vis, syn::Visibility::Public(_)),
                        raw_database_types,
                        leaks,
                    );
                }
            }
            syn::Item::Trait(trait_item)
                if !is_test_only(&trait_item.attrs)
                    && matches!(trait_item.vis, syn::Visibility::Public(_)) =>
            {
                let owner = trait_item.ident.to_string();
                for item in &trait_item.items {
                    let syn::TraitItem::Fn(method) = item else {
                        continue;
                    };
                    if is_test_only(&method.attrs) {
                        continue;
                    }
                    collect_database_signature(
                        path,
                        Some(&owner),
                        &method.sig,
                        true,
                        raw_database_types,
                        leaks,
                    );
                }
            }
            syn::Item::Mod(module) if !is_test_only(&module.attrs) => {
                if let Some((_, items)) = &module.content {
                    collect_database_callables_in_items(path, items, raw_database_types, leaks);
                }
            }
            _ => {}
        }
    }
}

fn collect_database_signature(
    path: &str,
    owner: Option<&str>,
    signature: &syn::Signature,
    expose_parameters: bool,
    raw_database_types: &BTreeSet<String>,
    leaks: &mut BTreeSet<OwnerDependencyLeak>,
) {
    let callable = signature.ident.to_string();
    if let syn::ReturnType::Type(_, output) = &signature.output {
        for dependency in type_names(output).intersection(raw_database_types) {
            let leak = owner.map_or_else(
                || OwnerDependencyLeak::FreeReturn {
                    path: path.to_string(),
                    line: signature.ident.span().start().line,
                    function: callable.clone(),
                    dependency: dependency.clone(),
                },
                |owner| OwnerDependencyLeak::Return {
                    path: path.to_string(),
                    line: signature.ident.span().start().line,
                    owner: owner.to_string(),
                    method: callable.clone(),
                    dependency: dependency.clone(),
                },
            );
            leaks.insert(leak);
        }
    }
    if !expose_parameters {
        return;
    }
    for input in &signature.inputs {
        let syn::FnArg::Typed(input) = input else {
            continue;
        };
        for dependency in type_names(&input.ty).intersection(raw_database_types) {
            let leak = owner.map_or_else(
                || OwnerDependencyLeak::FreeParameter {
                    path: path.to_string(),
                    line: signature.ident.span().start().line,
                    function: callable.clone(),
                    dependency: dependency.clone(),
                },
                |owner| OwnerDependencyLeak::Parameter {
                    path: path.to_string(),
                    line: signature.ident.span().start().line,
                    owner: owner.to_string(),
                    method: callable.clone(),
                    dependency: dependency.clone(),
                },
            );
            leaks.insert(leak);
        }
    }
}

fn collect_crate_root_session_fields(
    files: &[RustFile],
    internal_dependencies: &BTreeSet<String>,
    leaks: &mut BTreeSet<OwnerDependencyLeak>,
) {
    for file in files {
        if is_test_source(&file.relative_path)
            || !(file.relative_path.ends_with("/src/lib.rs")
                || file.relative_path.ends_with("/src/main.rs"))
        {
            continue;
        }
        for item in &file.syntax.items {
            let syn::Item::Struct(item) = item else {
                continue;
            };
            let session = item.ident.to_string();
            if is_test_only(&item.attrs) || !CLOSED_SESSION_TYPES.contains(&session.as_str()) {
                continue;
            }
            for field in &item.fields {
                for dependency in type_names(&field.ty).intersection(internal_dependencies) {
                    leaks.insert(OwnerDependencyLeak::CrateRootSessionField {
                        path: file.relative_path.clone(),
                        line: item.ident.span().start().line,
                        session: session.clone(),
                        dependency: dependency.clone(),
                    });
                }
            }
        }
    }
}

fn collect_receiver_methods(files: &[RustFile]) -> Vec<ReceiverMethod> {
    let mut methods = Vec::new();
    for file in files {
        if is_test_source(&file.relative_path) && !is_test_support_source(&file.relative_path) {
            continue;
        }
        collect_receiver_methods_in_items(&file.relative_path, &file.syntax.items, &mut methods);
    }
    methods
}

fn is_test_support_source(path: &str) -> bool {
    path.contains("/test_support/")
        || path.ends_with("/test_helpers.rs")
        || path.ends_with("/test_owner_graph.rs")
        || path.ends_with("/test_support.rs")
}

fn collect_receiver_methods_in_items(
    path: &str,
    items: &[syn::Item],
    methods: &mut Vec<ReceiverMethod>,
) {
    for item in items {
        match item {
            syn::Item::Impl(item) => {
                let Some(owner) = type_name(&item.self_ty) else {
                    continue;
                };
                for item in &item.items {
                    let syn::ImplItem::Fn(method) = item else {
                        continue;
                    };
                    if method.sig.receiver().is_none() {
                        continue;
                    }
                    methods.push(receiver_method(
                        path,
                        &owner,
                        &method.sig,
                        super::visibility_crosses_owner(&method.vis),
                    ));
                }
            }
            syn::Item::Trait(item) => {
                let owner = item.ident.to_string();
                for item in &item.items {
                    let syn::TraitItem::Fn(method) = item else {
                        continue;
                    };
                    if method.sig.receiver().is_none() {
                        continue;
                    }
                    methods.push(receiver_method(path, &owner, &method.sig, true));
                }
            }
            syn::Item::Mod(item) if !is_test_only(&item.attrs) => {
                if let Some((_, items)) = &item.content {
                    collect_receiver_methods_in_items(path, items, methods);
                }
            }
            _ => {}
        }
    }
}

fn receiver_method(
    path: &str,
    owner: &str,
    signature: &syn::Signature,
    crosses_owner: bool,
) -> ReceiverMethod {
    let output = match &signature.output {
        syn::ReturnType::Default => BTreeSet::new(),
        syn::ReturnType::Type(_, output) => type_names(output),
    };
    let parameters = signature
        .inputs
        .iter()
        .filter_map(|input| match input {
            syn::FnArg::Receiver(_) => None,
            syn::FnArg::Typed(input) => direct_type_name(&input.ty),
        })
        .collect();
    ReceiverMethod {
        path: path.to_string(),
        line: signature.ident.span().start().line,
        owner: owner.to_string(),
        method: signature.ident.to_string(),
        returns_owner: output.contains("Self") || output.contains(owner),
        mutates_owner: signature.receiver().is_some_and(|receiver| {
            receiver.mutability.is_some()
                || matches!(&receiver.kind, syn::ReceiverKind::Reference(_, _, Some(_)))
        }),
        crosses_owner,
        output,
        parameters,
    }
}

fn transitive_field_types(
    owner: &str,
    declared_types: &BTreeMap<String, StructInfo>,
) -> BTreeSet<String> {
    let mut fields = BTreeSet::new();
    let mut pending = vec![owner.to_string()];
    while let Some(current) = pending.pop() {
        let Some(info) = declared_types.get(&current) else {
            continue;
        };
        for field in &info.field_types {
            if fields.insert(field.clone()) && declared_types.contains_key(field) {
                pending.push(field.clone());
            }
        }
    }
    fields
}

fn direct_type_name(ty: &syn::Type) -> Option<String> {
    match ty {
        syn::Type::Reference(reference) => direct_type_name(&reference.elem),
        syn::Type::Path(_) => type_name(ty),
        _ => None,
    }
}

fn type_names(ty: &syn::Type) -> BTreeSet<String> {
    let mut names = TypeNames::default();
    names.visit_type(ty);
    names.names
}

#[cfg(test)]
mod tests {
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
    fn private_owner_helpers_can_pass_capabilities_between_owner_methods() {
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
        assert_eq!(leaks.len(), 1);
        assert!(matches!(
            &leaks[0],
            OwnerDependencyLeak::Return { owner, method, dependency, .. }
                if owner == "StoreRows"
                    && method == "encryption"
                    && dependency == "EncryptionService"
        ));
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
}
