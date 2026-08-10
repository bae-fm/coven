use std::collections::{BTreeMap, BTreeSet};

use syn::visit::Visit;

use super::{
    collect_declared_types, is_test_only, is_test_source, type_name, RustFile, StructInfo,
    TypeNames,
};

const INTERNAL_DEPENDENCY_TYPES: &[&str] = &[
    "BlobDecls",
    "CloudSyncObjectStorage",
    "Connection",
    "DatabaseCore",
    "Gates",
    "Hlc",
    "ProviderProbeStorage",
    "StoreDir",
    "Transaction",
];

const RAW_DATABASE_TYPES: &[&str] = &["Connection", "Transaction"];
const CLOSED_SESSION_TYPES: &[&str] = &["DatabaseSession", "StoreSession"];
const ALWAYS_FORBIDDEN_RETURNS: &[&str] = &[
    "Connection",
    "DatabaseCore",
    "ProviderProbeStorage",
    "Transaction",
];

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum OwnerDependencyLeak {
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
    let mut exposed_dependencies = BTreeMap::<String, BTreeSet<String>>::new();
    loop {
        let before = exposed_dependencies.clone();
        for method in &methods {
            if method.returns_owner {
                continue;
            }
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
    collect_crate_root_session_fields(files, &internal_dependencies, &mut leaks);
    for method in methods {
        if !method.returns_owner {
            let retained = retained_dependencies
                .get(&method.owner)
                .cloned()
                .unwrap_or_default();
            for output in &method.output {
                let returns_retained_dependency = always_forbidden_returns.contains(output)
                    || (internal_dependencies.contains(output) && retained.contains(output));
                let returns_leaking_wrapper = exposed_dependencies
                    .get(output)
                    .is_some_and(|nested| !nested.is_disjoint(&retained));
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
        if is_test_source(&file.relative_path) {
            continue;
        }
        collect_receiver_methods_in_items(&file.relative_path, &file.syntax.items, &mut methods);
    }
    methods
}

fn collect_receiver_methods_in_items(
    path: &str,
    items: &[syn::Item],
    methods: &mut Vec<ReceiverMethod>,
) {
    for item in items {
        match item {
            syn::Item::Impl(item) if !is_test_only(&item.attrs) => {
                let Some(owner) = type_name(&item.self_ty) else {
                    continue;
                };
                for item in &item.items {
                    let syn::ImplItem::Fn(method) = item else {
                        continue;
                    };
                    if method.sig.receiver().is_none() || is_test_only(&method.attrs) {
                        continue;
                    }
                    methods.push(receiver_method(path, &owner, &method.sig));
                }
            }
            syn::Item::Trait(item) if !is_test_only(&item.attrs) => {
                let owner = item.ident.to_string();
                for item in &item.items {
                    let syn::TraitItem::Fn(method) = item else {
                        continue;
                    };
                    if method.sig.receiver().is_none() || is_test_only(&method.attrs) {
                        continue;
                    }
                    methods.push(receiver_method(path, &owner, &method.sig));
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

fn receiver_method(path: &str, owner: &str, signature: &syn::Signature) -> ReceiverMethod {
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
}
