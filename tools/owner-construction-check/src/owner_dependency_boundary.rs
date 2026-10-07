//! An owner never hands out what it holds (§20.2), by returning it or by a
//! public field; callers ask it to do the work. And a method never takes a raw
//! capability, such as a database connection, as a parameter.
//!
//! The rules here read the declared types transitively: what an owner
//! retains is every type its fields reach. A method that returns a retained
//! internal dependency, a wrapper that itself exposes one, or a service the
//! owner retains, leaks it — unless the method consumes a task and hands
//! its product on to the next one.

use std::collections::{BTreeMap, BTreeSet};

use syn::spanned::Spanned;
use syn::visit::Visit;

use crate::database_boundary::RAW_SQLITE_HANDLES;
use crate::policy::Policy;
use crate::syntax::{
    collect_declared_types, is_test_only, is_test_source, type_name, type_names,
    visibility_crosses_owner, RustFile, StructInfo,
};

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
    consumes_owner: bool,
    borrows_output: bool,
}

fn names(lists: &[&[&str]]) -> BTreeSet<String> {
    lists
        .iter()
        .flat_map(|list| list.iter())
        .map(|name| (*name).to_string())
        .collect()
}

pub(crate) fn find_owner_dependency_leaks(
    files: &[RustFile],
    policy: &Policy,
) -> Vec<OwnerDependencyLeak> {
    let methods = collect_receiver_methods(files);
    let declared_types = collect_declared_types(files);
    let raw_database_types = RAW_SQLITE_HANDLES
        .iter()
        .map(|(handle, _)| (*handle).to_string())
        .collect::<BTreeSet<_>>();
    let unexported_capability_types = names(&[policy.unexported_capability_types]);
    let capability_types = names(&[policy.capability_types, policy.unexported_capability_types]);
    let exportable_capability_outputs = names(&[policy.exportable_capability_outputs]);
    let mut internal_dependencies = names(&[policy.internal_dependency_types]);
    internal_dependencies.extend(raw_database_types.iter().cloned());
    let mut always_forbidden_returns = names(&[policy.always_forbidden_returns]);
    always_forbidden_returns.extend(raw_database_types.iter().cloned());
    let retained_dependencies = declared_types
        .keys()
        .map(|owner| {
            (
                owner.clone(),
                transitive_field_types(owner, &declared_types),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let service_owners = infer_field_owners(&declared_types, &capability_types, policy);
    let retained_service_types = service_owners
        .iter()
        .cloned()
        .chain(capability_types.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut exposed_dependencies = BTreeMap::<String, BTreeSet<String>>::new();
    loop {
        let before = exposed_dependencies.clone();
        for method in &methods {
            if method.returns_owner {
                continue;
            }
            let is_composition_root = composition_root_matches(method, policy);
            let owner_is_service =
                service_owners.contains(&method.owner) || capability_types.contains(&method.owner);
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
                        && !is_composition_root
                        && retained_service_types.contains(output)
                        && retained.contains(output)
                        && !transfers_task(method, output, policy))
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

    let mut leaks = BTreeSet::new();
    collect_retained_service_owner_fields(
        files,
        &declared_types,
        &service_owners,
        &retained_service_types,
        &mut leaks,
    );
    collect_crate_root_session_fields(files, &internal_dependencies, policy, &mut leaks);
    collect_database_callables(files, &raw_database_types, policy, &mut leaks);
    for method in methods {
        let is_composition_root = composition_root_matches(&method, policy);
        let owner_is_service =
            service_owners.contains(&method.owner) || capability_types.contains(&method.owner);
        let retained = retained_dependencies
            .get(&method.owner)
            .cloned()
            .unwrap_or_default();
        if policy
            .raw_provider_operations
            .iter()
            .any(|(owner, methods)| {
                method.owner == *owner && methods.contains(&method.method.as_str())
            })
        {
            leaks.insert(OwnerDependencyLeak::RawProviderOperation {
                path: method.path.clone(),
                line: method.line,
                owner: method.owner.clone(),
                method: method.method.clone(),
            });
        }
        if !method.returns_owner {
            for output in &method.output {
                let returns_retained_dependency = always_forbidden_returns.contains(output)
                    || (internal_dependencies.contains(output) && retained.contains(output))
                    || (owner_is_service
                        && !is_composition_root
                        && unexported_capability_types.contains(output)
                        && !exportable_capability_outputs.contains(output))
                    || (owner_is_service
                        && !is_composition_root
                        && retained_service_types.contains(output)
                        && retained.contains(output)
                        && !transfers_task(&method, output, policy))
                    || (owner_is_service
                        && !is_composition_root
                        && returns_derived_service(&method.owner, output, &retained, policy));
                let returns_leaking_wrapper =
                    exposed_dependencies.get(output).is_some_and(|nested| {
                        let leaked = nested.intersection(&retained).collect::<BTreeSet<_>>();
                        leaked
                            .iter()
                            .any(|dependency| always_forbidden_returns.contains(*dependency))
                            || (!is_composition_root && !leaked.is_empty())
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
        for dependency in method
            .parameters
            .intersection(&retained)
            .filter(|dependency| {
                unexported_capability_types.contains(*dependency)
                    && always_forbidden_returns.contains(*dependency)
            })
        {
            leaks.insert(OwnerDependencyLeak::Parameter {
                path: method.path.clone(),
                line: method.line,
                owner: method.owner.clone(),
                method: method.method.clone(),
                dependency: dependency.clone(),
            });
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

/// A consumed task can hand its owned permit or prepared result to the next
/// task. Borrowed getters and wrappers exposing raw dependencies
/// still answer to the ordinary leak rules.
fn transfers_task(method: &ReceiverMethod, output: &str, policy: &Policy) -> bool {
    method.consumes_owner && !method.borrows_output && policy.task_types.contains(&output)
}

fn composition_root_matches(method: &ReceiverMethod, policy: &Policy) -> bool {
    policy.composition_roots.iter().any(|(path, owner, name)| {
        method.path == *path && method.owner == *owner && method.method == *name
    })
}

fn infer_field_owners(
    types: &BTreeMap<String, StructInfo>,
    capability_types: &BTreeSet<String>,
    policy: &Policy,
) -> BTreeSet<String> {
    let capabilities = capability_types
        .iter()
        .cloned()
        .chain(
            policy
                .field_capability_types
                .iter()
                .map(|name| (*name).to_string()),
        )
        .collect::<BTreeSet<_>>();
    let mut owners = BTreeSet::new();
    loop {
        let before = owners.len();
        for (name, info) in types {
            if policy.non_owner_types.contains(&name.as_str())
                || policy.borrowed_facade_types.contains(&name.as_str())
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
                    if !visibility_crosses_owner(&field.vis) {
                        continue;
                    }
                    let exposes_service = type_names(&field.ty).iter().any(|name| {
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

fn returns_derived_service(
    owner: &str,
    output: &str,
    retained: &BTreeSet<String>,
    policy: &Policy,
) -> bool {
    policy
        .derived_services
        .iter()
        .filter(|(derived, _)| *derived == output)
        .flat_map(|(_, sources)| sources.iter())
        .any(|source| owner == *source || retained.contains(*source))
}

/// The SQLite homes' callables: none returns a raw handle, and none reachable
/// outside its crate takes one.
fn collect_database_callables(
    files: &[RustFile],
    raw_database_types: &BTreeSet<String>,
    policy: &Policy,
    leaks: &mut BTreeSet<OwnerDependencyLeak>,
) {
    for file in files {
        if is_test_source(&file.relative_path)
            || !policy
                .capabilities
                .sqlite
                .homes
                .iter()
                .any(|home| file.relative_path.starts_with(home))
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
    let line = signature.ident.span().start().line;
    if let syn::ReturnType::Type(_, output) = &signature.output {
        for dependency in type_names(output).intersection(raw_database_types) {
            leaks.insert(match owner {
                None => OwnerDependencyLeak::FreeReturn {
                    path: path.to_string(),
                    line,
                    function: callable.clone(),
                    dependency: dependency.clone(),
                },
                Some(owner) => OwnerDependencyLeak::Return {
                    path: path.to_string(),
                    line,
                    owner: owner.to_string(),
                    method: callable.clone(),
                    dependency: dependency.clone(),
                },
            });
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
            leaks.insert(match owner {
                None => OwnerDependencyLeak::FreeParameter {
                    path: path.to_string(),
                    line,
                    function: callable.clone(),
                    dependency: dependency.clone(),
                },
                Some(owner) => OwnerDependencyLeak::Parameter {
                    path: path.to_string(),
                    line,
                    owner: owner.to_string(),
                    method: callable.clone(),
                    dependency: dependency.clone(),
                },
            });
        }
    }
}

fn collect_crate_root_session_fields(
    files: &[RustFile],
    internal_dependencies: &BTreeSet<String>,
    policy: &Policy,
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
            if is_test_only(&item.attrs) || !policy.closed_session_types.contains(&session.as_str())
            {
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
                    methods.push(receiver_method(path, &owner, &method.sig));
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
        .flat_map(|input| match input {
            syn::FnArg::Receiver(_) => BTreeSet::new(),
            syn::FnArg::Typed(input) => type_names(&input.ty),
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
        consumes_owner: signature
            .receiver()
            .is_some_and(|receiver| matches!(receiver.kind, syn::ReceiverKind::Value)),
        borrows_output: {
            let mut borrowed = BorrowedOutput(false);
            if let syn::ReturnType::Type(_, output) = &signature.output {
                borrowed.visit_type(output);
            }
            borrowed.0
        },
        output,
        parameters,
    }
}

struct BorrowedOutput(bool);

impl Visit<'_> for BorrowedOutput {
    fn visit_type_reference(&mut self, _: &syn::TypeReference) {
        self.0 = true;
    }

    fn visit_type_ptr(&mut self, _: &syn::TypePtr) {
        self.0 = true;
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

#[cfg(test)]
#[path = "owner_dependency_boundary_tests.rs"]
mod tests;
