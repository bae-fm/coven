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
use crate::finding::Finding;
use crate::owner_graph::OwnerGraph;
use crate::policy::Policy;
use crate::syntax::{
    is_test_only, is_test_source, type_name, type_names, visibility_crosses_owner, RustFile,
};

struct OwnerMethod {
    path: String,
    line: usize,
    owner: String,
    method: String,
    output: BTreeSet<String>,
    parameters: BTreeSet<String>,
    mutates_owner: bool,
    consumes_owner: bool,
    borrows_output: bool,
    public: bool,
}

pub(crate) fn find_owner_dependency_leaks(
    files: &[RustFile],
    policy: &Policy,
    graph: &OwnerGraph,
) -> Vec<Finding> {
    let methods = collect_owner_methods(files);
    let raw = RAW_SQLITE_HANDLES
        .iter()
        .map(|(name, _)| name.to_string())
        .collect::<BTreeSet<_>>();
    let capabilities = policy
        .capability_types
        .iter()
        .map(|name| name.to_string())
        .collect::<BTreeSet<_>>();
    let services = graph
        .owners
        .union(&capabilities)
        .cloned()
        .collect::<BTreeSet<_>>();
    let internal = policy
        .internal_dependency_types
        .iter()
        .map(|name| name.to_string())
        .chain(raw.iter().cloned())
        .collect::<BTreeSet<_>>();
    let retained = methods
        .iter()
        .map(|method| (method.owner.clone(), graph.retained(&method.owner)))
        .collect::<BTreeMap<_, _>>();
    let mut leaks = BTreeSet::new();
    collect_retained_service_owner_fields(files, graph, &graph.owners, &services, &mut leaks);
    collect_crate_root_session_fields(files, &internal, policy, &mut leaks);
    collect_database_callables(files, &raw, policy, &mut leaks);
    // A wrapper's getters expose the same dependencies as returning them directly.
    let mut exposed = BTreeMap::<String, BTreeSet<String>>::new();
    loop {
        let before = exposed.clone();
        for method in &methods {
            let held = &retained[&method.owner];
            let rooted = policy.is_composition_root(&method.path, &method.owner, &method.method);
            for output in &method.output {
                let retained_service = services.contains(output)
                    && held.contains(output)
                    && !transfers_task(method, output, policy);
                let new_service = method.public
                    && graph.owners.contains(output)
                    && !policy.task_types.contains(&output.as_str())
                    && !capabilities.contains(output);
                let leaks_directly = raw.contains(output)
                    || (internal.contains(output) && held.contains(output))
                    || (services.contains(&method.owner)
                        && !rooted
                        && (retained_service || new_service));
                let nested = before
                    .get(output)
                    .into_iter()
                    .flatten()
                    .filter(|name| held.contains(*name))
                    .cloned()
                    .collect::<BTreeSet<_>>();
                if leaks_directly || nested.iter().any(|name| raw.contains(name) || !rooted) {
                    leaks.insert(Finding::new(
                        &method.path,
                        method.line,
                        format!(
                            "{}::{} returns retained dependency {output}",
                            method.owner, method.method
                        ),
                        REMEDY,
                    ));
                }
                let entry = exposed.entry(method.owner.clone()).or_default();
                if leaks_directly {
                    entry.insert(output.clone());
                }
                entry.extend(nested);
            }
        }
        if exposed == before {
            break;
        }
    }
    for method in &methods {
        if method.mutates_owner {
            for dependency in method.parameters.intersection(&raw) {
                leaks.insert(Finding::new(
                    &method.path,
                    method.line,
                    format!(
                        "{}::{} accepts raw dependency {dependency}",
                        method.owner, method.method
                    ),
                    REMEDY,
                ));
            }
        }
    }
    leaks.into_iter().collect()
}

const REMEDY: &str = "an owner never hands out what it holds; callers ask it to do the work";

/// A consumed task can hand its owned permit or prepared result to the next
/// task. Borrowed getters and wrappers exposing raw dependencies
/// still answer to the ordinary leak rules.
fn transfers_task(method: &OwnerMethod, output: &str, policy: &Policy) -> bool {
    method.consumes_owner && !method.borrows_output && policy.task_types.contains(&output)
}

fn collect_retained_service_owner_fields(
    files: &[RustFile],
    graph: &OwnerGraph,
    service_owners: &BTreeSet<String>,
    retained_service_types: &BTreeSet<String>,
    leaks: &mut BTreeSet<Finding>,
) {
    for file in files {
        collect_retained_service_owner_fields_in_items(
            &file.relative_path,
            &file.syntax.items,
            graph,
            service_owners,
            retained_service_types,
            leaks,
        );
    }
}

fn collect_retained_service_owner_fields_in_items(
    path: &str,
    items: &[syn::Item],
    graph: &OwnerGraph,
    service_owners: &BTreeSet<String>,
    retained_service_types: &BTreeSet<String>,
    leaks: &mut BTreeSet<Finding>,
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
                            || !graph.retained(name).is_disjoint(retained_service_types)
                    });
                    if !service_owners.contains(&owner) && !exposes_service {
                        continue;
                    }
                    let line = field.span().start().line;
                    let field = field
                        .ident
                        .as_ref()
                        .map_or_else(|| index.to_string(), ToString::to_string);
                    leaks.insert(Finding::new(
                        path,
                        line,
                        format!("service owner {owner} exposes field {field}"),
                        REMEDY,
                    ));
                }
            }
            syn::Item::Mod(item) if !is_test_only(&item.attrs) => {
                if let Some((_, items)) = &item.content {
                    collect_retained_service_owner_fields_in_items(
                        path,
                        items,
                        graph,
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

/// The SQLite homes' callables: none returns a raw handle, and none reachable
/// outside its crate takes one.
fn collect_database_callables(
    files: &[RustFile],
    raw_database_types: &BTreeSet<String>,
    policy: &Policy,
    leaks: &mut BTreeSet<Finding>,
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
    leaks: &mut BTreeSet<Finding>,
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
    leaks: &mut BTreeSet<Finding>,
) {
    let callable = signature.ident.to_string();
    let line = signature.ident.span().start().line;
    if let syn::ReturnType::Type(_, output) = &signature.output {
        for dependency in type_names(output).intersection(raw_database_types) {
            let callable =
                owner.map_or_else(|| callable.clone(), |owner| format!("{owner}::{callable}"));
            leaks.insert(Finding::new(
                path,
                line,
                format!("{callable} returns retained dependency {dependency}"),
                REMEDY,
            ));
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
            let callable =
                owner.map_or_else(|| callable.clone(), |owner| format!("{owner}::{callable}"));
            leaks.insert(Finding::new(
                path,
                line,
                format!("{callable} accepts raw dependency {dependency}"),
                REMEDY,
            ));
        }
    }
}

fn collect_crate_root_session_fields(
    files: &[RustFile],
    internal_dependencies: &BTreeSet<String>,
    policy: &Policy,
    leaks: &mut BTreeSet<Finding>,
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
                    leaks.insert(Finding::new(&file.relative_path, item.ident.span().start().line,
                        format!("crate-root {session} exposes internal dependency {dependency} to every module"), REMEDY));
                }
            }
        }
    }
}

fn collect_owner_methods(files: &[RustFile]) -> Vec<OwnerMethod> {
    let mut methods = Vec::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        collect_owner_methods_in_items(&file.relative_path, &file.syntax.items, &mut methods);
    }
    methods
}

fn collect_owner_methods_in_items(path: &str, items: &[syn::Item], methods: &mut Vec<OwnerMethod>) {
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
                    methods.push(owner_method(
                        path,
                        &owner,
                        &method.sig,
                        visibility_crosses_owner(&method.vis),
                    ));
                }
            }
            syn::Item::Trait(item) => {
                let owner = item.ident.to_string();
                for item in &item.items {
                    let syn::TraitItem::Fn(method) = item else {
                        continue;
                    };
                    methods.push(owner_method(path, &owner, &method.sig, true));
                }
            }
            syn::Item::Mod(item) if !is_test_only(&item.attrs) => {
                if let Some((_, items)) = &item.content {
                    collect_owner_methods_in_items(path, items, methods);
                }
            }
            _ => {}
        }
    }
}

fn owner_method(path: &str, owner: &str, signature: &syn::Signature, public: bool) -> OwnerMethod {
    let mut output = match &signature.output {
        syn::ReturnType::Default => BTreeSet::new(),
        syn::ReturnType::Type(_, output) => type_names(output),
    };
    output.remove("Self");
    output.remove(owner);
    let parameters = signature
        .inputs
        .iter()
        .flat_map(|input| match input {
            syn::FnArg::Receiver(_) => BTreeSet::new(),
            syn::FnArg::Typed(input) => type_names(&input.ty),
        })
        .collect();
    OwnerMethod {
        public,
        path: path.to_string(),
        line: signature.ident.span().start().line,
        owner: owner.to_string(),
        method: signature.ident.to_string(),
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

#[cfg(test)]
#[path = "owner_dependency_boundary_tests.rs"]
mod tests;
