//! An owner never hands out what it holds (Appendix F.3), by returning it, by a
//! public field, or as an argument its caller's callback can keep
//! (`kept_callback_arguments`); callers ask it to do the work. And a method
//! never takes a raw capability, such as a database connection, as a
//! parameter.
//!
//! A handle is the one thing an owner hands out on purpose: its owner returns
//! it, and so does an owner holding that owner, at any depth, passing up a
//! fresh one it asked for. Nothing that holds the handle itself returns it,
//! not even a holder it was given to; what the owner keeps of its own
//! handles is the owner's, not its holder's. Shared state leaves a type only inside
//! a handle: no method that crosses its module returns a bare receiver or
//! lock, whether it holds that one or just made it. A lock an owner may keep
//! to itself (`Policy::private_state_types`) leaves it when a method returns
//! it through a shared pointer or a reference.
//!
//! The rules here read the declared types transitively: what an owner
//! retains is every type its fields reach. A method that returns a retained
//! internal dependency, a wrapper that itself exposes one, or a service the
//! owner retains, leaks it — unless the method consumes a task and hands
//! its product on to the next one. It reads no test code (`test_code.rs`).

use std::collections::{BTreeMap, BTreeSet};

use syn::spanned::Spanned;
use syn::visit::Visit;

use crate::database_boundary::RAW_SQLITE_HANDLES;
use crate::finding::Finding;
use crate::owner_graph::OwnerGraph;
use crate::policy::Policy;
use crate::syntax::{
    kept_callback_arguments, shared_type_names, supplied_type_names, type_name, type_names,
    visibility_crosses_owner, RustFile,
};
use crate::type_identity::TypeIdentity;

struct OwnerMethod {
    path: String,
    line: usize,
    owner: String,
    method: String,
    output: BTreeSet<String>,
    /// What the method's callbacks can keep of the arguments it gives them
    /// (`kept_callback_arguments`): handed out, as its output is.
    callback_output: BTreeSet<String>,
    /// What the output and those arguments hold through a shared pointer or
    /// a reference.
    shared_output: BTreeSet<String>,
    parameters: BTreeSet<String>,
    mutates_owner: bool,
    consumes_owner: bool,
    borrows_output: bool,
    /// Whether a kept callback argument is a reference or a pointer.
    borrows_callback_output: bool,
    public: bool,
}

/// How a method hands a value out: as what it returns, or as an argument
/// its caller's callback can keep.
#[derive(Clone, Copy)]
enum Route {
    Return,
    Callback,
}

impl OwnerMethod {
    /// Everything the method hands out, each by its route. A type both
    /// returned and given to a callback is named by its return.
    fn handed_out(&self) -> impl Iterator<Item = (&String, Route)> {
        self.output.iter().map(|output| (output, Route::Return)).chain(
            self.callback_output
                .iter()
                .filter(|output| !self.output.contains(*output))
                .map(|output| (output, Route::Callback)),
        )
    }
}

pub(crate) fn find_owner_dependency_leaks(
    files: &[RustFile],
    policy: &Policy,
    graph: &OwnerGraph,
) -> Vec<Finding> {
    let identity = &graph.identity;
    let methods = collect_owner_methods(files, &policy.shared_pointer_types, identity);
    // A rusqlite handle goes by its path where a workspace type has its name
    // (`type_identity.rs`).
    let raw = RAW_SQLITE_HANDLES
        .iter()
        .map(|(name, _)| {
            if graph.types.contains_key(*name) {
                format!("rusqlite::{name}")
            } else {
                name.to_string()
            }
        })
        .collect::<BTreeSet<_>>();
    let capabilities = policy.held_capabilities();
    let services = graph.owners.union(&capabilities).cloned().collect::<BTreeSet<_>>();
    let internal = policy.internal_dependency_types.union(&raw).cloned().collect::<BTreeSet<_>>();
    let retained = methods
        .iter()
        .map(|method| (method.owner.clone(), graph.retained(&method.owner)))
        .collect::<BTreeMap<_, _>>();
    let mut leaks = BTreeSet::new();
    collect_retained_service_owner_fields(files, graph, &services, identity, &mut leaks);
    collect_crate_root_session_fields(files, &internal, policy, identity, &mut leaks);
    collect_database_callables(files, &raw, policy, identity, &mut leaks);
    // A wrapper's getters expose the same dependencies as returning them directly.
    let mut exposed = BTreeMap::<String, BTreeSet<String>>::new();
    loop {
        let before = exposed.clone();
        for method in &methods {
            let held = &retained[&method.owner];
            let rooted = policy.is_composition_root(&method.path, &method.owner, &method.method);
            for (output, route) in method.handed_out() {
                if let Some(maker) = policy.handles.get(output) {
                    let message = if rooted || policy.makes_handle(&method.owner, output) {
                        None
                    } else if graph.retained_around(&method.owner, Some(maker)).contains(output) {
                        Some(format!(
                            "{}::{} hands out handle {output}, which it holds; only {maker} makes one",
                            method.owner, method.method
                        ))
                    } else if !held.contains(maker) {
                        Some(format!(
                            "{}::{} hands out handle {output}, which only {maker} and what holds it hand out",
                            method.owner, method.method
                        ))
                    } else {
                        None
                    };
                    if let Some(message) = message {
                        leaks.insert(Finding::new(
                            &method.path,
                            method.line,
                            message,
                            HANDLE_REMEDY,
                        ));
                    }
                    continue;
                }
                let shared_state = policy.shared_state_types.contains(output)
                    || (policy.private_state_types.contains(output)
                        && method.shared_output.contains(output));
                if shared_state {
                    if method.public && !rooted {
                        leaks.insert(Finding::new(
                            &method.path,
                            method.line,
                            format!(
                                "{}::{} hands out shared state {output}",
                                method.owner, method.method
                            ),
                            SHARED_STATE_REMEDY,
                        ));
                    }
                    continue;
                }
                let retained_service = services.contains(output)
                    && held.contains(output)
                    && !transfers_task(method, route, output, policy);
                let new_service = method.public
                    && graph.owners.contains(output)
                    && !policy.task_types.contains(output)
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
                        match route {
                            Route::Return => format!(
                                "{}::{} returns retained dependency {output}",
                                method.owner, method.method
                            ),
                            Route::Callback => format!(
                                "{}::{} hands its callback retained dependency {output}",
                                method.owner, method.method
                            ),
                        },
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

const HANDLE_REMEDY: &str =
    "a handle's owner makes it, and what holds that owner asks it for a fresh one; a holder receives one when it is built and keeps it";

const SHARED_STATE_REMEDY: &str =
    "shared state leaves its owner only inside a handle the policy names with that owner";

/// A consumed task can hand its owned permit or prepared result to the next
/// task. Borrowed getters and wrappers exposing raw dependencies
/// still answer to the ordinary leak rules.
fn transfers_task(method: &OwnerMethod, route: Route, output: &str, policy: &Policy) -> bool {
    let borrows = match route {
        Route::Return => method.borrows_output,
        Route::Callback => method.borrows_callback_output,
    };
    method.consumes_owner && !borrows && policy.task_types.contains(output)
}

fn collect_retained_service_owner_fields(
    files: &[RustFile],
    graph: &OwnerGraph,
    retained_service_types: &BTreeSet<String>,
    identity: &TypeIdentity,
    leaks: &mut BTreeSet<Finding>,
) {
    for file in files {
        collect_retained_service_owner_fields_in_items(
            &file.relative_path,
            &file.syntax.items,
            graph,
            retained_service_types,
            identity,
            leaks,
        );
    }
}

fn collect_retained_service_owner_fields_in_items(
    path: &str,
    items: &[syn::Item],
    graph: &OwnerGraph,
    retained_service_types: &BTreeSet<String>,
    identity: &TypeIdentity,
    leaks: &mut BTreeSet<Finding>,
) {
    for item in items {
        match item {
            syn::Item::Struct(item) => {
                let owner = item.ident.to_string();
                for (index, field) in item.fields.iter().enumerate() {
                    if !visibility_crosses_owner(&field.vis) {
                        continue;
                    }
                    let exposes_service =
                        supplied_type_names(&field.ty, identity.in_file(path)).iter().any(|name| {
                            retained_service_types.contains(name)
                                || !graph.retained(name).is_disjoint(retained_service_types)
                        });
                    if !graph.owners.contains(&owner) && !exposes_service {
                        continue;
                    }
                    let line = field.span().start().line;
                    let field =
                        field.ident.as_ref().map_or_else(|| index.to_string(), ToString::to_string);
                    leaks.insert(Finding::new(
                        path,
                        line,
                        format!("service owner {owner} exposes field {field}"),
                        REMEDY,
                    ));
                }
            }
            syn::Item::Mod(item) => {
                if let Some((_, items)) = &item.content {
                    collect_retained_service_owner_fields_in_items(
                        path,
                        items,
                        graph,
                        retained_service_types,
                        identity,
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
    identity: &TypeIdentity,
    leaks: &mut BTreeSet<Finding>,
) {
    for file in files {
        if !policy.database_homes().iter().any(|home| file.relative_path.starts_with(home.as_str()))
        {
            continue;
        }
        collect_database_callables_in_items(
            &file.relative_path,
            &file.syntax.items,
            raw_database_types,
            identity,
            leaks,
        );
    }
}

fn collect_database_callables_in_items(
    path: &str,
    items: &[syn::Item],
    raw_database_types: &BTreeSet<String>,
    identity: &TypeIdentity,
    leaks: &mut BTreeSet<Finding>,
) {
    for item in items {
        match item {
            syn::Item::Fn(function) => {
                collect_database_signature(
                    path,
                    None,
                    &function.sig,
                    matches!(function.vis, syn::Visibility::Public(_)),
                    raw_database_types,
                    identity,
                    leaks,
                );
            }
            syn::Item::Impl(implementation) => {
                let Some(owner) = type_name(&implementation.self_ty) else {
                    continue;
                };
                for item in &implementation.items {
                    let syn::ImplItem::Fn(method) = item else {
                        continue;
                    };
                    collect_database_signature(
                        path,
                        Some(&owner),
                        &method.sig,
                        matches!(method.vis, syn::Visibility::Public(_)),
                        raw_database_types,
                        identity,
                        leaks,
                    );
                }
            }
            syn::Item::Trait(trait_item)
                if matches!(trait_item.vis, syn::Visibility::Public(_)) =>
            {
                let owner = trait_item.ident.to_string();
                for item in &trait_item.items {
                    let syn::TraitItem::Fn(method) = item else {
                        continue;
                    };
                    collect_database_signature(
                        path,
                        Some(&owner),
                        &method.sig,
                        true,
                        raw_database_types,
                        identity,
                        leaks,
                    );
                }
            }
            syn::Item::Mod(module) => {
                if let Some((_, items)) = &module.content {
                    collect_database_callables_in_items(
                        path,
                        items,
                        raw_database_types,
                        identity,
                        leaks,
                    );
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
    identity: &TypeIdentity,
    leaks: &mut BTreeSet<Finding>,
) {
    let callable = signature.ident.to_string();
    let line = signature.ident.span().start().line;
    if let syn::ReturnType::Type(_, output) = &signature.output {
        for dependency in
            type_names(output, identity.in_file(path)).intersection(raw_database_types)
        {
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
        for dependency in
            type_names(&input.ty, identity.in_file(path)).intersection(raw_database_types)
        {
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
    identity: &TypeIdentity,
    leaks: &mut BTreeSet<Finding>,
) {
    for file in files {
        if !(file.relative_path.ends_with("/src/lib.rs")
            || file.relative_path.ends_with("/src/main.rs"))
        {
            continue;
        }
        for item in &file.syntax.items {
            let syn::Item::Struct(item) = item else {
                continue;
            };
            let session = item.ident.to_string();
            if !policy.closed_session_types.contains(&session) {
                continue;
            }
            for field in &item.fields {
                for dependency in type_names(&field.ty, identity.in_file(&file.relative_path))
                    .intersection(internal_dependencies)
                {
                    leaks.insert(Finding::new(&file.relative_path, item.ident.span().start().line,
                        format!("crate-root {session} exposes internal dependency {dependency} to every module"), REMEDY));
                }
            }
        }
    }
}

fn collect_owner_methods(
    files: &[RustFile],
    pointers: &BTreeSet<String>,
    identity: &TypeIdentity,
) -> Vec<OwnerMethod> {
    let mut methods = Vec::new();
    for file in files {
        collect_owner_methods_in_items(
            &file.relative_path,
            &file.syntax.items,
            pointers,
            identity,
            &mut methods,
        );
    }
    methods
}

fn collect_owner_methods_in_items(
    path: &str,
    items: &[syn::Item],
    pointers: &BTreeSet<String>,
    identity: &TypeIdentity,
    methods: &mut Vec<OwnerMethod>,
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
                    methods.push(owner_method(
                        path,
                        &owner,
                        &method.sig,
                        visibility_crosses_owner(&method.vis),
                        pointers,
                        identity,
                    ));
                }
            }
            syn::Item::Trait(item) => {
                let owner = item.ident.to_string();
                for item in &item.items {
                    let syn::TraitItem::Fn(method) = item else {
                        continue;
                    };
                    methods.push(owner_method(path, &owner, &method.sig, true, pointers, identity));
                }
            }
            syn::Item::Mod(item) => {
                if let Some((_, items)) = &item.content {
                    collect_owner_methods_in_items(path, items, pointers, identity, methods);
                }
            }
            _ => {}
        }
    }
}

fn owner_method(
    path: &str,
    owner: &str,
    signature: &syn::Signature,
    public: bool,
    pointers: &BTreeSet<String>,
    identity: &TypeIdentity,
) -> OwnerMethod {
    let (mut output, mut shared_output) = match &signature.output {
        syn::ReturnType::Default => (BTreeSet::new(), BTreeSet::new()),
        syn::ReturnType::Type(_, output) => (
            type_names(output, identity.in_file(path)),
            shared_type_names(output, pointers, identity.in_file(path)),
        ),
    };
    let kept = kept_callback_arguments(signature);
    let mut callback_output = BTreeSet::new();
    let mut borrows_callback_output = false;
    for argument in kept {
        callback_output.extend(type_names(argument, identity.in_file(path)));
        shared_output.extend(shared_type_names(argument, pointers, identity.in_file(path)));
        let mut borrowed = BorrowedOutput(false);
        borrowed.visit_type(argument);
        borrows_callback_output |= borrowed.0;
    }
    for names in [&mut output, &mut callback_output] {
        names.remove("Self");
        names.remove(owner);
    }
    let parameters = signature
        .inputs
        .iter()
        .flat_map(|input| match input {
            syn::FnArg::Receiver(_) => BTreeSet::new(),
            syn::FnArg::Typed(input) => type_names(&input.ty, identity.in_file(path)),
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
        callback_output,
        shared_output,
        borrows_callback_output,
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
