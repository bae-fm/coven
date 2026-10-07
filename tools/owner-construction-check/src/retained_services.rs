//! The retained owner graph: the services the policy's root owners hold,
//! transitively, for as long as the store is open (§20.2).
//!
//! - An owner never hands out what it holds: no method reachable outside the
//!   owner returns a retained service, or a capability the owner retains.
//! - Retained services are built only at composition roots, or — for a
//!   service replaced while the store is open — by its one lifetime authority.

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::{Delimiter, Span};
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::{parse_macro_body, token_paths};
use crate::owner_construction::{
    collect_associated_factories, collect_free_constructors, Constructor,
};
use crate::policy::Policy;
use crate::syntax::{
    collect_declared_types, could_be_free_function_path, could_be_local_associated_function_path,
    is_test_only, is_test_source, path_names, type_name, type_names, visibility_crosses_owner,
    RustFile, StructInfo,
};

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct ServiceReturnViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) owner: String,
    pub(crate) method: String,
    pub(crate) returned: String,
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct RetainedServiceConstructionViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) owner: String,
    pub(crate) method: String,
    pub(crate) service: String,
    pub(crate) authority: Option<String>,
}

/// `roots` and every owner or capability they hold, transitively.
pub(crate) fn collect_root_retained_types(
    types: &BTreeMap<String, StructInfo>,
    owners: &BTreeSet<String>,
    roots: &[&str],
    policy: &Policy,
) -> BTreeSet<String> {
    let mut retained = roots
        .iter()
        .filter(|root| types.contains_key(**root))
        .map(|root| (*root).to_string())
        .collect::<BTreeSet<_>>();
    let mut pending = retained.iter().cloned().collect::<Vec<_>>();
    while let Some(owner) = pending.pop() {
        let Some(info) = types.get(&owner) else {
            continue;
        };
        for child in &info.field_types {
            let is_retained_capability =
                owners.contains(child) || policy.capability_types.contains(&child.as_str());
            if types.contains_key(child) && is_retained_capability && retained.insert(child.clone())
            {
                pending.push(child.clone());
            }
        }
    }
    retained
}

pub(crate) fn find_service_return_violations(
    files: &[RustFile],
    retained_owners: &BTreeSet<String>,
    stateful_services: &BTreeSet<String>,
    policy: &Policy,
) -> Vec<ServiceReturnViolation> {
    let returned_services = stateful_services
        .iter()
        .filter(|service| {
            !policy.root_owner_types.contains(&service.as_str())
                && !policy.task_types.contains(&service.as_str())
                && !policy.capability_types.contains(&service.as_str())
                && !service.ends_with("Inner")
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let declared_types = collect_declared_types(files);
    let retained_capabilities = retained_owners
        .iter()
        .map(|owner| {
            let capabilities = collect_root_retained_types(
                &declared_types,
                stateful_services,
                &[owner.as_str()],
                policy,
            )
            .into_iter()
            .filter(|service| policy.capability_types.contains(&service.as_str()))
            .collect::<BTreeSet<_>>();
            (owner.clone(), capabilities)
        })
        .collect::<BTreeMap<_, _>>();
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        find_service_returns_in_items(
            &file.relative_path,
            &file.syntax.items,
            &ServiceReturnScope {
                retained_owners,
                returned_services: &returned_services,
                retained_capabilities: &retained_capabilities,
                policy,
            },
            &mut violations,
        );
    }
    violations.into_iter().collect()
}

struct ServiceReturnScope<'a> {
    retained_owners: &'a BTreeSet<String>,
    returned_services: &'a BTreeSet<String>,
    retained_capabilities: &'a BTreeMap<String, BTreeSet<String>>,
    policy: &'a Policy,
}

fn find_service_returns_in_items(
    path: &str,
    items: &[syn::Item],
    scope: &ServiceReturnScope<'_>,
    violations: &mut BTreeSet<ServiceReturnViolation>,
) {
    for item in items {
        match item {
            syn::Item::Impl(item) => {
                if is_test_only(&item.attrs) {
                    continue;
                }
                let owner = type_name(&item.self_ty).unwrap_or_else(|| "<impl>".to_string());
                if !scope.retained_owners.contains(&owner) {
                    continue;
                }
                let mut owner_returned_services = scope.returned_services.clone();
                if let Some(capabilities) = scope.retained_capabilities.get(&owner) {
                    owner_returned_services.extend(capabilities.iter().cloned());
                }
                for impl_item in &item.items {
                    let syn::ImplItem::Fn(method) = impl_item else {
                        continue;
                    };
                    if is_test_only(&method.attrs) || !visibility_crosses_owner(&method.vis) {
                        continue;
                    }
                    record_service_returns(
                        path,
                        &owner,
                        &method.sig.ident.to_string(),
                        &method.sig.output,
                        method.sig.ident.span(),
                        &owner_returned_services,
                        scope.policy,
                        violations,
                    );
                }
            }
            syn::Item::Mod(item) => {
                if is_test_only(&item.attrs) {
                    continue;
                }
                if let Some((_, items)) = &item.content {
                    find_service_returns_in_items(path, items, scope, violations);
                }
            }
            _ => {}
        }
    }
}

fn record_service_returns(
    path: &str,
    owner: &str,
    method: &str,
    output: &syn::ReturnType,
    span: Span,
    retained_services: &BTreeSet<String>,
    policy: &Policy,
    violations: &mut BTreeSet<ServiceReturnViolation>,
) {
    let syn::ReturnType::Type(_, output) = output else {
        return;
    };
    let mut names = type_names(output);
    if names.contains("Self") {
        names.insert(owner.to_string());
    }
    for returned in names.intersection(retained_services) {
        if returned == owner
            || policy
                .composition_roots
                .iter()
                .any(|(root_path, root_owner, root_method)| {
                    path == *root_path && owner == *root_owner && method == *root_method
                })
        {
            continue;
        }
        violations.insert(ServiceReturnViolation {
            path: path.to_string(),
            line: span.start().line,
            owner: owner.to_string(),
            method: method.to_string(),
            returned: returned.clone(),
        });
    }
}

pub(crate) fn find_retained_service_construction_violations(
    files: &[RustFile],
    retained_services: &BTreeSet<String>,
    policy: &Policy,
) -> Vec<RetainedServiceConstructionViolation> {
    let authorities = policy
        .lifetime_authorities
        .iter()
        .filter(|(service, authority)| {
            retained_services.contains(*service) && retained_services.contains(*authority)
        })
        .map(|(service, authority)| ((*service).to_string(), (*authority).to_string()))
        .collect::<BTreeMap<_, _>>();
    let retained_services = retained_services
        .iter()
        .filter(|service| {
            !policy.root_owner_types.contains(&service.as_str())
                && !policy.task_types.contains(&service.as_str())
                && !service.ends_with("Inner")
                && (!policy.capability_types.contains(&service.as_str())
                    || authorities.contains_key(*service))
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let associated_factories = collect_associated_factories(files, &retained_services);
    let free_constructors = collect_free_constructors(files, &retained_services);
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        let mut visitor = ServiceConstructionSiteVisitor {
            path: &file.relative_path,
            retained_services: &retained_services,
            authorities: &authorities,
            policy,
            associated_factories: &associated_factories,
            free_constructors: &free_constructors,
            current_callable: None,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

struct ServiceConstructionSiteVisitor<'a> {
    path: &'a str,
    retained_services: &'a BTreeSet<String>,
    authorities: &'a BTreeMap<String, String>,
    policy: &'a Policy,
    associated_factories: &'a BTreeMap<(String, String), BTreeSet<String>>,
    free_constructors: &'a BTreeMap<String, BTreeSet<String>>,
    current_callable: Option<Constructor>,
    violations: &'a mut BTreeSet<RetainedServiceConstructionViolation>,
}

impl ServiceConstructionSiteVisitor<'_> {
    fn record(&mut self, service: &str, span: Span) {
        if !self.retained_services.contains(service) {
            return;
        }
        let Some(caller) = &self.current_callable else {
            return;
        };
        if caller.owner == service {
            return;
        }
        let defines_factory = if caller.owner == "<free>" {
            self.free_constructors
                .get(&caller.method)
                .is_some_and(|services| services.contains(service))
        } else {
            self.associated_factories
                .get(&(caller.owner.clone(), caller.method.clone()))
                .is_some_and(|services| services.contains(service))
        };
        if defines_factory {
            return;
        }
        if self
            .policy
            .composition_roots
            .iter()
            .any(|(path, owner, method)| {
                *path == self.path && *owner == caller.owner && *method == caller.method
            })
        {
            return;
        }
        let authority = self.authorities.get(service).cloned();
        if authority.as_ref() == Some(&caller.owner) {
            return;
        }
        self.violations
            .insert(RetainedServiceConstructionViolation {
                path: self.path.to_string(),
                line: span.start().line,
                owner: caller.owner.clone(),
                method: caller.method.clone(),
                service: service.to_string(),
                authority,
            });
    }

    /// A call through a local associated factory or a free function that
    /// returns a retained service.
    fn check_call(&mut self, segments: &[String], span: Span) {
        if could_be_local_associated_function_path(segments) {
            if let [.., owner, method] = segments {
                self.record_associated_factory(owner, method, span);
            }
        }
        if could_be_free_function_path(segments) {
            if let Some(services) = segments
                .last()
                .and_then(|function| self.free_constructors.get(function))
                .cloned()
            {
                for service in &services {
                    self.record(service, span);
                }
            }
        }
    }

    /// `self.method(…)`: a factory of the caller's own type.
    fn check_own_method_call(&mut self, method: &str, span: Span) {
        if let Some(caller) = &self.current_callable {
            let owner = caller.owner.clone();
            self.record_associated_factory(&owner, method, span);
        }
    }

    fn record_associated_factory(&mut self, owner: &str, method: &str, span: Span) {
        let Some(services) = self
            .associated_factories
            .get(&(owner.to_string(), method.to_string()))
        else {
            return;
        };
        for service in services {
            self.record(service, span);
        }
    }
}

impl<'ast> Visit<'ast> for ServiceConstructionSiteVisitor<'_> {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_item_mod(self, node);
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if is_test_only(&node.attrs) {
            return;
        }
        let previous = self.current_callable.clone();
        let owner = type_name(&node.self_ty).unwrap_or_else(|| "<impl>".to_string());
        for item in &node.items {
            let syn::ImplItem::Fn(method) = item else {
                continue;
            };
            if is_test_only(&method.attrs) {
                continue;
            }
            self.current_callable = Some(Constructor {
                owner: owner.clone(),
                method: method.sig.ident.to_string(),
            });
            self.visit_block(&method.block);
        }
        self.current_callable = previous;
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        let previous = self.current_callable.replace(Constructor {
            owner: "<free>".to_string(),
            method: node.sig.ident.to_string(),
        });
        self.visit_block(&node.block);
        self.current_callable = previous;
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let syn::Expr::Path(function) = node.func.as_ref() {
            self.check_call(&path_names(&function.path), node.span());
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if matches!(node.receiver.as_ref(), syn::Expr::Path(path) if path.path.is_ident("self")) {
            self.check_own_method_call(&node.method.to_string(), node.span());
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if let [service] = path_names(&node.path).as_slice() {
            self.record(service, node.span());
        }
        visit::visit_expr_struct(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        match parse_macro_body(node) {
            Some(body) => body.visit(self),
            None => {
                for path in token_paths(node.tokens.clone()) {
                    match (path.followed_by, path.after_dot, path.segments.as_slice()) {
                        (Some(Delimiter::Parenthesis), true, [method])
                            if path.receiver.as_deref() == Some("self") =>
                        {
                            self.check_own_method_call(method, path.span);
                        }
                        (Some(Delimiter::Parenthesis), false, segments) => {
                            self.check_call(segments, path.span);
                        }
                        (Some(Delimiter::Brace), false, [service]) => {
                            self.record(service, path.span);
                        }
                        _ => {}
                    }
                }
            }
        }
        visit::visit_macro(self, node);
    }
}

#[cfg(test)]
#[path = "retained_services_tests.rs"]
mod tests;
