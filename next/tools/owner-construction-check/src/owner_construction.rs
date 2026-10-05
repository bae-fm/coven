//! An owner never builds another owner (§21.2): an owner's constructor takes
//! its collaborators as arguments, and owner graphs are built only at the
//! composition roots the policy names.
//!
//! An owner is inferred, not declared: a type holding a capability type, or
//! holding an owner. A constructor is a method of an owner that returns the
//! owner. Inside a constructor, building another owner — by struct literal, by
//! that owner's own factory, or through a free or associated function that
//! returns it — is the violation.

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::{Delimiter, Span};
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::{parse_macro_body, token_paths};
use crate::policy::Policy;
use crate::syntax::{
    could_be_free_function_path, output_contains_owner, path_names, type_name, type_names,
    RustFile, StructInfo,
};

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct Constructor {
    pub(crate) owner: String,
    pub(crate) method: String,
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct OwnerConstructionViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) parent: String,
    pub(crate) child: String,
}

/// Types that hold a capability, directly or through another owner, less the
/// policy's values and facades.
pub(crate) fn infer_owners(
    structs: &BTreeMap<String, StructInfo>,
    policy: &Policy,
) -> BTreeSet<String> {
    let capabilities = policy
        .capability_types
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();
    let mut owners = BTreeSet::new();
    loop {
        let before = owners.len();
        for (name, info) in structs {
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

pub(crate) fn collect_constructors(
    files: &[RustFile],
    owners: &BTreeSet<String>,
) -> BTreeSet<Constructor> {
    let mut constructors = BTreeSet::new();
    for file in files {
        let mut collector = ConstructorCollector {
            owners,
            constructors: &mut constructors,
        };
        collector.visit_file(&file.syntax);
    }
    constructors
}

/// Free function name → the owners it returns.
pub(crate) fn collect_free_constructors(
    files: &[RustFile],
    owners: &BTreeSet<String>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut constructors = BTreeMap::new();
    for file in files {
        let mut collector = FreeConstructorCollector {
            owners,
            constructors: &mut constructors,
        };
        collector.visit_file(&file.syntax);
    }
    constructors
}

/// `(type, associated function)` → the owners it returns.
pub(crate) fn collect_associated_factories(
    files: &[RustFile],
    owners: &BTreeSet<String>,
) -> BTreeMap<(String, String), BTreeSet<String>> {
    let mut trait_outputs = BTreeMap::new();
    for file in files {
        TraitFactoryCollector {
            outputs: &mut trait_outputs,
        }
        .visit_file(&file.syntax);
    }
    let mut factories = BTreeMap::new();
    for file in files {
        let mut collector = AssociatedFactoryCollector {
            owners,
            trait_outputs: &trait_outputs,
            factories: &mut factories,
        };
        collector.visit_file(&file.syntax);
    }
    factories
}

struct AssociatedFactoryCollector<'a> {
    owners: &'a BTreeSet<String>,
    trait_outputs: &'a BTreeMap<String, Vec<(String, BTreeSet<String>)>>,
    factories: &'a mut BTreeMap<(String, String), BTreeSet<String>>,
}

impl Visit<'_> for AssociatedFactoryCollector<'_> {
    fn visit_item_impl(&mut self, node: &syn::ItemImpl) {
        let Some(factory) = type_name(&node.self_ty) else {
            return;
        };
        if let Some((trait_path, _)) = &node.trait_ {
            if let Some(methods) = trait_path
                .segments
                .last()
                .and_then(|name| self.trait_outputs.get(&name.ident.to_string()))
            {
                for (method, names) in methods {
                    self.record(&factory, method, names);
                }
            }
        }
        for item in &node.items {
            let syn::ImplItem::Fn(method) = item else {
                continue;
            };
            let syn::ReturnType::Type(_, output) = &method.sig.output else {
                continue;
            };
            let names = type_names(output);
            self.record(&factory, &method.sig.ident.to_string(), &names);
        }
        visit::visit_item_impl(self, node);
    }
}

impl AssociatedFactoryCollector<'_> {
    fn record(&mut self, factory: &str, method: &str, names: &BTreeSet<String>) {
        let mut returned_owners = names
            .intersection(self.owners)
            .cloned()
            .collect::<BTreeSet<_>>();
        if names.contains("Self") && self.owners.contains(factory) {
            returned_owners.insert(factory.to_string());
        }
        if !returned_owners.is_empty() {
            self.factories
                .entry((factory.to_string(), method.to_string()))
                .or_default()
                .extend(returned_owners);
        }
    }
}

struct TraitFactoryCollector<'a> {
    outputs: &'a mut BTreeMap<String, Vec<(String, BTreeSet<String>)>>,
}

impl Visit<'_> for TraitFactoryCollector<'_> {
    fn visit_item_trait(&mut self, node: &syn::ItemTrait) {
        let methods = self.outputs.entry(node.ident.to_string()).or_default();
        for item in &node.items {
            if let syn::TraitItem::Fn(method) = item {
                if let syn::ReturnType::Type(_, output) = &method.sig.output {
                    methods.push((method.sig.ident.to_string(), type_names(output)));
                }
            }
        }
        visit::visit_item_trait(self, node);
    }
}

struct FreeConstructorCollector<'a> {
    owners: &'a BTreeSet<String>,
    constructors: &'a mut BTreeMap<String, BTreeSet<String>>,
}

impl Visit<'_> for FreeConstructorCollector<'_> {
    fn visit_item_fn(&mut self, node: &syn::ItemFn) {
        let syn::ReturnType::Type(_, output) = &node.sig.output else {
            return;
        };
        let returned_owners = type_names(output)
            .intersection(self.owners)
            .cloned()
            .collect::<BTreeSet<_>>();
        if !returned_owners.is_empty() {
            self.constructors
                .entry(node.sig.ident.to_string())
                .or_default()
                .extend(returned_owners);
        }
    }
}

struct ConstructorCollector<'a> {
    owners: &'a BTreeSet<String>,
    constructors: &'a mut BTreeSet<Constructor>,
}

impl<'ast> Visit<'ast> for ConstructorCollector<'_> {
    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        let Some(owner) = type_name(&node.self_ty) else {
            return;
        };
        if !self.owners.contains(&owner) {
            return;
        }
        for item in &node.items {
            let syn::ImplItem::Fn(method) = item else {
                continue;
            };
            if output_contains_owner(&method.sig.output, &owner) {
                self.constructors.insert(Constructor {
                    owner: owner.clone(),
                    method: method.sig.ident.to_string(),
                });
            }
        }
    }
}

pub(crate) fn find_owner_construction_violations(
    files: &[RustFile],
    owners: &BTreeSet<String>,
    constructors: &BTreeSet<Constructor>,
    free_constructors: &BTreeMap<String, BTreeSet<String>>,
    policy: &Policy,
) -> Vec<OwnerConstructionViolation> {
    let mut violations = BTreeSet::new();
    let associated_factories = collect_associated_factories(files, owners);
    for file in files {
        let mut visitor = ConstructionVisitor {
            path: &file.relative_path,
            owners,
            constructors,
            free_constructors,
            associated_factories: &associated_factories,
            policy,
            current_constructor: None,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

struct ConstructionVisitor<'a> {
    path: &'a str,
    owners: &'a BTreeSet<String>,
    constructors: &'a BTreeSet<Constructor>,
    free_constructors: &'a BTreeMap<String, BTreeSet<String>>,
    associated_factories: &'a BTreeMap<(String, String), BTreeSet<String>>,
    policy: &'a Policy,
    current_constructor: Option<Constructor>,
    violations: &'a mut BTreeSet<OwnerConstructionViolation>,
}

impl ConstructionVisitor<'_> {
    /// A call through an associated factory or a free function that returns
    /// an owner.
    fn check_call(&mut self, segments: &[String], span: Span) {
        if let [.., owner, method] = segments {
            if let Some(returned_owners) = self
                .associated_factories
                .get(&(owner.clone(), method.clone()))
            {
                for returned_owner in returned_owners {
                    self.record(returned_owner, span);
                }
            }
        }
        if could_be_free_function_path(segments) {
            if let Some(owners) = segments
                .last()
                .and_then(|function| self.free_constructors.get(function))
            {
                for owner in owners {
                    self.record(owner, span);
                }
            }
        }
    }

    fn check_struct_literal(&mut self, name: &str, span: Span) {
        if self.owners.contains(name) {
            self.record(name, span);
        }
    }

    fn record(&mut self, child: &str, span: Span) {
        let Some(parent) = &self.current_constructor else {
            return;
        };
        let tasks = self.policy.task_types;
        if parent.owner == child
            || child == format!("{}Inner", parent.owner)
            || (tasks.contains(&parent.owner.as_str()) && tasks.contains(&child))
            || self
                .policy
                .composition_roots
                .iter()
                .any(|(path, owner, method)| {
                    *path == self.path && *owner == parent.owner && *method == parent.method
                })
        {
            return;
        }
        self.violations.insert(OwnerConstructionViolation {
            path: self.path.to_string(),
            line: span.start().line,
            parent: format!("{}::{}", parent.owner, parent.method),
            child: child.to_string(),
        });
    }
}

impl<'ast> Visit<'ast> for ConstructionVisitor<'_> {
    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        let previous = self.current_constructor.clone();
        let owner = type_name(&node.self_ty);
        for item in &node.items {
            let syn::ImplItem::Fn(method) = item else {
                continue;
            };
            self.current_constructor = owner.as_ref().and_then(|owner| {
                let constructor = Constructor {
                    owner: owner.clone(),
                    method: method.sig.ident.to_string(),
                };
                self.constructors
                    .contains(&constructor)
                    .then_some(constructor)
            });
            self.visit_block(&method.block);
        }
        self.current_constructor = previous;
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let syn::Expr::Path(function) = node.func.as_ref() {
            self.check_call(&path_names(&function.path), node.span());
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if let Some(segment) = node.path.segments.last() {
            self.check_struct_literal(&segment.ident.to_string(), node.span());
        }
        visit::visit_expr_struct(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        match parse_macro_body(node) {
            Some(body) => body.visit(self),
            None => {
                for path in token_paths(node.tokens.clone()) {
                    match path.followed_by {
                        Some(Delimiter::Parenthesis) if !path.after_dot => {
                            self.check_call(&path.segments, path.span);
                        }
                        Some(Delimiter::Brace) if !path.after_dot => {
                            if let Some(name) = path.segments.last() {
                                self.check_struct_literal(name, path.span);
                            }
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
#[path = "owner_construction_tests.rs"]
mod tests;
