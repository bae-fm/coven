//! Capabilities are acquired at composition roots (§21.2). The policy names
//! interfaces; every implementation, including feature-gated fakes, contributes
//! a construction-only type. Explicit non-trait capabilities join the same set.
//!
//! Factory definitions may build their declared result, and their use sites are
//! checked. Receiver operations may derive non-trait file capabilities from an
//! injected owner. Test sources and `cfg(test)` items may assemble capabilities.
//! `Default` is forbidden on capability types: inferred `Default::default()`
//! cannot be resolved by a syntax checker.

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::{parse_macro_body, token_paths};
use crate::owner_construction::{collect_associated_factories, collect_free_constructors};
use crate::policy::Policy;
use crate::syntax::{
    could_be_free_function_path, is_test_only, is_test_source, path_names, type_name, type_names,
    RustFile,
};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum ConstructionKind {
    Value,
    DefaultImplementation,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct CapabilityConstructionViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) capability: String,
    pub(crate) kind: ConstructionKind,
}

/// Read implementations before inspecting use sites, across every source and
/// without evaluating cfg predicates. Both construction and parameter checks
/// use this set, so an implementation needs no additional policy entry.
pub(crate) fn construction_only_types(files: &[RustFile], policy: &Policy) -> BTreeSet<String> {
    let mut types = policy
        .construction_only_capability_types
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    for file in files {
        ImplementationCollector {
            traits: policy.capability_traits,
            types: &mut types,
        }
        .visit_file(&file.syntax);
    }
    types
}

struct ImplementationCollector<'a> {
    traits: &'a [&'a str],
    types: &'a mut BTreeSet<String>,
}

impl<'ast> Visit<'ast> for ImplementationCollector<'_> {
    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if let Some((path, _)) = &node.trait_ {
            if node.modifiers.polarity.is_none()
                && path.segments.last().is_some_and(|trait_name| {
                    self.traits.contains(&trait_name.ident.to_string().as_str())
                })
            {
                if let Some(name) = type_name(&node.self_ty) {
                    self.types.insert(name);
                }
            }
        }
        visit::visit_item_impl(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

pub(crate) fn find_capability_construction_violations(
    files: &[RustFile],
    policy: &Policy,
) -> Vec<CapabilityConstructionViolation> {
    let capabilities = construction_only_types(files, policy);
    let associated_factories = collect_associated_factories(files, &capabilities);
    let free_factories = collect_free_constructors(files, &capabilities);
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        let mut visitor = ConstructionVisitor {
            path: &file.relative_path,
            policy,
            capabilities: &capabilities,
            associated_factories: &associated_factories,
            free_factories: &free_factories,
            current_type: None,
            scope: ConstructionScope::Outside,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

enum ConstructionScope {
    Outside,
    CompositionRoot,
    Factory(BTreeSet<String>),
}

struct ConstructionVisitor<'a> {
    path: &'a str,
    policy: &'a Policy,
    capabilities: &'a BTreeSet<String>,
    associated_factories: &'a BTreeMap<(String, String), BTreeSet<String>>,
    free_factories: &'a BTreeMap<String, BTreeSet<String>>,
    current_type: Option<String>,
    scope: ConstructionScope,
    violations: &'a mut BTreeSet<CapabilityConstructionViolation>,
}

impl ConstructionVisitor<'_> {
    fn resolve_self<'a>(&'a self, name: &'a str) -> &'a str {
        match (name, &self.current_type) {
            ("Self", Some(name)) => name,
            _ => name,
        }
    }

    fn record(&mut self, name: &str, span: Span, kind: ConstructionKind) {
        let name = self.resolve_self(name);
        if !self.capabilities.contains(name) {
            return;
        }
        if kind == ConstructionKind::Value {
            match &self.scope {
                ConstructionScope::CompositionRoot => return,
                ConstructionScope::Factory(results) if results.contains(name) => return,
                _ => {}
            }
        }
        self.violations.insert(CapabilityConstructionViolation {
            path: self.path.to_string(),
            line: span.start().line,
            capability: name.to_string(),
            kind,
        });
    }

    fn check_value_path(&mut self, segments: &[String], span: Span) {
        if let Some(name) = segments.last() {
            // Unit values and tuple constructors, including constructor values
            // passed to another function without immediately being called.
            self.record(name, span, ConstructionKind::Value);
        }
        if let [.., owner, method] = segments {
            let owner = self.resolve_self(owner).to_string();
            if matches!(method.as_str(), "new" | "default") {
                self.record(&owner, span, ConstructionKind::Value);
            }
            if let Some(results) = self.associated_factories.get(&(owner, method.clone())) {
                for result in results {
                    self.record(result, span, ConstructionKind::Value);
                }
            }
        }
        if could_be_free_function_path(segments) {
            if let Some(results) = segments
                .last()
                .and_then(|name| self.free_factories.get(name))
            {
                for result in results {
                    self.record(result, span, ConstructionKind::Value);
                }
            }
        }
    }

    fn check_item_type(&mut self, ty: &syn::Type, span: Span) {
        for name in type_names(ty) {
            self.record(&name, span, ConstructionKind::Value);
        }
    }

    fn factory_results(&self, signature: &syn::Signature) -> BTreeSet<String> {
        let syn::ReturnType::Type(_, output) = &signature.output else {
            return BTreeSet::new();
        };
        type_names(output)
            .iter()
            .map(|name| self.resolve_self(name).to_string())
            .filter(|name| self.capabilities.contains(name))
            .filter(|name| {
                // An injected file owner can derive a file or store directory.
                // A receiver method on a trait implementation cannot acquire a
                // new clock/id source in place of using its injected instance.
                signature.receiver().is_none()
                    || (self
                        .policy
                        .construction_only_capability_types
                        .contains(&name.as_str())
                        && self.current_type.as_ref().is_some_and(|owner| {
                            self.policy.capability_types.contains(&owner.as_str())
                        }))
            })
            .collect()
    }

    fn check_default_derive(&mut self, name: &syn::Ident, attrs: &[syn::Attribute]) {
        for attribute in attrs {
            if derives_default(&attribute.meta) {
                self.record(
                    &name.to_string(),
                    attribute.span(),
                    ConstructionKind::DefaultImplementation,
                );
            }
        }
    }
}

fn derives_default(meta: &syn::Meta) -> bool {
    let syn::Meta::List(list) = meta else {
        return false;
    };
    if !list.path.is_ident("derive") && !list.path.is_ident("cfg_attr") {
        return false;
    }
    let Ok(children) = list.parse_args_with(
        syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
    ) else {
        // Malformed derives/cfg_attr are compiler errors, not valid declarations.
        return false;
    };
    if list.path.is_ident("derive") {
        children.iter().any(|child| {
            child
                .path()
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "Default")
        })
    } else {
        children.iter().skip(1).any(derives_default)
    }
}

impl<'ast> Visit<'ast> for ConstructionVisitor<'_> {
    fn visit_item(&mut self, node: &'ast syn::Item) {
        // Constants/statics declared inside a root are assembled there. Other
        // nested items are independent callables or scopes, not part of its work.
        let scope = if matches!(node, syn::Item::Const(_) | syn::Item::Static(_))
            && matches!(self.scope, ConstructionScope::CompositionRoot)
        {
            ConstructionScope::CompositionRoot
        } else {
            ConstructionScope::Outside
        };
        let previous = std::mem::replace(&mut self.scope, scope);
        visit::visit_item(self, node);
        self.scope = previous;
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if !is_test_only(&node.attrs) {
            visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_trait(&mut self, node: &'ast syn::ItemTrait) {
        if !is_test_only(&node.attrs) {
            let previous = self.current_type.replace(node.ident.to_string());
            visit::visit_item_trait(self, node);
            self.current_type = previous;
        }
    }

    fn visit_trait_item_fn(&mut self, node: &'ast syn::TraitItemFn) {
        if !is_test_only(&node.attrs) {
            let scope = ConstructionScope::Factory(self.factory_results(&node.sig));
            let previous = std::mem::replace(&mut self.scope, scope);
            visit::visit_trait_item_fn(self, node);
            self.scope = previous;
        }
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if !is_test_only(&node.attrs) {
            let scope = ConstructionScope::Factory(self.factory_results(&node.sig));
            let previous = std::mem::replace(&mut self.scope, scope);
            visit::visit_item_fn(self, node);
            self.scope = previous;
        }
    }

    fn visit_item_const(&mut self, node: &'ast syn::ItemConst) {
        if !is_test_only(&node.attrs) {
            self.check_item_type(&node.ty, node.span());
            visit::visit_item_const(self, node);
        }
    }

    fn visit_item_static(&mut self, node: &'ast syn::ItemStatic) {
        if !is_test_only(&node.attrs) {
            self.check_item_type(&node.ty, node.span());
            visit::visit_item_static(self, node);
        }
    }

    fn visit_impl_item_const(&mut self, node: &'ast syn::ImplItemConst) {
        if !is_test_only(&node.attrs) {
            self.check_item_type(&node.ty, node.span());
            visit::visit_impl_item_const(self, node);
        }
    }

    fn visit_trait_item_const(&mut self, node: &'ast syn::TraitItemConst) {
        if !is_test_only(&node.attrs) {
            self.check_item_type(&node.ty, node.span());
            visit::visit_trait_item_const(self, node);
        }
    }

    fn visit_item_struct(&mut self, node: &'ast syn::ItemStruct) {
        if !is_test_only(&node.attrs) {
            self.check_default_derive(&node.ident, &node.attrs);
            visit::visit_item_struct(self, node);
        }
    }

    fn visit_item_enum(&mut self, node: &'ast syn::ItemEnum) {
        if !is_test_only(&node.attrs) {
            self.check_default_derive(&node.ident, &node.attrs);
            visit::visit_item_enum(self, node);
        }
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if is_test_only(&node.attrs) {
            return;
        }
        let previous = std::mem::replace(&mut self.current_type, type_name(&node.self_ty));
        if node.trait_.as_ref().is_some_and(|(path, _)| {
            path.segments
                .last()
                .is_some_and(|segment| segment.ident == "Default")
        }) {
            self.record("Self", node.span(), ConstructionKind::DefaultImplementation);
        }
        visit::visit_item_impl(self, node);
        self.current_type = previous;
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        let method = node.sig.ident.to_string();
        let allowed = self
            .policy
            .composition_roots
            .iter()
            .any(|(path, owner, name)| {
                *path == self.path
                    && self.current_type.as_deref() == Some(*owner)
                    && *name == method
            });
        let scope = if allowed {
            ConstructionScope::CompositionRoot
        } else {
            ConstructionScope::Factory(self.factory_results(&node.sig))
        };
        let previous = std::mem::replace(&mut self.scope, scope);
        visit::visit_impl_item_fn(self, node);
        self.scope = previous;
    }

    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        let segments = match &node.qself {
            Some(qself) => type_name(&qself.ty).map(|owner| {
                std::iter::once(owner)
                    .chain(
                        node.path
                            .segments
                            .iter()
                            .skip(qself.position)
                            .map(|segment| segment.ident.to_string()),
                    )
                    .collect::<Vec<_>>()
            }),
            None => Some(path_names(&node.path)),
        };
        if let Some(segments) = segments {
            self.check_value_path(&segments, node.span());
        }
        visit::visit_expr_path(self, node);
    }

    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if let Some(segment) = node.path.segments.last() {
            self.record(
                &segment.ident.to_string(),
                node.span(),
                ConstructionKind::Value,
            );
        }
        visit::visit_expr_struct(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        match parse_macro_body(node) {
            Some(body) => body.visit(self),
            None => {
                for path in token_paths(node.tokens.clone()) {
                    if !path.after_dot {
                        self.check_value_path(&path.segments, path.span);
                    }
                }
            }
        }
        visit::visit_macro(self, node);
    }
}

#[cfg(test)]
#[path = "capability_construction_tests.rs"]
mod tests;
