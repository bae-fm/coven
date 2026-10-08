//! Owners and capabilities are constructed at composition roots (§20.2). The policy names
//! interfaces; every implementation, including feature-gated fakes, contributes
//! a construction-only type. Explicit non-trait capabilities join the same set.
//!
//! Capability factory definitions may build their declared result; owner factories
//! require a root entry themselves. Both have their use sites checked.
//! Only the policy's named receiver factories may derive capabilities
//! from an injected owner. Test sources and `cfg(test)` items may assemble capabilities.
//! `Default` is forbidden on capability types: inferred `Default::default()`
//! cannot be resolved by a syntax checker.

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::factories::{collect_associated_factories, collect_free_constructors};
use crate::finding::Finding;
use crate::macros::{parse_macro_body, token_paths};
use crate::owner_graph::OwnerGraph;
use crate::policy::Policy;
use crate::syntax::{
    could_be_free_function_path, could_be_local_associated_function_path, is_test_only,
    is_test_source, path_names, type_name, type_names, RustFile,
};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum ConstructionKind {
    Value,
    DefaultImplementation,
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
    graph: &OwnerGraph,
) -> Vec<Finding> {
    let capabilities = construction_only_types(files, policy);
    let owners = graph
        .owners
        .iter()
        .filter(|name| {
            !policy.task_types.contains(&name.as_str())
                && !policy.capability_types.contains(&name.as_str())
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let constructed = capabilities.union(&owners).cloned().collect();
    let associated_factories = collect_associated_factories(files, &constructed);
    let free_factories = collect_free_constructors(files, &constructed);
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        let mut visitor = ConstructionVisitor {
            path: &file.relative_path,
            policy,
            capabilities: &capabilities,
            owners: &owners,
            associated_factories: &associated_factories,
            free_factories: &free_factories,
            current_type: None,
            current_method: None,
            bindings: BTreeSet::new(),
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
    owners: &'a BTreeSet<String>,
    associated_factories: &'a BTreeMap<(String, String), BTreeSet<String>>,
    free_factories: &'a BTreeMap<String, BTreeSet<String>>,
    current_type: Option<String>,
    current_method: Option<String>,
    bindings: BTreeSet<String>,
    scope: ConstructionScope,
    violations: &'a mut BTreeSet<Finding>,
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
        if !self.capabilities.contains(name) && !self.owners.contains(name) {
            return;
        }
        if kind == ConstructionKind::Value {
            match &self.scope {
                ConstructionScope::CompositionRoot => return,
                ConstructionScope::Factory(results)
                    if !self.owners.contains(name) && results.contains(name) =>
                {
                    return
                }
                _ => {}
            }
        }
        let subject = if self.owners.contains(name) {
            "owner"
        } else {
            "capability"
        };
        let message = match kind {
            ConstructionKind::Value => format!("{}::{} constructs {subject} {name} outside a composition root", self.current_type.as_deref().unwrap_or("<free>"), self.current_method.as_deref().unwrap_or("<item>")),
            ConstructionKind::DefaultImplementation => format!("implements Default for {subject} {name}, which permits implicit construction through Default::default()"),
        };
        self.violations.insert(Finding::new(self.path, span.start().line, message,
            "construct owners and capabilities explicitly at the listed roots; inject them elsewhere; do not implement or derive Default"));
    }

    fn check_value_path(&mut self, segments: &[String], span: Span) {
        if matches!(segments, [name] if self.bindings.contains(name)) {
            return;
        }
        if let Some(name) = segments
            .last()
            .filter(|_| could_be_free_function_path(segments))
        {
            // Unit values and tuple constructors, including constructor values
            // passed to another function without immediately being called.
            self.record(name, span, ConstructionKind::Value);
        }
        if let [.., owner, method] = segments {
            let owner = self.resolve_self(owner).to_string();
            if matches!(method.as_str(), "new" | "default") {
                self.record(&owner, span, ConstructionKind::Value);
            }
            if let Some(results) = self
                .associated_factories
                .get(&(owner.clone(), method.clone()))
            {
                for result in results {
                    // A declared receiver factory uses an already supplied
                    // owner, including when called as Owner::method(&owner).
                    if (!self.owners.contains(result)
                        || could_be_local_associated_function_path(segments))
                        && !self.policy.capability_factories.iter().any(
                            |(_, factory, name, product)| {
                                *factory == owner && *name == method && *product == result
                            },
                        )
                    {
                        self.record(result, span, ConstructionKind::Value);
                    }
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

    fn check_own_method(&mut self, method: &str, span: Span) {
        if let Some(owner) = &self.current_type {
            if let Some(results) = self
                .associated_factories
                .get(&(owner.clone(), method.into()))
            {
                for result in results {
                    if self.owners.contains(result) {
                        self.record(result, span, ConstructionKind::Value);
                    }
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
                signature.receiver().is_none()
                    || self.policy.capability_factories.iter().any(
                        |(path, owner, method, product)| {
                            *path == self.path
                                && self.current_type.as_deref() == Some(*owner)
                                && signature.ident == *method
                                && *product == name
                        },
                    )
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

    fn bind_pattern(&mut self, pattern: &syn::Pat) {
        struct Bindings<'a>(&'a mut BTreeSet<String>);
        impl<'ast> Visit<'ast> for Bindings<'_> {
            fn visit_pat_ident(&mut self, node: &'ast syn::PatIdent) {
                self.0.insert(node.ident.to_string());
                visit::visit_pat_ident(self, node);
            }
        }
        Bindings(&mut self.bindings).visit_pat(pattern);
    }

    fn bind_inputs(&mut self, signature: &syn::Signature) {
        for input in &signature.inputs {
            if let syn::FnArg::Typed(input) = input {
                self.bind_pattern(&input.pat);
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
        let bindings = std::mem::take(&mut self.bindings);
        visit::visit_item(self, node);
        self.bindings = bindings;
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
            let bindings = std::mem::take(&mut self.bindings);
            self.bind_inputs(&node.sig);
            visit::visit_trait_item_fn(self, node);
            self.bindings = bindings;
            self.scope = previous;
        }
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if !is_test_only(&node.attrs) {
            let rooted =
                self.policy
                    .is_composition_root(self.path, "<free>", &node.sig.ident.to_string());
            let scope = if rooted {
                ConstructionScope::CompositionRoot
            } else {
                ConstructionScope::Factory(self.factory_results(&node.sig))
            };
            let previous = std::mem::replace(&mut self.scope, scope);
            let current_type = self.current_type.take();
            let method = self.current_method.replace(node.sig.ident.to_string());
            self.bind_inputs(&node.sig);
            visit::visit_item_fn(self, node);
            self.current_type = current_type;
            self.current_method = method;
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
        let allowed = self.policy.is_composition_root(
            self.path,
            self.current_type.as_deref().unwrap_or("<impl>"),
            &method,
        );
        let scope = if allowed {
            ConstructionScope::CompositionRoot
        } else {
            ConstructionScope::Factory(self.factory_results(&node.sig))
        };
        let previous = std::mem::replace(&mut self.scope, scope);
        let bindings = std::mem::take(&mut self.bindings);
        let method = self.current_method.replace(node.sig.ident.to_string());
        self.bind_inputs(&node.sig);
        visit::visit_impl_item_fn(self, node);
        self.current_method = method;
        self.bindings = bindings;
        self.scope = previous;
    }

    fn visit_block(&mut self, node: &'ast syn::Block) {
        let bindings = self.bindings.clone();
        visit::visit_block(self, node);
        self.bindings = bindings;
    }

    fn visit_local(&mut self, node: &'ast syn::Local) {
        // The initializer (and let-else branch) sees the previous binding.
        visit::visit_local(self, node);
        self.bind_pattern(&node.pat);
    }

    fn visit_expr_closure(&mut self, node: &'ast syn::ExprClosure) {
        let bindings = self.bindings.clone();
        for input in &node.inputs {
            self.bind_pattern(input);
        }
        visit::visit_expr_closure(self, node);
        self.bindings = bindings;
    }

    fn visit_arm(&mut self, node: &'ast syn::Arm) {
        let bindings = self.bindings.clone();
        self.bind_pattern(&node.pat);
        visit::visit_arm(self, node);
        self.bindings = bindings;
    }

    fn visit_expr_for_loop(&mut self, node: &'ast syn::ExprForLoop) {
        self.visit_expr(&node.expr);
        let bindings = self.bindings.clone();
        self.bind_pattern(&node.pat);
        self.visit_block(&node.body);
        self.bindings = bindings;
    }

    fn visit_expr_let(&mut self, node: &'ast syn::ExprLet) {
        self.visit_expr(&node.expr);
        self.bind_pattern(&node.pat);
    }

    fn visit_expr_if(&mut self, node: &'ast syn::ExprIf) {
        let bindings = self.bindings.clone();
        self.visit_expr(&node.cond);
        self.visit_block(&node.then_branch);
        self.bindings = bindings;
        if let Some((_, branch)) = &node.else_branch {
            self.visit_expr(branch);
        }
    }

    fn visit_expr_while(&mut self, node: &'ast syn::ExprWhile) {
        let bindings = self.bindings.clone();
        self.visit_expr(&node.cond);
        self.visit_block(&node.body);
        self.bindings = bindings;
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if matches!(node.receiver.as_ref(), syn::Expr::Path(path) if path.path.is_ident("self")) {
            self.check_own_method(&node.method.to_string(), node.span());
        }
        visit::visit_expr_method_call(self, node);
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
        self.check_value_path(&path_names(&node.path), node.span());
        visit::visit_expr_struct(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        match parse_macro_body(node) {
            Some(body) => body.visit(self),
            None => {
                for path in token_paths(node.tokens.clone()) {
                    if path.after_dot
                        && path.receiver.as_deref() == Some("self")
                        && path.followed_by == Some(proc_macro2::Delimiter::Parenthesis)
                    {
                        self.check_own_method(&path.segments[0], path.span);
                    } else if !path.after_dot {
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
