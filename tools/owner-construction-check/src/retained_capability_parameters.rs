//! A method never takes a raw capability as a parameter (Appendix F.2), such as a
//! store directory, whichever type the method belongs to: a capability is
//! fixed when what uses it is built, an owner for the store's life or a task
//! for one operation, and the method uses the one its value holds. Only
//! constructors and the composition roots accept the policy's
//! construction-only capabilities. A free function may take one.
//!
//! A handle (Appendix F.3) flows the same way, into whatever holds it: no
//! method but a constructor or a composition root takes one. A free function
//! may take one.

use std::collections::BTreeSet;

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::finding::Finding;
use crate::macros::parse_macro_body;
use crate::owner_graph::OwnerGraph;
use crate::syntax::{output_contains_owner, supplied_type_names, type_name, RustFile};
use owner_construction_policy::Policy;

/// The construction-only capabilities: the policy's rows, and every type
/// that implements one of its capability traits, read across the crates'
/// production code without evaluating cfg predicates other than `test`.
pub(crate) fn construction_only_types(files: &[RustFile], policy: &Policy) -> BTreeSet<String> {
    let mut types = policy.construction_only_capability_types.clone();
    for file in files {
        ImplementationCollector { traits: &policy.capability_traits, types: &mut types }
            .visit_file(&file.syntax);
    }
    types
}

struct ImplementationCollector<'a> {
    traits: &'a BTreeSet<String>,
    types: &'a mut BTreeSet<String>,
}

impl<'ast> Visit<'ast> for ImplementationCollector<'_> {
    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if let Some((path, _)) = &node.trait_ {
            if node.modifiers.polarity.is_none()
                && path
                    .segments
                    .last()
                    .is_some_and(|trait_name| self.traits.contains(&trait_name.ident.to_string()))
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

pub(crate) fn find_retained_capability_parameter_violations(
    files: &[RustFile],
    graph: &OwnerGraph,
    policy: &Policy,
) -> Vec<Finding> {
    let capabilities = construction_only_types(files, policy);
    let mut violations = BTreeSet::new();
    for file in files {
        find_in_items(
            &file.relative_path,
            &file.syntax.items,
            graph,
            policy,
            &capabilities,
            &mut violations,
        );
    }
    violations.into_iter().collect()
}

fn find_in_items(
    path: &str,
    items: &[syn::Item],
    graph: &OwnerGraph,
    policy: &Policy,
    capabilities: &BTreeSet<String>,
    violations: &mut BTreeSet<Finding>,
) {
    for item in items {
        match item {
            syn::Item::Impl(item) => {
                let Some(owner) = type_name(&item.self_ty) else {
                    continue;
                };
                for impl_item in &item.items {
                    let syn::ImplItem::Fn(method) = impl_item else {
                        continue;
                    };
                    if output_contains_owner(
                        &method.sig.output,
                        &owner,
                        graph.identity.in_file(path),
                    ) || policy.is_composition_root(path, &owner, &method.sig.ident.to_string())
                    {
                        continue;
                    }
                    for input in &method.sig.inputs {
                        let syn::FnArg::Typed(input) = input else {
                            continue;
                        };
                        let names = supplied_type_names(&input.ty, graph.identity.in_file(path));
                        for handle in policy.handles.keys().filter(|handle| names.contains(*handle))
                        {
                            violations.insert(Finding::new(
                                path,
                                input.span().start().line,
                                format!(
                                    "{owner}::{} takes handle {handle} outside a constructor",
                                    method.sig.ident
                                ),
                                "a holder receives a handle when it is built, or a free function as its argument",
                            ));
                        }
                        for capability in capabilities {
                            if names.contains(capability) {
                                violations.insert(Finding::new(path, input.span().start().line,
                                    format!("{owner}::{} accepts construction-only capability {capability} at runtime", method.sig.ident),
                                    "a method never takes a raw capability; its value is built with the one it uses, or a free function takes it"));
                            }
                        }
                    }
                }
            }
            syn::Item::Mod(item) => {
                if let Some((_, items)) = &item.content {
                    find_in_items(path, items, graph, policy, capabilities, violations);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
#[path = "retained_capability_parameters_tests.rs"]
mod tests;
