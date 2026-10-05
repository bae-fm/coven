//! Unit capabilities are constructed only at composition roots (§21.2).
//! A value path such as `SystemClock` constructs the capability even without
//! a function call: `SystemClock.now()` must use the clock given to its owner.
//! Test sources and `cfg(test)` items may assemble raw capabilities.

use std::collections::BTreeSet;

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::{parse_macro_body, token_paths};
use crate::policy::Policy;
use crate::syntax::{is_test_only, is_test_source, type_name, RustFile};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct CapabilityConstructionViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) capability: String,
}

pub(crate) fn find_capability_construction_violations(
    files: &[RustFile],
    policy: &Policy,
) -> Vec<CapabilityConstructionViolation> {
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        let mut visitor = ConstructionVisitor {
            path: &file.relative_path,
            policy,
            current_type: None,
            in_composition_root: false,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

struct ConstructionVisitor<'a> {
    path: &'a str,
    policy: &'a Policy,
    current_type: Option<String>,
    in_composition_root: bool,
    violations: &'a mut BTreeSet<CapabilityConstructionViolation>,
}

impl ConstructionVisitor<'_> {
    fn check_name(&mut self, name: &str, span: Span) {
        let name = match (name, &self.current_type) {
            ("Self", Some(name)) => name.as_str(),
            _ => name,
        };
        if !self.in_composition_root
            && self
                .policy
                .construction_only_capability_types
                .contains(&name)
        {
            self.violations.insert(CapabilityConstructionViolation {
                path: self.path.to_string(),
                line: span.start().line,
                capability: name.to_string(),
            });
        }
    }
}

impl<'ast> Visit<'ast> for ConstructionVisitor<'_> {
    fn visit_item(&mut self, node: &'ast syn::Item) {
        // Nested functions, constants and modules do not inherit the authority
        // of the method that declares them. Closures remain part of that method.
        let previous = std::mem::replace(&mut self.in_composition_root, false);
        visit::visit_item(self, node);
        self.in_composition_root = previous;
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if !is_test_only(&node.attrs) {
            visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if !is_test_only(&node.attrs) {
            visit::visit_item_fn(self, node);
        }
    }

    fn visit_item_const(&mut self, node: &'ast syn::ItemConst) {
        if !is_test_only(&node.attrs) {
            visit::visit_item_const(self, node);
        }
    }

    fn visit_item_static(&mut self, node: &'ast syn::ItemStatic) {
        if !is_test_only(&node.attrs) {
            visit::visit_item_static(self, node);
        }
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if is_test_only(&node.attrs) {
            return;
        }
        let previous = std::mem::replace(&mut self.current_type, type_name(&node.self_ty));
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
        let previous = std::mem::replace(&mut self.in_composition_root, allowed);
        visit::visit_impl_item_fn(self, node);
        self.in_composition_root = previous;
    }

    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if let Some(segment) = node.path.segments.last() {
            self.check_name(&segment.ident.to_string(), node.span());
        }
        visit::visit_expr_path(self, node);
    }

    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        // Rust also permits constructing a unit struct with empty braces.
        if node.fields.is_empty() {
            if let Some(segment) = node.path.segments.last() {
                self.check_name(&segment.ident.to_string(), node.span());
            }
        }
        visit::visit_expr_struct(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        match parse_macro_body(node) {
            Some(body) => body.visit(self),
            None => {
                for path in token_paths(node.tokens.clone()) {
                    if !path.after_dot {
                        if let Some(name) = path.segments.last() {
                            self.check_name(name, path.span);
                        }
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
