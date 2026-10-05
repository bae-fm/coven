//! Each capability is used directly in one place only (§21.2). Outside a
//! capability's homes, naming one of its crates, writing one of its paths or
//! calling one of its methods is the violation; everything else reaches the
//! capability through the owner that holds it.
//!
//! The check is syntactic: naming the crate or the path is the violation, so a
//! new direct use fails before compilation and review. Test sources and
//! `cfg(test)` items are exempt — fixtures may assemble raw material.

use std::collections::BTreeSet;

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::policy::{Capability, Gate, Policy};
use crate::syntax::{flatten_use_tree, is_test_only, is_test_source, RustFile};

#[derive(Debug)]
pub(crate) struct CapabilityBoundaryViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) capability: &'static str,
    pub(crate) kind: &'static str,
    pub(crate) homes: &'static [&'static str],
}

impl CapabilityBoundaryViolation {
    fn key(&self) -> (String, usize, &'static str) {
        (self.path.clone(), self.line, self.kind)
    }
}

pub(crate) fn find_capability_boundary_violations(
    files: &[RustFile],
    policy: &Policy,
) -> Vec<CapabilityBoundaryViolation> {
    let mut violations = Vec::new();
    let mut seen = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        let gated = policy
            .capabilities
            .all()
            .into_iter()
            .filter(|capability| {
                !capability
                    .homes
                    .iter()
                    .any(|home| file.relative_path.starts_with(home))
            })
            .flat_map(|capability| capability.gates.iter().map(move |gate| (capability, gate)))
            .collect::<Vec<_>>();
        if gated.is_empty() {
            continue;
        }
        let mut visitor = CapabilityBoundaryVisitor {
            path: &file.relative_path,
            gated: &gated,
            violations: &mut violations,
            seen: &mut seen,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.sort_by_key(CapabilityBoundaryViolation::key);
    violations
}

struct CapabilityBoundaryVisitor<'a, 'p> {
    path: &'a str,
    gated: &'a [(&'p Capability, &'p Gate)],
    violations: &'a mut Vec<CapabilityBoundaryViolation>,
    seen: &'a mut BTreeSet<(String, usize, &'static str)>,
}

impl<'p> CapabilityBoundaryVisitor<'_, 'p> {
    fn record(&mut self, capability: &'p Capability, gate: &'p Gate, span: Span) {
        let violation = CapabilityBoundaryViolation {
            path: self.path.to_string(),
            line: span.start().line,
            capability: capability.name,
            kind: gate.kind,
            homes: capability.homes,
        };
        if self.seen.insert(violation.key()) {
            self.violations.push(violation);
        }
    }

    /// `is_import` distinguishes `use` trees from expression, type, and macro
    /// paths. In an import, a bare crate name (`use open;`) references the
    /// crate; in an expression, a single-segment path (`open(...)`) is a local
    /// item and must not match a gated crate of the same name.
    fn check_segments(&mut self, segments: &[String], is_import: bool, span: Span) {
        for &(capability, gate) in self.gated {
            let first_is_gated_crate = (is_import || segments.len() >= 2)
                && segments
                    .first()
                    .is_some_and(|first| gate.crates.iter().any(|name| first == name));
            let contains_pattern = gate.path_patterns.iter().any(|pattern| {
                segments
                    .windows(pattern.len())
                    .any(|window| window.iter().zip(pattern.iter()).all(|(a, b)| a == b))
            });
            if first_is_gated_crate || contains_pattern {
                self.record(capability, gate, span);
            }
        }
    }
}

impl<'ast> Visit<'ast> for CapabilityBoundaryVisitor<'_, '_> {
    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let name = node.method.to_string();
        for &(capability, gate) in self.gated {
            if gate.method_patterns.iter().any(|pattern| *pattern == name) {
                self.record(capability, gate, node.method.span());
            }
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_attribute(&mut self, node: &'ast syn::Attribute) {
        if node.path().is_ident("doc") {
            return;
        }
        visit::visit_attribute(self, node);
    }

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
        visit::visit_item_impl(self, node);
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_item_fn(self, node);
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_impl_item_fn(self, node);
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        let mut paths = Vec::new();
        flatten_use_tree(&node.tree, &mut Vec::new(), &mut paths);
        for segments in paths {
            self.check_segments(&segments, true, node.span());
        }
    }

    fn visit_path(&mut self, node: &'ast syn::Path) {
        let segments = node
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        self.check_segments(&segments, false, node.span());
        visit::visit_path(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        let segments = node
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        self.check_segments(&segments, false, node.span());
        visit::visit_macro(self, node);
    }
}

#[cfg(test)]
#[path = "capability_boundaries_tests.rs"]
mod tests;
