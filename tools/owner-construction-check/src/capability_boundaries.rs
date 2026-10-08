//! Each capability is used directly in one place only (§20.2). Outside a
//! capability's homes, naming one of its crates, writing one of its paths or
//! calling one of its methods is the violation; everything else reaches the
//! capability through the owner that holds it.
//!
//! The check is syntactic: naming the crate or the path is the violation, so a
//! new direct use fails before compilation and review. It reads inside macro
//! calls too, so `format!("{}", Uuid::new_v4())` is a direct use. Test sources and
//! `cfg(test)` items are exempt — fixtures may assemble raw material.

use std::collections::BTreeSet;

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::finding::Finding;
use crate::macros::{parse_macro_body, token_paths};
use crate::policy::{Capability, Gate, Policy};
use crate::syntax::{flatten_use_tree, is_test_only, is_test_source, type_name, RustFile};

pub(crate) fn find_capability_boundary_violations(
    files: &[RustFile],
    policy: &Policy,
) -> Vec<Finding> {
    let mut violations = BTreeSet::new();
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
        let mut visitor = CapabilityBoundaryVisitor {
            path: &file.relative_path,
            gated: &gated,
            violations: &mut violations,
            policy,
            current_type: "<free>".into(),
            rooted: false,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

struct CapabilityBoundaryVisitor<'a, 'p> {
    path: &'a str,
    gated: &'a [(&'p Capability, &'p Gate)],
    violations: &'a mut BTreeSet<Finding>,
    policy: &'p Policy,
    current_type: String,
    rooted: bool,
}

impl<'p> CapabilityBoundaryVisitor<'_, 'p> {
    fn record(&mut self, capability: &'p Capability, gate: &'p Gate, span: Span) {
        self.violations.insert(Finding::new(
            self.path,
            span.start().line,
            format!(
                "{} ({}) is used directly only in {}",
                gate.kind,
                capability.name,
                if capability.homes.is_empty() {
                    "no file yet".to_string()
                } else {
                    capability.homes.join(", ")
                }
            ),
            "reach a capability through the owner that holds it, given to you when you are built",
        ));
    }

    fn record_task_start(&mut self, gate: &Gate, span: Span) {
        self.violations.insert(Finding::new(
            self.path,
            span.start().line,
            format!("{} outside a composition root", gate.kind),
            "start long-lived work only at a listed root, which must retain and stop it",
        ));
    }

    fn check_method(&mut self, name: &str, span: Span) {
        if !self.rooted {
            for gate in self.policy.task_starts {
                if gate.method_patterns.contains(&name) {
                    self.record_task_start(gate, span);
                }
            }
        }
        for &(capability, gate) in self.gated {
            if gate.method_patterns.contains(&name) {
                self.record(capability, gate, span);
            }
        }
    }

    /// `is_import` distinguishes `use` trees from expression, type, and macro
    /// paths. In an import, a bare crate name (`use open;`) references the
    /// crate; in an expression, a single-segment path (`open(...)`) is a local
    /// item and must not match a gated crate of the same name.
    fn check_segments(&mut self, segments: &[String], is_import: bool, span: Span) {
        if !self.rooted {
            for gate in self.policy.task_starts {
                if gate_matches(gate, segments, is_import) {
                    self.record_task_start(gate, span);
                }
            }
        }
        for &(capability, gate) in self.gated {
            if gate_matches(gate, segments, is_import) {
                self.record(capability, gate, span);
            }
        }
    }
}

fn gate_matches(gate: &Gate, segments: &[String], is_import: bool) -> bool {
    let first_is_gated_crate = (is_import || segments.len() >= 2)
        && segments
            .first()
            .is_some_and(|first| gate.crates.iter().any(|name| first == name));
    let contains_pattern = gate.path_patterns.iter().any(|pattern| {
        segments
            .windows(pattern.len())
            .any(|window| window.iter().zip(pattern.iter()).all(|(a, b)| a == b))
    });
    first_is_gated_crate || contains_pattern
}

impl<'ast> Visit<'ast> for CapabilityBoundaryVisitor<'_, '_> {
    fn visit_item(&mut self, node: &'ast syn::Item) {
        let previous = self.rooted;
        self.rooted = false;
        visit::visit_item(self, node);
        self.rooted = previous;
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        self.check_method(&node.method.to_string(), node.method.span());
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
        let previous = std::mem::replace(
            &mut self.current_type,
            type_name(&node.self_ty).unwrap_or_else(|| "<impl>".into()),
        );
        visit::visit_item_impl(self, node);
        self.current_type = previous;
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        let previous = self.rooted;
        self.rooted =
            self.policy
                .is_composition_root(self.path, "<free>", &node.sig.ident.to_string());
        visit::visit_item_fn(self, node);
        self.rooted = previous;
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        let previous = self.rooted;
        self.rooted = self.policy.is_composition_root(
            self.path,
            &self.current_type,
            &node.sig.ident.to_string(),
        );
        visit::visit_impl_item_fn(self, node);
        self.rooted = previous;
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
        match parse_macro_body(node) {
            Some(body) => body.visit(self),
            None => {
                for path in token_paths(node.tokens.clone()) {
                    if path.after_dot {
                        self.check_method(&path.segments[0], path.span);
                    } else {
                        self.check_segments(&path.segments, false, path.span);
                    }
                }
            }
        }
        visit::visit_macro(self, node);
    }
}

#[cfg(test)]
#[path = "capability_boundaries_tests.rs"]
mod tests;
