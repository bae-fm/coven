//! Each capability is used directly in one place only (Appendix F.2). Outside a
//! capability's homes, naming one of its crates, writing one of its paths or
//! calling one of its methods is the violation; everything else reaches the
//! capability through the owner that holds it.
//!
//! The check is syntactic: naming the crate or the path is the violation, so a
//! new direct use fails before compilation and review. A gate's `calls` match
//! only a function that code calls or passes as a value, by the path it
//! resolves to through its file's imports (`NameResolver`), so code may name
//! a capability's types and import its modules. The check reads inside macro
//! calls too, so `format!("{}", Uuid::new_v4())` is a direct use. It reads no
//! test code (`test_code.rs`): fixtures may assemble raw material.

use std::collections::BTreeSet;

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::finding::Finding;
use crate::macros::{parse_macro_body, token_paths, unparsed_tokens};
use crate::name_resolution::{qualified_path, NameResolver};
use crate::policy::{describe_homes, Capability, Gate, Policy};
use crate::syntax::{flatten_use_tree, type_name, RustFile};

pub(crate) fn find_capability_boundary_violations(
    files: &[RustFile],
    policy: &Policy,
) -> Vec<Finding> {
    let resolver = NameResolver::collect(files);
    let mut violations = BTreeSet::new();
    for file in files {
        let gated = policy
            .capabilities
            .iter()
            .filter(|capability| {
                !capability.homes.iter().any(|home| file.relative_path.starts_with(home))
            })
            .flat_map(|capability| capability.gates.iter().map(move |gate| (capability, gate)))
            .collect::<Vec<_>>();
        let mut visitor = CapabilityBoundaryVisitor {
            path: &file.relative_path,
            gated: &gated,
            violations: &mut violations,
            policy,
            resolver: &resolver,
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
    resolver: &'a NameResolver,
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
                describe_homes(&capability.homes)
            ),
            "reach a capability through the owner that holds it, given to you when you are built",
        ));
    }

    fn record_background_start(&mut self, gate: &Gate, span: Span) {
        self.violations.insert(Finding::new(
            self.path,
            span.start().line,
            format!("{} outside a composition root", gate.kind),
            "start background work only at a listed root, which must keep and stop it",
        ));
    }

    fn check_method(&mut self, name: &str, span: Span) {
        if !self.rooted {
            for gate in &self.policy.background_starts {
                if gate.method_patterns.iter().any(|method| method == name) {
                    self.record_background_start(gate, span);
                }
            }
        }
        for &(capability, gate) in self.gated {
            if gate.method_patterns.iter().any(|method| method == name) {
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
            for gate in &self.policy.background_starts {
                if gate_matches(gate, segments, is_import) {
                    self.record_background_start(gate, span);
                }
            }
        }
        for &(capability, gate) in self.gated {
            if gate_matches(gate, segments, is_import) {
                self.record(capability, gate, span);
            }
        }
    }

    /// A function that code calls or passes as a value, by its path as
    /// written (`""` first for a leading `::`), against the gates' `calls`.
    fn check_call(&mut self, segments: &[String], span: Span) {
        let resolved = self.resolver.resolve(self.path, span, segments);
        let path = match &resolved {
            Ok(path) => path.as_slice(),
            Err(_) => {
                let written = segments.strip_prefix(&[String::new()]).unwrap_or(segments);
                // A single name that resolves to nothing is a local binding.
                if written.len() < 2 {
                    return;
                }
                written
            }
        };
        if !self.rooted {
            for gate in &self.policy.background_starts {
                if holds_any(path, &gate.call_patterns) {
                    self.record_background_start(gate, span);
                }
            }
        }
        for &(capability, gate) in self.gated {
            if holds_any(path, &gate.call_patterns) {
                self.record(capability, gate, span);
            }
        }
    }
}

/// Whether `segments` holds one of `patterns` as adjacent segments.
fn holds_any(segments: &[String], patterns: &[Vec<String>]) -> bool {
    patterns.iter().any(|pattern| {
        segments
            .windows(pattern.len())
            .any(|window| window.iter().zip(pattern.iter()).all(|(a, b)| a == b))
    })
}

/// An expression path's segments as written, a qualified one's from its
/// type: `<File>::open` and `<File as Opener>::open` are `File::open`.
fn expression_path(node: &syn::ExprPath) -> Option<Vec<String>> {
    let Some(qself) = &node.qself else { return Some(qualified_path(&node.path)) };
    let syn::Type::Path(ty) = qself.ty.as_ref() else { return None };
    let rest = node.path.segments.iter().skip(qself.position);
    Some(
        qualified_path(&ty.path)
            .into_iter()
            .chain(rest.map(|segment| segment.ident.to_string()))
            .collect(),
    )
}

fn gate_matches(gate: &Gate, segments: &[String], is_import: bool) -> bool {
    // SQL value conversions, and the errors and column types they report,
    // acquire no connection or other capability. rusqlite exports `ToSql` both
    // at its root and from `types`.
    if segments.first().is_some_and(|name| name == "rusqlite") {
        let path: Vec<_> = segments.iter().map(String::as_str).collect();
        if matches!(
            path.as_slice(),
            ["rusqlite", "ToSql" | "Result" | "Error", ..]
                | [
                    "rusqlite",
                    "types",
                    "FromSql"
                        | "FromSqlError"
                        | "FromSqlResult"
                        | "ToSql"
                        | "ToSqlOutput"
                        | "Type"
                        | "Value"
                        | "ValueRef",
                    ..
                ]
        ) {
            return false;
        }
    }
    let first_is_gated_crate = (is_import || segments.len() >= 2)
        && segments.first().is_some_and(|first| gate.crates.iter().any(|name| first == name));
    first_is_gated_crate || holds_any(segments, &gate.path_patterns)
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

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        let previous = std::mem::replace(
            &mut self.current_type,
            type_name(&node.self_ty).unwrap_or_else(|| "<impl>".into()),
        );
        visit::visit_item_impl(self, node);
        self.current_type = previous;
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        let previous = self.rooted;
        self.rooted =
            self.policy.is_composition_root(self.path, "<free>", &node.sig.ident.to_string());
        visit::visit_item_fn(self, node);
        self.rooted = previous;
    }

    fn visit_item_static(&mut self, node: &'ast syn::ItemStatic) {
        let previous = self.rooted;
        self.rooted = self.policy.is_composition_root(self.path, "<free>", &node.ident.to_string());
        visit::visit_item_static(self, node);
        self.rooted = previous;
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
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

    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if let Some(segments) = expression_path(node) {
            self.check_call(&segments, node.span());
        }
        visit::visit_expr_path(self, node);
    }

    fn visit_path(&mut self, node: &'ast syn::Path) {
        let segments =
            node.segments.iter().map(|segment| segment.ident.to_string()).collect::<Vec<_>>();
        self.check_segments(&segments, false, node.span());
        visit::visit_path(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        let segments =
            node.path.segments.iter().map(|segment| segment.ident.to_string()).collect::<Vec<_>>();
        self.check_segments(&segments, false, node.span());
        match parse_macro_body(node) {
            Some(body) => body.visit(self),
            None => {
                for path in token_paths(unparsed_tokens(node)) {
                    if path.after_dot {
                        self.check_method(&path.segments[0], path.span);
                    } else {
                        self.check_segments(&path.segments, false, path.span);
                        if !path.type_position {
                            let leading = path.leading_colon.then(String::new);
                            let segments =
                                leading.into_iter().chain(path.segments).collect::<Vec<_>>();
                            self.check_call(&segments, path.span);
                        }
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
