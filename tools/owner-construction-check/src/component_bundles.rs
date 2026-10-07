//! A struct built only to be taken apart again, with every field public and
//! no methods, is not used to pass collaborators; they are passed by name
//! (§20.2).
//!
//! A bundle type has two or more fields, all reachable outside its module, and
//! one inherent method: an associated `new` returning it. Building one with
//! `Bundle::new(…)` straight into a destructuring `let` is the violation.

use std::collections::{BTreeMap, BTreeSet};

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::parse_macro_body;
use crate::syntax::{
    is_test_only, is_test_source, output_contains_owner, type_name, visibility_crosses_owner,
    RustFile,
};

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct ComponentBundleViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) bundle: String,
}

#[derive(Default)]
struct BundleTypeInfo {
    public_fields: usize,
    field_count: usize,
    /// `(name, returns the type, takes a receiver)` per inherent method.
    inherent_methods: Vec<(String, bool, bool)>,
}

pub(crate) fn find_component_bundle_violations(
    files: &[RustFile],
) -> Vec<ComponentBundleViolation> {
    let bundle_types = collect_bundle_types(files);
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        let mut visitor = ComponentBundleVisitor {
            path: &file.relative_path,
            bundle_types: &bundle_types,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

fn collect_bundle_types(files: &[RustFile]) -> BTreeSet<String> {
    let mut types = BTreeMap::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        collect_bundle_type_info(&file.syntax.items, &mut types);
    }
    types
        .into_iter()
        .filter_map(|(name, info)| {
            (info.field_count >= 2
                && info.field_count == info.public_fields
                && matches!(info.inherent_methods.as_slice(), [(method, true, false)] if method == "new"))
            .then_some(name)
        })
        .collect()
}

fn collect_bundle_type_info(items: &[syn::Item], types: &mut BTreeMap<String, BundleTypeInfo>) {
    for item in items {
        match item {
            syn::Item::Struct(item) if !is_test_only(&item.attrs) => {
                let info = types.entry(item.ident.to_string()).or_default();
                info.field_count = item.fields.len();
                info.public_fields = item
                    .fields
                    .iter()
                    .filter(|field| visibility_crosses_owner(&field.vis))
                    .count();
            }
            syn::Item::Impl(item) if item.trait_.is_none() && !is_test_only(&item.attrs) => {
                let Some(name) = type_name(&item.self_ty) else {
                    continue;
                };
                let info = types.entry(name.clone()).or_default();
                for method in &item.items {
                    let syn::ImplItem::Fn(method) = method else {
                        continue;
                    };
                    if is_test_only(&method.attrs) {
                        continue;
                    }
                    info.inherent_methods.push((
                        method.sig.ident.to_string(),
                        output_contains_owner(&method.sig.output, &name),
                        method.sig.receiver().is_some(),
                    ));
                }
            }
            syn::Item::Mod(item) if !is_test_only(&item.attrs) => {
                if let Some((_, items)) = &item.content {
                    collect_bundle_type_info(items, types);
                }
            }
            _ => {}
        }
    }
}

struct ComponentBundleVisitor<'a> {
    path: &'a str,
    bundle_types: &'a BTreeSet<String>,
    violations: &'a mut BTreeSet<ComponentBundleViolation>,
}

impl<'ast> Visit<'ast> for ComponentBundleVisitor<'_> {
    fn visit_local(&mut self, node: &'ast syn::Local) {
        let syn::Pat::Struct(pattern) = &node.pat else {
            return visit::visit_local(self, node);
        };
        let Some(initializer) = &node.init else {
            return visit::visit_local(self, node);
        };
        let syn::Expr::Call(call) = initializer.expr.as_ref() else {
            return visit::visit_local(self, node);
        };
        let syn::Expr::Path(function) = call.func.as_ref() else {
            return visit::visit_local(self, node);
        };
        let Some(bundle) = pattern
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
        else {
            return visit::visit_local(self, node);
        };
        let Some(constructor) = function.path.segments.last() else {
            return visit::visit_local(self, node);
        };
        let Some(owner) = function.path.segments.iter().rev().nth(1) else {
            return visit::visit_local(self, node);
        };
        if constructor.ident == "new"
            && owner.ident == bundle
            && self.bundle_types.contains(&bundle)
        {
            self.violations.insert(ComponentBundleViolation {
                path: self.path.to_string(),
                line: node.span().start().line,
                bundle,
            });
        }
        visit::visit_local(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
        visit::visit_macro(self, node);
    }
}

#[cfg(test)]
#[path = "component_bundles_tests.rs"]
mod tests;
