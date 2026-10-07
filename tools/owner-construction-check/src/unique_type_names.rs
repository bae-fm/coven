//! Name-based matching requires a unique declaration for each policy type,
//! construction-only capability and inferred owner across `crates/`. Policy
//! guards check that named types exist; this rule rejects ambiguous names.
//! Test-only declarations are excluded; `test-utils` declarations count.

use std::collections::{BTreeMap, BTreeSet};

use syn::visit::{self, Visit};

use crate::capability_construction::construction_only_types;
use crate::macros::parse_macro_body;
use crate::owner_construction::infer_owners;
use crate::policy::Policy;
use crate::syntax::{collect_declared_types, is_test_only, is_test_source, RustFile};

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct UniqueTypeNameViolation {
    pub(crate) name: String,
    pub(crate) paths: Vec<String>,
}

pub(crate) fn find_unique_type_name_violations(
    files: &[RustFile],
    policy: &Policy,
) -> Vec<UniqueTypeNameViolation> {
    let mut reasoned_about = policy
        .named_types()
        .into_iter()
        .map(|(_, name)| name.to_string())
        .collect::<BTreeSet<_>>();
    reasoned_about.extend(construction_only_types(files, policy));
    reasoned_about.extend(infer_owners(&collect_declared_types(files), policy));

    let mut declarations = BTreeMap::<String, Vec<String>>::new();
    for file in files {
        if is_test_source(&file.relative_path) || is_test_only(&file.syntax.attrs) {
            continue;
        }
        DeclarationCollector {
            path: &file.relative_path,
            declarations: &mut declarations,
        }
        .visit_file(&file.syntax);
    }
    declarations
        .into_iter()
        .filter(|(name, paths)| reasoned_about.contains(name) && paths.len() > 1)
        .map(|(name, mut paths)| {
            // Multiple modules can declare the name in the same file. Count
            // those declarations before listing each declaring file once.
            paths.sort();
            paths.dedup();
            UniqueTypeNameViolation { name, paths }
        })
        .collect()
}

struct DeclarationCollector<'a> {
    path: &'a str,
    declarations: &'a mut BTreeMap<String, Vec<String>>,
}

impl<'ast> Visit<'ast> for DeclarationCollector<'_> {
    fn visit_item(&mut self, node: &'ast syn::Item) {
        let (attrs, name) = match node {
            syn::Item::Struct(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Enum(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Union(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Trait(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Type(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Mod(item) => (&item.attrs, None),
            syn::Item::Fn(item) => (&item.attrs, None),
            syn::Item::Impl(item) => (&item.attrs, None),
            syn::Item::Const(item) => (&item.attrs, None),
            syn::Item::Static(item) => (&item.attrs, None),
            syn::Item::ForeignMod(item) => (&item.attrs, None),
            syn::Item::Macro(item) => (&item.attrs, None),
            _ => return,
        };
        if is_test_only(attrs) {
            return;
        }
        if let Some(name) = name {
            self.declarations
                .entry(name.to_string())
                .or_default()
                .push(self.path.to_string());
        }
        visit::visit_item(self, node);
    }

    fn visit_impl_item(&mut self, node: &'ast syn::ImplItem) {
        let attrs = match node {
            syn::ImplItem::Fn(item) => &item.attrs,
            syn::ImplItem::Const(item) => &item.attrs,
            syn::ImplItem::Type(item) => &item.attrs,
            syn::ImplItem::Macro(item) => &item.attrs,
            _ => return,
        };
        if !is_test_only(attrs) {
            visit::visit_impl_item(self, node);
        }
    }

    fn visit_trait_item(&mut self, node: &'ast syn::TraitItem) {
        let attrs = match node {
            syn::TraitItem::Fn(item) => &item.attrs,
            syn::TraitItem::Const(item) => &item.attrs,
            syn::TraitItem::Type(item) => &item.attrs,
            syn::TraitItem::Macro(item) => &item.attrs,
            _ => return,
        };
        if !is_test_only(attrs) {
            visit::visit_trait_item(self, node);
        }
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

#[cfg(test)]
#[path = "unique_type_names_tests.rs"]
mod tests;
