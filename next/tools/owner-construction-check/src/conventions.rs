//! The code conventions of §21.3 that read the syntax tree. They hold for every
//! Rust file in the workspace, tools included.
//!
//! - `pub(in path)` and `super::super::` are not used: an item needed
//!   elsewhere moves to where both callers can see it.
//! - `coven` re-exports the API at its root and keeps every module private.

use std::collections::BTreeSet;

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::syntax::RustFile;

/// The crate whose root re-exports the API (§21.3).
const API_CRATE_SOURCES: &str = "crates/coven/src/";

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum Convention {
    DeepParentPath,
    RestrictedVisibility,
    PublicApiModule,
}

impl Convention {
    pub(crate) fn message(self) -> &'static str {
        match self {
            Convention::DeepParentPath => {
                "paths cannot skip over the immediate parent module with super::super"
            }
            Convention::RestrictedVisibility => "pub(in path) visibility is forbidden",
            Convention::PublicApiModule => {
                "the coven crate keeps every module private and re-exports its API at its root"
            }
        }
    }

    pub(crate) fn remedy(self) -> &'static str {
        match self {
            Convention::DeepParentPath | Convention::RestrictedVisibility => {
                "move an item needed elsewhere to where both callers can see it"
            }
            Convention::PublicApiModule => "declare the module private and `pub use` its API items",
        }
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct ConventionViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) convention: Convention,
}

pub(crate) fn find_convention_violations(files: &[RustFile]) -> Vec<ConventionViolation> {
    let mut violations = BTreeSet::new();
    for file in files {
        let mut visitor = ConventionVisitor {
            path: &file.relative_path,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

struct ConventionVisitor<'a> {
    path: &'a str,
    violations: &'a mut BTreeSet<ConventionViolation>,
}

impl ConventionVisitor<'_> {
    fn record(&mut self, convention: Convention, span: Span) {
        self.violations.insert(ConventionViolation {
            path: self.path.to_string(),
            line: span.start().line,
            convention,
        });
    }
}

impl ConventionVisitor<'_> {
    fn check_tokens(&mut self, tokens: proc_macro2::TokenStream) {
        let trees = tokens.into_iter().collect::<Vec<_>>();
        for (index, tree) in trees.iter().enumerate() {
            match tree {
                proc_macro2::TokenTree::Group(group) => self.check_tokens(group.stream()),
                proc_macro2::TokenTree::Ident(ident) if ident == "pub" => {
                    if let Some(proc_macro2::TokenTree::Group(group)) = trees.get(index + 1) {
                        let restricted_in = group.delimiter()
                            == proc_macro2::Delimiter::Parenthesis
                            && matches!(
                                group.stream().into_iter().next(),
                                Some(proc_macro2::TokenTree::Ident(first)) if first == "in"
                            );
                        if restricted_in {
                            self.record(Convention::RestrictedVisibility, ident.span());
                        }
                    }
                }
                proc_macro2::TokenTree::Ident(ident) if ident == "super" => {
                    let skips_parent = matches!(
                        (trees.get(index + 1), trees.get(index + 2), trees.get(index + 3)),
                        (
                            Some(proc_macro2::TokenTree::Punct(first)),
                            Some(proc_macro2::TokenTree::Punct(second)),
                            Some(proc_macro2::TokenTree::Ident(next)),
                        ) if first.as_char() == ':' && second.as_char() == ':' && next == "super"
                    );
                    let follows_separator = index >= 2
                        && matches!(
                            &trees[index - 1],
                            proc_macro2::TokenTree::Punct(punct) if punct.as_char() == ':'
                        );
                    if skips_parent && !follows_separator {
                        self.record(Convention::DeepParentPath, ident.span());
                    }
                }
                _ => {}
            }
        }
    }
}

impl<'ast> Visit<'ast> for ConventionVisitor<'_> {
    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        if use_tree_skips_parent(&node.tree, 0) {
            self.record(Convention::DeepParentPath, node.span());
        }
        visit::visit_item_use(self, node);
    }

    fn visit_path(&mut self, node: &'ast syn::Path) {
        if node.segments.len() >= 2
            && node
                .segments
                .iter()
                .take(2)
                .all(|segment| segment.ident == "super")
        {
            self.record(Convention::DeepParentPath, node.span());
        }
        visit::visit_path(self, node);
    }

    fn visit_vis_restricted(&mut self, node: &'ast syn::VisRestricted) {
        if node.in_token.is_some() {
            self.record(Convention::RestrictedVisibility, node.span());
        }
        visit::visit_vis_restricted(self, node);
    }

    /// A macro's arguments are unparsed tokens the visitor never enters, and a
    /// `macro_rules!` body expands wherever it is used, so both rules read the
    /// tokens directly.
    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        self.check_tokens(node.tokens.clone());
        visit::visit_macro(self, node);
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if self.path.starts_with(API_CRATE_SOURCES)
            && !matches!(node.vis, syn::Visibility::Inherited)
        {
            self.record(Convention::PublicApiModule, node.ident.span());
        }
        visit::visit_item_mod(self, node);
    }
}

fn use_tree_skips_parent(tree: &syn::UseTree, leading_parents: usize) -> bool {
    match tree {
        syn::UseTree::Path(path) => {
            let leading_parents = if path.ident == "super" {
                leading_parents + 1
            } else {
                0
            };
            leading_parents >= 2 || use_tree_skips_parent(&path.tree, leading_parents)
        }
        syn::UseTree::Group(group) => group
            .items
            .iter()
            .any(|tree| use_tree_skips_parent(tree, leading_parents)),
        syn::UseTree::Name(_) | syn::UseTree::Rename(_) | syn::UseTree::Glob(_) => false,
    }
}

#[cfg(test)]
#[path = "conventions_tests.rs"]
mod tests;
