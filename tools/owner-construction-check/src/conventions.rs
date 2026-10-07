//! The code conventions of §20.3 that read the syntax tree. They hold for every
//! Rust file in the workspace, tools included.
//!
//! - `pub(in path)` and `super::super::` are not used: an item needed
//!   elsewhere moves to where both callers can see it.
//! - `coven` re-exports the API at its root and keeps every module private.
//! - A source file holds at most 1,000 lines.
//! - A source's tests live beside it in `<name>_tests.rs` (`test_layout.rs`).

use std::collections::BTreeSet;

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::token_paths;
use crate::syntax::RustFile;
use crate::test_layout::find_test_layout_violations;

/// The crate whose root re-exports the API (§20.3).
const API_CRATE_SOURCES: &str = "crates/coven/src/";

/// The most lines a source file holds (§20.3). There is no exception list.
const MAX_FILE_LINES: usize = 1_000;

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum Convention {
    DeepParentPath,
    RestrictedVisibility,
    PublicApiModule,
    /// A source file longer than `MAX_FILE_LINES`.
    LongFile {
        lines: usize,
    },
    /// A file named `<name>_test.rs`.
    SingularTestFile,
    /// A `<name>_tests.rs` with no `<name>.rs` beside it.
    OrphanTestFile,
    /// A `#[cfg(test)] mod … { … }` body inside a non-test source.
    InlineTestModule,
    /// A test-only module declaration other than
    /// `#[cfg(test)] #[path = "<name>_tests.rs"] mod tests;`.
    MisplacedTestModule,
}

impl Convention {
    pub(crate) fn remedy(self) -> &'static str {
        match self {
            Convention::DeepParentPath | Convention::RestrictedVisibility => {
                "move an item needed elsewhere to where both callers can see it"
            }
            Convention::PublicApiModule => "declare the module private and `pub use` its API items",
            Convention::LongFile { .. } => {
                "split a long file along its domain and ownership boundaries, without exposing an owner's state to make the split compile"
            }
            Convention::SingularTestFile
            | Convention::OrphanTestFile
            | Convention::InlineTestModule
            | Convention::MisplacedTestModule => {
                "a source's tests live beside it in <name>_tests.rs, declared `#[cfg(test)] #[path = \"<name>_tests.rs\"] mod tests;`"
            }
        }
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct ConventionViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) convention: Convention,
}

impl ConventionViolation {
    pub(crate) fn message(&self) -> String {
        let file = self.path.rsplit('/').next().unwrap_or(&self.path);
        match self.convention {
            Convention::DeepParentPath => {
                "paths cannot skip over the immediate parent module with super::super".to_string()
            }
            Convention::RestrictedVisibility => "pub(in path) visibility is forbidden".to_string(),
            Convention::PublicApiModule => {
                "the coven crate keeps every module private and re-exports its API at its root"
                    .to_string()
            }
            Convention::LongFile { lines } => {
                format!("holds {lines} lines; a source file holds at most {MAX_FILE_LINES}")
            }
            Convention::SingularTestFile => format!(
                "test files are named <name>_tests.rs; rename {file} to {}",
                file.replace("_test.rs", "_tests.rs")
            ),
            Convention::OrphanTestFile => format!(
                "{file} has no {} beside it to hold the tests of",
                file.replace("_tests.rs", ".rs")
            ),
            Convention::InlineTestModule => {
                "a test module's body lives in the sibling <name>_tests.rs, not inline".to_string()
            }
            Convention::MisplacedTestModule => format!(
                "a test module is declared `#[cfg(test)] #[path = \"{}\"] mod tests;`",
                file.replace(".rs", "_tests.rs")
            ),
        }
    }
}

pub(crate) fn find_convention_violations(files: &[RustFile]) -> Vec<ConventionViolation> {
    let mut violations = BTreeSet::new();
    for file in files {
        if file.lines > MAX_FILE_LINES {
            violations.insert(ConventionViolation {
                path: file.relative_path.clone(),
                line: MAX_FILE_LINES + 1,
                convention: Convention::LongFile { lines: file.lines },
            });
        }
        let mut visitor = ConventionVisitor {
            path: &file.relative_path,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.extend(find_test_layout_violations(files));
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

    fn check_tokens(&mut self, tokens: proc_macro2::TokenStream) {
        for path in token_paths(tokens.clone()) {
            if path.segments.len() >= 2 && path.segments[..2] == ["super", "super"] {
                self.record(Convention::DeepParentPath, path.span);
            }
        }
        self.check_restricted_visibility(tokens);
    }

    /// `pub` directly followed by a parenthesized group starting with `in`.
    fn check_restricted_visibility(&mut self, tokens: proc_macro2::TokenStream) {
        let trees = tokens.into_iter().collect::<Vec<_>>();
        for (index, tree) in trees.iter().enumerate() {
            match tree {
                proc_macro2::TokenTree::Group(group) => {
                    self.check_restricted_visibility(group.stream());
                }
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
