//! Where tests live (§20.3): a source's tests live beside it in
//! `<name>_tests.rs`, declared from the source as
//! `#[cfg(test)] #[path = "<name>_tests.rs"] mod tests;`, and a crate's
//! integration tests in its `tests/` directory.
//!
//! So a test module never has an inline body in a non-test source, a test file
//! is never named `<name>_test.rs`, and a `<name>_tests.rs` always has the
//! `<name>.rs` it tests beside it.

use std::collections::BTreeSet;

use syn::visit::{self, Visit};

use crate::conventions::Convention;
use crate::finding::Finding;
use crate::syntax::{is_integration_test_source, is_test_only, is_test_source, RustFile};

pub(crate) fn find_test_layout_violations(files: &[RustFile]) -> Vec<Finding> {
    let paths = files
        .iter()
        .map(|file| file.relative_path.as_str())
        .collect::<BTreeSet<_>>();
    let mut violations = Vec::new();
    for file in files {
        let path = file.relative_path.as_str();
        let (directory, name) = path.rsplit_once('/').unwrap_or(("", path));
        if name.ends_with("_test.rs") {
            violations.push(violation(path, 1, Convention::SingularTestFile));
        }
        if let Some(subject) = name.strip_suffix("_tests.rs") {
            let sibling = format!("{directory}/{subject}.rs");
            if !is_integration_test_source(path) && !paths.contains(sibling.as_str()) {
                violations.push(violation(path, 1, Convention::OrphanTestFile));
            }
        }
        if is_test_source(path) {
            continue;
        }
        let mut visitor = TestModuleVisitor {
            path,
            expected_path: name.replace(".rs", "_tests.rs"),
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations
}

fn violation(path: &str, line: usize, convention: Convention) -> Finding {
    convention.finding(path, line)
}

struct TestModuleVisitor<'a> {
    path: &'a str,
    /// `<name>_tests.rs` for this file's `<name>.rs`.
    expected_path: String,
    violations: &'a mut Vec<Finding>,
}

impl<'ast> Visit<'ast> for TestModuleVisitor<'_> {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if is_test_only(&node.attrs) {
            let line = node.ident.span().start().line;
            if node.content.is_some() {
                self.violations
                    .push(violation(self.path, line, Convention::InlineTestModule));
            } else if node.ident != "tests" || !self.declares_sibling_path(&node.attrs) {
                self.violations
                    .push(violation(self.path, line, Convention::MisplacedTestModule));
            }
        }
        visit::visit_item_mod(self, node);
    }
}

impl TestModuleVisitor<'_> {
    fn declares_sibling_path(&self, attrs: &[syn::Attribute]) -> bool {
        attrs.iter().any(|attribute| {
            matches!(
                &attribute.meta,
                syn::Meta::NameValue(syn::MetaNameValue {
                    path,
                    value: syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(value),
                        ..
                    }),
                    ..
                }) if path.is_ident("path") && value.value() == self.expected_path
            )
        })
    }
}

#[cfg(test)]
#[path = "test_layout_tests.rs"]
mod tests;
