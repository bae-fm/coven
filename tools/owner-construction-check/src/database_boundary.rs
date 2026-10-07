//! The SQLite capability (§20.2): nothing outside coven-database gets the
//! SQLite connection; it asks the database owner to run a write.
//!
//! Outside the SQLite homes, naming a raw rusqlite handle — connection,
//! session or transaction — is the violation, as is writing SQL that names one
//! of coven's own tables. rusqlite's values, results and parameter macros are
//! part of the API (Appendix E) and stay allowed.

use std::collections::BTreeSet;

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::{parse_macro_body, token_paths, token_strings};
use crate::policy::Policy;
use crate::syntax::RustFile;

/// rusqlite's raw handles. Only the database crate holds one.
pub(crate) const RAW_SQLITE_HANDLES: &[(&str, &str)] = &[
    ("Connection", "raw SQLite connection"),
    ("Session", "raw SQLite session"),
    ("Transaction", "raw SQLite transaction"),
];

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct DatabaseBoundaryViolation {
    pub(crate) path: String,
    pub(crate) line: usize,
    pub(crate) kind: String,
}

pub(crate) fn find_database_boundary_violations(
    files: &[RustFile],
    policy: &Policy,
) -> Vec<DatabaseBoundaryViolation> {
    let mut violations = BTreeSet::new();
    let coven_tables = collect_coven_table_names(files, policy);
    for file in files {
        if policy
            .capabilities
            .sqlite
            .homes
            .iter()
            .any(|home| file.relative_path.starts_with(home))
        {
            continue;
        }
        let mut visitor = DatabaseBoundaryVisitor {
            path: &file.relative_path,
            coven_tables: &coven_tables,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

struct DatabaseBoundaryVisitor<'a> {
    path: &'a str,
    coven_tables: &'a BTreeSet<String>,
    violations: &'a mut BTreeSet<DatabaseBoundaryViolation>,
}

impl DatabaseBoundaryVisitor<'_> {
    fn record(&mut self, kind: &str, span: Span) {
        self.violations.insert(DatabaseBoundaryViolation {
            path: self.path.to_string(),
            line: span.start().line,
            kind: kind.to_string(),
        });
    }

    fn check_sql(&mut self, literal: &syn::LitStr) {
        if let Some(table) = coven_table_in_sql(&literal.value(), self.coven_tables) {
            self.record(
                &format!("coven-owned SQL for table {table}"),
                literal.span(),
            );
        }
    }
}

impl<'ast> Visit<'ast> for DatabaseBoundaryVisitor<'_> {
    fn visit_attribute(&mut self, node: &'ast syn::Attribute) {
        if node.path().is_ident("doc") {
            return;
        }
        visit::visit_attribute(self, node);
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        let mut capabilities = BTreeSet::new();
        collect_forbidden_sqlite_imports(&node.tree, false, false, &mut capabilities);
        for capability in capabilities {
            self.record(capability, node.span());
        }
        visit::visit_item_use(self, node);
    }

    fn visit_path(&mut self, node: &'ast syn::Path) {
        if let Some(capability) = forbidden_sqlite_path(node) {
            self.record(capability, node.span());
        }
        visit::visit_path(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(capability) = forbidden_sqlite_path(&node.path) {
            self.record(capability, node.span());
        }
        match parse_macro_body(node) {
            Some(body) => body.visit(self),
            None => {
                for path in token_paths(node.tokens.clone()) {
                    if let Some(capability) = forbidden_sqlite_segments(&path.segments) {
                        self.record(capability, path.span);
                    }
                }
                for string in token_strings(node.tokens.clone()) {
                    self.check_sql(&string);
                }
            }
        }
        visit::visit_macro(self, node);
    }

    fn visit_lit_str(&mut self, node: &'ast syn::LitStr) {
        self.check_sql(node);
        visit::visit_lit_str(self, node);
    }
}

/// The tables the policy's schema file declares through its table macros.
fn collect_coven_table_names(files: &[RustFile], policy: &Policy) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();
    let Some((schema_file, table_macros)) = policy.database_schema else {
        return tables;
    };
    for file in files {
        if file.relative_path != schema_file {
            continue;
        }
        for item in &file.syntax.items {
            let syn::Item::Macro(item) = item else {
                continue;
            };
            if !item
                .ident
                .as_ref()
                .is_some_and(|ident| table_macros.iter().any(|name| ident == name))
            {
                continue;
            }
            collect_table_macro_invocations(item.mac.tokens.clone(), &mut tables);
        }
    }
    tables
}

fn collect_table_macro_invocations(
    tokens: proc_macro2::TokenStream,
    tables: &mut BTreeSet<String>,
) {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    for (index, token) in tokens.iter().enumerate() {
        let proc_macro2::TokenTree::Group(group) = token else {
            continue;
        };
        let is_table_invocation = index >= 3
            && matches!(&tokens[index - 3], proc_macro2::TokenTree::Punct(punct) if punct.as_char() == '$')
            && matches!(&tokens[index - 2], proc_macro2::TokenTree::Ident(ident) if ident == "visit")
            && matches!(&tokens[index - 1], proc_macro2::TokenTree::Punct(punct) if punct.as_char() == '!');
        if is_table_invocation {
            if let Some(proc_macro2::TokenTree::Ident(table)) = group.stream().into_iter().next() {
                tables.insert(table.to_string());
            }
        }
        collect_table_macro_invocations(group.stream(), tables);
    }
}

fn coven_table_in_sql<'a>(sql: &str, tables: &'a BTreeSet<String>) -> Option<&'a str> {
    let words = sql
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    let has = |word: &str| words.iter().any(|candidate| candidate == word);
    let has_pair = |first: &str, second: &str| {
        words
            .windows(2)
            .any(|pair| pair[0] == first && pair[1] == second)
    };
    let is_sql = has_pair("insert", "into")
        || has_pair("delete", "from")
        || has_pair("alter", "table")
        || has_pair("drop", "table")
        || has_pair("drop", "trigger")
        || has_pair("drop", "index")
        || has_pair("create", "table")
        || has_pair("create", "trigger")
        || has_pair("create", "index")
        || has_pair("replace", "into")
        || (has("select") && has("from"))
        || has("update")
        || sql.trim_start().to_ascii_lowercase().starts_with("pragma ");
    if !is_sql {
        return None;
    }
    tables
        .iter()
        .find(|table| words.iter().any(|word| word == table.as_str()))
        .map(String::as_str)
}

/// `under_database`: the import goes through coven-database's re-export of
/// rusqlite for the API, which names the database crate, not SQLite.
fn collect_forbidden_sqlite_imports(
    tree: &syn::UseTree,
    under_rusqlite: bool,
    under_database: bool,
    capabilities: &mut BTreeSet<&'static str>,
) {
    match tree {
        syn::UseTree::Path(path) => collect_forbidden_sqlite_imports(
            &path.tree,
            under_rusqlite || path.ident == "rusqlite",
            under_database || path.ident == "coven_database",
            capabilities,
        ),
        syn::UseTree::Name(name) => {
            record_import(&name.ident, under_rusqlite, under_database, capabilities);
        }
        syn::UseTree::Rename(rename) => {
            record_import(&rename.ident, under_rusqlite, under_database, capabilities);
        }
        syn::UseTree::Group(group) => {
            for item in &group.items {
                collect_forbidden_sqlite_imports(
                    item,
                    under_rusqlite,
                    under_database,
                    capabilities,
                );
            }
        }
        syn::UseTree::Glob(_) if under_rusqlite => {
            capabilities.insert("raw SQLite wildcard import");
        }
        syn::UseTree::Glob(_) => {}
    }
}

fn record_import(
    ident: &syn::Ident,
    under_rusqlite: bool,
    under_database: bool,
    capabilities: &mut BTreeSet<&'static str>,
) {
    if under_rusqlite {
        if let Some((_, kind)) = RAW_SQLITE_HANDLES
            .iter()
            .find(|(handle, _)| ident == handle)
        {
            capabilities.insert(*kind);
        }
    } else if ident == "rusqlite" && !under_database {
        capabilities.insert("raw SQLite crate import");
    }
}

fn forbidden_sqlite_path(path: &syn::Path) -> Option<&'static str> {
    forbidden_sqlite_segments(
        &path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>(),
    )
}

/// A raw handle named anywhere after `rusqlite` in a path.
fn forbidden_sqlite_segments(segments: &[String]) -> Option<&'static str> {
    let rusqlite = segments.iter().position(|segment| segment == "rusqlite")?;
    segments[rusqlite + 1..].iter().find_map(|segment| {
        RAW_SQLITE_HANDLES
            .iter()
            .find(|(handle, _)| segment == handle)
            .map(|(_, kind)| *kind)
    })
}

#[cfg(test)]
#[path = "database_boundary_tests.rs"]
mod tests;
