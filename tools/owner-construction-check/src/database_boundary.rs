//! The SQLite capability (Appendix F.2): nothing outside the database's homes
//! gets the SQLite connection; it asks the database owner to run a write.
//!
//! Outside the homes of the policy's database capability, naming a raw
//! rusqlite handle — connection, session or transaction — is the violation, as
//! is writing SQL that names one of the tables the database's schema module
//! declares. rusqlite's values, results and parameter macros stay allowed. It
//! reads no test code (`test_code.rs`).

use crate::finding::Finding;
use std::collections::BTreeSet;

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::{parse_macro_body, token_paths, token_strings};
use crate::module_sources::module_sources;
use crate::policy::{describe_homes, Policy};
use crate::syntax::RustFile;

/// rusqlite's raw handles. Only the database crate holds one.
pub(crate) const RAW_SQLITE_HANDLES: &[(&str, &str)] = &[
    ("Connection", "raw SQLite connection"),
    ("Session", "raw SQLite session"),
    ("Transaction", "raw SQLite transaction"),
];

pub(crate) fn find_database_boundary_violations(
    files: &[RustFile],
    policy: &Policy,
) -> Vec<Finding> {
    let mut violations = BTreeSet::new();
    let homes = policy.database_homes();
    let confined_to = describe_homes(homes);
    let tables = collect_schema_table_names(files, policy);
    let reexporting_crate = policy
        .database
        .as_ref()
        .and_then(|database| database.rusqlite_reexport.as_deref())
        .map(|package| package.replace('-', "_"));
    for file in files {
        if homes.iter().any(|home| file.relative_path.starts_with(home.as_str())) {
            continue;
        }
        let mut visitor = DatabaseBoundaryVisitor {
            path: &file.relative_path,
            confined_to: &confined_to,
            reexporting_crate: reexporting_crate.as_deref(),
            tables: &tables,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

struct DatabaseBoundaryVisitor<'a> {
    path: &'a str,
    /// The database's homes, as findings name them.
    confined_to: &'a str,
    /// The crate, as Rust names it, whose `rusqlite` re-export may be imported.
    reexporting_crate: Option<&'a str>,
    tables: &'a BTreeSet<String>,
    violations: &'a mut BTreeSet<Finding>,
}

impl DatabaseBoundaryVisitor<'_> {
    fn record(&mut self, kind: &str, span: Span) {
        self.violations.insert(Finding::new(
            self.path,
            span.start().line,
            format!("{kind} is confined to {}", self.confined_to),
            format!(
                "ask the database owner to run the work; raw SQLite and SQL on the database's own tables live in {}",
                self.confined_to
            ),
        ));
    }

    fn check_sql(&mut self, literal: &syn::LitStr) {
        if let Some(table) = schema_table_in_sql(&literal.value(), self.tables) {
            self.record(&format!("SQL on the database's own table {table}"), literal.span());
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
        collect_forbidden_sqlite_imports(
            &node.tree,
            self.reexporting_crate,
            false,
            false,
            &mut capabilities,
        );
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

/// The tables the policy's schema module declares through its table macros.
fn collect_schema_table_names(files: &[RustFile], policy: &Policy) -> BTreeSet<String> {
    let Some((schema_root, table_macros)) = policy.database.as_ref().and_then(|database| {
        database.schema.as_deref().map(|schema| (schema, &database.table_macros))
    }) else {
        return BTreeSet::new();
    };
    schema_table_macros(files, schema_root)
        .into_iter()
        .filter(|table_macro| table_macros.contains(&table_macro.name))
        .flat_map(|table_macro| table_macro.tables)
        .collect()
}

/// A top-level `macro_rules!` in the schema module and the tables its
/// `$visit!(table, …)` calls declare (none for a macro that is not a table list).
pub(crate) struct SchemaMacro {
    pub(crate) name: String,
    pub(crate) tables: BTreeSet<String>,
}

/// Every top-level `macro_rules!` in the schema module: the production file
/// `schema_root` and every module beneath it, wherever `#[path]` puts its file.
pub(crate) fn schema_table_macros(files: &[RustFile], schema_root: &str) -> Vec<SchemaMacro> {
    let modules = module_sources(files);
    let Some(schema_module) = modules.get(schema_root) else {
        return Vec::new();
    };
    files
        .iter()
        .filter(|file| {
            modules.get(&file.relative_path).is_some_and(|module| module.starts_with(schema_module))
        })
        .flat_map(|file| {
            file.syntax.items.iter().filter_map(|item| {
                let syn::Item::Macro(item) = item else {
                    return None;
                };
                let name = item.ident.as_ref()?.to_string();
                let mut tables = BTreeSet::new();
                collect_table_macro_invocations(item.mac.tokens.clone(), &mut tables);
                Some(SchemaMacro { name, tables })
            })
        })
        .collect()
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

fn schema_table_in_sql<'a>(sql: &str, tables: &'a BTreeSet<String>) -> Option<&'a str> {
    let words = sql
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    let has = |word: &str| words.iter().any(|candidate| candidate == word);
    let has_pair = |first: &str, second: &str| {
        words.windows(2).any(|pair| pair[0] == first && pair[1] == second)
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
    tables.iter().find(|table| words.iter().any(|word| word == table.as_str())).map(String::as_str)
}

/// `under_database`: the import goes through the policy's re-exporting crate,
/// whose `rusqlite` re-export names that crate, not SQLite.
fn collect_forbidden_sqlite_imports(
    tree: &syn::UseTree,
    reexporting_crate: Option<&str>,
    under_rusqlite: bool,
    under_database: bool,
    capabilities: &mut BTreeSet<&'static str>,
) {
    match tree {
        syn::UseTree::Path(path) => collect_forbidden_sqlite_imports(
            &path.tree,
            reexporting_crate,
            under_rusqlite || path.ident == "rusqlite",
            under_database || reexporting_crate.is_some_and(|name| path.ident == name),
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
                    reexporting_crate,
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
        if let Some((_, kind)) = RAW_SQLITE_HANDLES.iter().find(|(handle, _)| ident == handle) {
            capabilities.insert(*kind);
        }
    } else if ident == "rusqlite" && !under_database {
        capabilities.insert("raw SQLite crate import");
    }
}

fn forbidden_sqlite_path(path: &syn::Path) -> Option<&'static str> {
    forbidden_sqlite_segments(
        &path.segments.iter().map(|segment| segment.ident.to_string()).collect::<Vec<_>>(),
    )
}

/// A raw handle named anywhere after `rusqlite` in a path.
fn forbidden_sqlite_segments(segments: &[String]) -> Option<&'static str> {
    let rusqlite = segments.iter().position(|segment| segment == "rusqlite")?;
    segments[rusqlite + 1..].iter().find_map(|segment| {
        RAW_SQLITE_HANDLES.iter().find(|(handle, _)| segment == handle).map(|(_, kind)| *kind)
    })
}

#[cfg(test)]
#[path = "database_boundary_tests.rs"]
mod tests;
