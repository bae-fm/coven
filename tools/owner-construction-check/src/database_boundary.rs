//! The SQLite capability (Appendix F.2): nothing outside the database's homes
//! gets the SQLite connection; it asks the database owner to run a write.
//!
//! Outside the homes of the policy's database capability, naming a raw
//! rusqlite handle — connection, session or transaction — is the violation, as
//! is writing SQL that names one of the tables the database's schema module
//! declares. rusqlite's values, results and parameter macros stay allowed.
//!
//! Each table the policy places in a domain belongs to that domain, which
//! keeps the table's schema and every write to it in its homes, in every file,
//! the database's homes included (`sql_statements.rs`):
//!
//! - a statement that writes the table, or a trigger whose body does, is
//!   the domain's own work;
//! - the table's `CREATE TABLE` and `CREATE INDEX`, and a trigger on it that
//!   writes no domain's table, are its schema, read where the text is written:
//!   in a `.sql` file, or in a string literal, a macro's definition included;
//! - test code that writes or creates the table is the domain's tests. A
//!   home file's tests, `<name>_tests.rs` beside it, are in its home.
//!
//! Reads are free across domains. SQL the program builds at runtime, from
//! `format!` or by joining pieces, names its table nowhere in the source, so
//! the rule cannot see it.
//!
//! The SQL the rule reads is every string literal of the crates and their
//! `.sql` files, which the crates bring in with `include_str!`. Test code is
//! read only for the domains (`test_code.rs`).

use crate::finding::Finding;
use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::crate_code::CrateCode;
use crate::macros::{parse_macro_body, token_paths, token_strings, unparsed_tokens};
use crate::module_sources::module_sources;
use crate::policy::{describe_homes, Domain, Policy};
use crate::sql_statements::{line_of, statements, Creation};
use crate::syntax::RustFile;

/// rusqlite's raw handles. Only the database crate holds one.
pub(crate) const RAW_SQLITE_HANDLES: &[(&str, &str)] = &[
    ("Connection", "raw SQLite connection"),
    ("Session", "raw SQLite session"),
    ("Transaction", "raw SQLite transaction"),
];

pub(crate) fn find_database_boundary_violations(code: &CrateCode, policy: &Policy) -> Vec<Finding> {
    let mut violations = BTreeSet::new();
    let homes = policy.database_homes();
    let confined_to = describe_homes(homes);
    let tables = collect_schema_table_names(&code.written, policy);
    let reexporting_crate = policy
        .database
        .as_ref()
        .and_then(|database| database.rusqlite_reexport.as_deref())
        .map(|package| package.replace('-', "_"));
    let domains = Domains::of(policy);
    for file in &code.expanded {
        let mut visitor = DatabaseBoundaryVisitor {
            path: &file.relative_path,
            in_database: homes.iter().any(|home| file.relative_path.starts_with(home.as_str())),
            confined_to: &confined_to,
            reexporting_crate: reexporting_crate.as_deref(),
            tables: &tables,
            domains: &domains,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    for file in &code.sql {
        if homes.iter().any(|home| file.relative_path.starts_with(home.as_str())) {
            continue;
        }
        for statement in statements(&file.text) {
            if let Some(table) =
                schema_table_in_sql(&file.text[statement.at..statement.end], &tables)
            {
                violations.insert(Finding::new(
                    &file.relative_path,
                    line_of(&file.text, statement.at),
                    format!("SQL on the database's own table {table} is confined to {confined_to}"),
                    confined_remedy(&confined_to),
                ));
            }
        }
    }
    // The domains read SQL files whole, tests' included.
    for file in code.sql.iter().chain(&code.test_sql) {
        let line = |at| line_of(&file.text, at);
        domains.check_writes(&file.relative_path, &file.text, line, &mut violations);
        domains.check_schema(&file.relative_path, &file.text, line, &mut violations);
    }
    // Schema is read where its text is written, macro definitions included.
    for file in &code.written {
        SqlLiterals {
            visit: |literal: &syn::LitStr| {
                let line = literal.span().start().line;
                domains.check_schema(
                    &file.relative_path,
                    &literal.value(),
                    |_| line,
                    &mut violations,
                )
            },
        }
        .visit_file(&file.syntax);
    }
    for file in &code.tests {
        SqlLiterals {
            visit: |literal: &syn::LitStr| {
                let (sql, line) = (literal.value(), literal.span().start().line);
                domains.check_writes(&file.relative_path, &sql, |_| line, &mut violations);
                domains.check_schema(&file.relative_path, &sql, |_| line, &mut violations);
            },
        }
        .visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

fn confined_remedy(confined_to: &str) -> String {
    format!(
        "ask the database owner to run the work; raw SQLite and SQL on the database's own tables live in {confined_to}"
    )
}

/// The policy's domains, by table.
struct Domains<'a> {
    /// Each domain's tables, lowercase, and the domain.
    tables: BTreeMap<String, &'a Domain>,
}

impl<'a> Domains<'a> {
    fn of(policy: &'a Policy) -> Self {
        Self {
            tables: policy
                .database
                .iter()
                .flat_map(|database| &database.domains)
                .flat_map(|domain| {
                    domain.tables.iter().map(move |table| (table.to_lowercase(), domain))
                })
                .collect(),
        }
    }

    /// Each write `sql` makes to a domain's table outside the domain's homes.
    fn check_writes(
        &self,
        path: &str,
        sql: &str,
        line: impl Fn(usize) -> usize,
        violations: &mut BTreeSet<Finding>,
    ) {
        for write in statements(sql).into_iter().flat_map(|statement| statement.writes) {
            let Some(domain) = self.tables.get(&write.table) else { continue };
            if domain.holds(path) {
                continue;
            }
            violations.insert(Finding::new(
                path,
                line(write.at),
                format!(
                    "SQL writes {}, which only the {} domain writes, in {}",
                    write.table,
                    domain.name,
                    describe_homes(&domain.homes)
                ),
                "a table is written only in its domain's homes; ask the domain to make the write, or move the SQL there",
            ));
        }
    }

    /// Each part of a domain's schema `sql` creates outside the domain's
    /// homes: a table, an index on one, or a trigger on one writing no
    /// domain's table (a trigger writing one is its write, `check_writes`).
    fn check_schema(
        &self,
        path: &str,
        sql: &str,
        line: impl Fn(usize) -> usize,
        violations: &mut BTreeSet<Finding>,
    ) {
        for statement in statements(sql) {
            let (what, table) = match statement.creation {
                Some(Creation::Table(table)) => ("creates table", table),
                Some(Creation::Index(table)) => ("indexes", table),
                Some(Creation::Trigger(table))
                    if statement
                        .writes
                        .iter()
                        .all(|write| !self.tables.contains_key(&write.table)) =>
                {
                    ("creates a trigger, writing no domain's table, on", table)
                }
                _ => continue,
            };
            let Some(domain) = self.tables.get(&table) else { continue };
            if domain.holds(path) {
                continue;
            }
            violations.insert(Finding::new(
                path,
                line(statement.at),
                format!(
                    "SQL {what} {table}, whose schema the {} domain keeps in {}",
                    domain.name,
                    describe_homes(&domain.homes)
                ),
                "a domain keeps its tables' schema with it: each table's CREATE TABLE and CREATE INDEX, and a trigger on it writing no domain's table, sit in the table's domain's homes",
            ));
        }
    }
}

/// Every string literal of a syntax tree, inside macro calls and macro
/// definitions included.
struct SqlLiterals<F> {
    visit: F,
}

impl<'ast, F: FnMut(&syn::LitStr)> Visit<'ast> for SqlLiterals<F> {
    fn visit_attribute(&mut self, node: &'ast syn::Attribute) {
        if node.path().is_ident("doc") {
            return;
        }
        visit::visit_attribute(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        match parse_macro_body(node) {
            Some(body) => body.visit(self),
            None => {
                for string in token_strings(unparsed_tokens(node)) {
                    (self.visit)(&string);
                }
            }
        }
        visit::visit_macro(self, node);
    }

    fn visit_lit_str(&mut self, node: &'ast syn::LitStr) {
        (self.visit)(node);
        visit::visit_lit_str(self, node);
    }
}

struct DatabaseBoundaryVisitor<'a> {
    path: &'a str,
    /// Whether the file is in one of the database's homes, where raw handles
    /// and SQL on the database's own tables belong.
    in_database: bool,
    /// The database's homes, as findings name them.
    confined_to: &'a str,
    /// The crate, as Rust names it, whose `rusqlite` re-export may be imported.
    reexporting_crate: Option<&'a str>,
    tables: &'a BTreeSet<String>,
    domains: &'a Domains<'a>,
    violations: &'a mut BTreeSet<Finding>,
}

impl DatabaseBoundaryVisitor<'_> {
    fn record(&mut self, kind: &str, span: Span) {
        if self.in_database {
            return;
        }
        self.violations.insert(Finding::new(
            self.path,
            span.start().line,
            format!("{kind} is confined to {}", self.confined_to),
            confined_remedy(self.confined_to),
        ));
    }

    fn check_sql(&mut self, literal: &syn::LitStr) {
        let sql = literal.value();
        if let Some(table) = schema_table_in_sql(&sql, self.tables) {
            self.record(&format!("SQL on the database's own table {table}"), literal.span());
        }
        let line = literal.span().start().line;
        self.domains.check_writes(self.path, &sql, |_| line, self.violations);
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
                for path in token_paths(unparsed_tokens(node)) {
                    if let Some(capability) = forbidden_sqlite_segments(&path.segments) {
                        self.record(capability, path.span);
                    }
                }
                for string in token_strings(unparsed_tokens(node)) {
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
