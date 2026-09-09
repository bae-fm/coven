use super::*;

// The database crate's own files: raw SQLite and SQL are its subject, so the
// boundary exempts them.
pub(super) const DATABASE_MODULE_ROOT: &str = "crates/coven-database/src/lib.rs";
pub(super) const DATABASE_MODULE_DIR: &str = "crates/coven-database/src/";
// Declares the tables Coven owns, which the boundary reads to tell a Coven
// table name from a host's.
pub(super) const COVEN_SCHEMA_FILE: &str = "crates/coven-database/src/coven_schema.rs";

pub(super) fn find_database_boundary_violations(
    files: &[RustFile],
) -> Vec<DatabaseBoundaryViolation> {
    let mut violations = BTreeSet::new();
    let coven_tables = collect_coven_table_names(files);
    for file in files {
        if file.relative_path == DATABASE_MODULE_ROOT
            || file.relative_path.starts_with(DATABASE_MODULE_DIR)
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
        visit::visit_macro(self, node);
    }

    fn visit_lit_str(&mut self, node: &'ast syn::LitStr) {
        if let Some(table) = coven_table_in_sql(&node.value(), self.coven_tables) {
            self.record(&format!("Coven-owned SQL for table {table}"), node.span());
        }
        visit::visit_lit_str(self, node);
    }
}

pub(super) fn collect_coven_table_names(files: &[RustFile]) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();
    for file in files {
        if file.relative_path != COVEN_SCHEMA_FILE {
            continue;
        }
        for item in &file.syntax.items {
            let syn::Item::Macro(item) = item else {
                continue;
            };
            if !matches!(
                item.ident.as_ref().map(ToString::to_string).as_deref(),
                Some("coven_tables" | "coven_routing_tables")
            ) {
                continue;
            }
            collect_table_macro_invocations(item.mac.tokens.clone(), &mut tables);
        }
    }
    tables
}

pub(super) fn collect_table_macro_invocations(
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

pub(super) fn coven_table_in_sql<'a>(sql: &str, tables: &'a BTreeSet<String>) -> Option<&'a str> {
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

pub(super) const FORBIDDEN_SQLITE_CAPABILITIES: &[(&str, &str)] = &[
    ("Connection", "raw SQLite connection"),
    ("Session", "raw SQLite session"),
    ("Transaction", "raw SQLite transaction"),
];

pub(super) fn collect_forbidden_sqlite_imports(
    tree: &syn::UseTree,
    under_rusqlite: bool,
    under_database: bool,
    capabilities: &mut BTreeSet<&'static str>,
) {
    match tree {
        syn::UseTree::Path(path) => collect_forbidden_sqlite_imports(
            &path.tree,
            under_rusqlite || path.ident == "rusqlite",
            under_database || path.ident == "database" || path.ident == "coven_database",
            capabilities,
        ),
        syn::UseTree::Name(name) => {
            if under_rusqlite {
                if let Some((_, kind)) = FORBIDDEN_SQLITE_CAPABILITIES
                    .iter()
                    .find(|(capability, _)| name.ident == *capability)
                {
                    capabilities.insert(*kind);
                }
            } else if name.ident == "rusqlite" && !under_database {
                capabilities.insert("raw SQLite crate import");
            }
        }
        syn::UseTree::Rename(rename) => {
            if under_rusqlite {
                if let Some((_, kind)) = FORBIDDEN_SQLITE_CAPABILITIES
                    .iter()
                    .find(|(capability, _)| rename.ident == *capability)
                {
                    capabilities.insert(*kind);
                }
            } else if rename.ident == "rusqlite" && !under_database {
                capabilities.insert("raw SQLite crate import");
            }
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

pub(super) fn forbidden_sqlite_path(path: &syn::Path) -> Option<&'static str> {
    let mut under_rusqlite = false;
    for segment in &path.segments {
        if segment.ident == "rusqlite" {
            under_rusqlite = true;
            continue;
        }
        if !under_rusqlite {
            continue;
        }
        if let Some((_, kind)) = FORBIDDEN_SQLITE_CAPABILITIES
            .iter()
            .find(|(name, _)| segment.ident == *name)
        {
            return Some(*kind);
        }
    }
    None
}
