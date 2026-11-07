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
//!   the domain's own work, and so is a write to a view whose `INSTEAD OF`
//!   triggers write it, through views in turn (`schema_views.rs`);
//! - the table's `CREATE TABLE` and `CREATE INDEX`, and a trigger on it that
//!   writes no domain's table, are its schema;
//! - a domain keeps its views as it keeps its tables: those its `views` name,
//!   and those whose `INSTEAD OF` triggers write its tables alone. A view
//!   whose triggers write several domains' tables is named by the domain that
//!   keeps it;
//! - both are read where the text is written: in a `.sql` file, or in a
//!   string literal, a macro's definition included, so the code invoking a
//!   macro writes nothing itself;
//! - test code that writes or creates the table is the domain's tests. A
//!   home file's tests, `<name>_tests.rs` beside it, are in its home;
//! - in the homes' production code, a write runs in a method of the domain's
//!   owner, the plain form that opens its own write, or in a function or
//!   closure taking an open write, one of the policy's `write_types`: the
//!   form an operation spanning domains calls inside the write it opened. A
//!   module-level constant or macro is text, read where its user runs.
//!
//! Reads are free across domains, but SQL follows the policy's
//! `crate_order`: a domain is in the crate its homes are in, and the
//! production SQL of a crate fires a trigger on, and reads, only the tables
//! and views of its own crate's domains and of the crates before it. SQL the
//! program builds at runtime, from `format!` or by joining pieces, names its
//! table nowhere in the source, so the rule cannot see it.
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
use crate::schema_views::SchemaViews;
use crate::sql_statements::{line_of, statements, Creation, Statement};
use crate::syntax::{type_name, RustFile};

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
    let domains = Domains::of(policy, schema_views(code), code);
    for file in &code.expanded {
        let mut visitor = DatabaseBoundaryVisitor {
            path: &file.relative_path,
            in_database: homes.iter().any(|home| file.relative_path.starts_with(home.as_str())),
            confined_to: &confined_to,
            reexporting_crate: reexporting_crate.as_deref(),
            tables: &tables,
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
    // Production SQL follows the crate order.
    for file in &code.sql {
        let line = |at| line_of(&file.text, at);
        domains.check_crate_order(
            &file.relative_path,
            &file.package,
            &file.text,
            line,
            &mut violations,
        );
    }
    for file in &code.written {
        SqlLiterals::new(|literal: &syn::LitStr, scopes: &[Scope]| {
            let (sql, line) = (literal.value(), literal.span().start().line);
            domains.check_crate_order(
                &file.relative_path,
                &file.package,
                &sql,
                |_| line,
                &mut violations,
            );
            domains.check_write_paths(&file.relative_path, &sql, scopes, line, &mut violations);
        })
        .visit_file(&file.syntax);
    }
    // The domains read SQL files whole, tests' included.
    for file in code.sql.iter().chain(&code.test_sql) {
        let line = |at| line_of(&file.text, at);
        domains.check_writes(&file.relative_path, &file.text, line, &mut violations);
        domains.check_schema(&file.relative_path, &file.text, line, &mut violations);
    }
    // Writes and schema are read where their text is written, macro
    // definitions included: code invoking a macro writes nothing itself.
    for file in &code.written {
        SqlLiterals::new(|literal: &syn::LitStr, _: &[Scope]| {
            let (sql, line) = (literal.value(), literal.span().start().line);
            domains.check_writes(&file.relative_path, &sql, |_| line, &mut violations);
            domains.check_schema(&file.relative_path, &sql, |_| line, &mut violations);
        })
        .visit_file(&file.syntax);
    }
    for file in &code.tests {
        SqlLiterals::new(|literal: &syn::LitStr, _: &[Scope]| {
            let (sql, line) = (literal.value(), literal.span().start().line);
            domains.check_writes(&file.relative_path, &sql, |_| line, &mut violations);
            domains.check_schema(&file.relative_path, &sql, |_| line, &mut violations);
        })
        .visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

fn confined_remedy(confined_to: &str) -> String {
    format!(
        "ask the database owner to run the work; raw SQLite and SQL on the database's own tables live in {confined_to}"
    )
}

/// The views the crates' production SQL creates: in its `.sql` files and in
/// its string literals, as the domains read them.
pub(crate) fn schema_views(code: &CrateCode) -> SchemaViews {
    let mut texts = code.sql.iter().map(|file| file.text.clone()).collect::<Vec<_>>();
    for file in &code.written {
        SqlLiterals::new(|literal: &syn::LitStr, _: &[Scope]| texts.push(literal.value()))
            .visit_file(&file.syntax);
    }
    SchemaViews::collect(texts.iter().map(String::as_str))
}

/// The policy's domains, by table and by view.
struct Domains<'a> {
    /// Each domain's tables, lowercase, and the domain.
    tables: BTreeMap<String, &'a Domain>,
    /// Each view a domain keeps, lowercase, and the domain.
    views: BTreeMap<String, &'a Domain>,
    /// Each view no domain names whose `INSTEAD OF` triggers write several
    /// domains' tables, with those domains.
    shared_views: BTreeMap<String, BTreeSet<&'a str>>,
    schema_views: SchemaViews,
    /// Each domain's crate, by the domain's name: the latest in
    /// `crate_order` of the crates its homes hold sources in, with its
    /// place there.
    crates: BTreeMap<&'a str, (usize, &'a str)>,
    /// Each crate's place in `crate_order`.
    order: BTreeMap<&'a str, usize>,
    /// The types an open write is.
    write_types: BTreeSet<String>,
}

impl<'a> Domains<'a> {
    fn of(policy: &'a Policy, schema_views: SchemaViews, code: &CrateCode) -> Self {
        let domains = || policy.database.iter().flat_map(|database| &database.domains);
        let order: BTreeMap<&str, usize> = policy
            .crate_order
            .iter()
            .enumerate()
            .map(|(place, name)| (name.as_str(), place))
            .collect();
        let sources = code
            .written
            .iter()
            .map(|file| (&file.relative_path, &file.package))
            .chain(code.sql.iter().map(|file| (&file.relative_path, &file.package)));
        let mut crates: BTreeMap<&str, (usize, &str)> = BTreeMap::new();
        for (path, package) in sources {
            let Some((&name, &place)) = order.get_key_value(package.as_str()) else { continue };
            for domain in domains().filter(|domain| domain.holds(path)) {
                let entry = crates.entry(domain.name.as_str()).or_insert((place, name));
                if place > entry.0 {
                    *entry = (place, name);
                }
            }
        }
        let tables: BTreeMap<String, &Domain> = domains()
            .flat_map(|domain| {
                domain.tables.iter().map(move |table| (table.to_lowercase(), domain))
            })
            .collect();
        let mut views: BTreeMap<String, &Domain> = domains()
            .flat_map(|domain| domain.views.iter().map(move |view| (view.to_lowercase(), domain)))
            .collect();
        let mut shared_views = BTreeMap::new();
        for view in schema_views.names() {
            if views.contains_key(view) {
                continue;
            }
            let writers = schema_views
                .tables_behind(view)
                .iter()
                .filter_map(|table| tables.get(table).copied())
                .map(|domain| (domain.name.as_str(), domain))
                .collect::<BTreeMap<_, _>>();
            match writers.len() {
                0 => {}
                1 => {
                    views.extend(writers.into_values().map(|domain| (view.clone(), domain)));
                }
                _ => {
                    shared_views.insert(view.clone(), writers.into_keys().collect());
                }
            }
        }
        let write_types = policy
            .database
            .iter()
            .flat_map(|database| database.write_types.iter().cloned())
            .collect();
        Self { tables, views, shared_views, schema_views, crates, order, write_types }
    }

    /// Each write `sql`, a literal at `line` of a domain's home, makes to the
    /// domain's tables outside its owner's methods and outside every function
    /// and closure taking an open write.
    fn check_write_paths(
        &self,
        path: &str,
        sql: &str,
        scopes: &[Scope],
        line: usize,
        violations: &mut BTreeSet<Finding>,
    ) {
        let Some(innermost) = scopes.last() else { return };
        if self.write_types.is_empty()
            || scopes.iter().any(|scope| !scope.parameter_types.is_disjoint(&self.write_types))
        {
            return;
        }
        for statement in statements(sql) {
            let mut reported = BTreeSet::new();
            for write in &statement.writes {
                for table in self.schema_views.tables(&write.table, write.operation) {
                    let Some(domain) = self.tables.get(&table) else { continue };
                    if !domain.holds(path)
                        || scopes.iter().any(|scope| scope.owner.as_deref() == Some(&domain.owner))
                        || !reported.insert(table.clone())
                    {
                        continue;
                    }
                    violations.insert(Finding::new(
                        path,
                        line,
                        format!(
                            "{} writes {table}, which the {} domain writes in its owner {}'s methods or with an open write ({})",
                            innermost.name,
                            domain.name,
                            domain.owner,
                            self.write_types.iter().cloned().collect::<Vec<_>>().join(", ")
                        ),
                        "a domain's writes go through its owner's methods, which open the write, or through functions taking the write their caller opened",
                    ));
                }
            }
        }
    }

    /// The domain keeping the table or view `name`, lowercase.
    fn keeper(&self, name: &str) -> Option<&'a Domain> {
        self.tables.get(name).or_else(|| self.views.get(name)).copied()
    }

    /// Each trigger `sql`, production SQL of `package`, fires on, and each
    /// table or view it reads, that a domain of a later crate in
    /// `crate_order` keeps.
    fn check_crate_order(
        &self,
        path: &str,
        package: &str,
        sql: &str,
        line: impl Fn(usize) -> usize,
        violations: &mut BTreeSet<Finding>,
    ) {
        let Some(&place) = self.order.get(package) else { return };
        let later = |name: &str| {
            let domain = self.keeper(name)?;
            let &(kept, keeping) = self.crates.get(domain.name.as_str())?;
            (kept > place).then(|| {
                format!(
                    "which the {} domain keeps in {keeping}, after {package} in crate_order",
                    domain.name
                )
            })
        };
        let remedy = "a crate's SQL fires triggers on, and reads, only the tables and views of its own crate's domains and of the crates before it in crate_order";
        for statement in statements(sql) {
            if let Some(Creation::Trigger { table, .. }) = &statement.creation {
                if let Some(keeper) = later(table) {
                    violations.insert(Finding::new(
                        path,
                        line(statement.at),
                        format!("a trigger in {package} fires on {table}, {keeper}"),
                        remedy,
                    ));
                }
            }
            for read in &statement.reads {
                if let Some(keeper) = later(&read.table) {
                    violations.insert(Finding::new(
                        path,
                        line(read.at),
                        format!("SQL in {package} reads {}, {keeper}", read.table),
                        remedy,
                    ));
                }
            }
        }
    }

    /// Whether any table `statement` writes, through views or not, is in a
    /// domain.
    fn writes_a_domain(&self, statement: &Statement) -> bool {
        statement.writes.iter().any(|write| {
            self.schema_views
                .tables(&write.table, write.operation)
                .iter()
                .any(|table| self.tables.contains_key(table))
        })
    }

    /// Each write `sql` makes to a domain's table outside the domain's homes,
    /// directly or through a view.
    fn check_writes(
        &self,
        path: &str,
        sql: &str,
        line: impl Fn(usize) -> usize,
        violations: &mut BTreeSet<Finding>,
    ) {
        for statement in statements(sql) {
            let mut reported = BTreeSet::new();
            for write in &statement.writes {
                for table in self.schema_views.tables(&write.table, write.operation) {
                    let Some(domain) = self.tables.get(&table) else { continue };
                    if domain.holds(path) || !reported.insert(table.clone()) {
                        continue;
                    }
                    let through = if table == write.table {
                        String::new()
                    } else {
                        format!(" through the view {}", write.table)
                    };
                    violations.insert(Finding::new(
                        path,
                        line(write.at),
                        format!(
                            "SQL writes {table}{through}, which only the {} domain writes, in {}",
                            domain.name,
                            describe_homes(&domain.homes)
                        ),
                        "a table is written only in its domain's homes; ask the domain to make the write, or move the SQL there",
                    ));
                }
            }
        }
    }

    /// Each part of a domain's schema `sql` creates outside the domain's
    /// homes: a table, a view, an index on a table, or a trigger on either
    /// writing no domain's table (a trigger writing one is its write,
    /// `check_writes`); and each view no domain names whose triggers write
    /// several domains' tables.
    fn check_schema(
        &self,
        path: &str,
        sql: &str,
        line: impl Fn(usize) -> usize,
        violations: &mut BTreeSet<Finding>,
    ) {
        for statement in statements(sql) {
            let writes_a_domain = self.writes_a_domain(&statement);
            let (what, name) = match statement.creation {
                Some(Creation::Table(table)) => ("creates table", table),
                Some(Creation::View(view)) => ("creates view", view),
                Some(Creation::Index(table)) => ("indexes", table),
                Some(Creation::Trigger { table, .. }) if !writes_a_domain => {
                    ("creates a trigger, writing no domain's table, on", table)
                }
                _ => continue,
            };
            if let Some(writers) = self.shared_views.get(&name) {
                violations.insert(Finding::new(
                    path,
                    line(statement.at),
                    format!(
                        "SQL creates view {name}, whose INSTEAD OF triggers write the tables of domains {}",
                        writers.iter().copied().collect::<Vec<_>>().join(", ")
                    ),
                    "a view is kept by one domain: name it in the views of the domain that keeps it",
                ));
                continue;
            }
            let Some(domain) = self.keeper(&name) else {
                continue;
            };
            if domain.holds(path) {
                continue;
            }
            violations.insert(Finding::new(
                path,
                line(statement.at),
                format!(
                    "SQL {what} {name}, whose schema the {} domain keeps in {}",
                    domain.name,
                    describe_homes(&domain.homes)
                ),
                "a domain keeps its tables' and views' schema with it: each table's CREATE TABLE and CREATE INDEX, each view's CREATE VIEW, and a trigger on either writing no domain's table, sit in the domain's homes",
            ));
        }
    }
}

/// Every string literal of a syntax tree, inside macro calls and macro
/// definitions included. Inside a loop over a literal list of names,
/// `for table in ["a", "b"]`, a literal naming the loop's variable,
/// `"DELETE FROM {table}"`, is read once for each name.
struct SqlLiterals<F> {
    visit: F,
    /// The enclosing loops' variables, each with the names it takes.
    loops: Vec<(String, Vec<String>)>,
    /// The functions, methods and closures enclosing the literal, outermost
    /// first.
    scopes: Vec<Scope>,
    /// The type of the `impl` block being read.
    implemented: Option<String>,
}

/// A function, method or closure enclosing a literal.
pub(crate) struct Scope {
    /// How a finding names it: `name`, `Type::name` or a closure.
    pub(crate) name: String,
    /// For a method, its `impl` block's type.
    pub(crate) owner: Option<String>,
    /// Each name in its parameters' types: `&DatabaseConnection` names
    /// `DatabaseConnection`. A closure's untyped parameters name nothing.
    pub(crate) parameter_types: BTreeSet<String>,
}

/// Each name in `ty`'s paths, generic arguments included.
fn type_path_names(ty: &syn::Type) -> BTreeSet<String> {
    struct Names(BTreeSet<String>);
    impl<'ast> Visit<'ast> for Names {
        fn visit_path_segment(&mut self, node: &'ast syn::PathSegment) {
            self.0.insert(node.ident.to_string());
            visit::visit_path_segment(self, node);
        }
    }
    let mut names = Names(BTreeSet::new());
    names.visit_type(ty);
    names.0
}

fn parameter_types<'a>(inputs: impl IntoIterator<Item = &'a syn::FnArg>) -> BTreeSet<String> {
    inputs
        .into_iter()
        .filter_map(|input| match input {
            syn::FnArg::Typed(typed) => Some(type_path_names(&typed.ty)),
            syn::FnArg::Receiver(_) => None,
        })
        .flatten()
        .collect()
}

impl<F: FnMut(&syn::LitStr, &[Scope])> SqlLiterals<F> {
    fn new(visit: F) -> Self {
        Self { visit, loops: Vec::new(), scopes: Vec::new(), implemented: None }
    }

    fn within(&mut self, scope: Scope, visit: impl FnOnce(&mut Self)) {
        self.scopes.push(scope);
        visit(self);
        self.scopes.pop();
    }

    fn read(&mut self, literal: &syn::LitStr) {
        let mut texts = vec![literal.value()];
        for (variable, names) in &self.loops {
            let placeholder = format!("{{{variable}}}");
            if texts.iter().any(|text| text.contains(&placeholder)) {
                texts = texts
                    .iter()
                    .flat_map(|text| names.iter().map(|name| text.replace(&placeholder, name)))
                    .collect();
            }
        }
        for text in texts {
            (self.visit)(&syn::LitStr::new(&text, literal.span()), &self.scopes);
        }
    }
}

/// A loop's variable and the literal names it takes, when it loops over a
/// literal list of strings.
fn literal_loop(node: &syn::ExprForLoop) -> Option<(String, Vec<String>)> {
    let syn::Pat::Ident(variable) = node.pat.as_ref() else { return None };
    let syn::Expr::Array(array) = node.expr.as_ref() else { return None };
    let names = array
        .elems
        .iter()
        .map(|element| match element {
            syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(name), .. }) => Some(name.value()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    Some((variable.ident.to_string(), names))
}

impl<'ast, F: FnMut(&syn::LitStr, &[Scope])> Visit<'ast> for SqlLiterals<F> {
    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        let outer = self.implemented.replace(type_name(&node.self_ty).unwrap_or_default());
        visit::visit_item_impl(self, node);
        self.implemented = outer;
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        let owner = self.implemented.clone();
        let scope = Scope {
            name: format!("{}::{}", owner.as_deref().unwrap_or("<impl>"), node.sig.ident),
            owner,
            parameter_types: parameter_types(&node.sig.inputs),
        };
        self.within(scope, |visitor| visit::visit_impl_item_fn(visitor, node));
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        let scope = Scope {
            name: node.sig.ident.to_string(),
            owner: None,
            parameter_types: parameter_types(&node.sig.inputs),
        };
        self.within(scope, |visitor| visit::visit_item_fn(visitor, node));
    }

    fn visit_trait_item_fn(&mut self, node: &'ast syn::TraitItemFn) {
        let scope = Scope {
            name: node.sig.ident.to_string(),
            owner: None,
            parameter_types: parameter_types(&node.sig.inputs),
        };
        self.within(scope, |visitor| visit::visit_trait_item_fn(visitor, node));
    }

    fn visit_expr_closure(&mut self, node: &'ast syn::ExprClosure) {
        let enclosing = self.scopes.last().map_or("<item>".to_string(), |scope| scope.name.clone());
        let scope = Scope {
            name: format!("a closure in {enclosing}"),
            owner: None,
            parameter_types: node
                .inputs
                .iter()
                .filter_map(|input| match input {
                    syn::Pat::Type(typed) => Some(type_path_names(&typed.ty)),
                    _ => None,
                })
                .flatten()
                .collect(),
        };
        self.within(scope, |visitor| visit::visit_expr_closure(visitor, node));
    }

    fn visit_expr_for_loop(&mut self, node: &'ast syn::ExprForLoop) {
        match literal_loop(node) {
            Some(binding) => {
                self.visit_expr(&node.expr);
                self.loops.push(binding);
                self.visit_block(&node.body);
                self.loops.pop();
            }
            None => visit::visit_expr_for_loop(self, node),
        }
    }

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
                    self.read(&string);
                }
            }
        }
        visit::visit_macro(self, node);
    }

    fn visit_lit_str(&mut self, node: &'ast syn::LitStr) {
        self.read(node);
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
    let Some(schema_modules) = modules.get(schema_root) else {
        return Vec::new();
    };
    files
        .iter()
        .filter(|file| {
            modules.get(&file.relative_path).is_some_and(|paths| {
                paths.iter().any(|path| {
                    schema_modules.iter().any(|schema_module| path.starts_with(schema_module))
                })
            })
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
