use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

const CAPABILITY_TYPES: &[&str] = &[
    "ClockRef",
    "CloudHome",
    "CloudSyncStorage",
    "Database",
    "DeviceIdentityCustody",
    "EncryptionService",
    "Hlc",
    "MasterKeyCustody",
    "OAuthClients",
    "Runtime",
    "StoreDatabase",
    "StoreDir",
    "StoreKeys",
    "SyncStorage",
];

// These are configuration/value objects which happen to name a retained
// capability in their fields. They describe construction; they do not perform
// operations with that capability.
const NON_OWNER_TYPES: &[&str] = &["Config"];

const COMPOSITION_ROOTS: &[(&str, &str, &str)] = &[
    (
        "crates/coven/src/database/database_open.rs",
        "DatabaseCore",
        "open",
    ),
    (
        "crates/coven/src/database/database_runtime.rs",
        "Database",
        "open",
    ),
    (
        "crates/coven/src/database/database_runtime.rs",
        "Database",
        "open_initialized_store",
    ),
    (
        "crates/coven/src/database/database_runtime.rs",
        "Database",
        "open_with_hlc_and_coven_metadata",
    ),
    (
        "crates/coven/src/database/database_runtime.rs",
        "Database",
        "open_read_only",
    ),
    ("crates/coven/src/handle.rs", "CovenHandle", "new"),
    ("crates/coven/src/read_handle.rs", "CovenReadHandle", "new"),
    (
        "crates/coven/src/sync/test_owner_graph.rs",
        "TestOwnerGraph",
        "new",
    ),
    (
        "crates/coven/src/joining/transport_tests.rs",
        "TransportFixture",
        "build_with",
    ),
    (
        "crates/coven/src/sync/store/owner/writer/reclaim/tests.rs",
        "ReclaimJourneyFixture",
        "build",
    ),
    (
        "crates/coven/src/sync/test_helpers.rs",
        "TestDevice",
        "create",
    ),
    (
        "crates/coven/src/sync/test_helpers.rs",
        "TestDevice",
        "load",
    ),
    (
        "crates/coven/src/sync/test_helpers.rs",
        "TestDevice",
        "load_with_database",
    ),
    (
        "crates/coven/src/sync/test_helpers.rs",
        "TestStore",
        "create_with_protection",
    ),
    (
        "crates/coven/src/sync/test_helpers.rs",
        "TestStore",
        "create_with_protection_database",
    ),
    (
        "crates/coven/src/sync/test_helpers.rs",
        "TestStore",
        "with_store_and_keypair",
    ),
];

#[derive(Clone)]
struct RustFile {
    relative_path: String,
    syntax: syn::File,
}

#[derive(Default)]
struct TypeNames {
    names: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for TypeNames {
    fn visit_type_path(&mut self, node: &'ast syn::TypePath) {
        if let Some(segment) = node.path.segments.last() {
            self.names.insert(segment.ident.to_string());
        }
        visit::visit_type_path(self, node);
    }

    fn visit_type_trait_object(&mut self, node: &'ast syn::TypeTraitObject) {
        for bound in &node.bounds {
            if let syn::TypeParamBound::Trait(bound) = bound {
                if let Some(segment) = bound.path.segments.last() {
                    self.names.insert(segment.ident.to_string());
                }
            }
        }
        visit::visit_type_trait_object(self, node);
    }
}

#[derive(Clone)]
struct StructInfo {
    field_types: BTreeSet<String>,
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
struct Constructor {
    owner: String,
    method: String,
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
struct Violation {
    path: String,
    line: usize,
    parent: String,
    child: String,
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
struct DatabaseBoundaryViolation {
    path: String,
    line: usize,
    kind: String,
}

struct CheckResult {
    owner_construction: Vec<Violation>,
    database_boundary: Vec<DatabaseBoundaryViolation>,
}

fn main() {
    let mut arguments = std::env::args_os().skip(1).peekable();
    let database_boundary = arguments
        .next_if(|argument| argument == "--database-boundary")
        .is_some();
    let root = arguments
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    if arguments.next().is_some() {
        eprintln!("usage: owner-construction-check [--database-boundary] [root]");
        std::process::exit(2);
    }
    match check(&root, database_boundary) {
        Ok(result)
            if result.owner_construction.is_empty() && result.database_boundary.is_empty() => {}
        Ok(result) => {
            for violation in &result.owner_construction {
                eprintln!(
                    "{}:{}: owner constructor {} constructs retained owner {}",
                    violation.path, violation.line, violation.parent, violation.child
                );
            }
            for violation in &result.database_boundary {
                eprintln!(
                    "{}:{}: {} is forbidden outside the database module",
                    violation.path, violation.line, violation.kind
                );
            }
            if !result.owner_construction.is_empty() {
                eprintln!(
                    "retained owner constructors accept complete dependencies; construct owner graphs only in approved composition roots"
                );
            }
            if !result.database_boundary.is_empty() {
                eprintln!(
                    "database operations retain SQLite state and expose domain methods; move raw SQLite and SQL under crates/coven/src/database"
                );
            }
            std::process::exit(1);
        }
        Err(error) => {
            eprintln!("owner construction check failed: {error}");
            std::process::exit(2);
        }
    }
}

fn check(root: &Path, check_database_boundary: bool) -> Result<CheckResult, String> {
    let files = rust_files(root)?;
    let structs = collect_structs(&files);
    let owners = infer_owners(&structs);
    let constructors = collect_constructors(&files, &owners);
    let free_constructors = collect_free_constructors(&files, &owners);
    Ok(CheckResult {
        owner_construction: find_violations(&files, &owners, &constructors, &free_constructors),
        database_boundary: if check_database_boundary {
            find_database_boundary_violations(&files)
        } else {
            Vec::new()
        },
    })
}

fn find_database_boundary_violations(files: &[RustFile]) -> Vec<DatabaseBoundaryViolation> {
    let mut violations = BTreeSet::new();
    for file in files {
        if file.relative_path == "crates/coven/src/database.rs"
            || file.relative_path.starts_with("crates/coven/src/database/")
        {
            continue;
        }
        let mut visitor = DatabaseBoundaryVisitor {
            path: &file.relative_path,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

struct DatabaseBoundaryVisitor<'a> {
    path: &'a str,
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
    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        if use_tree_names_rusqlite(&node.tree) {
            self.record("raw SQLite path", node.span());
        }
        visit::visit_item_use(self, node);
    }

    fn visit_path(&mut self, node: &'ast syn::Path) {
        if node
            .segments
            .first()
            .is_some_and(|segment| segment.ident == "rusqlite")
        {
            self.record("raw SQLite path", node.span());
        }
        visit::visit_path(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if node
            .path
            .segments
            .first()
            .is_some_and(|segment| segment.ident == "rusqlite")
        {
            self.record("raw SQLite path", node.span());
        }
        visit::visit_macro(self, node);
    }

    fn visit_lit_str(&mut self, node: &'ast syn::LitStr) {
        if sql_statement(&node.value()) {
            self.record("SQL statement", node.span());
        }
        visit::visit_lit_str(self, node);
    }
}

fn use_tree_names_rusqlite(tree: &syn::UseTree) -> bool {
    match tree {
        syn::UseTree::Path(path) => path.ident == "rusqlite",
        syn::UseTree::Name(name) => name.ident == "rusqlite",
        syn::UseTree::Rename(rename) => rename.ident == "rusqlite",
        syn::UseTree::Group(group) => group.items.iter().any(use_tree_names_rusqlite),
        syn::UseTree::Glob(_) => false,
    }
}

fn sql_statement(value: &str) -> bool {
    let Some(keyword) = value.trim_start().split_ascii_whitespace().next() else {
        return false;
    };
    matches!(
        keyword.to_ascii_uppercase().as_str(),
        "ALTER"
            | "ATTACH"
            | "BEGIN"
            | "COMMIT"
            | "CREATE"
            | "DELETE"
            | "DETACH"
            | "DROP"
            | "INSERT"
            | "PRAGMA"
            | "REPLACE"
            | "ROLLBACK"
            | "SELECT"
            | "UPDATE"
            | "VACUUM"
            | "WITH"
    )
}

fn rust_files(root: &Path) -> Result<Vec<RustFile>, String> {
    let crates = root.join("crates");
    let mut paths = Vec::new();
    collect_rust_paths(&crates, &mut paths)?;
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let source = std::fs::read_to_string(&path)
                .map_err(|error| format!("read {}: {error}", path.display()))?;
            let syntax = syn::parse_file(&source)
                .map_err(|error| format!("parse {}: {error}", path.display()))?;
            let relative_path = path
                .strip_prefix(root)
                .map_err(|error| format!("relativize {}: {error}", path.display()))?
                .to_string_lossy()
                .replace('\\', "/");
            Ok(RustFile {
                relative_path,
                syntax,
            })
        })
        .collect()
}

fn collect_rust_paths(directory: &Path, output: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in std::fs::read_dir(directory)
        .map_err(|error| format!("read directory {}: {error}", directory.display()))?
    {
        let entry = entry.map_err(|error| format!("read directory entry: {error}"))?;
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            collect_rust_paths(&path, output)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
    Ok(())
}

fn collect_structs(files: &[RustFile]) -> BTreeMap<String, StructInfo> {
    let mut structs = BTreeMap::new();
    for file in files {
        for item in &file.syntax.items {
            collect_structs_from_item(item, &mut structs);
        }
    }
    structs
}

fn collect_structs_from_item(item: &syn::Item, structs: &mut BTreeMap<String, StructInfo>) {
    match item {
        syn::Item::Struct(item) => {
            let mut field_types = TypeNames::default();
            for field in &item.fields {
                field_types.visit_type(&field.ty);
            }
            structs.insert(
                item.ident.to_string(),
                StructInfo {
                    field_types: field_types.names,
                },
            );
        }
        syn::Item::Mod(item) => {
            if let Some((_, items)) = &item.content {
                for item in items {
                    collect_structs_from_item(item, structs);
                }
            }
        }
        _ => {}
    }
}

fn infer_owners(structs: &BTreeMap<String, StructInfo>) -> BTreeSet<String> {
    let capabilities = CAPABILITY_TYPES
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();
    let mut owners = BTreeSet::new();
    loop {
        let before = owners.len();
        for (name, info) in structs {
            if NON_OWNER_TYPES.contains(&name.as_str()) {
                continue;
            }
            if info
                .field_types
                .iter()
                .any(|field| capabilities.contains(field) || owners.contains(field))
            {
                owners.insert(name.clone());
            }
        }
        if owners.len() == before {
            return owners;
        }
    }
}

fn collect_constructors(files: &[RustFile], owners: &BTreeSet<String>) -> BTreeSet<Constructor> {
    let mut constructors = BTreeSet::new();
    for file in files {
        let mut collector = ConstructorCollector {
            owners,
            constructors: &mut constructors,
        };
        collector.visit_file(&file.syntax);
    }
    constructors
}

fn collect_free_constructors(
    files: &[RustFile],
    owners: &BTreeSet<String>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut constructors = BTreeMap::new();
    for file in files {
        let mut collector = FreeConstructorCollector {
            owners,
            constructors: &mut constructors,
        };
        collector.visit_file(&file.syntax);
    }
    constructors
}

struct FreeConstructorCollector<'a> {
    owners: &'a BTreeSet<String>,
    constructors: &'a mut BTreeMap<String, BTreeSet<String>>,
}

impl Visit<'_> for FreeConstructorCollector<'_> {
    fn visit_item_fn(&mut self, node: &syn::ItemFn) {
        let syn::ReturnType::Type(_, output) = &node.sig.output else {
            return;
        };
        let mut names = TypeNames::default();
        names.visit_type(output);
        let returned_owners = names
            .names
            .intersection(self.owners)
            .cloned()
            .collect::<BTreeSet<_>>();
        if !returned_owners.is_empty() {
            self.constructors
                .entry(node.sig.ident.to_string())
                .or_default()
                .extend(returned_owners);
        }
    }
}

struct ConstructorCollector<'a> {
    owners: &'a BTreeSet<String>,
    constructors: &'a mut BTreeSet<Constructor>,
}

impl<'ast> Visit<'ast> for ConstructorCollector<'_> {
    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        let Some(owner) = type_name(&node.self_ty) else {
            return;
        };
        if !self.owners.contains(&owner) {
            return;
        }
        for item in &node.items {
            let syn::ImplItem::Fn(method) = item else {
                continue;
            };
            if output_contains_owner(&method.sig.output, &owner) {
                self.constructors.insert(Constructor {
                    owner: owner.clone(),
                    method: method.sig.ident.to_string(),
                });
            }
        }
    }
}

fn output_contains_owner(output: &syn::ReturnType, owner: &str) -> bool {
    let syn::ReturnType::Type(_, output) = output else {
        return false;
    };
    let mut names = TypeNames::default();
    names.visit_type(output);
    names.names.contains("Self") || names.names.contains(owner)
}

fn find_violations(
    files: &[RustFile],
    owners: &BTreeSet<String>,
    constructors: &BTreeSet<Constructor>,
    free_constructors: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<Violation> {
    let mut violations = BTreeSet::new();
    for file in files {
        let mut visitor = ConstructionVisitor {
            path: &file.relative_path,
            owners,
            constructors,
            free_constructors,
            current_constructor: None,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

struct ConstructionVisitor<'a> {
    path: &'a str,
    owners: &'a BTreeSet<String>,
    constructors: &'a BTreeSet<Constructor>,
    free_constructors: &'a BTreeMap<String, BTreeSet<String>>,
    current_constructor: Option<Constructor>,
    violations: &'a mut BTreeSet<Violation>,
}

impl ConstructionVisitor<'_> {
    fn record(&mut self, child: &str, span: Span) {
        let Some(parent) = &self.current_constructor else {
            return;
        };
        if parent.owner == child
            || child == format!("{}Inner", parent.owner)
            || COMPOSITION_ROOTS.iter().any(|(path, owner, method)| {
                *path == self.path && *owner == parent.owner && *method == parent.method
            })
        {
            return;
        }
        self.violations.insert(Violation {
            path: self.path.to_string(),
            line: span.start().line,
            parent: format!("{}::{}", parent.owner, parent.method),
            child: child.to_string(),
        });
    }
}

impl<'ast> Visit<'ast> for ConstructionVisitor<'_> {
    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        let previous = self.current_constructor.clone();
        let owner = type_name(&node.self_ty);
        for item in &node.items {
            let syn::ImplItem::Fn(method) = item else {
                continue;
            };
            self.current_constructor = owner.as_ref().and_then(|owner| {
                let constructor = Constructor {
                    owner: owner.clone(),
                    method: method.sig.ident.to_string(),
                };
                self.constructors
                    .contains(&constructor)
                    .then_some(constructor)
            });
            self.visit_block(&method.block);
        }
        self.current_constructor = previous;
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let syn::Expr::Path(function) = node.func.as_ref() {
            let segments = function.path.segments.iter().collect::<Vec<_>>();
            if segments.len() >= 2 {
                let owner = segments[segments.len() - 2].ident.to_string();
                let method = segments[segments.len() - 1].ident.to_string();
                if self.constructors.contains(&Constructor {
                    owner: owner.clone(),
                    method,
                }) {
                    self.record(&owner, node.span());
                }
            }
            if let Some(method) = segments.last() {
                if let Some(owners) = self.free_constructors.get(&method.ident.to_string()) {
                    for owner in owners {
                        self.record(owner, node.span());
                    }
                }
            }
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if let Some(segment) = node.path.segments.last() {
            let owner = segment.ident.to_string();
            if self.owners.contains(&owner) {
                self.record(&owner, node.span());
            }
        }
        visit::visit_expr_struct(self, node);
    }
}

fn type_name(ty: &syn::Type) -> Option<String> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_owner_constructor_is_rejected() {
        let source = syn::parse_file(
            r#"
            struct StoreDatabase;
            struct Child { database: StoreDatabase }
            impl Child { fn new(database: StoreDatabase) -> Self { Self { database } } }
            struct Parent { child: Child }
            impl Parent { fn new(database: StoreDatabase) -> Self { Self { child: Child::new(database) } } }
            "#,
        )
        .expect("parse fixture");
        let files = vec![RustFile {
            relative_path: "fixture.rs".to_string(),
            syntax: source,
        }];
        let structs = collect_structs(&files);
        let owners = infer_owners(&structs);
        let constructors = collect_constructors(&files, &owners);
        let free_constructors = collect_free_constructors(&files, &owners);
        let violations = find_violations(&files, &owners, &constructors, &free_constructors);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].parent, "Parent::new");
        assert_eq!(violations[0].child, "Child");
    }

    #[test]
    fn injected_owner_is_accepted() {
        let source = syn::parse_file(
            r#"
            struct StoreDatabase;
            struct Child { database: StoreDatabase }
            impl Child { fn new(database: StoreDatabase) -> Self { Self { database } } }
            struct Parent { child: Child }
            impl Parent { fn new(child: Child) -> Self { Self { child } } }
            "#,
        )
        .expect("parse fixture");
        let files = vec![RustFile {
            relative_path: "fixture.rs".to_string(),
            syntax: source,
        }];
        let structs = collect_structs(&files);
        let owners = infer_owners(&structs);
        let constructors = collect_constructors(&files, &owners);
        let free_constructors = collect_free_constructors(&files, &owners);
        assert!(find_violations(&files, &owners, &constructors, &free_constructors).is_empty());
    }

    #[test]
    fn private_inner_representation_is_accepted() {
        let source = syn::parse_file(
            r#"
            struct StoreDatabase;
            struct ParentInner { database: StoreDatabase }
            struct Parent { inner: ParentInner }
            impl Parent { fn new(database: StoreDatabase) -> Self { Self { inner: ParentInner { database } } } }
            "#,
        )
        .expect("parse fixture");
        let files = vec![RustFile {
            relative_path: "fixture.rs".to_string(),
            syntax: source,
        }];
        let structs = collect_structs(&files);
        let owners = infer_owners(&structs);
        let constructors = collect_constructors(&files, &owners);
        let free_constructors = collect_free_constructors(&files, &owners);
        assert!(find_violations(&files, &owners, &constructors, &free_constructors).is_empty());
    }

    #[test]
    fn owner_constructor_cannot_hide_child_construction_behind_a_free_function() {
        let source = syn::parse_file(
            r#"
            struct StoreDatabase;
            struct Child { database: StoreDatabase }
            fn build_child(database: StoreDatabase) -> Child { Child { database } }
            struct Parent { child: Child }
            impl Parent { fn new(database: StoreDatabase) -> Self { Self { child: build_child(database) } } }
            "#,
        )
        .expect("parse fixture");
        let files = vec![RustFile {
            relative_path: "fixture.rs".to_string(),
            syntax: source,
        }];
        let structs = collect_structs(&files);
        let owners = infer_owners(&structs);
        let constructors = collect_constructors(&files, &owners);
        let free_constructors = collect_free_constructors(&files, &owners);
        let violations = find_violations(&files, &owners, &constructors, &free_constructors);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].child, "Child");
    }

    #[test]
    fn sqlite_types_and_sql_are_rejected_outside_the_database_module() {
        let source = syn::parse_file(
            r#"
            use rusqlite::Connection;
            fn leak(conn: &Connection) { conn.execute("DELETE FROM notes", []).unwrap(); }
            "#,
        )
        .expect("parse fixture");
        let files = vec![RustFile {
            relative_path: "crates/coven/src/sync/leak.rs".to_string(),
            syntax: source,
        }];
        let violations = find_database_boundary_violations(&files);
        assert_eq!(
            violations
                .iter()
                .map(|violation| violation.kind.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["raw SQLite path", "SQL statement"]),
        );
    }
}
