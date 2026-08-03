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
const NON_OWNER_TYPES: &[&str] = &[
    "Config",
    "ConnectionThread",
    "DatabaseState",
    "StoreDatabaseConnection",
];

const RETAINED_SERVICE_ROOT_TYPES: &[&str] =
    &["CovenHandle", "CovenReadHandle", "Database", "DatabaseCore"];

// These owners are deliberately created for one operation. They may be fields
// of a retained service, but the type itself does not name that retained role.
const OPERATION_SCOPED_OWNER_TYPES: &[&str] = &[
    "RemoteBlobSource",
    "RemoteStoreBlobAccess",
    "Store",
    "StoreBlobCache",
];

const LIFETIME_CONSTRUCTION_AUTHORITIES: &[(&str, &str)] = &[
    ("ConnectedBlobTransitions", "StoreSync"),
    ("SyncComponents", "StoreSync"),
    ("SyncLoopHandle", "StoreSync"),
];

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
        "crates/coven/src/joining/facade_tests.rs",
        "FacadeFixture",
        "build",
    ),
    (
        "crates/coven/src/sync/store/owner/writer/operation/acknowledgements/tests.rs",
        "LosingAckFixture",
        "create",
    ),
    (
        "crates/coven/src/sync/store/owner/writer/operation/reclaim/tests.rs",
        "ReclaimJourneyFixture",
        "build",
    ),
    (
        "crates/coven/src/sync/store/owner/writer/operation/operations/tests/merge_fixture.rs",
        "PreparedWriteFixture",
        "prepare",
    ),
    (
        "crates/coven/src/sync/cycle_tests.rs",
        "SamePrincipalApprovalFixture",
        "prepare",
    ),
    (
        "crates/coven/src/sync/store/membership/tests.rs",
        "MergeFixture",
        "new",
    ),
    (
        "crates/coven/src/sync/store_history_checkpoint_tests.rs",
        "PublishedHistory",
        "publish",
    ),
    (
        "crates/coven/src/sync/store/owner/circles/snapshots/tests.rs",
        "CircleSnapshotFixture",
        "initialize",
    ),
    (
        "crates/coven/src/blob/delete_tests.rs",
        "TombstoneCollector",
        "load",
    ),
    (
        "crates/coven/src/blob/upload_tests.rs",
        "UploadFixture",
        "with_home",
    ),
    (
        "crates/coven/src/sync/pull_tests.rs",
        "ExactMembershipChain",
        "load",
    ),
    (
        "crates/coven/src/sync/pull_tests.rs",
        "PersistedCycleRemoval",
        "build",
    ),
    (
        "crates/coven/src/sync/store/owner/circles/tests/recovery.rs",
        "RevokedOperation",
        "prepare",
    ),
    (
        "crates/coven/src/sync/store/owner/circles/tests/publication.rs",
        "ClosingFounderCircle",
        "build",
    ),
    (
        "crates/coven/src/sync/store/owner/circles/tests/publication.rs",
        "SilentParticipantCircle",
        "build",
    ),
    (
        "crates/coven/src/sync/store/owner/circles/tests/resolution.rs",
        "ConflictFixture",
        "build",
    ),
    (
        "crates/coven/src/sync/store/owner/circles/tests/rotation_required.rs",
        "RotationFixture",
        "build",
    ),
    (
        "crates/coven/src/sync/store/owner/circles/tests/rotation_required.rs",
        "ActiveMemberCircleSnapshot",
        "build",
    ),
    (
        "crates/coven/src/sync/store/owner/pull/tests.rs",
        "EffectiveAccessFixture",
        "create",
    ),
    (
        "crates/coven/src/sync/store/owner/writer/operation/snapshot/image.rs",
        "PublishedScopedSnapshot",
        "publish",
    ),
    (
        "crates/coven/src/sync/store_history_checkpoint_tests.rs",
        "MemberRemovalHistory",
        "create",
    ),
    ("crates/coven/src/coven.rs", "RemoteOnlyStoreBlob", "create"),
    (
        "crates/coven/src/sync/test_helpers.rs",
        "TestDevice",
        "create",
    ),
    (
        "crates/coven/src/sync/test_helpers.rs",
        "TestDevice",
        "create_with_database",
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
        "TestDevice",
        "open_with_database",
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

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
struct ServiceReturnViolation {
    path: String,
    line: usize,
    owner: String,
    method: String,
    returned: String,
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
struct RetainedServiceConstructionViolation {
    path: String,
    line: usize,
    owner: String,
    method: String,
    service: String,
    authority: Option<String>,
}

struct CheckResult {
    owner_construction: Vec<Violation>,
    database_boundary: Vec<DatabaseBoundaryViolation>,
    service_returns: Vec<ServiceReturnViolation>,
    retained_service_construction: Vec<RetainedServiceConstructionViolation>,
}

fn main() {
    let mut database_boundary = false;
    let mut retained_service_returns = false;
    let mut retained_service_construction = false;
    let mut root = None;
    for argument in std::env::args_os().skip(1) {
        if argument == "--database-boundary" {
            database_boundary = true;
        } else if argument == "--retained-service-returns" {
            retained_service_returns = true;
        } else if argument == "--retained-service-construction" {
            retained_service_construction = true;
        } else if root.is_none() {
            root = Some(PathBuf::from(argument));
        } else {
            eprintln!(
                "usage: owner-construction-check [--database-boundary] [--retained-service-returns] [--retained-service-construction] [root]"
            );
            std::process::exit(2);
        }
    }
    let root = root.unwrap_or_else(|| PathBuf::from("."));
    match check(
        &root,
        database_boundary,
        retained_service_returns,
        retained_service_construction,
    ) {
        Ok(result)
            if result.owner_construction.is_empty()
                && result.database_boundary.is_empty()
                && result.service_returns.is_empty()
                && result.retained_service_construction.is_empty() => {}
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
            for violation in &result.service_returns {
                eprintln!(
                    "{}:{}: {}::{} returns retained service {}",
                    violation.path,
                    violation.line,
                    violation.owner,
                    violation.method,
                    violation.returned
                );
            }
            for violation in &result.retained_service_construction {
                match &violation.authority {
                    Some(authority) => eprintln!(
                        "{}:{}: {}::{} constructs runtime-replaceable retained service {}; its lifetime authority is {}",
                        violation.path,
                        violation.line,
                        violation.owner,
                        violation.method,
                        violation.service,
                        authority
                    ),
                    None => eprintln!(
                        "{}:{}: {}::{} constructs retained service {} outside a composition root",
                        violation.path,
                        violation.line,
                        violation.owner,
                        violation.method,
                        violation.service
                    ),
                }
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
            if !result.service_returns.is_empty() {
                eprintln!(
                    "composition-root services use their retained children; owners do not return those children to callers"
                );
            }
            if !result.retained_service_construction.is_empty() {
                eprintln!(
                    "retained services are constructed at composition roots; declared runtime-replaceable services are constructed only by their root-retained lifetime owner"
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

fn check(
    root: &Path,
    check_database_boundary: bool,
    check_retained_service_returns: bool,
    check_retained_service_construction: bool,
) -> Result<CheckResult, String> {
    let files = rust_files(root)?;
    let structs = collect_structs(&files);
    let owners = infer_owners(&structs);
    let constructors = collect_constructors(&files, &owners);
    let free_constructors = collect_free_constructors(&files, &owners);
    let declared_types = collect_declared_types(&files);
    let service_owners = infer_owners(&declared_types);
    let retained_services = collect_root_retained_types(
        &declared_types,
        &service_owners,
        RETAINED_SERVICE_ROOT_TYPES,
    );
    Ok(CheckResult {
        owner_construction: find_violations(&files, &owners, &constructors, &free_constructors),
        database_boundary: if check_database_boundary {
            find_database_boundary_violations(&files)
        } else {
            Vec::new()
        },
        service_returns: if check_retained_service_returns {
            find_service_return_violations(&files, &retained_services)
        } else {
            Vec::new()
        },
        retained_service_construction: if check_retained_service_construction {
            find_retained_service_construction_violations(
                &files,
                &retained_services,
                LIFETIME_CONSTRUCTION_AUTHORITIES,
                COMPOSITION_ROOTS,
            )
        } else {
            Vec::new()
        },
    })
}

fn find_database_boundary_violations(files: &[RustFile]) -> Vec<DatabaseBoundaryViolation> {
    let mut violations = BTreeSet::new();
    let coven_tables = collect_coven_table_names(files);
    for file in files {
        if file.relative_path == "crates/coven/src/database.rs"
            || file.relative_path.starts_with("crates/coven/src/database/")
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

fn collect_coven_table_names(files: &[RustFile]) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();
    for file in files {
        if file.relative_path != "crates/coven/src/database/coven_schema.rs" {
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

const FORBIDDEN_SQLITE_CAPABILITIES: &[(&str, &str)] = &[
    ("Connection", "raw SQLite connection"),
    ("Session", "raw SQLite session"),
    ("Transaction", "raw SQLite transaction"),
];

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
            under_database || path.ident == "database",
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

fn forbidden_sqlite_path(path: &syn::Path) -> Option<&'static str> {
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

fn collect_declared_types(files: &[RustFile]) -> BTreeMap<String, StructInfo> {
    let mut types = BTreeMap::new();
    for file in files {
        for item in &file.syntax.items {
            collect_declared_types_from_item(item, &mut types);
        }
    }
    types
}

fn collect_declared_types_from_item(item: &syn::Item, types: &mut BTreeMap<String, StructInfo>) {
    let (name, field_types) = match item {
        syn::Item::Struct(item) => {
            let mut names = TypeNames::default();
            for field in &item.fields {
                names.visit_type(&field.ty);
            }
            (Some(item.ident.to_string()), names.names)
        }
        syn::Item::Enum(item) => {
            let mut names = TypeNames::default();
            for variant in &item.variants {
                for field in &variant.fields {
                    names.visit_type(&field.ty);
                }
            }
            (Some(item.ident.to_string()), names.names)
        }
        syn::Item::Type(item) => {
            let mut names = TypeNames::default();
            names.visit_type(&item.ty);
            (Some(item.ident.to_string()), names.names)
        }
        syn::Item::Trait(item) => (Some(item.ident.to_string()), BTreeSet::new()),
        syn::Item::Union(item) => {
            let mut names = TypeNames::default();
            for field in &item.fields.named {
                names.visit_type(&field.ty);
            }
            (Some(item.ident.to_string()), names.names)
        }
        syn::Item::Mod(item) => {
            if let Some((_, items)) = &item.content {
                for item in items {
                    collect_declared_types_from_item(item, types);
                }
            }
            return;
        }
        _ => return,
    };
    let Some(name) = name else {
        return;
    };
    types
        .entry(name)
        .or_insert_with(|| StructInfo {
            field_types: BTreeSet::new(),
        })
        .field_types
        .extend(field_types);
}

fn collect_root_retained_types(
    types: &BTreeMap<String, StructInfo>,
    owners: &BTreeSet<String>,
    roots: &[&str],
) -> BTreeSet<String> {
    let mut retained = roots
        .iter()
        .filter(|root| types.contains_key(**root))
        .map(|root| (*root).to_string())
        .collect::<BTreeSet<_>>();
    let mut pending = retained.iter().cloned().collect::<Vec<_>>();
    while let Some(owner) = pending.pop() {
        let Some(info) = types.get(&owner) else {
            continue;
        };
        for child in &info.field_types {
            let is_retained_capability =
                owners.contains(child) || CAPABILITY_TYPES.contains(&child.as_str());
            if types.contains_key(child) && is_retained_capability && retained.insert(child.clone())
            {
                pending.push(child.clone());
            }
        }
    }
    retained
}

fn find_service_return_violations(
    files: &[RustFile],
    retained_services: &BTreeSet<String>,
) -> Vec<ServiceReturnViolation> {
    let mut violations = BTreeSet::new();
    for file in files {
        find_service_returns_in_items(
            &file.relative_path,
            &file.syntax.items,
            retained_services,
            &mut violations,
        );
    }
    violations.into_iter().collect()
}

fn find_service_returns_in_items(
    path: &str,
    items: &[syn::Item],
    retained_services: &BTreeSet<String>,
    violations: &mut BTreeSet<ServiceReturnViolation>,
) {
    for item in items {
        match item {
            syn::Item::Impl(item) => {
                let owner = type_name(&item.self_ty).unwrap_or_else(|| "<impl>".to_string());
                for impl_item in &item.items {
                    let syn::ImplItem::Fn(method) = impl_item else {
                        continue;
                    };
                    record_service_returns(
                        path,
                        &owner,
                        &method.sig.ident.to_string(),
                        &method.sig.output,
                        method.sig.ident.span(),
                        retained_services,
                        violations,
                    );
                }
            }
            syn::Item::Fn(item) => record_service_returns(
                path,
                "<free>",
                &item.sig.ident.to_string(),
                &item.sig.output,
                item.sig.ident.span(),
                retained_services,
                violations,
            ),
            syn::Item::Mod(item) => {
                if let Some((_, items)) = &item.content {
                    find_service_returns_in_items(path, items, retained_services, violations);
                }
            }
            _ => {}
        }
    }
}

fn record_service_returns(
    path: &str,
    owner: &str,
    method: &str,
    output: &syn::ReturnType,
    span: Span,
    retained_services: &BTreeSet<String>,
    violations: &mut BTreeSet<ServiceReturnViolation>,
) {
    let syn::ReturnType::Type(_, output) = output else {
        return;
    };
    let mut names = TypeNames::default();
    names.visit_type(output);
    if names.names.contains("Self") {
        names.names.insert(owner.to_string());
    }
    for returned in names.names.intersection(retained_services) {
        if returned == owner
            || COMPOSITION_ROOTS
                .iter()
                .any(|(root_path, root_owner, root_method)| {
                    path == *root_path && owner == *root_owner && method == *root_method
                })
        {
            continue;
        }
        violations.insert(ServiceReturnViolation {
            path: path.to_string(),
            line: span.start().line,
            owner: owner.to_string(),
            method: method.to_string(),
            returned: returned.clone(),
        });
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

fn collect_associated_factories(
    files: &[RustFile],
    owners: &BTreeSet<String>,
) -> BTreeMap<(String, String), BTreeSet<String>> {
    let mut factories = BTreeMap::new();
    for file in files {
        let mut collector = AssociatedFactoryCollector {
            owners,
            factories: &mut factories,
        };
        collector.visit_file(&file.syntax);
    }
    factories
}

struct AssociatedFactoryCollector<'a> {
    owners: &'a BTreeSet<String>,
    factories: &'a mut BTreeMap<(String, String), BTreeSet<String>>,
}

impl Visit<'_> for AssociatedFactoryCollector<'_> {
    fn visit_item_impl(&mut self, node: &syn::ItemImpl) {
        let Some(factory) = type_name(&node.self_ty) else {
            return;
        };
        for item in &node.items {
            let syn::ImplItem::Fn(method) = item else {
                continue;
            };
            let syn::ReturnType::Type(_, output) = &method.sig.output else {
                continue;
            };
            let mut names = TypeNames::default();
            names.visit_type(output);
            let mut returned_owners = names
                .names
                .intersection(self.owners)
                .cloned()
                .collect::<BTreeSet<_>>();
            if names.names.contains("Self") && self.owners.contains(&factory) {
                returned_owners.insert(factory.clone());
            }
            if !returned_owners.is_empty() {
                self.factories
                    .entry((factory.clone(), method.sig.ident.to_string()))
                    .or_default()
                    .extend(returned_owners);
            }
        }
    }
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
    let associated_factories = collect_associated_factories(files, owners);
    for file in files {
        let mut visitor = ConstructionVisitor {
            path: &file.relative_path,
            owners,
            constructors,
            free_constructors,
            associated_factories: &associated_factories,
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
    associated_factories: &'a BTreeMap<(String, String), BTreeSet<String>>,
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
                if let Some(returned_owners) = self.associated_factories.get(&(owner, method)) {
                    for returned_owner in returned_owners {
                        self.record(returned_owner, node.span());
                    }
                }
            }
            if could_be_free_function_path(&segments) {
                let method = segments
                    .last()
                    .expect("free function path has at least one segment");
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

fn could_be_free_function_path(segments: &[&syn::PathSegment]) -> bool {
    segments.len() == 1
        || segments[..segments.len() - 1].iter().all(|segment| {
            let name = segment.ident.to_string();
            matches!(name.as_str(), "crate" | "self" | "super")
                || name
                    .chars()
                    .next()
                    .is_some_and(|character| character.is_lowercase())
        })
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

fn find_retained_service_construction_violations(
    files: &[RustFile],
    retained_services: &BTreeSet<String>,
    authorities: &[(&str, &str)],
    composition_roots: &[(&str, &str, &str)],
) -> Vec<RetainedServiceConstructionViolation> {
    let authorities = authorities
        .iter()
        .filter(|(service, authority)| {
            retained_services.contains(*service) && retained_services.contains(*authority)
        })
        .map(|(service, authority)| ((*service).to_string(), (*authority).to_string()))
        .collect::<BTreeMap<_, _>>();
    let retained_services = retained_services
        .iter()
        .filter(|service| {
            !RETAINED_SERVICE_ROOT_TYPES.contains(&service.as_str())
                && !OPERATION_SCOPED_OWNER_TYPES.contains(&service.as_str())
                && !service.ends_with("Inner")
                && (!CAPABILITY_TYPES.contains(&service.as_str())
                    || authorities.contains_key(*service))
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let associated_factories = collect_associated_factories(files, &retained_services);
    let free_constructors = collect_free_constructors(files, &retained_services);
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        let mut visitor = ServiceConstructionSiteVisitor {
            path: &file.relative_path,
            retained_services: &retained_services,
            authorities: &authorities,
            composition_roots,
            associated_factories: &associated_factories,
            free_constructors: &free_constructors,
            current_callable: None,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

fn is_test_source(path: &str) -> bool {
    path.contains("/tests/")
        || path.ends_with("/tests.rs")
        || path.ends_with("_tests.rs")
        || path
            .rsplit('/')
            .next()
            .is_some_and(|name| name.starts_with("test_"))
        || path.ends_with("/test_helpers.rs")
        || path.ends_with("/test_support.rs")
}

fn is_test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attribute| {
        attribute.path().is_ident("test")
            || (attribute.path().is_ident("cfg")
                && matches!(&attribute.meta, syn::Meta::List(list) if list.tokens.to_string().contains("test")))
    })
}

struct ServiceConstructionSiteVisitor<'a> {
    path: &'a str,
    retained_services: &'a BTreeSet<String>,
    authorities: &'a BTreeMap<String, String>,
    composition_roots: &'a [(&'a str, &'a str, &'a str)],
    associated_factories: &'a BTreeMap<(String, String), BTreeSet<String>>,
    free_constructors: &'a BTreeMap<String, BTreeSet<String>>,
    current_callable: Option<Constructor>,
    violations: &'a mut BTreeSet<RetainedServiceConstructionViolation>,
}

impl ServiceConstructionSiteVisitor<'_> {
    fn record(&mut self, service: &str, span: Span) {
        if !self.retained_services.contains(service) {
            return;
        }
        let Some(caller) = &self.current_callable else {
            return;
        };
        if caller.owner == service {
            return;
        }
        let defines_factory = if caller.owner == "<free>" {
            self.free_constructors
                .get(&caller.method)
                .is_some_and(|services| services.contains(service))
        } else {
            self.associated_factories
                .get(&(caller.owner.clone(), caller.method.clone()))
                .is_some_and(|services| services.contains(service))
        };
        if defines_factory {
            return;
        }
        if self.composition_roots.iter().any(|(path, owner, method)| {
            *path == self.path && *owner == caller.owner && *method == caller.method
        }) {
            return;
        }
        let authority = self.authorities.get(service).cloned();
        if authority.as_ref() == Some(&caller.owner) {
            return;
        }
        self.violations
            .insert(RetainedServiceConstructionViolation {
                path: self.path.to_string(),
                line: span.start().line,
                owner: caller.owner.clone(),
                method: caller.method.clone(),
                service: service.to_string(),
                authority,
            });
    }

    fn record_associated_factory(&mut self, owner: &str, method: &str, span: Span) {
        let Some(services) = self
            .associated_factories
            .get(&(owner.to_string(), method.to_string()))
        else {
            return;
        };
        for service in services {
            self.record(service, span);
        }
    }
}

impl<'ast> Visit<'ast> for ServiceConstructionSiteVisitor<'_> {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if is_test_only(&node.attrs) {
            return;
        }
        visit::visit_item_mod(self, node);
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if is_test_only(&node.attrs) {
            return;
        }
        let previous = self.current_callable.clone();
        let owner = type_name(&node.self_ty).unwrap_or_else(|| "<impl>".to_string());
        for item in &node.items {
            let syn::ImplItem::Fn(method) = item else {
                continue;
            };
            if is_test_only(&method.attrs) {
                continue;
            }
            self.current_callable = Some(Constructor {
                owner: owner.clone(),
                method: method.sig.ident.to_string(),
            });
            self.visit_block(&method.block);
        }
        self.current_callable = previous;
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        let previous = self.current_callable.replace(Constructor {
            owner: "<free>".to_string(),
            method: node.sig.ident.to_string(),
        });
        self.visit_block(&node.block);
        self.current_callable = previous;
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let syn::Expr::Path(function) = node.func.as_ref() {
            let segments = function.path.segments.iter().collect::<Vec<_>>();
            if segments.len() >= 2 {
                self.record_associated_factory(
                    &segments[segments.len() - 2].ident.to_string(),
                    &segments[segments.len() - 1].ident.to_string(),
                    node.span(),
                );
            }
            if could_be_free_function_path(&segments) {
                let method = segments
                    .last()
                    .expect("free function path has at least one segment");
                if let Some(services) = self.free_constructors.get(&method.ident.to_string()) {
                    for service in services {
                        self.record(service, node.span());
                    }
                }
            }
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if matches!(node.receiver.as_ref(), syn::Expr::Path(path) if path.path.is_ident("self")) {
            if let Some(caller) = &self.current_callable {
                self.record_associated_factory(
                    &caller.owner.clone(),
                    &node.method.to_string(),
                    node.span(),
                );
            }
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if node.path.segments.len() == 1 {
            let service = node
                .path
                .segments
                .last()
                .expect("single-segment struct path has a segment");
            self.record(&service.ident.to_string(), node.span());
        }
        visit::visit_expr_struct(self, node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_services_are_constructed_only_by_roots_or_lifetime_authorities() {
        let production = syn::parse_file(
            r#"
            struct Database;
            struct Child { database: Database }
            impl Child { fn new(database: Database) -> Self { Self { database } } }

            struct Root { child: Child }
            struct Prepared;
            impl Prepared {
                fn initialize(self, database: Database) -> Child { Child::new(database) }
            }
            impl Root {
                fn new(database: Database) -> Self {
                    Self { child: Prepared::initialize(Prepared, database) }
                }
            }

            struct Wrong { database: Database }
            impl Wrong { fn build(&self, database: Database) { Child::new(database); } }

            struct Session { database: Database }
            impl Session { fn new(database: Database) -> Self { Self { database } } }
            struct SessionOwner { session: Session }
            impl SessionOwner { fn replace(&self, database: Database) { Session::new(database); } }
            "#,
        )
        .expect("parse production fixture");
        let files = vec![RustFile {
            relative_path: "crates/coven/src/lib.rs".to_string(),
            syntax: production,
        }];
        let declared = collect_declared_types(&files);
        let owners = infer_owners(&declared);
        let retained = collect_root_retained_types(&declared, &owners, &["Root", "SessionOwner"]);
        let violations = find_retained_service_construction_violations(
            &files,
            &retained,
            &[("Session", "SessionOwner")],
            &[("crates/coven/src/lib.rs", "Root", "new")],
        );

        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].service, "Child");
        assert_eq!(violations[0].owner, "Wrong");
        assert_eq!(violations[0].method, "build");
    }

    #[test]
    fn lifetime_authority_must_be_retained_by_a_root() {
        let source = syn::parse_file(
            r#"
            struct Database;
            struct Child { database: Database }
            impl Child { fn new(database: Database) -> Self { Self { database } } }

            struct Root { child: Child }
            struct DetachedAuthority { database: Database }
            impl DetachedAuthority {
                fn reconnect(&self, database: Database) { Child::new(database); }
            }
            "#,
        )
        .expect("parse fixture");
        let files = vec![RustFile {
            relative_path: "fixture.rs".to_string(),
            syntax: source,
        }];
        let declared = collect_declared_types(&files);
        let owners = infer_owners(&declared);
        let retained = collect_root_retained_types(&declared, &owners, &["Root"]);
        assert!(LIFETIME_CONSTRUCTION_AUTHORITIES.contains(&("SyncLoopHandle", "StoreSync")));
        let violations = find_retained_service_construction_violations(
            &files,
            &retained,
            &[("Child", "DetachedAuthority")],
            &[],
        );

        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].service, "Child");
        assert_eq!(violations[0].owner, "DetachedAuthority");
        assert_eq!(violations[0].authority, None);
    }

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
    fn owner_cannot_return_a_service_retained_by_a_composition_root() {
        let source = syn::parse_file(
            r#"
            struct Database;
            struct StoreBlobAccess { database: Database }
            struct StoreSync { database: Database }
            struct CovenHandle { sync: StoreSync, blobs: StoreBlobAccess }

            impl StoreSync {
                fn blob_access(&self) -> Result<Option<Arc<StoreBlobAccess>>, Error> {
                    todo!()
                }
            }
            "#,
        )
        .expect("parse fixture");
        let files = vec![RustFile {
            relative_path: "fixture.rs".to_string(),
            syntax: source,
        }];
        let declared_types = collect_declared_types(&files);
        let owners = infer_owners(&declared_types);
        let retained_services =
            collect_root_retained_types(&declared_types, &owners, &["CovenHandle"]);
        let violations = find_service_return_violations(&files, &retained_services);

        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].owner, "StoreSync");
        assert_eq!(violations[0].returned, "StoreBlobAccess");
    }

    #[test]
    fn operation_scoped_values_and_a_services_own_constructor_are_returnable() {
        let source = syn::parse_file(
            r#"
            struct Database;
            struct StoreBlobAccess { database: Database }
            struct StoreSync { database: Database }
            struct CovenHandle { sync: StoreSync, blobs: StoreBlobAccess }
            struct AuthorizedWrite;

            impl StoreBlobAccess {
                fn new(database: Database) -> Self { Self { database } }
            }

            impl StoreSync {
                fn authorize(&self) -> AuthorizedWrite { AuthorizedWrite }
            }
            "#,
        )
        .expect("parse fixture");
        let files = vec![RustFile {
            relative_path: "fixture.rs".to_string(),
            syntax: source,
        }];
        let declared_types = collect_declared_types(&files);
        let owners = infer_owners(&declared_types);
        let retained_services =
            collect_root_retained_types(&declared_types, &owners, &["CovenHandle"]);

        assert!(find_service_return_violations(&files, &retained_services).is_empty());
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
    fn module_qualified_free_factory_is_rejected() {
        let source = syn::parse_file(
            r#"
            struct Database;
            struct Child { database: Database }
            mod factory {
                use super::*;
                pub(super) fn build_child(database: Database) -> Child { Child { database } }
            }
            struct Parent { child: Child }
            impl Parent {
                fn new(database: Database) -> Self {
                    Self { child: crate::factory::build_child(database) }
                }
            }
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
    fn owner_constructor_cannot_hide_child_construction_behind_an_associated_factory() {
        let source = syn::parse_file(
            r#"
            struct Database;
            struct Child { database: Database }
            struct Parent { child: Child }
            struct ChildFactory;

            impl ChildFactory {
                fn build(database: Database) -> Child { Child { database } }
            }

            impl Parent {
                fn new(database: Database) -> Self {
                    Self { child: ChildFactory::build(database) }
                }
            }
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
    fn qualified_call_does_not_match_an_unrelated_free_constructor() {
        let source = syn::parse_file(
            r#"
            struct StoreDatabase;
            struct Database { database: StoreDatabase }
            fn open(database: StoreDatabase) -> Database { Database { database } }

            struct ParsedValue;
            impl ParsedValue { fn open() -> Self { Self } }

            struct Parent { database: StoreDatabase, value: ParsedValue }
            impl Parent {
                fn new(database: StoreDatabase) -> Self {
                    Self { database, value: ParsedValue::open() }
                }
            }
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
    fn sqlite_connection_is_rejected_outside_the_database_module() {
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
            BTreeSet::from(["raw SQLite connection"]),
        );
    }

    #[test]
    fn every_raw_sqlite_ownership_path_is_rejected() {
        let source = syn::parse_file(
            r#"
            use rusqlite::{Connection as SqlConnection, Transaction};
            use rusqlite::session::Session;
            use rusqlite::*;
            use rusqlite as sqlite;
            fn qualified() -> rusqlite::Result<()> {
                let _ = rusqlite::Connection::open_in_memory()?;
                Ok(())
            }
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
            BTreeSet::from([
                "raw SQLite connection",
                "raw SQLite crate import",
                "raw SQLite session",
                "raw SQLite transaction",
                "raw SQLite wildcard import",
            ]),
        );
    }

    #[test]
    fn database_implementation_owns_raw_sqlite() {
        let source = syn::parse_file(
            r#"
            use rusqlite::{Connection, Transaction};
            fn transact(connection: &Connection, transaction: &Transaction<'_>) {}
            "#,
        )
        .expect("parse fixture");
        let files = vec![RustFile {
            relative_path: "crates/coven/src/database/transaction.rs".to_string(),
            syntax: source,
        }];

        assert!(find_database_boundary_violations(&files).is_empty());
    }

    #[test]
    fn host_sql_and_query_values_are_allowed_outside_the_database_module() {
        let source = syn::parse_file(
            r#"
            fn read(context: SqlReadContext<'_>) -> rusqlite::Result<String> {
                context.query_row(
                    "SELECT body FROM notes WHERE id = ?1",
                    rusqlite::params!["note-id"],
                    |row: &rusqlite::Row<'_>| row.get(0),
                )
            }
            "#,
        )
        .expect("parse fixture");
        let files = vec![RustFile {
            relative_path: "crates/coven/src/handle.rs".to_string(),
            syntax: source,
        }];

        assert!(find_database_boundary_violations(&files).is_empty());
    }

    #[test]
    fn database_owned_sqlite_reexport_is_allowed() {
        let source = syn::parse_file(
            r#"
            pub use database::rusqlite;
            "#,
        )
        .expect("parse fixture");
        let files = vec![RustFile {
            relative_path: "crates/coven/src/lib.rs".to_string(),
            syntax: source,
        }];

        assert!(find_database_boundary_violations(&files).is_empty());
    }

    #[test]
    fn coven_owned_sql_is_rejected_outside_database() {
        let schema = syn::parse_file(
            r#"
            macro_rules! coven_tables {
                ($visit:ident) => {
                    $visit!(protocol_state, "key TEXT PRIMARY KEY");
                };
            }
            macro_rules! coven_routing_tables {
                ($visit:ident) => {
                    $visit!(_coven_row_audiences, "table_name TEXT NOT NULL");
                };
            }
            "#,
        )
        .expect("parse schema fixture");
        let leak = syn::parse_file(
            r#"
            fn leak(database: DatabaseTestSql<'_>) {
                database.execute("DELETE FROM protocol_state", []).unwrap();
                database.query("SELECT * FROM _coven_row_audiences", [], |_| Ok(())).unwrap();
            }
            "#,
        )
        .expect("parse leak fixture");
        let files = vec![
            RustFile {
                relative_path: "crates/coven/src/database/coven_schema.rs".to_string(),
                syntax: schema,
            },
            RustFile {
                relative_path: "crates/coven/src/sync/leak.rs".to_string(),
                syntax: leak,
            },
        ];

        let violations = find_database_boundary_violations(&files);
        assert_eq!(
            violations
                .iter()
                .map(|violation| violation.kind.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "Coven-owned SQL for table _coven_row_audiences",
                "Coven-owned SQL for table protocol_state",
            ]),
        );
    }
}
