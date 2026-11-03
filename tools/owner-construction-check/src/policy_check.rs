//! The policy's own check (Appendix F.6), run before any rule: every member,
//! file, type and callable the policy names must exist, so the policy can't go
//! stale. A row keyed on a path that moved does not fail a rule — it stops
//! matching, and the rule silently covers nothing — so each one is resolved
//! against the workspace here instead.

use std::collections::{BTreeMap, BTreeSet};

use syn::visit::{self, Visit};

use crate::capability_construction::construction_only_types;
use crate::crate_code::CrateCode;
use crate::crate_dependencies::{dependency_entries, dependency_package};
use crate::database_boundary::schema_table_macros;
use crate::owner_graph::OwnerGraph;
use crate::policy::{Database, Policy};
use crate::sources::Workspace;
use crate::sql_statements::{statements, Creation};
use crate::syntax::{collect_declared_types, type_name, type_names, RustFile, TypeNames};

/// Each policy row that names something the workspace does not hold. Rows
/// naming code are held against the crates' code with their macros
/// expanded, so a method a macro writes can be a composition root; the
/// schema's table macros are read as written.
pub(crate) fn stale_rows(workspace: &Workspace, code: &CrateCode, policy: &Policy) -> Vec<String> {
    let crate_files = &code.expanded;
    let mut stale = Vec::new();
    if policy.rules.is_empty() {
        stale.push("rules: the policy selects no rules".to_string());
    }
    stale.extend(unknown_members(workspace, policy));
    stale.extend(unknown_libraries(workspace, policy));
    stale.extend(missing_homes(&code.written, policy));
    stale.extend(missing_composition_roots(crate_files, policy));
    stale.extend(invalid_capability_factories(crate_files, policy));
    stale.extend(invalid_handles(crate_files, policy));
    stale.extend(overlapping_state_rows(policy));
    stale.extend(undeclared_types(crate_files, policy));
    stale.extend(missing_capability_traits(crate_files, policy));
    stale.extend(invalid_database(code, workspace, policy));
    stale
}

fn member_packages(workspace: &Workspace) -> BTreeSet<String> {
    workspace.manifests.iter().filter_map(|manifest| manifest.package_name()).collect()
}

/// `tools`, `api_crate`, `crate_order` and `separated_crates` name members;
/// the crates among them are the members that are not tools.
fn unknown_members(workspace: &Workspace, policy: &Policy) -> Vec<String> {
    let members = member_packages(workspace);
    let mut stale = Vec::new();
    for tool in &policy.tools {
        if !members.contains(tool) {
            stale.push(format!("tools: {tool} is not a workspace member"));
        }
    }
    if let Some(api_crate) = &policy.api_crate {
        if !members.contains(api_crate) || policy.is_tool(api_crate) {
            stale.push(format!("api_crate: {api_crate} is not a crate of the workspace"));
        }
    }
    for name in &policy.crate_order {
        if !members.contains(name) || policy.is_tool(name) {
            stale.push(format!("crate_order: {name} is not a crate of the workspace"));
        }
    }
    for (first, second) in &policy.separated_crates {
        for name in [first, second] {
            if !policy.crate_order.contains(name) {
                stale.push(format!("separated_crates: {name} has no row in crate_order"));
            }
        }
    }
    stale
}

/// `library_names` names packages a member depends on.
fn unknown_libraries(workspace: &Workspace, policy: &Policy) -> Vec<String> {
    let dependencies = workspace
        .manifests
        .iter()
        .flat_map(|manifest| dependency_entries(&manifest.table))
        .map(|(name, entry)| dependency_package(workspace, name, entry))
        .collect::<BTreeSet<_>>();
    policy
        .library_names
        .keys()
        .filter(|package| !dependencies.contains(*package))
        .map(|package| format!("library_names: no member depends on {package}"))
        .collect()
}

/// A home is a source file, or a directory (ending in `/`) holding one.
fn holds_source(files: &[RustFile], home: &str) -> bool {
    files.iter().any(|file| home_holds(home, &file.relative_path))
}

fn home_holds(home: &str, path: &str) -> bool {
    if home.ends_with('/') {
        path.starts_with(home)
    } else {
        path == home
    }
}

fn missing_homes(files: &[RustFile], policy: &Policy) -> Vec<String> {
    policy
        .capabilities
        .iter()
        .flat_map(|capability| capability.homes.iter().map(move |home| (capability, home)))
        .filter(|(_, home)| !holds_source(files, home))
        .map(|(capability, home)| {
            format!("capabilities: {} has home {home}, which holds no source", capability.name)
        })
        .collect()
}

/// Every free function and method of the crates' production code, keyed by
/// its file, its type or trait (`<free>` for a free function) and its name.
/// A trait's method is the one its declaration names, as a connector's
/// `connect` is each implementation's.
fn declared_methods(files: &[RustFile]) -> BTreeMap<(String, String, String), syn::Signature> {
    struct MethodCollector<'a> {
        path: &'a str,
        methods: &'a mut BTreeMap<(String, String, String), syn::Signature>,
    }

    impl Visit<'_> for MethodCollector<'_> {
        fn visit_item_fn(&mut self, node: &syn::ItemFn) {
            self.methods.insert(
                (self.path.to_string(), "<free>".into(), node.sig.ident.to_string()),
                node.sig.clone(),
            );
            visit::visit_item_fn(self, node);
        }

        fn visit_item_trait(&mut self, node: &syn::ItemTrait) {
            for item in &node.items {
                if let syn::TraitItem::Fn(method) = item {
                    self.methods.insert(
                        (
                            self.path.to_string(),
                            node.ident.to_string(),
                            method.sig.ident.to_string(),
                        ),
                        method.sig.clone(),
                    );
                }
            }
            visit::visit_item_trait(self, node);
        }

        fn visit_item_impl(&mut self, node: &syn::ItemImpl) {
            let Some(owner) = type_name(&node.self_ty) else {
                return;
            };
            for item in &node.items {
                if let syn::ImplItem::Fn(method) = item {
                    self.methods.insert(
                        (self.path.to_string(), owner.clone(), method.sig.ident.to_string()),
                        method.sig.clone(),
                    );
                }
            }
            visit::visit_item_impl(self, node);
        }
    }

    let mut methods = BTreeMap::new();
    for file in files {
        MethodCollector { path: &file.relative_path, methods: &mut methods }
            .visit_file(&file.syntax);
    }
    methods
}

/// Every `static` of the crates' production code, keyed by its file and its
/// name: a root may name one whose value is an owner, such as a process-wide
/// registry.
fn declared_statics(files: &[RustFile]) -> BTreeSet<(String, String)> {
    struct StaticCollector<'a> {
        path: &'a str,
        statics: &'a mut BTreeSet<(String, String)>,
    }

    impl Visit<'_> for StaticCollector<'_> {
        fn visit_item_static(&mut self, node: &syn::ItemStatic) {
            self.statics.insert((self.path.to_string(), node.ident.to_string()));
            visit::visit_item_static(self, node);
        }
    }

    let mut statics = BTreeSet::new();
    for file in files {
        StaticCollector { path: &file.relative_path, statics: &mut statics }
            .visit_file(&file.syntax);
    }
    statics
}

fn missing_composition_roots(files: &[RustFile], policy: &Policy) -> Vec<String> {
    let methods = declared_methods(files);
    let statics = declared_statics(files);
    policy
        .composition_roots
        .iter()
        .filter(|root| {
            let function =
                methods.contains_key(&(root.path.clone(), root.owner.clone(), root.method.clone()));
            let static_item = root.owner == "<free>"
                && statics.contains(&(root.path.clone(), root.method.clone()));
            !function && !static_item
        })
        .map(|root| {
            format!(
                "composition_roots: {}: {}::{} names no function or static there",
                root.path, root.owner, root.method
            )
        })
        .collect()
}

/// A factory names a receiver method of a capability type, declared in its
/// file, returning a construction-only capability.
fn invalid_capability_factories(files: &[RustFile], policy: &Policy) -> Vec<String> {
    let methods = declared_methods(files);
    let capabilities = construction_only_types(files, policy);
    policy
        .capability_factories
        .iter()
        .filter(|factory| {
            let signature = methods.get(&(
                factory.path.clone(),
                factory.owner.clone(),
                factory.method.clone(),
            ));
            !policy.capability_types.contains(&factory.owner)
                || !capabilities.contains(&factory.product)
                || !signature.is_some_and(|signature| {
                    signature.receiver().is_some()
                        && match &signature.output {
                            syn::ReturnType::Type(_, output) => {
                                type_names(output, &policy.phantom_type_arguments)
                                    .contains(&factory.product)
                            }
                            syn::ReturnType::Default => false,
                        }
                })
        })
        .map(|factory| {
            format!(
                "capability_factories: {}: {}::{} -> {} is not a receiver method of a capability type returning that construction-only capability",
                factory.path, factory.owner, factory.method, factory.product
            )
        })
        .collect()
}

/// A handle is no capability or shared state row of its own, and its owner,
/// another type, has a method returning it: a row whose owner makes none
/// covers nothing.
fn invalid_handles(files: &[RustFile], policy: &Policy) -> Vec<String> {
    let methods = declared_methods(files);
    let mut stale = Vec::new();
    for (handle, owner) in &policy.handles {
        if handle == owner {
            stale.push(format!("handles: {handle} names itself as its owner"));
        } else if !methods.iter().any(|((_, maker, _), signature)| {
            maker == owner
                && match &signature.output {
                    syn::ReturnType::Type(_, output) => {
                        type_names(output, &policy.phantom_type_arguments).contains(handle)
                    }
                    syn::ReturnType::Default => false,
                }
        }) {
            stale.push(format!("handles: {owner} has no method returning {handle}"));
        }
        if policy.capability_types.contains(handle) {
            stale.push(format!("handles: {handle} is also in capability_types"));
        }
        if policy.shared_state_types.contains(handle) {
            stale.push(format!("handles: {handle} is also in shared_state_types"));
        }
    }
    stale
}

/// Shared state is no capability row, and private state neither of the two
/// nor a handle: a type is held one way.
fn overlapping_state_rows(policy: &Policy) -> Vec<String> {
    let mut stale = Vec::new();
    for name in policy.shared_state_types.intersection(&policy.capability_types) {
        stale.push(format!("shared_state_types: {name} is also in capability_types"));
    }
    for name in &policy.private_state_types {
        for (row, names) in [
            ("capability_types", &policy.capability_types),
            ("shared_state_types", &policy.shared_state_types),
        ] {
            if names.contains(name) {
                stale.push(format!("private_state_types: {name} is also in {row}"));
            }
        }
        if policy.handles.contains_key(name) {
            stale.push(format!("private_state_types: {name} is also a handle"));
        }
    }
    stale
}

/// A type the policy names is declared by a crate, or is one of its external
/// types: named by a crate's production source and declared by none.
fn undeclared_types(crate_files: &[RustFile], policy: &Policy) -> Vec<String> {
    let declared = collect_declared_types(
        crate_files,
        &policy.shared_pointer_types,
        &policy.phantom_type_arguments,
    );
    // Every name, type arguments of phantom generics included.
    let every_argument = BTreeSet::new();
    let mut mentioned = TypeNames::new(&every_argument);
    for file in crate_files {
        mentioned.visit_file(&file.syntax);
    }
    let named = policy.named_types();
    let mut stale = named
        .iter()
        .filter(|(_, name)| !policy.external_types.contains(*name) && !declared.contains_key(*name))
        .map(|(row, name)| format!("{row}: no crate declares {name}"))
        .collect::<Vec<_>>();
    for name in &policy.external_types {
        if declared.contains_key(name) {
            stale.push(format!("external_types: a crate declares {name}"));
        }
        if !mentioned.names.contains(name) {
            stale.push(format!("external_types: no crate names {name}"));
        }
        if !named.iter().any(|(_, named)| named == name) {
            stale.push(format!("external_types: no type row names {name}"));
        }
    }
    stale
}

fn missing_capability_traits(crate_files: &[RustFile], policy: &Policy) -> Vec<String> {
    struct Traits(BTreeSet<String>);

    impl<'ast> Visit<'ast> for Traits {
        fn visit_item_trait(&mut self, node: &'ast syn::ItemTrait) {
            self.0.insert(node.ident.to_string());
            visit::visit_item_trait(self, node);
        }
    }

    let mut traits = Traits(BTreeSet::new());
    for file in crate_files {
        traits.visit_file(&file.syntax);
    }
    policy
        .capability_traits
        .iter()
        .filter(|name| !traits.0.contains(*name))
        .map(|name| format!("capability_traits: no crate declares trait {name}"))
        .collect()
}

/// The database names one of the policy's capabilities; its schema module
/// exists and declares exactly its table macros, each once. A table list
/// declared outside the module, or one the policy omits, would leave its
/// tables unguarded by the database boundary.
///
/// With domains, each table of the schema is in one, and each of their tables
/// is the schema's, so a new table is placed when it lands and none goes
/// unguarded. The schema's tables are those its table macros declare and those
/// a `CREATE TABLE` in the crates' SQL files makes.
fn invalid_database(code: &CrateCode, workspace: &Workspace, policy: &Policy) -> Vec<String> {
    let Some(database) = &policy.database else {
        return Vec::new();
    };
    let mut stale = Vec::new();
    if !policy.capabilities.iter().any(|capability| capability.name == database.capability) {
        stale.push(format!("database: capability {} is not in capabilities", database.capability));
    }
    if let Some(package) = &database.rusqlite_reexport {
        if !member_packages(workspace).contains(package) {
            stale.push(format!("database: rusqlite_reexport {package} is not a workspace member"));
        }
    }
    stale.extend(invalid_domains(code, database, policy));
    let mut schema = code
        .sql
        .iter()
        .flat_map(|file| statements(&file.text))
        .filter_map(|statement| match statement.creation {
            Some(Creation::Table(table)) => Some(table),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    match &database.schema {
        None if !database.table_macros.is_empty() => {
            stale.push("database: table_macros without a schema module".to_string());
        }
        None => {}
        Some(schema_root)
            if !code.written.iter().any(|file| file.relative_path == *schema_root) =>
        {
            stale.push(format!("database: schema module root {schema_root} does not exist"));
            return stale;
        }
        Some(schema_root) => {
            let macros = schema_table_macros(&code.written, schema_root);
            for table_macro in &database.table_macros {
                let declared =
                    macros.iter().filter(|declared| declared.name == *table_macro).count();
                if declared != 1 {
                    stale.push(format!(
                        "database: the schema module under {schema_root} declares macro_rules! {table_macro} {declared} times, not once"
                    ));
                }
            }
            for declared in &macros {
                if !declared.tables.is_empty() && !database.table_macros.contains(&declared.name) {
                    stale.push(format!(
                        "database: schema table macro {} is missing from table_macros",
                        declared.name
                    ));
                }
            }
            schema.extend(
                macros
                    .iter()
                    .filter(|declared| database.table_macros.contains(&declared.name))
                    .flat_map(|declared| declared.tables.iter().map(|table| table.to_lowercase())),
            );
        }
    }
    if !database.domains.is_empty() {
        let placed = database
            .domains
            .iter()
            .flat_map(|domain| domain.tables.iter().map(|table| table.to_lowercase()))
            .collect::<BTreeSet<_>>();
        for table in schema.difference(&placed) {
            stale.push(format!("database: schema table {table} is in no domain"));
        }
        for table in placed.difference(&schema) {
            stale.push(format!("database: domain table {table} is not a schema table"));
        }
    }
    stale
}

/// Each domain has a unique name, homes that hold a source, Rust or SQL, and
/// an owner the checker infers as one; no table is in two domains.
fn invalid_domains(code: &CrateCode, database: &Database, policy: &Policy) -> Vec<String> {
    let inferred = OwnerGraph::collect(&code.expanded, policy).owners;
    let mut stale = Vec::new();
    let mut names = BTreeSet::new();
    let mut owners = BTreeMap::new();
    for domain in &database.domains {
        if !names.insert(domain.name.as_str()) {
            stale.push(format!("database: domain {} is named twice", domain.name));
        }
        for home in &domain.homes {
            if !holds_source(&code.written, home)
                && !code.sql.iter().any(|file| home_holds(home, &file.relative_path))
            {
                stale.push(format!(
                    "database: domain {} has home {home}, which holds no source",
                    domain.name
                ));
            }
        }
        if !inferred.contains(&domain.owner) {
            stale.push(format!(
                "database: domain {} names owner {}, which holds no capability, shared state or owner",
                domain.name, domain.owner
            ));
        }
        for table in &domain.tables {
            if let Some(other) = owners.insert(table.to_lowercase(), domain.name.as_str()) {
                stale.push(format!(
                    "database: table {table} is in domains {other} and {}",
                    domain.name
                ));
            }
        }
    }
    stale
}

#[cfg(test)]
#[path = "policy_check_tests.rs"]
mod tests;
