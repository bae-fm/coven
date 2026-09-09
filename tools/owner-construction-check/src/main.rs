use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

mod capability_boundaries;
mod module_dependencies;
mod owner_dependency_boundary;
use module_dependencies::{
    find_module_dependency_violations, ModuleDependencyError, ModuleDependencyViolation,
};
use owner_dependency_boundary::{
    find_owner_dependency_leaks, find_owner_dependency_leaks_with_policy, OwnerDependencyLeak,
};

use capability_boundaries::{
    find_capability_boundary_violations, CapabilityBoundaryViolation, GatedCapability,
    AMBIENT_BOUNDARY, CRYPTO_BOUNDARY, FILESYSTEM_BOUNDARY, KEYRING_BOUNDARY, NETWORK_BOUNDARY,
    RUNTIME_BOUNDARY, VERIFICATION_ARTIFACT_BOUNDARY,
};

mod owner_policy;
use owner_policy::*;

#[derive(Clone)]
pub(crate) struct RustFile {
    pub(crate) relative_path: String,
    pub(crate) syntax: syn::File,
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

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
struct RetainedCapabilityParameterViolation {
    path: String,
    line: usize,
    owner: String,
    method: String,
    capability: String,
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
struct TransientComponentBundleViolation {
    path: String,
    line: usize,
    bundle: String,
}

#[derive(Default)]
struct BundleTypeInfo {
    public_fields: usize,
    field_count: usize,
    inherent_methods: Vec<(String, bool, bool)>,
}

fn find_transient_component_bundle_violations(
    files: &[RustFile],
) -> Vec<TransientComponentBundleViolation> {
    let bundle_types = collect_transient_component_bundle_types(files);
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        let mut visitor = TransientComponentBundleVisitor {
            path: &file.relative_path,
            bundle_types: &bundle_types,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

fn collect_transient_component_bundle_types(files: &[RustFile]) -> BTreeSet<String> {
    let mut types = BTreeMap::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        collect_bundle_type_info(&file.syntax.items, &mut types);
    }
    types
        .into_iter()
        .filter_map(|(name, info)| {
            (info.field_count >= 2
                && info.field_count == info.public_fields
                && matches!(info.inherent_methods.as_slice(), [(method, true, false)] if method == "new"))
            .then_some(name)
        })
        .collect()
}

fn collect_bundle_type_info(items: &[syn::Item], types: &mut BTreeMap<String, BundleTypeInfo>) {
    for item in items {
        match item {
            syn::Item::Struct(item) if !is_test_only(&item.attrs) => {
                let info = types.entry(item.ident.to_string()).or_default();
                info.field_count = item.fields.len();
                info.public_fields = item
                    .fields
                    .iter()
                    .filter(|field| visibility_crosses_owner(&field.vis))
                    .count();
            }
            syn::Item::Impl(item) if item.trait_.is_none() && !is_test_only(&item.attrs) => {
                let Some(name) = type_name(&item.self_ty) else {
                    continue;
                };
                let info = types.entry(name).or_default();
                for method in &item.items {
                    let syn::ImplItem::Fn(method) = method else {
                        continue;
                    };
                    if is_test_only(&method.attrs) {
                        continue;
                    }
                    info.inherent_methods.push((
                        method.sig.ident.to_string(),
                        output_contains_owner(
                            &method.sig.output,
                            &type_name(&item.self_ty).expect("inherent impl type"),
                        ),
                        method.sig.receiver().is_some(),
                    ));
                }
            }
            syn::Item::Mod(item) if !is_test_only(&item.attrs) => {
                if let Some((_, items)) = &item.content {
                    collect_bundle_type_info(items, types);
                }
            }
            _ => {}
        }
    }
}

struct TransientComponentBundleVisitor<'a> {
    path: &'a str,
    bundle_types: &'a BTreeSet<String>,
    violations: &'a mut BTreeSet<TransientComponentBundleViolation>,
}

impl<'ast> Visit<'ast> for TransientComponentBundleVisitor<'_> {
    fn visit_local(&mut self, node: &'ast syn::Local) {
        let syn::Pat::Struct(pattern) = &node.pat else {
            return visit::visit_local(self, node);
        };
        let Some(initializer) = &node.init else {
            return visit::visit_local(self, node);
        };
        let syn::Expr::Call(call) = initializer.expr.as_ref() else {
            return visit::visit_local(self, node);
        };
        let syn::Expr::Path(function) = call.func.as_ref() else {
            return visit::visit_local(self, node);
        };
        let Some(bundle) = pattern
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
        else {
            return visit::visit_local(self, node);
        };
        let Some(constructor) = function.path.segments.last() else {
            return visit::visit_local(self, node);
        };
        let Some(owner) = function.path.segments.iter().rev().nth(1) else {
            return visit::visit_local(self, node);
        };
        if constructor.ident == "new"
            && owner.ident == bundle
            && self.bundle_types.contains(&bundle)
        {
            self.violations.insert(TransientComponentBundleViolation {
                path: self.path.to_string(),
                line: node.span().start().line,
                bundle,
            });
        }
        visit::visit_local(self, node);
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
struct DeepParentPathViolation {
    path: String,
    line: usize,
}

fn find_deep_parent_path_violations(files: &[RustFile]) -> Vec<DeepParentPathViolation> {
    let mut violations = BTreeSet::new();
    for file in files {
        let mut visitor = DeepParentPathVisitor {
            path: &file.relative_path,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

struct DeepParentPathVisitor<'a> {
    path: &'a str,
    violations: &'a mut BTreeSet<DeepParentPathViolation>,
}

impl DeepParentPathVisitor<'_> {
    fn record(&mut self, span: Span) {
        self.violations.insert(DeepParentPathViolation {
            path: self.path.to_string(),
            line: span.start().line,
        });
    }
}

impl<'ast> Visit<'ast> for DeepParentPathVisitor<'_> {
    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        if use_tree_skips_parent(&node.tree, 0) {
            self.record(node.span());
        }
        visit::visit_item_use(self, node);
    }

    fn visit_path(&mut self, node: &'ast syn::Path) {
        if node
            .segments
            .iter()
            .take(2)
            .all(|segment| segment.ident == "super")
            && node.segments.len() >= 2
        {
            self.record(node.span());
        }
        visit::visit_path(self, node);
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

fn find_retained_capability_parameter_violations(
    files: &[RustFile],
    owners: &BTreeSet<String>,
    constructors: &BTreeSet<Constructor>,
) -> Vec<RetainedCapabilityParameterViolation> {
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        find_retained_capability_parameters_in_items(
            &file.relative_path,
            &file.syntax.items,
            owners,
            constructors,
            &mut violations,
        );
    }
    violations.into_iter().collect()
}

fn find_retained_capability_parameters_in_items(
    path: &str,
    items: &[syn::Item],
    owners: &BTreeSet<String>,
    constructors: &BTreeSet<Constructor>,
    violations: &mut BTreeSet<RetainedCapabilityParameterViolation>,
) {
    for item in items {
        match item {
            syn::Item::Impl(item) => {
                if is_test_only(&item.attrs) {
                    continue;
                }
                let Some(owner) = type_name(&item.self_ty) else {
                    continue;
                };
                if !owners.contains(&owner) {
                    continue;
                }
                for impl_item in &item.items {
                    let syn::ImplItem::Fn(method) = impl_item else {
                        continue;
                    };
                    if is_test_only(&method.attrs) {
                        continue;
                    }
                    let callable = Constructor {
                        owner: owner.clone(),
                        method: method.sig.ident.to_string(),
                    };
                    if constructors.contains(&callable)
                        || COMPOSITION_ROOTS
                            .iter()
                            .any(|(root_path, root_owner, root_method)| {
                                path == *root_path
                                    && owner == *root_owner
                                    && method.sig.ident == *root_method
                            })
                    {
                        continue;
                    }
                    for input in &method.sig.inputs {
                        let syn::FnArg::Typed(input) = input else {
                            continue;
                        };
                        let mut names = TypeNames::default();
                        names.visit_type(&input.ty);
                        for capability in CONSTRUCTION_ONLY_CAPABILITY_TYPES {
                            if names.names.contains(*capability) {
                                violations.insert(RetainedCapabilityParameterViolation {
                                    path: path.to_string(),
                                    line: input.span().start().line,
                                    owner: owner.clone(),
                                    method: method.sig.ident.to_string(),
                                    capability: (*capability).to_string(),
                                });
                            }
                        }
                    }
                }
            }
            syn::Item::Mod(item) => {
                if is_test_only(&item.attrs) {
                    continue;
                }
                if let Some((_, items)) = &item.content {
                    find_retained_capability_parameters_in_items(
                        path,
                        items,
                        owners,
                        constructors,
                        violations,
                    );
                }
            }
            _ => {}
        }
    }
}

struct CheckResult {
    owner_construction: Vec<Violation>,
    database_boundary: Vec<DatabaseBoundaryViolation>,
    owner_dependency_leaks: Vec<OwnerDependencyLeak>,
    service_returns: Vec<ServiceReturnViolation>,
    retained_service_construction: Vec<RetainedServiceConstructionViolation>,
    retained_capability_parameters: Vec<RetainedCapabilityParameterViolation>,
    transient_component_bundles: Vec<TransientComponentBundleViolation>,
    deep_parent_paths: Vec<DeepParentPathViolation>,
    capability_boundaries: Vec<CapabilityBoundaryViolation>,
    module_dependencies: Vec<ModuleDependencyViolation>,
}

#[derive(Debug, thiserror::Error)]
enum CheckError {
    #[error("read {}: {source}", path.display())]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse {}: {source}", path.display())]
    ParseFile {
        path: PathBuf,
        #[source]
        source: syn::Error,
    },
    #[error("relativize {} against {}: {source}", path.display(), root.display())]
    Relativize {
        path: PathBuf,
        root: PathBuf,
        #[source]
        source: std::path::StripPrefixError,
    },
    #[error("read directory {}: {source}", path.display())]
    ReadDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("read directory entry in {}: {source}", path.display())]
    ReadDirectoryEntry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    ModuleDependency(#[from] ModuleDependencyError),
}

const CAPABILITY_BOUNDARY_FLAGS: &[(&str, &[GatedCapability])] = &[
    ("--network-boundary", NETWORK_BOUNDARY),
    ("--crypto-boundary", CRYPTO_BOUNDARY),
    ("--keyring-boundary", KEYRING_BOUNDARY),
    ("--runtime-boundary", RUNTIME_BOUNDARY),
    ("--ambient-boundary", AMBIENT_BOUNDARY),
    ("--filesystem-boundary", FILESYSTEM_BOUNDARY),
    (
        "--verification-artifact-boundary",
        VERIFICATION_ARTIFACT_BOUNDARY,
    ),
];

fn main() {
    let mut database_boundary = false;
    let mut owner_dependency_boundary = false;
    let mut retained_service_returns = false;
    let mut retained_service_construction = false;
    let mut retained_capability_parameters = false;
    let mut transient_component_bundles = false;
    let mut capability_boundaries: Vec<&'static [GatedCapability]> = Vec::new();
    let mut module_dependencies = false;
    let mut owner_dependency_only = false;
    let mut rust_roots = Vec::new();
    let mut extra_capability_types = Vec::new();
    let mut allowed_capability_outputs = Vec::new();
    let mut root = None;
    let mut arguments = std::env::args_os().skip(1);
    while let Some(argument) = arguments.next() {
        if argument == "--module-dependencies" {
            module_dependencies = true;
        } else if argument == "--database-boundary" {
            database_boundary = true;
        } else if argument == "--owner-dependency-boundary" {
            owner_dependency_boundary = true;
        } else if argument == "--retained-service-returns" {
            retained_service_returns = true;
        } else if argument == "--retained-service-construction" {
            retained_service_construction = true;
        } else if argument == "--retained-capability-parameters" {
            retained_capability_parameters = true;
        } else if argument == "--transient-component-bundles" {
            transient_component_bundles = true;
        } else if argument == "--owner-dependency-only" {
            owner_dependency_only = true;
        } else if argument == "--rust-root" {
            let Some(path) = arguments.next() else {
                print_usage_and_exit();
            };
            rust_roots.push(PathBuf::from(path));
        } else if argument == "--capability-type" {
            let Some(name) = arguments.next() else {
                print_usage_and_exit();
            };
            extra_capability_types.push(name.to_string_lossy().into_owned());
        } else if argument == "--allowed-capability-output" {
            let Some(name) = arguments.next() else {
                print_usage_and_exit();
            };
            allowed_capability_outputs.push(name.to_string_lossy().into_owned());
        } else if let Some((_, boundary)) = CAPABILITY_BOUNDARY_FLAGS
            .iter()
            .find(|(flag, _)| argument == *flag)
        {
            capability_boundaries.push(boundary);
        } else if root.is_none() {
            root = Some(PathBuf::from(argument));
        } else {
            print_usage_and_exit();
        }
    }
    let root = root.unwrap_or_else(|| PathBuf::from("."));
    if owner_dependency_only {
        if database_boundary
            || owner_dependency_boundary
            || retained_service_returns
            || retained_service_construction
            || retained_capability_parameters
            || transient_component_bundles
            || !capability_boundaries.is_empty()
            || module_dependencies
        {
            print_usage_and_exit();
        }
        let rust_roots = if rust_roots.is_empty() {
            vec![PathBuf::from(".")]
        } else {
            rust_roots
        };
        match check_owner_dependency_only(
            &root,
            &rust_roots,
            &extra_capability_types,
            &allowed_capability_outputs,
        ) {
            Ok(leaks) if leaks.is_empty() => return,
            Ok(leaks) => {
                for leak in &leaks {
                    print_owner_dependency_leak(leak);
                }
                eprintln!(
                    "owners use retained dependencies internally and expose closed operations to callers"
                );
                std::process::exit(1);
            }
            Err(error) => {
                eprintln!("owner construction check failed: {error}");
                std::process::exit(2);
            }
        }
    }
    if !rust_roots.is_empty()
        || !extra_capability_types.is_empty()
        || !allowed_capability_outputs.is_empty()
    {
        print_usage_and_exit();
    }
    match check(
        &root,
        database_boundary,
        owner_dependency_boundary,
        retained_service_returns,
        retained_service_construction,
        retained_capability_parameters,
        transient_component_bundles,
        &capability_boundaries,
        module_dependencies,
    ) {
        Ok(result)
            if result.owner_construction.is_empty()
                && result.database_boundary.is_empty()
                && result.owner_dependency_leaks.is_empty()
                && result.service_returns.is_empty()
                && result.retained_service_construction.is_empty()
                && result.retained_capability_parameters.is_empty()
                && result.transient_component_bundles.is_empty()
                && result.deep_parent_paths.is_empty()
                && result.capability_boundaries.is_empty()
                && result.module_dependencies.is_empty() => {}
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
            for violation in &result.owner_dependency_leaks {
                print_owner_dependency_leak(violation);
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
            for violation in &result.retained_capability_parameters {
                eprintln!(
                    "{}:{}: {}::{} accepts construction-only capability {} at runtime",
                    violation.path,
                    violation.line,
                    violation.owner,
                    violation.method,
                    violation.capability
                );
            }
            for violation in &result.transient_component_bundles {
                eprintln!(
                    "{}:{}: {} only bundles components for immediate destructuring",
                    violation.path, violation.line, violation.bundle
                );
            }
            for violation in &result.deep_parent_paths {
                eprintln!(
                    "{}:{}: paths cannot skip over the immediate parent module with super::super",
                    violation.path, violation.line
                );
            }
            for violation in &result.capability_boundaries {
                eprintln!(
                    "{}:{}: {} is confined to {}",
                    violation.path,
                    violation.line,
                    violation.kind,
                    violation.homes.join(", ")
                );
            }
            for violation in &result.module_dependencies {
                eprintln!(
                    "{}:{}: {} ({:?}) references {} ({:?}) against the dependency direction",
                    violation.path,
                    violation.line,
                    violation.from,
                    violation.from_region,
                    violation.to,
                    violation.to_region
                );
            }
            if !result.owner_construction.is_empty() {
                eprintln!(
                    "retained owner constructors accept complete dependencies; construct owner graphs only in approved composition roots"
                );
            }
            if !result.database_boundary.is_empty() {
                eprintln!(
                    "database operations retain SQLite state and expose domain methods; move raw SQLite and SQL under crates/coven-database"
                );
            }
            if !result.owner_dependency_leaks.is_empty() {
                eprintln!(
                    "owners use retained dependencies internally and expose closed operations to callers"
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
            if !result.retained_capability_parameters.is_empty() {
                eprintln!(
                    "construction-only capabilities are bound when owner graphs are composed and are never accepted by runtime owner methods"
                );
            }
            if !result.transient_component_bundles.is_empty() {
                eprintln!(
                    "construct each component at the handle that retains it; a bundle type needs behavior or an invariant of its own"
                );
            }
            if !result.deep_parent_paths.is_empty() {
                eprintln!(
                    "import the capability from the immediate parent or from its domain boundary"
                );
            }
            if !result.capability_boundaries.is_empty() {
                eprintln!(
                    "raw capabilities live with their declared owners; compose the owner that retains the capability instead of naming its crates or construction paths"
                );
            }
            if !result.module_dependencies.is_empty() {
                eprintln!(
                    "module references point down the architecture: host → domain → replication → protocol/database/storage, and below those the coven-keys and coven-foundation crates"
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

fn print_usage_and_exit() -> ! {
    eprintln!(
        "usage: owner-construction-check [--database-boundary] [--owner-dependency-boundary] [--retained-service-returns] [--retained-service-construction] [--retained-capability-parameters] [--transient-component-bundles] [--network-boundary] [--crypto-boundary] [--keyring-boundary] [--runtime-boundary] [--ambient-boundary] [--filesystem-boundary] [--verification-artifact-boundary] [--module-dependencies] [root]\n       owner-construction-check --owner-dependency-only [--rust-root path]... [--capability-type Type]... [--allowed-capability-output Type]... [root]"
    );
    std::process::exit(2);
}

fn print_owner_dependency_leak(violation: &OwnerDependencyLeak) {
    match violation {
        OwnerDependencyLeak::Field {
            path,
            line,
            owner,
            field,
        } => eprintln!("{path}:{line}: retained-service owner {owner} exposes field {field}"),
        OwnerDependencyLeak::CrateRootSessionField {
            path,
            line,
            session,
            dependency,
        } => eprintln!(
            "{path}:{line}: crate-root {session} exposes retained dependency {dependency} to every descendant module"
        ),
        OwnerDependencyLeak::Return {
            path,
            line,
            owner,
            method,
            dependency,
        } => eprintln!(
            "{path}:{line}: {owner}::{method} returns retained dependency {dependency}"
        ),
        OwnerDependencyLeak::Parameter {
            path,
            line,
            owner,
            method,
            dependency,
        } => eprintln!("{path}:{line}: {owner}::{method} accepts raw dependency {dependency}"),
        OwnerDependencyLeak::FreeReturn {
            path,
            line,
            function,
            dependency,
        } => eprintln!("{path}:{line}: {function} returns raw dependency {dependency}"),
        OwnerDependencyLeak::FreeParameter {
            path,
            line,
            function,
            dependency,
        } => eprintln!("{path}:{line}: {function} accepts raw dependency {dependency}"),
        OwnerDependencyLeak::RawProviderOperation {
            path,
            line,
            owner,
            method,
        } => eprintln!("{path}:{line}: {owner}::{method} exposes raw provider-object access"),
    }
}

fn check_owner_dependency_only(
    root: &Path,
    rust_roots: &[PathBuf],
    extra_capability_types: &[String],
    allowed_capability_outputs: &[String],
) -> Result<Vec<OwnerDependencyLeak>, CheckError> {
    let files = rust_files_in(root, rust_roots)?;
    Ok(find_owner_dependency_leaks_with_policy(
        &files,
        extra_capability_types,
        allowed_capability_outputs,
    ))
}

fn check(
    root: &Path,
    check_database_boundary: bool,
    check_owner_dependency_boundary: bool,
    check_retained_service_returns: bool,
    check_retained_service_construction: bool,
    check_retained_capability_parameters: bool,
    check_transient_component_bundles: bool,
    capability_boundaries: &[&'static [GatedCapability]],
    check_module_dependencies: bool,
) -> Result<CheckResult, CheckError> {
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
        owner_dependency_leaks: if check_owner_dependency_boundary {
            find_owner_dependency_leaks(&files)
        } else {
            Vec::new()
        },
        service_returns: if check_retained_service_returns {
            find_service_return_violations(
                &files,
                &retained_services,
                &service_owners,
                RETAINED_SERVICE_ROOT_TYPES,
            )
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
        retained_capability_parameters: if check_retained_capability_parameters {
            find_retained_capability_parameter_violations(&files, &owners, &constructors)
        } else {
            Vec::new()
        },
        transient_component_bundles: if check_transient_component_bundles {
            find_transient_component_bundle_violations(&files)
        } else {
            Vec::new()
        },
        deep_parent_paths: find_deep_parent_path_violations(&files),
        capability_boundaries: capability_boundaries
            .iter()
            .flat_map(|boundary| find_capability_boundary_violations(&files, boundary))
            .collect(),
        module_dependencies: if check_module_dependencies {
            find_module_dependency_violations(&files)?
        } else {
            Vec::new()
        },
    })
}

mod database_boundary;
use database_boundary::*;

mod repository_sources;
use repository_sources::*;

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
    retained_owners: &BTreeSet<String>,
    stateful_services: &BTreeSet<String>,
    root_types: &[&str],
) -> Vec<ServiceReturnViolation> {
    let returned_services = stateful_services
        .iter()
        .filter(|service| {
            !root_types.contains(&service.as_str())
                && !OPERATION_SCOPED_OWNER_TYPES.contains(&service.as_str())
                && !CAPABILITY_TYPES.contains(&service.as_str())
                && !service.ends_with("Inner")
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let declared_types = collect_declared_types(files);
    let retained_capabilities = retained_owners
        .iter()
        .map(|owner| {
            let capabilities =
                collect_root_retained_types(&declared_types, stateful_services, &[owner.as_str()])
                    .into_iter()
                    .filter(|service| CAPABILITY_TYPES.contains(&service.as_str()))
                    .collect::<BTreeSet<_>>();
            (owner.clone(), capabilities)
        })
        .collect::<BTreeMap<_, _>>();
    let mut violations = BTreeSet::new();
    for file in files {
        if is_test_source(&file.relative_path) {
            continue;
        }
        find_service_returns_in_items(
            &file.relative_path,
            &file.syntax.items,
            retained_owners,
            &returned_services,
            &retained_capabilities,
            &mut violations,
        );
    }
    violations.into_iter().collect()
}

fn find_service_returns_in_items(
    path: &str,
    items: &[syn::Item],
    retained_owners: &BTreeSet<String>,
    returned_services: &BTreeSet<String>,
    retained_capabilities: &BTreeMap<String, BTreeSet<String>>,
    violations: &mut BTreeSet<ServiceReturnViolation>,
) {
    for item in items {
        match item {
            syn::Item::Impl(item) => {
                if is_test_only(&item.attrs) {
                    continue;
                }
                let owner = type_name(&item.self_ty).unwrap_or_else(|| "<impl>".to_string());
                if !retained_owners.contains(&owner) {
                    continue;
                }
                let mut owner_returned_services = returned_services.clone();
                if let Some(capabilities) = retained_capabilities.get(&owner) {
                    owner_returned_services.extend(capabilities.iter().cloned());
                }
                for impl_item in &item.items {
                    let syn::ImplItem::Fn(method) = impl_item else {
                        continue;
                    };
                    if is_test_only(&method.attrs) {
                        continue;
                    }
                    if !visibility_crosses_owner(&method.vis) {
                        continue;
                    }
                    record_service_returns(
                        path,
                        &owner,
                        &method.sig.ident.to_string(),
                        &method.sig.output,
                        method.sig.ident.span(),
                        &owner_returned_services,
                        violations,
                    );
                }
            }
            syn::Item::Mod(item) => {
                if is_test_only(&item.attrs) {
                    continue;
                }
                if let Some((_, items)) = &item.content {
                    find_service_returns_in_items(
                        path,
                        items,
                        retained_owners,
                        returned_services,
                        retained_capabilities,
                        violations,
                    );
                }
            }
            _ => {}
        }
    }
}

fn visibility_crosses_owner(visibility: &syn::Visibility) -> bool {
    match visibility {
        syn::Visibility::Inherited => false,
        syn::Visibility::Restricted(restricted) => !restricted.path.is_ident("self"),
        syn::Visibility::Public(_) => true,
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
            if NON_OWNER_TYPES.contains(&name.as_str())
                || BORROWED_FACADE_TYPES.contains(&name.as_str())
            {
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
            || (OPERATION_SCOPED_OWNER_TYPES.contains(&parent.owner.as_str())
                && OPERATION_SCOPED_OWNER_TYPES.contains(&child))
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

mod retained_service_construction;
use retained_service_construction::*;

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
