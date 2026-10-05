//! The guard tests of §21.4: every file, crate, type and composition root the
//! policy file names must exist, so the policy can't go stale. A row keyed on
//! a path that moved does not fail a rule — it stops matching, and the rule
//! silently covers nothing — so each one is resolved against the workspace
//! here instead.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use syn::visit::{self, Visit};

use super::POLICY;
use crate::policy::Policy;
use crate::sources::load;
use crate::syntax::{collect_declared_types, type_name};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the checker lives two directories under the workspace root")
        .to_path_buf()
}

/// Capability homes name the paths §21.2 assigns ahead of the code. A home
/// inside a crate that has not landed is skipped; one inside a crate that has
/// must exist.
fn stranded_homes(root: &Path, policy: &Policy) -> Vec<String> {
    policy
        .capabilities
        .all()
        .into_iter()
        .flat_map(|capability| {
            capability
                .homes
                .iter()
                .map(move |home| (capability.name, *home))
        })
        .filter(|(_, home)| {
            let mut segments = home.split('/');
            match (segments.next(), segments.next()) {
                (Some("crates"), Some(name)) => {
                    root.join("crates").join(name).join("Cargo.toml").is_file()
                }
                _ => true,
            }
        })
        .filter(|(_, home)| {
            let resolved = root.join(home);
            if home.ends_with('/') {
                !resolved.is_dir()
            } else {
                !resolved.is_file()
            }
        })
        .map(|(capability, home)| format!("{capability}: {home}"))
        .collect()
}

#[test]
fn every_capability_home_in_a_landed_crate_exists() {
    let stranded = stranded_homes(&workspace_root(), &POLICY);
    assert!(
        stranded.is_empty(),
        "capability homes no longer resolve, so the capabilities keyed on them cover nothing:\n{}",
        stranded.join("\n")
    );
}

#[test]
fn a_home_is_skipped_until_its_crate_lands_and_checked_after() {
    let root = std::env::temp_dir().join(format!(
        "owner-construction-check-homes-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let clock_crate = root.join("crates/coven-foundation");
    std::fs::create_dir_all(clock_crate.join("src")).expect("create fixture crate");
    std::fs::write(clock_crate.join("Cargo.toml"), "").expect("write fixture manifest");

    let stranded = stranded_homes(&root, &POLICY);
    std::fs::remove_dir_all(&root).expect("remove fixture workspace");

    let foundation_homes = POLICY
        .capabilities
        .all()
        .into_iter()
        .flat_map(|capability| {
            capability
                .homes
                .iter()
                .filter(|home| home.starts_with("crates/coven-foundation/"))
                .map(move |home| format!("{}: {home}", capability.name))
        })
        .collect::<Vec<_>>();
    assert!(!foundation_homes.is_empty());
    assert_eq!(stranded, foundation_homes);
}

#[test]
fn every_composition_root_names_an_existing_method() {
    struct MethodCollector<'a> {
        path: &'a str,
        methods: &'a mut BTreeSet<(String, String, String)>,
    }

    impl Visit<'_> for MethodCollector<'_> {
        fn visit_item_impl(&mut self, node: &syn::ItemImpl) {
            let Some(owner) = type_name(&node.self_ty) else {
                return;
            };
            for item in &node.items {
                if let syn::ImplItem::Fn(method) = item {
                    self.methods.insert((
                        self.path.to_string(),
                        owner.clone(),
                        method.sig.ident.to_string(),
                    ));
                }
            }
            visit::visit_item_impl(self, node);
        }
    }

    let workspace = load(&workspace_root()).expect("read the workspace");
    let mut methods = BTreeSet::new();
    for file in &workspace.files {
        MethodCollector {
            path: &file.relative_path,
            methods: &mut methods,
        }
        .visit_file(&file.syntax);
    }
    let missing = POLICY
        .composition_roots
        .iter()
        .filter(|(path, owner, method)| {
            !methods.contains(&(
                (*path).to_string(),
                (*owner).to_string(),
                (*method).to_string(),
            ))
        })
        .map(|(path, owner, method)| format!("{path}: {owner}::{method}"))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "composition roots name methods that do not exist:\n{}",
        missing.join("\n")
    );
}

/// Every type name the policy holds, with the row it came from.
fn named_types(policy: &Policy) -> Vec<(&'static str, &'static str)> {
    let lists: [(&str, &[&str]); 12] = [
        ("capability_types", policy.capability_types),
        (
            "construction_only_capability_types",
            policy.construction_only_capability_types,
        ),
        ("non_owner_types", policy.non_owner_types),
        ("borrowed_facade_types", policy.borrowed_facade_types),
        ("root_owner_types", policy.root_owner_types),
        ("task_types", policy.task_types),
        (
            "internal_dependency_types",
            policy.internal_dependency_types,
        ),
        ("always_forbidden_returns", policy.always_forbidden_returns),
        ("closed_session_types", policy.closed_session_types),
        ("field_capability_types", policy.field_capability_types),
        (
            "unexported_capability_types",
            policy.unexported_capability_types,
        ),
        (
            "exportable_capability_outputs",
            policy.exportable_capability_outputs,
        ),
    ];
    let mut named = lists
        .into_iter()
        .flat_map(|(row, names)| names.iter().map(move |name| (row, *name)))
        .collect::<Vec<_>>();
    for (service, authority) in policy.lifetime_authorities {
        named.push(("lifetime_authorities", service));
        named.push(("lifetime_authorities", authority));
    }
    for (owner, _) in policy.raw_provider_operations {
        named.push(("raw_provider_operations", owner));
    }
    for (derived, sources) in policy.derived_services {
        named.push(("derived_services", derived));
        named.extend(sources.iter().map(|source| ("derived_services", *source)));
    }
    named
}

#[test]
fn every_type_the_policy_names_is_declared() {
    let workspace = load(&workspace_root()).expect("read the workspace");
    let crate_files = workspace
        .files
        .into_iter()
        .filter(|file| file.is_crate_source())
        .collect::<Vec<_>>();
    let declared = collect_declared_types(&crate_files);
    let missing = named_types(&POLICY)
        .into_iter()
        .filter(|(_, name)| !declared.contains_key(*name))
        .map(|(row, name)| format!("{row}: {name}"))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "the policy names types no crate declares:\n{}",
        missing.join("\n")
    );
}

#[test]
fn every_crate_the_order_names_is_in_the_workspace() {
    let workspace = load(&workspace_root()).expect("read the workspace");
    let packages = workspace
        .manifests
        .iter()
        .filter(|manifest| manifest.relative_path.starts_with("crates/"))
        .filter_map(|manifest| manifest.table.get("package")?.get("name")?.as_str())
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let missing = POLICY
        .crate_order
        .iter()
        .chain(
            POLICY
                .separated_crates
                .iter()
                .flat_map(|(first, second)| [first, second]),
        )
        .filter(|name| !packages.contains(**name))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "crate_order and separated_crates name crates not in the workspace: {missing:?}"
    );
    for (first, second) in POLICY.separated_crates {
        assert!(POLICY.crate_order.contains(first) && POLICY.crate_order.contains(second));
    }
}

#[test]
fn the_database_schema_file_declares_its_table_macros() {
    let Some((schema_file, table_macros)) = POLICY.database_schema else {
        return;
    };
    let workspace = load(&workspace_root()).expect("read the workspace");
    let schema = workspace
        .files
        .iter()
        .find(|file| file.relative_path == schema_file)
        .unwrap_or_else(|| panic!("database schema file {schema_file} does not exist"));
    let declared = schema
        .syntax
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Macro(item) => item.ident.as_ref().map(ToString::to_string),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for table_macro in table_macros {
        assert!(
            declared.contains(*table_macro),
            "{schema_file} declares no macro_rules! {table_macro}"
        );
    }
}
