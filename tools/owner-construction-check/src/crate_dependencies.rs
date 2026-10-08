//! The dependency rules of §20.1, read from the member manifests.
//!
//! - Each crate depends only on crates above it in the list — earlier in the
//!   policy's `crate_order` — and the separated pairs never depend on each
//!   other. Every crate under `crates/` has a row, so a new crate is placed
//!   when it lands. Dev-dependencies answer to the same order: a test that
//!   reaches up the list belongs to the crate whose types it builds.
//! - Each external dependency's version is set once, in the workspace: a
//!   member names a dependency with `workspace = true` and, at most, the
//!   features it needs and whether it is optional.
//!
//! The manifests are the rule's subject, not the source's `use` paths: Cargo
//! refuses a reference to a crate the manifest does not declare, so a
//! declaration is where an edge between crates begins.

use std::collections::BTreeSet;

use crate::finding::Finding;
use crate::policy::Policy;
use crate::sources::{Manifest, Workspace};

const DEPENDENCY_TABLES: &[&str] = &["dependencies", "dev-dependencies", "build-dependencies"];

/// What a member may say about a dependency besides `workspace = true`.
const MEMBER_DEPENDENCY_KEYS: &[&str] = &["workspace", "features", "optional"];

const REMEDY: &str = "each crate depends only on crates above it in §20.1's list, and each external dependency's version is set once in [workspace.dependencies]";

pub(crate) fn find_crate_dependency_violations(
    workspace: &Workspace,
    policy: &Policy,
) -> Vec<Finding> {
    let mut violations = BTreeSet::new();
    for manifest in &workspace.manifests {
        let is_crate = manifest.relative_path.starts_with("crates/");
        let package = package_name(manifest);
        let rank = package
            .as_deref()
            .and_then(|package| policy.crate_order.iter().position(|name| *name == package));
        if let (true, Some(package), None) = (is_crate, &package, rank) {
            violations.insert(Finding::new(
                &manifest.relative_path,
                1,
                format!("crate {package} has no row in the policy's crate_order"),
                REMEDY,
            ));
        }
        for (dependency, entry) in dependency_entries(&manifest.table) {
            let keys = entry_keys(entry);
            let from_workspace = keys.iter().any(|key| key == "workspace")
                && entry
                    .get("workspace")
                    .and_then(toml::Value::as_bool)
                    .unwrap_or(false)
                && keys
                    .iter()
                    .all(|key| MEMBER_DEPENDENCY_KEYS.contains(&key.as_str()));
            if !from_workspace {
                violations.insert(Finding::new(&manifest.relative_path, 1, format!("dependency {dependency} sets [{}] itself instead of `workspace = true` with only features or optional", keys.join(", ")), REMEDY));
            }
            let (Some(from), Some(from_rank)) = (&package, rank) else {
                continue;
            };
            let to = workspace_package(workspace, dependency);
            let Some(to_rank) = policy.crate_order.iter().position(|name| *name == to) else {
                continue;
            };
            if policy.separated_crates.iter().any(|pair| {
                *pair == (from.as_str(), to.as_str()) || *pair == (to.as_str(), from.as_str())
            }) {
                violations.insert(Finding::new(
                    &manifest.relative_path,
                    1,
                    format!("{from} depends on {to}; the two never depend on each other"),
                    REMEDY,
                ));
            } else if to_rank >= from_rank {
                violations.insert(Finding::new(
                    &manifest.relative_path,
                    1,
                    format!("{from} depends on {to}, which is not above it in crate_order"),
                    REMEDY,
                ));
            }
        }
    }
    violations.into_iter().collect()
}

fn package_name(manifest: &Manifest) -> Option<String> {
    manifest
        .table
        .get("package")?
        .get("name")?
        .as_str()
        .map(str::to_string)
}

/// Every dependency entry of every dependency table, target-specific ones
/// included.
fn dependency_entries(table: &toml::Table) -> Vec<(&String, &toml::Value)> {
    let targets = match table.get("target") {
        Some(toml::Value::Table(targets)) => targets
            .values()
            .filter_map(toml::Value::as_table)
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    std::iter::once(table)
        .chain(targets)
        .flat_map(|table| {
            DEPENDENCY_TABLES
                .iter()
                .filter_map(|name| table.get(*name).and_then(toml::Value::as_table))
        })
        .flat_map(|dependencies| dependencies.iter())
        .collect()
}

/// The keys of a dependency entry; a bare version string has none.
fn entry_keys(entry: &toml::Value) -> Vec<String> {
    match entry {
        toml::Value::Table(entry) => entry.keys().cloned().collect(),
        _ => vec!["version".to_string()],
    }
}

/// The package a member's dependency key resolves to through the workspace's
/// dependency table, which may rename it.
fn workspace_package(workspace: &Workspace, dependency: &str) -> String {
    workspace
        .workspace_dependencies
        .get(dependency)
        .and_then(|entry| entry.get("package"))
        .and_then(toml::Value::as_str)
        .unwrap_or(dependency)
        .to_string()
}

#[cfg(test)]
#[path = "crate_dependencies_tests.rs"]
mod tests;
