//! Every rule, run over the workspace, and its findings.

use crate::capability_boundaries::find_capability_boundary_violations;
use crate::capability_construction::find_capability_construction_violations;
use crate::component_bundles::find_component_bundle_violations;
use crate::conventions::find_convention_violations;
use crate::crate_code::CrateCode;
use crate::crate_dependencies::find_crate_dependency_violations;
use crate::database_boundary::find_database_boundary_violations;
use crate::domain_owners::find_domain_owner_forwarding;
use crate::finding::Finding;
use crate::owner_dependency_boundary::find_owner_dependency_leaks;
use crate::owner_graph::OwnerGraph;
use crate::policy::{Policy, Rule};
use crate::retained_capability_parameters::find_retained_capability_parameter_violations;
use crate::sources::Workspace;
#[cfg(test)]
use crate::syntax::RustFile;
use crate::task_state::find_task_state_leaks;
use crate::unique_type_names::find_unique_type_name_violations;

pub(crate) struct Report(pub(crate) Vec<Finding>);

#[derive(Debug, thiserror::Error)]
#[error("unknown workspace package {0}")]
pub(crate) struct UnknownPackage(String);

/// Package selection limits diagnostics, never the workspace used to resolve
/// names and ownership. An empty selection checks the whole workspace.
pub(crate) fn check_packages(
    workspace: &Workspace,
    code: &CrateCode,
    policy: &Policy,
    packages: &[String],
) -> Result<Report, UnknownPackage> {
    let roots = packages
        .iter()
        .map(|package| {
            workspace
                .manifests
                .iter()
                .find(|manifest| {
                    manifest
                        .table
                        .get("package")
                        .and_then(|table| table.get("name"))
                        .and_then(toml::Value::as_str)
                        == Some(package.as_str())
                })
                .map(|manifest| manifest.relative_path.trim_end_matches("Cargo.toml"))
                .ok_or_else(|| UnknownPackage(package.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(check_paths(workspace, code, policy, &roots))
}

/// The policy's selected rules. The capability and owner rules read the
/// crates' code (`crate_code.rs`); conventions read every file as written,
/// tools and tests included. An invocation the checker could not expand is a
/// finding whenever a rule reads the crates' code.
fn check_paths(workspace: &Workspace, code: &CrateCode, policy: &Policy, roots: &[&str]) -> Report {
    let files = &code.expanded;
    let graph = OwnerGraph::collect(files, policy);
    let mut findings = Vec::new();
    if policy.rules.iter().any(|rule| rule.reads_crate_code()) {
        findings.extend(code.unexpanded.iter().cloned());
    }
    if policy.selects(Rule::CrateDependencies) {
        findings.extend(find_crate_dependency_violations(workspace, policy));
    }
    if policy.selects(Rule::DatabaseBoundary) {
        findings.extend(find_database_boundary_violations(code, policy));
        findings.extend(find_domain_owner_forwarding(files, policy, &graph));
    }
    if policy.selects(Rule::OwnerDependencies) {
        findings.extend(find_owner_dependency_leaks(files, policy, &graph));
        findings.extend(find_task_state_leaks(files, policy, &graph));
    }
    if policy.selects(Rule::RetainedCapabilityParameters) {
        findings.extend(find_retained_capability_parameter_violations(files, &graph, policy));
    }
    if policy.selects(Rule::ComponentBundles) {
        findings.extend(find_component_bundle_violations(files, &graph.identity));
    }
    if policy.selects(Rule::CapabilityBoundaries) {
        findings.extend(find_capability_boundary_violations(files, policy));
    }
    if policy.selects(Rule::CapabilityConstruction) {
        findings.extend(find_capability_construction_violations(files, policy, &graph));
    }
    if policy.selects(Rule::Conventions) {
        findings.extend(find_convention_violations(&workspace.files, policy.api_crate.as_deref()));
    }
    findings.retain(|finding| {
        roots.is_empty()
            || finding.path == "Cargo.toml"
            || roots.iter().any(|root| finding.path.starts_with(root))
    });
    // Ambiguous names invalidate the workspace graph, even when the first
    // declaration (where the diagnostic points) is outside selected packages.
    if policy.rules.iter().any(|rule| rule.reads_owner_graph()) {
        findings.extend(find_unique_type_name_violations(files, policy, &graph));
    }
    findings.sort();
    findings.dedup();
    Report(findings)
}

impl Report {
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Each finding, then one explanation per broken rule.
    pub(crate) fn lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        let mut remedies = Vec::new();
        for finding in &self.0 {
            lines.push(format!("{}:{}: {}", finding.path, finding.line, finding.message));
            if !remedies.contains(&finding.remedy.as_str()) {
                remedies.push(finding.remedy.as_str());
            }
        }
        lines.extend(remedies.into_iter().map(str::to_string));
        lines
    }
}

#[cfg(test)]
#[path = "report_tests.rs"]
mod tests;
