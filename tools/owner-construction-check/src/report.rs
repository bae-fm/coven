//! Every rule, run over the workspace, and its findings.

use crate::capability_boundaries::find_capability_boundary_violations;
use crate::capability_construction::find_capability_construction_violations;
use crate::component_bundles::find_component_bundle_violations;
use crate::conventions::find_convention_violations;
use crate::crate_dependencies::find_crate_dependency_violations;
use crate::database_boundary::find_database_boundary_violations;
use crate::finding::Finding;
use crate::owner_dependency_boundary::find_owner_dependency_leaks;
use crate::owner_graph::OwnerGraph;
use crate::policy::Policy;
use crate::retained_capability_parameters::find_retained_capability_parameter_violations;
use crate::sources::Workspace;
#[cfg(test)]
use crate::syntax::RustFile;
use crate::unique_type_names::find_unique_type_name_violations;

pub(crate) struct Report(pub(crate) Vec<Finding>);

/// The capability and owner rules read crates; conventions also read tools.
pub(crate) fn check(workspace: &Workspace, policy: &Policy) -> Report {
    let files = workspace
        .files
        .iter()
        .filter(|file| file.is_crate_source())
        .cloned()
        .collect::<Vec<_>>();
    let graph = OwnerGraph::collect(&files, policy);
    let mut findings = Vec::new();
    findings.extend(find_unique_type_name_violations(&files, policy, &graph));
    findings.extend(find_crate_dependency_violations(workspace, policy));
    findings.extend(find_database_boundary_violations(&files, policy));
    findings.extend(find_owner_dependency_leaks(&files, policy, &graph));
    findings.extend(find_retained_capability_parameter_violations(
        &files,
        &graph.owners,
        policy,
    ));
    findings.extend(find_component_bundle_violations(&files));
    findings.extend(find_capability_boundary_violations(&files, policy));
    findings.extend(find_capability_construction_violations(
        &files, policy, &graph,
    ));
    findings.extend(find_convention_violations(&workspace.files));
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
            lines.push(format!(
                "{}:{}: {}",
                finding.path, finding.line, finding.message
            ));
            if !remedies.contains(&finding.remedy) {
                remedies.push(finding.remedy);
            }
        }
        lines.extend(remedies.into_iter().map(str::to_string));
        lines
    }
}

#[cfg(test)]
#[path = "report_tests.rs"]
mod tests;
