//! Every rule, run over the workspace, and what the run found.

use crate::capability_boundaries::{
    find_capability_boundary_violations, CapabilityBoundaryViolation,
};
use crate::capability_construction::{
    find_capability_construction_violations, CapabilityConstructionViolation,
};
use crate::component_bundles::{find_component_bundle_violations, ComponentBundleViolation};
use crate::conventions::{find_convention_violations, ConventionViolation};
use crate::crate_dependencies::{find_crate_dependency_violations, CrateDependencyViolation};
use crate::database_boundary::{find_database_boundary_violations, DatabaseBoundaryViolation};
use crate::owner_construction::{
    collect_constructors, collect_free_constructors, find_owner_construction_violations,
    infer_owners, OwnerConstructionViolation,
};
use crate::owner_dependency_boundary::{find_owner_dependency_leaks, OwnerDependencyLeak};
use crate::policy::Policy;
use crate::retained_capability_parameters::{
    find_retained_capability_parameter_violations, RetainedCapabilityParameterViolation,
};
use crate::retained_services::{
    collect_root_retained_types, find_retained_service_construction_violations,
    find_service_return_violations, RetainedServiceConstructionViolation, ServiceReturnViolation,
};
use crate::sources::Workspace;
use crate::syntax::{collect_declared_types, collect_structs, RustFile};

pub(crate) struct Report {
    crate_dependencies: Vec<CrateDependencyViolation>,
    owner_construction: Vec<OwnerConstructionViolation>,
    database_boundary: Vec<DatabaseBoundaryViolation>,
    owner_dependency_leaks: Vec<OwnerDependencyLeak>,
    service_returns: Vec<ServiceReturnViolation>,
    retained_service_construction: Vec<RetainedServiceConstructionViolation>,
    retained_capability_parameters: Vec<RetainedCapabilityParameterViolation>,
    component_bundles: Vec<ComponentBundleViolation>,
    capability_boundaries: Vec<CapabilityBoundaryViolation>,
    capability_construction: Vec<CapabilityConstructionViolation>,
    conventions: Vec<ConventionViolation>,
}

/// Runs every rule. The §21.1 and §21.2 rules read the crates; the §21.3
/// conventions read every Rust file, tools included.
pub(crate) fn check(workspace: &Workspace, policy: &Policy) -> Report {
    let crate_files = workspace
        .files
        .iter()
        .filter(|file| file.is_crate_source())
        .cloned()
        .collect::<Vec<RustFile>>();
    let files = crate_files.as_slice();
    let structs = collect_structs(files);
    let owners = infer_owners(&structs, policy);
    let constructors = collect_constructors(files, &owners);
    let free_constructors = collect_free_constructors(files, &owners);
    let declared_types = collect_declared_types(files);
    let service_owners = infer_owners(&declared_types, policy);
    let retained_services = collect_root_retained_types(
        &declared_types,
        &service_owners,
        policy.root_owner_types,
        policy,
    );
    Report {
        crate_dependencies: find_crate_dependency_violations(workspace, policy),
        owner_construction: find_owner_construction_violations(
            files,
            &owners,
            &constructors,
            &free_constructors,
            policy,
        ),
        database_boundary: find_database_boundary_violations(files, policy),
        owner_dependency_leaks: find_owner_dependency_leaks(files, policy),
        service_returns: find_service_return_violations(
            files,
            &retained_services,
            &service_owners,
            policy,
        ),
        retained_service_construction: find_retained_service_construction_violations(
            files,
            &retained_services,
            policy,
        ),
        retained_capability_parameters: find_retained_capability_parameter_violations(
            files,
            &owners,
            &constructors,
            policy,
        ),
        component_bundles: find_component_bundle_violations(files),
        capability_boundaries: find_capability_boundary_violations(files, policy),
        capability_construction: find_capability_construction_violations(files, policy),
        conventions: find_convention_violations(&workspace.files),
    }
}

impl Report {
    pub(crate) fn is_empty(&self) -> bool {
        self.crate_dependencies.is_empty()
            && self.owner_construction.is_empty()
            && self.database_boundary.is_empty()
            && self.owner_dependency_leaks.is_empty()
            && self.service_returns.is_empty()
            && self.retained_service_construction.is_empty()
            && self.retained_capability_parameters.is_empty()
            && self.component_bundles.is_empty()
            && self.capability_boundaries.is_empty()
            && self.capability_construction.is_empty()
            && self.conventions.is_empty()
    }

    /// Each finding on its own line, then one line per broken rule saying
    /// what the rule asks for.
    pub(crate) fn lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        let mut remedies = Vec::new();
        for violation in &self.crate_dependencies {
            lines.push(match violation {
                CrateDependencyViolation::NotFromWorkspace {
                    manifest,
                    dependency,
                    keys,
                } => format!(
                    "{manifest}: dependency {dependency} sets [{}] itself instead of `workspace = true` with only features or optional",
                    keys.join(", ")
                ),
                CrateDependencyViolation::Unplaced { manifest, package } => format!(
                    "{manifest}: crate {package} has no row in the policy's crate_order"
                ),
                CrateDependencyViolation::Upward {
                    manifest,
                    from,
                    to,
                } => format!("{manifest}: {from} depends on {to}, which is not above it in crate_order"),
                CrateDependencyViolation::Separated {
                    manifest,
                    from,
                    to,
                } => format!("{manifest}: {from} depends on {to}; the two never depend on each other"),
            });
        }
        if !self.crate_dependencies.is_empty() {
            remedies.push("each crate depends only on crates above it in §21.1's list, and each external dependency's version is set once in [workspace.dependencies]");
        }
        for violation in &self.owner_construction {
            lines.push(format!(
                "{}:{}: owner constructor {} constructs owner {}",
                violation.path, violation.line, violation.parent, violation.child
            ));
        }
        if !self.owner_construction.is_empty() {
            remedies.push("an owner is given its collaborators; owner graphs are built only at the policy's composition roots");
        }
        for violation in &self.database_boundary {
            lines.push(format!(
                "{}:{}: {} is confined to coven-database",
                violation.path, violation.line, violation.kind
            ));
        }
        if !self.database_boundary.is_empty() {
            remedies.push("ask the database owner to run the work; raw SQLite and coven's own SQL live in coven-database");
        }
        for violation in &self.owner_dependency_leaks {
            lines.push(owner_dependency_leak_line(violation));
        }
        if !self.owner_dependency_leaks.is_empty() {
            remedies.push("owners use what they hold internally and offer closed work to callers");
        }
        for violation in &self.service_returns {
            lines.push(format!(
                "{}:{}: {}::{} returns retained service {}",
                violation.path,
                violation.line,
                violation.owner,
                violation.method,
                violation.returned
            ));
        }
        if !self.service_returns.is_empty() {
            remedies.push("an owner never hands out what it holds; callers ask it to do the work");
        }
        for violation in &self.retained_service_construction {
            lines.push(match &violation.authority {
                Some(authority) => format!(
                    "{}:{}: {}::{} constructs {}, whose lifetime authority is {}",
                    violation.path,
                    violation.line,
                    violation.owner,
                    violation.method,
                    violation.service,
                    authority
                ),
                None => format!(
                    "{}:{}: {}::{} constructs retained service {} outside a composition root",
                    violation.path,
                    violation.line,
                    violation.owner,
                    violation.method,
                    violation.service
                ),
            });
        }
        if !self.retained_service_construction.is_empty() {
            remedies.push("retained services are built at composition roots, or by the one lifetime authority of a service replaced while the store is open");
        }
        for violation in &self.retained_capability_parameters {
            lines.push(format!(
                "{}:{}: {}::{} accepts construction-only capability {} at runtime",
                violation.path,
                violation.line,
                violation.owner,
                violation.method,
                violation.capability
            ));
        }
        if !self.retained_capability_parameters.is_empty() {
            remedies.push(
                "a method never takes a raw capability; it uses the one its owner was built with",
            );
        }
        for violation in &self.component_bundles {
            lines.push(format!(
                "{}:{}: {} only bundles components to be destructured",
                violation.path, violation.line, violation.bundle
            ));
        }
        if !self.component_bundles.is_empty() {
            remedies.push("pass collaborators by name; a bundle type needs behavior or an invariant of its own");
        }
        for violation in &self.capability_boundaries {
            lines.push(format!(
                "{}:{}: {} ({}) is used directly only in {}",
                violation.path,
                violation.line,
                violation.kind,
                violation.capability,
                if violation.homes.is_empty() {
                    "no file yet".to_string()
                } else {
                    violation.homes.join(", ")
                }
            ));
        }
        if !self.capability_boundaries.is_empty() {
            remedies.push("reach a capability through the owner that holds it, given to you when you are built");
        }
        for violation in &self.capability_construction {
            lines.push(format!(
                "{}:{}: constructs capability {} outside a composition root",
                violation.path, violation.line, violation.capability
            ));
        }
        if !self.capability_construction.is_empty() {
            remedies.push("construct unit capabilities at composition roots; elsewhere use the capability given to the owner");
        }
        for violation in &self.conventions {
            lines.push(format!(
                "{}:{}: {}",
                violation.path,
                violation.line,
                violation.message()
            ));
            let remedy = violation.convention.remedy();
            if !remedies.contains(&remedy) {
                remedies.push(remedy);
            }
        }
        lines.extend(remedies.into_iter().map(str::to_string));
        lines
    }
}

fn owner_dependency_leak_line(violation: &OwnerDependencyLeak) -> String {
    match violation {
        OwnerDependencyLeak::Field {
            path,
            line,
            owner,
            field,
        } => format!("{path}:{line}: service owner {owner} exposes field {field}"),
        OwnerDependencyLeak::CrateRootSessionField {
            path,
            line,
            session,
            dependency,
        } => format!(
            "{path}:{line}: crate-root {session} exposes internal dependency {dependency} to every module"
        ),
        OwnerDependencyLeak::Return {
            path,
            line,
            owner,
            method,
            dependency,
        } => format!("{path}:{line}: {owner}::{method} returns retained dependency {dependency}"),
        OwnerDependencyLeak::Parameter {
            path,
            line,
            owner,
            method,
            dependency,
        } => format!("{path}:{line}: {owner}::{method} accepts raw dependency {dependency}"),
        OwnerDependencyLeak::FreeReturn {
            path,
            line,
            function,
            dependency,
        } => format!("{path}:{line}: {function} returns raw dependency {dependency}"),
        OwnerDependencyLeak::FreeParameter {
            path,
            line,
            function,
            dependency,
        } => format!("{path}:{line}: {function} accepts raw dependency {dependency}"),
        OwnerDependencyLeak::RawProviderOperation {
            path,
            line,
            owner,
            method,
        } => format!("{path}:{line}: {owner}::{method} exposes raw provider-object access"),
    }
}

#[cfg(test)]
#[path = "report_tests.rs"]
mod tests;
