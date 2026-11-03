//! The structural checker of Appendix F.6. It reads the syntax tree of every
//! Rust file, every SQL file and every member manifest in the workspace and
//! holds them to the dependency rules of Appendix F.1, the capability and owner
//! rules of F.2 and F.3, and the conventions of F.5 — the ones the workspace's
//! policy file, `owner-construction-check.toml` at its root, selects, against
//! that policy.
//!
//! `owner-construction-check [workspace root] [package ...]` checks the policy,
//! then runs its rules. Package names limit diagnostics while keeping the
//! complete workspace for analysis. It exits 1 when a rule is broken and 2 for
//! invalid inputs, a policy row naming something the workspace lacks among them.

mod capability_boundaries;
mod capability_construction;
mod component_bundles;
mod conventions;
mod crate_code;
mod crate_dependencies;
mod database_boundary;
mod domain_owners;
mod factories;
mod finding;
mod macro_expansion;
mod macro_rules;
mod macros;
mod module_sources;
mod name_resolution;
mod owner_dependency_boundary;
mod owner_graph;
mod policy;
mod policy_check;
mod report;
mod retained_capability_parameters;
mod rust_prelude;
mod sources;
mod sql_statements;
mod syntax;
mod task_state;
mod test_code;
mod test_layout;
mod type_identity;
mod unique_type_names;

use std::path::PathBuf;

fn main() {
    let mut arguments = std::env::args_os().skip(1);
    let root = match arguments.next() {
        None => PathBuf::from("."),
        Some(root) => PathBuf::from(root),
    };
    let packages = match arguments.map(|name| name.into_string()).collect::<Result<Vec<_>, _>>() {
        Ok(packages) => packages,
        Err(name) => {
            eprintln!("owner-construction-check: package name is not UTF-8: {name:?}");
            std::process::exit(2);
        }
    };
    let policy = match policy::Policy::read(&root) {
        Ok(policy) => policy,
        Err(error) => {
            eprintln!("owner-construction-check: {error}");
            std::process::exit(2);
        }
    };
    let workspace = match sources::load(&root, &policy.library_names) {
        Ok(workspace) => workspace,
        Err(error) => {
            eprintln!("owner-construction-check: {error}");
            std::process::exit(2);
        }
    };
    let code = crate_code::CrateCode::read(&workspace, &policy);
    let stale = policy_check::stale_rows(&workspace, &code, &policy);
    if !stale.is_empty() {
        eprintln!(
            "owner-construction-check: {} names what the workspace does not hold:",
            policy::FILE_NAME
        );
        for row in stale {
            eprintln!("  {row}");
        }
        std::process::exit(2);
    }
    let report = match report::check_packages(&workspace, &code, &policy, &packages) {
        Ok(report) => report,
        Err(error) => {
            eprintln!("owner-construction-check: {error}");
            std::process::exit(2);
        }
    };
    if !report.is_empty() {
        for line in report.lines() {
            eprintln!("{line}");
        }
        std::process::exit(1);
    }
}
