//! The structural checker of Appendix F.6. It reads the syntax tree of every Rust
//! file and every member manifest in the workspace and holds them to the
//! dependency rules of Appendix F.1, the capability and owner rules of F.2 and F.3 —
//! against the policy file, `owner_policy.rs` — and the conventions of F.5.
//!
//! `owner-construction-check [workspace root] [package ...]` runs every rule.
//! Package names limit diagnostics while keeping the complete workspace for
//! analysis. It exits 1 when a rule is broken and 2 for invalid inputs.

mod capability_boundaries;
mod capability_construction;
mod component_bundles;
mod conventions;
mod crate_dependencies;
mod database_boundary;
mod factories;
mod finding;
mod macro_expansion;
mod macros;
mod module_sources;
mod name_resolution;
mod owner_dependency_boundary;
mod owner_graph;
mod owner_policy;
mod policy;
mod report;
mod retained_capability_parameters;
mod rust_prelude;
mod sources;
mod syntax;
mod test_layout;
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
    let workspace = match sources::load(&root) {
        Ok(workspace) => workspace,
        Err(error) => {
            eprintln!("owner-construction-check: {error}");
            std::process::exit(2);
        }
    };
    let report = match report::check_packages(&workspace, &owner_policy::POLICY, &packages) {
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
