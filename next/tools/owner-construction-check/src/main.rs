//! The structural checker of §21.4. It reads the syntax tree of every Rust
//! file and every member manifest in the workspace and holds them to the
//! dependency rules of §21.1, the capability and owner rules of §21.2 —
//! against the policy file, `owner_policy.rs` — and the conventions of §21.3.
//!
//! `owner-construction-check [workspace root]` runs every rule; there is no
//! way to run fewer. It exits 1 when a rule is broken and 2 when the workspace
//! can't be read.

mod capability_boundaries;
mod component_bundles;
mod conventions;
mod crate_dependencies;
mod database_boundary;
mod macros;
mod owner_construction;
mod owner_dependency_boundary;
mod owner_policy;
mod policy;
mod report;
mod retained_capability_parameters;
mod retained_services;
mod sources;
mod syntax;
mod test_layout;

use std::path::PathBuf;

fn main() {
    let mut arguments = std::env::args_os().skip(1);
    let root = match (arguments.next(), arguments.next()) {
        (None, None) => PathBuf::from("."),
        (Some(root), None) => PathBuf::from(root),
        _ => {
            eprintln!("usage: owner-construction-check [workspace root]");
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
    let report = report::check(&workspace, &owner_policy::POLICY);
    if !report.is_empty() {
        for line in report.lines() {
            eprintln!("{line}");
        }
        std::process::exit(1);
    }
}
