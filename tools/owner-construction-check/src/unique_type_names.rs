//! Name-based matching requires a unique declaration for each policy type,
//! construction-only capability and inferred owner across `crates/`. Policy
//! guards check that named types exist; this rule rejects ambiguous names.
//! Test-only declarations are excluded; `test-utils` declarations count.

use std::collections::BTreeSet;

use crate::capability_construction::construction_only_types;
use crate::finding::Finding;
use crate::owner_graph::OwnerGraph;
use crate::policy::Policy;
use crate::syntax::RustFile;

pub(crate) fn find_unique_type_name_violations(
    files: &[RustFile],
    policy: &Policy,
    graph: &OwnerGraph,
) -> Vec<Finding> {
    let mut reasoned_about = policy
        .named_types()
        .into_iter()
        .map(|(_, name)| name.to_string())
        .collect::<BTreeSet<_>>();
    reasoned_about.extend(construction_only_types(files, policy));
    reasoned_about.extend(graph.owners.iter().cloned());

    graph.types.iter()
        .filter(|(name, info)| reasoned_about.contains(*name) && info.declarations.len() > 1)
        .map(|(name, info)| {
            let mut declarations = info.declarations.clone();
            declarations.sort();
            let (path, line) = &declarations[0];
            let paths = declarations.iter().map(|(path, _)| path.clone()).collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>();
            Finding::new(path, *line, format!("type {name} is declared more than once: {}", paths.join(", ")),
                "type names used by the policy, construction-only capabilities and inferred owners must be unique across crates/")
        }).collect()
}

#[cfg(test)]
#[path = "unique_type_names_tests.rs"]
mod tests;
