//! The single field graph and owner inference shared by every owner rule.

use crate::policy::Policy;
use crate::syntax::{collect_declared_types, RustFile, StructInfo};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct OwnerGraph {
    pub(crate) types: BTreeMap<String, StructInfo>,
    pub(crate) owners: BTreeSet<String>,
}

impl OwnerGraph {
    pub(crate) fn collect(files: &[RustFile], policy: &Policy) -> Self {
        let types = collect_declared_types(files);
        let owners = infer_owners(&types, policy);
        Self { types, owners }
    }

    pub(crate) fn retained(&self, owner: &str) -> BTreeSet<String> {
        let mut fields = BTreeSet::new();
        let mut pending = vec![owner.to_string()];
        while let Some(current) = pending.pop() {
            if let Some(info) = self.types.get(&current) {
                for field in &info.field_types {
                    if fields.insert(field.clone()) {
                        pending.push(field.clone());
                    }
                }
            }
        }
        fields
    }
}

fn infer_owners(structs: &BTreeMap<String, StructInfo>, policy: &Policy) -> BTreeSet<String> {
    let capabilities = policy
        .capability_types
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();
    let mut owners = BTreeSet::new();
    loop {
        let before = owners.len();
        for (name, info) in structs {
            if policy.non_owner_types.contains(&name.as_str())
                || policy.borrowed_facade_types.contains(&name.as_str())
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

#[cfg(test)]
#[path = "owner_graph_tests.rs"]
mod tests;
