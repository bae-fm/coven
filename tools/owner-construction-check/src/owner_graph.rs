//! The single field graph and owner inference shared by every owner rule,
//! and the names its types go by (`type_identity.rs`).

use crate::policy::Policy;
use crate::syntax::{collect_declared_types, RustFile, StructInfo};
use crate::type_identity::TypeIdentity;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct OwnerGraph {
    pub(crate) identity: TypeIdentity,
    pub(crate) types: BTreeMap<String, StructInfo>,
    pub(crate) owners: BTreeSet<String>,
}

impl OwnerGraph {
    pub(crate) fn collect(files: &[RustFile], policy: &Policy) -> Self {
        let identity = TypeIdentity::of(files, &policy.phantom_type_arguments);
        let types = collect_declared_types(files, &policy.shared_pointer_types, &identity);
        let owners = infer_owners(&types, policy);
        Self { identity, types, owners }
    }

    pub(crate) fn retained(&self, owner: &str) -> BTreeSet<String> {
        self.retained_around(owner, None)
    }

    /// What `owner` retains other than through `around`: the types its
    /// fields reach without looking inside an `around` they hold.
    pub(crate) fn retained_around(&self, owner: &str, around: Option<&str>) -> BTreeSet<String> {
        let mut fields = BTreeSet::new();
        let mut pending = vec![owner.to_string()];
        while let Some(current) = pending.pop() {
            if let Some(info) = self.types.get(&current) {
                for field in &info.field_types {
                    if fields.insert(field.clone()) && around != Some(field.as_str()) {
                        pending.push(field.clone());
                    }
                }
            }
        }
        fields
    }
}

/// The types that share private state (`Policy::private_state_types`): one
/// holding it through a shared pointer or a reference, and one holding it
/// by value that is itself held so, directly or inside what is. A type that
/// holds a lock no one else reaches keeps it private.
fn sharing_private_state(
    types: &BTreeMap<String, StructInfo>,
    policy: &Policy,
) -> BTreeSet<String> {
    let private = &policy.private_state_types;
    let mut shared = types
        .values()
        .flat_map(|info| info.shared_field_types.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut pending = shared.iter().cloned().collect::<Vec<_>>();
    while let Some(name) = pending.pop() {
        for field in types.get(&name).into_iter().flat_map(|info| &info.field_types) {
            if shared.insert(field.clone()) {
                pending.push(field.clone());
            }
        }
    }
    types
        .iter()
        .filter(|(name, info)| {
            !info.shared_field_types.is_disjoint(private)
                || (shared.contains(*name) && !info.field_types.is_disjoint(private))
        })
        .map(|(name, _)| name.clone())
        .collect()
}

fn infer_owners(structs: &BTreeMap<String, StructInfo>, policy: &Policy) -> BTreeSet<String> {
    let capabilities = policy.held_capabilities();
    let sharing = sharing_private_state(structs, policy);
    let mut owners = BTreeSet::new();
    loop {
        let before = owners.len();
        for (name, info) in structs {
            if policy.non_owner_types.contains(name) || policy.borrowed_facade_types.contains(name)
            {
                continue;
            }
            if sharing.contains(name)
                || info
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
