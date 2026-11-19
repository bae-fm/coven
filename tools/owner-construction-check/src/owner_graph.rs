//! The field graph of the workspace's declared types, the names its types go
//! by (`type_identity.rs`), and the owners inferred from it, which the policy
//! check reads to hold each database domain's owner to being one.

use crate::syntax::{collect_declared_types, RustFile, StructInfo};
use crate::type_identity::TypeIdentity;
use owner_construction_policy::Policy;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct OwnerGraph {
    pub(crate) identity: TypeIdentity,
    pub(crate) types: BTreeMap<String, StructInfo>,
    pub(crate) owners: BTreeSet<String>,
}

impl OwnerGraph {
    pub(crate) fn collect(files: &[RustFile], policy: &Policy) -> Self {
        let identity = TypeIdentity::of(files);
        let types = collect_declared_types(files, &policy.shared_pointer_types, &identity);
        let owners = infer_owners(&types, policy);
        Self { identity, types, owners }
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
