//! The owner rules' and the read rule's type rows as the checker reads them.
//! The policy names each of those types by its path
//! (`Policy::owner_type_rows`, `ReadShapes`), which the typed lints read.
//! The checker reads some of them: it infers owners from them
//! (`owner_graph.rs`), its read and domain-owner rules follow the shared
//! pointers, and its read rule reads the read rule's types. It matches types
//! by name (`type_identity.rs`), so it reads each row as its path's last
//! segment.

use std::collections::{BTreeMap, BTreeSet};

use owner_construction_policy::{type_name, Policy};

/// Replaces each owner and read type row's path with the type's name.
pub(crate) fn read_by_name(policy: &mut Policy) {
    let lists = [
        &mut policy.capability_types,
        &mut policy.shared_state_types,
        &mut policy.private_state_types,
        &mut policy.shared_pointer_types,
        &mut policy.capability_traits,
        &mut policy.construction_only_capability_types,
        &mut policy.non_owner_types,
        &mut policy.borrowed_facade_types,
        &mut policy.task_types,
        &mut policy.internal_dependency_types,
        &mut policy.closed_session_types,
    ];
    for list in lists {
        *list = list.iter().map(|path| type_name(path).to_string()).collect::<BTreeSet<_>>();
    }
    policy.handles = policy
        .handles
        .iter()
        .map(|(handle, owner)| (type_name(handle).to_string(), type_name(owner).to_string()))
        .collect::<BTreeMap<_, _>>();
    for factory in &mut policy.capability_factories {
        factory.product = type_name(&factory.product).to_string();
    }
    if let Some(read) = &mut policy.read_shapes {
        let lists = [
            &mut read.read_types,
            &mut read.live_query_types,
            &mut read.value_stream_types,
            &mut read.request_query_types,
            &mut read.kept_state_types,
        ];
        for list in lists {
            *list = list.iter().map(|path| type_name(path).to_string()).collect::<BTreeSet<_>>();
        }
    }
}
