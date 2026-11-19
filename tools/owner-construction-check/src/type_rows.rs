//! The type rows the checker reads by name. The policy names each type of
//! the owner rules and each domain's owner by its path
//! (`Policy::owner_type_rows`), which the typed lints read. The checker reads
//! two of those rows: the type graph follows the shared pointers
//! (`type_graph.rs`), and the database rule finds each domain's writes in
//! its owner's methods and the domain-owner rule its forwarding wrappers. It
//! matches types by name (`type_identity.rs`), so it reads each row as its
//! path's last segment.

use std::collections::BTreeSet;

use owner_construction_policy::{type_name, Policy};

/// Replaces the shared pointers' and the domain owners' paths with the
/// types' names.
pub(crate) fn read_by_name(policy: &mut Policy) {
    policy.shared_pointer_types = policy
        .shared_pointer_types
        .iter()
        .map(|path| type_name(path).to_string())
        .collect::<BTreeSet<_>>();
    for domain in policy.database.iter_mut().flat_map(|database| &mut database.domains) {
        domain.owner = type_name(&domain.owner).to_string();
    }
}
