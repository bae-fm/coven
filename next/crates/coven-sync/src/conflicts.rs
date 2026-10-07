//! Appendix C's conflict predicates and total preference order.

use coven_crypto::MemberId;
use coven_database::StoreLogCheck;
use coven_format::store_log::{MemberRole, StoreChange, StoreLogEntry};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::Audience;

use crate::replay::had_read;

fn priority(change: &StoreChange) -> u8 {
    use StoreChange::*;
    match change {
        RemoveMember { .. }
        | RemoveDevice { .. }
        | DeleteCircle { .. }
        | RemoveCircleMember { .. } => 0,
        AddMember {
            role: MemberRole::Admin,
            ..
        }
        | ChangeRole {
            role: MemberRole::Admin,
            ..
        } => 2,
        _ => 1,
    }
}

pub(crate) fn before(a: &StoreLogEntry, b: &StoreLogEntry) -> bool {
    (priority(&a.change), a.timestamp) < (priority(&b.change), b.timestamp)
}

fn member_target<'a>(entry: &'a StoreLogEntry, view: &'a StoreLogCheck) -> Option<&'a MemberId> {
    use StoreChange::*;
    match &entry.change {
        AddMember { keys, .. } => Some(&keys.signing),
        RemoveMember { member, .. }
        | ChangeRole { member, .. }
        | AddCircleMember { member, .. }
        | RemoveCircleMember { member, .. } => Some(member),
        AddDevice { .. } => Some(&entry.author),
        RemoveDevice { .. } => match view {
            StoreLogCheck::DeviceOwner(owner) => Some(owner),
            _ => unreachable!("authorized device removal has an observed owner"),
        },
        _ => None,
    }
}

fn device_target(change: &StoreChange) -> Option<DeviceId> {
    match change {
        StoreChange::AddDevice { device, .. } | StoreChange::RemoveDevice { device } => {
            Some(*device)
        }
        _ => None,
    }
}

fn circle_target(change: &StoreChange) -> Option<CircleId> {
    use StoreChange::*;
    match change {
        CreateCircle { circle, .. }
        | RenameCircle { circle, .. }
        | DeleteCircle { circle }
        | AddCircleMember { circle, .. }
        | RemoveCircleMember { circle, .. } => Some(*circle),
        RaiseSchema { snapshot, .. } | RaiseFormat { snapshot, .. } | Reset { snapshot } => {
            match snapshot.audience {
                Audience::Circle(circle) => Some(circle),
                Audience::Store => None,
            }
        }
        _ => None,
    }
}

fn same_target<T: PartialEq>(a: Option<T>, b: Option<T>) -> bool {
    matches!((a,b), (Some(a),Some(b)) if a==b)
}

// Keys and device names are carried by effective additions and rotations.
// The model's meaning compares the membership changes themselves; its
// already-in-place rule keeps repeated additions without replacing their data.
fn same_meaning(
    a: &StoreLogEntry,
    va: &StoreLogCheck,
    b: &StoreLogEntry,
    vb: &StoreLogCheck,
) -> bool {
    use StoreChange::*;
    match (&a.change, &b.change) {
        (CreateStore { .. }, CreateStore { .. }) => true,
        (
            AddMember {
                keys: m, role: r, ..
            },
            AddMember {
                keys: n, role: s, ..
            },
        ) => m.signing == n.signing && r == s,
        (AddMember { keys, role: r, .. }, ChangeRole { member, role: s })
        | (ChangeRole { member, role: s }, AddMember { keys, role: r, .. }) => {
            keys.signing == *member && r == s
        }
        (
            RemoveMember {
                member: m,
                circle_keys: c,
                ..
            },
            RemoveMember {
                member: n,
                circle_keys: d,
                ..
            },
        ) => m == n && c.iter().map(|k| k.circle).eq(d.iter().map(|k| k.circle)),
        (ChangeRole { member: m, role: r }, ChangeRole { member: n, role: s }) => m == n && r == s,
        (AddDevice { device: d, .. }, AddDevice { device: e, .. }) => {
            d == e && a.author == b.author
        }
        (RemoveDevice { device: d }, RemoveDevice { device: e }) => {
            d == e && member_target(a, va) == member_target(b, vb)
        }
        (
            CreateCircle {
                circle: c, name: x, ..
            },
            CreateCircle {
                circle: d, name: y, ..
            },
        ) => c == d && x == y && a.author == b.author,
        (RenameCircle { circle: c, name: x }, RenameCircle { circle: d, name: y }) => {
            c == d && x == y
        }
        (DeleteCircle { circle: c }, DeleteCircle { circle: d }) => c == d,
        (
            AddCircleMember {
                circle: c,
                member: m,
            },
            AddCircleMember {
                circle: d,
                member: n,
            },
        )
        | (
            RemoveCircleMember {
                circle: c,
                member: m,
                ..
            },
            RemoveCircleMember {
                circle: d,
                member: n,
                ..
            },
        ) => c == d && m == n,
        (
            RaiseSchema {
                version: v,
                snapshot: s,
            },
            RaiseSchema {
                version: w,
                snapshot: t,
            },
        ) => v == w && s == t,
        (
            RaiseFormat {
                version: v,
                snapshot: s,
            },
            RaiseFormat {
                version: w,
                snapshot: t,
            },
        ) => v == w && s == t,
        (Reset { snapshot: s }, Reset { snapshot: t }) => s == t,
        _ => false,
    }
}

fn replaces_key(change: &StoreChange, circle: CircleId) -> bool {
    match change {
        StoreChange::RemoveCircleMember { circle: id, .. } => *id == circle,
        StoreChange::RemoveMember { circle_keys, .. } => {
            circle_keys.iter().any(|key| key.circle == circle)
        }
        _ => false,
    }
}

fn deletes_circle(view: &StoreLogCheck, change: &StoreChange, circle: CircleId) -> bool {
    use StoreChange::*;
    match change {
        DeleteCircle { circle: id } => *id == circle,
        RemoveMember { .. } | RemoveCircleMember { .. } => match view {
            StoreLogCheck::DeletedCircles(circles) => circles.contains(&circle),
            _ => unreachable!("authorized member removal records circle deletions"),
        },
        _ => false,
    }
}

fn circle_name(change: &StoreChange) -> Option<(CircleId, &str)> {
    match change {
        StoreChange::CreateCircle { circle, name, .. }
        | StoreChange::RenameCircle { circle, name } => Some((*circle, name)),
        _ => None,
    }
}

fn special(va: &StoreLogCheck, a: &StoreChange, vb: &StoreLogCheck, b: &StoreChange) -> bool {
    use StoreChange::*;
    let keys_and_snapshots = match (a, b) {
        (AddMember { .. }, RemoveMember { .. }) | (RemoveMember { .. }, AddMember { .. }) => true,
        (RemoveMember { .. }, RemoveMember { .. }) => true,
        (AddCircleMember { circle, .. }, _) => replaces_key(b, *circle),
        (_, AddCircleMember { circle, .. }) => replaces_key(a, *circle),
        (RemoveCircleMember { circle, .. }, _) => replaces_key(b, *circle),
        (_, RemoveCircleMember { circle, .. }) => replaces_key(a, *circle),
        (
            RaiseSchema {
                version: v,
                snapshot: s,
            },
            RaiseSchema {
                version: w,
                snapshot: t,
            },
        ) => s.audience == t.audience && v == w && s != t,
        (
            RaiseFormat {
                version: v,
                snapshot: s,
            },
            RaiseFormat {
                version: w,
                snapshot: t,
            },
        ) => s.audience == t.audience && v == w && s != t,
        (Reset { snapshot: s }, Reset { snapshot: t }) => s.audience == t.audience && s != t,
        (
            Reset { snapshot: s },
            RaiseSchema { snapshot: t, .. } | RaiseFormat { snapshot: t, .. },
        )
        | (
            RaiseSchema { snapshot: t, .. } | RaiseFormat { snapshot: t, .. },
            Reset { snapshot: s },
        ) => s.audience == t.audience,
        _ => false,
    };
    keys_and_snapshots
        || circle_target(b).is_some_and(|circle| deletes_circle(va, a, circle))
        || circle_target(a).is_some_and(|circle| deletes_circle(vb, b, circle))
        || matches!((circle_name(a), circle_name(b)), (Some((c,x)),Some((d,y))) if c==d && x!=y)
}

pub(crate) fn conflict(
    a: &StoreLogEntry,
    va: &StoreLogCheck,
    b: &StoreLogEntry,
    vb: &StoreLogCheck,
) -> bool {
    a.position != b.position
        && !had_read(a, b)
        && !had_read(b, a)
        && !same_meaning(a, va, b, vb)
        && (same_target(member_target(a, va), member_target(b, vb))
            || same_target(device_target(&a.change), device_target(&b.change))
            || special(va, &a.change, vb, &b.change))
}

#[cfg(test)]
#[path = "conflicts_tests.rs"]
mod tests;
