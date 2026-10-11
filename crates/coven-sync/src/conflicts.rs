//! §9 and Appendix C11's contradictions, with the replay's preference order.

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

fn membership_state(change: &StoreChange) -> Option<&MemberId> {
    match change {
        StoreChange::AddMember { keys, .. } => Some(&keys.signing),
        StoreChange::RemoveMember { member } | StoreChange::ChangeRole { member, .. } => {
            Some(member)
        }
        _ => None,
    }
}

// These are membership requirements of the change, not its author's authority.
// Authority remains fixed by the recorded past even if the author is removed.
fn required_member<'a>(entry: &'a StoreLogEntry, view: &'a StoreLogCheck) -> Option<&'a MemberId> {
    use StoreChange::*;
    match &entry.change {
        ChangeRole { member, .. }
        | AddCircleMember { member, .. }
        | RemoveCircleMember { member, .. } => Some(member),
        SetAccess { .. } | AddDevice { .. } | CreateCircle { .. } | DeleteCircle { .. } => {
            Some(&entry.author)
        }
        RemoveDevice { .. } => match view {
            StoreLogCheck::DeviceOwner(owner) => Some(owner),
            _ => unreachable!("authorized device removal has an observed owner"),
        },
        _ => None,
    }
}

fn removes_required(a: &StoreLogEntry, b: &StoreLogEntry, vb: &StoreLogCheck) -> bool {
    match &a.change {
        StoreChange::RemoveMember { member } => required_member(b, vb) == Some(member),
        _ => false,
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
        RaiseSchema { snapshot, .. } | Reset { snapshot } => match snapshot.audience {
            Audience::Circle(circle) => Some(circle),
            Audience::Store => None,
        },
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
        (SetAccess { access: x }, SetAccess { access: y }) => a.author == b.author && x == y,
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
        (RemoveMember { member: m }, RemoveMember { member: n }) => m == n,
        (ChangeRole { member: m, role: r }, ChangeRole { member: n, role: s }) => m == n && r == s,
        (AddDevice { device: d, .. }, AddDevice { device: e, .. }) => {
            d == e && a.author == b.author
        }
        (RemoveDevice { device: d }, RemoveDevice { device: e }) => {
            d == e && required_member(a, va) == required_member(b, vb)
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
        (Reset { snapshot: s }, Reset { snapshot: t }) => s == t,
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

fn contradiction(
    a: &StoreLogEntry,
    va: &StoreLogCheck,
    b: &StoreLogEntry,
    vb: &StoreLogCheck,
) -> bool {
    use StoreChange::*;
    let access_membership_and_snapshots = match (&a.change, &b.change) {
        (SetAccess { .. }, SetAccess { .. }) => a.author == b.author,
        (
            AddCircleMember {
                circle: c,
                member: m,
            },
            RemoveCircleMember {
                circle: d,
                member: n,
            },
        )
        | (
            RemoveCircleMember {
                circle: c,
                member: m,
            },
            AddCircleMember {
                circle: d,
                member: n,
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
        ) => s.audience == t.audience && v == w && s != t,
        (Reset { snapshot: s }, Reset { snapshot: t }) => s.audience == t.audience && s != t,
        (Reset { snapshot: s }, RaiseSchema { snapshot: t, .. })
        | (RaiseSchema { snapshot: t, .. }, Reset { snapshot: s }) => s.audience == t.audience,
        _ => false,
    };
    !same_meaning(a, va, b, vb)
        && (same_target(membership_state(&a.change), membership_state(&b.change))
            || same_target(device_target(&a.change), device_target(&b.change))
            || removes_required(a, b, vb)
            || removes_required(b, a, va)
            || access_membership_and_snapshots
            || circle_target(&b.change).is_some_and(|circle| deletes_circle(va, &a.change, circle))
            || circle_target(&a.change).is_some_and(|circle| deletes_circle(vb, &b.change, circle)))
}

pub(crate) fn conflict(
    a: &StoreLogEntry,
    va: &StoreLogCheck,
    b: &StoreLogEntry,
    vb: &StoreLogCheck,
) -> bool {
    a.position != b.position && !had_read(a, b) && !had_read(b, a) && contradiction(a, va, b, vb)
}

#[cfg(test)]
#[path = "conflicts_tests.rs"]
pub(crate) mod tests;
