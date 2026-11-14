//! What a circle's own entries do once `checked_change` has admitted them, and
//! what a member's removal from the store does to the circles it was in
//! (§15.5, §15.8, §15.9). A circle left empty is deleted at that slot, whether
//! its last member left it or was removed from the store.

use crate::{EntryTarget, OperationRejection};
use coven_database::{StoreCircle, StoreState};
use coven_foundation::id_source::{CircleId, MemberId};
use coven_merge::Slot;

/// Make `circle` with `author` its first member. An id the state already
/// holds, deleted or not, is gone: a deleted circle is never made again.
pub(crate) fn create(
    next: &mut StoreState,
    circle: CircleId,
    name: &str,
    author: MemberId,
    base: u32,
) -> Result<(), OperationRejection> {
    if next.circles.contains_key(&circle) {
        return Err(OperationRejection::TargetGone(EntryTarget::Circle(circle)));
    }
    next.circles.insert(circle, StoreCircle { name: name.into(), members: [author].into(), base });
    Ok(())
}

/// Whether live `circle` is the one an earlier application of the same
/// admission made for `member`: it holds the member, under that name.
pub(crate) fn made_for(
    state: &StoreState,
    circle: CircleId,
    member: &MemberId,
    name: &str,
) -> bool {
    !state.circle_deletions.contains_key(&circle)
        && state
            .circles
            .get(&circle)
            .is_some_and(|record| record.members.contains(member) && record.name == name)
}

/// Rename live `circle`; false when it already has that name.
pub(crate) fn rename(next: &mut StoreState, circle: CircleId, name: &str) -> bool {
    let record = next.circles.get_mut(&circle).expect("a live circle");
    if record.name == name {
        return false;
    }
    record.name = name.into();
    true
}

/// Delete live `circle` at `slot`; it keeps its name and loses its members.
pub(crate) fn delete(next: &mut StoreState, circle: CircleId, slot: Slot) {
    next.circles.get_mut(&circle).expect("a live circle").members.clear();
    next.circle_deletions.insert(circle, slot);
}

/// Add `member`, who must still be in the store, to live `circle`; false when
/// it is already there.
pub(crate) fn add(
    next: &mut StoreState,
    circle: CircleId,
    member: MemberId,
) -> Result<bool, OperationRejection> {
    if next.active_member(&member).is_none() {
        return Err(OperationRejection::TargetGone(EntryTarget::Member(member)));
    }
    Ok(next.circles.get_mut(&circle).expect("a live circle").members.insert(member))
}

/// Take `member` out of live `circle` at `slot`, as `author`'s entry; false
/// when it isn't there. The entry introduces the circle's replacement key,
/// which its author makes, so an author removing themselves while others stay
/// would know the key meant to shut them out: that is refused (§15.8).
pub(crate) fn remove(
    next: &mut StoreState,
    circle: CircleId,
    member: &MemberId,
    author: &MemberId,
    slot: Slot,
) -> Result<bool, OperationRejection> {
    let members = &next.circles[&circle].members;
    if !members.contains(member) {
        return Ok(false);
    }
    if member == author && members.len() > 1 {
        return Err(OperationRejection::NotAllowed);
    }
    leave(next, circle, member, slot);
    Ok(true)
}

/// The store's removal of `member` at `slot` takes it out of every live circle.
pub(crate) fn leave_all(next: &mut StoreState, member: &MemberId, slot: Slot) {
    let live: Vec<CircleId> = next
        .circles
        .keys()
        .filter(|circle| !next.circle_deletions.contains_key(circle))
        .copied()
        .collect();
    for circle in live {
        leave(next, circle, member, slot);
    }
}

/// `member` leaves live `circle` at `slot`; a circle it leaves empty is deleted
/// there.
fn leave(next: &mut StoreState, circle: CircleId, member: &MemberId, slot: Slot) {
    let record = next.circles.get_mut(&circle).expect("a live circle");
    record.members.remove(member);
    if record.members.is_empty() {
        next.circle_deletions.insert(circle, slot);
    }
}

#[cfg(test)]
#[path = "circle_effects_tests.rs"]
mod tests;
