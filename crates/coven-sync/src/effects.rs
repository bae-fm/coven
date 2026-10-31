//! Administrative authority and effects at one preceding sequence prefix (§10).

use crate::{EntryTarget, OperationRejection};
use coven_database::{
    AdminEffect, Boundary, DeviceState, StoreCircle, StoreDevice, StoreIdentity, StoreMember,
    StoreState, StoreVersion,
};
use coven_format::{
    admin::{MemberRole, StoreChange},
    sequence::{Purpose, SlotHeader, SnapshotAnnouncement},
};
use coven_foundation::id_source::MemberId;
use coven_merge::{Audience, Slot};
use std::collections::BTreeMap;

pub(crate) fn member<'a>(state: &'a StoreState, id: &MemberId) -> Option<&'a StoreMember> {
    state.members.get(id).filter(|member| !member.removed)
}

/// Whether this member's removal has applied: the person left the store (§14.1).
pub(crate) fn left(state: &StoreState, id: &MemberId) -> bool {
    state.members.get(id).is_some_and(|member| member.removed)
}

pub(crate) fn belongs(state: &StoreState, audience: &Audience, id: &MemberId) -> bool {
    member(state, id).is_some()
        && match audience {
            Audience::Store => true,
            Audience::Circle(id_circle) => state.reads_circle(*id_circle, id),
        }
}

fn admin(state: &StoreState, id: &MemberId) -> bool {
    member(state, id).is_some_and(|member| member.role == MemberRole::Admin)
}

fn fresh(state: &StoreState, keys: &coven_crypto::MemberPublicKeys) -> bool {
    state
        .bound_keys
        .iter()
        .all(|bound| bound.signing != keys.signing && bound.sealing != keys.sealing)
}

/// Resolve an activation through its earlier authenticated announcement.
fn boundary(
    change: &StoreChange,
    slot: Slot,
    announcements: &BTreeMap<Slot, SnapshotAnnouncement>,
) -> Result<Option<Boundary>, OperationRejection> {
    let (at, purpose) = match change {
        StoreChange::RaiseSchema { version, announcement } => {
            (*announcement, Purpose::Raise(*version))
        }
        StoreChange::Reset { announcement } => (*announcement, Purpose::Reset),
        _ => return Ok(None),
    };
    let announced = announcements
        .get(&at)
        .filter(|a| at < slot && a.purpose == purpose)
        .ok_or(OperationRejection::NotAllowed)?;
    Ok(Some(match purpose {
        Purpose::Raise(number) => Boundary::SchemaChange(StoreVersion {
            number,
            snapshot: announced.snapshot.clone(),
            entry: slot,
        }),
        Purpose::Reset => Boundary::Reset { entry: slot, snapshot: announced.snapshot.clone() },
        Purpose::Ordinary => unreachable!("activation selected a preparation"),
    }))
}

/// Validate once and produce the state that commits with the slot. A satisfied
/// request has no effect; an unauthorized object never supplies an effect.
pub(crate) fn checked_change(
    state: &StoreState,
    header: &SlotHeader,
    slot: Slot,
    change: &StoreChange,
    announcements: &BTreeMap<Slot, SnapshotAnnouncement>,
    boundaries: &[coven_database::BoundaryCoverage],
) -> Result<Option<AdminEffect>, OperationRejection> {
    use OperationRejection::{NotAllowed, TargetGone};
    use StoreChange::*;
    let author = &header.author;
    let writer = header.device;
    let boundary = boundary(change, slot, announcements)?;
    let active = member(state, author).is_some();
    let is_admin = admin(state, author);
    let permitted = match change {
        CreateStore { .. } => slot == Slot(1) && state.store.is_none(),
        AddDevice { .. } => active,
        _ => {
            active && state.devices.get(&writer).is_some_and(|d| d.active() && d.member == *author)
        }
    };
    if !permitted {
        return Err(NotAllowed);
    }
    if let DeleteCircle { circle } = change {
        if state.circle_deletions.contains_key(circle) {
            return Ok(None);
        }
    }
    let authorized = match change {
        CreateStore { .. } | AddDevice { .. } | CreateCircle { .. } => true,
        RenameStore { .. } | AddMember { .. } | RemoveMember { .. } | ChangeRole { .. } => is_admin,
        SetAccess { member, access } => {
            member == author
                || (is_admin && matches!(access, coven_format::MemberAccess::S3AccessKey { .. }))
        }
        ReplaceMemberKeys { member, .. } => member == author || is_admin,
        RemoveDevice { device } => {
            state.devices.get(device).is_some_and(|d| d.member == *author || is_admin)
        }
        RenameCircle { circle, .. }
        | DeleteCircle { circle }
        | AddCircleMember { circle, .. }
        | RemoveCircleMember { circle, .. } => state.reads_circle(*circle, author),
        RotateKey { audience, .. } => belongs(state, audience, author),
        RaiseSchema { .. } => {
            belongs(state, &boundary.as_ref().expect("resolved raise").snapshot().audience, author)
        }
        Reset { .. } => match &boundary.as_ref().expect("resolved reset").snapshot().audience {
            Audience::Store => is_admin,
            audience => belongs(state, audience, author),
        },
    };
    if !authorized {
        return Err(NotAllowed);
    }
    let mut next = state.clone();
    match change {
        CreateStore { store, name, admin, access, device_name, .. } => {
            next.store = Some(StoreIdentity { id: *store, name: name.clone(), named_at: slot });
            next.members.insert(
                *author,
                StoreMember {
                    keys: *admin,
                    access: access.clone(),
                    role: MemberRole::Admin,
                    removed: false,
                },
            );
            next.bound_keys.insert(*admin);
            next.devices.insert(
                writer,
                StoreDevice {
                    member: *author,
                    name: device_name.clone(),
                    state: DeviceState::Active,
                },
            );
        }
        RenameStore { name } => {
            let store = next.store.as_mut().expect("authorized store");
            if store.name == *name {
                return Ok(None);
            }
            store.name = name.clone();
            store.named_at = slot;
        }
        AddMember { member: id, keys, role, access } => {
            if let Some(existing) = state.members.get(id) {
                if !existing.removed
                    && existing.keys == *keys
                    && existing.role == *role
                    && existing.access == *access
                {
                    return Ok(None);
                }
                return Err(TargetGone(EntryTarget::Member(*id)));
            }
            if !fresh(state, keys) {
                return Err(OperationRejection::KeysAlreadyUsed);
            }
            next.members.insert(
                *id,
                StoreMember { keys: *keys, access: access.clone(), role: *role, removed: false },
            );
            next.bound_keys.insert(*keys);
        }
        RemoveMember { member: id } => {
            let target = next.members.get_mut(id).ok_or(TargetGone(EntryTarget::Member(*id)))?;
            if target.removed {
                return Ok(None);
            }
            target.removed = true;
            for device in next.devices.values_mut().filter(|device| device.member == *id) {
                device.state = DeviceState::Removed;
            }
            for (circle_id, circle) in &mut next.circles {
                if !next.circle_deletions.contains_key(circle_id) {
                    circle.members.remove(id);
                    if circle.members.is_empty() {
                        next.circle_deletions.insert(*circle_id, slot);
                    }
                }
            }
        }
        ChangeRole { member: id, role } => {
            let target = next
                .members
                .get_mut(id)
                .filter(|m| !m.removed)
                .ok_or(TargetGone(EntryTarget::Member(*id)))?;
            if target.role == *role {
                return Ok(None);
            }
            target.role = *role;
        }
        SetAccess { member: id, access } => {
            let target = next
                .members
                .get_mut(id)
                .filter(|m| !m.removed)
                .ok_or(TargetGone(EntryTarget::Member(*id)))?;
            if target.access.same_credential(access) {
                return Ok(None);
            }
            target.access = access.clone();
        }
        ReplaceMemberKeys { member: id, expected, keys } => {
            let target = member(state, id).ok_or(TargetGone(EntryTarget::Member(*id)))?;
            if target.keys != *expected {
                return Err(OperationRejection::MemberKeysChanged(*id));
            }
            if !fresh(state, keys) {
                return Err(OperationRejection::KeysAlreadyUsed);
            }
            next.members.get_mut(id).expect("checked target").keys = *keys;
            next.bound_keys.insert(*keys);
        }
        AddDevice { name, replaces } => {
            if state.devices.contains_key(&writer) {
                return Err(TargetGone(EntryTarget::Device(writer)));
            }
            if let Some(old) = replaces {
                let previous =
                    next.devices.get_mut(old).ok_or(TargetGone(EntryTarget::Device(*old)))?;
                if previous.member != *author || *old == writer {
                    return Err(NotAllowed);
                }
                if previous.active() {
                    previous.state = DeviceState::Replaced;
                }
            }
            next.devices.insert(
                writer,
                StoreDevice { member: *author, name: name.clone(), state: DeviceState::Active },
            );
        }
        RemoveDevice { device } => {
            let target =
                next.devices.get_mut(device).ok_or(TargetGone(EntryTarget::Device(*device)))?;
            if target.removed() {
                return Ok(None);
            }
            target.state = DeviceState::Removed;
        }
        CreateCircle { circle, name, .. } => {
            if state.circles.contains_key(circle) {
                return Err(TargetGone(EntryTarget::Circle(*circle)));
            }
            next.circles
                .insert(*circle, StoreCircle { name: name.clone(), members: [*author].into() });
        }
        RenameCircle { circle, name } => {
            let target = next.circles.get_mut(circle).expect("authorized circle");
            if target.name == *name {
                return Ok(None);
            }
            target.name = name.clone();
        }
        DeleteCircle { circle } => {
            next.circles.get_mut(circle).expect("authorized circle").members.clear();
            next.circle_deletions.insert(*circle, slot);
        }
        AddCircleMember { circle, member: id } => {
            if member(state, id).is_none() {
                return Err(TargetGone(EntryTarget::Member(*id)));
            }
            if !next.circles.get_mut(circle).expect("authorized circle").members.insert(*id) {
                return Ok(None);
            }
        }
        RemoveCircleMember { circle, member: id } => {
            let target = next.circles.get_mut(circle).expect("authorized circle");
            if !target.members.remove(id) {
                return Ok(None);
            }
            if target.members.is_empty() {
                next.circle_deletions.insert(*circle, slot);
            }
        }
        RotateKey { .. } => {}
        RaiseSchema { .. } | Reset { .. } => {
            let boundary = boundary.as_ref().expect("resolved activation");
            if boundary.already_in_place(state) {
                return Ok(None);
            }
            let announcement = match change {
                RaiseSchema { announcement, .. } | Reset { announcement } => {
                    &announcements[announcement]
                }
                _ => unreachable!("resolved boundary"),
            };
            if announcements.values().any(|prior| {
                prior.snapshot.audience == announcement.snapshot.audience
                    && prior.covers > announcement.covers
            }) || boundaries.iter().any(|prior| {
                prior.boundary.snapshot().audience == announcement.snapshot.audience
                    && prior.boundary.entry().0 > announcement.covers
            }) {
                return Err(NotAllowed);
            }
            boundary.apply(&mut next);
        }
    }
    if !next.members.values().any(|m| !m.removed && m.role == MemberRole::Admin) {
        return Err(OperationRejection::NoAdminLeft);
    }
    Ok(Some(AdminEffect {
        change: change.clone(),
        state: next,
        boundary,
        reload: Vec::new(),
        provider: Default::default(),
    }))
}

#[cfg(test)]
#[path = "effects_tests.rs"]
mod tests;
