//! Appendix C's authority, already-in-place and checked effects, with retained data.

use coven_crypto::MemberId;
use coven_database::{
    DropReason, StoreCircle, StoreDevice, StoreIdentity, StoreLogState, StoreMember, StoreVersion,
};
use coven_format::store_log::{MemberRole, StoreChange, StoreLogEntry};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::Audience;

pub(crate) fn member<'a>(state: &'a StoreLogState, id: &MemberId) -> Option<&'a StoreMember> {
    state.members.get(id).filter(|member| !member.removed)
}

pub(crate) fn device(state: &StoreLogState, id: DeviceId) -> Option<&StoreDevice> {
    state.devices.get(&id).filter(|device| !device.removed)
}

pub(crate) fn circle(state: &StoreLogState, id: CircleId) -> Option<&StoreCircle> {
    state.circles.get(&id).filter(|circle| !circle.deleted)
}

fn admin(state: &StoreLogState, id: &MemberId) -> bool {
    member(state, id).is_some_and(|member| member.role == MemberRole::Admin)
}

fn in_circle(state: &StoreLogState, id: CircleId, who: &MemberId) -> bool {
    member(state, who).is_some()
        && circle(state, id).is_some_and(|circle| circle.members.contains(who))
}

pub(crate) fn authorize(view: &StoreLogState, entry: &StoreLogEntry) -> Result<(), DropReason> {
    use StoreChange::*;
    let author = &entry.author;
    let allowed = match &entry.change {
        CreateStore { .. } => view.store.is_none(),
        AddMember { .. } | RemoveMember { .. } | ChangeRole { .. } => admin(view, author),
        AddDevice { .. } | CreateCircle { .. } => member(view, author).is_some(),
        RaiseSchema { snapshot, .. } | RaiseFormat { snapshot, .. } => match snapshot.audience {
            Audience::Store => member(view, author).is_some(),
            Audience::Circle(circle) => in_circle(view, circle, author),
        },
        RemoveDevice { device: id } => device(view, *id).is_some_and(|device| {
            member(view, author).is_some() && (device.member == *author || admin(view, author))
        }),
        RenameCircle { circle, .. }
        | DeleteCircle { circle }
        | AddCircleMember { circle, .. }
        | RemoveCircleMember { circle, .. } => in_circle(view, *circle, author),
        Reset { snapshot } => match snapshot.audience {
            Audience::Store => admin(view, author),
            Audience::Circle(circle) => in_circle(view, circle, author),
        },
    };
    if !allowed {
        return Err(DropReason::NotAllowed);
    }
    // The model assumes truthful lists at its boundary. Real entries must
    // prove that list against the same author view, even for an unchanged effect.
    if let RemoveMember {
        member,
        circle_keys,
        ..
    } = &entry.change
    {
        let expected = view.circles.iter().filter_map(|(id, circle)| {
            (!circle.deleted && circle.members.contains(member) && circle.members.len() > 1)
                .then_some(*id)
        });
        if !expected.eq(circle_keys.iter().map(|key| key.circle)) {
            return Err(DropReason::WrongCircleKeys);
        }
    }
    Ok(())
}

pub(crate) fn already_in_place(state: &StoreLogState, entry: &StoreLogEntry) -> bool {
    use StoreChange::*;
    match &entry.change {
        CreateStore { .. } => state.store.is_some(),
        AddMember { keys, role } => member(state, &keys.signing).is_some_and(|m| m.role == *role),
        ChangeRole { member: id, role } => member(state, id).is_some_and(|m| m.role == *role),
        RemoveMember { member: id, .. } => member(state, id).is_none(),
        AddDevice { device: id, .. } => {
            device(state, *id).is_some_and(|d| d.member == entry.author)
        }
        RemoveDevice { device: id } => device(state, *id).is_none(),
        CreateCircle {
            circle: id, name, ..
        } => circle(state, *id).is_some_and(|c| {
            c.name == *name && c.members.len() == 1 && c.members.contains(&entry.author)
        }),
        RenameCircle { circle: id, name } => circle(state, *id).is_some_and(|c| c.name == *name),
        DeleteCircle { circle: id } => circle(state, *id).is_none(),
        AddCircleMember { circle, member } => in_circle(state, *circle, member),
        RemoveCircleMember { circle, member, .. } => !in_circle(state, *circle, member),
        RaiseSchema { version, snapshot } => {
            state.schema.get(&snapshot.audience).is_some_and(|v| {
                *version < v.number || (*version == v.number && *snapshot == v.snapshot)
            })
        }
        RaiseFormat { version, snapshot } => {
            state.format.get(&snapshot.audience).is_some_and(|v| {
                *version < v.number || (*version == v.number && *snapshot == v.snapshot)
            })
        }
        Reset { snapshot } => state.resets.get(&snapshot.audience) == Some(snapshot),
    }
}

pub(crate) fn effect(
    state: &StoreLogState,
    view: &StoreLogState,
    entry: &StoreLogEntry,
) -> Result<StoreLogState, DropReason> {
    use StoreChange::*;
    let mut next = state.clone();
    if let RaiseSchema { snapshot, .. } | RaiseFormat { snapshot, .. } | Reset { snapshot } =
        &entry.change
    {
        let exists = match snapshot.audience {
            Audience::Store => state.store.is_some(),
            Audience::Circle(id) => circle(state, id).is_some(),
        };
        if !exists {
            return Err(DropReason::TargetGone);
        }
    }
    match &entry.change {
        CreateStore {
            store,
            name,
            admin,
            key,
            device_name,
        } => {
            if state.store.is_some() {
                return Err(DropReason::TargetGone);
            }
            next = StoreLogState::default();
            next.store = Some(StoreIdentity {
                id: *store,
                name: name.clone(),
                key: *key,
            });
            next.members.insert(
                entry.author.clone(),
                StoreMember {
                    sealing: admin.sealing,
                    role: MemberRole::Admin,
                    removed: false,
                },
            );
            next.devices.insert(
                entry.position.device,
                StoreDevice {
                    member: entry.author.clone(),
                    name: device_name.clone(),
                    removed: false,
                },
            );
        }
        AddMember { keys, role } => {
            if state.store.is_none() {
                return Err(DropReason::TargetGone);
            }
            next.members.insert(
                keys.signing.clone(),
                StoreMember {
                    sealing: keys.sealing,
                    role: *role,
                    removed: false,
                },
            );
        }
        RemoveMember {
            member: id,
            key,
            circle_keys,
        } => {
            if member(state, id).is_none() {
                return Err(DropReason::TargetGone);
            }
            next.members.get_mut(id).expect("checked member").removed = true;
            next.store.as_mut().expect("members require creation").key = *key;
            for device in next.devices.values_mut().filter(|d| d.member == *id) {
                device.removed = true;
            }
            for circle in next.circles.values_mut().filter(|c| !c.deleted) {
                circle.members.remove(id);
                circle.deleted = circle.members.is_empty();
            }
            for replacement in circle_keys {
                if let Some(circle) = next.circles.get_mut(&replacement.circle) {
                    circle.key = replacement.key;
                }
            }
        }
        ChangeRole { member: id, role } => {
            if member(state, id).is_none() {
                return Err(DropReason::TargetGone);
            }
            next.members.get_mut(id).expect("checked member").role = *role;
        }
        AddDevice { device: id, name } => {
            if member(state, &entry.author).is_none() || device(state, *id).is_some() {
                return Err(DropReason::TargetGone);
            }
            next.devices.insert(
                *id,
                StoreDevice {
                    member: entry.author.clone(),
                    name: name.clone(),
                    removed: false,
                },
            );
        }
        RemoveDevice { device: id } => {
            let owner = &device(view, *id)
                .expect("authorized removal has an observed owner")
                .member;
            if device(state, *id).is_none_or(|d| d.member != *owner) {
                return Err(DropReason::TargetGone);
            }
            next.devices.get_mut(id).expect("checked device").removed = true;
        }
        CreateCircle {
            circle: id,
            name,
            key,
        } => {
            if member(state, &entry.author).is_none() || circle(state, *id).is_some() {
                return Err(DropReason::TargetGone);
            }
            next.circles.insert(
                *id,
                StoreCircle {
                    name: name.clone(),
                    key: *key,
                    deleted: false,
                    members: [entry.author.clone()].into(),
                },
            );
        }
        RenameCircle { circle: id, name } => {
            if circle(state, *id).is_none() {
                return Err(DropReason::TargetGone);
            }
            next.circles.get_mut(id).expect("checked circle").name = name.clone();
        }
        DeleteCircle { circle: id } => {
            if circle(state, *id).is_none() {
                return Err(DropReason::TargetGone);
            }
            let circle = next.circles.get_mut(id).expect("checked circle");
            circle.deleted = true;
            circle.members.clear();
        }
        AddCircleMember {
            circle: id,
            member: who,
        } => {
            if circle(state, *id).is_none() || member(state, who).is_none() {
                return Err(DropReason::TargetGone);
            }
            next.circles
                .get_mut(id)
                .expect("checked circle")
                .members
                .insert(who.clone());
        }
        RemoveCircleMember {
            circle: id,
            member: who,
            key,
        } => {
            if !circle(state, *id).is_some_and(|c| c.members.contains(who)) {
                return Err(DropReason::TargetGone);
            }
            let circle = next.circles.get_mut(id).expect("checked circle");
            circle.members.remove(who);
            circle.key = *key;
            circle.deleted = circle.members.is_empty();
        }
        RaiseSchema { version, snapshot } => {
            next.schema.insert(
                snapshot.audience.clone(),
                StoreVersion {
                    number: *version,
                    snapshot: snapshot.clone(),
                    entry: entry.position,
                },
            );
        }
        RaiseFormat { version, snapshot } => {
            next.format.insert(
                snapshot.audience.clone(),
                StoreVersion {
                    number: *version,
                    snapshot: snapshot.clone(),
                    entry: entry.position,
                },
            );
        }
        Reset { snapshot } => {
            next.resets
                .insert(snapshot.audience.clone(), snapshot.clone());
        }
    }
    if next.store.is_some()
        && !next
            .members
            .values()
            .any(|member| !member.removed && member.role == MemberRole::Admin)
    {
        return Err(DropReason::NoAdminLeft);
    }
    Ok(next)
}

#[cfg(test)]
#[path = "effects_tests.rs"]
pub(crate) mod tests;
