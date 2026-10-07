//! Appendix C's authority, already-in-place and checked effects, with retained data.

use coven_crypto::MemberId;
use coven_database::{
    DropReason, StoreCircle, StoreDevice, StoreIdentity, StoreLogCheck, StoreLogState, StoreMember,
    StoreVersion,
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

pub(crate) fn check(view: &StoreLogState, entry: &StoreLogEntry) -> StoreLogCheck {
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
        return StoreLogCheck::NotAllowed;
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
            return StoreLogCheck::WrongCircleKeys;
        }
    }
    match &entry.change {
        RemoveDevice { device: id } => StoreLogCheck::DeviceOwner(
            device(view, *id)
                .expect("authorized device removal")
                .member
                .clone(),
        ),
        RemoveMember { member, .. } | RemoveCircleMember { member, .. } => {
            let circles = view
                .circles
                .iter()
                .filter_map(|(id, circle)| {
                    let targets = match &entry.change {
                        RemoveCircleMember { circle, .. } => circle == id,
                        _ => true,
                    };
                    (targets
                        && !circle.deleted
                        && circle.members.len() == 1
                        && circle.members.contains(member))
                    .then_some(*id)
                })
                .collect();
            StoreLogCheck::DeletedCircles(circles)
        }
        _ => StoreLogCheck::Allowed,
    }
}

pub(crate) fn already_in_place(state: &StoreLogState, entry: &StoreLogEntry) -> bool {
    use StoreChange::*;
    match &entry.change {
        CreateStore { .. } => state.store.is_some(),
        AddMember { keys, role, .. } => {
            member(state, &keys.signing).is_some_and(|m| m.role == *role)
        }
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

pub(crate) fn check_effect(
    state: &StoreLogState,
    view: &StoreLogCheck,
    entry: &StoreLogEntry,
) -> Result<(), DropReason> {
    use StoreChange::*;
    let targets_exist = match &entry.change {
        CreateStore { .. } => state.store.is_none(),
        AddMember { .. } => state.store.is_some(),
        RemoveMember { member: id, .. } | ChangeRole { member: id, .. } => {
            member(state, id).is_some()
        }
        AddDevice { device: id, .. } => {
            member(state, &entry.author).is_some() && device(state, *id).is_none()
        }
        RemoveDevice { device: id } => {
            let StoreLogCheck::DeviceOwner(owner) = view else {
                unreachable!("authorized removal has an observed owner")
            };
            device(state, *id).is_some_and(|device| device.member == *owner)
        }
        CreateCircle { circle: id, .. } => {
            member(state, &entry.author).is_some() && circle(state, *id).is_none()
        }
        RenameCircle { circle: id, .. } | DeleteCircle { circle: id } => {
            circle(state, *id).is_some()
        }
        AddCircleMember {
            circle: id,
            member: who,
        } => circle(state, *id).is_some() && member(state, who).is_some(),
        RemoveCircleMember {
            circle: id,
            member: who,
            ..
        } => circle(state, *id).is_some_and(|circle| circle.members.contains(who)),
        RaiseSchema { snapshot, .. } | RaiseFormat { snapshot, .. } | Reset { snapshot } => {
            match snapshot.audience {
                Audience::Store => state.store.is_some(),
                Audience::Circle(id) => circle(state, id).is_some(),
            }
        }
    };
    if !targets_exist {
        return Err(DropReason::TargetGone);
    }
    // Evaluate the resulting admin set before conflicts, without copying state
    // for an effect that might lose or cause the scan to restart.
    let removed_admin = match &entry.change {
        CreateStore { .. }
        | AddMember {
            role: MemberRole::Admin,
            ..
        }
        | ChangeRole {
            role: MemberRole::Admin,
            ..
        } => return Ok(()),
        RemoveMember { member, .. } | ChangeRole { member, .. } => Some(member),
        AddMember { keys, .. } => Some(&keys.signing),
        _ => None,
    };
    if state.store.is_some()
        && !state.members.iter().any(|(id, member)| {
            !member.removed && member.role == MemberRole::Admin && Some(id) != removed_admin
        })
    {
        return Err(DropReason::NoAdminLeft);
    }
    Ok(())
}

/// Apply an effect only after its targets, remaining admin and conflicts pass.
pub(crate) fn apply_effect(next: &mut StoreLogState, entry: &StoreLogEntry) {
    use StoreChange::*;
    match &entry.change {
        CreateStore {
            store,
            name,
            admin,
            access,
            key,
            device_name,
        } => {
            *next = StoreLogState::default();
            next.store = Some(StoreIdentity {
                id: *store,
                name: name.clone(),
                key: *key,
            });
            next.members.insert(
                entry.author.clone(),
                StoreMember {
                    sealing: admin.sealing,
                    access: access.clone(),
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
        AddMember { keys, role, access } => {
            next.members.insert(
                keys.signing.clone(),
                StoreMember {
                    sealing: keys.sealing,
                    access: access.clone(),
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
            next.members.get_mut(id).expect("checked member").role = *role;
        }
        AddDevice { device: id, name } => {
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
            next.devices.get_mut(id).expect("checked device").removed = true;
        }
        CreateCircle {
            circle: id,
            name,
            key,
        } => {
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
            next.circles.get_mut(id).expect("checked circle").name = name.clone();
        }
        DeleteCircle { circle: id } => {
            let circle = next.circles.get_mut(id).expect("checked circle");
            circle.deleted = true;
            circle.members.clear();
        }
        AddCircleMember {
            circle: id,
            member: who,
        } => {
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
}

#[cfg(test)]
#[path = "effects_tests.rs"]
pub(crate) mod tests;
