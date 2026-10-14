//! Appendix C's authority, already-in-place and checked effects, with retained data.

use coven_crypto::MemberId;
use coven_database::{
    CircleRemoval, DropReason, EntryTarget, StoreCircle, StoreDevice, StoreIdentity, StoreLogCheck,
    StoreLogState, StoreMember,
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
    state
        .circles
        .get(&id)
        .filter(|_| !state.circle_deletions.contains_key(&id))
}

fn admin(state: &StoreLogState, id: &MemberId) -> bool {
    member(state, id).is_some_and(|member| member.role == MemberRole::Admin)
}

fn in_circle(state: &StoreLogState, id: CircleId, who: &MemberId) -> bool {
    member(state, who).is_some()
        && circle(state, id).is_some_and(|circle| circle.members.contains(who))
}

/// Authority shared by replay and work that precedes an entry's payload,
/// such as inviting a member or writing a reset snapshot.
pub(crate) enum Authority {
    Creation,
    Admin,
    Member,
    Circle(CircleId),
    Device(DeviceId),
}

impl Authority {
    pub(crate) fn reset(audience: &Audience) -> Self {
        match audience {
            Audience::Store => Self::Admin,
            Audience::Circle(circle) => Self::Circle(*circle),
        }
    }

    pub(crate) fn check(&self, state: &StoreLogState, author: &MemberId) -> Result<(), DropReason> {
        let allowed = match self {
            Self::Creation => state.store.is_none(),
            Self::Admin => admin(state, author),
            Self::Member => member(state, author).is_some(),
            Self::Circle(id) => in_circle(state, *id, author),
            Self::Device(id) => device(state, *id).is_some_and(|device| {
                member(state, author).is_some()
                    && (device.member == *author || admin(state, author))
            }),
        };
        if allowed {
            Ok(())
        } else {
            Err(DropReason::NotAllowed)
        }
    }

    fn for_change(change: &StoreChange) -> Self {
        use StoreChange::*;
        match change {
            CreateStore { .. } => Self::Creation,
            AddMember { .. } | RemoveMember { .. } | ChangeRole { .. } => Self::Admin,
            SetAccess { .. } | AddDevice { .. } | CreateCircle { .. } => Self::Member,
            RotateKey { audience, .. } => Self::audience(audience),
            RaiseSchema { snapshot, .. } => Self::audience(&snapshot.audience),
            RemoveDevice { device } => Self::Device(*device),
            RenameCircle { circle, .. }
            | DeleteCircle { circle }
            | AddCircleMember { circle, .. }
            | RemoveCircleMember { circle, .. } => Self::Circle(*circle),
            Reset { snapshot } => Self::reset(&snapshot.audience),
        }
    }

    fn audience(audience: &Audience) -> Self {
        match audience {
            Audience::Store => Self::Member,
            Audience::Circle(circle) => Self::Circle(*circle),
        }
    }
}

pub(crate) fn check(view: &StoreLogState, entry: &StoreLogEntry) -> StoreLogCheck {
    check_change(view, &entry.author, &entry.change)
}

fn check_change(view: &StoreLogState, author: &MemberId, change: &StoreChange) -> StoreLogCheck {
    #[cfg(test)]
    crate::replay::tests::author_check();
    use StoreChange::*;
    if Authority::for_change(change).check(view, author).is_err() {
        return StoreLogCheck::NotAllowed;
    }
    match change {
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
                    let targets = match change {
                        RemoveCircleMember { circle, .. } => circle == id,
                        _ => true,
                    };
                    (targets
                        && !view.circle_deletions.contains_key(id)
                        && circle.members.contains(member))
                    .then_some((
                        *id,
                        if circle.members.len() == 1 {
                            CircleRemoval::Circle
                        } else {
                            CircleRemoval::Member
                        },
                    ))
                })
                .collect();
            StoreLogCheck::MemberRemoval(circles)
        }
        _ => StoreLogCheck::Allowed,
    }
}

pub(crate) fn change_in_place(
    state: &StoreLogState,
    author: &MemberId,
    change: &StoreChange,
) -> bool {
    use StoreChange::*;
    match change {
        RotateKey { .. } => false,
        CreateStore { .. } => state.store.is_some(),
        AddMember { keys, role, .. } => {
            member(state, &keys.signing).is_some_and(|m| m.role == *role)
        }
        SetAccess { access } => member(state, author).is_some_and(|m| m.access == *access),
        ChangeRole { member: id, role } => member(state, id).is_some_and(|m| m.role == *role),
        RemoveMember { member: id, .. } => member(state, id).is_none(),
        AddDevice { device: id, .. } => device(state, *id).is_some_and(|d| d.member == *author),
        RemoveDevice { device: id } => device(state, *id).is_none(),
        CreateCircle {
            circle: id, name, ..
        } => circle(state, *id)
            .is_some_and(|c| c.name == *name && c.members.len() == 1 && c.members.contains(author)),
        RenameCircle { circle: id, name } => circle(state, *id).is_some_and(|c| c.name == *name),
        DeleteCircle { circle: id } => circle(state, *id).is_none(),
        AddCircleMember { circle, member } => in_circle(state, *circle, member),
        RemoveCircleMember { circle, member, .. } => !in_circle(state, *circle, member),
        RaiseSchema { version, snapshot } => {
            coven_database::Boundary::schema_in_place(state, *version, snapshot)
        }
        Reset { snapshot } => coven_database::Boundary::reset_in_place(state, snapshot),
    }
}

/// A candidate reading the entire current log has no concurrent opponents.
/// These are exactly the authority, no-op, target and admin checks used by replay.
pub(crate) fn validate_change(
    state: &StoreLogState,
    author: &MemberId,
    change: &StoreChange,
) -> Result<bool, DropReason> {
    validate_effect(state, &check_change(state, author, change), author, change)
}

/// Returns whether the authorized change is already in place.
pub(crate) fn validate_effect(
    state: &StoreLogState,
    view: &StoreLogCheck,
    author: &MemberId,
    change: &StoreChange,
) -> Result<bool, DropReason> {
    use StoreChange::*;
    if *view == StoreLogCheck::NotAllowed {
        return Err(DropReason::NotAllowed);
    }
    if change_in_place(state, author, change) {
        return Ok(true);
    }
    let missing = match change {
        RotateKey { .. } | CreateStore { .. } | AddMember { .. } => None,
        SetAccess { .. } => member(state, author)
            .is_none()
            .then(|| EntryTarget::Member(author.clone())),
        RemoveMember { member: id } | ChangeRole { member: id, .. } => member(state, id)
            .is_none()
            .then(|| EntryTarget::Member(id.clone())),
        AddDevice { device: id, .. } => {
            if member(state, author).is_none() {
                Some(EntryTarget::Member(author.clone()))
            } else {
                device(state, *id)
                    .is_some()
                    .then_some(EntryTarget::Device(*id))
            }
        }
        RemoveDevice { device: id } => {
            let StoreLogCheck::DeviceOwner(owner) = view else {
                unreachable!("authorized removal has an observed owner")
            };
            device(state, *id)
                .is_none_or(|device| device.member != *owner)
                .then_some(EntryTarget::Device(*id))
        }
        CreateCircle { circle: id, .. } => {
            if member(state, author).is_none() {
                Some(EntryTarget::Member(author.clone()))
            } else {
                circle(state, *id)
                    .is_some()
                    .then_some(EntryTarget::Circle(*id))
            }
        }
        RenameCircle { circle: id, .. } | DeleteCircle { circle: id } => circle(state, *id)
            .is_none()
            .then_some(EntryTarget::Circle(*id)),
        AddCircleMember {
            circle: id,
            member: who,
        }
        | RemoveCircleMember {
            circle: id,
            member: who,
        } => match circle(state, *id) {
            None => Some(EntryTarget::Circle(*id)),
            Some(circle) => {
                let exists = match change {
                    AddCircleMember { .. } => member(state, who).is_some(),
                    _ => circle.members.contains(who),
                };
                (!exists).then(|| EntryTarget::Member(who.clone()))
            }
        },
        RaiseSchema { snapshot, .. } | Reset { snapshot } => match snapshot.audience {
            Audience::Store => None,
            Audience::Circle(id) => circle(state, id)
                .is_none()
                .then_some(EntryTarget::Circle(id)),
        },
    };
    if let Some(target) = missing {
        return Err(DropReason::TargetGone(target));
    }
    // Evaluate the resulting admin set before conflicts, without copying state
    // for an effect that might lose or cause the scan to restart.
    let removed_admin = match change {
        CreateStore { .. }
        | AddMember {
            role: MemberRole::Admin,
            ..
        }
        | ChangeRole {
            role: MemberRole::Admin,
            ..
        } => return Ok(false),
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
    Ok(false)
}

/// Apply an effect only after its targets, remaining admin and conflicts pass.
pub(crate) fn apply_effect(next: &mut StoreLogState, view: &StoreLogCheck, entry: &StoreLogEntry) {
    use StoreChange::*;
    match &entry.change {
        RotateKey { audience, key, .. } => match audience {
            Audience::Store => {
                next.store.as_mut().expect("rotation follows creation").key = *key;
            }
            Audience::Circle(id) => {
                // Rotation has no membership effect, including after concurrent deletion.
                if let Some(circle) = next.circles.get_mut(id) {
                    circle.key = *key;
                }
            }
        },
        CreateStore {
            store,
            name,
            admin,
            access,
            key,
            device_name,
            ..
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
        RemoveMember { member: id } => {
            next.members.get_mut(id).expect("checked member").removed = true;
            for device in next.devices.values_mut().filter(|d| d.member == *id) {
                device.removed = true;
            }
            for (circle_id, circle) in &mut next.circles {
                if !next.circle_deletions.contains_key(circle_id) {
                    circle.members.remove(id);
                    if view.deletes_circle(*circle_id) {
                        circle.members.clear();
                        next.circle_deletions.insert(*circle_id, entry.position);
                    }
                }
            }
        }
        SetAccess { access } => {
            next.members
                .get_mut(&entry.author)
                .expect("checked author")
                .access = access.clone();
        }
        ChangeRole { member: id, role } => {
            next.members.get_mut(id).expect("checked member").role = *role;
        }
        AddDevice {
            device: id, name, ..
        } => {
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
            ..
        } => {
            next.circle_deletions.remove(id);
            next.circles.insert(
                *id,
                StoreCircle {
                    name: name.clone(),
                    key: *key,
                    members: [entry.author.clone()].into(),
                },
            );
        }
        RenameCircle { circle: id, name } => {
            next.circles.get_mut(id).expect("checked circle").name = name.clone();
        }
        DeleteCircle { circle: id } => {
            let circle = next.circles.get_mut(id).expect("checked circle");
            next.circle_deletions.insert(*id, entry.position);
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
        } => {
            let circle = next.circles.get_mut(id).expect("checked circle");
            circle.members.remove(who);
            if view.deletes_circle(*id) {
                circle.members.clear();
                next.circle_deletions.insert(*id, entry.position);
            }
        }
        RaiseSchema { .. } | Reset { .. } => {
            coven_database::Boundary::from_entry(entry)
                .expect("matched boundary entry")
                .apply(next);
        }
    }
}

#[cfg(test)]
#[path = "effects_tests.rs"]
pub(crate) mod tests;
