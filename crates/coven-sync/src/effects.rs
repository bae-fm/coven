//! Appendix C's authority, already-in-place and checked effects, with retained data.

use coven_database::{
    CircleRemoval, DeviceState, DropReason, EntryTarget, StoreCircle, StoreDevice, StoreIdentity,
    StoreLogCheck, StoreLogState, StoreMember,
};
use coven_format::store_log::{MemberRole, StoreChange, StoreLogEntry};
use coven_foundation::id_source::{CircleId, DeviceId, MemberId};
use coven_merge::Audience;

pub(crate) fn member<'a>(state: &'a StoreLogState, id: &MemberId) -> Option<&'a StoreMember> {
    state.members.get(id).filter(|member| !member.removed)
}

/// A registered device that is not removed. A replaced id can still be removed,
/// which stops its old copies for good instead of resetting them (§10, §13).
pub(crate) fn device(state: &StoreLogState, id: DeviceId) -> Option<&StoreDevice> {
    state.devices.get(&id).filter(|device| !device.removed())
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
    /// Recording a member's access: their own, or an admin's replacement S3 key.
    Access {
        target: MemberId,
        s3: bool,
    },
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
            // Both must be members in the recorded view; an admin's role there,
            // not provider-admin access, permits recording another's S3 key (§9).
            Self::Access { target, s3 } => {
                member(state, author).is_some()
                    && member(state, target).is_some()
                    && (target == author || (*s3 && admin(state, author)))
            }
        };
        if allowed {
            Ok(())
        } else {
            Err(DropReason::NotAllowed)
        }
    }

    fn for_change(change: &StoreChange) -> Option<Self> {
        use StoreChange::*;
        Some(match change {
            // Sentinel: removed in T105, which introduces the keyed replay of
            // member-key replacements (self or another admin, fresh keys, and
            // their own conflicts and landing windows). Until then no view
            // authorizes one, so its entry drops as `NotAllowed`.
            ReplaceMemberKeys { .. } => return None,
            CreateStore { .. } => Self::Creation,
            AddMember { .. } | RemoveMember { .. } | ChangeRole { .. } => Self::Admin,
            SetAccess { member, access } => Self::Access {
                target: *member,
                s3: matches!(access, coven_format::MemberAccess::S3AccessKey { .. }),
            },
            AddDevice { .. } | CreateCircle { .. } => Self::Member,
            RotateKey { audience, .. } => Self::audience(audience),
            RaiseSchema { snapshot, .. } => Self::audience(&snapshot.audience),
            RemoveDevice { device } => Self::Device(*device),
            RenameCircle { circle, .. }
            | DeleteCircle { circle }
            | AddCircleMember { circle, .. }
            | RemoveCircleMember { circle, .. } => Self::Circle(*circle),
            Reset { snapshot } => Self::reset(&snapshot.audience),
        })
    }

    pub(crate) fn audience(audience: &Audience) -> Self {
        match audience {
            Audience::Store => Self::Member,
            Audience::Circle(circle) => Self::Circle(*circle),
        }
    }
}

/// An old id's entry must precede its reading a kept replacement of that id,
/// judged in its recorded past including its own earlier entries (§9, §10).
pub(crate) fn check(view: &StoreLogState, entry: &StoreLogEntry) -> StoreLogCheck {
    if view
        .devices
        .get(&entry.position.device)
        .is_some_and(|device| device.state == DeviceState::Replaced)
    {
        return StoreLogCheck::NotAllowed;
    }
    check_change(view, &entry.author, &entry.change)
}

fn check_change(view: &StoreLogState, author: &MemberId, change: &StoreChange) -> StoreLogCheck {
    #[cfg(test)]
    crate::replay::tests::author_check();
    use StoreChange::*;
    if Authority::for_change(change).is_none_or(|authority| authority.check(view, author).is_err())
    {
        return StoreLogCheck::NotAllowed;
    }
    // An addition cannot change an active member's keys; only a replacement
    // does (§9, D6).
    if let AddMember {
        member: id, keys, ..
    } = change
    {
        if member(view, id).is_some_and(|added| added.keys != *keys) {
            return StoreLogCheck::NotAllowed;
        }
    }
    // Only the old id's own member replaces it, with a distinct new id (D6).
    if let AddDevice {
        replaces: Some(old),
        ..
    } = change
    {
        if view
            .devices
            .get(old)
            .is_none_or(|old| old.member != *author)
        {
            return StoreLogCheck::NotAllowed;
        }
    }
    match change {
        RemoveDevice { device: id } => {
            StoreLogCheck::DeviceOwner(device(view, *id).expect("authorized device removal").member)
        }
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
        // A replacement is an event even when its payload repeats (§9).
        RotateKey { .. } | ReplaceMemberKeys { .. } => false,
        CreateStore { .. } => state.store.is_some(),
        AddMember {
            member: id,
            keys,
            role,
            ..
        } => member(state, id).is_some_and(|m| m.role == *role && m.keys == *keys),
        SetAccess { member: id, access } => {
            member(state, id).is_some_and(|m| m.access.same_credential(access))
        }
        ChangeRole { member: id, role } => member(state, id).is_some_and(|m| m.role == *role),
        RemoveMember { member: id, .. } => member(state, id).is_none(),
        AddDevice {
            device: id,
            replaces,
            ..
        } => {
            device(state, *id).is_some_and(|d| d.member == *author)
                && replaces.is_none_or(|old| state.devices.get(&old).is_none_or(|d| !d.active()))
        }
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
        ReplaceMemberKeys { .. } => unreachable!("no view authorizes a key replacement"),
        SetAccess { member: id, .. } => member(state, id)
            .is_none()
            .then_some(EntryTarget::Member(*id)),
        RemoveMember { member: id } | ChangeRole { member: id, .. } => member(state, id)
            .is_none()
            .then_some(EntryTarget::Member(*id)),
        AddDevice {
            device: id,
            replaces,
            ..
        } => {
            if member(state, author).is_none() {
                Some(EntryTarget::Member(*author))
            } else if let Some(old) = replaces.filter(|old| !state.devices.contains_key(old)) {
                Some(EntryTarget::Device(old))
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
                Some(EntryTarget::Member(*author))
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
                (!exists).then_some(EntryTarget::Member(*who))
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
        AddMember { member, .. } => Some(member),
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
        // A rotation's key comes from its received introduction (§11); it has
        // no membership effect, including after concurrent deletion.
        RotateKey { .. } => {}
        CreateStore {
            store,
            name,
            admin,
            access,
            device_name,
            ..
        } => {
            *next = StoreLogState::default();
            next.store = Some(StoreIdentity {
                id: *store,
                name: name.clone(),
            });
            next.members.insert(
                entry.author,
                StoreMember {
                    keys: *admin,
                    access: access.clone(),
                    role: MemberRole::Admin,
                    removed: false,
                },
            );
            next.devices.insert(
                entry.position.device,
                StoreDevice {
                    member: entry.author,
                    name: device_name.clone(),
                    state: DeviceState::Active,
                },
            );
        }
        AddMember {
            member,
            keys,
            role,
            access,
        } => {
            next.members.insert(
                *member,
                StoreMember {
                    keys: *keys,
                    access: access.clone(),
                    role: *role,
                    removed: false,
                },
            );
        }
        RemoveMember { member: id } => {
            next.members.get_mut(id).expect("checked member").removed = true;
            for device in next.devices.values_mut().filter(|d| d.member == *id) {
                device.state = DeviceState::Removed;
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
        SetAccess { member: id, access } => {
            next.members.get_mut(id).expect("checked member").access = access.clone();
        }
        ChangeRole { member: id, role } => {
            next.members.get_mut(id).expect("checked member").role = *role;
        }
        AddDevice {
            device: id,
            name,
            replaces,
        } => {
            next.devices.insert(
                *id,
                StoreDevice {
                    member: entry.author,
                    name: name.clone(),
                    state: DeviceState::Active,
                },
            );
            // Concurrent replacements each keep their new id; the old id stays
            // replaced, and a removal stays a removal (§10).
            if let Some(old) = replaces {
                let old = next.devices.get_mut(old).expect("checked replaced device");
                if old.active() {
                    old.state = DeviceState::Replaced;
                }
            }
        }
        RemoveDevice { device: id } => {
            next.devices.get_mut(id).expect("checked device").state = DeviceState::Removed;
        }
        CreateCircle {
            circle: id, name, ..
        } => {
            next.circle_deletions.remove(id);
            next.circles.insert(
                *id,
                StoreCircle {
                    name: name.clone(),
                    members: [entry.author].into(),
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
                .insert(*who);
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
        ReplaceMemberKeys { .. } => unreachable!("no view authorizes a key replacement"),
    }
}

#[cfg(test)]
#[path = "effects_tests.rs"]
pub(crate) mod tests;
