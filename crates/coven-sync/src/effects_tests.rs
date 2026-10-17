use super::*;
use coven_foundation::id_source::{CircleId, DeviceId, KeyId, StoreId};

fn id(n: u128) -> MemberId {
    MemberId(uuid::Uuid::from_u128(n))
}
fn keys(n: u8) -> coven_crypto::MemberPublicKeys {
    coven_format::test_utils::keys_from_seed(n).public_keys()
}
fn access(n: u8) -> coven_format::MemberAccess {
    coven_format::MemberAccess::S3AccessKey {
        access_key_id: n.to_string(),
        principal: coven_format::S3Principal::Console,
    }
}
fn header(device: u64, author: u128, after: u64) -> SlotHeader {
    SlotHeader {
        device: DeviceId(device),
        author: id(author),
        after,
        claim: after + 1,
    }
}
fn household() -> StoreState {
    let change = StoreChange::CreateStore {
        store: StoreId(uuid::Uuid::from_u128(10)),
        name: "household".into(),
        admin: keys(1),
        access: access(1),
        device_name: "Ana".into(),
        key: KeyId(uuid::Uuid::from_u128(11)),
        key_hash: [1; 32],
    };
    let mut state = checked_change(
        &StoreState::default(),
        &header(1, 1, 0),
        Slot(1),
        &change,
        &BTreeMap::new(),
        &[],
    )
    .unwrap()
    .unwrap()
    .state;
    state = checked_change(
        &state,
        &header(1, 1, 1),
        Slot(2),
        &StoreChange::AddMember {
            member: id(2),
            keys: keys(2),
            role: MemberRole::Admin,
            access: access(2),
        },
        &BTreeMap::new(),
        &[],
    )
    .unwrap()
    .unwrap()
    .state;
    checked_change(
        &state,
        &header(2, 2, 2),
        Slot(3),
        &StoreChange::AddDevice {
            name: "Ben".into(),
            replaces: None,
        },
        &BTreeMap::new(),
        &[],
    )
    .unwrap()
    .unwrap()
    .state
}
fn apply(
    state: &StoreState,
    author: u64,
    change: StoreChange,
) -> Result<Option<AdminEffect>, OperationRejection> {
    checked_change(
        state,
        &header(author, author as u128, 3),
        Slot(4),
        &change,
        &BTreeMap::new(),
        &[],
    )
}
#[test]
fn removal_preserves_bindings_and_refuses_a_later_action_by_the_removed_author() {
    let state = household();
    let next = apply(&state, 1, StoreChange::RemoveMember { member: id(2) })
        .unwrap()
        .unwrap()
        .state;
    assert!(next.members[&id(2)].removed);
    assert_eq!(next.devices[&DeviceId(2)].state, DeviceState::Removed);
    assert!(next.bound_keys.contains(&keys(2)));
    assert!(matches!(
        apply(&next, 2, StoreChange::RemoveMember { member: id(1) }),
        Err(OperationRejection::NotAllowed)
    ));
}
#[test]
fn an_admin_cannot_remove_the_last_admin_and_a_removed_id_cannot_return() {
    let state = household();
    let next = apply(&state, 1, StoreChange::RemoveMember { member: id(2) })
        .unwrap()
        .unwrap()
        .state;
    assert!(matches!(
        apply(&next, 1, StoreChange::RemoveMember { member: id(1) }),
        Err(OperationRejection::NoAdminLeft)
    ));
    assert!(matches!(
        apply(
            &next,
            1,
            StoreChange::AddMember {
                member: id(2),
                keys: keys(3),
                role: MemberRole::Member,
                access: access(3)
            }
        ),
        Err(OperationRejection::TargetGone(EntryTarget::Member(_)))
    ));
}
#[test]
fn replacement_keeps_its_expected_binding_and_never_reuses_either_public_key() {
    let state = household();
    let next = apply(
        &state,
        1,
        StoreChange::ReplaceMemberKeys {
            member: id(2),
            expected: keys(2),
            keys: keys(3),
        },
    )
    .unwrap()
    .unwrap()
    .state;
    assert!(matches!(
        apply(
            &next,
            1,
            StoreChange::ReplaceMemberKeys {
                member: id(2),
                expected: keys(2),
                keys: keys(4)
            }
        ),
        Err(OperationRejection::MemberKeysChanged(_))
    ));
    for proposed in [
        keys(2),
        coven_crypto::MemberPublicKeys {
            signing: keys(4).signing,
            sealing: keys(2).sealing,
        },
    ] {
        assert!(matches!(
            apply(
                &next,
                1,
                StoreChange::ReplaceMemberKeys {
                    member: id(2),
                    expected: keys(3),
                    keys: proposed
                }
            ),
            Err(OperationRejection::KeysAlreadyUsed)
        ));
    }
}
#[test]
fn already_satisfied_deletion_finishes_without_requiring_lost_membership() {
    let mut state = household();
    let circle = CircleId(uuid::Uuid::from_u128(20));
    state.circles.insert(
        circle,
        StoreCircle {
            name: "Gifts".into(),
            members: Default::default(),
        },
    );
    state.circle_deletions.insert(circle, Slot(3));
    assert!(apply(&state, 1, StoreChange::DeleteCircle { circle })
        .unwrap()
        .is_none());
}
#[test]
fn activating_a_preparation_checks_coverage_against_later_announcements() {
    let state = household();
    let snapshot = |number, covers| SnapshotAnnouncement {
        snapshot: coven_format::admin::SnapshotId {
            audience: Audience::Store,
            device: DeviceId(1),
            number,
        },
        covers,
        purpose: Purpose::Reset,
        digest: [number as u8; 32],
    };
    let announcements = [(Slot(4), snapshot(1, 3)), (Slot(6), snapshot(2, 5))].into();
    assert!(checked_change(
        &state,
        &header(1, 1, 6),
        Slot(7),
        &StoreChange::Reset {
            announcement: Slot(4)
        },
        &announcements,
        &[]
    )
    .is_err());
}

#[test]
fn a_floor_keeps_the_schema_and_reset_selected_at_its_own_prefix() {
    let mut history = crate::history::History::default();
    let ordinary = SnapshotAnnouncement {
        snapshot: coven_format::admin::SnapshotId {
            audience: Audience::Store,
            device: DeviceId(1),
            number: 1,
        },
        purpose: Purpose::Ordinary,
        covers: 10,
        digest: [7; 32],
    };
    let prepared = SnapshotAnnouncement {
        snapshot: coven_format::admin::SnapshotId {
            number: 2,
            ..ordinary.snapshot.clone()
        },
        purpose: Purpose::Reset,
        covers: 12,
        digest: [8; 32],
    };
    history.boundaries.push(coven_database::BoundaryCoverage {
        boundary: Boundary::Reset {
            entry: Slot(15),
            snapshot: prepared.snapshot.clone(),
        },
        included: 12,
    });
    assert!(history.usable_at(&ordinary, 11));
    assert!(!history.usable_at(&prepared, 11));
    assert!(!history.usable_at(&ordinary, 15));
    assert!(history.usable_at(&prepared, 15));
}
