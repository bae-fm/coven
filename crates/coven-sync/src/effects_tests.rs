use super::*;
use coven_foundation::id_source::{CircleId, DeviceId, KeyId, StoreId};
use {coven_database::StoreCircle, coven_merge::slot};

pub(crate) fn id(n: u128) -> MemberId {
    MemberId(uuid::Uuid::from_u128(n))
}
fn keys(n: u8) -> coven_crypto::MemberPublicKeys {
    coven_format::test_utils::keys_from_seed(n).public_keys()
}
fn access(n: u8) -> coven_format::MemberAccess {
    coven_format::MemberAccess::S3AccessKey { access_key_id: n.to_string() }
}
pub(crate) fn header(device: u64, author: u128, after: u64) -> SlotHeader {
    SlotHeader { device: DeviceId(device), author: id(author), after, claim: after + 1 }
}
pub(crate) fn household() -> StoreState {
    let change = StoreChange::CreateStore {
        store: StoreId(uuid::Uuid::from_u128(10)),
        name: "household".into(),
        admin: keys(1),
        access: access(1),
        device_name: "Ana".into(),
        key: KeyId(uuid::Uuid::from_u128(11)),
        key_hash: [1; 32],
    };
    let state = checked_change(
        &StoreState::default(),
        &header(1, 1, 0),
        slot(1),
        &change,
        &BTreeMap::new(),
        &[],
    )
    .unwrap()
    .unwrap()
    .state;
    checked_change(
        &state,
        &header(1, 1, 1),
        slot(2),
        &admit(2, MemberRole::Admin, 2),
        &BTreeMap::new(),
        &[],
    )
    .unwrap()
    .unwrap()
    .state
}
/// Admit member `n` with seed `n`'s keys and access and `role`, registering
/// device `device` for them (§13.2).
fn admit(n: u8, role: MemberRole, device: u64) -> StoreChange {
    StoreChange::AdmitMember {
        member: id(n.into()),
        keys: keys(n),
        role,
        access: access(n),
        device: DeviceId(device),
        device_name: format!("device {device}"),
    }
}
pub(crate) fn apply(
    state: &StoreState,
    author: u64,
    change: StoreChange,
) -> Result<Option<AdminEffect>, OperationRejection> {
    checked_change(
        state,
        &header(author, author as u128, 3),
        slot(4),
        &change,
        &BTreeMap::new(),
        &[],
    )
}
#[test]
fn repeating_an_active_member_requires_the_exact_binding() {
    use coven_format::MemberAccess;
    for recorded in [access(2), MemberAccess::ProviderAccount("Ben@example.com".into())] {
        let mut state = household();
        state.members.get_mut(&id(2)).unwrap().access = recorded.clone();
        let request = |keys, role, access, device| StoreChange::AdmitMember {
            member: id(2),
            keys,
            role,
            access,
            device: DeviceId(device),
            device_name: "Ben".into(),
        };
        assert!(apply(&state, 1, request(keys(2), MemberRole::Admin, recorded.clone(), 2))
            .unwrap()
            .is_none());
        let different_access = match &recorded {
            MemberAccess::S3AccessKey { access_key_id } => {
                MemberAccess::S3AccessKey { access_key_id: format!("{access_key_id}-other") }
            }
            MemberAccess::ProviderAccount(_) => {
                MemberAccess::ProviderAccount("ben@example.com".into())
            }
        };
        for change in [
            request(keys(3), MemberRole::Admin, recorded.clone(), 2),
            request(keys(2), MemberRole::Member, recorded.clone(), 2),
            request(keys(2), MemberRole::Admin, different_access, 2),
            request(keys(2), MemberRole::Admin, recorded, 9),
        ] {
            assert!(matches!(
                apply(&state, 1, change),
                Err(OperationRejection::TargetGone(EntryTarget::Member(member))) if member == id(2)
            ));
        }
    }
}
#[test]
fn removal_preserves_bindings_and_refuses_a_later_action_by_the_removed_author() {
    let state = household();
    let next =
        apply(&state, 1, StoreChange::RemoveMember { member: id(2) }).unwrap().unwrap().state;
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
    let next =
        apply(&state, 1, StoreChange::RemoveMember { member: id(2) }).unwrap().unwrap().state;
    assert!(matches!(
        apply(&next, 1, StoreChange::RemoveMember { member: id(1) }),
        Err(OperationRejection::NoAdminLeft)
    ));
    assert!(matches!(
        apply(
            &next,
            1,
            StoreChange::AdmitMember {
                member: id(2),
                keys: keys(3),
                role: MemberRole::Member,
                access: access(3),
                device: DeviceId(3),
                device_name: "Ben's new phone".into(),
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
        StoreChange::ReplaceMemberKeys { member: id(2), expected: keys(2), keys: keys(3) },
    )
    .unwrap()
    .unwrap()
    .state;
    assert!(matches!(
        apply(
            &next,
            1,
            StoreChange::ReplaceMemberKeys { member: id(2), expected: keys(2), keys: keys(4) }
        ),
        Err(OperationRejection::MemberKeysChanged(_))
    ));
    for proposed in [
        keys(2),
        coven_crypto::MemberPublicKeys { signing: keys(4).signing, sealing: keys(2).sealing },
    ] {
        assert!(matches!(
            apply(
                &next,
                1,
                StoreChange::ReplaceMemberKeys { member: id(2), expected: keys(3), keys: proposed }
            ),
            Err(OperationRejection::KeysAlreadyUsed)
        ));
    }
}
#[test]
fn already_satisfied_deletion_finishes_without_requiring_lost_membership() {
    let mut state = household();
    let circle = CircleId(uuid::Uuid::from_u128(20));
    state
        .circles
        .insert(circle, StoreCircle { name: "Gifts".into(), members: Default::default(), base: 0 });
    state.circle_deletions.insert(circle, slot(3));
    assert!(apply(&state, 1, StoreChange::DeleteCircle { circle }).unwrap().is_none());
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
        breaking_version: 0,
        purpose: Purpose::Reset,
        digest: [number as u8; 32],
    };
    let announcements = [(slot(4), snapshot(1, 3)), (slot(6), snapshot(2, 5))].into();
    assert!(checked_change(
        &state,
        &header(1, 1, 6),
        slot(7),
        &StoreChange::Reset { announcement: slot(4) },
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
        breaking_version: 0,
        digest: [7; 32],
    };
    let prepared = SnapshotAnnouncement {
        snapshot: coven_format::admin::SnapshotId { number: 2, ..ordinary.snapshot.clone() },
        purpose: Purpose::Reset,
        covers: 12,
        breaking_version: 0,
        digest: [8; 32],
    };
    history.boundaries.push(coven_database::BoundaryCoverage {
        boundary: Boundary::Reset { entry: slot(15), snapshot: prepared.snapshot.clone() },
        included: 12,
    });
    assert!(history.usable_at(&ordinary, 11));
    assert!(!history.usable_at(&prepared, 11));
    assert!(!history.usable_at(&ordinary, 15));
    assert!(history.usable_at(&prepared, 15));
}

fn announced(number: u64, covers: u64, purpose: Purpose) -> SnapshotAnnouncement {
    SnapshotAnnouncement {
        snapshot: coven_format::admin::SnapshotId {
            audience: Audience::Store,
            device: DeviceId(1),
            number,
        },
        covers,
        breaking_version: 0,
        purpose,
        digest: [number as u8; 32],
    }
}

fn activate(
    change: StoreChange,
    announcements: &BTreeMap<Slot, SnapshotAnnouncement>,
    boundaries: &[coven_database::BoundaryCoverage],
) -> Result<Option<AdminEffect>, OperationRejection> {
    checked_change(&household(), &header(1, 1, 19), slot(20), &change, announcements, boundaries)
}

#[test]
fn a_raise_covering_less_than_an_earlier_announcement_is_refused() {
    let raise = StoreChange::RaiseSchema { version: 2, announcement: slot(10) };
    let ordinary = |covers| {
        BTreeMap::from([
            (slot(10), announced(1, 8, Purpose::Raise(2))),
            (slot(12), announced(2, covers, Purpose::Ordinary)),
        ])
    };
    assert!(matches!(
        activate(raise.clone(), &ordinary(11), &[]),
        Err(OperationRejection::NotAllowed)
    ));
    // Equal coverage and another audience's later coverage are allowed.
    assert!(activate(raise.clone(), &ordinary(8), &[]).unwrap().is_some());
    let mut circle = ordinary(11);
    circle.get_mut(&slot(12)).unwrap().snapshot.audience =
        Audience::Circle(CircleId(uuid::Uuid::from_u128(30)));
    assert!(activate(raise, &circle, &[]).unwrap().is_some());
}

#[test]
fn a_reset_covering_less_than_a_preceding_raise_slot_is_refused() {
    let announcements = BTreeMap::from([
        (slot(10), announced(1, 8, Purpose::Raise(2))),
        (slot(14), announced(2, 13, Purpose::Reset)),
        (slot(15), announced(3, 9, Purpose::Reset)),
    ]);
    let raised = coven_database::BoundaryCoverage {
        boundary: Boundary::SchemaChange(StoreVersion {
            number: 2,
            snapshot: announcements[&slot(10)].snapshot.clone(),
            entry: slot(11),
        }),
        included: 8,
    };
    // The reset covering 9 was captured before the raise at slot 11.
    assert!(matches!(
        activate(
            StoreChange::Reset { announcement: slot(15) },
            &BTreeMap::from([
                (slot(10), announcements[&slot(10)].clone()),
                (slot(15), announcements[&slot(15)].clone())
            ]),
            std::slice::from_ref(&raised),
        ),
        Err(OperationRejection::NotAllowed)
    ));
    // Covering the raise slot and every earlier announcement, it activates.
    assert!(activate(
        StoreChange::Reset { announcement: slot(14) },
        &BTreeMap::from([
            (slot(10), announcements[&slot(10)].clone()),
            (slot(14), announcements[&slot(14)].clone())
        ]),
        std::slice::from_ref(&raised),
    )
    .unwrap()
    .is_some());
}

/// Only an admin resets the store, and only a member of a circle resets it
/// (§19.4).
#[test]
fn only_an_admin_resets_the_store_and_only_a_circle_member_resets_its_circle() {
    let mut state = apply(&household(), 1, admit(3, MemberRole::Member, 3)).unwrap().unwrap().state;
    let circle = CircleId(uuid::Uuid::from_u128(20));
    state
        .circles
        .insert(circle, StoreCircle { name: "Gifts".into(), members: [id(1)].into(), base: 0 });
    let announcement = |audience| SnapshotAnnouncement {
        snapshot: coven_format::admin::SnapshotId { audience, device: DeviceId(1), number: 1 },
        covers: 10,
        breaking_version: 0,
        purpose: Purpose::Reset,
        digest: [1; 32],
    };
    let reset = |author: u64, audience: Audience| {
        checked_change(
            &state,
            &header(author, author as u128, 19),
            slot(20),
            &StoreChange::Reset { announcement: slot(10) },
            &BTreeMap::from([(slot(10), announcement(audience))]),
            &[],
        )
    };
    assert!(matches!(reset(3, Audience::Store), Err(OperationRejection::NotAllowed)));
    assert!(reset(1, Audience::Store).unwrap().is_some());
    assert!(matches!(reset(3, Audience::Circle(circle)), Err(OperationRejection::NotAllowed)));
    assert!(reset(1, Audience::Circle(circle)).unwrap().is_some());
}

mod sequence_differential {
    use super::*;
    use crate::sequence_sync::tests::Group;
    use coven_database::{ApplyOutcome, IncomingSlot, SlotContent};
    use coven_format::{floor::OriginalSlot, sequence::SlotFrame};
    use serde_json::{json, Value};

    #[derive(Clone, Debug)]
    enum Action {
        Rename(&'static str),
        Admit(u128, MemberRole, u64),
        Remove(u128),
        Role(u128, MemberRole),
        Register(u64, u128),
        RemoveDevice(u64),
        Replace(u128),
    }
    fn role(role: MemberRole) -> &'static str {
        match role {
            MemberRole::Admin => "admin",
            MemberRole::Member => "member",
        }
    }
    impl Action {
        fn json(&self) -> Value {
            match self {
                Self::Rename(name) => json!({"kind":"renameStore","name":name}),
                Self::Admit(m, r, d) => {
                    json!({"kind":"admitMember","member":m,"role":role(*r),"device":d})
                }
                Self::Remove(m) => json!({"kind":"removeMember","member":m}),
                Self::Role(m, r) => json!({"kind":"changeRole","member":m,"role":role(*r)}),
                Self::Register(d, m) => {
                    json!({"kind":"addDevice","device":d,"member":m,"replaces":null})
                }
                Self::RemoveDevice(d) => json!({"kind":"removeDevice","device":d}),
                Self::Replace(m) => json!({"kind":"replaceKey","member":m}),
            }
        }
        fn change(&self, state: &StoreState, seed: u8) -> StoreChange {
            match self {
                Self::Rename(name) => StoreChange::RenameStore { name: (*name).into() },
                Self::Admit(m, r, d) => StoreChange::AdmitMember {
                    member: id(*m),
                    keys: keys(seed),
                    role: *r,
                    access: access(seed),
                    device: DeviceId(*d),
                    device_name: "admitted".into(),
                },
                Self::Remove(m) => StoreChange::RemoveMember { member: id(*m) },
                Self::Role(m, r) => StoreChange::ChangeRole { member: id(*m), role: *r },
                Self::Register(..) => {
                    StoreChange::AddDevice { name: "installation".into(), replaces: None }
                }
                Self::RemoveDevice(d) => StoreChange::RemoveDevice { device: DeviceId(*d) },
                Self::Replace(m) => StoreChange::ReplaceMemberKeys {
                    member: id(*m),
                    expected: state.members[&id(*m)].keys,
                    keys: keys(seed),
                },
            }
        }
    }

    async fn apply_checked(
        db: &coven_database::Database,
        group: &Group,
        header: SlotHeader,
        change: StoreChange,
        effect: Option<AdminEffect>,
        seed: u8,
    ) {
        let slot = slot(header.after + 1);
        let path = coven_storage::ObjectPath::slot(group.store, slot.get().try_into().unwrap());
        let signer = coven_crypto::MemberIdentity::new(
            header.author,
            coven_format::test_utils::keys_from_seed(seed),
        );
        let object = crate::slot_object::seal_bounded(
            &path,
            &header,
            &SlotFrame::Admin(change),
            coven_format::sealed_slot::SlotObjectPrefix {
                format: coven_format::FormatVersion::CURRENT,
                key: group.ring.store_key_ids().next().unwrap(),
                kind: coven_format::sealed_slot::SlotKind::Admin,
            },
            &group.ring,
            &signer,
        )
        .unwrap();
        let mut digest = coven_crypto::ObjectHasher::new();
        digest.update(&object);
        assert_eq!(
            db.apply_slot::<crate::PendingRecord, std::io::Empty, _>(
                IncomingSlot {
                    slot,
                    header,
                    digest: *digest.finish().as_bytes(),
                    content: SlotContent::Admin { original: OriginalSlot { slot, object }, effect },
                },
                coven_database::test_utils::idle_writers(),
                || Ok(())
            )
            .await
            .unwrap(),
            ApplyOutcome::Applied
        );
    }

    async fn compare(actions: &[(u64, Action)]) -> usize {
        let input = json!({"kind":"apply","member_count":4,
            "initial":{"name":"sequence tests","members":[{"member":1,"role":"admin","key_gen":0},{"member":2,"role":"admin","key_gen":0}],
                "devices":[{"device":1,"member":1},{"device":2,"member":2}]},"audiences":[],
            "slots":actions.iter().enumerate().map(|(i,(d,a))|json!({"kind":"admin","device":d,"claim":i+1,"digest":format!("slot-{}",i+1),"entry":a.json()})).collect::<Vec<_>>()});
        let lean = crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input);
        let group = Group::new();
        let observer = group.device(9).await;
        let db = observer.database();
        // The model starts after these creation and admission slots.
        let key = group.ring.store_key_ids().next().unwrap();
        let initial = [
            (
                header(1, 1, 0),
                StoreChange::CreateStore {
                    store: group.store,
                    name: "sequence tests".into(),
                    admin: keys(1),
                    access: access(1),
                    device_name: "Ana".into(),
                    key,
                    key_hash: group.ring.store_key(key).unwrap().hash(),
                },
                1,
            ),
            (header(1, 1, 1), admit(2, MemberRole::Admin, 2), 1),
        ];
        for (header, change, seed) in initial {
            let state = db.store_state().await.unwrap();
            let effect = checked_change(
                &state,
                &header,
                slot(header.after + 1),
                &change,
                &BTreeMap::new(),
                &[],
            )
            .unwrap();
            assert!(effect.is_some());
            apply_checked(db, &group, header, change, effect, seed).await;
        }
        let mut seeds = [0, 1, 2, 0];
        let mut generations = [0u64; 4];
        for (i, (author, action)) in actions.iter().enumerate() {
            let expected = &lean["slots"][i]["ledger"];
            let refused = expected["refused"].as_array().unwrap().contains(&json!(i + 1));
            let before = db.store_state().await.unwrap();
            let member = match action {
                Action::Register(_, m) => *m,
                _ => before.devices.get(&DeviceId(*author)).map_or(2, |d| d.member.0.as_u128()),
            };
            let hdr = header(*author, member, i as u64 + 2);
            let at = slot(i as u64 + 3);
            let change = action.change(&before, (i + 10) as u8);
            let checked = checked_change(&before, &hdr, at, &change, &BTreeMap::new(), &[]);
            // addDevice has no independently supplied target in the Rust format:
            // the slot author is the installation being registered.
            let effect = match checked {
                Ok(effect) => effect,
                Err(error) => {
                    assert!(refused, "Rust refused model-valid slot: {error:?}; {input}");
                    None
                }
            };
            let had_effect = effect.is_some();
            let signer = seeds[member as usize];
            if had_effect {
                match action {
                    Action::Admit(m, ..) | Action::Replace(m) => {
                        seeds[*m as usize] = (i + 10) as u8
                    }
                    _ => (),
                }
            }
            apply_checked(db, &group, hdr, change, effect, signer).await;
            let state = db.store_state().await.unwrap();
            // Key generations project changes to the stored pair, independently
            // of whether admission returned an effect for the requested replacement.
            for (member, current) in &state.members {
                if before.members.get(member).is_some_and(|old| old.keys != current.keys) {
                    generations[member.0.as_u128() as usize] += 1;
                }
            }
            let members: Vec<_> = state
                .members
                .iter()
                .filter(|(_, m)| !m.removed)
                .map(|(m, data)| json!([m.0.as_u128(), role(data.role)]))
                .collect();
            let retired: Vec<_> = state
                .members
                .iter()
                .filter(|(_, m)| m.removed)
                .map(|(m, _)| m.0.as_u128())
                .collect();
            let devices: Vec<_> = state
                .devices
                .iter()
                .map(|(d, data)| json!([d.0, data.member.0.as_u128()]))
                .collect();
            let removed: Vec<_> =
                state.devices.iter().filter(|(_, d)| d.removed()).map(|(d, _)| d.0).collect();
            for (field, rust) in [
                ("name", json!(state.store.as_ref().unwrap().name)),
                ("members", json!(members)),
                ("retired", json!(retired)),
                ("devices", json!(devices)),
                ("removed", json!(removed)),
                (
                    "key_generations",
                    json!(generations
                        .iter()
                        .enumerate()
                        .map(|(m, g)| json!([m, g]))
                        .collect::<Vec<_>>()),
                ),
            ] {
                assert_eq!(rust, expected[field], "field {field}, prefix {}, input {input}", i + 1);
            }
            // Rust completes already-satisfied calls without an entry. Injected
            // redundant slots and model structural refusals both have no effect.
            if refused {
                assert!(!had_effect, "refused slot changed Rust state: {input}");
            }
            assert_eq!(db.applied_slot().await.unwrap(), at.get());
        }
        db.close().await.unwrap();
        actions.len()
    }

    #[tokio::test]
    #[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
    async fn sequence_admin_matches_lean() {
        use Action::*;
        let choices = [
            (1, Rename("Family")),
            (2, Rename("家")),
            (3, Rename("Refused")),
            (1, Remove(1)),
            (1, Role(1, MemberRole::Member)),
            (2, Role(2, MemberRole::Member)),
            (1, Remove(2)),
            (1, Admit(2, MemberRole::Member, 5)),
            (1, Admit(3, MemberRole::Member, 6)),
            (1, Role(2, MemberRole::Member)),
            (2, Remove(1)),
            (2, Role(1, MemberRole::Member)),
            (1, RemoveDevice(2)),
            (2, RemoveDevice(1)),
            (3, Register(3, 2)),
            (3, Register(3, 3)),
            (2, Replace(2)),
            (2, Replace(1)),
            (3, Remove(1)),
        ];
        let mut histories = 0;
        let mut prefixes = 0;
        // Includes removing Ben and attempting to add the same member id again.
        for first in &choices {
            for second in &choices {
                prefixes += compare(&[first.clone(), second.clone()]).await;
                histories += 1;
            }
        }
        let exhaustive = histories;
        for history in [
            vec![
                (1, Rename("First")),
                (1, Role(2, MemberRole::Member)),
                (2, Rename("Refused")),
                (1, Role(2, MemberRole::Admin)),
                (2, Rename("Second")),
                (2, Rename("Second")),
                (2, Role(1, MemberRole::Member)),
                (1, Rename("Refused again")),
                (2, Rename("Final")),
            ],
            vec![(1, Role(2, MemberRole::Member)), (3, Register(3, 2)), (2, RemoveDevice(3))],
            vec![(1, Remove(2)), (1, Admit(3, MemberRole::Member, 6)), (3, Register(3, 3))],
        ] {
            prefixes += compare(&history).await;
            histories += 1;
        }

        for seed in 1..=64u64 {
            let mut rng = seed;
            let mut history = Vec::new();
            for _ in 0..12 {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                history.push(choices[(rng % choices.len() as u64) as usize].clone());
            }
            prefixes += compare(&history).await;
            histories += 1;
        }
        eprintln!(
            "sequence apply admin: {exhaustive} exhaustive + 3 directed + {} seeded histories, {prefixes} prefixes",
            histories - exhaustive - 3
        );
    }
}
/// Ana removes a device, or renames a circle, that the state she has applied
/// doesn't hold: it registered in a slot she hasn't read, or never existed.
/// She may remove any device and is in no circle it lacks, so what refuses the
/// change is its target, not her authority (§10.4).
#[test]
fn a_target_the_state_doesnt_hold_is_gone_not_forbidden() {
    let state = household();
    let device = DeviceId(9);
    assert_eq!(
        apply(&state, 1, StoreChange::RemoveDevice { device }).err(),
        Some(OperationRejection::TargetGone(EntryTarget::Device(device)))
    );
    let circle = CircleId(uuid::Uuid::from_u128(12));
    for change in [
        StoreChange::RenameCircle { circle, name: "Gifts".into() },
        StoreChange::AddCircleMember { circle, member: id(2) },
        StoreChange::RemoveCircleMember { circle, member: id(2) },
    ] {
        assert_eq!(
            apply(&state, 1, change).err(),
            Some(OperationRejection::TargetGone(EntryTarget::Circle(circle)))
        );
    }
}

fn apply_from(
    state: &StoreState,
    device: u64,
    author: u128,
    change: StoreChange,
) -> Result<Option<AdminEffect>, OperationRejection> {
    checked_change(state, &header(device, author, 3), slot(4), &change, &BTreeMap::new(), &[])
}

pub(crate) fn gifts() -> CircleId {
    CircleId(uuid::Uuid::from_u128(12))
}

pub(crate) fn create_circle(circle: CircleId, name: &str) -> StoreChange {
    StoreChange::CreateCircle {
        circle,
        name: name.into(),
        key: KeyId(uuid::Uuid::from_u128(circle.0.as_u128() + 100)),
        key_hash: [2; 32],
        breaking_version: 0,
    }
}

/// Any member creates a circle and is its first member; only the circle's
/// members manage it, so an admin outside it can't (§10.1).
#[test]
fn any_member_creates_a_circle_and_only_its_members_manage_it() {
    let state = household();
    let made = apply(&state, 2, create_circle(gifts(), "Gifts")).unwrap().unwrap().state;
    assert_eq!(made.circles[&gifts()].members, [id(2)].into());

    let rotation = || StoreChange::RotateKey {
        audience: Audience::Circle(gifts()),
        key: KeyId(uuid::Uuid::from_u128(500)),
        key_hash: [3; 32],
    };
    let changes = || {
        [
            StoreChange::RenameCircle { circle: gifts(), name: "Presents".into() },
            StoreChange::AddCircleMember { circle: gifts(), member: id(1) },
            StoreChange::RemoveCircleMember { circle: gifts(), member: id(2) },
            StoreChange::DeleteCircle { circle: gifts() },
            rotation(),
        ]
    };
    for change in changes() {
        assert_eq!(
            apply(&made, 1, change.clone()).err(),
            Some(OperationRejection::NotAllowed),
            "{change:?}"
        );
        assert!(apply(&made, 2, change.clone()).is_ok(), "{change:?}");
    }
}

/// A circle starts at the breaking version its creation recorded: a raise
/// of it to that version changes nothing, and one past it applies (§15.5,
/// §18.3).
#[test]
fn a_raise_of_a_circle_must_go_past_its_base() {
    let StoreChange::CreateCircle { circle, name, key, key_hash, .. } =
        create_circle(gifts(), "Gifts")
    else {
        unreachable!("a circle's creation")
    };
    let creation = StoreChange::CreateCircle { circle, name, key, key_hash, breaking_version: 2 };
    let made = apply(&household(), 2, creation).unwrap().unwrap().state;
    assert_eq!(made.circles[&gifts()].base, 2);
    assert_eq!(made.reached(&Audience::Circle(gifts())), 2);
    assert_eq!(made.reached(&Audience::Store), 0);
    let raise = |version| {
        let mut announcement = announced(1, 8, Purpose::Raise(version));
        announcement.snapshot.audience = Audience::Circle(gifts());
        checked_change(
            &made,
            &header(2, 2, 19),
            slot(20),
            &StoreChange::RaiseSchema { version, announcement: slot(10) },
            &BTreeMap::from([(slot(10), announcement)]),
            &[],
        )
    };
    assert!(matches!(raise(2), Ok(None)));
    let raised = raise(3).unwrap().unwrap().state;
    assert_eq!(raised.reached(&Audience::Circle(gifts())), 3);
}

/// Only a device's own member registers a new device in its place; several
/// new devices of that member may replace the same old id, each keeping its
/// own id (§19.3).
#[test]
fn only_its_member_replaces_a_device_id_and_several_may_replace_one() {
    let state = household();
    let phone = DeviceId(1);
    let replace = |name: &str| StoreChange::AddDevice { name: name.into(), replaces: Some(phone) };

    assert_eq!(
        apply_from(&state, 5, 2, replace("Ben's")).err(),
        Some(OperationRejection::NotAllowed)
    );

    let first = apply_from(&state, 5, 1, replace("Tablet")).unwrap().unwrap().state;
    let both = apply_from(&first, 6, 1, replace("Laptop")).unwrap().unwrap().state;
    assert_eq!(both.devices[&phone].state, DeviceState::Replaced);
    for new in [DeviceId(5), DeviceId(6)] {
        assert_eq!(both.devices[&new].state, DeviceState::Active);
        assert_eq!(both.devices[&new].member, id(1));
    }
}

/// Replacing a member's keys changes the keys alone: the member's access,
/// role, circle memberships and device ids stay (§10, §14.3).
#[test]
fn replacing_a_members_keys_keeps_access_role_circles_and_devices() {
    let state = apply(&household(), 2, create_circle(gifts(), "Gifts")).unwrap().unwrap().state;
    let replaced = apply(
        &state,
        1,
        StoreChange::ReplaceMemberKeys { member: id(2), expected: keys(2), keys: keys(3) },
    )
    .unwrap()
    .unwrap()
    .state;

    let (before, after) = (&state.members[&id(2)], &replaced.members[&id(2)]);
    assert_eq!(after.keys, keys(3));
    assert_ne!(before.keys, after.keys);
    assert_eq!(after.access, before.access);
    assert_eq!(after.role, before.role);
    assert_eq!(after.removed, before.removed);
    assert_eq!(replaced.circles, state.circles);
    assert_eq!(replaced.devices, state.devices);
}

/// Adds member 3, a plain member with a device, to `household()`.
pub(crate) fn with_a_plain_member(state: &StoreState) -> StoreState {
    apply(state, 1, admit(3, MemberRole::Member, 3)).unwrap().unwrap().state
}

fn circle_made_by_the_plain_member() -> StoreState {
    apply(&with_a_plain_member(&household()), 3, create_circle(gifts(), "Gifts"))
        .unwrap()
        .unwrap()
        .state
}

/// Any member can make a circle and is its first member. Its own members
/// rename it, add and remove its members and delete it; an admin outside it
/// can do none of that (§15.5).
#[test]
fn a_member_makes_a_circle_and_only_its_members_change_or_delete_it() {
    let state = circle_made_by_the_plain_member();
    assert_eq!(state.circles[&gifts()].members.iter().copied().collect::<Vec<_>>(), [id(3)]);
    for change in [
        StoreChange::RenameCircle { circle: gifts(), name: "Presents".into() },
        StoreChange::AddCircleMember { circle: gifts(), member: id(2) },
        StoreChange::RemoveCircleMember { circle: gifts(), member: id(3) },
        StoreChange::DeleteCircle { circle: gifts() },
    ] {
        assert_eq!(apply(&state, 1, change).err(), Some(OperationRejection::NotAllowed));
    }
    let state =
        apply(&state, 3, StoreChange::RenameCircle { circle: gifts(), name: "Presents".into() })
            .unwrap()
            .unwrap()
            .state;
    let state = apply(&state, 3, StoreChange::AddCircleMember { circle: gifts(), member: id(2) })
        .unwrap()
        .unwrap()
        .state;
    assert!(state.circles[&gifts()].members.contains(&id(2)));
    let state =
        apply(&state, 3, StoreChange::RemoveCircleMember { circle: gifts(), member: id(2) })
            .unwrap()
            .unwrap()
            .state;
    assert!(!state.circles[&gifts()].members.contains(&id(2)));
    let state =
        apply(&state, 3, StoreChange::DeleteCircle { circle: gifts() }).unwrap().unwrap().state;
    assert!(state.circle_deletions.contains_key(&gifts()));
}

/// Renames apply in slot order and the latest allowed one names the circle;
/// a member outside it changes nothing; a rename leaves the circle's
/// members as they were (§15.5).
#[test]
fn circle_renames_apply_in_slot_order_and_the_latest_valid_one_names_it() {
    let mut state = circle_made_by_the_plain_member();
    state = apply(&state, 3, StoreChange::AddCircleMember { circle: gifts(), member: id(2) })
        .unwrap()
        .unwrap()
        .state;
    let members = state.circles[&gifts()].members.clone();
    let renames = [
        (3, "First", true),
        (1, "Outsider", false),
        (2, "Second", true),
        (3, "Third", true),
        (1, "Late outsider", false),
    ];
    let mut named = "Gifts".to_owned();
    for (author, name, allowed) in renames {
        let result =
            apply(&state, author, StoreChange::RenameCircle { circle: gifts(), name: name.into() });
        if allowed {
            state = result.unwrap().unwrap().state;
            named = name.to_owned();
        } else {
            assert_eq!(result.err(), Some(OperationRejection::NotAllowed));
        }
        assert_eq!(state.circles[&gifts()].name, named);
        assert_eq!(state.circles[&gifts()].members, members);
    }
    assert_eq!(named, "Third");
}

/// A circle left empty is deleted at that slot, whether its last member left
/// it or was removed from the store. A later entry that adds, removes or
/// renames finds its target gone (§15.5, §15.8).
#[test]
fn a_circle_left_empty_is_deleted_at_that_slot() {
    let left = apply(
        &circle_made_by_the_plain_member(),
        3,
        StoreChange::RemoveCircleMember { circle: gifts(), member: id(3) },
    )
    .unwrap()
    .unwrap()
    .state;
    let removed =
        apply(&circle_made_by_the_plain_member(), 1, StoreChange::RemoveMember { member: id(3) })
            .unwrap()
            .unwrap()
            .state;
    for state in [left, removed] {
        assert_eq!(state.circle_deletions[&gifts()], slot(4));
        assert!(state.circles[&gifts()].members.is_empty());
        for change in [
            StoreChange::AddCircleMember { circle: gifts(), member: id(2) },
            StoreChange::RemoveCircleMember { circle: gifts(), member: id(3) },
            StoreChange::RenameCircle { circle: gifts(), name: "Presents".into() },
        ] {
            let gone = Some(OperationRejection::TargetGone(EntryTarget::Circle(gifts())));
            assert_eq!(apply(&state, 1, change).err(), gone, "a deleted circle is gone");
        }
    }
}
