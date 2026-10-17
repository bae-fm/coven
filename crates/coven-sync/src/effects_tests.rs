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

fn announced(number: u64, covers: u64, purpose: Purpose) -> SnapshotAnnouncement {
    SnapshotAnnouncement {
        snapshot: coven_format::admin::SnapshotId {
            audience: Audience::Store,
            device: DeviceId(1),
            number,
        },
        covers,
        purpose,
        digest: [number as u8; 32],
    }
}

fn activate(
    change: StoreChange,
    announcements: &BTreeMap<Slot, SnapshotAnnouncement>,
    boundaries: &[coven_database::BoundaryCoverage],
) -> Result<Option<AdminEffect>, OperationRejection> {
    checked_change(
        &household(),
        &header(1, 1, 19),
        Slot(20),
        &change,
        announcements,
        boundaries,
    )
}

#[test]
fn a_raise_covering_less_than_an_earlier_announcement_is_refused() {
    let raise = StoreChange::RaiseSchema {
        version: 2,
        announcement: Slot(10),
    };
    let ordinary = |covers| {
        BTreeMap::from([
            (Slot(10), announced(1, 8, Purpose::Raise(2))),
            (Slot(12), announced(2, covers, Purpose::Ordinary)),
        ])
    };
    assert!(matches!(
        activate(raise.clone(), &ordinary(11), &[]),
        Err(OperationRejection::NotAllowed)
    ));
    // Equal coverage and another audience's later coverage are allowed.
    assert!(activate(raise.clone(), &ordinary(8), &[])
        .unwrap()
        .is_some());
    let mut circle = ordinary(11);
    circle.get_mut(&Slot(12)).unwrap().snapshot.audience =
        Audience::Circle(CircleId(uuid::Uuid::from_u128(30)));
    assert!(activate(raise, &circle, &[]).unwrap().is_some());
}

#[test]
fn a_reset_covering_less_than_a_preceding_raise_slot_is_refused() {
    let announcements = BTreeMap::from([
        (Slot(10), announced(1, 8, Purpose::Raise(2))),
        (Slot(14), announced(2, 13, Purpose::Reset)),
        (Slot(15), announced(3, 9, Purpose::Reset)),
    ]);
    let raised = coven_database::BoundaryCoverage {
        boundary: Boundary::SchemaChange(StoreVersion {
            number: 2,
            snapshot: announcements[&Slot(10)].snapshot.clone(),
            entry: Slot(11),
        }),
        included: 8,
    };
    // The reset covering 9 was captured before the raise at slot 11.
    assert!(matches!(
        activate(
            StoreChange::Reset {
                announcement: Slot(15)
            },
            &BTreeMap::from([
                (Slot(10), announcements[&Slot(10)].clone()),
                (Slot(15), announcements[&Slot(15)].clone())
            ]),
            std::slice::from_ref(&raised),
        ),
        Err(OperationRejection::NotAllowed)
    ));
    // Covering the raise slot and every earlier announcement, it activates.
    assert!(activate(
        StoreChange::Reset {
            announcement: Slot(14)
        },
        &BTreeMap::from([
            (Slot(10), announcements[&Slot(10)].clone()),
            (Slot(14), announcements[&Slot(14)].clone())
        ]),
        std::slice::from_ref(&raised),
    )
    .unwrap()
    .is_some());
}

mod sequence_differential {
    use super::*;
    use crate::sequence_sync::tests::Group;
    use coven_database::{ApplyOutcome, IncomingSlot, SlotContent};
    use coven_format::{floor::OriginalSlot, sequence::SlotFrame};
    use serde_json::{json, Value};

    #[derive(Clone, Debug)]
    enum Action {
        Add(u128, MemberRole),
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
                Self::Add(m, r) => json!({"kind":"addMember","member":m,"role":role(*r)}),
                Self::Remove(m) => json!({"kind":"removeMember","member":m}),
                Self::Role(m, r) => json!({"kind":"changeRole","member":m,"role":role(*r)}),
                Self::Register(d, m) => json!({"kind":"addDevice","device":d,"member":m}),
                Self::RemoveDevice(d) => json!({"kind":"removeDevice","device":d}),
                Self::Replace(m) => json!({"kind":"replaceKey","member":m}),
            }
        }
        fn change(&self, state: &StoreState, seed: u8) -> StoreChange {
            match self {
                Self::Add(m, r) => StoreChange::AddMember {
                    member: id(*m),
                    keys: keys(seed),
                    role: *r,
                    access: access(seed),
                },
                Self::Remove(m) => StoreChange::RemoveMember { member: id(*m) },
                Self::Role(m, r) => StoreChange::ChangeRole {
                    member: id(*m),
                    role: *r,
                },
                Self::Register(..) => StoreChange::AddDevice {
                    name: "installation".into(),
                    replaces: None,
                },
                Self::RemoveDevice(d) => StoreChange::RemoveDevice {
                    device: DeviceId(*d),
                },
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
        let slot = Slot(header.after + 1);
        let path = coven_storage::ObjectPath::slot(group.store, slot.0.try_into().unwrap());
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
                    content: SlotContent::Admin {
                        original: OriginalSlot { slot, object },
                        effect
                    },
                },
                || Ok(()),
                None
            )
            .await
            .unwrap(),
            ApplyOutcome::Applied
        );
    }

    async fn compare(actions: &[(u64, Action)]) -> usize {
        let input = json!({"kind":"apply","member_count":4,
            "initial":{"members":[{"member":1,"role":"admin","key_gen":0},{"member":2,"role":"admin","key_gen":0}],
                "devices":[{"device":1,"member":1},{"device":2,"member":2}]},"audiences":[],
            "slots":actions.iter().enumerate().map(|(i,(d,a))|json!({"kind":"admin","device":d,"claim":i+1,"digest":format!("slot-{}",i+1),"entry":a.json()})).collect::<Vec<_>>()});
        let lean = crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input);
        let group = Group::new();
        let observer = group.device(9).await;
        let db = observer.database();
        // The model starts after these creation, membership and registration slots.
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
            (
                header(1, 1, 1),
                StoreChange::AddMember {
                    member: id(2),
                    keys: keys(2),
                    role: MemberRole::Admin,
                    access: access(2),
                },
                1,
            ),
            (
                header(2, 2, 2),
                StoreChange::AddDevice {
                    name: "Ben".into(),
                    replaces: None,
                },
                2,
            ),
        ];
        for (header, change, seed) in initial {
            let state = db.store_state().await.unwrap();
            let effect = checked_change(
                &state,
                &header,
                Slot(header.after + 1),
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
            let refused = expected["refused"]
                .as_array()
                .unwrap()
                .contains(&json!(i + 1));
            let before = db.store_state().await.unwrap();
            let member = match action {
                Action::Register(_, m) => *m,
                _ => before
                    .devices
                    .get(&DeviceId(*author))
                    .map_or(2, |d| d.member.0.as_u128()),
            };
            let hdr = header(*author, member, i as u64 + 3);
            let at = Slot(i as u64 + 4);
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
                    Action::Add(m, _) | Action::Replace(m) => seeds[*m as usize] = (i + 10) as u8,
                    _ => (),
                }
            }
            apply_checked(db, &group, hdr, change, effect, signer).await;
            let state = db.store_state().await.unwrap();
            // Key generations project changes to the stored pair, independently
            // of whether admission returned an effect for the requested replacement.
            for (member, current) in &state.members {
                if before
                    .members
                    .get(member)
                    .is_some_and(|old| old.keys != current.keys)
                {
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
            let removed: Vec<_> = state
                .devices
                .iter()
                .filter(|(_, d)| d.removed())
                .map(|(d, _)| d.0)
                .collect();
            for (field, rust) in [
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
                assert_eq!(
                    rust,
                    expected[field],
                    "field {field}, prefix {}, input {input}",
                    i + 1
                );
            }
            // Rust completes already-satisfied calls without an entry. Injected
            // redundant slots and model structural refusals both have no effect.
            if refused {
                assert!(!had_effect, "refused slot changed Rust state: {input}");
            }
            assert_eq!(db.applied_slot().await.unwrap(), at.0);
        }
        db.close().await.unwrap();
        actions.len()
    }

    #[tokio::test]
    #[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
    async fn sequence_admin_matches_lean() {
        use Action::*;
        let choices = [
            (1, Remove(1)),
            (1, Role(1, MemberRole::Member)),
            (2, Role(2, MemberRole::Member)),
            (1, Remove(2)),
            (1, Add(2, MemberRole::Member)),
            (1, Add(3, MemberRole::Member)),
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
                (1, Role(2, MemberRole::Member)),
                (3, Register(3, 2)),
                (2, RemoveDevice(3)),
            ],
            vec![
                (1, Remove(2)),
                (1, Add(3, MemberRole::Member)),
                (3, Register(3, 3)),
            ],
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
            "sequence apply admin: {exhaustive} exhaustive + 2 directed + {} seeded histories, {prefixes} prefixes",
            histories - exhaustive - 2
        );
    }
}
