//! `checked_change`, whose circle entries, admissions and member removals
//! these effects carry out, against the sequence model's circle lifecycle
//! (`CircleLifecycle.effect`): the same circle histories, slot by slot, give
//! the same verdicts, names, members, deletion slots and member standing
//! (§13.2, §15.5, §15.8, §15.9).
use crate::effects::checked_change;
use crate::effects::tests::{
    admit, apply, circle as admission_circle, create_circle, gifts, header, household, id, keys,
    remove_circle_member, with_a_plain_member,
};
use crate::{EntryTarget, OperationRejection};
use coven_database::{AdminEffect, StoreState};
use coven_format::admin::{MemberRole, NewCircle, StoreChange};
use coven_foundation::id_source::CircleId;
use coven_foundation::id_source::DeviceId;
use coven_merge::slot;
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Ana (1) and Ben (2) are admins and Cy (3) a member, each with a device of
/// the same number; Dee (4) was never added. Removals are Ana's, who stays.
const PEOPLE: [u128; 4] = [1, 2, 3, 4];
const CIRCLES: [u128; 2] = [1, 2];
const NAMES: [&str; 2] = ["Gifts", "Presents"];
/// The setup's slots come first: a history's slot `i` is `slot(BEFORE + i)`.
const BEFORE: u64 = 5;

#[derive(Clone, Copy, Debug)]
enum Entry {
    Create {
        author: u128,
        circle: u128,
        name: usize,
    },
    Rename {
        author: u128,
        circle: u128,
        name: usize,
    },
    Delete {
        author: u128,
        circle: u128,
    },
    Add {
        author: u128,
        circle: u128,
        member: u128,
    },
    Remove {
        author: u128,
        circle: u128,
        member: u128,
    },
    RemoveMember(u128),
    /// An admin admits `member` with `circle`, named `name`.
    Admit {
        author: u128,
        member: u128,
        circle: u128,
        name: usize,
    },
    /// An admin recovers `member` with a new `circle`, named `name`.
    Recover {
        author: u128,
        member: u128,
        circle: u128,
        name: usize,
    },
}

fn circle(n: u128) -> CircleId {
    CircleId(uuid::Uuid::from_u128(20 + n))
}

impl Entry {
    fn json(self) -> Value {
        let (kind, author, circle, field) = match self {
            Self::Create { author, circle, name } => {
                ("create", author, circle, Some(("name", json!(NAMES[name]))))
            }
            Self::Rename { author, circle, name } => {
                ("rename", author, circle, Some(("name", json!(NAMES[name]))))
            }
            Self::Delete { author, circle } => ("delete", author, circle, None),
            Self::Add { author, circle, member } => {
                ("add", author, circle, Some(("member", json!(member as u64))))
            }
            Self::Remove { author, circle, member } => {
                ("remove", author, circle, Some(("member", json!(member as u64))))
            }
            Self::RemoveMember(member) => {
                return json!({"kind": "removeMember", "member": member as u64});
            }
            Self::Admit { author, member, circle, name }
            | Self::Recover { author, member, circle, name } => {
                let kind = if matches!(self, Self::Admit { .. }) { "admit" } else { "recover" };
                return json!({"kind": kind, "author": author as u64, "member": member as u64,
                    "circle": circle as u64, "name": NAMES[name]});
            }
        };
        let mut entry = json!({"kind": kind, "author": author as u64, "circle": circle as u64});
        if let Some((key, value)) = field {
            entry[key] = value;
        }
        entry
    }

    fn author(self) -> u128 {
        match self {
            Self::Create { author, .. }
            | Self::Rename { author, .. }
            | Self::Delete { author, .. }
            | Self::Add { author, .. }
            | Self::Remove { author, .. }
            | Self::Admit { author, .. }
            | Self::Recover { author, .. } => author,
            Self::RemoveMember(_) => 1,
        }
    }

    /// The change this entry makes in `state`, at slot `at`. An admission or
    /// a recovery of a member whose circle is already made for them repeats
    /// the request the first was: the member's own keys and registered
    /// device, which "already done" compares. Any other recovery is a new
    /// request, with keys and a device nothing used before.
    fn change(self, state: &StoreState, at: u64) -> StoreChange {
        match self {
            Self::Admit { member, circle: c, name, .. } => {
                let role = if member <= 2 { MemberRole::Admin } else { MemberRole::Member };
                let mut change = admit(member as u8, role, member as u64);
                let StoreChange::AdmitMember { circle, keys, role, access, device, .. } =
                    &mut change
                else {
                    unreachable!()
                };
                *circle = new_circle(c, name);
                // A member whose circle is already made for them is admitted
                // again with the request the first was: the keys, role,
                // access and device they hold, which "already done" compares.
                let held = state.members.get(&id(member));
                let registered = state.devices.iter().find(|(_, held)| held.member == id(member));
                if let (Some(held), Some((registered, _))) = (held, registered) {
                    if crate::circle_effects::made_for(
                        state,
                        circle.circle,
                        &id(member),
                        NAMES[name],
                    ) {
                        *keys = held.keys;
                        *role = held.role;
                        *access = held.access.clone().expect("a member admitted with access");
                        *device = *registered;
                    }
                }
                change
            }
            Self::Recover { member, circle: c, name, .. } => {
                let current = state.members.get(&id(member)).map(|held| held.keys);
                let repeats =
                    crate::circle_effects::made_for(state, circle(c), &id(member), NAMES[name]);
                let (keys, device) = match current {
                    Some(held) if repeats => (held, member as u64),
                    _ => (keys((100 + at) as u8), 100 + at),
                };
                StoreChange::RecoverMember {
                    member: id(member),
                    expected: current.unwrap_or(keys),
                    keys,
                    access: None,
                    device: DeviceId(device),
                    device_name: "recovered".into(),
                    circle: new_circle(c, name),
                }
            }
            other => other.plain(),
        }
    }

    fn plain(self) -> StoreChange {
        match self {
            Self::Create { circle: c, name, .. } => create_circle(circle(c), NAMES[name]),
            Self::Rename { circle: c, name, .. } => {
                StoreChange::RenameCircle { circle: circle(c), name: NAMES[name].into() }
            }
            Self::Delete { circle: c, .. } => StoreChange::DeleteCircle { circle: circle(c) },
            Self::Add { circle: c, member, .. } => {
                StoreChange::AddCircleMember { circle: circle(c), member: id(member) }
            }
            Self::Remove { circle: c, member, .. } => remove_circle_member(circle(c), id(member)),
            Self::RemoveMember(member) => StoreChange::RemoveMember { member: id(member) },
            Self::Admit { .. } | Self::Recover { .. } => unreachable!("admissions need a state"),
        }
    }
}

/// The circle an admission makes: id `c` of this test's circles, named `name`.
fn new_circle(c: u128, name: usize) -> NewCircle {
    NewCircle { circle: circle(c), name: NAMES[name].into(), ..admission_circle(0) }
}

/// Every entry over the people and circles above, with removals of the
/// members other than Ana.
fn alphabet() -> Vec<Entry> {
    let mut entries = Vec::new();
    for author in PEOPLE {
        for circle in CIRCLES {
            entries.push(Entry::Delete { author, circle });
            for name in 0..NAMES.len() {
                entries.push(Entry::Create { author, circle, name });
                entries.push(Entry::Rename { author, circle, name });
            }
            for member in PEOPLE {
                entries.push(Entry::Add { author, circle, member });
                entries.push(Entry::Remove { author, circle, member });
            }
        }
    }
    // Only an admin admits or recovers (`Authority` decides that).
    for author in [1, 2] {
        for member in PEOPLE {
            for circle in CIRCLES {
                for name in 0..NAMES.len() {
                    entries.push(Entry::Admit { author, member, circle, name });
                    entries.push(Entry::Recover { author, member, circle, name });
                }
            }
        }
    }
    entries.extend([2, 3, 4].map(Entry::RemoveMember));
    entries
}

fn outcome(checked: &Result<Option<AdminEffect>, OperationRejection>) -> &'static str {
    match checked {
        Ok(Some(_)) => "applied",
        Ok(None) => "unchanged",
        Err(OperationRejection::NotAllowed) => "notAllowed",
        Err(OperationRejection::TargetGone(EntryTarget::Circle(_))) => "goneCircle",
        Err(OperationRejection::TargetGone(EntryTarget::Member(_))) => "goneMember",
        Err(other) => panic!("a circle entry refused as {other:?}, which the model doesn't name"),
    }
}

fn observe(state: &StoreState, outcome: &str) -> Value {
    let standing: Vec<_> = PEOPLE
        .iter()
        .map(|&m| {
            let standing = match state.members.get(&id(m)) {
                None => "absent",
                Some(member) if member.removed => "removed",
                Some(_) => "active",
            };
            json!([m as u64, standing])
        })
        .collect();
    let circles: Vec<_> = CIRCLES
        .iter()
        .map(|&c| {
            let record = state.circles.get(&circle(c)).map(|record| {
                let members: Vec<_> = record.members.iter().map(|m| m.0.as_u128() as u64).collect();
                let deleted =
                    state.circle_deletions.get(&circle(c)).map(|slot| slot.get() - BEFORE);
                json!({"name": record.name, "members": members, "deleted": deleted})
            });
            json!([c as u64, record])
        })
        .collect();
    json!({"outcome": outcome, "standing": standing, "circles": circles})
}

#[test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
fn circle_lifecycle_matches_lean() {
    let entries = alphabet();
    // Ben makes Gifts and adds Cy; Cy makes Presents alone. Every pair of
    // entries follows, so each entry meets live circles and, after an entry
    // that empties or deletes one, a deleted one.
    let start = [
        Entry::Create { author: 2, circle: 1, name: 0 },
        Entry::Add { author: 2, circle: 1, member: 3 },
        Entry::Create { author: 3, circle: 2, name: 1 },
    ];
    let mut histories = Vec::new();
    for first in &entries {
        for second in &entries {
            histories.push([start.as_slice(), &[*first, *second]].concat());
        }
    }
    let pairs = histories.len();
    for seed in 1..=256u64 {
        let mut rng = seed;
        histories.push(
            (0..14)
                .map(|_| {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    entries[(rng % entries.len() as u64) as usize]
                })
                .collect(),
        );
    }
    let cases: Vec<Vec<Value>> =
        histories.iter().map(|history| history.iter().map(|e| e.json()).collect()).collect();
    let input = json!({"kind": "circleLifecycle", "people": PEOPLE.map(|m| m as u64),
        "active": [1, 2, 3], "circles": CIRCLES.map(|c| c as u64), "histories": cases});
    let lean = crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input);
    let start = with_a_plain_member(&household());
    let mut seen = BTreeMap::<&str, usize>::new();
    let lean = lean.as_array().unwrap();
    assert_eq!(lean.len(), histories.len());
    for ((history, case), expected) in histories.iter().zip(&cases).zip(lean) {
        assert_eq!(expected.as_array().unwrap().len(), history.len(), "{}", json!(case));
        let mut state = start.clone();
        for (i, entry) in history.iter().enumerate() {
            let slot = slot(BEFORE + i as u64 + 1);
            let author = entry.author();
            let checked = checked_change(
                &state,
                &header(author as u64, author, slot.get() - 1),
                slot,
                &entry.change(&state, slot.get()),
                &BTreeMap::new(),
                &[],
            );
            let outcome = outcome(&checked);
            *seen.entry(outcome).or_default() += 1;
            if let Ok(Some(effect)) = checked {
                state = effect.state;
            }
            assert_eq!(observe(&state, outcome), expected[i], "slot {} of {}", i + 1, json!(case));
        }
    }
    for kind in ["applied", "unchanged", "notAllowed", "goneCircle", "goneMember"] {
        assert!(seen.contains_key(kind), "no history reached {kind}: {seen:?}");
    }
    eprintln!(
        "circle lifecycle: {pairs} pairs after a fixed start + {} seeded histories; {seen:?}",
        histories.len() - pairs
    );
}

/// Removing a circle's last member deletes the circle at the removal's slot;
/// a circle someone else remains in survives (§14.1).
#[test]
fn removing_a_circles_last_member_deletes_it_at_that_slot() {
    let shared = CircleId(uuid::Uuid::from_u128(13));
    let mut state = household();
    for change in [
        create_circle(gifts(), "Gifts"),
        create_circle(shared, "Shared"),
        StoreChange::AddCircleMember { circle: shared, member: id(1) },
    ] {
        state = apply(&state, 2, change).unwrap().unwrap().state;
    }
    let removed =
        apply(&state, 1, StoreChange::RemoveMember { member: id(2) }).unwrap().unwrap().state;
    assert_eq!(removed.circle_deletions.get(&gifts()), Some(&slot(4)));
    assert!(removed.circles[&gifts()].members.is_empty());
    assert_eq!(removed.circle_deletions.get(&shared), None);
    assert_eq!(removed.circles[&shared].members, [id(1)].into());
}

/// An admission applies the member, their device and their circle together:
/// the member is the circle's first and only member, at the base the entry
/// names, and the effect says which circle it made (§13.2).
#[test]
fn an_admission_makes_the_member_their_device_and_circle_in_one_effect() {
    let before = household();
    let admission = admit(4, MemberRole::Member, 4);
    let effect = apply(&before, 1, admission.clone()).unwrap().unwrap();
    let made = admission_circle(4);
    assert_eq!(effect.change.made_circle(), Some(&made.circle));
    assert!(effect.state.members.contains_key(&id(4)));
    assert!(effect.state.devices.contains_key(&DeviceId(4)));
    let record = &effect.state.circles[&made.circle];
    assert_eq!(record.members, [id(4)].into());
    assert_eq!((record.name.as_str(), record.base), (made.name.as_str(), made.breaking_version));
    assert!(!before.circles.contains_key(&made.circle));
    // Neither another admission's circle nor the person's earlier circle
    // changes.
    assert_eq!(effect.state.circles.len(), before.circles.len() + 1);
}

/// A circle id the history already uses, live or deleted, refuses the
/// admission and changes nothing: a request naming it is never offered, and
/// a forged entry that does is refused (§13.2).
#[test]
fn an_admission_naming_a_used_circle_is_refused_and_changes_nothing() {
    let mut state = with_a_plain_member(&household());
    let admission = admit(4, MemberRole::Member, 4);
    let used = admission_circle(4).circle;
    state = apply(&state, 3, create_circle(used, "Gifts")).unwrap().unwrap().state;
    for deleted in [false, true] {
        if deleted {
            state = apply(&state, 3, StoreChange::DeleteCircle { circle: used })
                .unwrap()
                .unwrap()
                .state;
        }
        let refused = apply(&state, 1, admission.clone());
        assert!(
            matches!(&refused, Err(OperationRejection::TargetGone(EntryTarget::Circle(c))) if *c == used),
            "deleted: {deleted}"
        );
        assert!(!state.members.contains_key(&id(4)));
    }
}

/// The same admission again, once its circle holds the member under its
/// name, changes nothing; the circle missing or another name is a different
/// request (§13.2).
#[test]
fn an_admission_repeated_with_its_circle_present_is_already_done() {
    let admission = admit(4, MemberRole::Member, 4);
    let state = apply(&household(), 1, admission.clone()).unwrap().unwrap().state;
    assert!(apply(&state, 1, admission.clone()).unwrap().is_none());
    let StoreChange::AdmitMember { circle, .. } = &admission else { unreachable!() };
    let mut renamed = circle.clone();
    renamed.name = "another".into();
    let mut other = admission.clone();
    let StoreChange::AdmitMember { circle, .. } = &mut other else { unreachable!() };
    *circle = renamed;
    assert!(matches!(
        apply(&state, 1, other),
        Err(OperationRejection::TargetGone(EntryTarget::Member(_)))
    ));
    // The member there without their circle is not the admission done: the
    // member exists, so the entry has nothing to admit.
    let mut without = state.clone();
    without.circles.remove(&admission_circle(4).circle);
    assert!(matches!(
        apply(&without, 1, admission),
        Err(OperationRejection::TargetGone(EntryTarget::Member(_)))
    ));
}

/// An admission records the person's name and circle in the member's state,
/// which a device of theirs reads as it registers; a recovery records the
/// new circle and no name (§13.2).
#[test]
fn an_admission_records_its_members_facts_and_a_recovery_replaces_them() {
    use coven_database::Joined;
    let admission = admit(4, MemberRole::Member, 4);
    let state = apply(&household(), 1, admission.clone()).unwrap().unwrap().state;
    assert_eq!(
        state.members[&id(4)].joined,
        Some(Joined { person: Some("Person".into()), circle: admission_circle(4).circle })
    );
    // The store's creator was made by no admission.
    assert_eq!(state.members[&id(1)].joined, None);
    let recovery = StoreChange::RecoverMember {
        member: id(4),
        expected: keys(4),
        keys: keys(9),
        access: None,
        device: DeviceId(9),
        device_name: "new phone".into(),
        circle: admission_circle(9),
    };
    let recovered = apply(&state, 1, recovery).unwrap().unwrap().state;
    assert_eq!(
        recovered.members[&id(4)].joined,
        Some(Joined { person: None, circle: admission_circle(9).circle })
    );
}
