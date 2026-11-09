//! `checked_change`, whose circle entries and member removals these effects
//! carry out, against the sequence model's circle lifecycle
//! (`CircleLifecycle.effect`): the same circle histories, slot by slot, give
//! the same verdicts, names, members, deletion slots and member standing
//! (§15.5, §15.8, §15.9).
use crate::effects::checked_change;
use crate::effects::tests::{
    apply, create_circle, gifts, header, household, id, with_a_plain_member,
};
use crate::{EntryTarget, OperationRejection};
use coven_database::{AdminEffect, StoreState};
use coven_format::admin::StoreChange;
use coven_foundation::id_source::CircleId;
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
    Create { author: u128, circle: u128, name: usize },
    Rename { author: u128, circle: u128, name: usize },
    Delete { author: u128, circle: u128 },
    Add { author: u128, circle: u128, member: u128 },
    Remove { author: u128, circle: u128, member: u128 },
    RemoveMember(u128),
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
            | Self::Remove { author, .. } => author,
            Self::RemoveMember(_) => 1,
        }
    }

    fn change(self) -> StoreChange {
        match self {
            Self::Create { circle: c, name, .. } => create_circle(circle(c), NAMES[name]),
            Self::Rename { circle: c, name, .. } => {
                StoreChange::RenameCircle { circle: circle(c), name: NAMES[name].into() }
            }
            Self::Delete { circle: c, .. } => StoreChange::DeleteCircle { circle: circle(c) },
            Self::Add { circle: c, member, .. } => {
                StoreChange::AddCircleMember { circle: circle(c), member: id(member) }
            }
            Self::Remove { circle: c, member, .. } => {
                StoreChange::RemoveCircleMember { circle: circle(c), member: id(member) }
            }
            Self::RemoveMember(member) => StoreChange::RemoveMember { member: id(member) },
        }
    }
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
                &entry.change(),
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
