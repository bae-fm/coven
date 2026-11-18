//! Rust and Lean agree on what each admin entry does to the store state.

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
                person: "Person".into(),
                circle: circle(*m as u8),
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
    db: &coven_database::StoreDatabase,
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
                device_name: "Ana".into(),
                key,
                key_hash: group.ring.store_key(key).unwrap().hash(),
                person: "Ana".into(),
                circle: circle(1),
                circles: vec![],
            },
            1,
        ),
        (header(1, 1, 1), StoreChange::SetUpStorage { access: access(1) }, 1),
        (header(1, 1, 2), admit(2, MemberRole::Admin, 2), 1),
    ];
    for (header, change, seed) in initial {
        let state = db.store_state().await.unwrap();
        let effect =
            checked_change(&state, &header, slot(header.after + 1), &change, &BTreeMap::new(), &[])
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
        let hdr = header(*author, member, i as u64 + 3);
        let at = slot(i as u64 + 4);
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
                Action::Admit(m, ..) | Action::Replace(m) => seeds[*m as usize] = (i + 10) as u8,
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
        let retired: Vec<_> =
            state.members.iter().filter(|(_, m)| m.removed).map(|(m, _)| m.0.as_u128()).collect();
        let devices: Vec<_> =
            state.devices.iter().map(|(d, data)| json!([d.0, data.member.0.as_u128()])).collect();
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
