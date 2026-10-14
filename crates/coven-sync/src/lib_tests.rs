//! CurrentReplay adapter: modeled membership and fates, with Rust-only checks separate.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use coven_database::{
    DatabaseBuilder, DropReason, EntryOutcome, StoreLog, StoreLogCheck, StoreLogReplay,
    StoreLogState,
};
use coven_format::{
    store_log::{MemberRole, StoreChange},
    Object,
};
use coven_foundation::id_source::MemberId;
use coven_foundation::{
    files::{StoreDir, StoreLayout},
    id_source::{CircleId, DeviceId, SequentialIds, StoreId},
};
use coven_merge::Audience;
use serde_json::{json, Value};

use crate::{
    effects::tests::{raise, rotation, snapshot},
    replay::circles::tests::circle_history,
    replay::tests::*,
};

pub(crate) struct Generator(pub(crate) u64);

impl Generator {
    pub(crate) fn pick(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }

    pub(crate) fn history(&mut self) -> History {
        let mut h = household(MemberRole::Admin, MemberRole::Admin);
        for (id, name) in [(0, "Gifts"), (1, "Notes")] {
            h.all(0, 0, make(id, name));
            h.all(0, 0, join(id, 1));
            h.all(0, 0, join(id, 2));
        }
        // Arbitrary actions use a complete past. Concurrent branches exercise
        // boundaries, renames, deletion and rotations, whose policies agree.
        // Independent membership changes and removal tiers are compared below;
        // publication boundaries are compared separately.
        for _ in 0..3 {
            let author = self.pick(5) as u8;
            let mut change = self.action(&replay(&h.entries).state);
            // CurrentReplay records only a member's own access; an admin's
            // replacement for another member is compared by the S3 key runner.
            if let StoreChange::SetAccess { member: target, .. } = &mut change {
                *target = member(author);
            }
            h.all(author, u64::from(author), change);
            let past: Vec<_> = (0..h.entries.len()).collect();
            let mut changes = self.concurrent_changes();
            if self.pick(2) == 0 {
                changes.swap(0, 1);
            }
            for (author, change) in [0, 2].into_iter().zip(changes) {
                h.push(author, u64::from(author), &past, change);
            }
        }
        h
    }

    fn audience(&mut self) -> Audience {
        match self.pick(3) {
            0 => Audience::Store,
            c => Audience::Circle(circle(c as u64 - 1)),
        }
    }

    fn concurrent_changes(&mut self) -> [StoreChange; 2] {
        let reset = |audience| StoreChange::Reset {
            snapshot: snapshot(30, audience),
        };
        let audience = self.audience();
        match self.pick(19) {
            0 => [rotation(audience.clone(), 100), rotation(audience, 101)],
            1 => [delete(0), rotation(Audience::Circle(circle(0)), 102)],
            2 => [rename(0, "Birthdays"), rotation(audience, 103)],
            3 => [reset(audience.clone()), rotation(audience, 104)],
            4 => [
                reset(audience.clone()),
                raise(2, 30 + self.pick(2) as u64, audience),
            ],
            5 => [raise(2, 30, audience.clone()), raise(2, 31, audience)],
            6 => [raise(2, 30, audience.clone()), raise(3, 31, audience)],
            7 => [reset(self.audience()), raise(2, 30, audience)],
            8 => [raise(2, 30, self.audience()), raise(2, 31, audience)],
            9 => [delete(0), raise(2, 30, Audience::Circle(circle(0)))],
            10 => [rename(0, "Birthdays"), rename(0, "Presents")],
            11 => [rename(0, "Birthdays"), delete(0)],
            12 => [delete(0), remove(0)],
            13 => [leave(0, 1), delete(0)],
            14 => [leave(0, 0), leave(0, 2)],
            15 => [join(0, 1), leave(0, 1)],
            16 => [remove(1), remove(2)],
            17 => [leave(0, 0), join(0, 1)],
            18 => [leave(0, 0), leave(0, 0)],
            _ => unreachable!(),
        }
    }

    fn action(&mut self, view: &StoreLogState) -> StoreChange {
        let m = self.pick(5) as u8;
        let c = self.pick(3) as u64;
        let role = if self.pick(2) == 0 {
            MemberRole::Admin
        } else {
            MemberRole::Member
        };
        match self.pick(14) {
            0 => add(m, role),
            1 => remove(m),
            2 => crate::replay::tests::role(m, role),
            3 => device(self.pick(9) as u64),
            4 => {
                let devices: Vec<_> = view
                    .devices
                    .iter()
                    .filter_map(|(id, d)| (!d.removed()).then_some(*id))
                    .collect();
                if devices.is_empty() {
                    device(self.pick(9) as u64)
                } else {
                    StoreChange::RemoveDevice {
                        device: devices[self.pick(devices.len())],
                    }
                }
            }
            5 => make(c, &format!("Circle {}", self.pick(3))),
            6 => rename(c, &format!("Circle {}", self.pick(3))),
            7 => delete(c),
            8 => join(c, m),
            9 => leave(c, m),
            10 => StoreChange::RaiseSchema {
                version: self.pick(3) as u32 + 1,
                snapshot: snapshot(self.pick(5) as u64 + 1, self.audience()),
            },
            11 => StoreChange::Reset {
                snapshot: snapshot(
                    self.pick(5) as u64 + 1,
                    if self.pick(4) == 0 {
                        Audience::Store
                    } else {
                        Audience::Circle(circle(c))
                    },
                ),
            },
            12 => StoreChange::SetAccess {
                member: member(0),
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: if self.pick(3) == 0 {
                        "fixture-access-key".into()
                    } else {
                        format!("key-{}", self.pick(3))
                    },
                    principal: coven_format::S3Principal::Console,
                },
            },
            13 => StoreChange::RotateKey {
                audience: self.audience(),
                key: key(self.pick(100) as u64 + 1000),
                key_hash: [0x91; 32],
            },
            _ => unreachable!(),
        }
    }
}

fn member_number(id: &MemberId) -> u64 {
    (0..5)
        .find(|&i| member(i) == *id)
        .expect("generated member") as u64
}
fn circle_number(id: CircleId) -> u64 {
    u64::try_from(id.0.as_u128()).unwrap()
}
pub(crate) fn audience_number(a: &Audience) -> u64 {
    match a {
        Audience::Store => 0,
        Audience::Circle(id) => circle_number(*id) + 1,
    }
}
fn role_number(role: MemberRole) -> u64 {
    match role {
        MemberRole::Admin => 0,
        MemberRole::Member => 1,
    }
}

pub(crate) fn input(h: &History, stored: &[u64], received: &[Vec<usize>]) -> Value {
    assert_eq!(stored.len(), h.entries.len());
    let publications = publication_times(h, stored);
    let entries: Vec<_> = h.entries.iter().enumerate().map(|(i,entry)| {
        if let StoreChange::RaiseSchema { snapshot, .. } | StoreChange::Reset { snapshot } = &entry.change {
            assert_eq!(snapshot.device, DeviceId(900), "snapshot numbers identify one fixture writer");
        }
        let past: Vec<_> = h.entries[..i].iter().enumerate().filter_map(|(j,e)| crate::replay::had_read(entry,e).then_some(j)).collect();
        let action = match &entry.change {
            StoreChange::CreateStore { access, key, key_hash, .. } => json!({"kind":0,"access":serde_json::to_string(access).unwrap(),"key":key_number(*key),"key_hash":key_hash}),
            StoreChange::AddMember { member, role, access, .. } => json!({"kind":1,"member":member_number(member),"role":role_number(*role),"access":serde_json::to_string(access).unwrap()}),
            StoreChange::RemoveMember { member } => json!({"kind":2,"member":member_number(member)}),
            StoreChange::ChangeRole { member, role } => json!({"kind":3,"member":member_number(member),"role":role_number(*role)}),
            StoreChange::AddDevice { device, replaces, .. } => {
                assert!(replaces.is_none(), "CurrentReplay does not model replacement registrations");
                json!({"kind":4,"member":member_number(&entry.author),"device":device.0})
            },
            StoreChange::RemoveDevice { device } => {
                let view = crate::replay(&past.iter().map(|&i| h.entries[i].clone()).collect::<Vec<_>>(), &publications).state;
                let owner = &view.devices[device];
                assert!(!owner.removed(), "generator may remove only an observed device");
                json!({"kind":5,"member":member_number(&owner.member),"device":device.0})
            }
            StoreChange::CreateCircle { circle, name, key, key_hash } => json!({"kind":6,"circle":circle_number(*circle),"name":name,"key":key_number(*key),"key_hash":key_hash}),
            StoreChange::RenameCircle { circle, name } => json!({"kind":7,"circle":circle_number(*circle),"name":name}),
            StoreChange::DeleteCircle { circle } => json!({"kind":8,"circle":circle_number(*circle)}),
            StoreChange::AddCircleMember { circle, member } => json!({"kind":9,"circle":circle_number(*circle),"member":member_number(member)}),
            StoreChange::RemoveCircleMember { circle, member, .. } => json!({"kind":10,"circle":circle_number(*circle),"member":member_number(member)}),
            StoreChange::RaiseSchema { version, snapshot } => json!({"kind":11,"version":version,"snapshot":{"audience":audience_number(&snapshot.audience),"number":snapshot.number}}),
            StoreChange::Reset { snapshot } => json!({"kind":13,"snapshot":{"audience":audience_number(&snapshot.audience),"number":snapshot.number}}),
            StoreChange::RotateKey { audience, key, key_hash } => json!({"kind":15,"audience":audience_number(audience),"key":key_number(*key),"key_hash":key_hash}),
            StoreChange::SetAccess { member, access } => json!({"kind":14,"member":member_number(member),"access":serde_json::to_string(access).unwrap()}),
            StoreChange::ReplaceMemberKeys { .. } => unreachable!("CurrentReplay has no member-key replacement"),
        };
        // Attempt at the latest observed landing; a later stored time is a retry.
        let attempted = past.iter().map(|&p| stored[p]).max().unwrap_or(stored[0]);
        json!({"author":member_number(&entry.author),"device":entry.position.device.0,"past":past,"action":action,"stored":stored[i],"attempted":attempted})
    }).collect();
    json!({"entries":entries,"window":30,"received":received})
}

pub(crate) fn projected(h: &History, replay: &StoreLogReplay) -> Value {
    let state = &replay.state;
    let mut members: Vec<_> = state
        .members
        .iter()
        .filter_map(|(id, m)| (!m.removed).then_some([member_number(id), role_number(m.role)]))
        .collect();
    members.sort();
    let mut access: Vec<_> = state
        .members
        .iter()
        .map(|(id, m)| (member_number(id), serde_json::to_string(&m.access).unwrap()))
        .collect();
    access.sort();
    let devices: Vec<_> = state
        .devices
        .iter()
        .filter_map(|(id, d)| (!d.removed()).then_some([id.0, member_number(&d.member)]))
        .collect();
    let circles: Vec<_> = state
        .circles
        .iter()
        .filter(|(id, _)| !state.circle_deletions.contains_key(id))
        .map(|(id, c)| {
            let mut members: Vec<_> = c.members.iter().map(member_number).collect();
            members.sort();
            json!([circle_number(*id), c.name, members])
        })
        .collect();
    let index = |entry| h.entries.iter().position(|e| e.position == entry).unwrap();
    let circle_causes: Vec<_> = state
        .circle_deletions
        .iter()
        .map(|(circle, entry)| [circle_number(*circle) as usize, index(*entry)])
        .collect();
    let mut versions = vec![];
    for (a, v) in &state.schema {
        versions.push(json!([
            audience_number(a),
            v.number,
            v.snapshot.number,
            index(v.entry)
        ]));
    }
    let resets: Vec<_> = state
        .resets
        .iter()
        .map(|(a, s)| [audience_number(a), s.number])
        .collect();
    let kept: Vec<_> = h
        .entries
        .iter()
        .enumerate()
        .filter_map(|(i, e)| {
            (replay.entries.get(&e.position) == Some(&EntryOutcome::Kept)).then_some(i)
        })
        .collect();
    let dropped: Vec<_> = h
        .entries
        .iter()
        .enumerate()
        .filter_map(|(i, e)| {
            matches!(
                replay.entries.get(&e.position),
                Some(EntryOutcome::Dropped(_))
            )
            .then_some(i)
        })
        .collect();
    let late: Vec<_> = h
        .entries
        .iter()
        .enumerate()
        .filter_map(|(i, e)| {
            (replay.entries.get(&e.position)
                == Some(&EntryOutcome::Dropped(DropReason::LandedTooLate)))
            .then_some(i)
        })
        .collect();
    json!({"landed_too_late":late,"created":state.store.is_some(),"members":members,"access":access,"devices":devices,"circles":circles,"versions":versions,"resets":resets,"kept":kept,"dropped":dropped,"circle_causes":circle_causes})
}

pub(crate) fn key_number(id: coven_foundation::id_source::KeyId) -> u64 {
    u64::try_from(id.0.as_u128()).unwrap()
}

pub(crate) fn run_lean(variable: &str, input: &Value) -> std::process::Output {
    let runner =
        std::env::var_os(variable).unwrap_or_else(|| panic!("{variable} must name a Lean runner"));
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start Lean runner");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.to_string().as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

pub(crate) fn lean_results(inputs: &[Value]) -> Vec<Vec<Value>> {
    let output = run_lean("COVEN_STORELOG_LEAN", &json!({"histories": inputs}));
    assert!(
        output.status.success(),
        "Lean runner failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Vec<Vec<Value>> =
        serde_json::from_slice(&output.stdout).expect("Lean result array");
    assert_eq!(actual.len(), inputs.len());
    for (results, input) in actual.iter().zip(inputs) {
        assert_eq!(results.len(), input["received"].as_array().unwrap().len());
    }
    actual
}

fn arrival_order(h: &History, mut pick: impl FnMut(usize) -> usize) -> Vec<usize> {
    let mut order = Vec::new();
    while order.len() < h.entries.len() {
        let ready: Vec<_> =
            h.entries
                .iter()
                .enumerate()
                .filter_map(|(i, e)| {
                    (!order.contains(&i)
                        && h.entries.iter().enumerate().all(|(j, prior)| {
                            !crate::replay::had_read(e, prior) || order.contains(&j)
                        }))
                    .then_some(i)
                })
                .collect();
        assert!(
            !ready.is_empty(),
            "history must admit a causal arrival order"
        );
        order.push(ready[pick(ready.len())]);
    }
    order
}

pub(crate) fn received_prefixes(orders: &[Vec<usize>]) -> Vec<Vec<usize>> {
    orders
        .iter()
        .flat_map(|order| (0..=order.len()).map(|n| order[..n].to_vec()))
        .collect()
}

pub(crate) fn publication_times(
    h: &History,
    stored: &[u64],
) -> std::collections::BTreeMap<coven_format::value::EntryId, std::time::SystemTime> {
    assert_eq!(h.entries.len(), stored.len());
    h.entries
        .iter()
        .zip(stored)
        .map(|(e, day)| {
            (
                e.position,
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(day * 86400),
            )
        })
        .collect()
}

pub(crate) fn compare_arrivals(
    h: &History,
    orders: &[Vec<usize>],
    actual: &[Value],
    context: &Value,
) {
    let stored: Vec<_> = context["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["stored"].as_u64().unwrap())
        .collect();
    let publications = publication_times(h, &stored);
    assert_eq!(actual.len(), orders.len() * (h.entries.len() + 1));
    for (order, results) in orders.iter().zip(actual.chunks_exact(h.entries.len() + 1)) {
        let mut log = StoreLog::default();
        assert_eq!(results[0], projected(h, &log.replay));
        for (n, &i) in order.iter().enumerate() {
            let entries: Vec<_> = order[..=n].iter().map(|&j| h.entries[j].clone()).collect();
            let result = crate::replay(&entries, &publications);
            let (checked, incremental) =
                crate::replay_entry(&log, h.entries[i].clone(), &publications);
            assert_eq!(
                incremental, result,
                "cached replay, order {order:?}, prefix {n}"
            );
            assert_eq!(
                results[n + 1],
                projected(h, &result),
                "order {order:?}, prefix {}, input {context}",
                n + 1
            );
            log.entries.push(checked);
            log.replay = incremental;
        }
        assert_eq!(log.replay, crate::replay(&h.entries, &publications));
    }
}

#[test]
#[ignore = "requires COVEN_STORELOG_LEAN, supplied by scripts/check.sh"]
fn lean_differential_generated() {
    // Each batch sends complete histories and several received sets per history.
    for first in (1..=2048).step_by(64) {
        let cases: Vec<_> = (first..first + 64)
            .map(|seed| {
                let mut generator = Generator(seed);
                let h = generator.history();
                let orders = vec![
                    arrival_order(&h, |_| 0),
                    arrival_order(&h, |n| n - 1),
                    arrival_order(&h, |n| generator.pick(n)),
                ];
                // Duration units are days; tied landings and gaps fit within the
                // 30-day window. Late landings are asserted separately.
                let mut time = seed * 100;
                let stored: Vec<_> = h
                    .entries
                    .iter()
                    .map(|_| {
                        time += generator.pick(2) as u64;
                        time
                    })
                    .collect();
                let input = input(&h, &stored, &received_prefixes(&orders));
                (h, orders, input)
            })
            .collect();
        let inputs: Vec<_> = cases.iter().map(|(_, _, input)| input.clone()).collect();
        let actual = lean_results(&inputs);
        for ((h, orders, input), result) in cases.iter().zip(actual) {
            compare_arrivals(h, orders, &result, input);
        }
    }
}

pub(crate) fn orders(h: &History) -> Vec<Vec<usize>> {
    vec![arrival_order(h, |_| 0), arrival_order(h, |n| n - 1)]
}

fn sequential_times(h: &History) -> Vec<u64> {
    (0..h.entries.len() as u64).collect()
}

#[test]
#[ignore = "requires COVEN_STORELOG_LEAN, supplied by scripts/check.sh"]
fn lean_differential_checked_examples() {
    let mut histories = vec![History::new()];
    let mut creation = History::new();
    creation.all(0, 0, make(0, "Gifts"));
    histories.push(creation);

    let mut rotations = circle_history();
    let past: Vec<_> = (0..8).collect();
    for (author, writer, id) in [(0, 0, 10), (1, 1, 11), (3, 3, 12)] {
        rotations.push(
            author,
            writer,
            &past,
            rotation(Audience::Circle(circle(0)), id),
        );
    }
    rotations.push(
        1,
        1,
        &[0, 1, 2, 3, 4, 5, 6, 7, 9],
        rotation(Audience::Store, 13),
    );
    assert_eq!(rotations.drops(&replay(&rotations.entries)), [10]);
    histories.push(rotations);

    for action in [remove(1), delete(0)] {
        let mut h = circle_history();
        h.push(0, 0, &past, action);
        h.push(1, 1, &past, rotation(Audience::Circle(circle(0)), 11));
        assert!(h.drops(&replay(&h.entries)).is_empty());
        histories.push(h);
    }
    let mut deletion = circle_history();
    deletion.push(0, 0, &past, delete(0));
    deletion.push(1, 1, &past, join(0, 2));
    assert_eq!(deletion.drops(&replay(&deletion.entries)), [9]);
    histories.push(deletion);

    let mut empty = circle_history();
    empty.push(0, 0, &past, leave(0, 1));
    empty.push(0, 2, &past, remove(0));
    assert!(replay(&empty.entries)
        .state
        .circle_deletions
        .contains_key(&circle(0)));
    histories.push(empty);

    let mut last_admins = History::new();
    last_admins.all(0, 0, add(1, MemberRole::Admin));
    last_admins.push(0, 0, &[0, 1], remove(1));
    last_admins.push(1, 1, &[0, 1], remove(0));
    assert_eq!(last_admins.drops(&replay(&last_admins.entries)), [3]);
    assert_eq!(
        replay(&last_admins.entries).entries[&last_admins.entries[3].position],
        EntryOutcome::Dropped(DropReason::NoAdminLeft)
    );
    histories.push(last_admins);

    let mut devices = History::new();
    devices.push(0, 0, &[0], device(1));
    devices.push(0, 2, &[0], device(2));
    assert_eq!(
        projected(&devices, &replay(&devices.entries)),
        expected(
            &[[0, 0]],
            &[0],
            &[[0, 0], [1, 0], [2, 0]],
            json!([]),
            &[0, 1, 2],
            &[],
        )
    );
    histories.push(devices);

    // CurrentKeyExamples.removals: both admins remove a different member,
    // then the remaining circle member and an admin rotate their audiences.
    let mut removals = History::new();
    removals.all(0, 0, add(1, MemberRole::Admin));
    removals.all(0, 0, add(2, MemberRole::Member));
    removals.all(0, 0, add(3, MemberRole::Member));
    for m in 1..=3 {
        removals.all(m, u64::from(m), device(u64::from(m)));
    }
    removals.all(1, 1, make(0, "Gifts"));
    removals.all(1, 1, join(0, 2));
    removals.all(1, 1, join(0, 3));
    let past: Vec<_> = (0..10).collect();
    removals.push(0, 0, &past, remove(2));
    removals.push(1, 1, &past, remove(3));
    removals.all(1, 1, rotation(Audience::Circle(circle(0)), 2));
    removals.all(0, 0, rotation(Audience::Store, 3));
    removals.all(1, 1, rotation(Audience::Store, 4));
    for n in [12, 14, 15] {
        let prefix = removals.prefix(n);
        assert_eq!(
            projected(&prefix, &replay(&prefix.entries)),
            expected(
                &[[0, 0], [1, 0]],
                &[0, 1, 2, 3],
                &[[0, 0], [1, 1]],
                json!([[0, "Gifts", [1]]]),
                &(0..n).collect::<Vec<_>>(),
                &[],
            )
        );
        histories.push(prefix);
    }

    histories.extend(crate::conflicts::tests::independent_histories());
    histories.extend(
        crate::conflicts::tests::contradictory_histories()
            .into_iter()
            .map(|(h, _)| h),
    );

    let inputs: Vec<_> = histories
        .iter()
        .map(|h| input(h, &sequential_times(h), &received_prefixes(&orders(h))))
        .collect();
    for ((h, input), actual) in histories.iter().zip(&inputs).zip(lean_results(&inputs)) {
        compare_arrivals(h, &orders(h), &actual, input);
    }
}

fn expected(
    members: &[[u64; 2]],
    access_members: &[u64],
    devices: &[[u64; 2]],
    circles: Value,
    kept: &[usize],
    dropped: &[usize],
) -> Value {
    let access = serde_json::to_string(&coven_format::MemberAccess::S3AccessKey {
        access_key_id: "fixture-access-key".into(),
        principal: coven_format::S3Principal::Console,
    })
    .unwrap();
    let access: Vec<_> = access_members.iter().map(|m| json!([m, access])).collect();
    json!({"landed_too_late":[],"created":true,"members":members,"access":access,"devices":devices,
        "circles":circles,"versions":[],"resets":[],"kept":kept,"dropped":dropped,"circle_causes":[]})
}

#[test]
#[ignore = "requires COVEN_STORELOG_LEAN, supplied by scripts/check.sh"]
fn lean_differential_circle_replay() {
    let mut refill = circle_history();
    let past: Vec<_> = (0..8).collect();
    refill.push(0, 0, &past, leave(0, 1));
    refill.push(0, 2, &past, remove(0));
    refill.push(1, 1, &past, join(0, 2));
    let mut tiers = gifts();
    tiers.push(0, 0, &[0, 1, 2, 3, 4], leave(0, 1));
    tiers.push(1, 1, &[0, 1, 2, 3, 4], delete(0));
    for h in [refill, tiers]
        .into_iter()
        .chain(crate::replay::circles::tests::cause_histories())
    {
        let orders = orders(&h);
        let input = input(&h, &sequential_times(&h), &received_prefixes(&orders));
        let actual = lean_results(std::slice::from_ref(&input));
        compare_arrivals(&h, &orders, &actual[0], &input);
    }
}

#[test]
#[ignore = "requires COVEN_STORELOG_LEAN, supplied by scripts/check.sh"]
fn lean_differential_rejects_invalid_inputs() {
    let mut h = gifts();
    h.all(0, 0, rotation(Audience::Store, 20));
    let valid = input(&h, &sequential_times(&h), &[vec![0]]);
    for index in [0, 3, 5] {
        for hash in [json!([1]), json!(vec![256; 32])] {
            let mut bad = valid.clone();
            bad["entries"][index]["action"]["key_hash"] = hash;
            let output = run_lean("COVEN_STORELOG_LEAN", &json!({"histories":[bad]}));
            assert!(!output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stderr).contains("key hash must contain 32 bytes")
            );
        }
    }
    for (index, field, value, reason) in [
        (1, "past", json!([1]), "violates Valid"),
        (0, "attempted", json!(1), "attempt follows storage time"),
        (
            2,
            "attempted",
            json!(0),
            "recorded read follows attempt time",
        ),
    ] {
        let mut bad = valid.clone();
        bad["entries"][index][field] = value;
        let output = run_lean("COVEN_STORELOG_LEAN", &json!({"histories":[bad]}));
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(reason));
    }
    for (field, value, reason) in [
        ("received", json!([[1]]), "not causally closed"),
        ("received", json!([[99]]), "outside history"),
        ("entries", json!([]), "must contain creation"),
    ] {
        let mut bad = valid.clone();
        bad[field] = value;
        let output = run_lean("COVEN_STORELOG_LEAN", &json!({"histories":[bad]}));
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(reason));
    }
}

#[test]
fn current_projection_preserves_commitments_and_rust_only_data() {
    let mut h = circle_history();
    h.all(0, 0, rotation(Audience::Store, 20));
    h.all(0, 0, remove(1));
    h.all(0, 0, leave(0, 0));
    let query = input(&h, &sequential_times(&h), &[vec![0]]);
    for i in [0, 6, 8] {
        let (id, hash) = match &h.entries[i].change {
            StoreChange::CreateStore { key, key_hash, .. }
            | StoreChange::CreateCircle { key, key_hash, .. }
            | StoreChange::RotateKey { key, key_hash, .. } => (key, key_hash),
            _ => unreachable!(),
        };
        assert_eq!(query["entries"][i]["action"]["key"], json!(key_number(*id)));
        assert_eq!(query["entries"][i]["action"]["key_hash"], json!(hash));
    }
    for i in [9, 10] {
        let action = &query["entries"][i]["action"];
        assert!(action.get("key").is_none());
        assert!(action.get("key_hash").is_none());
        assert!(action.get("circles").is_none());
    }
    for entry in &h.entries {
        let object = Object::StoreLog(entry.clone());
        assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
    }
    let result = replay(&h.entries);
    assert_eq!(result.state.store.as_ref().unwrap().name, "Home");
    assert_eq!(result.state.members[&member(1)].keys, keys(1));
    assert_eq!(result.state.devices[&DeviceId(0)].name, "Ana’s phone");
    assert_eq!(result.state.devices[&DeviceId(1)].name, "Device 1");
    assert_eq!(newest_key(&h, Audience::Store), Some(key(20)));
    assert_eq!(newest_key(&h, Audience::Circle(circle(0))), Some(key(10)));
}

#[test]
#[should_panic(expected = "CurrentReplay does not model replacement registrations")]
fn projection_rejects_replacement_registrations() {
    let mut h = History::new();
    h.all(
        0,
        0,
        StoreChange::AddDevice {
            device: DeviceId(1),
            name: "Replacement".into(),
            replaces: Some(DeviceId(0)),
        },
    );
    input(&h, &sequential_times(&h), &[vec![0, 1]]);
}

#[test]
#[should_panic(expected = "snapshot numbers identify one fixture writer")]
fn projection_rejects_an_unmapped_snapshot_writer() {
    let mut h = History::new();
    let mut boundary = snapshot(1, Audience::Store);
    boundary.device = DeviceId(901);
    h.all(0, 0, StoreChange::Reset { snapshot: boundary });
    input(&h, &sequential_times(&h), &[vec![0, 1]]);
}

pub(crate) async fn open(directory: StoreDir) -> coven_database::Database {
    DatabaseBuilder::new(directory)
        .synced_tables(vec![])
        .migrations(vec![])
        .open()
        .await
        .unwrap()
}

#[tokio::test]
async fn cached_author_views_survive_reopening_and_a_deletions_reversal() {
    let mut h = gifts();
    h.push(0, 0, &[0, 1, 2, 3, 4], remove(1));
    h.push(1, 1, &[0, 1, 2, 3, 4], delete(0));
    let temporary = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(temporary.path().to_owned());
    let directory = layout
        .create_store_dir(
            StoreId(uuid::Uuid::from_u128(1)),
            "Home",
            &SequentialIds::new(),
        )
        .unwrap();
    let mut log = StoreLog::default();
    for i in [0, 1, 2, 3, 4, 6, 5] {
        let database = open(directory.clone()).await;
        assert_eq!(database.store_log().await.unwrap(), log);
        let previous = log.entries.clone();
        let (entry, result) = replay_entry(&log, h.entries[i].clone());
        database
            .apply_store_log::<crate::PendingRecord>(entry, result.clone(), Vec::new())
            .await
            .unwrap();
        log = database.store_log().await.unwrap();
        assert_eq!(log.replay, result);
        for old in previous {
            assert!(
                log.entries.contains(&old),
                "an applied author view never changes"
            );
        }
        if i == 6 {
            assert!(log.replay.state.circle_deletions.contains_key(&circle(0)));
        }
        database.close().await.unwrap();
    }
    assert_eq!(log.replay, replay(&h.entries));
    assert!(!log.replay.state.circle_deletions.contains_key(&circle(0)));
}

#[tokio::test]
async fn every_author_view_check_is_durable_and_reused_after_reopening() {
    let mut h = household(MemberRole::Admin, MemberRole::Member);
    h.all(0, 0, make(0, "Shared"));
    h.all(0, 0, join(0, 1));
    h.all(1, 1, make(1, "Private"));
    h.all(
        0,
        0,
        StoreChange::RemoveDevice {
            device: DeviceId(1),
        },
    );
    h.all(0, 0, rename(1, "Outside admin"));
    h.all(0, 0, remove(1));
    let temporary = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(temporary.path().to_owned());
    let directory = layout
        .create_store_dir(
            StoreId(uuid::Uuid::from_u128(1)),
            "Home",
            &SequentialIds::new(),
        )
        .unwrap();
    let mut log = StoreLog::default();
    for (index, entry) in h.entries.iter().enumerate() {
        let database = open(directory.clone()).await;
        assert_eq!(database.store_log().await.unwrap(), log);
        let (checked, result) = replay_entry(&log, entry.clone());
        assert_eq!(result, replay(&h.entries[..=index]));
        database
            .apply_store_log::<crate::PendingRecord>(checked.clone(), result.clone(), Vec::new())
            .await
            .unwrap();
        log.entries.push(checked);
        log.entries.sort_by_key(|e| e.entry.timestamp);
        log.replay = result;
        assert_eq!(database.store_log().await.unwrap(), log);
        database.close().await.unwrap();
    }
    assert_eq!(log.entries[8].check, StoreLogCheck::DeviceOwner(member(1)));
    assert_eq!(log.entries[9].check, StoreLogCheck::NotAllowed);
    assert_eq!(
        log.entries[10].check,
        StoreLogCheck::MemberRemoval(
            [
                (circle(0), coven_database::CircleRemoval::Member),
                (circle(1), coven_database::CircleRemoval::Circle)
            ]
            .into()
        )
    );
}

#[test]
#[ignore = "requires COVEN_STORELOG_LEAN, supplied by scripts/check.sh"]
fn lean_differential_online_administration() {
    let mut cases = Vec::new();
    for base in [History::new(), circle_history()] {
        for author in 0..5 {
            for change in [
                add(4, MemberRole::Member),
                role(0, MemberRole::Member),
                role(4, MemberRole::Admin),
                remove(0),
                remove(4),
                device(20),
                StoreChange::RemoveDevice {
                    device: DeviceId(0),
                },
                make(2, "Private"),
                rename(0, "Renamed"),
                rename(2, "Absent"),
                delete(0),
                join(0, 4),
                leave(0, 1),
                StoreChange::Reset {
                    snapshot: snapshot(40, Audience::Store),
                },
                StoreChange::Reset {
                    snapshot: snapshot(40, Audience::Circle(circle(0))),
                },
            ] {
                let current = replay(&base.entries);
                let validated =
                    crate::effects::validate_change(&current.state, &member(author), &change);
                let mut h = base.clone();
                h.all(author, u64::from(author), change);
                let replayed = replay(&h.entries);
                let entry = h.entries.last().unwrap();
                match &replayed.entries[&entry.position] {
                    EntryOutcome::Kept => {
                        assert!(validated.is_ok(), "{:?}: {validated:?}", entry.change)
                    }
                    EntryOutcome::Dropped(reason) => assert_eq!(validated, Err(reason.clone())),
                }
                let received = vec![(0..h.entries.len()).collect()];
                let query = input(&h, &sequential_times(&h), &received);
                cases.push((projected(&h, &replayed), query));
            }
        }
    }
    let queries: Vec<_> = cases.iter().map(|(_, query)| query.clone()).collect();
    for ((expected, query), actual) in cases.into_iter().zip(lean_results(&queries)) {
        assert_eq!(actual, vec![expected], "{query}");
    }
}
