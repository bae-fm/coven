//! Adapter and generated inputs for the independent Lean `resolve` executable.

use std::{
    collections::BTreeSet,
    io::Write,
    process::{Command, Stdio},
};

use coven_crypto::MemberId;
use coven_database::{
    CovenMigrationPolicy, DatabaseBuilder, EntryOutcome, StoreLog, StoreLogCheck, StoreLogReplay,
    StoreLogState,
};
use coven_format::store_log::{CircleKeyId, MemberRole, StoreChange};
use coven_foundation::{
    files::{StoreDir, StoreLayout},
    id_source::{CircleId, DeviceId, SequentialIds, StoreId},
};
use coven_merge::Audience;
use serde_json::{json, Value};

use crate::{
    effects::tests::{raise, snapshot},
    replay,
    replay::tests::*,
    replay_entry,
};

struct Generator(u64);

impl Generator {
    fn pick(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }

    fn history(&mut self) -> History {
        let mut h = household(MemberRole::Admin, MemberRole::Admin);
        for (id, name) in [(0, "Gifts"), (1, "Notes")] {
            h.all(0, 0, make(id, name));
            h.all(0, 0, join(id, 1));
            h.all(0, 0, join(id, 2));
        }
        if self.pick(2) == 0 {
            let mut changes = self.concurrent_changes();
            if self.pick(2) == 0 {
                changes.swap(0, 1);
            }
            let past: Vec<_> = (0..h.entries.len()).collect();
            for (author, change) in [0, 2].into_iter().zip(changes) {
                h.push(author, u64::from(author), &past, change);
            }
        }
        let size = 14 + self.pick(7);
        while h.entries.len() < size {
            let author = self.pick(5) as u8;
            let device = self.pick(8) as u64;
            let mut past = BTreeSet::from([0]);
            for (i, entry) in h.entries.iter().enumerate() {
                if entry.position.device == DeviceId(device) || self.pick(4) == 0 {
                    past.insert(i);
                }
            }
            for i in (0..h.entries.len()).rev() {
                if past.contains(&i) {
                    for (j, entry) in h.entries[..i].iter().enumerate() {
                        if crate::replay::had_read(&h.entries[i], entry) {
                            past.insert(j);
                        }
                    }
                }
            }
            let view = replay(
                &past
                    .iter()
                    .map(|&i| h.entries[i].clone())
                    .collect::<Vec<_>>(),
            )
            .state;
            let change = self.action(&view);
            h.push(
                author,
                device,
                &past.into_iter().collect::<Vec<_>>(),
                change,
            );
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
        let format = self.pick(2) == 0;
        match self.pick(12) {
            0 => [remove(1, &[0, 1]), remove(2, &[0, 1])],
            1 => [leave(0, 1), leave(0, 2)],
            2 => [remove(1, &[0, 1]), leave(0, 2)],
            3 => [leave(0, 1), remove(2, &[0, 1])],
            4 => [
                reset(audience.clone()),
                raise(format, 2, 30 + self.pick(2) as u64, audience),
            ],
            5 => [
                raise(format, 2, 30, audience.clone()),
                raise(format, 2, 31, audience),
            ],
            6 => [
                raise(format, 2, 30, audience.clone()),
                raise(format, 3, 31, audience),
            ],
            7 => [reset(self.audience()), raise(format, 2, 30, audience)],
            8 => [
                raise(format, 2, 30, self.audience()),
                raise(format, 2, 31, audience),
            ],
            9 => [delete(0), raise(format, 2, 30, Audience::Circle(circle(0)))],
            10 => [rename(0, "Birthdays"), rename(0, "Presents")],
            11 => [rename(0, "Birthdays"), delete(0)],
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
            1 => StoreChange::RemoveMember {
                member: member(m),
                key: key(self.0),
                circle_keys: view
                    .circles
                    .iter()
                    .filter_map(|(id, c)| {
                        (!c.deleted && c.members.len() > 1 && c.members.contains(&member(m)))
                            .then_some(CircleKeyId {
                                circle: *id,
                                key: key(self.0),
                            })
                    })
                    .collect(),
            },
            2 => crate::replay::tests::role(m, role),
            3 => device(self.pick(9) as u64),
            4 => {
                let devices: Vec<_> = view
                    .devices
                    .iter()
                    .filter_map(|(id, d)| (!d.removed).then_some(*id))
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
            11 => StoreChange::RaiseFormat {
                version: self.pick(3) as u16 + 1,
                snapshot: snapshot(self.pick(5) as u64 + 1, self.audience()),
            },
            12 => StoreChange::Reset {
                snapshot: snapshot(
                    self.pick(5) as u64 + 1,
                    if self.pick(4) == 0 {
                        Audience::Store
                    } else {
                        Audience::Circle(circle(c))
                    },
                ),
            },
            13 => StoreChange::SetAccess {
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: if self.pick(3) == 0 {
                        "fixture-access-key".into()
                    } else {
                        format!("key-{}", self.pick(3))
                    },
                },
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
fn audience_number(a: &Audience) -> u64 {
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

fn input(h: &History) -> Value {
    let entries: Vec<_> = h.entries.iter().enumerate().map(|(i,entry)| {
        let past: Vec<_> = h.entries[..i].iter().enumerate().filter_map(|(j,e)| crate::replay::had_read(entry,e).then_some(j)).collect();
        let action = match &entry.change {
            StoreChange::CreateStore { access, .. } => json!({"kind":0,"access":serde_json::to_string(access).unwrap()}),
            StoreChange::AddMember { keys, role, access } => json!({"kind":1,"member":member_number(&keys.signing),"role":role_number(*role),"access":serde_json::to_string(access).unwrap()}),
            StoreChange::RemoveMember { member, circle_keys, .. } => json!({"kind":2,"member":member_number(member),"circles":circle_keys.iter().map(|k| circle_number(k.circle)).collect::<Vec<_>>()}),
            StoreChange::ChangeRole { member, role } => json!({"kind":3,"member":member_number(member),"role":role_number(*role)}),
            StoreChange::AddDevice { device, .. } => json!({"kind":4,"member":member_number(&entry.author),"device":device.0}),
            StoreChange::RemoveDevice { device } => {
                let view = replay(&past.iter().map(|&i| h.entries[i].clone()).collect::<Vec<_>>()).state;
                let owner = &view.devices[device];
                assert!(!owner.removed, "generator may remove only an observed device");
                json!({"kind":5,"member":member_number(&owner.member),"device":device.0})
            }
            StoreChange::CreateCircle { circle, name, .. } => json!({"kind":6,"circle":circle_number(*circle),"name":name}),
            StoreChange::RenameCircle { circle, name } => json!({"kind":7,"circle":circle_number(*circle),"name":name}),
            StoreChange::DeleteCircle { circle } => json!({"kind":8,"circle":circle_number(*circle)}),
            StoreChange::AddCircleMember { circle, member } => json!({"kind":9,"circle":circle_number(*circle),"member":member_number(member)}),
            StoreChange::RemoveCircleMember { circle, member, .. } => json!({"kind":10,"circle":circle_number(*circle),"member":member_number(member)}),
            StoreChange::RaiseSchema { version, snapshot } => json!({"kind":11,"version":version,"snapshot":{"audience":audience_number(&snapshot.audience),"number":snapshot.number}}),
            StoreChange::RaiseFormat { version, snapshot } => json!({"kind":12,"version":version,"snapshot":{"audience":audience_number(&snapshot.audience),"number":snapshot.number}}),
            StoreChange::Reset { snapshot } => json!({"kind":13,"snapshot":{"audience":audience_number(&snapshot.audience),"number":snapshot.number}}),
            StoreChange::SetAccess { access } => json!({"kind":14,"member":member_number(&entry.author),"access":serde_json::to_string(access).unwrap()}),
        };
        json!({"author":member_number(&entry.author),"device":entry.position.device.0,"past":past,"action":action})
    }).collect();
    json!({"entries":entries})
}

fn projected(h: &History, replay: &StoreLogReplay) -> Value {
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
        .filter_map(|(id, d)| (!d.removed).then_some([id.0, member_number(&d.member)]))
        .collect();
    let circles: Vec<_> = state
        .circles
        .iter()
        .filter(|(_, c)| !c.deleted)
        .map(|(id, c)| {
            let mut members: Vec<_> = c.members.iter().map(member_number).collect();
            members.sort();
            json!([circle_number(*id), c.name, members])
        })
        .collect();
    let index = |entry| h.entries.iter().position(|e| e.position == entry).unwrap();
    let mut versions = vec![];
    for (a, v) in &state.schema {
        versions.push(json!([
            0,
            audience_number(a),
            v.number,
            v.snapshot.number,
            index(v.entry)
        ]));
    }
    for (a, v) in &state.format {
        versions.push(json!([
            1,
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
        .filter_map(|(i, e)| (replay.entries[&e.position] == EntryOutcome::Kept).then_some(i))
        .collect();
    json!({"created":state.store.is_some(),"members":members,"access":access,"devices":devices,"circles":circles,"versions":versions,"resets":resets,"kept":kept,"dropped":h.drops(replay)})
}

#[test]
#[ignore = "requires COVEN_STORELOG_LEAN, supplied by scripts/check.sh"]
fn lean_differential() {
    let runner = std::env::var_os("COVEN_STORELOG_LEAN")
        .expect("COVEN_STORELOG_LEAN must name storelogRunner");
    let histories: Vec<_> = (1..=2048).map(|seed| Generator(seed).history()).collect();
    let inputs: Vec<_> = histories.iter().map(input).collect();
    let input = json!({"histories":inputs});
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start storelogRunner");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "Lean runner failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Vec<Value> = serde_json::from_slice(&output.stdout).expect("Lean result array");
    assert_eq!(actual.len(), histories.len());
    for (index, (history, actual)) in histories.iter().zip(actual).enumerate() {
        let result = replay(&history.entries);
        assert_eq!(
            actual,
            projected(history, &result),
            "seed {}, input {}",
            index + 1,
            inputs[index]
        );
        let mut incremental = StoreLog::default();
        for entry in &history.entries {
            let (checked, replay) = crate::replay_entry(&incremental, entry.clone());
            incremental.entries.push(checked);
            incremental.replay = replay;
        }
        assert_eq!(incremental.replay, result, "incremental seed {}", index + 1);
        let reversed: Vec<_> = history.entries.iter().rev().cloned().collect();
        assert_eq!(replay(&reversed), result, "seed {}", index + 1);
    }
}

async fn open(directory: StoreDir) -> coven_database::Database {
    DatabaseBuilder::new(directory)
        .synced_tables(vec![])
        .migrations(vec![])
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
        .open()
        .await
        .unwrap()
}

#[tokio::test]
async fn cached_author_views_survive_reopening_and_a_deletions_reversal() {
    let mut h = gifts();
    h.push(0, 0, &[0, 1, 2, 3, 4], leave(0, 1));
    h.push(1, 1, &[0, 1, 2, 3, 4], delete(0));
    let temporary = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(temporary.path().to_owned());
    let directory = layout
        .create_store_dir(
            StoreId(uuid::Uuid::from_u128(1)),
            "Reversal",
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
            .apply_store_log(entry, result.clone())
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
            assert!(log.replay.state.circles[&circle(0)].deleted);
        }
        database.close().await.unwrap();
    }
    assert_eq!(log.replay, replay(&h.entries));
    assert!(!log.replay.state.circles[&circle(0)].deleted);
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
    h.all(0, 0, remove(1, &[]));
    h.all(0, 0, remove(1, &[0]));
    let temporary = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(temporary.path().to_owned());
    let directory = layout
        .create_store_dir(
            StoreId(uuid::Uuid::from_u128(1)),
            "Checks",
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
            .apply_store_log(checked.clone(), result.clone())
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
    assert_eq!(log.entries[10].check, StoreLogCheck::WrongCircleKeys);
    assert_eq!(
        log.entries[11].check,
        StoreLogCheck::DeletedCircles([circle(1)].into())
    );
}
