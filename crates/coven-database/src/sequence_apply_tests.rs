use crate::{
    test_utils::tests::{insert, run_lean, Generator},
    tests::TestStore,
    *,
};
use coven_format::{
    admin::StoreChange,
    sequence::SlotHeader,
    value::Value as Scalar,
    write::WriteRecord,
    write_sources::{CellSource, ChangeId, WriteSources},
};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, Operation, Slot, Timestamp};
use serde_json::{json, Value};
use std::{collections::BTreeMap, ffi::OsStr};

const SCHEMA: &str = "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY,a TEXT UNIQUE,b TEXT);
CREATE TABLE owned(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT)";

fn record(
    device: u64,
    number: u64,
    tick: u64,
    row: usize,
    value: usize,
    owner: usize,
) -> WriteRecord {
    let mut w = insert(device, row);
    w.header.position.device = DeviceId(device);
    w.header.position.number = number;
    w.header.timestamp = Timestamp::new(tick, 0, DeviceId(device)).unwrap();
    let Operation::Insert(cols) = &mut w.parts[0].rows[0].change.operation else {
        unreachable!()
    };
    cols.get_mut("a").unwrap().value = Scalar::Text(value.to_string());
    let mut owned = w.parts[0].rows[0].clone();
    owned.row.table = "owned".into();
    owned.row.key = coven_format::key::encode_key(&[Scalar::Text(owner.to_string())]).unwrap();
    let Operation::Insert(cols) = &mut owned.change.operation else {
        unreachable!()
    };
    cols.get_mut("id").unwrap().value = Scalar::Text(owner.to_string());
    w.parts[0].rows.push(owned);
    w.parts[0].rows.sort_by(|a, b| a.row.cmp(&b.row));
    w
}

fn deletion(original: &WriteRecord, number: u64, read: u64, tick: u64) -> WriteRecord {
    let mut w = original.clone();
    w.header.position.number = number;
    w.header.timestamp = Timestamp::new(tick, 0, w.header.position.device).unwrap();
    w.header.read_through = read;
    w.parts[0].rows.retain(|r| r.row.table == "rows");
    let r = &mut w.parts[0].rows[0];
    let source = ChangeId {
        write: original.header.position,
        row: r.row.clone(),
    };
    let Operation::Insert(cols) = &r.change.operation else {
        unreachable!()
    };
    r.old = cols
        .iter()
        .map(|(name, c)| (name.clone(), c.value.clone()))
        .collect();
    r.sources = WriteSources {
        generation: Some(source.clone()),
        setters: cols
            .keys()
            .map(|c| {
                (
                    c.clone(),
                    CellSource {
                        change: source.clone(),
                        column: c.clone(),
                    },
                )
            })
            .collect(),
    };
    r.change.generation = 1;
    r.change.operation = Operation::Delete;
    w
}

// Each tuple selects device, unique value and owner-row key; chunks share a slot.
async fn compare(
    runner: &OsStr,
    choices: &[(u64, usize, usize)],
    batch: usize,
    cutoff: usize,
    release: bool,
) -> usize {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![
                SyncedTable::new("rows", RowIdentity::SharedKey),
                SyncedTable::new("owned", RowIdentity::SharedKey).owner_only(),
            ],
            SCHEMA,
        )
        .await
        .unwrap();
    for d in 1..=2 {
        TestStore::register(&db, DeviceId(d), d as u8);
    }
    db.inspect_writer(|db| {
        db.internal_execute("UPDATE _coven_members SET role='admin'", [])
            .unwrap()
    });
    let mut slots = Vec::new();
    let mut rust = Vec::new();
    let mut numbers = [0u64; 3];
    let mut reads = [0u64; 3];
    // Concurrent writers have opposite timestamp order to their slot order.
    // Reading a prefix advances the author's clock beyond every observed write.
    let mut ticks = [0u64, 9000, 8000];
    let mut originals = Vec::new();
    let mut claims = BTreeMap::new();
    let mut key_gen = [0; 3];
    for chunk in choices.chunks(batch) {
        // A slot has one author, so subsequent tuple devices are deliberately
        // represented by the first tuple's author for this batch.
        let device = chunk[0].0;
        let at = slots.len() as u64 + 1;
        let mut writes = Vec::new();
        let mut rows = Vec::new();
        let mut owners = Vec::new();
        let mut wn = Vec::new();
        for (_, value, owner) in chunk {
            let row = originals.len() + 1;
            numbers[device as usize] += 1;
            ticks[device as usize] += 1;
            let n = numbers[device as usize];
            let mut w = record(device, n, ticks[device as usize], row, *value, *owner);
            w.header.read_through = reads[device as usize];
            claims.insert(w.header.position, (at, n));
            rows.push(json!({"row":row,"live":true,"value":value,"inputs":[[at,n]]}));
            owners.push(json!([owner, n]));
            wn.push(n);
            originals.push(w.clone());
            let mut stream = crate::sequence::tests::stream(w.into());
            // Replacement cases include both old-key originals and writes
            // made under the current member key.
            if key_gen[device as usize] == 1 {
                stream.signer = crate::tests::keys(8).signing;
            }
            writes.push(stream);
        }
        slots.push(json!({"kind":"writes","device":device,"claim":at,"digest":format!("{at}"),"key_gen":key_gen[device as usize],
            "parts":[{"audience":0,"version":1,"read_through":reads[device as usize],"unique_claims":[],"owner_creations":owners,"writes":wn,"rows":rows}]}));
        db.apply_slot::<crate::pending::tests::Record, _, _>(
            IncomingSlot {
                slot: Slot(at),
                header: SlotHeader {
                    device: DeviceId(device),
                    claim: at,
                    author: crate::tests::member(device as u8),
                    after: at - 1,
                },
                digest: [at as u8; 32],
                content: SlotContent::Writes(writes),
            },
            || Ok(()),
            None,
        )
        .await
        .unwrap();
        rust.push(project(&db, &claims));
        if slots.len() == 1 && cutoff > 0 {
            let at = slots.len() as u64 + 1;
            let mut state = db.store_state().await.unwrap();
            let (entry, change) = match cutoff {
                1 => {
                    state.devices.get_mut(&DeviceId(2)).unwrap().state = DeviceState::Removed;
                    (
                        json!({"kind":"removeDevice","device":2}),
                        StoreChange::RemoveDevice {
                            device: DeviceId(2),
                        },
                    )
                }
                2 => {
                    state
                        .members
                        .get_mut(&crate::tests::member(2))
                        .unwrap()
                        .removed = true;
                    state.devices.get_mut(&DeviceId(2)).unwrap().state = DeviceState::Removed;
                    (
                        json!({"kind":"removeMember","member":2}),
                        StoreChange::RemoveMember {
                            member: crate::tests::member(2),
                        },
                    )
                }
                3 | 4 => {
                    state
                        .members
                        .get_mut(&crate::tests::member(2))
                        .unwrap()
                        .keys = crate::tests::keys(8);
                    if cutoff == 4 {
                        key_gen[2] = 1;
                    }
                    (
                        json!({"kind":"replaceKey","member":2}),
                        StoreChange::ReplaceMemberKeys {
                            member: crate::tests::member(2),
                            expected: coven_crypto::MemberPublicKeys {
                                signing: crate::test_utils::signer(),
                                sealing: crate::tests::keys(2).sealing,
                            },
                            keys: crate::tests::keys(8),
                        },
                    )
                }
                _ => unreachable!(),
            };
            slots.push(json!({"kind":"admin","device":1,"claim":at,"digest":format!("{at}"),"entry":entry}));
            db.admin_fixture(change, state, None).await.unwrap();
            rust.push(project(&db, &claims));
        }
        // A counted deletion releases the first original; cut-off deletions
        // leave the live claims unchanged.
        if release && originals.len() >= 2 && originals.len() <= batch.max(2) {
            let original = &originals[0];
            let device = original.header.position.device.0;
            let at = slots.len() as u64 + 1;
            numbers[device as usize] += 1;
            let n = numbers[device as usize];
            reads[device as usize] = at - 1;
            ticks[device as usize] = ticks.iter().max().unwrap() + 1;
            let w = deletion(original, n, at - 1, ticks[device as usize]);
            let row = original.parts[0]
                .rows
                .iter()
                .find(|r| r.row.table == "rows")
                .unwrap();
            let Operation::Insert(cols) = &row.change.operation else {
                unreachable!()
            };
            let Scalar::Text(value) = &cols["a"].value else {
                unreachable!()
            };
            let (setter, number) = claims[&original.header.position];
            slots.push(json!({"kind":"writes","device":device,"claim":at,"digest":format!("{at}"),"key_gen":0,
                "parts":[{"audience":0,"version":1,"read_through":w.header.read_through,"unique_claims":[],"owner_creations":[],"writes":[n],
                "rows":[{"row":1,"live":false,"value":value.parse::<usize>().unwrap(),"inputs":[[setter,number]]}]}]}));
            db.receive(w.into()).await.unwrap();
            rust.push(project(&db, &claims));
        }
    }
    let input = json!({"kind":"apply","member_count":3,
        "initial":{"name":"sequence tests","members":[{"member":1,"role":"admin","key_gen":0},{"member":2,"role":"admin","key_gen":0}],
            "devices":[{"device":1,"member":1},{"device":2,"member":2}]},
        "audiences":[{"audience":0,"version":1,"writers":[1,2]}],"slots":slots});
    let lean = run_lean(runner, &input);
    for (i, actual) in rust.iter().enumerate() {
        let expected = &lean["slots"][i]["audiences"][0];
        let winners: Vec<_> = expected["live_winners"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| !v[1].is_null())
            .cloned()
            .collect();
        assert_eq!(
            *actual,
            json!({"winners":winners,"owners":expected["owner_creations"],"refused":expected["refused"],"through":lean["slots"][i]["slot"]}),
            "prefix {}, {input}",
            i + 1
        );
    }
    db.close().await.unwrap();
    slots.len()
}

fn project(db: &Database, claims: &BTreeMap<coven_merge::WriteId, (u64, u64)>) -> Value {
    db.inspect_writer(|c| {
        let winners = c
            .query("SELECT a,id FROM rows ORDER BY a", [], |r| {
                Ok(json!([
                    r.get::<_, String>(0)?.parse::<usize>().unwrap(),
                    r.get::<_, String>(1)?.parse::<usize>().unwrap()
                ]))
            })
            .unwrap();
        let mut owners = Vec::new();
        crate::ownership::visit::<DbError>(c, &Audience::Store, |row, creation| {
            if row.table == "owned" {
                let key = coven_format::key::decode_key(&row.key).unwrap();
                let Scalar::Text(key) = &key[0] else {
                    unreachable!()
                };
                owners.push(json!([
                    key.parse::<usize>().unwrap(),
                    claims[&creation.write].0,
                    creation.write.number,
                    creation.write.device.0,
                    crate::ownership::member(c, creation.creator.write.device)
                        .unwrap()
                        .unwrap()
                        .0
                        .as_bytes()[0]
                ]));
            }
            Ok(())
        })
        .unwrap();
        let refused = c
            .query(
                "SELECT slot FROM _coven_write_refusals ORDER BY slot",
                [],
                |r| Ok(crate::write_encoding::counter(r.get(0)?)),
            )
            .unwrap();
        json!({"winners":winners,"owners":owners,"refused":refused,"through":crate::sequence::applied(c).unwrap()})
    })
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_rows_match_lean() {
    let runner = std::env::var_os("COVEN_SEQUENCE_LEAN").expect("sequenceRunner");
    let mut histories = 0;
    let mut prefixes = 0;
    for mask in 0..16 {
        for values in [[0, 1, 0, 1], [0, 0, 0, 1]] {
            for cutoff in 0..5 {
                for release in [false, true] {
                    let choices: Vec<_> = values
                        .iter()
                        .enumerate()
                        .map(|(i, value)| (1 + ((mask >> i) & 1), *value, i % 2))
                        .collect();
                    prefixes += compare(&runner, &choices, 1, cutoff, release).await;
                    histories += 1;
                }
            }
        }
    }
    let exhaustive = histories;
    for device in 1..=2 {
        prefixes += compare(&runner, &[(device, 0, 0), (device, 1, 0)], 2, 0, false).await;
        histories += 1;
    }

    for seed in 0..64 {
        let mut rng = Generator::new(seed + 0x51a7);
        let choices: Vec<_> = (0..8)
            .map(|_| (1 + rng.pick(2) as u64, rng.pick(3), rng.pick(3)))
            .collect();
        prefixes += compare(
            &runner,
            &choices,
            1 + rng.pick(3),
            rng.pick(5),
            rng.pick(2) == 0,
        )
        .await;
        histories += 1;
    }
    eprintln!(
        "sequence apply rows: {exhaustive} exhaustive + 2 directed + {} seeded histories, {prefixes} prefixes",
        histories - exhaustive - 2
    );
}
