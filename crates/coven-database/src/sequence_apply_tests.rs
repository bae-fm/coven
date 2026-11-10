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
use coven_merge::slot;
use coven_merge::{Audience, Operation, Timestamp};
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
    let Operation::Insert(cols) = &mut w.parts[0].rows[0].change.operation else { unreachable!() };
    cols.get_mut("a").unwrap().value = Scalar::Text(value.to_string());
    let mut owned = w.parts[0].rows[0].clone();
    owned.row.table = "owned".into();
    owned.row.key = crate::tests::text_key(&owner.to_string());
    let Operation::Insert(cols) = &mut owned.change.operation else { unreachable!() };
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
    let source = ChangeId { write: original.header.position, row: r.row.clone() };
    let Operation::Insert(cols) = &r.change.operation else { unreachable!() };
    r.old = cols.iter().map(|(name, c)| (name.clone(), c.value.clone())).collect();
    r.sources = WriteSources {
        generation: Some(source.clone()),
        setters: cols
            .keys()
            .map(|c| (c.clone(), CellSource { change: source.clone(), column: c.clone() }))
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
        db.transaction(|db| {
            let mut state = crate::store_state_tables::read(db)?;
            for member in state.members.values_mut() {
                member.role = coven_format::admin::MemberRole::Admin;
            }
            crate::store_state_tables::replace(db, &state)
        })
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
                slot: slot(at),
                header: SlotHeader {
                    device: DeviceId(device),
                    claim: at,
                    author: crate::tests::member(device as u8),
                    after: at - 1,
                },
                digest: [at as u8; 32],
                content: SlotContent::Writes { size: 1, writes },
            },
            crate::test_utils::idle_writers(),
            || Ok(()),
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
                        StoreChange::RemoveDevice { device: DeviceId(2) },
                    )
                }
                2 => {
                    state.members.get_mut(&crate::tests::member(2)).unwrap().removed = true;
                    state.devices.get_mut(&DeviceId(2)).unwrap().state = DeviceState::Removed;
                    (
                        json!({"kind":"removeMember","member":2}),
                        StoreChange::RemoveMember { member: crate::tests::member(2) },
                    )
                }
                3 | 4 => {
                    state.members.get_mut(&crate::tests::member(2)).unwrap().keys =
                        crate::tests::keys(8);
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
            let row = original.parts[0].rows.iter().find(|r| r.row.table == "rows").unwrap();
            let Operation::Insert(cols) = &row.change.operation else { unreachable!() };
            let Scalar::Text(value) = &cols["a"].value else { unreachable!() };
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
                    crate::store_state_tables::device_member(c, creation.creator.write.device)
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
                |r| crate::write_encoding::counter(r.get(0)?),
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
        let choices: Vec<_> =
            (0..8).map(|_| (1 + rng.pick(2) as u64, rng.pick(3), rng.pick(3))).collect();
        prefixes +=
            compare(&runner, &choices, 1 + rng.pick(3), rng.pick(5), rng.pick(2) == 0).await;
        histories += 1;
    }
    eprintln!(
        "sequence apply rows: {exhaustive} exhaustive + 2 directed + {} seeded histories, {prefixes} prefixes",
        histories - exhaustive - 2
    );
}

/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_slot_records SELECT n,CASE i%3 WHEN 0 THEN 'admin' WHEN 1 THEN 'key' ELSE 'announcement' END,key FROM fixture;
         INSERT INTO _coven_write_slots SELECT n,i+1 FROM fixture;
         INSERT INTO _coven_write_refusals SELECT n,n FROM fixture;
         INSERT INTO _coven_write_slot_audiences SELECT audience,n FROM fixture;",
    )
    .unwrap();
}

/// Every device that has applied the same writes ends with the same merge
/// records, not only the same app tables, whether it made some of them
/// itself or only received them (§8). Records compare by the rows they name:
/// a stored row's and a loss's local ids are each device's own.
#[tokio::test]
async fn devices_that_applied_the_same_slots_hold_the_same_merge_records() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let ana_store = TestStore::with_ids(&ids);
    let carol_store = TestStore::with_ids(&ids);
    let tables = || {
        vec![
            SyncedTable::new("rows", RowIdentity::SharedKey),
            SyncedTable::new("owned", RowIdentity::SharedKey).owner_only(),
        ]
    };
    let ana = ana_store.schema(tables(), SCHEMA).await.unwrap();
    let carol = carol_store.schema(tables(), SCHEMA).await.unwrap();
    let ben_device = DeviceId(900);
    let ana_device = ana_store.device();
    for db in [&ana, &carol] {
        TestStore::register(db, ana_device, 1);
        TestStore::register(db, ben_device, 2);
    }

    // Ana commits two writes locally, the second contending with Ben's write
    // for a unique value.
    ana.write(|sql| {
        sql.execute("INSERT INTO rows(id,a,b) VALUES('r1','taken','x')", [])?;
        Ok(())
    })
    .await
    .unwrap();
    ana.write(|sql| {
        sql.execute("INSERT INTO rows(id,a,b) VALUES('r2','shared','y')", [])?;
        sql.execute("INSERT INTO owned(id,a,b) VALUES('o1','mine','z')", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let ours = ana.test_queued_writes().await.unwrap();
    assert_eq!(ours.len(), 2);
    let mut theirs = record(ben_device.0, 1, 5, 3, 0, 7);
    theirs.parts[0].rows.retain(|row| row.row.table == "rows");
    let Operation::Insert(columns) = &mut theirs.parts[0].rows[0].change.operation else {
        unreachable!()
    };
    columns.get_mut("a").unwrap().value = Scalar::Text("shared".into());

    for db in [&ana, &carol] {
        for (slot, device, writes) in
            [(1, ana_device, ours.clone()), (2, ben_device, vec![theirs.clone()])]
        {
            db.apply_slot::<crate::pending::tests::Record, _, _>(
                IncomingSlot {
                    slot: coven_merge::slot(slot),
                    header: SlotHeader {
                        device,
                        claim: slot,
                        author: crate::tests::member(if slot == 1 { 1 } else { 2 }),
                        after: slot - 1,
                    },
                    digest: [slot as u8; 32],
                    content: SlotContent::Writes {
                        size: 1,
                        writes: writes
                            .into_iter()
                            .map(|write| crate::sequence::tests::stream(write.into()))
                            .collect(),
                    },
                },
                crate::test_utils::idle_writers(),
                || Ok(()),
            )
            .await
            .unwrap();
        }
    }
    let merged = |db: &Database| {
        const ROW: &str = "r.table_name,r.key,r.audience,r.generation";
        let statements = [
            ("rows", "SELECT * FROM rows ORDER BY 1".to_owned()),
            ("owned", "SELECT * FROM owned ORDER BY 1".to_owned()),
            ("_coven_writes", "SELECT * FROM _coven_writes ORDER BY 1".to_owned()),
            ("_coven_rows", format!("SELECT {ROW},r.write_id FROM _coven_rows r ORDER BY 1,2,3,4")),
            ("_coven_cells", format!("SELECT {ROW},c.column_id,c.write_id FROM _coven_cells c JOIN _coven_rows r ON r.id=c.row_id ORDER BY 1,2,3,4,5")),
            ("_coven_references", format!("SELECT {ROW},f.column_id,f.foreign_key_id,f.parent_table,f.parent_key,f.parent_audience,f.parent_generation FROM _coven_references f JOIN _coven_rows r ON r.id=f.row_id ORDER BY 1,2,3,4,5,6")),
            ("_coven_claims", format!("SELECT {ROW},c.constraint_id,c.value FROM _coven_claims c JOIN _coven_rows r ON r.id=c.row_id ORDER BY 1,2,3,4,5")),
            ("_coven_written_values", format!("SELECT {ROW},v.column_id,v.value FROM _coven_written_values v JOIN _coven_rows r ON r.id=v.row_id ORDER BY 1,2,3,4,5")),
            ("_coven_lost", "SELECT table_name,key,audience,generation,column_id,value,set_by,frozen,replacement_kind,replaced_by FROM _coven_lost ORDER BY 1,2,3,4,5,7,9".to_owned()),
        ];
        db.inspect_writer(|db| {
            statements
                .into_iter()
                .map(|(table, sql)| {
                    let values: Vec<Vec<crate::types::Value>> = db
                        .query(&sql, [], |r| {
                            (0..r.as_ref().column_count()).map(|i| r.get(i)).collect()
                        })
                        .unwrap();
                    (table, values)
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        })
    };
    let (ana_merge, carol_merge) = (merged(&ana), merged(&carol));
    assert_eq!(ana_merge.len(), 9, "{:?}", ana_merge.keys().collect::<Vec<_>>());
    assert!(!ana_merge["_coven_lost"].is_empty(), "the contention lost a value");
    for table in ana_merge.keys() {
        assert_eq!(ana_merge[table], carol_merge[table], "{table}");
    }
    ana.close().await.unwrap();
    carol.close().await.unwrap();
}

/// Applying a slot stages its rows in temporary tables the writer keeps,
/// empty between slots, so a slot changes no temporary table: each change
/// would expire every statement prepared on the writer, and SQLite would parse
/// and plan all of them again in every slot.
#[tokio::test]
async fn applying_a_slot_leaves_the_temporary_tables_as_they_were() {
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
    let ben = DeviceId(900);
    TestStore::register(&db, ben, 2);
    let mut versions = Vec::new();
    for slot in 1..=3u64 {
        let mut write = record(ben.0, slot, slot, slot as usize, slot as usize, 0);
        write.parts[0].rows.retain(|row| row.row.table == "rows");
        db.apply_slot::<crate::pending::tests::Record, _, _>(
            IncomingSlot {
                slot: coven_merge::slot(slot),
                header: SlotHeader {
                    device: ben,
                    claim: slot,
                    author: crate::tests::member(2),
                    after: slot - 1,
                },
                digest: [slot as u8; 32],
                content: SlotContent::Writes {
                    size: 1,
                    writes: vec![crate::sequence::tests::stream(write.into())],
                },
            },
            crate::test_utils::idle_writers(),
            || Ok(()),
        )
        .await
        .unwrap();
        versions.push(db.inspect_writer(|writer| {
            writer.query_row("PRAGMA temp.schema_version", [], |r| r.get::<_, i64>(0)).unwrap()
        }));
    }
    assert_eq!(versions[2], versions[1]);
    db.close().await.unwrap();
}

/// Ana's queued writes move email `a@` from user `a` to user `p`: one
/// renames `a` to `x@`, the next gives `p` the freed address. Ben's slot
/// edits `a`'s name, which reaches `a` and her first write, but not her
/// second, whose row `p` keeps its fold. Counting `a` before her first write
/// folds again sees `a` still holding `a@` beside `p`, but once the queue
/// folds the two claims no longer meet: both users show, and nothing is
/// taken out (§6.2).
#[tokio::test]
async fn a_unique_claim_counted_beside_an_unreached_row_leaves_nothing_taken_out() {
    let store = TestStore::new();
    let db = store
        .builder(
            vec![SyncedTable::new("users", RowIdentity::SharedKey)],
            vec![Migration::addition(
                1,
                "users",
                "CREATE TABLE users(id TEXT NOT NULL PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
            )],
        )
        .open()
        .await
        .unwrap();
    crate::write::tests::sql(
        &db,
        "INSERT INTO users VALUES('a','a@','A'); INSERT INTO users VALUES('p','p@','P')",
    )
    .await
    .unwrap();
    for record in crate::write::tests::records(&db) {
        db.receive(record.into()).await.unwrap();
    }
    crate::write::tests::sql(&db, "UPDATE users SET email='x@' WHERE id='a'").await.unwrap();
    crate::write::tests::sql(&db, "UPDATE users SET email='a@' WHERE id='p'").await.unwrap();
    let users = |db: &Database| -> Vec<(String, String, String)> {
        db.inspect_writer(|db| {
            db.query("SELECT id,email,name FROM users ORDER BY id", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap()
        })
    };
    let name = [("name", Scalar::Text("Ben's A".into()))];
    crate::tests::remote_update_beside_queue(&db, "users", "a", &name).await;
    assert_eq!(
        users(&db),
        [
            ("a".to_owned(), "x@".to_owned(), "Ben's A".to_owned()),
            ("p".to_owned(), "a@".to_owned(), "P".to_owned()),
        ]
    );
    assert!(db.lost_values().await.unwrap().is_empty(), "{:?}", db.lost_values().await);
    let taken_out: i64 = db.inspect_writer(|db| {
        db.query_row("SELECT count(*) FROM _coven_lost WHERE replacement_kind='rules'", [], |r| {
            r.get(0)
        })
        .unwrap()
    });
    assert_eq!(taken_out, 0);
    db.close().await.unwrap();
}

/// Ben's write 2 lands having read slot 1; his write 3 then arrives claiming
/// it read nothing, less than his own earlier write. No genuine device makes
/// such a write, and forgetting kept records relies on reads never going
/// back, so the slot is refused like any invalid write (§7.1).
#[tokio::test]
async fn a_write_reading_less_than_its_devices_last_landed_write_refuses_its_slot() {
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
    let ben = DeviceId(900);
    TestStore::register(&db, ben, 2);
    let apply = |slot: u64, read_through: u64| {
        let mut write = record(ben.0, slot, slot, slot as usize, slot as usize, 0);
        write.parts[0].rows.retain(|row| row.row.table == "rows");
        write.header.read_through = read_through;
        db.apply_slot::<crate::pending::tests::Record, _, _>(
            IncomingSlot {
                slot: coven_merge::slot(slot),
                header: SlotHeader {
                    device: ben,
                    claim: slot,
                    author: crate::tests::member(2),
                    after: slot - 1,
                },
                digest: [slot as u8; 32],
                content: SlotContent::Writes {
                    size: 1,
                    writes: vec![crate::sequence::tests::stream(write.into())],
                },
            },
            crate::test_utils::idle_writers(),
            || Ok(()),
        )
    };
    apply(1, 0).await.unwrap();
    apply(2, 1).await.unwrap();
    let error = apply(3, 0).await.unwrap_err();
    let write = coven_merge::WriteId { device: ben, number: 3 };
    assert!(
        matches!(
            &error,
            DbError::InvalidWrite { write: refused, error: coven_merge::MergeError::CausalClosure(named) }
                if *refused == write && *named == write
        ),
        "{error:?}"
    );
    db.close().await.unwrap();
}

/// Ben's writes land in slots 1 to 3, each slot 100 sealed bytes. A raise's
/// announcement covering slot 3 forgets nothing: until its raise lands it
/// isn't the snapshot a new device starts from. Ben's ordinary store
/// snapshot covering slot 2 is: those slots no longer count toward the
/// store's next snapshot, so their sizes go, and what counts after slot 2
/// is still slot 3 and the announcements.
#[tokio::test]
async fn an_ordinary_announcement_forgets_the_slot_sizes_it_covers() {
    let store = TestStore::new();
    let db =
        store.schema(vec![SyncedTable::new("rows", RowIdentity::SharedKey)], SCHEMA).await.unwrap();
    let ben = DeviceId(900);
    TestStore::register(&db, ben, 2);
    let header = |slot: u64| SlotHeader {
        device: ben,
        claim: slot,
        author: crate::tests::member(2),
        after: slot - 1,
    };
    for slot in 1..=3u64 {
        let mut write = record(ben.0, slot, slot, slot as usize, slot as usize, 0);
        write.parts[0].rows.retain(|row| row.row.table == "rows");
        write.header.read_through = slot - 1;
        db.apply_slot::<crate::pending::tests::Record, _, _>(
            IncomingSlot {
                slot: coven_merge::slot(slot),
                header: header(slot),
                digest: [slot as u8; 32],
                content: SlotContent::Writes {
                    size: 100,
                    writes: vec![crate::sequence::tests::stream(write.into())],
                },
            },
            crate::test_utils::idle_writers(),
            || Ok(()),
        )
        .await
        .unwrap();
    }
    let announce = |slot: u64, covers: u64, purpose| {
        db.apply_slot::<crate::pending::tests::Record, std::io::Cursor<Vec<u8>>, _>(
            IncomingSlot {
                slot: coven_merge::slot(slot),
                header: header(slot),
                digest: [slot as u8; 32],
                content: SlotContent::Announcement {
                    original: coven_format::floor::OriginalSlot {
                        slot: coven_merge::slot(slot),
                        object: vec![0; 10],
                    },
                    accepted: Some(coven_format::sequence::SnapshotAnnouncement {
                        snapshot: coven_format::admin::SnapshotId {
                            audience: Audience::Store,
                            device: ben,
                            number: slot,
                        },
                        covers,
                        breaking_version: 0,
                        purpose,
                        digest: [0; 32],
                    }),
                },
            },
            crate::test_utils::idle_writers(),
            || Ok(()),
        )
    };
    let counted = || {
        db.inspect_writer(|c| {
            ["_coven_write_slots", "_coven_write_slot_audiences"].map(|table| {
                c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get::<_, i64>(0))
                    .unwrap()
            })
        })
    };
    assert_eq!(db.snapshot_slot_bytes(Audience::Store, 0).await.unwrap(), 300);

    announce(4, 3, coven_format::sequence::Purpose::Raise(1)).await.unwrap();
    assert_eq!(counted(), [3, 3]);
    announce(5, 2, coven_format::sequence::Purpose::Ordinary).await.unwrap();
    assert_eq!(counted(), [1, 1]);
    // Slot 3's writes and the two announcement slots, of 10 bytes each.
    assert_eq!(db.snapshot_slot_bytes(Audience::Store, 2).await.unwrap(), 120);
    assert_eq!(
        db.snapshot_slot_bytes(Audience::Store, 0).await.unwrap(),
        120,
        "the slots the snapshot covers count nothing"
    );
    db.close().await.unwrap();
}
