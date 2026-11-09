use crate::{tests::TestStore, *};
use coven_format::{
    merge_fields,
    sequence::SlotHeader,
    snapshot_rows::{AuthoredRecord, ChangeVerdict},
    value::Value as Scalar,
    write::WriteRecord,
    write_sources::{CellSource, ChangeId, WriteSources},
};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, ColumnValue, Operation, Timestamp, WriteId};
use std::collections::BTreeMap;

const SCHEMA: &str = "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT)";
const BEN: DeviceId = DeviceId(900);
const CLEO: DeviceId = DeviceId(901);
/// A device that can still write and never does.
const DAN: DeviceId = DeviceId(902);

fn id(device: DeviceId, number: u64) -> WriteId {
    WriteId { device, number }
}

async fn store() -> (TestStore, Database) {
    let store = TestStore::new();
    let db =
        store.schema(vec![SyncedTable::new("rows", RowIdentity::SharedKey)], SCHEMA).await.unwrap();
    TestStore::register(&db, BEN, 2);
    TestStore::register(&db, CLEO, 3);
    TestStore::register(&db, DAN, 4);
    (store, db)
}

/// `write` stamped `tick`, having read through `read`.
fn made(mut write: WriteRecord, write_id: WriteId, tick: u64, read: u64) -> WriteRecord {
    write.header.position = write_id;
    write.header.timestamp = Timestamp::new(tick, 0, write_id.device).unwrap();
    write.header.read_through = read;
    write
}

/// An insert of row `key`.
fn insert(write: WriteId, tick: u64, read: u64, key: usize) -> WriteRecord {
    made(crate::test_utils::tests::insert(0, key), write, tick, read)
}

/// An update of row 1's `b` to `value`, over the generation `created`
/// started and the `b` that `setter` set.
fn update(write: WriteId, tick: u64, read: u64, created: WriteId, setter: WriteId) -> WriteRecord {
    let mut record = insert(write, tick, read, 1);
    let row = &mut record.parts[0].rows[0];
    let source = |write| ChangeId { write, row: row.row.clone() };
    row.sources = WriteSources {
        generation: Some(source(created)),
        setters: BTreeMap::from([(
            "b".into(),
            CellSource { change: source(setter), column: "b".into() },
        )]),
    };
    row.change.generation = 1;
    row.change.operation = Operation::Update(BTreeMap::from([(
        "b".into(),
        ColumnValue { value: Scalar::Text(format!("b by {write:?}")), parents: BTreeMap::new() },
    )]));
    record
}

/// A delete of row 1, over the generation `created` started, `id` and `a`
/// as `created` set them and `b` as `setter` set it.
fn delete(write: WriteId, tick: u64, read: u64, created: WriteId, setter: WriteId) -> WriteRecord {
    let mut record = insert(write, tick, read, 1);
    let row = &mut record.parts[0].rows[0];
    let source = |write| ChangeId { write, row: row.row.clone() };
    let Operation::Insert(columns) = &row.change.operation else { unreachable!() };
    row.old = columns.iter().map(|(name, cell)| (name.clone(), cell.value.clone())).collect();
    row.old.insert("b".into(), Scalar::Text(format!("b by {setter:?}")));
    row.sources = WriteSources {
        generation: Some(source(created)),
        setters: columns
            .keys()
            .map(|column| {
                let by = if column == "b" { setter } else { created };
                (column.clone(), CellSource { change: source(by), column: column.clone() })
            })
            .collect(),
    };
    row.change.generation = 1;
    row.change.operation = Operation::Delete;
    record
}

async fn land(db: &Database, slot: u64, write: WriteRecord, writers: &[DeviceId]) {
    try_land(db, slot, write, writers).await.unwrap();
}

/// Apply `write` alone in `slot`, with `writers` the other devices that can
/// still write.
async fn try_land(
    db: &Database,
    slot: u64,
    write: WriteRecord,
    writers: &[DeviceId],
) -> Result<ApplyOutcome, DbError> {
    let device = write.header.position.device;
    let seed = if device == BEN { 2 } else { 3 };
    db.apply_slot::<crate::pending::tests::Record, _, _>(
        IncomingSlot {
            slot: coven_merge::slot(slot),
            header: SlotHeader {
                device,
                claim: slot,
                author: crate::tests::member(seed),
                after: slot - 1,
            },
            digest: [slot as u8; 32],
            content: SlotContent::Writes {
                size: 1,
                writes: vec![crate::sequence::tests::stream(write.into())],
            },
        },
        Writers::new(writers.iter().map(|device| (*device, None)).collect()),
        || Ok(()),
    )
    .await
}

/// What this device keeps of `write`'s part in the store: its kept records,
/// their verdicts, its part header and leaf hashes, and the write's header
/// evidence.
fn kept(db: &Database, write: WriteId) -> [i64; 5] {
    let id = merge_fields::encode_write_id(&write).unwrap();
    db.inspect_writer(|c| {
        [
            "_coven_authored_changes",
            "_coven_authored_admissions",
            "_coven_authored_parts",
            "_coven_part_leaves",
            "_coven_write_evidence",
        ]
        .map(|table| {
            c.query_row(&format!("SELECT count(*) FROM {table} WHERE write_id=?1"), [&id], |r| {
                r.get(0)
            })
            .unwrap()
        })
    })
}

/// The writes whose kept changes a snapshot of the store carries.
fn snapshot_changes(db: &Database) -> Vec<WriteId> {
    db.inspect_writer(|c| {
        let mut writes = Vec::new();
        let mut part = None;
        crate::write_sources::visit_evidence::<DbError>(c, &Audience::Store, |record| {
            match record {
                AuthoredRecord::Part(header) => part = Some(header.header.position),
                AuthoredRecord::Change(_) => writes.push(part.unwrap()),
            }
            Ok(())
        })
        .unwrap();
        writes
    })
}

/// Ben inserts row 1 and edits its `b`; Cleo replaces his edit in slot 3.
/// Each then lands a write: Ben's reads through slot 3, Cleo's through 4.
async fn overwritten(db: &Database, writers: &[DeviceId]) {
    land(db, 1, insert(id(BEN, 1), 1, 0, 1), writers).await;
    land(db, 2, update(id(BEN, 2), 2, 1, id(BEN, 1), id(BEN, 1)), writers).await;
    land(db, 3, update(id(CLEO, 1), 3, 2, id(BEN, 1), id(BEN, 2)), writers).await;
    land(db, 4, insert(id(BEN, 3), 4, 3, 2), writers).await;
}

/// Ben's edit stopped being current in slot 3. Once Ben has landed a write
/// that read slot 3 and Cleo one that read slot 4, no write either can still
/// land names it: its record, verdict, part header and leaf hashes go, and
/// the write's header evidence with them, and a snapshot no longer carries
/// it. His insert and Cleo's edit are current and stay.
#[tokio::test]
async fn an_edit_every_writer_wrote_past_is_forgotten() {
    let (_store, db) = store().await;
    overwritten(&db, &[BEN, CLEO]).await;
    // Cleo's last write read only slot 2: she could still name Ben's edit.
    assert_eq!(kept(&db, id(BEN, 2)), [1, 1, 1, 1, 1]);
    assert!(snapshot_changes(&db).contains(&id(BEN, 2)));

    land(&db, 5, insert(id(CLEO, 2), 5, 4, 3), &[BEN, CLEO]).await;
    assert_eq!(kept(&db, id(BEN, 2)), [0; 5]);
    assert!(!snapshot_changes(&db).contains(&id(BEN, 2)));
    assert_eq!(kept(&db, id(BEN, 1))[0], 1);
    assert_eq!(kept(&db, id(CLEO, 1))[0], 1);
}

/// The verdict this device gave the change of `write`.
fn verdict(db: &Database, write: WriteId) -> ChangeVerdict {
    let id = merge_fields::encode_write_id(&write).unwrap();
    db.inspect_writer(|c| {
        let admission: Vec<u8> = c
            .query_row(
                "SELECT admission FROM _coven_authored_admissions WHERE write_id=?1",
                [&id],
                |r| r.get(0),
            )
            .unwrap();
        merge_fields::decode_change_verdict(&admission).unwrap()
    })
}

/// A forged write of Ben's or Cleo's, reading through slot 5, names Ben's
/// edit that stopped being current in slot 3. A device that still holds the
/// edit's record, since Dan's idle phone holds it back, drops the source as
/// one that stopped within the write's past; a device that forgot it, and
/// its write's entry, drops it as a landed source it holds no record of,
/// which the forged write read. Both give the forged change the same
/// verdict: as an update it depends on the dropped source, and as a delete
/// it didn't begin with the cells it claims to replace.
#[tokio::test]
async fn a_forged_write_naming_a_forgotten_edit_is_judged_alike() {
    for forged in [
        update(id(BEN, 4), 6, 5, id(BEN, 1), id(BEN, 2)),
        delete(id(BEN, 4), 6, 5, id(BEN, 1), id(BEN, 2)),
        update(id(CLEO, 3), 6, 5, id(BEN, 1), id(BEN, 2)),
        delete(id(CLEO, 3), 6, 5, id(BEN, 1), id(BEN, 2)),
    ] {
        let mut verdicts = Vec::new();
        for writers in [&[BEN, CLEO, DAN][..], &[BEN, CLEO]] {
            let (_store, db) = store().await;
            overwritten(&db, writers).await;
            land(&db, 5, insert(id(CLEO, 2), 5, 4, 3), writers).await;
            assert_eq!(kept(&db, id(BEN, 2))[0], i64::from(writers.contains(&DAN)));
            land(&db, 6, forged.clone(), writers).await;
            let verdict = verdict(&db, forged.header.position);
            assert!(!verdict.retained(), "{verdict:?}");
            verdicts.push(verdict);
        }
        assert_eq!(verdicts[0], verdicts[1]);
    }
}

/// With no other device that can still write and nothing queued here, no
/// write still to be judged can name a change that stopped by the applied
/// slot: each slot forgets what stopped in it, the next one too.
#[tokio::test]
async fn with_no_other_writer_each_slot_forgets_what_stopped_in_it() {
    let (_store, db) = store().await;
    land(&db, 1, insert(id(BEN, 1), 1, 0, 1), &[]).await;
    land(&db, 2, update(id(BEN, 2), 2, 1, id(BEN, 1), id(BEN, 1)), &[]).await;
    land(&db, 3, update(id(CLEO, 1), 3, 2, id(BEN, 1), id(BEN, 2)), &[]).await;
    assert_eq!(kept(&db, id(BEN, 2)), [0; 5]);
    assert_eq!(kept(&db, id(CLEO, 1))[0], 1);
    land(&db, 4, update(id(CLEO, 2), 4, 3, id(BEN, 1), id(CLEO, 1)), &[]).await;
    assert_eq!(kept(&db, id(CLEO, 1)), [0; 5]);
    assert_eq!(kept(&db, id(BEN, 1))[0], 1);
}

/// Whether this device keeps `write`'s applied-write entry, and its
/// plaintext header.
fn entry(db: &Database, write: WriteId) -> (bool, bool) {
    db.inspect_writer(|c| {
        (
            crate::write_metadata::stored_write(c, write).unwrap().is_some(),
            crate::write_headers::read(c, write).unwrap().is_some(),
        )
    })
}

/// The writes section 0 of a snapshot of the store names.
async fn snapshot_writes(db: &Database) -> Vec<WriteId> {
    let snapshot = crate::snapshot_write::tests::capture(
        db,
        crate::snapshot_write::tests::id(Audience::Store),
    )
    .await;
    crate::snapshot_write::tests::decode(&snapshot)
        .1
        .into_iter()
        .filter_map(|record| match record {
            coven_format::snapshot::SnapshotRecord::Write(write) => Some(write.id),
            _ => None,
        })
        .collect()
}

/// Once its record is forgotten, nothing names Ben's write 2 by its entry:
/// no merge row, lost value or kept counted state. Ben's write 3 is his last
/// landed write, and Cleo's write 1 in slot 3 is the latest stamped one at or
/// below the slot both have read past. So Ben's write 2's applied-write entry
/// and plaintext header go, and a snapshot no longer names it. The writes
/// still named stay, and every writes slot still counts toward a snapshot
/// request. With Dan's idle phone holding everything back, it stays.
#[tokio::test]
async fn an_applied_write_nothing_names_is_forgotten() {
    for writers in [&[BEN, CLEO][..], &[BEN, CLEO, DAN]] {
        let (_store, db) = store().await;
        overwritten(&db, writers).await;
        land(&db, 5, insert(id(CLEO, 2), 5, 4, 3), writers).await;
        let idle = writers.contains(&DAN);
        assert_eq!(entry(&db, id(BEN, 2)), (idle, idle));
        assert_eq!(snapshot_writes(&db).await.contains(&id(BEN, 2)), idle);
        for write in [id(BEN, 1), id(CLEO, 1), id(BEN, 3), id(CLEO, 2)] {
            assert_eq!(entry(&db, write), (true, true), "{write:?}");
        }
        assert_eq!(db.snapshot_slot_bytes(Audience::Store, 0).await.unwrap(), 5);
        db.close().await.unwrap();
    }
}

/// Ben's forged write 4 names his write 2's edit once this device forgot
/// write 2's entry. The forged change's verdict names write 2, which section
/// 0 no longer holds: a loader finds it landed from Ben's later writes there,
/// and the snapshot loads. Write 2 landing again in a later slot is still
/// refused as a repeated write.
#[tokio::test]
async fn a_forgotten_write_is_still_known_by_number() {
    let (_store, db) = store().await;
    overwritten(&db, &[BEN, CLEO]).await;
    land(&db, 5, insert(id(CLEO, 2), 5, 4, 3), &[BEN, CLEO]).await;
    land(&db, 6, update(id(BEN, 4), 6, 5, id(BEN, 1), id(BEN, 2)), &[BEN, CLEO]).await;
    assert_eq!(entry(&db, id(BEN, 2)), (false, false));
    let named = verdict(&db, id(BEN, 4));
    assert!(
        matches!(&named, ChangeVerdict::Dependent(sources) if sources.iter().any(|source| source.write == id(BEN, 2))),
        "{named:?}"
    );
    let snapshot = crate::snapshot_write::tests::capture(
        &db,
        crate::snapshot_write::tests::id(Audience::Store),
    )
    .await;
    let (_target_store, target) = store().await;
    let (expected, prefix, plaintext, _) = snapshot.input();
    crate::snapshot_write::tests::load_one(&target, expected, prefix, plaintext).await.unwrap();
    target.close().await.unwrap();

    let repeated = try_land(&db, 7, update(id(BEN, 2), 2, 1, id(BEN, 1), id(BEN, 1)), &[BEN, CLEO])
        .await
        .unwrap_err();
    assert!(
        matches!(
            repeated,
            DbError::InvalidWrite { error: coven_merge::MergeError::DuplicateWrite(write), .. }
                if write == id(BEN, 2)
        ),
        "{repeated:?}"
    );
    db.close().await.unwrap();
}

/// Ben's edit, write 2, stops being current in slot 5, when only slot 3 has
/// been read past by every writer: its record stays, but nothing names write
/// 2 by its entry any more, and Ben's write 3 is his last. The entry goes
/// and the record stays: a snapshot still carries the record, with its stop
/// slot, under its part's header, and loads.
#[tokio::test]
async fn a_record_outlives_its_writes_entry() {
    let (_store, db) = store().await;
    let writers = &[BEN, CLEO];
    land(&db, 1, insert(id(BEN, 1), 1, 0, 1), writers).await;
    land(&db, 2, update(id(BEN, 2), 2, 1, id(BEN, 1), id(BEN, 1)), writers).await;
    land(&db, 3, insert(id(CLEO, 1), 3, 2, 2), writers).await;
    land(&db, 4, insert(id(BEN, 3), 4, 3, 3), writers).await;
    land(&db, 5, update(id(CLEO, 2), 5, 4, id(BEN, 1), id(BEN, 2)), writers).await;
    assert_eq!(entry(&db, id(BEN, 2)), (false, false));
    assert_eq!(kept(&db, id(BEN, 2))[0], 1);
    assert!(snapshot_changes(&db).contains(&id(BEN, 2)));
    let snapshot = crate::snapshot_write::tests::capture(
        &db,
        crate::snapshot_write::tests::id(Audience::Store),
    )
    .await;
    let (_target_store, target) = store().await;
    let (expected, prefix, plaintext, _) = snapshot.input();
    crate::snapshot_write::tests::load_one(&target, expected, prefix, plaintext).await.unwrap();
    assert_eq!(kept(&target, id(BEN, 2))[0], 1);
    target.close().await.unwrap();
    db.close().await.unwrap();
}

/// Fill the forgetting tables for the plan audit, inside its transaction
/// (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_forgotten VALUES(1,counter(1),1);
         INSERT INTO _coven_unnamed_writes SELECT i FROM fixture;",
    )
    .unwrap();
}

/// The full scans the forgetting statements' plans may contain, each with
/// its reason, as the plan audit asks (`internal_schema_scan_reasons.rs`).
pub(crate) fn scan_reason(
    file: &str,
    template: &str,
    _sql: &str,
    relation: &str,
) -> Option<&'static str> {
    match (file, template, relation) {
        (_, _, "_coven_forgotten") => Some("the table holds at most one row"),
        (
            "forget_entries.rs",
            "SELECT write_id FROM _coven_unnamed_writes" | "DELETE FROM _coven_unnamed_writes",
            "_coven_unnamed_writes",
        ) => Some(
            "each applied slot looks once at every write something stopped naming since the last",
        ),
        ("forget_entries.rs", template, "_coven_published_writes")
            if template.contains("FROM _coven_published_writes WHERE true") =>
        {
            Some("letting the whole counted state go looks again at every write it named")
        }
        _ => None,
    }
}
