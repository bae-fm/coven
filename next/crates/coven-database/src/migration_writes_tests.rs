use crate::{
    tests::TestStore,
    write::tests::{notes, records, sql, NOTES},
    *,
};
use coven_format::write_stream::decode_plaintext;
use coven_format::{value::Value as WireValue, write::WriteDisposition};
use coven_merge::{Operation, RowId, RowState};

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn queued_bytes(db: &Database) -> Vec<Vec<u8>> {
    db.inspect_writer(|db| {
        db.query(
            "SELECT record FROM coven_uploads ORDER BY number",
            [],
            |r| r.get(0),
        )
        .unwrap()
    })
}

async fn seal(db: &Database, byte: u8) -> Vec<u8> {
    let write = records(db).remove(0);
    let encoder = coven_format::write_stream::WriteEncoder::new(&write).unwrap();
    let length = coven_format::sealed_write::sealed_length(
        encoder.header_frame().len(),
        &encoder
            .header()
            .parts
            .iter()
            .map(|part| part.plaintext_length)
            .collect::<Vec<_>>(),
    )
    .unwrap() as usize;
    crate::upload::tests::attempt(db, vec![byte; length])
        .await
        .unwrap();
    crate::upload::tests::sealed(db).await.1
}

fn initial() -> Migration {
    Migration::sql(1, "notes", NOTES)
}
fn rename(version: u32, from: &'static str, to: &'static str) -> Migration {
    Migration::run(version, "rename", move |sql| {
        sql.execute_batch(&format!("ALTER TABLE notes RENAME COLUMN {from} TO {to}"))?;
        Ok(())
    })
    .writes(move |change| {
        change.rename_column(from, to);
        Ok(())
    })
}
pub(crate) fn state(db: &Database, row: &RowId) -> RowState<WireValue> {
    db.inspect_writer_schema(|db, schema| {
        let app = crate::write_rows::AppView::after(db, schema);
        crate::merge_store::MergeStore::new(db, &app)
            .row(row)
            .unwrap()
            .state
    })
}
#[tokio::test]
async fn conversions_keep_identity_through_each_breaking_version() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('42','one','body')")
        .await
        .unwrap();
    sql(&db, "UPDATE notes SET title='two'").await.unwrap();
    sql(&db, "UPDATE notes SET title='three',body='other'")
        .await
        .unwrap();
    let original = records(&db);
    db.close().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let db = store
        .builder(
            notes(),
            vec![
                initial(),
                rename(2, "title", "name"),
                Migration::sql(3, "addition", "CREATE TABLE local_extra(value TEXT)").writes(
                    move |_| {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                ),
                rename(4, "name", "heading"),
            ],
        )
        .open()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let converted = records(&db);
    for (old, new) in original.iter().zip(&converted) {
        assert_eq!(old.header.position, new.header.position);
        assert_eq!(old.header.timestamp, new.header.timestamp);
        assert_eq!(old.header.had_read, new.header.had_read);
        assert_eq!(new.header.schema_version, 4);
        assert!(new.parts[0].rows[0]
            .old
            .keys()
            .all(|name| name != "title" && name != "name"));
    }
    sql(&db, "UPDATE notes SET heading='four'").await.unwrap();
    assert_eq!(converted[3].header.disposition, WriteDisposition::Migration);
    assert_eq!(records(&db)[4].header.position.number, 5);
    db.close().await.unwrap();
}

#[tokio::test]
async fn additions_leave_queue_bytes_and_callbacks_untouched() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('42','one','body')")
        .await
        .unwrap();
    let original = queued_bytes(&db);
    db.close().await.unwrap();
    let db = store
        .builder(
            notes(),
            vec![
                initial(),
                Migration::sql(2, "color", "ALTER TABLE notes ADD COLUMN color TEXT")
                    .writes(|_| panic!("addition conversion")),
            ],
        )
        .open()
        .await
        .unwrap();
    assert_eq!(queued_bytes(&db), original);
    db.close().await.unwrap();
}

#[tokio::test]
async fn no_converter_marks_waiting_records_lost_without_changing_local_losses() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('sealed','fixed','body')")
        .await
        .unwrap();
    let sealed = seal(&db, 1).await;
    sql(
        &db,
        "INSERT INTO notes VALUES('42','one','body'),('43','other','')",
    )
    .await
    .unwrap();
    sql(
        &db,
        "UPDATE notes SET title='two' WHERE id='42'; DELETE FROM notes WHERE id='43'",
    )
    .await
    .unwrap();
    let original = records(&db);
    db.close().await.unwrap();
    let db = store
        .builder(
            notes(),
            vec![
                initial(),
                Migration::sql(2, "index", "CREATE INDEX titles ON notes(title)"),
                Migration::sql(3, "index again", "CREATE INDEX bodies ON notes(body)"),
            ],
        )
        .open()
        .await
        .unwrap();
    let actual = records(&db);
    assert_eq!(actual[0], original[0]);
    for (old, new) in original[1..].iter().zip(&actual[1..]) {
        let mut expected = old.clone();
        expected.header.disposition = WriteDisposition::Lost(2);
        assert_eq!(*new, expected);
    }
    assert!(db.lost_values().await.unwrap().is_empty());
    sql(&db, "UPDATE notes SET title='three' WHERE id='42'")
        .await
        .unwrap();
    assert!(db.lost_values().await.unwrap().is_empty());
    db.inspect_writer(|db| {
        assert_eq!(
            db.query_row(
                "SELECT sealed_bytes FROM coven_upload_seals WHERE number=?1",
                [1u64.to_be_bytes().as_slice()],
                |r| r.get::<_, Vec<u8>>(0)
            )
            .unwrap(),
            sealed
        )
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn converter_and_format_failures_roll_back_schema_queue_and_metadata() {
    for malformed in [false, true] {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('42','one','body')")
            .await
            .unwrap();
        let original = records(&db);
        let row = original[0].parts[0].rows[0].row.clone();
        let before = state(&db, &row);
        db.close().await.unwrap();
        let error = store
            .builder(
                notes(),
                vec![
                    initial(),
                    rename(2, "title", "name"),
                    Migration::sql(3, "bad conversion", "CREATE INDEX bodies ON notes(body)")
                        .writes(move |change| {
                            if malformed {
                                change.columns.clear();
                                Ok(())
                            } else {
                                Err(DbError::ClockOutOfRange)
                            }
                        }),
                ],
            )
            .open()
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            CovenError::Migration(MigrationError::Failed {
                version: 3,
                name: "bad conversion",
                ..
            })
        ));
        let db = store.schema(notes(), NOTES).await.unwrap();
        assert_eq!(records(&db), original);
        assert_eq!(state(&db, &row), before);
        assert_eq!(db.schema_version().await.unwrap(), 1);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn conversion_changes_only_uploads_while_the_migration_write_sets_new_cells() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('42','one','body')")
        .await
        .unwrap();
    sql(&db, "UPDATE notes SET title='two'").await.unwrap();
    db.close().await.unwrap();
    let db=store.builder(notes(),vec![initial(),Migration::sql(2,"derived","ALTER TABLE notes RENAME COLUMN title TO name; ALTER TABLE notes ADD COLUMN length INTEGER").writes(|row| {
        row.rename_column("title","name");
        for column in &mut row.columns {
            if column.name=="name" {
                for value in [&mut column.old,&mut column.new].into_iter().flatten() {
                    if let types::Value::Text(text)=value { text.make_ascii_uppercase(); }
                }
            }
        }
        if row.op==ChangeOp::Insert { row.columns.push(ColumnChange::new("length",None,Some(types::Value::Integer(7)))); }
        Ok(())
    })]).open().await.unwrap();
    let actual = db
        .read(|sql| {
            Ok(sql.query_row("SELECT name,length FROM notes", [], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?))
            })?)
        })
        .await
        .unwrap();
    assert_eq!(actual, ("two".into(), None));
    let queue = records(&db);
    let current = state(&db, &queue[0].parts[0].rows[0].row);
    assert_eq!(current.cells()["name"].write, queue[1].header.position);
    assert_eq!(current.cells()["length"].write, queue[2].header.position);
    let Operation::Insert(columns) = &queue[0].parts[0].rows[0].change.operation else {
        panic!("insert")
    };
    assert_eq!(columns["name"].value, WireValue::Text("ONE".into()));
    assert_eq!(columns["length"].value, WireValue::Integer(7));
    db.close().await.unwrap();
}

pub(crate) const REFERENCES:&str="CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL,other TEXT)";
pub(crate) fn reference_tables() -> Vec<SyncedTable> {
    ["parents", "children"]
        .into_iter()
        .map(|t| SyncedTable::new(t, RowIdentity::SharedKey))
        .collect()
}
pub(crate) fn reference_migration() -> Migration {
    Migration::sql(1, "references", REFERENCES)
}

#[tokio::test]
async fn a_sealed_prefix_stays_fixed_while_its_later_update_converts() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('42','one','body')")
        .await
        .unwrap();
    let sealed = seal(&db, 255).await;
    sql(&db, "UPDATE notes SET title='two'").await.unwrap();
    let original = records(&db);
    let bytes = queued_bytes(&db);
    db.close().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let db = store
        .builder(
            notes(),
            vec![
                initial(),
                Migration::sql(2, "rename", "ALTER TABLE notes RENAME COLUMN title TO name")
                    .writes(move |row| {
                        count.fetch_add(1, Ordering::SeqCst);
                        row.rename_column("title", "name");
                        Ok(())
                    }),
            ],
        )
        .open()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let actual = records(&db);
    assert_eq!(actual[0], original[0]);
    assert_eq!(queued_bytes(&db)[0], bytes[0]);
    assert_eq!(actual[1].header.schema_version, 2);
    let current = state(&db, &actual[1].parts[0].rows[0].row);
    assert_eq!(current.cells()["name"].write, actual[1].header.position);
    assert_eq!(current.cells()["body"].write, actual[0].header.position);
    assert!(!current.cells().contains_key("title"));
    db.inspect_writer(|db| {
        assert_eq!(
            db.query_row("SELECT sealed_bytes FROM coven_upload_seals", [], |r| r
                .get::<_, Vec<u8>>(0))
                .unwrap(),
            sealed
        )
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn another_applied_winner_is_preserved_while_older_waiting_writes_convert() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let store = TestStore::with_ids(&ids);
    let peer_store = TestStore::with_ids(&ids);
    let db = store.schema(notes(), NOTES).await.unwrap();
    let peer = peer_store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('42','one','body')")
        .await
        .unwrap();
    peer.apply_downloaded(records(&db).remove(0).into())
        .await
        .unwrap();
    sql(&peer, "UPDATE notes SET title='other device'")
        .await
        .unwrap();
    let record = records(&peer).remove(0);
    let remote = record.header.position.device;
    db.apply_downloaded(record.into()).await.unwrap();
    peer.close().await.unwrap();
    db.close().await.unwrap();
    let db = store
        .builder(notes(), vec![initial(), rename(2, "title", "name")])
        .open()
        .await
        .unwrap();
    let row = &records(&db)[0].parts[0].rows[0].row;
    let current = state(&db, row);
    assert_eq!(current.cells()["name"].write.device, remote);
    assert_eq!(
        current.cells()["name"].value.value,
        WireValue::Text("other device".into())
    );
    assert!(current.lost().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn removing_and_adding_columns_updates_metadata_without_reusing_column_positions() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('42','one','body')")
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = store
        .builder(
            notes(),
            vec![
                initial(),
                Migration::sql(
                    2,
                    "replace body",
                    "ALTER TABLE notes DROP COLUMN body; ALTER TABLE notes ADD COLUMN extra TEXT",
                )
                .writes(|row| {
                    row.columns.retain(|c| c.name != "body");
                    row.columns.reverse();
                    row.columns.push(ColumnChange::new(
                        "extra",
                        None,
                        Some(types::Value::Text("new".into())),
                    ));
                    Ok(())
                }),
            ],
        )
        .open()
        .await
        .unwrap();
    let queue = records(&db);
    let row = &queue[0].parts[0].rows[0];
    let Operation::Insert(columns) = &row.change.operation else {
        panic!("insert")
    };
    assert!(!columns.contains_key("body"));
    assert_eq!(columns["extra"].value, WireValue::Text("new".into()));
    let current = state(&db, &row.row);
    assert!(!current.cells().contains_key("body"));
    assert_eq!(current.cells()["title"].write, queue[0].header.position);
    assert_eq!(current.cells()["extra"].write, queue[1].header.position);
    assert_eq!(current.cells()["extra"].value.value, WireValue::Null);
    sql(&db, "UPDATE notes SET extra='edited'").await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn an_untouched_sealed_column_can_be_dropped() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('42','one','body')")
        .await
        .unwrap();
    seal(&db, 1).await;
    sql(&db, "UPDATE notes SET title='two'").await.unwrap();
    db.close().await.unwrap();
    let db = store
        .builder(
            notes(),
            vec![
                initial(),
                Migration::sql(2, "drop body", "ALTER TABLE notes DROP COLUMN body")
                    .writes(|_| Ok(())),
            ],
        )
        .open()
        .await
        .unwrap();
    let actual = records(&db);
    let current = state(&db, &actual[1].parts[0].rows[0].row);
    assert!(!current.cells().contains_key("body"));
    assert_eq!(current.cells()["title"].write, actual[1].header.position);
    db.close().await.unwrap();
}

#[tokio::test]
async fn additions_before_and_after_the_breaking_change_keep_their_distinct_setters() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('42','one','body')")
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = store
        .builder(
            notes(),
            vec![
                initial(),
                Migration::sql(
                    2,
                    "color",
                    "ALTER TABLE notes ADD COLUMN color TEXT DEFAULT 'blue'",
                ),
                Migration::sql(3,"rename and index","ALTER TABLE notes RENAME COLUMN title TO name; CREATE UNIQUE INDEX colors ON notes(color)").writes(|row| { row.rename_column("title","name"); Ok(()) }),
                Migration::sql(
                    4,
                    "length",
                    "ALTER TABLE notes ADD COLUMN length INTEGER DEFAULT 7",
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    let actual = records(&db);
    assert_eq!(actual[0].header.schema_version, 3);
    let current = state(&db, &actual[0].parts[0].rows[0].row);
    assert!(!current.cells().contains_key("color"));
    assert_eq!(current.cells()["length"].write, actual[1].header.position);
    assert_eq!(
        db.read(
            |sql| Ok(sql.query_row("SELECT color,length FROM notes", [], |r| Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?
            )))?)
        )
        .await
        .unwrap(),
        ("blue".into(), 7)
    );
    sql(&db, "UPDATE notes SET name='two'").await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_chunked_waiting_record_converts_and_reopens_as_one_write() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db,"WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1200) INSERT INTO notes SELECT printf('%04d',i),'title',zeroblob(256) FROM n").await.unwrap();
    let original = records(&db).remove(0);
    assert!(queued_bytes(&db)[0].len() > 4 * coven_format::chunks::CHUNK_SIZE);
    db.close().await.unwrap();
    let migrations = || vec![initial(), rename(2, "title", "name")];
    let db = store.builder(notes(), migrations()).open().await.unwrap();
    let bytes = queued_bytes(&db);
    let converted = decode_plaintext(&bytes[0]).unwrap();
    assert_eq!(converted.header.position, original.header.position);
    assert_eq!(converted.header.timestamp, original.header.timestamp);
    assert_eq!(converted.header.schema_version, 2);
    assert_eq!(converted.parts[0].rows.len(), 1200);
    for row in &converted.parts[0].rows {
        let Operation::Insert(columns) = &row.change.operation else {
            panic!("insert")
        };
        assert_eq!(columns["name"].value, WireValue::Text("title".into()));
    }
    assert_eq!(
        records(&db)[1].header.disposition,
        WriteDisposition::Migration
    );
    db.close().await.unwrap();
    let db = store.builder(notes(), migrations()).open().await.unwrap();
    assert_eq!(queued_bytes(&db), bytes);
    db.close().await.unwrap();
}

#[tokio::test]
async fn old_migration_and_lost_records_keep_their_bytes_on_later_updates() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    db.close().await.unwrap();
    let second = || Migration::sql(2, "index", "CREATE INDEX titles ON notes(title)");
    let db = store
        .builder(notes(), vec![initial(), second()])
        .open()
        .await
        .unwrap();
    let original = queued_bytes(&db);
    assert!(matches!(
        records(&db)[0].header.disposition,
        WriteDisposition::Lost(2)
    ));
    assert_eq!(
        records(&db)[1].header.disposition,
        WriteDisposition::Migration
    );
    db.close().await.unwrap();
    let db = store
        .builder(
            notes(),
            vec![
                initial(),
                second(),
                Migration::sql(3, "another index", "CREATE INDEX bodies ON notes(body)")
                    .writes(|_| panic!("lost and migration records do not convert")),
            ],
        )
        .open()
        .await
        .unwrap();
    let actual = queued_bytes(&db);
    assert_eq!(actual[..2], original);
    assert_eq!(
        records(&db)[2].header.disposition,
        WriteDisposition::Migration
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn parent_renames_override_a_dropped_namesake_in_waiting_references() {
    const SCHEMA: &str = "CREATE TABLE a(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE b(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES a(id))";
    let store = TestStore::new();
    let db = store
        .schema(
            ["a", "b", "children"]
                .into_iter()
                .map(|t| SyncedTable::new(t, RowIdentity::SharedKey))
                .collect(),
            SCHEMA,
        )
        .await
        .unwrap();
    sql(&db,"INSERT INTO a VALUES('p'); INSERT INTO b VALUES('p'); INSERT INTO children VALUES('c','p')").await.unwrap();
    // The old b's insert stays in a fixed upload. Only the child update converts.
    seal(&db, 1).await;
    sql(&db, "UPDATE children SET parent=NULL").await.unwrap();
    sql(&db, "UPDATE children SET parent='p'").await.unwrap();
    db.close().await.unwrap();
    let db = store.builder(vec![SyncedTable::new("b",RowIdentity::SharedKey).key_columns(["key"]),SyncedTable::new("children",RowIdentity::SharedKey)],vec![Migration::sql(1,"initial",SCHEMA),Migration::sql(2,"rename","DROP TABLE b; ALTER TABLE a RENAME TO intermediate; ALTER TABLE intermediate RENAME TO b; ALTER TABLE b RENAME COLUMN id TO key; ALTER TABLE children RENAME COLUMN parent TO root").writes(|row| { row.rename_column("parent","root"); Ok(()) })]).open().await.unwrap();
    let queue = records(&db);
    let Operation::Update(columns) = &queue[2].parts[0].rows[0].change.operation else {
        panic!("update")
    };
    let reference = &columns["root"].parents[&coven_merge::ForeignKey::new(["root"], "b", ["key"])];
    assert_eq!(reference.row.table, "b");
    assert_eq!(reference.generation, 1);
    assert_eq!(
        state(&db, &queue[2].parts[0].rows[0].row).cells()["root"].write,
        queue[2].header.position
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_late_converted_title_competes_with_the_title_setter_and_leaves_the_slug() {
    use coven_format::{
        value::WritePositions,
        write::{WriteHeader, WritePart, WriteRecord},
    };
    use coven_foundation::{clock::FixedClock, id_source::DeviceId};
    use coven_merge::{Audience, Change, ColumnValue, Timestamp, WriteId};
    use std::{
        collections::BTreeMap,
        time::{Duration, UNIX_EPOCH},
    };
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    sql(&db, "UPDATE notes SET title='new title'")
        .await
        .unwrap();
    let original = records(&db);
    let old_stamp = original[1].header.timestamp.milliseconds();
    db.close().await.unwrap();
    let db = store.builder(notes(),vec![initial(),Migration::sql(2,"slug","ALTER TABLE notes RENAME COLUMN title TO name; ALTER TABLE notes ADD COLUMN slug TEXT; UPDATE notes SET slug=replace(name,' ','-')").writes(|r| { r.rename_column("title","name"); Ok(()) })]).clock(Arc::new(FixedClock::new(UNIX_EPOCH+Duration::from_millis(old_stamp+1000)))).open().await.unwrap();
    let late = WriteId {
        device: DeviceId(999),
        number: 1,
    };
    let row = original[0].parts[0].rows[0].row.clone();
    let record = WriteRecord {
        header: WriteHeader {
            store_log_read: coven_format::value::EntryPositions(Vec::new()),
            position: late,
            timestamp: Timestamp::new(old_stamp + 1, 0, late.device).unwrap(),
            had_read: WritePositions(vec![original[0].header.position]),
            schema_version: 2,
            disposition: WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            dismissals: Vec::new(),
            audience: Audience::Store,
            rows: vec![coven_format::write::RowChange {
                row: row.clone(),
                change: Change {
                    generation: 1,
                    operation: Operation::Update(
                        [(
                            "name".into(),
                            ColumnValue {
                                value: WireValue::Text("late title".into()),
                                parents: BTreeMap::new(),
                            },
                        )]
                        .into(),
                    ),
                },
                old: [("name".into(), WireValue::Text("title".into()))].into(),
            }],
        }],
    };
    assert_eq!(
        db.apply_downloaded(record.into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    let current = state(&db, &row);
    assert_eq!(current.cells()["name"].write, late);
    assert_eq!(
        current.cells()["name"].value.value,
        WireValue::Text("late title".into())
    );
    assert_eq!(
        current.cells()["slug"].value.value,
        WireValue::Text("new-title".into())
    );
    assert_eq!(
        current.cells()["slug"].write,
        records(&db)[2].header.position
    );
    db.close().await.unwrap();
}
