use crate::test_utils::tests::*;
use crate::tests::TestStore;
use crate::write::tests::{count, records};
use crate::{Database, RowIdentity, SyncedTable};
use coven_format::value::Value as Scalar;
use coven_format::write::WriteRecord;
use coven_foundation::id_source::CircleId;
use coven_merge::{Audience, ColumnValue, Operation, RowId};
use coven_merge::{ForeignKey, Parent};
use std::collections::BTreeMap;

const MIGRATION_SCHEMA: &str = "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,a INTEGER CHECK(a>=0),b TEXT,parent TEXT REFERENCES rows(id) ON DELETE CASCADE,UNIQUE(audience,b)); CREATE INDEX rows_parent ON rows(parent)";
fn migration_tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("rows", RowIdentity::IndependentUuid).audience_column("audience")]
}
pub(crate) fn circle_id() -> CircleId {
    CircleId(uuid::Uuid::from_u128(1))
}

pub(crate) fn migration_insert(n: u64, key: usize, audience: Audience) -> WriteRecord {
    let mut write = insert(n, key);
    let key =
        uuid::Uuid::from_u128(0x00000000000040008000000000000000 + key as u128 + 1).to_string();
    let row = &mut write.parts[0].rows[0];
    row.row.key = coven_format::key::encode_key(&[Scalar::Text(key.clone())]).unwrap();
    row.row.audience = audience.clone();
    let Operation::Insert(cols) = &mut row.change.operation else { unreachable!() };
    cols.get_mut("id").unwrap().value = Scalar::Text(key);
    cols.get_mut("a").unwrap().value = Scalar::Integer(1);
    cols.insert("parent".into(), ColumnValue { value: Scalar::Null, parents: BTreeMap::new() });
    cols.insert(
        "audience".into(),
        ColumnValue {
            value: Scalar::Text(crate::write_encoding::audience_text(&audience)),
            parents: BTreeMap::new(),
        },
    );
    write.parts[0].audience = audience;
    write
}
fn generations(db: &Database, row: &RowId) -> Vec<(u64, coven_merge::WriteId)> {
    db.inspect_writer_schema(|db, schema| {
        let store = crate::merge_store::MergeStore::from_schema(db, &schema.schema);
        Ok::<_, crate::DbError>(
            store.row(row)?.state.generations().iter().map(|(g, w)| (*g, *w)).collect(),
        )
    })
    .unwrap()
}

fn setters(loss: &crate::LostValue) -> Vec<(String, coven_merge::WriteId)> {
    let mut cells: Vec<_> = match &loss.lost {
        crate::Lost::Cell(cell) => vec![(cell.column.clone(), cell.set_by)],
        crate::Lost::Row(cells) => cells.iter().map(|c| (c.column.clone(), c.set_by)).collect(),
    };
    cells.sort();
    cells
}

fn row_of(write: &WriteRecord) -> RowId {
    write.parts[0].rows[0].row.clone()
}

fn migrated(store: &TestStore, converter: bool) -> crate::DatabaseBuilder {
    let old_writes = if converter {
        crate::OldWrites::Convert(std::sync::Arc::new(|_| Ok(())))
    } else {
        crate::OldWrites::Drop
    };
    let index =
        crate::Migration::breaking(2, "index", "CREATE INDEX rows_a ON rows(a)", old_writes);
    store.builder(
        migration_tables(),
        vec![crate::Migration::addition(1, "rows", MIGRATION_SCHEMA), index],
    )
}

#[tokio::test]
async fn deleted_circle_rows_are_included_in_migration_removals() {
    use crate::Replacement;
    let parent = migration_insert(0, 0, Audience::Circle(circle_id()));
    let mut child = migration_insert(1, 1, Audience::Circle(circle_id()));
    let Operation::Insert(columns) = &mut child.parts[0].rows[0].change.operation else {
        unreachable!()
    };
    columns.insert(
        "parent".into(),
        ColumnValue {
            value: coven_format::key::decode_key(&row_of(&parent).key).unwrap().remove(0),
            parents: BTreeMap::from([(
                ForeignKey::new(["parent"], "rows", ["id"]),
                Parent { row: row_of(&parent), generation: 1 },
            )]),
        },
    );
    child.header.read_through = 1;
    let mut first = migration_insert(2, 2, Audience::Store);
    let mut second = migration_insert(3, 3, Audience::Store);
    for write in [&mut first, &mut second] {
        let Operation::Insert(columns) = &mut write.parts[0].rows[0].change.operation else {
            unreachable!()
        };
        columns.get_mut("b").unwrap().value = Scalar::Text("same".into());
    }
    let store = TestStore::new();
    let db = store.schema(migration_tables(), MIGRATION_SCHEMA).await.unwrap();
    for write in [&parent, &child, &first, &second] {
        assert_eq!(db.receive(write.clone().into()).await.unwrap(), crate::ApplyOutcome::Applied);
    }
    crate::tests::delete_circle(&db, circle_id()).await.unwrap();
    assert_eq!(count(&db, "rows"), 1);
    let hidden = db.lost_values().await.unwrap();
    assert_eq!(hidden.len(), 3);
    db.close().await.unwrap();
    let db = migrated(&store, false).open().await.unwrap();
    let marker = records(&db).last().unwrap().header.position;
    for write in [&parent, &child] {
        assert_eq!(generations(&db, &row_of(write)), [(1, write.header.position), (2, marker)]);
    }
    assert_eq!(generations(&db, &row_of(&second)), [(1, second.header.position), (2, marker)]);
    let losses = db.lost_values().await.unwrap();
    assert_eq!(losses.len(), 3, "{losses:#?}");
    let frozen: Vec<_> = losses
        .iter()
        .filter(|l| matches!(l.replaced_by, Replacement::SchemaChange { version: 2 }))
        .collect();
    assert_eq!(frozen.len(), 3);
    db.close().await.unwrap();
}

/// A converted late edit loses to the migration's counted delete. Loading the
/// same prefix keeps its generations, original evidence and frozen values.
#[tokio::test]
async fn late_edits_to_a_migration_deleted_row_lose_to_its_delete() {
    use crate::write::tests::sql;
    let ids = coven_foundation::id_source::SequentialIds::new();
    let stores: Vec<_> = (0..4).map(|_| TestStore::with_ids(&ids)).collect();
    let ana = stores[0].schema(migration_tables(), MIGRATION_SCHEMA).await.unwrap();
    let carol = stores[1].schema(migration_tables(), MIGRATION_SCHEMA).await.unwrap();
    let target = stores[2].schema(migration_tables(), MIGRATION_SCHEMA).await.unwrap();
    sql(&ana,"INSERT INTO rows(id,audience,a,b) VALUES('00000000-0000-4000-8000-000000000001','store',1,'x')").await.unwrap();
    sql(&carol,"INSERT INTO rows(id,audience,a,b) VALUES('00000000-0000-4000-8000-000000000002','store',1,'x')").await.unwrap();
    let created = records(&carol)[0].clone();
    sql(&carol, "UPDATE rows SET a=5").await.unwrap();
    let edit = records(&carol)[1].clone();
    for write in [records(&ana)[0].clone(), created.clone()] {
        target.receive(write.into()).await.unwrap();
    }
    target.close().await.unwrap();
    let target = migrated(&stores[2], true).open().await.unwrap();
    target.exclude_later_writes(2).await;
    let marker = records(&target).last().unwrap().header.position;
    let fresh = migrated(&stores[3], true).open().await.unwrap();
    for write in records(&fresh) {
        target.receive(write.into()).await.unwrap();
    }
    crate::snapshot_write::tests::assert_loaded_losses(&target, &fresh).await;
    for db in [&target, &fresh] {
        db.receive(edit.clone().into()).await.unwrap();
        assert_eq!(generations(db, &row_of(&created)), [(1, created.header.position), (2, marker)]);
        let losses = db.lost_values().await.unwrap();
        assert!(losses.iter().any(|loss| loss.replaced_by == crate::Replacement::Write(marker)
            && setters(loss) == vec![("a".into(), edit.header.position)]));
        assert!(losses
            .iter()
            .any(|loss| loss.replaced_by == crate::Replacement::SchemaChange { version: 2 }));
    }
    assert_eq!(target.lost_values().await.unwrap(), fresh.lost_values().await.unwrap());
    for db in [ana, carol, target, fresh] {
        db.close().await.unwrap();
    }
}

pub(crate) fn circle_tables() -> Vec<SyncedTable> {
    migration_tables()
}

/// The circle fixture schema, optionally with a droppable column `c`.
pub(crate) fn circle_schema(with_c: bool) -> &'static str {
    if with_c {
        "CREATE TABLE rows(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,a INTEGER CHECK(a>=0),b TEXT,parent TEXT REFERENCES rows(id) ON DELETE CASCADE,c TEXT,UNIQUE(audience,b)); CREATE INDEX rows_parent ON rows(parent)"
    } else {
        MIGRATION_SCHEMA
    }
}

/// One circle row setting every column of `circle_schema(true)`.
pub(crate) fn circle_row() -> WriteRecord {
    let mut write = migration_insert(0, 0, Audience::Circle(circle_id()));
    let Operation::Insert(columns) = &mut write.parts[0].rows[0].change.operation else {
        unreachable!()
    };
    columns.insert(
        "c".into(),
        ColumnValue { value: Scalar::Text("extra".into()), parents: BTreeMap::new() },
    );
    write
}

mod queued {
    //! A local breaking migration leaves every queued original record as it
    //! was authored (§18.5); only the local state and its marker change.
    use crate::{
        tests::TestStore,
        write::tests::{notes, records, sql, NOTES},
        *,
    };
    use coven_format::{value::Value as WireValue, write::WriteDisposition};
    use coven_merge::{Operation, RowId, RowState};
    use std::sync::Arc;

    fn queued_bytes(db: &Database) -> Vec<Vec<u8>> {
        db.inspect_writer(|db| {
            db.query("SELECT record FROM _coven_uploads ORDER BY number", [], |r| r.get(0)).unwrap()
        })
    }
    fn initial() -> Migration {
        Migration::addition(1, "notes", NOTES)
    }
    fn state(db: &Database, row: &RowId) -> RowState<WireValue> {
        db.inspect_writer_schema(|db, schema| {
            let app = crate::write_rows::AppView::after(db, schema);
            crate::merge_store::MergeStore::new(db, &app).row(row).unwrap().state
        })
    }

    #[tokio::test]
    async fn neither_additions_nor_breaking_changes_touch_queued_records_or_call_converters() {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('42','one','body')").await.unwrap();
        // A chunked record keeps its bytes as one write.
        sql(&db,"WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1200) INSERT INTO notes SELECT printf('%04d',i),'title',zeroblob(256) FROM n").await.unwrap();
        let original = queued_bytes(&db);
        assert!(original[1].len() > 4 * coven_format::chunks::CHUNK_SIZE);
        db.close().await.unwrap();
        let migrations = || {
            vec![
                initial(),
                Migration::addition(2, "color", "ALTER TABLE notes ADD COLUMN color TEXT"),
                Migration::breaking(
                    3,
                    "rename",
                    "ALTER TABLE notes RENAME COLUMN title TO name",
                    crate::OldWrites::Convert(std::sync::Arc::new(|_| {
                        panic!("a local migration never converts the queue")
                    })),
                ),
            ]
        };
        let db = store.builder(notes(), migrations()).open().await.unwrap();
        assert_eq!(queued_bytes(&db)[..2], original[..]);
        assert_eq!(records(&db)[2].header.disposition, WriteDisposition::Migration);
        db.close().await.unwrap();
        let db = store.builder(notes(), migrations()).open().await.unwrap();
        assert_eq!(queued_bytes(&db)[..2], original[..]);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_attempted_and_an_untried_record_keep_their_local_setters() {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('42','one','body')").await.unwrap();
        crate::upload::tests::attempt(&db, 255).await.unwrap();
        sql(&db, "UPDATE notes SET title='two'").await.unwrap();
        let original = queued_bytes(&db);
        let writes = records(&db);
        db.close().await.unwrap();
        let db = store
            .builder(
                notes(),
                vec![
                    initial(),
                    Migration::breaking(
                        2,
                        "rename",
                        "ALTER TABLE notes RENAME COLUMN title TO name",
                        crate::OldWrites::Drop,
                    ),
                ],
            )
            .open()
            .await
            .unwrap();
        assert_eq!(queued_bytes(&db)[..2], original[..]);
        let current = state(&db, &writes[1].parts[0].rows[0].row);
        assert_eq!(current.cells()["name"].write, writes[1].header.position);
        assert_eq!(current.cells()["body"].write, writes[0].header.position);
        assert!(!current.cells().contains_key("title"));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn another_applied_winner_keeps_its_cell_through_the_migration() {
        let ids = coven_foundation::id_source::SequentialIds::new();
        let store = TestStore::with_ids(&ids);
        let peer_store = TestStore::with_ids(&ids);
        let db = store.schema(notes(), NOTES).await.unwrap();
        let peer = peer_store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('42','one','body')").await.unwrap();
        db.receive(records(&db)[0].clone().into()).await.unwrap();
        peer.receive(records(&db).remove(0).into()).await.unwrap();
        sql(&peer, "UPDATE notes SET title='other device'").await.unwrap();
        let record = records(&peer).remove(0);
        let remote = record.header.position.device;
        db.receive(record.into()).await.unwrap();
        peer.close().await.unwrap();
        db.close().await.unwrap();
        let db = store
            .builder(
                notes(),
                vec![
                    initial(),
                    Migration::breaking(
                        2,
                        "rename",
                        "ALTER TABLE notes RENAME COLUMN title TO name",
                        crate::OldWrites::Drop,
                    ),
                ],
            )
            .open()
            .await
            .unwrap();
        let row = &records(&db)[0].parts[0].rows[0].row;
        let current = state(&db, row);
        assert_eq!(current.cells()["name"].write.device, remote);
        assert_eq!(current.cells()["name"].value.value, WireValue::Text("other device".into()));
        assert!(current.lost().is_empty());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn removed_and_added_columns_take_new_setters_without_reusing_positions() {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('42','one','body')").await.unwrap();
        let original = queued_bytes(&db);
        db.close().await.unwrap();
        let db = store
            .builder(
                notes(),
                vec![
                    initial(),
                    Migration::breaking(
                        2,
                        "replace body",
                        "ALTER TABLE notes DROP COLUMN body; ALTER TABLE notes ADD COLUMN extra TEXT",
                        crate::OldWrites::Drop,
                    ),
                ],
            )
            .open()
            .await
            .unwrap();
        assert_eq!(queued_bytes(&db)[..1], original[..]);
        let queue = records(&db);
        let current = state(&db, &queue[0].parts[0].rows[0].row);
        assert!(!current.cells().contains_key("body"));
        assert_eq!(current.cells()["title"].write, queue[0].header.position);
        assert_eq!(current.cells()["extra"].write, queue[1].header.position);
        assert_eq!(current.cells()["extra"].value.value, WireValue::Null);
        sql(&db, "UPDATE notes SET extra='edited'").await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_column_used_only_by_attempted_writes_can_be_dropped() {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('42','one','body')").await.unwrap();
        crate::upload::tests::attempt(&db, 1).await.unwrap();
        sql(&db, "UPDATE notes SET title='two'").await.unwrap();
        db.close().await.unwrap();
        let db = store
            .builder(
                notes(),
                vec![
                    initial(),
                    Migration::breaking(
                        2,
                        "drop body",
                        "ALTER TABLE notes DROP COLUMN body",
                        crate::OldWrites::Drop,
                    ),
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
    async fn additions_around_the_breaking_change_keep_their_distinct_setters() {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        crate::raise_capture::tests::connect(&db).await;
        sql(&db, "INSERT INTO notes VALUES('42','one','body')").await.unwrap();
        db.close().await.unwrap();
        let db = store
            .builder(
                notes(),
                vec![
                    initial(),
                    Migration::addition(2, "color", "ALTER TABLE notes ADD COLUMN color TEXT DEFAULT 'blue'"),
                    Migration::breaking(3, "rename and index", "ALTER TABLE notes RENAME COLUMN title TO name; CREATE UNIQUE INDEX colors ON notes(color)", crate::OldWrites::Drop),
                    Migration::addition(4, "length", "ALTER TABLE notes ADD COLUMN length INTEGER DEFAULT 7"),
                ],
            )
            .open()
            .await
            .unwrap();
        let actual = records(&db);
        // The original record keeps its schema; the batch raises once, to 4.
        assert_eq!(actual[0].header.schema_version, 1);
        assert_eq!(db.raise_captures().await.unwrap().first().map_or(0, |c| c.version), 4);
        let current = state(&db, &actual[0].parts[0].rows[0].row);
        assert!(!current.cells().contains_key("color"));
        assert_eq!(current.cells()["length"].write, actual[1].header.position);
        assert_eq!(
            db.read(|sql| Ok(sql.query_row("SELECT color,length FROM notes", [], |r| Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?
            )))?))
            .await
            .unwrap(),
            ("blue".into(), 7)
        );
        sql(&db, "UPDATE notes SET name='two'").await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn publication_intent_commits_with_the_schema_queue_and_migration_write() {
        let migrations = || {
            vec![
                Migration::addition(1, "notes", NOTES),
                Migration::breaking(
                    2,
                    "rename",
                    "ALTER TABLE notes RENAME COLUMN title TO name",
                    crate::OldWrites::Drop,
                ),
            ]
        };
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        crate::raise_capture::tests::connect(&db).await;
        sql(&db, "INSERT INTO notes VALUES('42','title','body')").await.unwrap();
        let original = records(&db);
        db.close().await.unwrap();
        assert!(store
            .builder(notes(), migrations())
            .migration_operation(|_| Err(DbError::ClockOutOfRange))
            .open()
            .await
            .is_err());
        let db = store.schema(notes(), NOTES).await.unwrap();
        assert_eq!(db.schema_version().await.unwrap(), 1);
        assert_eq!(records(&db), original);
        assert!(db.operations().await.unwrap().is_empty());
        assert!(db.raise_captures().await.unwrap().is_empty());
        assert_eq!(db.raise_captures().await.unwrap().first().map_or(0, |c| c.version), 0);
        db.close().await.unwrap();
        let db = store
            .builder(notes(), migrations())
            .migration_operation(|version| {
                Ok(NewOperation {
                    kind: "test-publication".into(),
                    data: version.to_be_bytes().to_vec(),
                    started_by: "coven".into(),
                    pauses_writes: false,
                })
            })
            .open()
            .await
            .unwrap();
        let pending = db.operations().await.unwrap().remove(0);
        assert_eq!(pending.last_step, 1);
        assert_eq!(pending.data, 2u32.to_be_bytes());
        assert_eq!(db.schema_version().await.unwrap(), 2);
        assert_eq!(records(&db).last().unwrap().header.disposition, WriteDisposition::Migration);
        assert_eq!(db.raise_captures().await.unwrap().len(), 1);
        db.close().await.unwrap();
        let db = store
            .builder(notes(), migrations())
            .migration_operation(|_| panic!("already committed"))
            .open()
            .await
            .unwrap();
        assert_eq!(db.operations().await.unwrap()[0].id, pending.id);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_late_new_schema_title_competes_with_the_title_setter_and_leaves_the_slug() {
        use coven_format::write::{WriteHeader, WritePart, WriteRecord};
        use coven_foundation::{clock::FixedClock, id_source::DeviceId};
        use coven_merge::{Audience, Change, ColumnValue, Timestamp, WriteId};
        use std::{
            collections::BTreeMap,
            time::{Duration, UNIX_EPOCH},
        };
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('n','title','body')").await.unwrap();
        sql(&db, "UPDATE notes SET title='new title'").await.unwrap();
        let original = records(&db);
        let old_stamp = original[1].header.timestamp.milliseconds();
        db.close().await.unwrap();
        let db = store.builder(notes(),vec![initial(),Migration::breaking(2, "slug", "ALTER TABLE notes RENAME COLUMN title TO name; ALTER TABLE notes ADD COLUMN slug TEXT; UPDATE notes SET slug=replace(name,' ','-')", crate::OldWrites::Convert(std::sync::Arc::new(|r| { r.rename_column("title","name"); Ok(()) })))]).clock(Arc::new(FixedClock::new(UNIX_EPOCH+Duration::from_millis(old_stamp+1000)))).open().await.unwrap();
        let late = WriteId { device: DeviceId(999), number: 1 };
        let row = original[0].parts[0].rows[0].row.clone();
        let record = WriteRecord {
            header: WriteHeader {
                position: late,
                timestamp: Timestamp::new(old_stamp + 1, 0, late.device).unwrap(),
                read_through: 1,
                schema_version: 2,
                disposition: WriteDisposition::Apply,
            },
            parts: vec![WritePart {
                executions: Vec::new(),
                dismissals: Vec::new(),
                audience: Audience::Store,
                rows: vec![coven_format::write::RowChange {
                    creator: None,
                    execution: Default::default(),
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
                    sources: coven_format::write_sources::WriteSources {
                        generation: Some(coven_format::write_sources::ChangeId {
                            write: original[0].header.position,
                            row: row.clone(),
                        }),
                        setters: [(
                            "name".into(),
                            coven_format::write_sources::CellSource {
                                change: coven_format::write_sources::ChangeId {
                                    write: original[0].header.position,
                                    row: row.clone(),
                                },
                                column: "title".into(),
                            },
                        )]
                        .into(),
                    },
                }],
            }],
        };
        for write in records(&db) {
            db.receive(write.into()).await.unwrap();
        }
        assert_eq!(db.receive(record.into()).await.unwrap(), ApplyOutcome::Applied);
        let current = state(&db, &row);
        assert_eq!(current.cells()["name"].write, late);
        assert_eq!(current.cells()["name"].value.value, WireValue::Text("late title".into()));
        assert_eq!(current.cells()["slug"].value.value, WireValue::Text("new-title".into()));
        assert_eq!(current.cells()["slug"].write, records(&db)[2].header.position);
        db.close().await.unwrap();
    }
}
