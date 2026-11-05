//! A received late old-schema change converts through the winning raise's
//! callback (§18.6, E13). Upload records never change; each case authors
//! writes on an old-schema device and receives them on a migrated one.
use crate::{
    tests::TestStore,
    write::tests::{records, sql},
    *,
};
use coven_format::value::Value as WireValue;
use coven_format::write::WriteRecord;
use coven_foundation::id_source::SequentialIds;
use coven_merge::{RowId, RowState};

const REFERENCES:&str="CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL,other TEXT)";
fn reference_tables() -> Vec<SyncedTable> {
    ["parents", "children"]
        .into_iter()
        .map(|t| SyncedTable::new(t, RowIdentity::SharedKey))
        .collect()
}

fn state(db: &Database, row: &RowId) -> RowState<WireValue> {
    db.inspect_writer_schema(|db, schema| {
        let app = crate::write_rows::AppView::after(db, schema);
        crate::merge_store::MergeStore::new(db, &app).row(row).unwrap().state
    })
}

fn row(writes: &[WriteRecord], table: &str) -> RowId {
    writes
        .iter()
        .flat_map(|w| &w.parts)
        .flat_map(|p| &p.rows)
        .find(|r| r.row.table == table)
        .unwrap()
        .row
        .clone()
}

/// An old-schema device authors `statements`; a device whose app migrated to
/// version 2 with `migration` holds a kept raise to 2 that covers none of them.
struct Case {
    _stores: (TestStore, TestStore),
    source: Database,
    receiver: Database,
    writes: Vec<WriteRecord>,
}

impl Case {
    async fn new(
        tables: impl Fn() -> Vec<SyncedTable>,
        schema: &'static str,
        statements: &[&'static str],
        migration: Migration,
    ) -> Self {
        let ids = SequentialIds::new();
        let stores = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let source = stores.0.schema(tables(), schema).await.unwrap();
        for statement in statements {
            sql(&source, statement).await.unwrap();
        }
        let writes = records(&source);
        let receiver = stores
            .1
            .builder(tables(), vec![Migration::addition(1, "initial", schema), migration])
            .open()
            .await
            .unwrap();
        receiver.exclude_later_writes(2).await;
        Self { _stores: stores, source, receiver, writes }
    }

    /// Receive every write, stopping at the first failure.
    async fn receive(&self) -> Result<(), DbError> {
        for write in &self.writes {
            assert_eq!(self.receiver.receive(write.clone().into()).await?, ApplyOutcome::Applied);
        }
        Ok(())
    }

    /// A failed conversion commits nothing: no position passes and the
    /// original upload records stay as authored.
    async fn assert_unchanged(&self, before: &[WriteRecord]) {
        for write in &self.writes {
            assert!(self
                .receiver
                .checked_write_header(write.header.position)
                .await
                .unwrap()
                .is_none());
        }
        assert_eq!(records(&self.source), before);
    }

    async fn close(self) {
        self.source.close().await.unwrap();
        self.receiver.close().await.unwrap();
    }
}

#[tokio::test]
async fn reference_renames_and_column_reordering_keep_original_parent_generations() {
    let case = Case::new(
        reference_tables,
        REFERENCES,
        &[
            "INSERT INTO parents VALUES('p'); INSERT INTO children VALUES('c','p','value')",
            "DELETE FROM parents",
            "INSERT INTO parents VALUES('p')",
        ],
        Migration::breaking(
            2,
            "parent name",
            "ALTER TABLE children RENAME COLUMN parent TO root",
            crate::OldWrites::Convert(std::sync::Arc::new(|row| {
                if row.table == "children" {
                    row.rename_column("parent", "root");
                    row.columns.reverse();
                }
                Ok(())
            })),
        ),
    )
    .await;
    let original = records(&case.source);
    // The converted insert keeps the parent generation its author named.
    case.receiver.receive(case.writes[0].clone().into()).await.unwrap();
    let child = state(&case.receiver, &row(&case.writes, "children"));
    let parent = &child.cells()["root"].value.parents
        [&coven_merge::ForeignKey::new(["root"], "parents", ["id"])];
    assert_eq!(parent.generation, 1);
    let parent = parent.row.clone();
    for write in &case.writes[1..] {
        case.receiver.receive(write.clone().into()).await.unwrap();
    }
    assert_eq!(state(&case.receiver, &parent).generation(), 3);
    assert_eq!(records(&case.source), original);
    sql(&case.receiver, "UPDATE children SET other='after'").await.unwrap();
    case.close().await;
}

#[tokio::test]
async fn changing_or_adding_a_reference_fails_the_receipt() {
    for variant in 0..5 {
        let migration = Migration::breaking(
            2,
            "bad reference",
            "ALTER TABLE children ADD COLUMN added TEXT REFERENCES parents(id) ON DELETE SET NULL",
            crate::OldWrites::Convert(std::sync::Arc::new(move |row| {
                if row.table == "children" {
                    if variant == 4 {
                        let mut added =
                            row.columns.iter().find(|c| c.name == "parent").unwrap().clone();
                        added.name = "added".into();
                        row.columns.push(added);
                    } else if variant == 2 {
                        row.columns.push(ColumnChange::new(
                            "added",
                            None,
                            Some(types::Value::Null),
                        ));
                    } else if variant == 3 {
                        row.columns.retain(|c| c.name != "parent");
                        row.columns.push(ColumnChange::new(
                            "parent",
                            None,
                            Some(types::Value::Text("p".into())),
                        ));
                    } else {
                        let parent = row.columns.iter_mut().find(|c| c.name == "parent").unwrap();
                        parent.new = Some(if variant == 0 {
                            types::Value::Text("q".into())
                        } else {
                            types::Value::Null
                        });
                    }
                }
                Ok(())
            })),
        );
        let case = Case::new(
            reference_tables,
            REFERENCES,
            &["INSERT INTO parents VALUES('p'),('q'); INSERT INTO children VALUES('c','p','value')"],
            migration,
        )
        .await;
        let original = records(&case.source);
        assert!(
            matches!(case.receive().await, Err(DbError::MigrationReference { .. })),
            "variant {variant}"
        );
        case.assert_unchanged(&original).await;
        case.close().await;
    }
}

#[tokio::test]
async fn a_null_reference_is_still_a_reference_when_converting() {
    let case = Case::new(
        reference_tables,
        REFERENCES,
        &["INSERT INTO parents VALUES('p'); INSERT INTO children VALUES('c',NULL,'value')"],
        Migration::breaking(
            2,
            "index",
            "CREATE INDEX refs ON children(parent)",
            crate::OldWrites::Convert(std::sync::Arc::new(|row| {
                if let Some(column) = row.columns.iter_mut().find(|c| c.name == "parent") {
                    column.new = Some(types::Value::Text("p".into()));
                }
                Ok(())
            })),
        ),
    )
    .await;
    let original = records(&case.source);
    assert!(matches!(case.receive().await, Err(DbError::MigrationReference { .. })));
    case.assert_unchanged(&original).await;
    case.close().await;
}

#[tokio::test]
async fn composite_reference_updates_keep_the_complete_tuple_through_renames() {
    const SCHEMA: &str = "CREATE TABLE parents(a TEXT NOT NULL,b TEXT NOT NULL,PRIMARY KEY(a,b)); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT,FOREIGN KEY(a,b) REFERENCES parents(a,b) ON DELETE CASCADE)";
    let tables = || {
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey).key_columns(["a", "b"]),
            SyncedTable::new("children", RowIdentity::SharedKey),
        ]
    };
    let case = Case::new(
        tables,
        SCHEMA,
        &[
            "INSERT INTO parents VALUES('a','b'),('a','c'); INSERT INTO children VALUES('c','a','b')",
            "UPDATE children SET b='c'",
        ],
        Migration::breaking(2, "rename composite", "ALTER TABLE children RENAME COLUMN a TO renamed", crate::OldWrites::Convert(std::sync::Arc::new(|row| {
            if row.table == "children" {
                row.rename_column("a", "intermediate");
                row.rename_column("intermediate", "renamed");
            }
            Ok(())
        }))),
    )
    .await;
    case.receive().await.unwrap();
    let child = state(&case.receiver, &row(&case.writes, "children"));
    let cells = child.cells();
    assert_eq!(cells["b"].value.parents.keys().next().unwrap().columns.0, ["renamed", "b"]);
    assert_eq!(cells["renamed"].value.parents, cells["b"].value.parents);
    assert_eq!(cells["renamed"].value.value, WireValue::Text("a".into()));
    assert_eq!(cells["b"].value.value, WireValue::Text("c".into()));
    assert_eq!(cells["b"].write, case.writes[1].header.position);
    case.close().await;
}

/// The device cannot know which parent generation the old insert would
/// have meant for a new reference column, so its receipt fails and waits (E13).
#[tokio::test]
async fn an_old_insert_cannot_convert_into_a_new_reference_column() {
    let case = Case::new(
        reference_tables,
        REFERENCES,
        &["INSERT INTO parents VALUES('p'); INSERT INTO children VALUES('c','p','value')"],
        Migration::breaking(
            2,
            "optional reference",
            "ALTER TABLE children ADD COLUMN added TEXT REFERENCES parents(id) ON DELETE SET NULL",
            crate::OldWrites::Convert(std::sync::Arc::new(|_| Ok(()))),
        ),
    )
    .await;
    let original = records(&case.source);
    assert!(matches!(
        case.receive().await,
        Err(DbError::Snapshot(crate::SnapshotError::Inconsistent(
            "present row is missing a key or reference cell"
        )))
    ));
    case.assert_unchanged(&original).await;
    case.close().await;
}

#[tokio::test]
async fn two_references_keep_distinct_primary_key_orders() {
    const SCHEMA: &str = "CREATE TABLE parents(id TEXT NOT NULL,alternate TEXT NOT NULL,PRIMARY KEY(id,alternate)); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT,other TEXT,FOREIGN KEY(parent,other) REFERENCES parents,FOREIGN KEY(parent,other) REFERENCES parents(alternate,id))";
    let tables = || {
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey).key_columns(["id", "alternate"]),
            SyncedTable::new("children", RowIdentity::SharedKey),
        ]
    };
    let case = Case::new(
        tables,
        SCHEMA,
        &["INSERT INTO parents VALUES('1','2'),('2','1'); INSERT INTO children VALUES('c','1','2')"],
        Migration::breaking(2, "rename", "ALTER TABLE children RENAME COLUMN parent TO renamed", crate::OldWrites::Convert(std::sync::Arc::new(|row| {
            if row.table == "children" {
                row.rename_column("parent", "renamed");
            }
            Ok(())
        }))),
    )
    .await;
    case.receive().await.unwrap();
    let child = state(&case.receiver, &row(&case.writes, "children"));
    let parents = &child.cells()["renamed"].value.parents;
    assert_eq!(parents.len(), 2);
    for (target, values) in [(["id", "alternate"], ["1", "2"]), (["alternate", "id"], ["2", "1"])] {
        let key = coven_merge::ForeignKey::new(["renamed", "other"], "parents", target);
        assert_eq!(
            coven_format::key::decode_key(&parents[&key].row.key).unwrap(),
            values.map(|v| WireValue::Text(v.into()))
        );
    }
    case.close().await;
}

#[tokio::test]
async fn replacing_a_reference_target_cannot_reuse_the_old_parents_generation() {
    const SCHEMA: &str = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE others(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id))";
    let tables = || {
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey),
            SyncedTable::new("others", RowIdentity::SharedKey),
            SyncedTable::new("children", RowIdentity::SharedKey),
        ]
    };
    let case = Case::new(
        tables,
        SCHEMA,
        &["INSERT INTO parents VALUES('1'); INSERT INTO others VALUES('1'); INSERT INTO children VALUES('c','1')"],
        Migration::breaking(2, "retarget", "CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES others(id)); INSERT INTO rebuilt SELECT * FROM children; DROP TABLE children; ALTER TABLE rebuilt RENAME TO children", crate::OldWrites::Convert(std::sync::Arc::new(|_| Ok(())))),
    )
    .await;
    let original = records(&case.source);
    assert!(matches!(case.receive().await, Err(DbError::MigrationReference { .. })));
    case.assert_unchanged(&original).await;
    case.close().await;
}

#[tokio::test]
async fn invalid_conversions_report_typed_causes_and_commit_nothing() {
    const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT)";
    for variant in 0..9 {
        let declarations = || vec![SyncedTable::new("notes", RowIdentity::SharedKey)];
        let migration = Migration::breaking(2, "conversion", "CREATE TABLE no_key(id TEXT,title TEXT); CREATE TABLE other(id TEXT NOT NULL PRIMARY KEY,title TEXT); CREATE INDEX titles ON notes(title)", crate::OldWrites::Convert(std::sync::Arc::new(move |row| {
            let column = row
                .columns
                .iter()
                .find(|c| c.name == "title")
                .unwrap()
                .clone();
            match variant {
                0 => row.columns.push(column),
                1 => row.op = ChangeOp::Update,
                2 => row.rename_column("title", "absent"),
                3 => row.key = RowKey(vec!["7".to_owned().into(), "extra".to_owned().into()]),
                4 => row.table = "missing".into(),
                5 => row.table = "no_key".into(),
                6 => row.key = RowKey(vec!["different".to_owned().into()]),
                7 => {
                    row.op = ChangeOp::Delete;
                    for column in &mut row.columns {
                        column.old = column.new.take();
                    }
                }
                8 => row.table = "other".into(),
                _ => unreachable!(),
            }
            Ok(())
        })));
        let case =
            Case::new(declarations, SCHEMA, &["INSERT INTO notes VALUES('7','before')"], migration)
                .await;
        let original = records(&case.source);
        let error = case.receive().await.unwrap_err();
        match variant {
            0 => assert!(
                matches!(error, DbError::MigrationDuplicateColumn { ref table, ref column } if table == "notes" && column == "title")
            ),
            1 => assert!(
                matches!(error, DbError::MigrationColumnOperation { ref table, op: ChangeOp::Update, has_old: false, has_new: true, .. } if table == "notes")
            ),
            2 => assert!(
                matches!(error, DbError::MigrationColumnMissing { ref table, ref column } if table == "notes" && column == "absent")
            ),
            3 => assert!(
                matches!(error, DbError::MigrationKeyArity { ref table, expected: 1, actual: 2 } if table == "notes")
            ),
            4 => assert!(
                matches!(error, DbError::MigrationTableMissing { ref table } if table == "missing")
            ),
            5 => assert!(
                matches!(error, DbError::MigrationPrimaryKeyMissing { ref table } if table == "no_key")
            ),
            6..=8 => assert!(matches!(
                error,
                DbError::WriteFormat(coven_format::Error::Invalid {
                    field: "converted row identity or operation",
                    ..
                })
            )),
            _ => unreachable!(),
        }
        case.assert_unchanged(&original).await;
        case.close().await;
    }
}

mod data_sources {
    //! Ben's late old-schema writes 9 and 10 after a raise (§18.6): the data
    //! rule runs on original changes before any converter (E13).
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const NOTES: &str =
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL)";
    fn tables() -> Vec<SyncedTable> {
        vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
    }
    fn initial() -> Migration {
        Migration::addition(1, "notes", NOTES)
    }
    fn rename(
        version: u32,
        from: &'static str,
        to: &'static str,
        calls: Option<Arc<AtomicUsize>>,
    ) -> Migration {
        let sql: &'static str =
            Box::leak(format!("ALTER TABLE notes RENAME COLUMN {from} TO {to}").into_boxed_str());
        let old_writes = match calls {
            Some(calls) => crate::OldWrites::Convert(Arc::new(move |row| {
                calls.fetch_add(1, Ordering::SeqCst);
                row.rename_column(from, to);
                Ok(())
            })),
            None => crate::OldWrites::Drop,
        };
        Migration::breaking(version, "rename", sql, old_writes)
    }
    async fn open(store: &TestStore, migrations: Vec<Migration>) -> Database {
        store.builder(tables(), migrations).open().await.unwrap()
    }
    async fn names(db: &Database, column: &'static str) -> Vec<(String, String)> {
        db.read(move |sql| {
            Ok(sql.query(&format!("SELECT id,{column} FROM notes ORDER BY id"), [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?)
        })
        .await
        .unwrap()
    }
    fn source(write: &WriteRecord, id: &str) -> crate::ChangeSource {
        crate::ChangeSource {
            write: write.header.position,
            table: "notes".into(),
            key: RowKey(vec![types::Value::Text(id.into())]),
            audience: coven_merge::Audience::Store,
        }
    }

    /// Ben's 9 creates a note and is excluded by an explicit drop choice;
    /// his 10 edits that note and renames an unrelated playlist. With the
    /// converter, 10's note edit is a frozen dependency loss naming 9 and its
    /// playlist rename converts and applies; the callback never sees the note.
    #[tokio::test]
    async fn a_change_built_on_an_excluded_write_stays_frozen_beside_a_converted_one() {
        for setter_only in [false, true] {
            let ids = SequentialIds::new();
            let (ben_store, receiver_store) =
                (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
            let ben = ben_store.schema(tables(), NOTES).await.unwrap();
            sql(&ben, "INSERT INTO notes VALUES('p','playlist','b'),('n','covered note','b')")
                .await
                .unwrap();
            if setter_only {
                // 9 replaces the covered note's title: its generation survives.
                sql(&ben, "UPDATE notes SET title='excluded title' WHERE id='n'").await.unwrap();
            } else {
                sql(&ben, "DELETE FROM notes WHERE id='n'").await.unwrap();
                sql(&ben, "INSERT INTO notes VALUES('n','new note','b')").await.unwrap();
            }
            ben.write(|sql| {
                sql.execute("UPDATE notes SET title='edited' WHERE id='n'", [])?;
                sql.execute("UPDATE notes SET title='renamed playlist' WHERE id='p'", [])?;
                Ok(())
            })
            .await
            .unwrap();
            let writes = records(&ben);
            // The receiver applied Ben's first write before its app migrated;
            // the kept raise covers it.
            let receiver = open(&receiver_store, vec![initial()]).await;
            receiver.receive(writes[0].clone().into()).await.unwrap();
            receiver.close().await.unwrap();
            let receiver =
                open(&receiver_store, vec![initial(), rename(2, "title", "name", None)]).await;
            receiver.exclude_later_writes(2).await;
            for write in &writes[1..writes.len() - 1] {
                receiver.receive(write.clone().into()).await.unwrap();
            }
            receiver.close().await.unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let receiver = open(
                &receiver_store,
                vec![initial(), rename(2, "title", "name", Some(calls.clone()))],
            )
            .await;
            let ten = writes.last().unwrap();
            assert_eq!(receiver.receive(ten.clone().into()).await.unwrap(), ApplyOutcome::Applied);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let note = if setter_only {
                vec![source(&writes[1], "n")]
            } else {
                vec![source(&writes[2], "n"), source(&writes[2], "n")]
            };
            let dependency = receiver
                .lost_values()
                .await
                .unwrap()
                .into_iter()
                .filter_map(|loss| match loss.replaced_by {
                    Replacement::DataDependency { write, sources } => Some((write, sources)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(dependency, [(ten.header.position, note)]);
            // Neither excluded write applied: the covered note keeps its value.
            let rows = names(&receiver, "name").await;
            assert_eq!(
                rows,
                [
                    ("n".to_owned(), "covered note".to_owned()),
                    ("p".to_owned(), "renamed playlist".to_owned())
                ]
            );
            // Reopening keeps the committed interpretation.
            receiver.close().await.unwrap();
            let receiver = open(
                &receiver_store,
                vec![initial(), rename(2, "title", "name", Some(calls.clone()))],
            )
            .await;
            assert_eq!(names(&receiver, "name").await, rows);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            receiver.close().await.unwrap();
            ben.close().await.unwrap();
        }
    }

    /// Ben's 9 is excluded on a device that then takes a snapshot; a device
    /// loading it receives Ben's 10 late and still treats 9 as a source that
    /// doesn't count, so 10's edit of the same note is a frozen dependency
    /// loss while its playlist change converts (§16.1, §9.5).
    #[tokio::test]
    async fn a_loaded_snapshot_keeps_an_excluded_writes_verdict_for_later_dependents() {
        let ids = SequentialIds::new();
        let (ben_store, receiver_store, loader_store) =
            (TestStore::with_ids(&ids), TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let calls = Arc::new(AtomicUsize::new(0));
        // The receiver drops late old-schema writes; the loader converts them.
        let migrations = |old_writes| {
            vec![
                initial(),
                Migration::breaking(
                    2,
                    "index",
                    "CREATE INDEX notes_title ON notes(title)",
                    old_writes,
                ),
            ]
        };
        let ben = ben_store.schema(tables(), NOTES).await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('p','playlist','b'),('n','covered note','b')")
            .await
            .unwrap();
        sql(&ben, "UPDATE notes SET title='excluded title' WHERE id='n'").await.unwrap();
        ben.write(|sql| {
            sql.execute("UPDATE notes SET title='edited' WHERE id='n'", [])?;
            sql.execute("UPDATE notes SET title='renamed playlist' WHERE id='p'", [])?;
            Ok(())
        })
        .await
        .unwrap();
        let writes = records(&ben);
        let receiver = open(&receiver_store, vec![initial()]).await;
        receiver.receive(writes[0].clone().into()).await.unwrap();
        receiver.close().await.unwrap();
        let receiver = open(&receiver_store, migrations(crate::OldWrites::Drop)).await;
        receiver.exclude_later_writes(2).await;
        receiver.receive(writes[1].clone().into()).await.unwrap();
        let snapshot =
            crate::snapshot_write::tests::frames(&receiver, coven_merge::Audience::Store).await;
        open(&loader_store, vec![initial()]).await.close().await.unwrap();
        let counted = calls.clone();
        let converting = crate::OldWrites::Convert(Arc::new(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
        let loader = open(&loader_store, migrations(converting)).await;
        loader.exclude_later_writes(2).await;
        loader
            .load_snapshots::<crate::pending::tests::Record, std::io::Cursor<Vec<u8>>>(
                crate::tests::reload(
                    &loader,
                    vec![snapshot.input()],
                    vec![],
                    Arc::new(crate::test_utils::TestAuthorship),
                )
                .await,
                None,
            )
            .await
            .unwrap();
        let ten = &writes[2];
        assert_eq!(loader.receive(ten.clone().into()).await.unwrap(), ApplyOutcome::Applied);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let dependency = loader
            .lost_values()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|loss| match loss.replaced_by {
                Replacement::DataDependency { write, sources } => Some((write, sources)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(dependency, [(ten.header.position, vec![source(&writes[1], "n")])]);
        assert_eq!(
            names(&loader, "title").await,
            [
                ("n".to_owned(), "covered note".to_owned()),
                ("p".to_owned(), "renamed playlist".to_owned())
            ]
        );
        for db in [ben, receiver, loader] {
            db.close().await.unwrap();
        }
    }

    /// A late write converts through every raise between its version and the
    /// winning one, in version order; a covered write never reaches a callback.
    #[tokio::test]
    async fn repeated_raises_convert_in_version_order_and_skip_covered_writes() {
        let ids = SequentialIds::new();
        let (ben_store, receiver_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let ben = ben_store.schema(tables(), NOTES).await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('n','covered','b')").await.unwrap();
        sql(&ben, "UPDATE notes SET title='late'").await.unwrap();
        // An empty delete still converts as a delete of its row.
        sql(&ben, "DELETE FROM notes").await.unwrap();
        let mut writes = records(&ben);
        writes[2].parts[0].rows[0].old.clear();
        let calls = Arc::new(AtomicUsize::new(0));
        let receiver = open(&receiver_store, vec![initial()]).await;
        receiver.receive(writes[0].clone().into()).await.unwrap();
        receiver.close().await.unwrap();
        let receiver = open(
            &receiver_store,
            vec![
                initial(),
                rename(2, "title", "heading", Some(calls.clone())),
                rename(3, "heading", "name", Some(calls.clone())),
            ],
        )
        .await;
        receiver.exclude_later_writes(3).await;
        assert_eq!(
            receiver.receive(writes[0].clone().into()).await.unwrap(),
            ApplyOutcome::AlreadyApplied
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        receiver.receive(writes[1].clone().into()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(names(&receiver, "name").await, [("n".into(), "late".into())]);
        receiver.receive(writes[2].clone().into()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert!(names(&receiver, "name").await.is_empty());
        assert!(receiver.lost_values().await.unwrap().is_empty());
        receiver.close().await.unwrap();
        ben.close().await.unwrap();
    }

    /// Ben's 9 creates a note and an explicit drop choice excludes it. A later
    /// raise to version 4, whose converter would accept the write, keeps the
    /// frozen loss with its values and cause; receiving 9 again neither calls
    /// the converter nor revives the note (§18.6).
    #[tokio::test]
    async fn a_frozen_loss_keeps_its_values_and_cause_through_a_later_raise() {
        let ids = SequentialIds::new();
        let (ben_store, receiver_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let ben = ben_store.schema(tables(), NOTES).await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('n','title','b')").await.unwrap();
        let nine = records(&ben).remove(0);
        let receiver =
            open(&receiver_store, vec![initial(), rename(2, "title", "name", None)]).await;
        receiver.exclude_later_writes(2).await;
        assert_eq!(receiver.receive(nine.clone().into()).await.unwrap(), ApplyOutcome::Applied);
        let frozen = receiver.lost_values().await.unwrap();
        assert!(
            matches!(&frozen[..], [loss] if loss.replaced_by == Replacement::SchemaChange { version: 2 }),
            "{frozen:?}"
        );
        receiver.close().await.unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let receiver = open(
            &receiver_store,
            vec![
                initial(),
                rename(2, "title", "name", None),
                Migration::addition(3, "color", "ALTER TABLE notes ADD COLUMN color TEXT"),
                rename(4, "name", "heading", Some(calls.clone())),
            ],
        )
        .await;
        receiver.exclude_later_writes(4).await;
        assert_eq!(receiver.lost_values().await.unwrap(), frozen);
        assert_eq!(receiver.receive(nine.into()).await.unwrap(), ApplyOutcome::AlreadyApplied);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(names(&receiver, "heading").await.is_empty());
        assert_eq!(receiver.lost_values().await.unwrap(), frozen);
        receiver.close().await.unwrap();
        ben.close().await.unwrap();
    }

    /// Ben's writes 1 and 2 are excluded by a drop choice. His migrated app
    /// then writes 4, after a marker, with both earlier writes among its own
    /// predecessors: the writes it recorded as read order it, and never
    /// exclude it, so every receiver applies it with no data-dependency
    /// loss (§18.6).
    #[tokio::test]
    async fn recorded_reads_order_a_write_but_never_exclude_it() {
        let ids = SequentialIds::new();
        let (ben_store, receiver_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let ben = ben_store.schema(tables(), NOTES).await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('a','first','b')").await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('b','second','b')").await.unwrap();
        ben.close().await.unwrap();
        let ben = open(&ben_store, vec![initial(), rename(2, "title", "name", None)]).await;
        sql(&ben, "INSERT INTO notes(id,name,body) VALUES('z','third','b')").await.unwrap();
        let writes = records(&ben);
        let numbers: Vec<_> = writes.iter().map(|w| w.header.position.number).collect();
        assert_eq!(numbers, [1, 2, 3, 4]);
        assert_eq!(writes[3].header.schema_version, 2);

        let receiver =
            open(&receiver_store, vec![initial(), rename(2, "title", "name", None)]).await;
        receiver.exclude_later_writes(2).await;
        for write in &writes {
            assert_eq!(
                receiver.receive(write.clone().into()).await.unwrap(),
                ApplyOutcome::Applied
            );
        }
        assert_eq!(names(&receiver, "name").await, [("z".to_owned(), "third".to_owned())]);
        let losses = receiver.lost_values().await.unwrap();
        assert_eq!(losses.len(), 2, "{losses:?}");
        assert!(losses
            .iter()
            .all(|loss| loss.replaced_by == Replacement::SchemaChange { version: 2 }));
        receiver.close().await.unwrap();
        ben.close().await.unwrap();
    }

    /// Note n keeps its generation from a covered write, its title set by the
    /// excluded 9. Ben's 10 edits that title and renames a playlist: the
    /// title edit is frozen on its excluded setter, the rename converts. A
    /// later edit of the playlist applies; a later edit of the frozen title
    /// loses with it (§18.6).
    #[tokio::test]
    async fn the_replaced_setter_decides_and_later_changes_follow_each_part() {
        let ids = SequentialIds::new();
        let (ben_store, receiver_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let ben = ben_store.schema(tables(), NOTES).await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('p','playlist','b'),('n','covered note','b')")
            .await
            .unwrap();
        sql(&ben, "UPDATE notes SET title='excluded title' WHERE id='n'").await.unwrap();
        ben.write(|sql| {
            sql.execute("UPDATE notes SET title='edited' WHERE id='n'", [])?;
            sql.execute("UPDATE notes SET title='renamed playlist' WHERE id='p'", [])?;
            Ok(())
        })
        .await
        .unwrap();
        sql(&ben, "UPDATE notes SET title='playlist again' WHERE id='p'").await.unwrap();
        sql(&ben, "UPDATE notes SET title='built on the frozen edit' WHERE id='n'").await.unwrap();
        let writes = records(&ben);
        assert_eq!(writes.len(), 5);

        let receiver = open(&receiver_store, vec![initial()]).await;
        receiver.receive(writes[0].clone().into()).await.unwrap();
        receiver.close().await.unwrap();
        let receiver =
            open(&receiver_store, vec![initial(), rename(2, "title", "name", None)]).await;
        receiver.exclude_later_writes(2).await;
        receiver.receive(writes[1].clone().into()).await.unwrap();
        receiver.close().await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let receiver =
            open(&receiver_store, vec![initial(), rename(2, "title", "name", Some(calls.clone()))])
                .await;
        for write in &writes[2..] {
            assert_eq!(
                receiver.receive(write.clone().into()).await.unwrap(),
                ApplyOutcome::Applied
            );
        }
        // Only the playlist's changes reach the converter.
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            names(&receiver, "name").await,
            [
                ("n".to_owned(), "covered note".to_owned()),
                ("p".to_owned(), "playlist again".to_owned())
            ]
        );
        let dependencies: Vec<_> = receiver
            .lost_values()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|loss| match loss.replaced_by {
                Replacement::DataDependency { write, sources } => Some((write, sources)),
                _ => None,
            })
            .collect();
        assert_eq!(
            dependencies,
            [
                (writes[2].header.position, vec![source(&writes[1], "n")]),
                (writes[4].header.position, vec![source(&writes[2], "n")]),
            ]
        );
        receiver.close().await.unwrap();
        ben.close().await.unwrap();
    }
}

/// A step's classification comes from its statements, never this device's
/// rows: a receiver that ran a data step on an empty table, as a fresh
/// device does, converts a late old-schema write exactly as one with rows
/// does (§18.1).
#[tokio::test]
async fn a_data_step_run_on_an_empty_table_still_converts_late_writes() {
    use crate::write::tests::{notes, NOTES};
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = calls.clone();
    let slug = "ALTER TABLE notes ADD COLUMN slug TEXT; UPDATE notes SET slug=lower(title)";
    let migration = Migration::breaking(
        2,
        "slug",
        slug,
        crate::OldWrites::Convert(std::sync::Arc::new(move |_| {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })),
    );
    let insert = "INSERT INTO notes(id,title,body) VALUES('n','Title','b')";
    let case = Case::new(notes, NOTES, &[insert], migration).await;
    assert_eq!(case.receiver.applied_migrations().unwrap()[1].change, MigrationChange::Breaking);
    case.receive().await.unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(case.receiver.lost_values().await.unwrap().is_empty());
    case.close().await;
}

const TITLES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL)";

fn title_tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

/// A version 2 that converts late old-schema writes unchanged and counts
/// each change its callback sees.
fn counting(calls: &std::sync::Arc<std::sync::atomic::AtomicUsize>) -> Vec<Migration> {
    let calls = calls.clone();
    vec![
        Migration::addition(1, "notes", TITLES),
        Migration::breaking(
            2,
            "titles",
            "CREATE INDEX titles ON notes(title)",
            crate::OldWrites::Convert(std::sync::Arc::new(move |_| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })),
        ),
    ]
}

async fn note_titles(db: &Database) -> Vec<(String, String)> {
    db.read(|sql| {
        Ok(sql
            .query("SELECT id,title FROM notes ORDER BY id", [], |r| Ok((r.get(0)?, r.get(1)?)))?)
    })
    .await
    .unwrap()
}

/// Ben's old-schema insert is sealed under a member key whose replacement
/// slot comes before it, so Ana refuses it whole, with a converter declared:
/// no callback, no row, no lost value. Carol, who applied the insert before
/// the replacement, edits the note in a write built on it; that change uses
/// data Ana refused, so it is a frozen loss naming Ben's write, and the
/// callback never sees it (§14.1, §18.6).
#[tokio::test]
async fn a_write_cut_off_by_key_replacement_stays_refused_under_a_converter() {
    let ids = SequentialIds::new();
    let (ana_store, ben_store, carol_store) =
        (TestStore::with_ids(&ids), TestStore::with_ids(&ids), TestStore::with_ids(&ids));
    let ben = ben_store.schema(title_tables(), TITLES).await.unwrap();
    let carol = carol_store.schema(title_tables(), TITLES).await.unwrap();
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ana = ana_store.builder(title_tables(), counting(&calls)).open().await.unwrap();
    for db in [&ana, &ben, &carol] {
        TestStore::register(db, ben_store.device(), 2);
        TestStore::register(db, carol_store.device(), 3);
    }
    sql(&ben, "INSERT INTO notes VALUES('n','first')").await.unwrap();
    let insert = records(&ben).remove(0);
    carol.receive(insert.clone().into()).await.unwrap();
    sql(&carol, "UPDATE notes SET title='second'").await.unwrap();
    let edit = records(&carol).remove(0);

    ana.exclude_later_writes(2).await;
    let state = ana.store_state().await.unwrap();
    ana.admin_fixture(
        coven_format::admin::StoreChange::ReplaceMemberKeys {
            member: crate::tests::member(2),
            expected: coven_format::test_utils::keys_from_seed(0xA5).public_keys(),
            keys: coven_format::test_utils::keys_from_seed(2).public_keys(),
        },
        state,
        None,
    )
    .await
    .unwrap();
    ana.receive(insert.clone().into()).await.unwrap();
    // Carol's edit read Ben's insert at the slot Ana gave it.
    let mut edit = edit;
    edit.header.read_through = ana.applied_slot().await.unwrap();
    assert!(note_titles(&ana).await.is_empty());
    assert!(ana.lost_values().await.unwrap().is_empty());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);

    ana.receive(edit.clone().into()).await.unwrap();
    assert!(note_titles(&ana).await.is_empty());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    let dependency: Vec<_> =
        ana.lost_values().await.unwrap().into_iter().map(|loss| loss.replaced_by).collect();
    assert!(
        matches!(&dependency[..], [Replacement::DataDependency { write, sources }]
            if *write == edit.header.position
                && sources.iter().all(|source| source.write == insert.header.position)),
        "{dependency:?}"
    );
    for db in [ana, ben, carol] {
        db.close().await.unwrap();
    }
}

/// Another member's old-schema edit of a row she did not create is refused by
/// the owner rule against the current declaration before any converter runs:
/// the row keeps its title, the loss names its owner, and the callback never
/// sees the change (§9.6, §18.6).
#[tokio::test]
async fn an_old_write_the_owner_rule_refuses_never_reaches_the_converter() {
    let ids = SequentialIds::new();
    let (ana_store, mallory_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
    let owned = || vec![SyncedTable::new("notes", RowIdentity::SharedKey).owner_only()];
    let ana = ana_store.schema(owned(), TITLES).await.unwrap();
    // Mallory's app declares no owner-only tables, so it authors the edit.
    let mallory = mallory_store.schema(title_tables(), TITLES).await.unwrap();
    for db in [&ana, &mallory] {
        TestStore::register(db, ana_store.device(), 1);
        TestStore::register(db, mallory_store.device(), 2);
    }
    sql(&ana, "INSERT INTO notes VALUES('n','mine')").await.unwrap();
    let created = records(&ana).remove(0);
    crate::tests::publish(&ana).await.unwrap();
    mallory.receive(created.into()).await.unwrap();
    sql(&mallory, "UPDATE notes SET title='taken'").await.unwrap();
    let forged = records(&mallory).remove(0);
    ana.close().await.unwrap();

    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ana = ana_store.builder(owned(), counting(&calls)).open().await.unwrap();
    ana.exclude_later_writes(2).await;
    ana.receive(forged.clone().into()).await.unwrap();
    assert_eq!(note_titles(&ana).await, [("n".to_owned(), "mine".to_owned())]);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    let losses: Vec<_> =
        ana.lost_values().await.unwrap().into_iter().map(|loss| loss.replaced_by).collect();
    assert_eq!(
        losses,
        [Replacement::NotAuthorized {
            write: forged.header.position,
            reason: RowAuthorizationFailure::NotOwner { owner: crate::tests::member(1) },
        }]
    );
    ana.close().await.unwrap();
    mallory.close().await.unwrap();
}
