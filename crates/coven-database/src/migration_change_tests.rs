//! A received late old-schema change converts through the winning raise's
//! callback (§17.1, E13). Upload records never change; each case authors
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
        crate::merge_store::MergeStore::new(db, &app)
            .row(row)
            .unwrap()
            .state
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
            .builder(
                tables(),
                vec![Migration::sql(1, "initial", schema), migration],
            )
            .open()
            .await
            .unwrap();
        receiver.exclude_later_writes(2).await;
        Self {
            _stores: stores,
            source,
            receiver,
            writes,
        }
    }

    /// Receive every write, stopping at the first failure.
    async fn receive(&self) -> Result<(), DbError> {
        for write in &self.writes {
            assert_eq!(
                self.receiver.receive(write.clone().into()).await?,
                ApplyOutcome::Applied
            );
        }
        Ok(())
    }

    /// A failed conversion commits nothing: no position passes and the
    /// original upload records stay as authored.
    async fn assert_unchanged(&self, before: &[WriteRecord]) {
        let positions = self
            .receiver
            .sync_state(Vec::new())
            .await
            .unwrap()
            .positions;
        assert!(self
            .writes
            .iter()
            .all(|w| !positions.covers(w.header.position)));
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
        Migration::sql(
            2,
            "parent name",
            "ALTER TABLE children RENAME COLUMN parent TO root",
        )
        .writes(|row| {
            if row.table == "children" {
                row.rename_column("parent", "root");
                row.columns.reverse();
            }
            Ok(())
        }),
    )
    .await;
    let original = records(&case.source);
    // The converted insert keeps the parent generation its author named.
    case.receiver
        .receive(case.writes[0].clone().into())
        .await
        .unwrap();
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
    sql(&case.receiver, "UPDATE children SET other='after'")
        .await
        .unwrap();
    case.close().await;
}

#[tokio::test]
async fn changing_or_adding_a_reference_fails_the_receipt() {
    for variant in 0..5 {
        let migration = Migration::sql(
            2,
            "bad reference",
            "ALTER TABLE children ADD COLUMN added TEXT REFERENCES parents(id) ON DELETE SET NULL",
        )
        .writes(move |row| {
            if row.table == "children" {
                if variant == 4 {
                    let mut added = row
                        .columns
                        .iter()
                        .find(|c| c.name == "parent")
                        .unwrap()
                        .clone();
                    added.name = "added".into();
                    row.columns.push(added);
                } else if variant == 2 {
                    row.columns
                        .push(ColumnChange::new("added", None, Some(types::Value::Null)));
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
        });
        let case = Case::new(
            reference_tables,
            REFERENCES,
            &["INSERT INTO parents VALUES('p'),('q'); INSERT INTO children VALUES('c','p','value')"],
            migration,
        )
        .await;
        let original = records(&case.source);
        assert!(
            matches!(
                case.receive().await,
                Err(DbError::MigrationReference { .. })
            ),
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
        Migration::sql(2, "index", "CREATE INDEX refs ON children(parent)").writes(|row| {
            if let Some(column) = row.columns.iter_mut().find(|c| c.name == "parent") {
                column.new = Some(types::Value::Text("p".into()));
            }
            Ok(())
        }),
    )
    .await;
    let original = records(&case.source);
    assert!(matches!(
        case.receive().await,
        Err(DbError::MigrationReference { .. })
    ));
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
        Migration::sql(
            2,
            "rename composite",
            "ALTER TABLE children RENAME COLUMN a TO renamed",
        )
        .writes(|row| {
            if row.table == "children" {
                row.rename_column("a", "intermediate");
                row.rename_column("intermediate", "renamed");
            }
            Ok(())
        }),
    )
    .await;
    case.receive().await.unwrap();
    let child = state(&case.receiver, &row(&case.writes, "children"));
    let cells = child.cells();
    assert_eq!(
        cells["b"].value.parents.keys().next().unwrap().columns.0,
        ["renamed", "b"]
    );
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
        Migration::sql(
            2,
            "optional reference",
            "ALTER TABLE children ADD COLUMN added TEXT REFERENCES parents(id) ON DELETE SET NULL",
        )
        .writes(|_| Ok(())),
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
        Migration::sql(
            2,
            "rename",
            "ALTER TABLE children RENAME COLUMN parent TO renamed",
        )
        .writes(|row| {
            if row.table == "children" {
                row.rename_column("parent", "renamed");
            }
            Ok(())
        }),
    )
    .await;
    case.receive().await.unwrap();
    let child = state(&case.receiver, &row(&case.writes, "children"));
    let parents = &child.cells()["renamed"].value.parents;
    assert_eq!(parents.len(), 2);
    for (target, values) in [
        (["id", "alternate"], ["1", "2"]),
        (["alternate", "id"], ["2", "1"]),
    ] {
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
        Migration::sql(2,"retarget","CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES others(id)); INSERT INTO rebuilt SELECT * FROM children; DROP TABLE children; ALTER TABLE rebuilt RENAME TO children").writes(|_| Ok(())),
    )
    .await;
    let original = records(&case.source);
    assert!(matches!(
        case.receive().await,
        Err(DbError::MigrationReference { .. })
    ));
    case.assert_unchanged(&original).await;
    case.close().await;
}

#[tokio::test]
async fn invalid_conversions_report_typed_causes_and_commit_nothing() {
    const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT)";
    for variant in 0..9 {
        let declarations = || vec![SyncedTable::new("notes", RowIdentity::SharedKey)];
        let migration = Migration::sql(
            2,
            "conversion",
            "CREATE TABLE no_key(id TEXT,title TEXT); CREATE TABLE other(id TEXT NOT NULL PRIMARY KEY,title TEXT); CREATE INDEX titles ON notes(title)",
        )
        .writes(move |row| {
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
        });
        let case = Case::new(
            declarations,
            SCHEMA,
            &["INSERT INTO notes VALUES('7','before')"],
            migration,
        )
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
    //! Ben's late old-schema writes 9 and 10 after a raise (§17.1): the data
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
        Migration::sql(1, "notes", NOTES)
    }
    fn rename(
        version: u32,
        from: &'static str,
        to: &'static str,
        calls: Option<Arc<AtomicUsize>>,
    ) -> Migration {
        let sql: &'static str =
            Box::leak(format!("ALTER TABLE notes RENAME COLUMN {from} TO {to}").into_boxed_str());
        let migration = Migration::sql(version, "rename", sql);
        match calls {
            Some(calls) => migration.writes(move |row| {
                calls.fetch_add(1, Ordering::SeqCst);
                row.rename_column(from, to);
                Ok(())
            }),
            None => migration,
        }
    }
    async fn open(store: &TestStore, migrations: Vec<Migration>) -> Database {
        store.builder(tables(), migrations).open().await.unwrap()
    }
    async fn names(db: &Database, column: &'static str) -> Vec<(String, String)> {
        db.read(move |sql| {
            Ok(sql.query(
                &format!("SELECT id,{column} FROM notes ORDER BY id"),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?)
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

    /// Ben's 9 creates a note and is excluded before any converter exists;
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
            sql(
                &ben,
                "INSERT INTO notes VALUES('p','playlist','b'),('n','covered note','b')",
            )
            .await
            .unwrap();
            if setter_only {
                // 9 replaces the covered note's title: its generation survives.
                sql(&ben, "UPDATE notes SET title='excluded title' WHERE id='n'")
                    .await
                    .unwrap();
            } else {
                sql(&ben, "DELETE FROM notes WHERE id='n'").await.unwrap();
                sql(&ben, "INSERT INTO notes VALUES('n','new note','b')")
                    .await
                    .unwrap();
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
            let receiver = open(
                &receiver_store,
                vec![initial(), rename(2, "title", "name", None)],
            )
            .await;
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
            assert_eq!(
                receiver.receive(ten.clone().into()).await.unwrap(),
                ApplyOutcome::Applied
            );
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

    /// A late write converts through every raise between its version and the
    /// winning one, in version order; a covered write never reaches a callback.
    #[tokio::test]
    async fn repeated_raises_convert_in_version_order_and_skip_covered_writes() {
        let ids = SequentialIds::new();
        let (ben_store, receiver_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
        let ben = ben_store.schema(tables(), NOTES).await.unwrap();
        sql(&ben, "INSERT INTO notes VALUES('n','covered','b')")
            .await
            .unwrap();
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
        assert_eq!(
            names(&receiver, "name").await,
            [("n".into(), "late".into())]
        );
        receiver.receive(writes[2].clone().into()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert!(names(&receiver, "name").await.is_empty());
        assert!(receiver.lost_values().await.unwrap().is_empty());
        receiver.close().await.unwrap();
        ben.close().await.unwrap();
    }
}

/// A step's classification comes from its statements, never this device's
/// rows: a receiver that ran a data step on an empty table, as a fresh
/// device does, converts a late old-schema write exactly as one with rows
/// does (§17.1).
#[tokio::test]
async fn a_data_step_run_on_an_empty_table_still_converts_late_writes() {
    use crate::write::tests::{notes, NOTES};
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = calls.clone();
    let slug = "ALTER TABLE notes ADD COLUMN slug TEXT; UPDATE notes SET slug=lower(title)";
    let migration = Migration::sql(2, "slug", slug).writes(move |_| {
        counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    });
    let insert = "INSERT INTO notes(id,title,body) VALUES('n','Title','b')";
    let case = Case::new(notes, NOTES, &[insert], migration).await;
    assert_eq!(
        case.receiver.applied_migrations().unwrap()[1].change,
        MigrationChange::Breaking
    );
    case.receive().await.unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(case.receiver.lost_values().await.unwrap().is_empty());
    case.close().await;
}
