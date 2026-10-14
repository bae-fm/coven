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
