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
    let Operation::Insert(cols) = &mut row.change.operation else {
        unreachable!()
    };
    cols.get_mut("id").unwrap().value = Scalar::Text(key);
    cols.get_mut("a").unwrap().value = Scalar::Integer(1);
    cols.insert(
        "parent".into(),
        ColumnValue {
            value: Scalar::Null,
            parents: BTreeMap::new(),
        },
    );
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
            store
                .row(row)?
                .state
                .generations()
                .iter()
                .map(|(g, w)| (*g, *w))
                .collect(),
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
    let mut index = crate::Migration::sql(2, "index", "CREATE INDEX rows_a ON rows(a)");
    if converter {
        index = index.writes(|_| Ok(()));
    }
    store.builder(
        migration_tables(),
        vec![crate::Migration::sql(1, "rows", MIGRATION_SCHEMA), index],
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
            value: coven_format::key::decode_key(&row_of(&parent).key)
                .unwrap()
                .remove(0),
            parents: BTreeMap::from([(
                ForeignKey::new(["parent"], "rows", ["id"]),
                Parent {
                    row: row_of(&parent),
                    generation: 1,
                },
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
    let db = store
        .schema(migration_tables(), MIGRATION_SCHEMA)
        .await
        .unwrap();
    for write in [&parent, &child, &first, &second] {
        assert_eq!(
            db.receive(write.clone().into()).await.unwrap(),
            crate::ApplyOutcome::Applied
        );
    }
    crate::tests::delete_circle(&db, circle_id()).await.unwrap();
    assert_eq!(count(&db, "rows"), 1);
    let hidden = db.lost_values().await.unwrap();
    assert_eq!(hidden.len(), 3);
    db.close().await.unwrap();
    let db = migrated(&store, false).open().await.unwrap();
    let marker = records(&db).last().unwrap().header.position;
    for write in [&parent, &child] {
        assert_eq!(
            generations(&db, &row_of(write)),
            [(1, write.header.position), (2, marker)]
        );
    }
    assert_eq!(
        generations(&db, &row_of(&second)),
        [(1, second.header.position), (2, marker)]
    );
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
    let ana = stores[0]
        .schema(migration_tables(), MIGRATION_SCHEMA)
        .await
        .unwrap();
    let carol = stores[1]
        .schema(migration_tables(), MIGRATION_SCHEMA)
        .await
        .unwrap();
    let target = stores[2]
        .schema(migration_tables(), MIGRATION_SCHEMA)
        .await
        .unwrap();
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
        assert_eq!(
            generations(db, &row_of(&created)),
            [(1, created.header.position), (2, marker)]
        );
        let losses = db.lost_values().await.unwrap();
        assert!(losses
            .iter()
            .any(|loss| loss.replaced_by == crate::Replacement::Write(marker)
                && setters(loss) == vec![("a".into(), edit.header.position)]));
        assert!(losses
            .iter()
            .any(|loss| loss.replaced_by == crate::Replacement::SchemaChange { version: 2 }));
    }
    assert_eq!(
        target.lost_values().await.unwrap(),
        fresh.lost_values().await.unwrap()
    );
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
        ColumnValue {
            value: Scalar::Text("extra".into()),
            parents: BTreeMap::new(),
        },
    );
    write
}
