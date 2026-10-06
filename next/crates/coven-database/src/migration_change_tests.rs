use crate::migration_writes::tests::{reference_migration, reference_tables, state, REFERENCES};
use crate::{
    tests::TestStore,
    write::tests::{records, sql},
    *,
};
use coven_format::value::Value as WireValue;
use coven_merge::Operation;

#[tokio::test]
async fn reference_renames_and_column_reordering_keep_original_parent_generations() {
    let store = TestStore::new();
    let db = store.schema(reference_tables(), REFERENCES).await.unwrap();
    sql(
        &db,
        "INSERT INTO parents VALUES('p'); INSERT INTO children VALUES('c','p','value')",
    )
    .await
    .unwrap();
    sql(&db, "DELETE FROM parents").await.unwrap();
    sql(&db, "INSERT INTO parents VALUES('p')").await.unwrap();
    let original = records(&db);
    db.close().await.unwrap();
    let db = store
        .builder(
            reference_tables(),
            vec![
                reference_migration(),
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
            ],
        )
        .open()
        .await
        .unwrap();
    let converted = records(&db);
    let child = converted[0].parts[0]
        .rows
        .iter()
        .find(|r| r.row.table == "children")
        .unwrap();
    let Operation::Insert(values) = &child.change.operation else {
        panic!("insert")
    };
    let parent =
        &values["root"].parents[&coven_merge::ForeignKey::new(["root"], "parents", ["id"])];
    assert_eq!(parent.generation, 1);
    assert_eq!(state(&db, &parent.row).generation(), 3);
    assert_eq!(converted[0].header.timestamp, original[0].header.timestamp);
    sql(&db, "UPDATE children SET other='after'").await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn changing_or_adding_a_reference_rolls_back_the_named_migration() {
    for case in 0..5 {
        let store = TestStore::new();
        let db = store.schema(reference_tables(), REFERENCES).await.unwrap();
        sql(
            &db,
            "INSERT INTO parents VALUES('p'),('q'); INSERT INTO children VALUES('c','p','value')",
        )
        .await
        .unwrap();
        let original = records(&db);
        db.close().await.unwrap();
        let migration = Migration::sql(
            2,
            "bad reference",
            "ALTER TABLE children ADD COLUMN added TEXT REFERENCES parents(id) ON DELETE SET NULL",
        )
        .writes(move |row| {
            if row.table == "children" {
                if case == 4 {
                    let mut added = row
                        .columns
                        .iter()
                        .find(|c| c.name == "parent")
                        .unwrap()
                        .clone();
                    added.name = "added".into();
                    row.columns.push(added);
                } else if case == 2 {
                    row.columns
                        .push(ColumnChange::new("added", None, Some(types::Value::Null)));
                } else if case == 3 {
                    row.columns.retain(|c| c.name != "parent");
                    row.columns.push(ColumnChange::new(
                        "parent",
                        None,
                        Some(types::Value::Text("p".into())),
                    ));
                } else {
                    let parent = row.columns.iter_mut().find(|c| c.name == "parent").unwrap();
                    parent.new = Some(if case == 0 {
                        types::Value::Text("q".into())
                    } else {
                        types::Value::Null
                    });
                }
            }
            Ok(())
        });
        let error = store
            .builder(reference_tables(), vec![reference_migration(), migration])
            .open()
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error,CovenError::Migration(MigrationError::Failed {version:2,name:"bad reference",source}) if matches!(*source,DbError::MigrationReference{..})),
            "case {case}"
        );
        let db = store.schema(reference_tables(), REFERENCES).await.unwrap();
        assert_eq!(records(&db), original);
        assert_eq!(db.schema_version().await.unwrap(), 1);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_null_reference_is_still_a_reference_when_converting() {
    let store = TestStore::new();
    let db = store.schema(reference_tables(), REFERENCES).await.unwrap();
    sql(
        &db,
        "INSERT INTO parents VALUES('p'); INSERT INTO children VALUES('c',NULL,'value')",
    )
    .await
    .unwrap();
    db.close().await.unwrap();
    let error = store
        .builder(
            reference_tables(),
            vec![
                reference_migration(),
                Migration::sql(2, "index", "CREATE INDEX refs ON children(parent)").writes(|row| {
                    if let Some(column) = row.columns.iter_mut().find(|c| c.name == "parent") {
                        column.new = Some(types::Value::Text("p".into()));
                    }
                    Ok(())
                }),
            ],
        )
        .open()
        .await
        .err()
        .unwrap();
    assert!(
        matches!(error,CovenError::Migration(MigrationError::Failed {source,..}) if matches!(*source,DbError::MigrationReference{..}))
    );
}

#[tokio::test]
async fn partial_composite_reference_updates_keep_renamed_unchanged_columns() {
    let schema = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,a TEXT NOT NULL,b TEXT NOT NULL,UNIQUE(a,b)); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT,FOREIGN KEY(a,b) REFERENCES parents(a,b) ON DELETE CASCADE)";
    let store = TestStore::new();
    let db = store.schema(reference_tables(), schema).await.unwrap();
    sql(
        &db,
        "INSERT INTO parents VALUES('p','a','b'),('q','a','c'); INSERT INTO children VALUES('c','a','b')",
    )
    .await
    .unwrap();
    sql(&db, "UPDATE children SET b='c'").await.unwrap();
    db.close().await.unwrap();
    let db = store
        .builder(
            reference_tables(),
            vec![
                Migration::sql(1, "initial", schema),
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
            ],
        )
        .open()
        .await
        .unwrap();
    let actual = records(&db);
    let Operation::Update(columns) = &actual[1].parts[0].rows[0].change.operation else {
        panic!("update")
    };
    assert_eq!(
        columns["b"].parents.keys().next().unwrap().columns.0,
        ["renamed", "b"]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn converting_does_not_add_a_reference_that_the_old_write_did_not_set() {
    let store = TestStore::new();
    let db = store.schema(reference_tables(), REFERENCES).await.unwrap();
    sql(
        &db,
        "INSERT INTO parents VALUES('p'); INSERT INTO children VALUES('c','p','value')",
    )
    .await
    .unwrap();
    db.close().await.unwrap();
    let db = store.builder(reference_tables(),vec![reference_migration(),Migration::sql(2,"optional reference","ALTER TABLE children ADD COLUMN added TEXT REFERENCES parents(id) ON DELETE SET NULL").writes(|_| Ok(()))]).open().await.unwrap();
    sql(&db, "UPDATE children SET other='after'").await.unwrap();
    assert!(records(&db)[0].parts[0]
        .rows
        .iter()
        .all(|r| match &r.change.operation {
            Operation::Insert(columns) | Operation::Update(columns) =>
                !columns.contains_key("added"),
            Operation::Delete => true,
        }));
    db.close().await.unwrap();
}

#[tokio::test]
async fn two_references_on_one_column_keep_distinct_target_columns() {
    let schema="CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,alternate TEXT UNIQUE); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT,FOREIGN KEY(parent) REFERENCES parents,FOREIGN KEY(parent) REFERENCES parents(alternate))";
    let fixture = TestStore::new();
    let db = fixture.schema(reference_tables(), schema).await.unwrap();
    sql(
        &db,
        "INSERT INTO parents VALUES('1','2'),('2','1'); INSERT INTO children VALUES('c','1')",
    )
    .await
    .unwrap();
    db.close().await.unwrap();
    let db = fixture
        .builder(
            reference_tables(),
            vec![
                Migration::sql(1, "references", schema),
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
            ],
        )
        .open()
        .await
        .unwrap();
    let converted = records(&db);
    let child = converted[0].parts[0]
        .rows
        .iter()
        .find(|r| r.row.table == "children")
        .unwrap();
    let Operation::Insert(columns) = &child.change.operation else {
        panic!("insert")
    };
    let parents = &columns["renamed"].parents;
    assert_eq!(parents.len(), 2);
    for (target, id) in [("id", "1"), ("alternate", "2")] {
        let key = coven_merge::ForeignKey::new(["renamed"], "parents", [target]);
        assert_eq!(
            coven_format::key::decode_key(&parents[&key].row.key).unwrap(),
            [WireValue::Text(id.into())]
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn replacing_a_reference_target_cannot_reuse_the_old_parents_generation() {
    let schema="CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,alternate TEXT UNIQUE); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id))";
    let fixture = TestStore::new();
    let db = fixture.schema(reference_tables(), schema).await.unwrap();
    sql(
        &db,
        "INSERT INTO parents VALUES('1','2'),('2','1'); INSERT INTO children VALUES('c','1')",
    )
    .await
    .unwrap();
    db.close().await.unwrap();
    let error=fixture.builder(reference_tables(),vec![Migration::sql(1,"references",schema),Migration::sql(2,"retarget","CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(alternate)); INSERT INTO rebuilt SELECT * FROM children; DROP TABLE children; ALTER TABLE rebuilt RENAME TO children").writes(|_| Ok(()))]).open().await.err().unwrap();
    assert!(
        matches!(error,CovenError::Migration(MigrationError::Failed{version:2,name:"retarget",source}) if matches!(*source,DbError::MigrationReference{..}))
    );
}
