use crate::tests::TestStore;
use crate::{Migration, RowIdentity, SyncedTable};

#[tokio::test]
async fn declaration_case_preserves_downloads_and_file_retention() {
    use crate::write::tests::{count, records, sql};
    let source_store = TestStore::new();
    let receiver_store = TestStore::new();
    let schema = "CREATE TABLE Notes(id TEXT NOT NULL PRIMARY KEY, body TEXT)";
    let source = source_store
        .schema(
            vec![SyncedTable::new("NOTES", RowIdentity::SharedKey)],
            schema,
        )
        .await
        .unwrap();
    let receiver = receiver_store
        .schema(
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            schema,
        )
        .await
        .unwrap();
    sql(&source, "INSERT INTO Notes VALUES('n','body')")
        .await
        .unwrap();
    let record = records(&source).pop().unwrap();
    assert_eq!(record.parts[0].rows[0].row.table, "Notes");
    let retained = source.retained_files().await.map(|_| ());
    let downloaded = receiver.apply_downloaded(record.into()).await;
    assert!(
        retained.is_ok() && downloaded.is_ok(),
        "retention: {retained:?}; download: {downloaded:?}"
    );
    assert_eq!(count(&receiver, "Notes"), 1);
    source.close().await.unwrap();
    receiver.close().await.unwrap();
}

#[tokio::test]
async fn unique_terms_follow_changed_constraints_when_reopening() {
    const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,body TEXT); CREATE UNIQUE INDEX claim ON notes(title)";
    let store = TestStore::new();
    let tables = || vec![SyncedTable::new("notes", RowIdentity::SharedKey)];
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    db.close().await.unwrap();
    let db = store
        .builder(
            tables(),
            vec![
                Migration::sql(1, "schema", SCHEMA),
                Migration::sql(
                    2,
                    "change unique columns",
                    "DROP INDEX claim; CREATE UNIQUE INDEX claim ON notes(body)",
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    db.inspect_writer_schema(|_, schema| {
        assert_eq!(schema.rules["notes"].unique[0].identity, ["body"].into());
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn foreign_key_column_identities_survive_sqlite_id_reordering() {
    const SCHEMA: &str="CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT,FOREIGN KEY(a) REFERENCES parents(id),FOREIGN KEY(b) REFERENCES parents(id))";
    let store = TestStore::new();
    let tables = || {
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey),
            SyncedTable::new("notes", RowIdentity::SharedKey),
        ]
    };
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    crate::write::tests::sql(
        &db,
        "INSERT INTO parents VALUES('1'),('2'); INSERT INTO notes VALUES('3','1','2')",
    )
    .await
    .unwrap();
    db.close().await.unwrap();
    let db=store.builder(tables(),vec![Migration::sql(1,"schema",SCHEMA),Migration::sql(2,"reorder references","CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT,FOREIGN KEY(b) REFERENCES parents(id),FOREIGN KEY(a) REFERENCES parents(id)); INSERT INTO rebuilt SELECT * FROM notes; DROP TABLE notes; ALTER TABLE rebuilt RENAME TO notes")]).open().await.unwrap();
    crate::write::tests::sql(&db, "UPDATE notes SET a='2' WHERE id='3'")
        .await
        .unwrap();
    let record = crate::write::tests::records(&db).pop().unwrap();
    let coven_merge::Operation::Update(columns) = &record.parts[0].rows[0].change.operation else {
        panic!("update")
    };
    assert!(columns["a"]
        .parents
        .contains_key(&coven_merge::ForeignKey::new(["a"], "parents", ["id"])));
    assert!(!columns["a"]
        .parents
        .contains_key(&coven_merge::ForeignKey::new(["b"], "parents", ["id"])));
    db.close().await.unwrap();
}

#[tokio::test]
async fn foreign_keys_on_one_column_keep_both_target_tables() {
    let store = TestStore::new();
    let db = store.schema(
        ["lefts", "rights", "children"].into_iter().map(|t| SyncedTable::new(t, RowIdentity::SharedKey)).collect(),
        "CREATE TABLE lefts(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE rights(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT,FOREIGN KEY(parent) REFERENCES lefts(id),FOREIGN KEY(parent) REFERENCES rights(id))",
    ).await.unwrap();
    crate::write::tests::sql(&db, "INSERT INTO lefts VALUES('1'); INSERT INTO rights VALUES('1'); INSERT INTO children VALUES('c','1')").await.unwrap();
    let record = crate::write::tests::records(&db).pop().unwrap();
    let child = record
        .parts
        .iter()
        .flat_map(|p| &p.rows)
        .find(|r| r.row.table == "children")
        .unwrap();
    let coven_merge::Operation::Insert(columns) = &child.change.operation else {
        panic!("insert")
    };
    let targets: std::collections::BTreeSet<_> = columns["parent"]
        .parents
        .values()
        .map(|p| p.row.table.as_str())
        .collect();
    assert_eq!(targets, ["lefts", "rights"].into());
    db.close().await.unwrap();
}

#[tokio::test]
async fn foreign_keys_on_one_column_keep_both_target_column_lists() {
    let store = TestStore::new();
    let db = store.schema(
        ["parents", "children"].into_iter().map(|t| SyncedTable::new(t, RowIdentity::SharedKey)).collect(),
        "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,alternate TEXT UNIQUE); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT,FOREIGN KEY(parent) REFERENCES parents,FOREIGN KEY(parent) REFERENCES parents(alternate))",
    ).await.unwrap();
    crate::write::tests::sql(
        &db,
        "INSERT INTO parents VALUES('1','2'),('2','1'); INSERT INTO children VALUES('c','1')",
    )
    .await
    .unwrap();
    let record = crate::write::tests::records(&db).pop().unwrap();
    let child = record
        .parts
        .iter()
        .flat_map(|p| &p.rows)
        .find(|r| r.row.table == "children")
        .unwrap();
    let coven_merge::Operation::Insert(columns) = &child.change.operation else {
        panic!("insert")
    };
    let parents = &columns["parent"].parents;
    assert_eq!(parents.len(), 2);
    for (target, id) in [("id", "1"), ("alternate", "2")] {
        let key = coven_merge::ForeignKey::new(["parent"], "parents", [target]);
        assert_eq!(
            coven_format::key::decode_key(&parents[&key].row.key).unwrap(),
            [coven_format::value::Value::Text(id.into())]
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn removing_and_restoring_two_parents_keeps_each_foreign_key_reason() {
    use crate::removal::tests::remove;
    use crate::write::tests::{count, sql};
    use coven_format::value::Value;
    use coven_merge::{ForeignKey, Rule};
    let store = TestStore::new();
    let db = store.schema(
        ["lefts", "rights", "children"].into_iter().map(|t| SyncedTable::new(t, RowIdentity::SharedKey)).collect(),
        "CREATE TABLE lefts(id TEXT NOT NULL PRIMARY KEY,enabled INT CONSTRAINT enabled CHECK(enabled=1)); CREATE TABLE rights(id TEXT NOT NULL PRIMARY KEY,enabled INT CONSTRAINT enabled CHECK(enabled=1)); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT,FOREIGN KEY(parent) REFERENCES lefts(id),FOREIGN KEY(parent) REFERENCES rights(id))",
    ).await.unwrap();
    sql(&db, "INSERT INTO lefts VALUES('1',1); INSERT INTO rights VALUES('1',1); INSERT INTO children VALUES('c','1')").await.unwrap();
    let left = Rule::ForeignKey(ForeignKey::new(["parent"], "lefts", ["id"]));
    let right = Rule::ForeignKey(ForeignKey::new(["parent"], "rights", ["id"]));
    remove(&db, "children", "c", &[], [left, right.clone()].into());
    for parent in ["lefts", "rights"] {
        remove(
            &db,
            parent,
            "1",
            &[("enabled", Value::Integer(0))],
            [Rule::Check("enabled".into())].into(),
        );
    }
    sql(&db, "INSERT INTO lefts VALUES('1',1)").await.unwrap();
    assert_eq!(count(&db, "children"), 0);
    let rules = db.inspect_writer(|db| {
        db.query_row(
            "SELECT replaced_by FROM _coven_lost WHERE table_name='children' AND column_id IS NULL",
            [],
            |r| Ok(coven_format::merge_fields::decode_rules(&r.get::<_, Vec<u8>>(0)?).unwrap()),
        )
        .unwrap()
    });
    assert_eq!(rules, [right].into());
    sql(&db, "INSERT INTO rights VALUES('1',1)").await.unwrap();
    assert_eq!(count(&db, "children"), 1);
    assert_eq!(count(&db, "_coven_lost"), 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn ordinary_sqlite_can_maintain_a_store_without_coven_functions() {
    let store = TestStore::new();
    let db = store.schema(["parents", "notes"].into_iter().map(|t| SyncedTable::new(t, RowIdentity::SharedKey)).collect(),
        "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES parents(id), title TEXT UNIQUE); CREATE INDEX notes_parent ON notes(parent)").await.unwrap();
    crate::write::tests::sql(
        &db,
        "INSERT INTO parents VALUES('p'); INSERT INTO notes VALUES('n','p','Title')",
    )
    .await
    .unwrap();
    db.close().await.unwrap();
    let raw = rusqlite::Connection::open(store.database_path()).unwrap();
    raw.execute_batch("VACUUM; REINDEX").unwrap();
    assert_eq!(
        raw.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    let created: i64 = raw.query_row("SELECT count(*) FROM sqlite_schema WHERE type='index' AND tbl_name IN ('notes','parents') AND substr(lower(name),1,7) = '_coven_'", [], |r| r.get(0)).unwrap();
    assert_eq!(created, 0);
}

#[tokio::test]
async fn column_expression_and_partial_unique_identities_keep_each_reason() {
    use crate::removal::tests::remove;
    use crate::write::tests::sql;
    use coven_format::value::Value;
    use coven_merge::{Rule, UniqueConstraint};
    use std::collections::BTreeSet;
    for partial in [false, true] {
        let store = TestStore::new();
        let db = store.schema(vec![SyncedTable::new("notes", RowIdentity::SharedKey)], if partial {
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,active INT); CREATE UNIQUE INDEX active_title ON notes(title) WHERE active=1"
        } else {
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,active INT); CREATE UNIQUE INDEX folded_title ON notes(lower(title))"
        }).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('45','Groceries',1)")
            .await
            .unwrap();
        sql(&db, "INSERT INTO notes VALUES('46','Shopping',1)")
            .await
            .unwrap();
        let column = UniqueConstraint::from(["title"]);
        let other = if partial {
            UniqueConstraint {
                terms: vec!["title".into()],
                partial: Some("active=1".into()),
            }
        } else {
            UniqueConstraint::from(["lower(title)"])
        };
        let expected: BTreeSet<_> =
            [Rule::Unique(column.clone()), Rule::Unique(other.clone())].into();
        remove(
            &db,
            "notes",
            "46",
            &[("title", Value::Text("Groceries".into()))],
            expected.clone(),
        );
        sql(&db, "UPDATE notes SET active=2 WHERE id='45'")
            .await
            .unwrap();
        let expected = if partial {
            [Rule::Unique(column)].into()
        } else {
            expected
        };
        db.inspect_writer(|db| {
            let rules = db.query_row("SELECT replaced_by FROM _coven_lost WHERE table_name='notes' AND column_id IS NULL",[],|r| Ok(coven_format::merge_fields::decode_rules(&r.get::<_,Vec<u8>>(0)?).unwrap())).unwrap();
            assert_eq!(rules,expected);
            let identities:BTreeSet<_> = db.query("SELECT c.identity FROM _coven_claims v JOIN _coven_constraints c ON c.id=v.constraint_id",[],|r| Ok(coven_format::merge_fields::decode_unique_constraint(&r.get::<_,Vec<u8>>(0)?).unwrap())).unwrap().into_iter().collect();
            assert_eq!(identities,[UniqueConstraint::from(["title"]),other].into());
        });
        sql(&db, "DELETE FROM notes WHERE id='45'").await.unwrap();
        assert_eq!(crate::write::tests::count(&db, "_coven_claims"), 0);
        assert_eq!(crate::write::tests::count(&db, "notes"), 1);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn explicit_collation_terms_do_not_merge_with_bare_column_constraints() {
    use crate::write::tests::sql;
    use coven_format::value::Value;
    use coven_merge::Rule;
    for schema in [
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,edited INT); CREATE UNIQUE INDEX folded ON notes(title COLLATE NOCASE)",
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,edited INT,UNIQUE(title COLLATE NOCASE))",
    ] {
        let store = TestStore::new();
        let db = store.schema(vec![SyncedTable::new("notes", RowIdentity::SharedKey)], schema).await.unwrap();
        sql(&db,"INSERT INTO notes VALUES('45','Groceries',0),('46','Shopping',0)").await.unwrap();
        let expected = Rule::Unique(["title COLLATE NOCASE"].into());
        crate::removal::tests::remove(&db,"notes","46",&[("title",Value::Text("GROCERIES".into()))],[expected.clone()].into());
        sql(&db,"UPDATE notes SET edited=1 WHERE id='45'").await.unwrap();
        db.inspect_writer(|db| {
            let rules = db.query_row("SELECT replaced_by FROM _coven_lost WHERE column_id IS NULL",[],|r| Ok(coven_format::merge_fields::decode_rules(&r.get::<_,Vec<u8>>(0)?).unwrap())).unwrap();
            assert_eq!(rules,[expected].into());
            assert_eq!(db.query_row("SELECT count(*) FROM _coven_claims",[],|r| r.get::<_,u32>(0)).unwrap(),2);
        });
        db.close().await.unwrap();
    }
}
