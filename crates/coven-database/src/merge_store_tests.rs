use crate::tests::TestStore;
use crate::write::tests::{records, sql};
use crate::{RowIdentity, SyncedTable};
use coven_format::{merge_fields, value::Value};
use coven_merge::{Operation, WriteId};
use rusqlite::params;

#[tokio::test]
async fn a_lost_composite_key_cell_keeps_the_parent_its_own_write_named() {
    use coven_foundation::id_source::DeviceId;
    use coven_merge::{Audience, RowId, Timestamp};
    let fixture = TestStore::new();
    let db = fixture.schema(vec![
        SyncedTable::new("parents", RowIdentity::SharedKey).key_columns(["a","b"]),
        SyncedTable::new("children", RowIdentity::SharedKey),
    ], "CREATE TABLE parents(a TEXT NOT NULL,b TEXT NOT NULL,PRIMARY KEY(a,b)); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT,FOREIGN KEY(a,b) REFERENCES parents(a,b))").await.unwrap();
    sql(&db,"INSERT INTO parents VALUES('0','0'),('a','0'),('a','b'),('c','0'),('c','b'); INSERT INTO children VALUES('child','0','0')").await.unwrap();
    sql(&db, "UPDATE children SET a='a'").await.unwrap();
    sql(&db, "UPDATE children SET b='b'").await.unwrap();
    let records = records(&db);
    let mut incoming = records[1].clone();
    let device = DeviceId(incoming.header.position.device.0.wrapping_add(1));
    incoming.header.position = WriteId { device, number: 1 };
    incoming.header.timestamp = Timestamp::next(
        Some(records[2].header.timestamp),
        records[2].header.timestamp.milliseconds(),
        device,
    )
    .unwrap();
    crate::tests::publish_through(&db, records[0].header.position.number).await.unwrap();
    incoming.header.read_through = 1;
    let Operation::Update(columns) = &mut incoming.parts[0].rows[0].change.operation else {
        panic!("update")
    };
    let Operation::Update(previous) = &records[2].parts[0].rows[0].change.operation else {
        panic!("update")
    };
    let expected = previous["a"].parents.clone();
    columns.get_mut("a").unwrap().value = Value::Text("c".into());
    for parent in columns.values_mut().flat_map(|value| value.parents.values_mut()) {
        parent.row.key =
            coven_format::key::encode_key(&[Value::Text("c".into()), Value::Text("0".into())])
                .unwrap();
    }
    db.inspect_writer_schema(|db,schema| {
        db.transaction(|db| {
            let app = crate::write_rows::AppView::after(db,schema);
            let store = super::MergeStore::new(db,&app);
            let updates = store.apply(&incoming)?;
            crate::write_commit::commit(db,&incoming,&store,&updates)?;
            db.internal_execute("UPDATE children SET a='c',b='0'",[])?;
            Ok(())
        }).unwrap();
        let value = db.query_row("SELECT value FROM _coven_lost WHERE table_name='children' AND column_id=(SELECT id FROM _coven_columns WHERE table_name='children' AND column_name='a')",[],|r| Ok(merge_fields::decode_column_value(&r.get::<_,Vec<u8>>(0)?).unwrap())).unwrap();
        assert_eq!(value.value,Value::Text("a".into()));
        assert_eq!(value.parents,expected);
        let parent = RowId { table:"parents".into(),key:coven_format::key::encode_key(&[Value::Text("c".into()),Value::Text("0".into())]).unwrap(),audience:Audience::Store };
        let children = crate::row_queries::children(db,schema.table("children"),&coven_merge::ForeignKey::new(["a","b"],"parents",["a","b"]),&parent)?;
        assert_eq!(children.len(),1);
        Ok::<_,crate::DbError>(())
    }).unwrap();
    db.close().await.unwrap();
}

/// Finding the newest write a write had read walks the writes from the
/// newest down, and sorts none, so a restore applying its 5,000th slot reads
/// one write rather than 5,000 (§21.2).
#[tokio::test]
async fn the_newest_write_read_is_found_from_the_newest_down() {
    let store = TestStore::new();
    let db = store
        .builder(crate::test_utils::notes_tables(), crate::test_utils::notes_migrations())
        .open()
        .await
        .unwrap();
    let plan = db.inspect_writer(|writer| {
        writer
            .query(
                &format!("EXPLAIN QUERY PLAN {}", super::newest_read()),
                params![[0u8; 8].as_slice(), [0u8; 8].as_slice(), [0u8; 8].as_slice()],
                |r| r.get::<_, String>(3),
            )
            .unwrap()
    });
    assert_eq!(plan, ["SCAN _coven_writes USING INDEX sqlite_autoindex__coven_writes_1"]);
    db.close().await.unwrap();
}

/// Reading the merge rows of many keys at once keeps what reading each key
/// would: every audience a key is held in, and an empty set for a key held
/// by none.
#[tokio::test]
async fn keys_read_together_match_keys_read_one_by_one() {
    use coven_merge::Audience;
    const ANA: &str = "00000000-0000-4000-8000-00000000000a";
    let store = TestStore::new();
    let db = store
        .schema(
            vec![
                SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")
            ],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,title TEXT)",
        )
        .await
        .unwrap();
    store.join_circle(
        &db,
        coven_foundation::id_source::CircleId(uuid::Uuid::parse_str(ANA).unwrap()),
    );
    db.write(|context| {
        context.execute(
            "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001',?1,'Circle')",
            [ANA],
        )?;
        context.execute(
            "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000002',?1,'Circle')",
            [ANA],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    let mut other = records(&db)[0].parts[0].rows[0].clone();
    other.row.audience = Audience::Store;
    let Operation::Insert(columns) = &mut other.change.operation else { panic!("insert") };
    columns.get_mut("audience").unwrap().value = Value::Text("store".into());
    crate::tests::remote_write(&db, other).await;
    db.inspect_writer_schema(|db, schema| {
        let mut keys: Vec<(String, Vec<u8>)> =
            db.query("SELECT DISTINCT table_name,key FROM _coven_rows", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?;
        keys.push(("notes".into(), b"absent".to_vec()));
        let app = crate::write_rows::AppView::after(db, schema);
        let together = super::MergeStore::new(db, &app);
        together.load_key_rows(keys.iter().map(|(t, k)| (t.as_str(), k.as_slice())))?;
        let apart = super::MergeStore::new(db, &app);
        let mut two = false;
        for (table, key) in &keys {
            let rows = together.key_rows(table, key)?;
            two |= rows.len() == 2;
            assert_eq!(rows, apart.key_rows(table, key)?);
        }
        assert!(two, "a key is held by two audiences");
        Ok::<_, crate::DbError>(())
    })
    .unwrap();
    db.close().await.unwrap();
}

/// A write's keys are looked up one by one in the key index, however many
/// keys the statement holds and however many rows the store has: the
/// planner left to order the join scans every merge row instead.
#[tokio::test]
async fn reading_a_writes_keys_searches_the_key_index_and_scans_no_merge_rows() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT)",
        )
        .await
        .unwrap();
    db.write(|context| {
        for n in 0..600 {
            context.execute("INSERT INTO notes VALUES(?1,'t')", [format!("note-{n}")])?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let plan: Vec<String> = db
        .inspect_writer_schema(|db, _| {
            db.query(
                &format!("EXPLAIN QUERY PLAN {}", super::key_rows_sql(super::KEY_CHUNK)),
                rusqlite::params_from_iter(vec![
                    rusqlite::types::Value::Null;
                    2 * super::KEY_CHUNK
                ]),
                |r| r.get::<_, String>(3),
            )
        })
        .unwrap();
    assert!(plan.iter().any(|step| step.starts_with("SEARCH r USING COVERING INDEX")), "{plan:?}");
    assert!(!plan.iter().any(|step| step.starts_with("SCAN r")), "{plan:?}");
    db.close().await.unwrap();
}

/// The scans this module's statements may make, with their reasons, for the
/// plan audit (`internal_schema_scan_reasons.rs`): `template` is the statement as
/// written, `sql` as run.
pub(crate) fn scan_reason(
    file: &str,
    template: &str,
    _sql: &str,
    relation: &str,
) -> Option<&'static str> {
    match (file, template, relation) {
        (
            "merge_store.rs",
            "WITH k(t,k) AS (VALUES {values}) SELECT DISTINCT r.table_name,r.key,r.audience FROM k CROSS JOIN _coven_rows r ON r.table_name=k.t AND r.key=k.k",
            "2" | "k",
        ) => Some("a write's keys are the statement's own values"),
        _ => None,
    }
}
