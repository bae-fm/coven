use crate::tests::TestStore;
use crate::write::tests::{records, sql};
use crate::{RowIdentity, SyncedTable};
use coven_format::{merge_fields, value::Value};
use coven_merge::{Operation, WriteId};

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
    incoming.header.had_read =
        coven_format::value::WritePositions(vec![records[0].header.position]);
    let Operation::Update(columns) = &mut incoming.parts[0].rows[0].change.operation else {
        panic!("update")
    };
    let expected = columns["a"].parents.clone();
    columns.get_mut("a").unwrap().value = Value::Text("c".into());
    for parent in columns.get_mut("a").unwrap().parents.values_mut() {
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
            db.internal_execute("UPDATE children SET a='c'",[])?;
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
