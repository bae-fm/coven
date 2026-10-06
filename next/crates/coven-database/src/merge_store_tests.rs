use crate::tests::TestStore;
use crate::write::tests::{records, sql};
use crate::{RowIdentity, SyncedTable};
use coven_format::{merge_fields, value::Value};
use coven_merge::{Operation, WriteId};

#[tokio::test]
async fn retained_lost_references_also_read_null_after_parent_deletion() {
    let store = TestStore::new();
    let db = store.schema(["notes", "links"].into_iter().map(|name| SyncedTable::new(name, RowIdentity::SharedKey)).collect(), "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE SET NULL)").await.unwrap();
    sql(
        &db,
        "INSERT INTO notes VALUES('43'),('44'); INSERT INTO links VALUES('6','43')",
    )
    .await
    .unwrap();
    let first = records(&db)[0].clone();
    let reference = match &first.parts[0]
        .rows
        .iter()
        .find(|row| row.row.table == "links")
        .unwrap()
        .change
        .operation
    {
        Operation::Insert(columns) => columns["note_id"].clone(),
        _ => unreachable!(),
    };
    sql(&db, "UPDATE links SET note_id='44'").await.unwrap();
    let second = records(&db)[1].header.position;
    let concurrent = WriteId {
        device: coven_foundation::id_source::DeviceId(first.header.position.device.0 + 1),
        number: 1,
    };
    db.inspect_writer(|db| {
        let stamp = coven_merge::Timestamp::new(first.header.timestamp.milliseconds(), first.header.timestamp.counter(), concurrent.device).unwrap();
        db.internal_execute("INSERT INTO coven_writes(timestamp,number,had_read) VALUES(?1,?2,?3)", crate::params![merge_fields::encode_timestamp(&stamp).unwrap(), concurrent.number.to_be_bytes().as_slice(), merge_fields::encode_write_positions(&coven_format::value::WritePositions(vec![first.header.position])).unwrap()]).unwrap();
        db.internal_execute("INSERT INTO coven_positions(device,number) VALUES(?1,?2)",crate::params![concurrent.device.0.to_be_bytes().as_slice(),concurrent.number.to_be_bytes().as_slice()]).unwrap();
        db.internal_execute("INSERT INTO coven_lost(table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by) VALUES('links',?1,'store',?2,(SELECT id FROM coven_columns WHERE table_name='links' AND column_name='note_id'),?3,?4,'write',?5)", crate::params![coven_format::key::encode_key(&[Value::Text("6".into())]).unwrap(), 1u64.to_be_bytes().as_slice(), merge_fields::encode_column_value(&reference).unwrap(), merge_fields::encode_write_id(&concurrent).unwrap(), merge_fields::encode_write_id(&second).unwrap()]).unwrap();
    });
    sql(&db, "DELETE FROM notes WHERE id='43'").await.unwrap();
    let retained = db.inspect_writer(|db| {
        db.query_row("SELECT value,set_by FROM coven_lost", [], |r| {
            Ok((
                merge_fields::decode_column_value(&r.get::<_, Vec<u8>>(0)?).unwrap(),
                merge_fields::decode_write_id(&r.get::<_, Vec<u8>>(1)?).unwrap(),
            ))
        })
        .unwrap()
    });
    // Lost cells retain the written reference; its reading follows the parent's
    // generation without a reverse lookup or rewrite of every historical cell.
    assert_eq!(retained.0.value, reference.value);
    let parent = retained.0.parents.values().next().unwrap();
    let generation =
        db.inspect_writer(|db| crate::write_record::generation(db, &parent.row).unwrap());
    let reading = coven_merge::resolve_reference(
        &first.parts[0]
            .rows
            .iter()
            .find(|r| r.row.table == "links")
            .unwrap()
            .row,
        &coven_merge::Reference {
            parent: parent.clone(),
            on_delete: coven_merge::OnDelete::SetNull { permitted: true },
        },
        generation,
        None,
    )
    .unwrap();
    assert_eq!(reading, coven_merge::ReferenceValue::Null);
    assert_eq!(retained.0.parents, reference.parents);
    assert_eq!(retained.1, concurrent);
    db.close().await.unwrap();
}

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
        let value = db.query_row("SELECT value FROM coven_lost WHERE table_name='children' AND column_id=(SELECT id FROM coven_columns WHERE table_name='children' AND column_name='a')",[],|r| Ok(merge_fields::decode_column_value(&r.get::<_,Vec<u8>>(0)?).unwrap())).unwrap();
        assert_eq!(value.value,Value::Text("a".into()));
        assert_eq!(value.parents,expected);
        let parent = RowId { table:"parents".into(),key:coven_format::key::encode_key(&[Value::Text("c".into()),Value::Text("0".into())]).unwrap(),audience:Audience::Store };
        let children = crate::row_queries::children(db,schema.table("children"),&coven_merge::ForeignKey::new(["a","b"],"parents",["a","b"]),&parent)?;
        assert_eq!(children.len(),1);
        Ok::<_,crate::DbError>(())
    }).unwrap();
    db.close().await.unwrap();
}
