use std::collections::BTreeMap;

use crate::{tests::TestStore, Migration, RowIdentity, SyncedTable};
use coven_format::{
    merge_fields::*,
    snapshot_rows::LostWriteCause,
    value::{EntryId, Value},
};
use coven_foundation::id_source::DeviceId;
use coven_merge::{ColumnValue, WriteId};

const NOTE_ID: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

fn declarations() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::IndependentUuid)
        .key_columns(["id", "number"])
        .audience_column("audience")]
}

fn migrations() -> Vec<Migration> {
    vec![Migration::sql(1, "notes", "CREATE TABLE notes(id TEXT NOT NULL, number INTEGER NOT NULL, audience TEXT NOT NULL, title TEXT, body BLOB, PRIMARY KEY(id,number))")]
}

#[tokio::test]
async fn migrations_change_present_values_without_a_second_copy() {
    let store = TestStore::new();
    let db = store
        .builder(declarations(), migrations())
        .open()
        .await
        .unwrap();
    crate::write::tests::sql(&db, "INSERT INTO notes VALUES('f47ac10b-58cc-4372-a567-0e02b2c3d479',1,'store','before',x'00ff2a')").await.unwrap();
    let original = crate::write::tests::records(&db).remove(0);
    db.close().await.unwrap();
    let mut migrations = migrations();
    migrations.push(Migration::sql(
        2,
        "rewrite title",
        "UPDATE notes SET title='migrated'",
    ));
    let db = store
        .builder(declarations(), migrations)
        .open()
        .await
        .unwrap();
    let migration = crate::write::tests::records(&db).pop().unwrap();
    db.inspect_writer_schema(|sql, schema| {
        let visible = crate::write_rows::AppView::after(sql, schema);
        let merge = crate::merge_store::MergeStore::new(sql, &visible);
        let state = merge.row(&original.parts[0].rows[0].row).unwrap().state;
        assert_eq!(
            state.cells()["title"].value.value,
            Value::Text("migrated".into())
        );
        assert_eq!(state.cells()["title"].write, migration.header.position);
        assert_eq!(state.cells()["body"].write, original.header.position);
        assert_eq!(
            state.cells()["body"].value.value,
            Value::Blob(vec![0, 255, 42])
        );
        assert!(state.lost().is_empty());
        let cells = sql
            .query(
                "SELECT name FROM pragma_table_info('_coven_cells') ORDER BY cid",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(cells, ["column_id", "row_id", "write_id"]);
    });
    db.close().await.unwrap();
}
#[tokio::test]
async fn only_the_spec_tables_are_created() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.inspect_writer(|sql| {
        let objects = sql
            .query(
                "SELECT type,name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .unwrap();
        for (kind, name) in objects {
            assert!(name.starts_with("_coven_"), "{kind} {name}");
        }
        let tables = sql
            .query(
                "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            tables,
            [
                "_coven_access_keys_to_delete",
                "_coven_applied_boundaries",
                "_coven_cache",
                "_coven_cache_budgets",
                "_coven_cells",
                "_coven_circle_members",
                "_coven_circles",
                "_coven_claims",
                "_coven_columns",
                "_coven_constraints",
                "_coven_device_files",
                "_coven_devices",
                "_coven_file_chunks",
                "_coven_file_removals",
                "_coven_file_upload_chunks",
                "_coven_file_uploads",
                "_coven_fingerprint_leaves",
                "_coven_fingerprint_sums",
                "_coven_foreign_keys",
                "_coven_key_uploads",
                "_coven_loaded_audiences",
                "_coven_lost",
                "_coven_members",
                "_coven_operations",
                "_coven_positions",
                "_coven_reference_values",
                "_coven_references",
                "_coven_resets",
                "_coven_rows",
                "_coven_snapshot_schema",
                "_coven_store",
                "_coven_store_log",
                "_coven_store_log_key_uploads",
                "_coven_store_log_uploads",
                "_coven_uploads",
                "_coven_user_files",
                "_coven_versions",
                "_coven_write_upload_sessions",
                "_coven_writes",
                "sqlite_sequence"
            ]
        );
        let cells = sql
            .query(
                "SELECT name FROM pragma_table_info('_coven_cells') ORDER BY cid",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(cells, ["column_id", "row_id", "write_id"]);
        let fields = sql
            .query(
                "SELECT name FROM pragma_table_info('_coven_operations') ORDER BY cid",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            fields,
            ["id", "kind", "last_step", "data", "started_by", "failure"]
        );
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_lost_row_does_not_require_an_accepted_generation() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let key = coven_format::key::encode_key(&[Value::Text(NOTE_ID.into())]).unwrap();
    let write = WriteId {
        device: DeviceId(u64::MAX),
        number: u64::MAX,
    };
    let values = BTreeMap::from([(
        "title".into(),
        ColumnValue {
            value: Value::Text("lost".into()),
            parents: BTreeMap::new(),
        },
    )]);
    let setters = BTreeMap::from([("title".into(), write)]);
    let entry = EntryId {
        device: DeviceId(u64::MAX),
        number: u64::MAX,
    };
    for cause in [
        LostWriteCause::SchemaChange(u32::MAX),
        LostWriteCause::Reset(entry),
    ] {
        db.inspect_writer(|sql| {
            sql.internal_execute("INSERT INTO _coven_lost(table_name,key,audience,generation,value,set_by,replacement_kind,replaced_by,retired) VALUES (?1,?2,?3,?4,?5,?6,'excluded',?7,1)",
                ("notes", &key, "store", 0u64.to_be_bytes().to_vec(), encode_columns(&values).unwrap(), encode_setters(&setters).unwrap(), encode_exclusion(write, cause).unwrap())).unwrap();
            let stored = sql.query_row("SELECT replaced_by FROM _coven_lost WHERE id=last_insert_rowid()", [], |r| r.get::<_, Vec<u8>>(0)).unwrap();
            assert_eq!(decode_exclusion(&stored).unwrap().1, cause);
            assert!(sql.internal_execute("UPDATE _coven_lost SET retired=0 WHERE id=last_insert_rowid()", []).is_err());
            assert_eq!(sql.query_row("SELECT count(*) FROM _coven_rows", [], |r| r.get::<_, i64>(0)).unwrap(), 0);
        });
    }
    db.close().await.unwrap();
}
