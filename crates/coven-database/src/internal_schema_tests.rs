use crate::{tests::TestStore, Migration, RowIdentity, SyncedTable};
use coven_format::value::Value;

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
                "_coven_access_keys",
                "_coven_authored_changes",
                "_coven_authored_parts",
                "_coven_boundaries",
                "_coven_cache",
                "_coven_cache_budgets",
                "_coven_catch_up",
                "_coven_cells",
                "_coven_circle_deletions",
                "_coven_circle_members",
                "_coven_circles",
                "_coven_claims",
                "_coven_column_origins",
                "_coven_columns",
                "_coven_constraints",
                "_coven_conversion_schemas",
                "_coven_data_inputs",
                "_coven_data_losses",
                "_coven_data_origins",
                "_coven_data_reloads",
                "_coven_data_rows",
                "_coven_device_files",
                "_coven_devices",
                "_coven_file_chunks",
                "_coven_file_removals",
                "_coven_file_upload_chunks",
                "_coven_file_uploads",
                "_coven_finality",
                "_coven_fingerprint_leaves",
                "_coven_fingerprint_sums",
                "_coven_foreign_keys",
                "_coven_input_file_refs",
                "_coven_key_publications",
                "_coven_key_uploads",
                "_coven_loaded_audiences",
                "_coven_local_effect_causes",
                "_coven_local_effect_rows",
                "_coven_local_effects",
                "_coven_log_downloads",
                "_coven_log_inputs",
                "_coven_lost",
                "_coven_members",
                "_coven_operations",
                "_coven_peer_positions",
                "_coven_pending",
                "_coven_pending_resets",
                "_coven_positions",
                "_coven_positions_post",
                "_coven_queued_file_refs",
                "_coven_reference_values",
                "_coven_references",
                "_coven_row_file_refs",
                "_coven_rows",
                "_coven_snapshot_schema",
                "_coven_store_log",
                "_coven_store_log_uploads",
                "_coven_table_origins",
                "_coven_uploads",
                "_coven_user_files",
                "_coven_write_headers",
                "_coven_write_input_frames",
                "_coven_write_inputs",
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
            ["id", "kind", "last_step", "data", "started_by"]
        );
    });
    db.close().await.unwrap();
}

#[path = "internal_query_plan_tests.rs"]
mod query_plans;
