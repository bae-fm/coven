use crate::{tests::TestStore, Migration, RowIdentity, SyncedTable};
use coven_format::value::Value;

fn declarations() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::IndependentUuid)
        .key_columns(["id", "number"])
        .audience_column("audience")]
}

fn migrations() -> Vec<Migration> {
    vec![Migration::addition(1, "notes", "CREATE TABLE notes(id TEXT NOT NULL, number INTEGER NOT NULL, audience TEXT NOT NULL, title TEXT, body BLOB, PRIMARY KEY(id,number))")]
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
    migrations.push(Migration::breaking(
        2,
        "rewrite title",
        "UPDATE notes SET title='migrated'",
        crate::OldWrites::Drop,
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
        let mut views = Vec::new();
        for (kind, name) in objects {
            if name.starts_with("coven_") {
                views.push((kind, name));
            } else {
                assert!(name.starts_with("_coven_"), "{kind} {name}");
            }
        }
        // What the app's SQL reads (Appendix C.1): the views, and the pins
        // coven keeps as a table, which the app's triggers may follow.
        assert_eq!(
            views,
            [
                ("table".into(), "coven_pins".into()),
                ("table".into(), "coven_upload_queue".into()),
                ("view".into(), "coven_abandoned".into()),
                ("view".into(), "coven_circle_members".into()),
                ("view".into(), "coven_circles".into()),
                ("view".into(), "coven_devices".into()),
                ("view".into(), "coven_downloading".into()),
                ("view".into(), "coven_members".into()),
                ("view".into(), "coven_pending".into()),
                ("view".into(), "coven_sending".into()),
                ("view".into(), "coven_upload_pause".into()),
                ("view".into(), "coven_uploads".into()),
            ]
        );
        let tables = sql
            .query(
                "SELECT name FROM sqlite_schema WHERE type='table'
                 AND name NOT IN('coven_pins','coven_upload_queue') ORDER BY name",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            tables,
            [
                "_coven_abandoned_attachments",
                "_coven_absent_snapshots",
                "_coven_authored_changes",
                "_coven_authored_parts",
                "_coven_bound_keys",
                "_coven_boundaries",
                "_coven_cache",
                "_coven_cache_budgets",
                "_coven_cache_writes",
                "_coven_cells",
                "_coven_circle_deletions",
                "_coven_circle_members",
                "_coven_circles",
                "_coven_claim_attempts",
                "_coven_claim_chunks",
                "_coven_claim_counters",
                "_coven_claim_journal",
                "_coven_claims",
                "_coven_column_origins",
                "_coven_columns",
                "_coven_constraints",
                "_coven_conversion_schemas",
                "_coven_creations",
                "_coven_cutoffs",
                "_coven_data_reloads",
                "_coven_device_files",
                "_coven_device_reset",
                "_coven_devices",
                "_coven_file_candidates",
                "_coven_file_chunks",
                "_coven_file_removals",
                "_coven_file_scans",
                "_coven_file_uploads",
                "_coven_foreign_keys",
                "_coven_last_claims",
                "_coven_lost",
                "_coven_members",
                "_coven_migration_losses",
        "_coven_migration_prefix",
                "_coven_operations",
                "_coven_owner_tables",
                "_coven_part_leaves",
                "_coven_part_proofs",
                "_coven_pending",
                "_coven_pending_attachments",
                "_coven_pin_requests",
                "_coven_provider_access",
                "_coven_published",
                "_coven_published_creations",
                "_coven_published_file_refs",
                "_coven_published_losses",
                "_coven_published_rows",
                "_coven_queued_file_refs",
                "_coven_raise_captures",
                "_coven_references",
                "_coven_retries",
                "_coven_row_file_refs",
                "_coven_row_files",
                "_coven_rows",
                "_coven_sequence",
                "_coven_slot_records",
                "_coven_snapshot_reservation",
                "_coven_snapshot_schema",
                "_coven_store",
                "_coven_table_origins",
                "_coven_upload_pause",
                "_coven_uploads",
                "_coven_user_files",
                "_coven_write_evidence",
                "_coven_write_headers",
                "_coven_write_pause",
                "_coven_write_refusals",
                "_coven_write_slots",
                "_coven_writes",
                "_coven_written_values",
                "sqlite_sequence",
                // The statistics opening gathers (§4).
                "sqlite_stat1",
                "sqlite_stat4",
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

// EXPLAIN QUERY PLAN checks every internal statement against a populated
// schema, allowing only the scans these reasons name; its tests are theirs.
#[path = "internal_schema_scan_reasons.rs"]
mod scans;
