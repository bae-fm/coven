use crate::{tests::TestStore, Migration, RowIdentity, SyncedTable};
use coven_format::{
    merge_fields::*,
    snapshot_rows::LostWriteCause,
    value::{EntryId, Value},
};
use coven_foundation::id_source::DeviceId;
use coven_merge::{ColumnValue, WriteId};
use std::collections::BTreeMap;

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
                "_coven_access_keys",
                "_coven_boundaries",
                "_coven_cache",
                "_coven_cache_budgets",
                "_coven_catch_up",
                "_coven_cells",
                "_coven_circle_deletions",
                "_coven_circle_members",
                "_coven_circles",
                "_coven_claims",
                "_coven_columns",
                "_coven_constraints",
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
                "_coven_pending",
                "_coven_pending_resets",
                "_coven_positions",
                "_coven_queued_file_refs",
                "_coven_reference_values",
                "_coven_references",
                "_coven_row_file_refs",
                "_coven_rows",
                "_coven_snapshot_schema",
                "_coven_store_log",
                "_coven_store_log_uploads",
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

mod query_plans {
    use rusqlite::Connection;
    use std::collections::BTreeSet;
    mod fixture {
        use rusqlite::{
            fallible_iterator::FallibleIterator, functions::FunctionFlags, Batch, Connection,
        };

        /// Thousands of rows, shared devices/audiences, multiple cells per row and
        /// nullable/partial-index cases; singleton tables retain their real constraints.
        pub(super) fn populated() -> Connection {
            let db = Connection::open_in_memory().unwrap();
            let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
            db.create_scalar_function("counter", 1, flags, |ctx| {
                Ok(ctx.get::<i64>(0)?.to_be_bytes().to_vec())
            })
            .unwrap();
            // These expressions only need to prepare, never execute, in EXPLAIN.
            db.create_scalar_function("_coven_loss_order", 2, flags, |_| Ok(Vec::<u8>::new()))
                .unwrap();
            db.create_scalar_function("_coven_fingerprint_replace", 3, flags, |_| {
                Ok(vec![0u8; 32])
            })
            .unwrap();
            db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA automatic_index=OFF;")
                .unwrap();
            let schema = crate::internal_schema::initial_schema();
            let mut migration = Batch::new(&db, &schema);
            while let Some(mut statement) = migration.next().unwrap() {
                // Initial schema DDL may scan: it runs once, before the store has rows.
                let sql = statement.expanded_sql().unwrap();
                let mut explain = db.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
                let mut plans = explain.raw_query();
                while plans.next().unwrap().is_some() {}
                statement.execute([]).unwrap();
            }
            db.execute_batch(&super::source::staging_schema()).unwrap();
            db.execute_batch(
        "BEGIN;
         CREATE TEMP TABLE fixture AS
         WITH RECURSIVE numbers(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM numbers WHERE i<4096)
         SELECT i, counter(i) AS n, counter((i-1)%64+1) AS device,
                'table_' || ((i-1)%32) AS tab, 'audience_' || ((i-1)%64) AS audience,
                CAST(printf('%032d',i) AS BLOB) AS key FROM numbers;
         INSERT INTO _coven_operations(id,kind,last_step,data,started_by)
           SELECT i,'kind_' || (i%32),1,x'00','app' FROM fixture;
         INSERT INTO _coven_pending(subject,reporter,value,operation,object,coven_version)
           SELECT key,device,x'00',CASE WHEN i%2=0 THEN i END,
                  CAST(x'00' || device || n AS BLOB),'1' FROM fixture;
         INSERT INTO _coven_store_log(device,number,record,timestamp,author_view,stored_at,outcome)
           SELECT device,n,zeroblob(128),CAST(n || device AS BLOB),x'00',zeroblob(13),'kept' FROM fixture;
         INSERT INTO _coven_store_log_uploads VALUES(counter(1),counter(1),zeroblob(128),CAST(counter(1) || counter(1) AS BLOB),zeroblob(16));
         INSERT INTO _coven_key_publications SELECT n,n FROM fixture;
         INSERT INTO _coven_log_inputs
           SELECT CASE WHEN i%2=0 THEN 'entry' ELSE 'key' END,device,n,x'00',zeroblob(64) FROM fixture;
         INSERT INTO _coven_log_downloads
           SELECT CASE WHEN i%2=0 THEN 'entry' ELSE 'key' END,n,n FROM fixture;
         INSERT INTO _coven_key_uploads
           SELECT device,n,zeroblob(128),CASE WHEN i%4=0 THEN counter(1) END,
                  CASE WHEN i%4=0 THEN counter(1) END FROM fixture;
         INSERT INTO _coven_members SELECT key,key,x'00','member',i%2 FROM fixture;
         INSERT INTO _coven_devices SELECT n,key,'device ' || i,i%2 FROM fixture;
         INSERT INTO _coven_circles SELECT 'circle_' || i,'circle ' || i FROM fixture;
         INSERT INTO _coven_circle_members SELECT 'circle_' || i,key FROM fixture;
         INSERT INTO _coven_circle_deletions SELECT 'circle_' || i,device,n FROM fixture;
         INSERT INTO _coven_data_inputs SELECT i,audience,'snapshot',device,n,1,x'00' FROM fixture;
         INSERT INTO _coven_row_file_refs SELECT device,substr(key,1,16),tab,key FROM fixture;
         INSERT INTO _coven_input_file_refs SELECT device,substr(key,1,16),i FROM fixture;
         INSERT INTO _coven_data_origins SELECT 'audience_' || i,i,x'' FROM fixture;
         INSERT INTO _coven_data_reloads SELECT 'audience_' || i,key,i%2 FROM fixture;
         INSERT INTO _coven_data_rows SELECT i,tab,key,x'00' FROM fixture;
         INSERT INTO _coven_data_losses SELECT i,0,x'00' FROM fixture;
         INSERT INTO _coven_write_inputs SELECT audience,CAST(n || device AS BLOB),x'00' FROM fixture;
         INSERT INTO _coven_write_input_frames SELECT audience,CAST(n || device AS BLOB),0,x'00' FROM fixture;
         INSERT INTO _coven_boundaries SELECT device,n,'schema',1,audience,device,n,x'00',i,1 FROM fixture;
         INSERT INTO _coven_pending_resets SELECT key,device,device,n FROM fixture;
         INSERT INTO _coven_loaded_audiences SELECT 'audience_' || i FROM fixture;
         INSERT INTO _coven_local_effects SELECT i,tab,key,audience FROM fixture;
         INSERT INTO _coven_local_effect_causes SELECT i,device,n FROM fixture;
         INSERT INTO _coven_local_effect_rows SELECT i,0,tab,'row',NULL,x'00',
           CASE WHEN i%2=0 THEN tab END,CASE WHEN i%2=0 THEN x'00' END FROM fixture;
         INSERT INTO _coven_fingerprint_sums SELECT 'audience_' || i,zeroblob(32) FROM fixture;
         INSERT INTO _coven_fingerprint_leaves SELECT audience,key,key,key FROM fixture;
         INSERT INTO _coven_writes
           SELECT i,CAST(n || device AS BLOB),n,x'00' FROM fixture;
         INSERT INTO _coven_positions SELECT n,n FROM fixture;
         INSERT INTO _coven_write_headers SELECT device,n,x'00' FROM fixture;
         INSERT INTO _coven_columns SELECT i,tab,'column_' || i FROM fixture;
         INSERT INTO _coven_rows SELECT i,tab,key,audience,counter(1),i FROM fixture;
         INSERT INTO _coven_cells
           SELECT (i-1)%1024+1+offset,i,i FROM fixture
           CROSS JOIN (SELECT 0 AS offset UNION ALL SELECT 1024 UNION ALL SELECT 2048);
         INSERT INTO _coven_foreign_keys SELECT i,tab,n FROM fixture;
         INSERT INTO _coven_reference_values
           SELECT column_id,row_id,'written value' FROM _coven_cells WHERE column_id<=1024;
         INSERT INTO _coven_references SELECT i,(i-1)%1024+1,i,tab,key,audience,counter(1) FROM fixture;
         INSERT INTO _coven_constraints SELECT i,tab,n FROM fixture;
         INSERT INTO _coven_claims SELECT i,i,audience,key FROM fixture;
         INSERT INTO _coven_lost
           (table_name,key,audience,generation,column_id,value,set_by,retired,replacement_kind,replaced_by)
           SELECT tab,key,audience,counter(1),CASE WHEN i%3=0 THEN i END,
                  zeroblob(64),n,i%2,CASE WHEN i%3=0 THEN 'write' ELSE 'rules' END,n FROM fixture;
         INSERT INTO _coven_uploads SELECT device,n,zeroblob(256),NULL FROM fixture;
         INSERT INTO _coven_queued_file_refs SELECT device,substr(key,1,16),device,n FROM fixture;
         INSERT INTO _coven_write_upload_sessions SELECT device,n,x'00' FROM fixture;
         INSERT INTO _coven_user_files SELECT tab,key,'file',key,key,n,zeroblob(13) FROM fixture;
         INSERT INTO _coven_device_files SELECT tab,key,'file',key,'path_' || i FROM fixture;
         INSERT INTO _coven_file_removals SELECT 'removed_' || i,'files' FROM fixture;
         INSERT INTO _coven_access_keys SELECT 'access_' || i,CASE WHEN i%2=0 THEN i END FROM fixture;
         INSERT INTO _coven_file_uploads(id,reference,queued_at) SELECT i,key,zeroblob(13) FROM fixture;
         INSERT INTO _coven_file_chunks SELECT tab,key,'file',key,0,key FROM fixture;
         INSERT INTO _coven_file_upload_chunks SELECT i,0,key FROM fixture;
         INSERT INTO _coven_cache(namespace,file_id,chunk,path,size,last_read,pinned,checked_hash)
           SELECT audience,'file_' || i,CASE WHEN (i/64)%4=0 THEN -2 ELSE 0 END,'cache_' || i,
                  65536,i,CASE WHEN (i/64)%4=0 THEN 1 ELSE 0 END,
                  CASE WHEN (i/64)%4=0 THEN key END FROM fixture;
         INSERT INTO _coven_cache_budgets SELECT 'audience_' || i,counter(1048576) FROM fixture;
         INSERT INTO _coven_snapshot_values SELECT audience,tab,key,x'00',1 FROM fixture;
         DROP TABLE fixture;
         COMMIT;"
    ).unwrap();
            let violations: i64 = db
                .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(violations, 0);
            let tables: Vec<String> = db
                .prepare(
                    "SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB '_coven_*'",
                )
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            for table in tables {
                let count: i64 = db
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
                    .unwrap();
                match table.as_str() {
                    "_coven_store_log_uploads"
                    | "_coven_snapshot_schema"
                    | "_coven_catch_up"
                    | "_coven_finality" => {
                        assert_eq!(count, 1)
                    }
                    _ => assert!(
                        count >= 4096,
                        "populate {table} before explaining its queries"
                    ),
                }
            }
            db
        }

        #[tokio::test]
        #[ignore = "one database-size measurement, run explicitly"]
        async fn ten_thousand_row_store_size() {
            let store = crate::tests::TestStore::new();
            let db = store.schema(
        vec![crate::SyncedTable::new("notes", crate::RowIdentity::IndependentUuid)],
        "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL,body TEXT NOT NULL)",
    ).await.unwrap();
            crate::write::tests::sql(
                &db,
                "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10000)
         INSERT INTO notes SELECT printf('00000000-0000-4000-8000-%012x',i),
         'Note ' || i,printf('%0128d',i) FROM n",
            )
            .await
            .unwrap();
            db.close().await.unwrap();
            eprintln!(
                "10,000-row store: {} bytes",
                std::fs::metadata(store.database_path()).unwrap().len()
            );
        }
    }
    mod scans {
        use super::source::Statement;
        /// Exceptions name the exact source statement and scanned relation. A nested
        /// lookup in a whole-table enumeration still has to use its own index.
        pub(super) fn reason(statement: &Statement, relation: &str) -> Option<&'static str> {
            let file = statement.file.as_str();
            let sql = statement.template.as_str();
            match (file, sql, relation) {
        ("database_file_retention.rs", "SELECT device,file FROM _coven_row_file_refs UNION SELECT device,file FROM _coven_input_file_refs UNION SELECT device,file FROM _coven_queued_file_refs", "_coven_row_file_refs" | "_coven_input_file_refs" | "_coven_queued_file_refs") =>
            Some("file retention returns every protected fixed path from the sparse indexes"),
        ("pending.rs", "INSERT INTO _coven_pending_resets(subject,reporter,entry_device,entry_number) SELECT p.subject,p.reporter,?1,?2 FROM _coven_pending AS p WHERE NOT EXISTS(SELECT 1 FROM _coven_access_keys AS k WHERE k.operation=p.operation) ON CONFLICT DO NOTHING", "p") =>
            Some("a newly applied reset suppresses every earlier observation but S3 key deletions"),
        ("pending.rs", sql, "p") if sql.starts_with("UPDATE _coven_pending AS p SET suppressed=EXISTS(") =>
            Some("replay rejudges each observation against its indexed reset causes"),
        ("entry_inputs.rs", "INSERT INTO temp._coven_snapshot_writes SELECT substr(timestamp,9,8),number,timestamp,had_read FROM _coven_writes", "_coven_writes") =>
            Some("retained snapshots validate references against all applied write metadata"),
        ("local_effects.rs", "SELECT EXISTS(SELECT 1 FROM _coven_local_effects)", "_coven_local_effects") =>
            Some("the emptiness check stops at the first kept effect"),
        ("local_effects.rs", sql, "_coven_local_effects") if sql.starts_with("DELETE FROM _coven_local_effects WHERE NOT EXISTS(") =>
            Some("release checks every kept local effect against finality"),
        ("entry_inputs.rs", "SELECT DISTINCT audience FROM _coven_data_inputs", "_coven_data_inputs") =>
            Some("release checks every audience that still retains replay inputs"),
        ("entry_inputs.rs", "SELECT DISTINCT entry_device,entry_number FROM _coven_pending_resets", "_coven_pending_resets") =>
            Some("release checks every reset that still conditionally suppresses observations"),
        ("entry_effects.rs", "SELECT audience FROM _coven_data_reloads", "_coven_data_reloads") =>
            Some("sync and replay read every audience awaiting replacement data"),
        ("sqlite.rs", "PRAGMA integrity_check", "database") =>
            Some("corruption detection checks every stored row and index"),
        ("database_recovery.rs", "PRAGMA table_info({table})", "database") =>
            Some("recovery inspects every column in the copied table's definition"),
        ("download.rs", "SELECT device,number FROM _coven_positions ORDER BY device", "_coven_positions") =>
            Some("positions() returns every device's consumed position"),
        ("store_log.rs" | "snapshot_write.rs", "SELECT device,max(number) FROM _coven_store_log GROUP BY device ORDER BY device", "_coven_store_log") =>
            Some("compute the complete store-log frontier for all devices"),
        ("database_recovery.rs", "SELECT record,sealing_keys FROM _coven_uploads ORDER BY rowid", "_coven_uploads") =>
            Some("recovery salvages every queued write in insertion order"),
        ("database_recovery.rs", "SELECT * FROM {table}", table)
            if statement.sql == format!("SELECT * FROM {table}") =>
            Some("recovery copies every row of the local journal or file table"),
        ("database_snapshots.rs", "SELECT device,number FROM _coven_uploads ORDER BY device,number", "_coven_uploads") =>
            Some("snapshot coverage includes every waiting write"),
        ("database_sync.rs", "SELECT EXISTS(SELECT 1 FROM _coven_uploads)", "_coven_uploads") =>
            Some("unfiltered EXISTS stops after the first queue row"),
        ("download.rs", "SELECT (SELECT publication FROM _coven_snapshot_schema WHERE EXISTS(SELECT 1 FROM _coven_members))", "_coven_snapshot_schema" | "_coven_members") =>
            Some("the schema row is singular; unfiltered membership EXISTS stops after its first row"),
        ("file_queue.rs", "SELECT id,reference,queued_at,attempts,last_attempt_at,failure,identity,session,stored,unused FROM _coven_file_uploads ORDER BY id", "_coven_file_uploads") =>
            Some("the file queue reader returns every queued attachment"),
        ("file_removals.rs", "SELECT path,area FROM _coven_file_removals ORDER BY area,path", "_coven_file_removals") =>
            Some("file removal visits every queued filesystem deletion"),
        ("key_upload.rs", "SELECT writer,number,bytes,before_device,before_number FROM _coven_key_uploads ORDER BY writer,number", "_coven_key_uploads") =>
            Some("the key-copy queue reader returns every reserved copy"),
        ("operation.rs", "SELECT id,kind,last_step,data,started_by FROM _coven_operations ORDER BY id", "_coven_operations") =>
            Some("operations() returns every unfinished journal entry"),
        ("pending.rs", "SELECT subject,reporter,value,operation,object,coven_version FROM _coven_pending" | "{SELECT} WHERE suppressed=0", "_coven_pending") =>
            Some("pending() returns every recorded condition"),
        ("snapshot_coverage.rs", "SELECT audience FROM _coven_loaded_audiences UNION SELECT audience FROM _coven_rows UNION SELECT audience FROM _coven_lost UNION SELECT 'store'", "_coven_loaded_audiences" | "_coven_rows" | "_coven_lost") =>
            Some("reload discovers the union of all locally represented audiences"),
        ("snapshot_load.rs", "SELECT device,number FROM _coven_store_log ORDER BY device,number", "_coven_store_log") =>
            Some("reload compares the entire committed entry set with its captured set"),
        ("snapshot_state.rs", "SELECT EXISTS(SELECT 1 FROM temp._coven_snapshot_values v WHERE matched=0 OR (matched=2) != EXISTS(SELECT 1 FROM _coven_lost l WHERE l.table_name=v.table_name AND l.key=v.key AND l.audience=v.audience AND l.retired=0 AND l.replacement_kind='rules'))", "v") =>
            Some("snapshot validation checks every staged row; its loss lookup must be indexed"),
        ("snapshot_write.rs", "SELECT {WRITE_DEVICE_SQL},number FROM _coven_writes", "_coven_writes") =>
            Some("snapshot counts include all applied write metadata"),
        ("snapshot_write.rs", "SELECT timestamp,number,had_read FROM _coven_writes ORDER BY {WRITE_DEVICE_SQL},number", "_coven_writes") =>
            Some("snapshot encoding emits all applied write metadata"),
        ("snapshot_writes.rs", "SELECT u.rowid,u.device,u.number,w.timestamp,w.had_read FROM _coven_uploads u LEFT JOIN _coven_writes w ON substr(w.timestamp,9,8)=u.device AND w.number=u.number ORDER BY u.device,u.number", "u") =>
            Some("reload replays every waiting write; its metadata lookup must be indexed"),
        ("sqlite.rs", "{} ORDER BY l.id", "l") =>
            Some("the public losses reader returns all losses"),
        ("store_log.rs", "SELECT device,number,outcome,beaten_device,beaten_number,target FROM _coven_store_log", "_coven_store_log") =>
            Some("replay verifies the complete previously committed entry set"),
        ("store_log_tables.rs", "SELECT record,outcome,beaten_device,beaten_number,target,author_view,stored_at FROM _coven_store_log", "_coven_store_log") =>
            Some("store_log() returns every replay entry"),
        ("store_log_tables.rs", "SELECT member,sealing,role,removed,access FROM _coven_members", "_coven_members") =>
            Some("store_log() returns the complete member map"),
        ("store_log_tables.rs", "SELECT device,member,name,removed FROM _coven_devices", "_coven_devices") =>
            Some("store_log() returns the complete device map"),
        ("store_log_tables.rs", "SELECT circle,name FROM _coven_circles", "_coven_circles") =>
            Some("store_log() returns the complete circle map"),
        ("store_log_tables.rs", "SELECT circle,member FROM _coven_circle_members", "_coven_circle_members") =>
            Some("store_log() returns every circle membership"),
        ("store_log_tables.rs", "SELECT circle,entry_device,entry_number FROM _coven_circle_deletions", "_coven_circle_deletions") =>
            Some("store_log() returns every circle deletion and its cause"),
        ("store_log_tables.rs", "DELETE FROM {table}", table)
            if statement.sql == format!("DELETE FROM {table}") =>
            Some("replay replaces the entire derived table; foreign-key lookups must be indexed"),
        ("store_log_upload.rs", "SELECT record,sealing_key FROM _coven_store_log_uploads", "_coven_store_log_uploads") =>
            Some("the store-log queue has an enforced maximum of one row"),
        ("upload.rs", "SELECT rowid,device,number,sealing_keys FROM _coven_uploads ORDER BY rowid LIMIT 1", "_coven_uploads") =>
            Some("rowid order with LIMIT 1 reads only the oldest queued write"),
        ("upload.rs", "SELECT rowid,device,number FROM _coven_uploads ORDER BY rowid", "_coven_uploads") =>
            Some("the queue reader lists every waiting write in insertion order"),
        ("write_boundary.rs", "SELECT b.kind,b.version,b.audience,b.snapshot_device,b.snapshot_number,b.entry_device,b.entry_number,b.included,b.applied FROM _coven_store_log l JOIN _coven_boundaries b ON b.entry_device=l.device AND b.entry_number=l.number WHERE l.outcome='kept' ORDER BY l.timestamp", "l") =>
            Some("boundary selection reads every kept boundary in replay order"),
        ("migration_snapshot.rs", "SELECT DISTINCT lower(table_name) FROM _coven_columns", "_coven_columns") =>
            Some("once per migration: discover every table with retained column metadata"),
        ("migration_writes.rs", "SELECT device,number,record FROM _coven_uploads WHERE sealing_keys IS NULL ORDER BY device,number", "_coven_uploads") =>
            Some("once per migration: convert every unattempted queued write"),
        ("migration_state.rs", "CREATE TEMP TABLE _coven_migration_retired AS SELECT DISTINCT table_name,key,audience FROM _coven_lost WHERE retired=0 AND replacement_kind='rules'", "_coven_lost") =>
            Some("once per migration: freeze all removed rows before changing their schema"),
        ("migration_state.rs", "SELECT id,table_name,column_name FROM _coven_columns", "_coven_columns") =>
            Some("once per migration: carry all column definitions through renames and drops"),
        ("migration_state.rs", "SELECT id,table_name,identity FROM _coven_foreign_keys", "_coven_foreign_keys") =>
            Some("once per migration: carry all foreign-key definitions through schema changes"),
        ("migration_state.rs", "SELECT id,table_name,identity FROM _coven_constraints", "_coven_constraints") =>
            Some("once per migration: carry all unique constraints through schema changes"),
        ("migration_state.rs", "DELETE FROM _coven_cells WHERE column_id=?1"
            | "UPDATE {table} SET column_id=?2 WHERE column_id=?1"
            | "DELETE FROM _coven_columns WHERE id=?1"
            | "DELETE FROM _coven_columns WHERE id=?1 AND NOT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=?1)", "_coven_cells") =>
            Some("once per migration: move/drop a column and check its cell references"),
        ("migration_state.rs", "DELETE FROM _coven_lost WHERE table_name=?1 AND retired=0", "_coven_lost") =>
            Some("once per migration: remove active losses of a dropped app table"),
        ("migration_state.rs", "DELETE FROM _coven_lost WHERE column_id=?1 AND retired=0 AND replacement_kind='write'"
            | "SELECT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=?1 AND retired=1)"
            | "UPDATE _coven_lost SET column_id=?2 WHERE column_id=?1 AND retired=0 AND replacement_kind='write'", "_coven_lost") =>
            Some("once per migration: separate active and retired losses when moving columns"),
        _ => None,
    }
        }
    }
    mod source {
        use std::{
            collections::{BTreeMap, BTreeSet},
            path::Path,
        };

        use proc_macro2::{TokenStream, TokenTree};
        use rusqlite::Connection;
        use syn::{spanned::Spanned, visit::Visit};
        struct Source {
            strings: Vec<(usize, String)>,
            table_loops: Vec<TableLoop>,
        }
        struct TableLoop {
            variable: String,
            start: usize,
            end: usize,
            tables: Vec<String>,
        }

        impl<'ast> Visit<'ast> for Source {
            fn visit_expr_for_loop(&mut self, node: &'ast syn::ExprForLoop) {
                if let (syn::Pat::Ident(variable), syn::Expr::Array(array)) =
                    (&*node.pat, &*node.expr)
                {
                    if variable.ident == "table" || variable.ident == "child" {
                        let tables = array
                            .elems
                            .iter()
                            .map(|expr| match expr {
                                syn::Expr::Lit(syn::ExprLit {
                                    lit: syn::Lit::Str(value),
                                    ..
                                }) => value.value(),
                                _ => panic!(
                                    "table loop needs an explicit SQL expansion at line {}",
                                    expr.span().start().line
                                ),
                            })
                            .collect();
                        self.table_loops.push(TableLoop {
                            variable: variable.ident.to_string(),
                            start: node.body.span().start().line,
                            end: node.body.span().end().line,
                            tables,
                        });
                    }
                }
                syn::visit::visit_expr_for_loop(self, node);
            }
        }

        pub(super) struct Statement {
            pub file: String,
            pub line: usize,
            pub template: String,
            pub sql: String,
        }
        /// Read the shipping SQL, including format strings, rather than maintaining a
        /// second copy of each query that can keep passing after its caller changes.
        pub(super) fn statements(db: &Connection) -> Vec<Statement> {
            let sources = sources();
            let tables: BTreeSet<String> = db
                .prepare(
                    "SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB '_coven_*'",
                )
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            let pending = select_constant(&sources["pending.rs"].strings, "SELECT subject,");
            let losses =
                select_constant(&sources["loss_record.rs"].strings, "SELECT l.table_name,");
            let columns = select_constant(
                &sources["snapshot_write.rs"].strings,
                "SELECT c.table_name,",
            );
            let mut statements = Vec::new();
            for (file, source) in &sources {
                if file == "internal_schema.rs" {
                    // The fixture explains each initial DDL statement before executing
                    // it; generated trigger bodies are explained separately below.
                    continue;
                }
                for (line, sql) in &source.strings {
                    let template = normalize(sql);
                    // These constants are fragments; their callers below append the
                    // predicate/order. Pending's SELECT is also executed on its own.
                    if (file == "loss_record.rs" && sql == &losses)
                        || (file == "snapshot_write.rs" && sql == &columns)
                    {
                        continue;
                    }
                    let sql = sql
                        .trim_start()
                        .replace(
                            "{WRITE_DEVICE_SQL}",
                            crate::internal_schema::WRITE_DEVICE_SQL,
                        )
                        .replace("{COLUMNS}", &columns)
                        .replace(
                            "{SELECT}",
                            if file == "pending.rs" {
                                &pending
                            } else {
                                &losses
                            },
                        );
                    let sql = if sql.starts_with("{} WHERE l.") || sql.starts_with("{} ORDER BY l.")
                    {
                        sql.replacen("{}", &losses, 1)
                    } else {
                        sql
                    };
                    let keyword = sql
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .to_ascii_uppercase();
                    let diagnostic = matches!(
                        (file.as_str(), sql.as_str()),
                        ("sqlite.rs", "PRAGMA integrity_check")
                            | ("database_recovery.rs", "PRAGMA table_info({table})")
                    );
                    if !matches!(
                        keyword.as_str(),
                        "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "WITH" | "CREATE"
                    ) && !diagnostic
                    {
                        continue;
                    }
                    for sql in expand(db, file, *line, &sql, source) {
                        // App schema introspection and app-defined tables do not use the
                        // internal schema's indexes. Temporary staging is covered when
                        // the same statement also reads a persistent internal table.
                        if !diagnostic
                            && !sql
                                .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                                .any(|word| tables.contains(&word.to_ascii_lowercase()))
                        {
                            continue;
                        }
                        assert!(!sql.contains('{'), "unexpanded SQL in {file}:{line}: {sql}");
                        statements.push(Statement {
                            file: file.clone(),
                            line: *line,
                            template: template.clone(),
                            sql,
                        });
                    }
                }
            }
            // Explain the actual generated trigger bodies, with OLD values represented
            // by bound parameters. SQLite's top-level query plan omits trigger bodies.
            let mut triggers = db
                .prepare("SELECT name,sql FROM sqlite_schema WHERE type='trigger'")
                .unwrap();
            for trigger in triggers
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .unwrap()
            {
                let (name, sql) = trigger.unwrap();
                let (_, body) = sql.split_once("BEGIN").unwrap();
                let (body, _) = body.rsplit_once("END").unwrap();
                let mut body = body.to_owned();
                for (index, column) in ["table_name", "key", "column_name", "identity"]
                    .iter()
                    .enumerate()
                {
                    body = body.replace(&format!("OLD.{column}"), &format!("?{}", index + 1));
                }
                statements.push(Statement {
                    file: name,
                    line: 0,
                    template: normalize(&body),
                    sql: body,
                });
            }
            statements
        }

        fn normalize(sql: &str) -> String {
            sql.split_whitespace().collect::<Vec<_>>().join(" ")
        }

        fn sources() -> BTreeMap<String, Source> {
            let mut sources = BTreeMap::new();
            for entry in
                std::fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("src")).unwrap()
            {
                let path = entry.unwrap().path();
                let name = path.file_name().unwrap().to_str().unwrap();
                if path.extension().is_some_and(|e| e == "rs")
                    && !name.ends_with("_tests.rs")
                    && name != "test_utils.rs"
                {
                    let text = std::fs::read_to_string(&path).unwrap();
                    let mut source = Source {
                        strings: Vec::new(),
                        table_loops: Vec::new(),
                    };
                    literals(text.parse().unwrap(), &mut source.strings);
                    source.visit_file(&syn::parse_file(&text).unwrap());
                    sources.insert(name.to_owned(), source);
                }
            }
            sources
        }

        pub(super) fn staging_schema() -> String {
            select_constant(
                &sources()["snapshot_state.rs"].strings,
                "CREATE TEMP TABLE _coven_snapshot_values",
            )
        }

        fn literals(tokens: TokenStream, strings: &mut Vec<(usize, String)>) {
            for token in tokens {
                match token {
                    TokenTree::Group(group) => literals(group.stream(), strings),
                    TokenTree::Literal(literal) => {
                        if let Ok(syn::Lit::Str(string)) =
                            syn::parse_str::<syn::Lit>(&literal.to_string())
                        {
                            strings.push((literal.span().start().line, string.value()));
                        }
                    }
                    _ => {}
                }
            }
        }

        fn select_constant(strings: &[(usize, String)], prefix: &str) -> String {
            let values: Vec<_> = strings
                .iter()
                .filter(|(_, sql)| sql.starts_with(prefix))
                .collect();
            assert_eq!(values.len(), 1, "SQL constant {prefix}");
            values[0].1.clone()
        }

        fn replace(sql: &str, name: &str, values: &[&str]) -> Vec<String> {
            values
                .iter()
                .map(|value| sql.replace(name, value))
                .collect()
        }

        fn expand(
            db: &Connection,
            file: &str,
            line: usize,
            sql: &str,
            source: &Source,
        ) -> Vec<String> {
            if !sql.contains('{') {
                return vec![sql.to_owned()];
            }
            if file == "rowid_policy.rs"
                && normalize(sql) == "CREATE TEMP TRIGGER {} BEFORE UPDATE ON {}.{} WHEN old.{alias} IS NOT new.{alias} BEGIN SELECT _coven_rowid_changed(); END"
            {
                // The guard compares an app row's addresses and calls a function;
                // it reads no persistent internal table or index.
                return Vec::new();
            }
            if sql.contains("{metadata}") {
                assert_eq!(file, "row_queries.rs");
                return [
                    ("_coven_columns", "column_name"),
                    ("_coven_foreign_keys", "identity"),
                    ("_coven_constraints", "identity"),
                ]
                .into_iter()
                .map(|(table, column)| sql.replace("{metadata}", table).replace("{column}", column))
                .collect();
            }
            if sql.contains("{local_table}") {
                assert_eq!(file, "file_write.rs");
                return replace(
                    sql,
                    "{local_table}",
                    &["_coven_user_files", "_coven_device_files"],
                );
            }
            if sql.contains("{FINAL}") {
                assert!(matches!(file, "entry_inputs.rs" | "local_effects.rs"));
                let condition =
                    select_constant(&sources()["entry_inputs.rs"].strings, "(substr(l.stored_at");
                return vec![sql.replace("{FINAL}", &condition)];
            }
            if sql.contains("{table}") || sql.contains("{child}") {
                let variable = if sql.contains("{table}") {
                    "table"
                } else {
                    "child"
                };
                let loops: Vec<_> = source
                    .table_loops
                    .iter()
                    .filter(|binding| {
                        binding.variable == variable
                            && ((binding.start..=binding.end).contains(&line)
                // copy_table receives the loop's table as a function argument.
                || file == "database_recovery.rs")
                    })
                    .collect();
                assert_eq!(
                    loops.len(),
                    1,
                    "unclassified table expansion in {file}:{line}: {sql}"
                );
                let tables = &loops[0].tables;
                return tables
                    .iter()
                    .map(|table| {
                        let sql = sql.replace("{table}", table).replace("{child}", table);
                        if sql.contains("{placeholders}") {
                            let count: i64 = db
                                .query_row(
                                    "SELECT count(*) FROM pragma_table_info(?1)",
                                    [table],
                                    |r| r.get(0),
                                )
                                .unwrap();
                            sql.replace("{placeholders}", &vec!["?"; count as usize].join(","))
                        } else {
                            sql
                        }
                    })
                    .collect();
            }
            // The remaining builders address app tables or their per-schema temporary
            // mirrors, not tables declared by initial_schema. Keep this list explicit:
            // a new builder must be classified instead of disappearing from the audit.
            assert!(
                matches!(
                    file,
                    "database_file_retention.rs"
                        | "file_database.rs"
                        | "file_row.rs"
                        | "file_write.rs"
                        | "key_scope.rs"
                        | "local_effects.rs"
                        | "migration_run.rs"
                        | "migration_snapshot.rs"
                        | "removal.rs"
                        | "removal_schema.rs"
                        | "removal_sql.rs"
                        | "removal_view.rs"
                        | "write_rows.rs"
                        | "write_schema.rs"
                ),
                "unclassified SQL builder in {file}: {sql}"
            );
            assert!(
                !sql.contains("_coven_") || file == "migration_snapshot.rs",
                "internal SQL builder: {sql}"
            );
            Vec::new()
        }
    }

    #[test]
    fn internal_query_plans() {
        let db = fixture::populated();
        let statements = source::statements(&db);
        assert!(
            statements.len() > 300,
            "internal SQL inventory unexpectedly empty"
        );
        let mut used = BTreeSet::new();
        // Unique indexes also enforce integrity when EQP does not name them.
        let indexes: BTreeSet<String> = db
            .prepare(
                "SELECT name FROM sqlite_schema WHERE type='index' AND sql LIKE 'CREATE INDEX %'",
            )
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let mut failures = Vec::new();
        // A freshly opened store has no statistics. Also check populated/analyzed
        // stores, so neither skip-scans nor one accidental join order hide a gap.
        for analyzed in [false, true] {
            if analyzed {
                db.execute_batch("ANALYZE").unwrap();
            }
            for statement in &statements {
                let plans = plans(&db, statement);
                for plan in &plans {
                    used.extend(
                        plan.split_whitespace()
                            .filter(|word| indexes.contains(*word))
                            .map(str::to_owned),
                    );
                }
                if plans.iter().any(|plan| unapproved_scan(statement, plan)) {
                    failures.push(format!(
                        "{}:{} (analyzed={analyzed}): {}\n{}",
                        statement.file,
                        statement.line,
                        statement.sql,
                        plans.join("\n")
                    ));
                }
                // EQP labels an unindexed max() as SEARCH even when its VM walks
                // every entry. Execute parameter-free reads with no reported scans
                // as well; this covers both the cache counter and the write clock.
                if plans.iter().all(|plan| !plan.starts_with("SCAN ")) {
                    let mut read = db.prepare(&statement.sql).unwrap();
                    if read.readonly() && read.parameter_count() == 0 {
                        let mut rows = read.raw_query();
                        while rows.next().unwrap().is_some() {}
                        drop(rows);
                        // VM counters have no relation name. Only the listed
                        // diagnostic pragmas allow an unnamed database-wide walk.
                        if read.get_status(rusqlite::StatementStatus::FullscanStep) != 0
                            && scans::reason(statement, "database").is_none()
                        {
                            failures.push(format!(
                                "{}:{}: hidden scan in {}",
                                statement.file, statement.line, statement.sql
                            ));
                        }
                    }
                }
            }
        }
        for index in indexes.difference(&used) {
            failures.push(format!("unused secondary index: {index}"));
        }
        assert!(failures.is_empty(), "{}", failures.join("\n\n"));
    }

    fn plans(db: &Connection, statement: &source::Statement) -> Vec<String> {
        let mut prepared = db
            .prepare(&format!("EXPLAIN QUERY PLAN {}", statement.sql))
            .unwrap_or_else(|error| {
                panic!(
                    "{}:{}: {}\n{error}",
                    statement.file, statement.line, statement.sql
                )
            });
        let mut rows = prepared.raw_query();
        let mut plans = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            plans.push(row.get::<_, String>(3).unwrap());
        }
        plans
    }

    fn unapproved_scan(statement: &source::Statement, plan: &str) -> bool {
        let Some(scan) = plan.strip_prefix("SCAN ") else {
            return false;
        };
        // Constant rows and materialized compound-query results aren't table reads.
        if scan == "CONSTANT ROW" || scan.starts_with("(subquery-") {
            return false;
        }
        let relation = scan.split_whitespace().next().unwrap();
        scans::reason(statement, relation).is_none()
    }

    #[test]
    fn an_allowed_enumeration_does_not_hide_a_scanning_join() {
        let db = fixture::populated();
        let statements = source::statements(&db);
        let statement = statements
            .iter()
            .find(|s| s.file == "snapshot_writes.rs" && s.sql.starts_with("SELECT u.rowid"))
            .unwrap();
        assert!(plans(&db, statement)
            .iter()
            .all(|plan| !unapproved_scan(statement, plan)));
        db.execute_batch("DROP INDEX _coven_write_position")
            .unwrap();
        assert!(plans(&db, statement)
            .iter()
            .any(|plan| unapproved_scan(statement, plan)));
    }
}
