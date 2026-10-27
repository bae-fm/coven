use crate::{tests::TestStore, Migration, RowIdentity, SyncedTable};
use coven_format::value::Value;
use rusqlite::Connection;
use std::collections::BTreeSet;

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
                ("view".into(), "coven_devices".into()),
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

// EXPLAIN QUERY PLAN checks for every internal statement against a populated schema.
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
        for (name, arguments) in [("_coven_unix_ms", 1), ("coven_key", -1)] {
            db.create_scalar_function(name, arguments, flags, |_| Ok(Vec::<u8>::new()))
                .unwrap();
        }
        // Like coven's own, this varies, so SQLite never evaluates it while
        // planning a statement.
        db.create_scalar_function("_coven_device", 0, FunctionFlags::SQLITE_UTF8, |_| {
            Ok(Vec::<u8>::new())
        })
        .unwrap();
        crate::UploadsInFlight::new(crate::observation::CommitObserver::new())
            .register(&db)
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
            CAST(printf('%032d',i) AS BLOB) AS key,
            CAST(printf('%016d',i) AS BLOB) AS member FROM numbers;
     INSERT INTO _coven_operations(id,kind,last_step,data,started_by)
       SELECT i,'kind_' || (i%32),1,x'00','app' FROM fixture;
     INSERT INTO _coven_provider_access SELECT 'account_' || i,i%2,NULL,NULL,i,i%2 FROM fixture;
     INSERT INTO _coven_pending(subject,value,operation,object,device,file,coven_version)
       SELECT key,x'00',CASE WHEN i%2=0 THEN i END,
              CAST(x'00' || n AS BLOB),CASE WHEN i%2=1 THEN device END,
              CASE WHEN i%2=1 THEN substr(key,17,16) END,'1' FROM fixture;
     INSERT INTO _coven_members SELECT member,key,key,x'00','member',i%2 FROM fixture;
     INSERT INTO _coven_bound_keys SELECT key,key FROM fixture;
     INSERT INTO _coven_devices SELECT n,member,'device ' || i,CASE i%3 WHEN 0 THEN 'active' WHEN 1 THEN 'removed' ELSE 'replaced' END FROM fixture;
     INSERT INTO _coven_circles SELECT 'circle_' || i,'circle ' || i FROM fixture;
     INSERT INTO _coven_circle_members SELECT 'circle_' || i,member FROM fixture;
     INSERT INTO _coven_circle_deletions SELECT 'circle_' || i,n FROM fixture;
     INSERT INTO _coven_row_file_refs SELECT device,substr(key,1,16),tab,key FROM fixture;
     INSERT INTO _coven_row_files SELECT tab,key,audience,device,substr(key,17,16),'file_' || i,zeroblob(32) FROM fixture;
     INSERT INTO _coven_data_reloads SELECT 'audience_' || i,key FROM fixture;
     INSERT INTO _coven_boundaries SELECT n,'schema',1,audience,device,n,counter(1),1 FROM fixture;
     INSERT INTO _coven_raise_captures SELECT 'audience_' || i,2,n,n FROM fixture;
     INSERT INTO _coven_writes
       SELECT i,CAST(n || device AS BLOB),n,counter(0),n FROM fixture;
     INSERT INTO _coven_device_reset VALUES(1,zeroblob(8),counter(1));
     INSERT INTO _coven_write_headers SELECT device,n,x'00' FROM fixture;
     INSERT INTO _coven_authored_parts SELECT CAST(device || n AS BLOB),audience,x'00',1 FROM fixture;
     INSERT INTO _coven_authored_changes SELECT CAST(device || n AS BLOB),audience,tab,key,x'00',NULL FROM fixture;
     INSERT INTO _coven_creations SELECT tab,key,audience,CAST(device || n AS BLOB),x'00' FROM fixture;
     INSERT INTO _coven_write_evidence SELECT CAST(device || n AS BLOB),x'00',x'00',x'',zeroblob(64) FROM fixture;
     INSERT INTO _coven_part_leaves SELECT CAST(device || n AS BLOB),audience,zeroblob(32) FROM fixture;
     INSERT INTO _coven_part_proofs SELECT CAST(device || n AS BLOB),audience,x'00',zeroblob(32) FROM fixture;
     INSERT INTO _coven_owner_tables SELECT DISTINCT 'table_' || i FROM fixture;
     INSERT INTO _coven_table_origins SELECT i,tab,1,tab FROM fixture;
     INSERT INTO _coven_column_origins SELECT i,tab,'column_' || i,1,tab,'column_' || i FROM fixture;
     INSERT INTO _coven_conversion_schemas SELECT i,x'00',x'00',x'00',x'00' FROM fixture;
     INSERT INTO _coven_columns SELECT i,tab,'column_' || i FROM fixture;
     INSERT INTO _coven_rows SELECT i,tab,key,audience,counter(1),i FROM fixture;
     INSERT INTO _coven_cells
       SELECT (i-1)%1024+1+offset,i,i FROM fixture
       CROSS JOIN (SELECT 0 AS offset UNION ALL SELECT 1024 UNION ALL SELECT 2048);
     INSERT INTO _coven_foreign_keys SELECT i,tab,n FROM fixture;
     INSERT INTO _coven_written_values
       SELECT column_id,row_id,'written value' FROM _coven_cells WHERE column_id<=1024;
     INSERT INTO _coven_references SELECT i,(i-1)%1024+1,i,tab,key,audience,counter(1) FROM fixture;
     INSERT INTO _coven_constraints SELECT i,tab,n FROM fixture;
     INSERT INTO _coven_claims SELECT i,i,audience,key FROM fixture;
     INSERT INTO _coven_lost
       (table_name,key,audience,generation,column_id,value,set_by,frozen,replacement_kind,replaced_by)
       SELECT tab,key,audience,counter(1),CASE WHEN i%3=0 THEN i END,
              zeroblob(64),n,i%2,CASE WHEN i%3=0 THEN 'write' ELSE 'rules' END,n FROM fixture;
     INSERT INTO _coven_uploads SELECT device,n,zeroblob(256),NULL,NULL FROM fixture;
     INSERT INTO _coven_queued_file_refs SELECT device,substr(key,1,16),device,n FROM fixture;
     INSERT INTO _coven_user_files SELECT tab,key,'file',key,key,n,zeroblob(13) FROM fixture;
     INSERT INTO _coven_file_removals SELECT 'removed_' || i,'files' FROM fixture;
     INSERT INTO _coven_cache_writes SELECT 'writing_' || i FROM fixture;
     INSERT INTO _coven_file_uploads(id,reference,device,file,queued_at,pinned)
       SELECT i,key,device,substr(key,17,16),zeroblob(13),i%2 FROM fixture;
     INSERT INTO _coven_retries(operation,upload,failures,not_before,wait)
       SELECT CASE WHEN i%2=0 THEN i END,CASE WHEN i%2=1 THEN i END,1,zeroblob(13),1000 FROM fixture;
     INSERT INTO _coven_retries(scope,failures,not_before,wait)
       VALUES('join',1,zeroblob(13),1000),('provider',0,zeroblob(13),600000);
     INSERT INTO _coven_device_files SELECT device,substr(key,17,16),'path_' || i FROM fixture;
     INSERT INTO _coven_file_candidates SELECT device,substr(key,17,16) FROM fixture;
     INSERT INTO _coven_file_scans SELECT n,n FROM fixture;
     INSERT INTO _coven_upload_pause VALUES(1);
     INSERT INTO _coven_write_pause VALUES(1,1);
     INSERT INTO _coven_pin_requests SELECT audience,'file_' || i,key FROM fixture;
     INSERT INTO _coven_file_chunks SELECT device,substr(key,17,16),0,key FROM fixture;
     INSERT INTO _coven_pending_attachments SELECT i,tab,key,audience,counter(1),key FROM fixture;
     INSERT INTO _coven_abandoned_attachments(table_name,key,device,file,reason)
       SELECT tab,key,device,CAST(zeroblob(8) || n AS BLOB),'replaced' FROM fixture;
     INSERT INTO _coven_cache(namespace,file_id,chunk,path,size,last_read,pinned,checked_hash)
       SELECT audience,'file_' || i,CASE WHEN (i/64)%4=0 THEN -2 ELSE 0 END,'cache_' || i,
              65536,i,CASE WHEN (i/64)%4=0 THEN 1 ELSE 0 END,
              CASE WHEN (i/64)%4=0 THEN key END FROM fixture;
     INSERT INTO _coven_cache_budgets(namespace,bytes) SELECT 'audience_' || i,counter(1048576) FROM fixture WHERE true ON CONFLICT(namespace) DO UPDATE SET bytes=excluded.bytes;
     INSERT INTO _coven_snapshot_values SELECT audience,tab,key,x'00',1 FROM fixture;
     INSERT INTO _coven_store VALUES(1,zeroblob(16),'fixture',x'0000000000000001',1);
     INSERT INTO _coven_published VALUES(1);
     INSERT INTO _coven_published_rows SELECT tab,key,audience,x'00',i%2 FROM fixture;
     INSERT INTO _coven_published_losses SELECT i,tab,key,audience,x'00' FROM fixture;
     INSERT INTO _coven_published_creations SELECT tab,key,audience,CAST(device || n AS BLOB),x'00' FROM fixture;
     INSERT INTO _coven_published_file_refs SELECT device,substr(key,17,16) FROM fixture;
     INSERT INTO _coven_migration_prefix VALUES(1,counter(1),counter(4096),2);
     INSERT INTO _coven_migration_losses SELECT CAST(device || n AS BLOB),audience,key FROM fixture;
     INSERT INTO _coven_last_claims SELECT n,n,key,n FROM fixture;
     INSERT INTO _coven_absent_snapshots SELECT audience,device,n FROM fixture;
     INSERT INTO _coven_claim_counters SELECT n,n FROM fixture;
     INSERT INTO _coven_claim_journal SELECT device,n,NULL,i,'landed',1 FROM fixture WHERE i=1;
     INSERT INTO _coven_claim_attempts SELECT counter(1),counter(1),i,'store/seq/' || i,key,counter(32),NULL FROM fixture;
     INSERT INTO _coven_claim_chunks SELECT counter(1),counter(1),i,0,key FROM fixture;
     INSERT INTO _coven_slot_records SELECT n,CASE i%3 WHEN 0 THEN 'admin' WHEN 1 THEN 'key' ELSE 'announcement' END,key FROM fixture;
     INSERT INTO _coven_write_refusals SELECT n,n FROM fixture;
     INSERT INTO _coven_cutoffs SELECT n,CASE WHEN i%3=0 THEN device END,CASE WHEN i%3<>0 THEN member END,CASE WHEN i%3=2 THEN key END FROM fixture;
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
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB '_coven_*'")
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
                "_coven_store"
                | "_coven_published"
                | "_coven_migration_prefix"
                | "_coven_sequence"
                | "_coven_snapshot_reservation"
                | "_coven_snapshot_schema"
                | "_coven_device_reset"
                | "_coven_upload_pause"
                | "_coven_write_pause"
                | "_coven_claim_journal" => {
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
#[path = "internal_schema_scan_reasons.rs"]
mod scans;
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
            if let (syn::Pat::Ident(variable), syn::Expr::Array(array)) = (&*node.pat, &*node.expr)
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
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB '_coven_*'")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let pending = select_constant(&sources["pending.rs"].strings, "SELECT subject,");
        let losses = select_constant(&sources["loss_record.rs"].strings, "SELECT l.table_name,");
        let mut statements = Vec::new();
        for (file, source) in &sources {
            if matches!(
                file.as_str(),
                "internal_schema.rs"
                    | "internal_schema_ownership.rs"
                    | "internal_schema_sequence.rs"
                    | "internal_schema_views.rs"
                    | "internal_schema_pins.rs"
                    | "internal_schema_upload_queue.rs"
            ) {
                // The fixture explains each initial DDL statement before executing
                // it; generated trigger bodies are explained separately below.
                continue;
            }
            for (line, sql) in &source.strings {
                let template = normalize(sql);
                // These constants are fragments; their callers below append the
                // predicate/order. Pending's SELECT is also executed on its own.
                if file == "loss_record.rs" && sql == &losses {
                    continue;
                }
                let sql = sql
                    .trim_start()
                    .replace(
                        "{WRITE_DEVICE_SQL}",
                        crate::internal_schema::WRITE_DEVICE_SQL,
                    )
                    .replace("{select}", &losses)
                    .replace(
                        "{SELECT}",
                        if file == "pending.rs" {
                            &pending
                        } else {
                            &losses
                        },
                    );
                let sql = if sql.starts_with("{} WHERE l.") || sql.starts_with("{} ORDER BY l.") {
                    sql.replacen("{}", &losses, 1)
                } else {
                    sql
                };
                let sql =
                    if file == "published_migration.rs" && sql.starts_with("CREATE TEMP TABLE") {
                        // The table exists in this fixture; explain the copied rows.
                        sql.split_once(" AS ")
                            .unwrap()
                            .1
                            .split(';')
                            .next()
                            .unwrap()
                            .to_owned()
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
        // Explain the actual generated trigger bodies, with OLD and NEW values
        // represented by bound parameters. SQLite's top-level query plan omits trigger bodies.
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
            for (index, column) in [
                "table_name",
                "key",
                "column_name",
                "identity",
                // Before `file`, which it starts with.
                "file_id",
                "namespace",
                "size",
                "device",
                "file",
                "account",
                "access_key_id",
                "upload",
                "chunk",
                "id",
            ]
            .iter()
            .enumerate()
            {
                for row in ["OLD", "NEW"] {
                    body = body.replace(&format!("{row}.{column}"), &format!("?{}", index + 1));
                }
            }
            // A body may run several statements; each is explained.
            for statement in body.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                statements.push(Statement {
                    file: name.clone(),
                    line: 0,
                    template: normalize(statement),
                    sql: statement.to_owned(),
                });
            }
        }
        // What the app's SQL reads, each read whole as a list shows it.
        let mut views = db
            .prepare("SELECT name FROM sqlite_schema WHERE type IN ('view','table') AND name GLOB 'coven_*'")
            .unwrap();
        for view in views.query_map([], |r| r.get::<_, String>(0)).unwrap() {
            let view = view.unwrap();
            let sql = format!("SELECT * FROM {view}");
            statements.push(Statement {
                file: view,
                line: 0,
                template: normalize(&sql),
                sql,
            });
        }
        statements
    }

    fn normalize(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn sources() -> BTreeMap<String, Source> {
        let mut sources = BTreeMap::new();
        for entry in std::fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("src")).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_str().unwrap();
            // The scan reasons are this test's own table, not shipping SQL.
            if path.extension().is_some_and(|e| e == "rs")
                && !name.ends_with("_tests.rs")
                && name != "test_utils.rs"
                && name != "internal_schema_scan_reasons.rs"
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
        let sources = sources();
        let mut sql = select_constant(
            &sources["snapshot_state.rs"].strings,
            "CREATE TEMP TABLE _coven_snapshot_values",
        ) + &select_constant(
            &sources["snapshot_view.rs"].strings,
            "CREATE TEMP TABLE _coven_snapshot_losses",
        );
        for name in ["published_migration.rs", "migration_run.rs"] {
            for (_, literal) in &sources[name].strings {
                for statement in literal.split(';').map(str::trim).filter(|s| {
                    s.starts_with("CREATE TEMP TABLE _coven_carried_")
                        || s.starts_with("CREATE TEMP TABLE _coven_migration_frozen")
                        || s.starts_with("CREATE INDEX temp._coven_carried_")
                }) {
                    sql.push_str(statement);
                    sql.push(';');
                }
            }
        }
        sql
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

    fn expand(db: &Connection, file: &str, line: usize, sql: &str, source: &Source) -> Vec<String> {
        if !sql.contains('{') {
            return vec![sql.to_owned()];
        }
        if file == "reference_assignments.rs" {
            // These objects capture each write's app operations. They read
            // no persistent internal table.
            assert!(matches!(normalize(sql).as_str(),
                "CREATE TEMP TABLE {TABLE}(table_name TEXT, column_name TEXT)"
                | "DELETE FROM temp.{TABLE}"
                | "CREATE TEMP TRIGGER {} AFTER UPDATE OF {name} ON main.{} WHEN old.{name} IS NULL AND new.{name} IS NULL BEGIN INSERT INTO {TABLE} VALUES('{}','{}'); END"
                | "SELECT rowid FROM temp.{TABLE}"
            ), "unclassified assignment capture SQL: {sql}");
            return Vec::new();
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
        // A row's cells, and the written values it keeps, bound as lists of
        // a length only the row knows; two of each stand for any.
        if file == "write_commit.rs" && sql.contains("{values}") {
            return vec![sql.replace("{values}", "(?,?,?),(?,?,?)")];
        }
        if file == "written_values.rs" && sql.contains("{others}") {
            return vec![sql.replace("{others}", "?,?")];
        }
        if file == "loss_record.rs" && sql.contains("{table}") {
            return ["_coven_lost", "temp._coven_snapshot_losses"]
                .into_iter()
                .map(|table| sql.replace("{table}", table))
                .collect();
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
                    binding.variable == variable && (binding.start..=binding.end).contains(&line)
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
                            .query_row("SELECT count(*) FROM pragma_table_info(?1)", [table], |r| {
                                r.get(0)
                            })
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

/// Coven's triggers keep `coven_pins` what `_coven_pin_source` says, however
/// the tables behind it were filled: the fixture writes each of them in turn.
#[test]
fn the_pins_table_holds_what_its_source_says() {
    let db = fixture::populated();
    let differ = |sql: &str| -> i64 { db.query_row(sql, [], |r| r.get(0)).unwrap() };
    let kept = differ("SELECT count(*) FROM coven_pins");
    assert!(kept > 0);
    assert_eq!(
        differ("SELECT count(*) FROM (SELECT * FROM coven_pins EXCEPT SELECT * FROM _coven_pin_source)"),
        0
    );
    assert_eq!(
        differ("SELECT count(*) FROM (SELECT * FROM _coven_pin_source EXCEPT SELECT * FROM coven_pins)"),
        0
    );
}

/// Coven's triggers keep `coven_upload_queue` the files `coven_uploads`
/// lists, however the tables behind it were filled.
#[test]
fn the_upload_queue_table_holds_what_coven_uploads_lists() {
    let db = fixture::populated();
    let differ = |sql: &str| -> i64 { db.query_row(sql, [], |r| r.get(0)).unwrap() };
    assert!(differ("SELECT count(*) FROM coven_upload_queue") > 0);
    for (from, to) in [
        ("coven_upload_queue", "coven_uploads"),
        ("coven_uploads", "coven_upload_queue"),
    ] {
        assert_eq!(
            differ(&format!(
                "SELECT count(*) FROM (SELECT table_name,key,file,position FROM {from}
                 EXCEPT SELECT table_name,key,file,position FROM {to})"
            )),
            0,
            "{from} beyond {to}"
        );
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
        .prepare("SELECT name FROM sqlite_schema WHERE type='index' AND sql LIKE 'CREATE INDEX %'")
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
        .find(|s| s.file == "queued_writes.rs" && s.sql.contains("JOIN _coven_writes"))
        .unwrap();
    assert!(plans(&db, statement)
        .iter()
        .all(|plan| !unapproved_scan(statement, plan)));
    db.execute_batch("DROP INDEX _coven_write_identity; DROP INDEX _coven_write_slot")
        .unwrap();
    assert!(plans(&db, statement)
        .iter()
        .any(|plan| unapproved_scan(statement, plan)));
}
