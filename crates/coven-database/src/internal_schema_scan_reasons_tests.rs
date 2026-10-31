//! Every internal statement's query plan, against a populated schema, scans
//! only what `internal_schema_scan_reasons.rs` names; and the tables coven's
//! triggers keep agree with what they keep, however that schema was filled.

use super::{reason, Statement};
use rusqlite::Connection;
use std::collections::BTreeSet;

/// A populated internal schema the plan checks explain every statement
/// against.
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
        for (name, arguments) in [
            ("_coven_unix_ms", 1),
            ("_coven_unix_seconds", 1),
            ("_coven_unix_nanos", 1),
            ("coven_key", -1),
            ("_coven_file_id", 1),
            ("_coven_file_of_key", 1),
            ("_coven_stored_device", 1),
            ("_coven_stored_object", 1),
            ("_coven_stored_cache_id", 1),
        ] {
            db.create_scalar_function(name, arguments, flags, |_| Ok(Vec::<u8>::new()))
                .unwrap();
        }
        // Like coven's own, this varies, so SQLite never evaluates it while
        // planning a statement.
        db.create_scalar_function("_coven_device", 0, FunctionFlags::SQLITE_UTF8, |_| {
            Ok(Vec::<u8>::new())
        })
        .unwrap();
        crate::transfers_in_flight::TransfersInFlight::new(
            crate::observation::CommitObserver::new(),
        )
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
     INSERT INTO _coven_pending(subject,value,operation,object,work,device,file,coven_version)
       SELECT key,x'00',CASE WHEN i%2=0 THEN i END,
              CAST(x'00' || n AS BLOB),CASE WHEN i%4=1 THEN 'upload' WHEN i%4=3 THEN 'download' END,
              CASE WHEN i%2=1 THEN device END,
              CASE WHEN i%2=1 THEN substr(key,17,16) END,'1' FROM fixture;
     INSERT INTO _coven_members SELECT member,key,key,x'00','member',i%2 FROM fixture;
     INSERT INTO _coven_bound_keys SELECT key,key FROM fixture;
     INSERT INTO _coven_devices SELECT n,member,'device ' || i,CASE i%3 WHEN 0 THEN 'active' WHEN 1 THEN 'removed' ELSE 'replaced' END FROM fixture;
     INSERT INTO _coven_circles SELECT 'circle_' || i,'circle ' || i FROM fixture;
     INSERT INTO _coven_circle_members SELECT 'circle_' || i,member FROM fixture;
     INSERT INTO _coven_circle_deletions SELECT 'circle_' || i,n FROM fixture;
     INSERT INTO _coven_row_file_refs SELECT device,substr(key,1,16),tab,key FROM fixture;
     INSERT INTO _coven_stored_files SELECT 'file_' || i,audience,device,substr(key,17,16),'file_' || i,key FROM fixture;
     INSERT INTO coven_files SELECT 'file_' || i,audience,1,zeroblob(32),'device 1','files' FROM fixture;
     INSERT INTO _coven_file_references SELECT 'file_' || i,tab,key,'column_' || i FROM fixture;
     INSERT INTO _coven_file_columns SELECT tab,'column_' || i,i%2 FROM fixture;
     INSERT INTO _coven_unreferenced_files SELECT 'file_' || i FROM fixture;
     INSERT INTO _coven_data_reloads SELECT 'audience_' || i,key FROM fixture;
     INSERT INTO _coven_boundaries SELECT n,'schema',1,audience,device,n,counter(1),1 FROM fixture;
     INSERT INTO _coven_raise_captures SELECT 'audience_' || i,2,n,n FROM fixture;
     INSERT INTO _coven_writes
       SELECT i,CAST(n || device AS BLOB),n,counter(0),n FROM fixture;
     INSERT INTO _coven_device_reset VALUES(1,zeroblob(8),counter(1));
     INSERT INTO _coven_sql_versions SELECT CASE i%2 WHEN 0 THEN 'collation' ELSE 'function' END,'name_' || i,i FROM fixture;
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
     INSERT INTO _coven_user_files SELECT 'file_' || i,CAST('path_' || i AS BLOB),i,zeroblob(13) FROM fixture;
     INSERT INTO _coven_copies(path,temporary,state,size,modified_at)
       SELECT 'path_' || i,'temporary_' || i,CASE i%4 WHEN 0 THEN 'writing' WHEN 1 THEN 'placed' WHEN 2 THEN 'kept' ELSE 'lost' END,
         CASE WHEN i%4!=0 THEN i END,CASE WHEN i%4!=0 THEN zeroblob(13) END FROM fixture;
     INSERT INTO _coven_copy_folders SELECT 'folder_' || (i%7),'path_' || i,i%2 FROM fixture;
     INSERT INTO _coven_file_removals SELECT 'removed_' || i,'files' FROM fixture;
     INSERT INTO _coven_cache_writes SELECT 'writing_' || i FROM fixture;
     INSERT INTO _coven_moves(id,state)
       SELECT i,CASE i%4 WHEN 0 THEN 'moving' WHEN 1 THEN 'failed' WHEN 2 THEN 'cancelled' ELSE 'finished' END FROM fixture;
     INSERT INTO _coven_file_uploads(id,location,device,file,size,hash,copy,move,stored,queued_at)
       SELECT i,'location_' || i,device,substr(key,17,16),1,zeroblob(32),'copy_' || i,i,i%2,zeroblob(13) FROM fixture;
     INSERT INTO _coven_upload_files SELECT 'file_' || i,i,i%2 FROM fixture;
     INSERT INTO _coven_pin_requests SELECT 'file_' || i,CASE WHEN i%2=0 THEN 'path_' || i END FROM fixture;
     INSERT INTO _coven_retries(operation,upload,failures,not_before,wait)
       SELECT CASE WHEN i%2=0 THEN i END,CASE WHEN i%2=1 THEN i END,1,zeroblob(13),1000 FROM fixture;
     INSERT INTO _coven_retries(scope,failures,not_before,wait)
       VALUES('join',1,zeroblob(13),1000),('provider',0,zeroblob(13),600000);
     INSERT INTO _coven_device_files SELECT 'file_' || i,'path_' || i,zeroblob(32) FROM fixture;
     INSERT INTO _coven_copy_chunks SELECT 'path_' || i,0,zeroblob(32) FROM fixture;
     INSERT INTO _coven_file_candidates SELECT device,substr(key,17,16),n FROM fixture;
     INSERT INTO _coven_file_scans SELECT n,n FROM fixture;
     INSERT INTO _coven_upload_pause VALUES(1);
     INSERT INTO _coven_write_pause VALUES(1,1);
     INSERT INTO _coven_file_chunks SELECT device,substr(key,17,16),0,key FROM fixture;
     INSERT INTO _coven_cache(namespace,file_id,chunk,path,size,last_read,pinned,checked_hash)
       SELECT audience,'file_' || i,CASE WHEN (i/64)%4=0 THEN -2 ELSE 0 END,'cache_' || i,
              65536,i,CASE WHEN (i/64)%4=0 THEN 1 ELSE 0 END,
              CASE WHEN (i/64)%4=0 THEN key END FROM fixture;
     INSERT INTO _coven_cache_budgets(namespace,bytes) SELECT 'audience_' || i,counter(1048576) FROM fixture WHERE true ON CONFLICT(namespace) DO UPDATE SET bytes=excluded.bytes;
     INSERT INTO _coven_install_fixture SELECT key,audience,'1',1,counter(1),device,n,0,1,1,x'00',device,n,key,audience,counter(1) FROM fixture;
     INSERT INTO _coven_install_generations SELECT tab,key,audience,counter(1),device,n FROM fixture;
     INSERT INTO _coven_install_parts SELECT CAST(device || n AS BLOB),audience,x'00',i%2 FROM fixture;
     INSERT INTO _coven_install_authored SELECT CAST(device || n AS BLOB),audience,tab,key,x'00',NULL FROM fixture;
     INSERT INTO _coven_install_before SELECT tab,key,audience,i%2 FROM fixture;
     INSERT INTO _coven_install_region SELECT tab,key,audience FROM fixture WHERE i%16=0;
     INSERT INTO _coven_download_replaced SELECT DISTINCT audience FROM fixture WHERE i<=4;
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
     INSERT INTO _coven_write_slots SELECT n,i+1 FROM fixture;
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

/// Every internal SQL statement in the crate's sources, as the plan checks
/// explain them.
mod source {
    use super::Statement;
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
                if !matches!(
                    keyword.as_str(),
                    "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "WITH" | "CREATE"
                ) {
                    continue;
                }
                for sql in expand(db, file, *line, &sql, source) {
                    // App schema introspection and app-defined tables do not use the
                    // internal schema's indexes. Temporary staging is covered when
                    // the same statement also reads a persistent internal table.
                    if !sql
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
                "object",
                "file",
                "copy",
                "account",
                "access_key_id",
                "upload",
                "chunk",
                "path",
                "id",
            ]
            .iter()
            .enumerate()
            {
                for row in ["OLD", "NEW"] {
                    body = body.replace(&format!("{row}.{column}"), &format!("?{}", index + 1));
                }
            }
            // RAISE() runs only inside a trigger; its lookups are explained
            // with a plain value in its place.
            while let Some(start) = body.find("RAISE(") {
                let end = start + body[start..].find(')').expect("RAISE closes") + 1;
                body.replace_range(start..end, "NULL");
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
            .prepare(
                "SELECT name FROM sqlite_schema WHERE type IN ('view','table') AND name GLOB 'coven_*'",
            )
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
            "CREATE TEMP TABLE _coven_snapshot_columns",
        ) + &select_constant(
            &sources["snapshot_view.rs"].strings,
            "CREATE TEMP TABLE _coven_snapshot_losses",
        ) + &select_constant(
            &sources["snapshot_install.rs"].strings,
            "CREATE TEMP TABLE _coven_install_generations",
        ) + &select_constant(
            &sources["download_stage.rs"].strings,
            "CREATE TEMP TABLE _coven_download_rows",
        );
        // One synced table's install table, laid out as `TableLayout` lays
        // out every one: a column of values, setters and parents each.
        sql.push_str(
            "CREATE TEMP TABLE _coven_install_fixture(coven_key,coven_audience,coven_synced,coven_merged,
               coven_generation,coven_device,coven_number,coven_earlier,coven_keyed,coven_present,
               coven_v0,coven_d0,coven_n0,coven_k0,coven_a0,coven_g0,UNIQUE(coven_key,coven_audience));",
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
        if file == "rowid_policy.rs"
            && normalize(sql) == "CREATE TEMP TRIGGER {} BEFORE UPDATE ON {}.{} WHEN old.{alias} IS NOT new.{alias} BEGIN SELECT _coven_rowid_changed(); END"
        {
            // The guard compares an app row's addresses and calls a function;
            // it reads no persistent internal table or index.
            return Vec::new();
        }
        if matches!(
            file,
            "snapshot_install.rs"
                | "snapshot_conflicts.rs"
                | "snapshot_materialize.rs"
                | "snapshot_files.rs"
        ) {
            // A synced table's install table, as the fixture lays out one.
            let expanded = [
                ("{installed}", "temp._coven_install_fixture"),
                ("{key}", "coven_key"),
                ("{audience_column}", "coven_audience"),
                ("{generation}", "coven_generation"),
                ("{device}", "coven_device"),
                ("{number}", "coven_number"),
                ("{merged}", "coven_merged"),
                ("{present}", "coven_present"),
                ("{synced}", "coven_synced"),
                ("{setter_device}", "coven_d0"),
                ("{setter_number}", "coven_n0"),
                ("{parent_key}", "coven_k0"),
                ("{parent_audience}", "coven_a0"),
                ("{parent_generation}", "coven_g0"),
                ("{value}", "coven_v0"),
                ("{column}", "coven_v0"),
                (
                    "{clean}",
                    "i.coven_present=1 AND NOT EXISTS(SELECT 1 FROM temp._coven_install_region g WHERE g.table_name=?1 AND g.key=i.coven_key AND g.audience=i.coven_audience)",
                ),
            ]
            .into_iter()
            .fold(sql.to_owned(), |expanded, (name, value)| {
                expanded.replace(name, value)
            });
            if !expanded.contains('{') {
                return vec![expanded];
            }
            // The others read the install's own temporary tables and the
            // app's.
            assert!(
                !sql.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                    .any(|word| word.starts_with("_coven_") && !word.starts_with("_coven_install_")),
                "internal SQL builder: {sql}"
            );
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
                "change_capture.rs"
                    | "database_file_retention.rs"
                    | "derived.rs"
                    | "file_database.rs"
                    | "file_write_copies.rs"
                    | "migration_run.rs"
                    | "migration_snapshot.rs"
                    | "removal.rs"
                    | "removal_schema.rs"
                    | "removal_sql.rs"
                    | "removal_view.rs"
                    | "scope_term.rs"
                    | "scope_values.rs"
                    | "snapshot_layout.rs"
                    | "snapshot_load.rs"
                    | "snapshot_preparation.rs"
                    | "snapshot_staging.rs"
                    | "sql_versions.rs"
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
                "SELECT count(*) FROM (SELECT file,position FROM {from}
                 EXCEPT SELECT file,position FROM {to})"
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
                    // VM counters have no relation name. Only a statement
                    // listed for "database" may walk unnamed entries.
                    if read.get_status(rusqlite::StatementStatus::FullscanStep) != 0
                        && reason(statement, "database").is_none()
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

fn plans(db: &Connection, statement: &Statement) -> Vec<String> {
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

fn unapproved_scan(statement: &Statement, plan: &str) -> bool {
    let Some(scan) = plan.strip_prefix("SCAN ") else {
        return false;
    };
    // Constant rows and materialized compound-query results aren't table reads.
    if scan == "CONSTANT ROW" || scan.starts_with("(subquery-") {
        return false;
    }
    let relation = scan.split_whitespace().next().unwrap();
    reason(statement, relation).is_none()
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
