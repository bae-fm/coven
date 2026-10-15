//! Full scans the internal query plans may contain, each with its reason.
use super::source::Statement;
/// Exceptions name the exact source statement and scanned relation. A nested
/// lookup in a whole-table enumeration still has to use its own index.
pub(super) fn reason(statement: &Statement, relation: &str) -> Option<&'static str> {
    let file = statement.file.as_str();
    let sql = statement.template.as_str();
    match (file, sql, relation) {
("database_positions.rs", "SELECT device,observation FROM _coven_peer_positions", "_coven_peer_positions") =>
    Some("one completed positions scan compares every retained peer identity"),
("database_sync.rs", "SELECT EXISTS(SELECT 1 FROM _coven_data_reloads)", "_coven_data_reloads") =>
    Some("publication stops at the first unfinished audience reload"),
("database_file_retention.rs", "SELECT device,file FROM _coven_row_file_refs UNION SELECT device,file FROM _coven_input_file_refs UNION SELECT device,file FROM _coven_queued_file_refs UNION SELECT device,file FROM _coven_file_uploads", "_coven_row_file_refs" | "_coven_input_file_refs" | "_coven_queued_file_refs" | "_coven_file_uploads") =>
    Some("file retention returns every protected fixed path from the sparse indexes"),
("pending.rs", "INSERT INTO _coven_pending_resets(subject,reporter,entry_device,entry_number) SELECT p.subject,p.reporter,?1,?2 FROM _coven_pending AS p WHERE NOT EXISTS(SELECT 1 FROM _coven_access_keys AS k WHERE k.operation=p.operation) AND NOT EXISTS(SELECT 1 FROM _coven_provider_access AS a WHERE a.operation=p.operation) ON CONFLICT DO NOTHING", "p") =>
    Some("a newly applied reset suppresses earlier observations except unresolved provider access"),
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
("members.rs", "SELECT member,role,signing,sealing FROM _coven_members WHERE removed=0", "_coven_members") =>
    Some("the member list returns every current member"),
("write_landing.rs", "SELECT device,number,stored_at,header,signer,late FROM _coven_write_landings", "_coven_write_landings") =>
    Some("replay, release and retention check every verdict a retirement can still change"),
("database_file_retention.rs", sql, "_coven_file_scans") if sql.starts_with("DELETE FROM _coven_file_scans WHERE NOT EXISTS(") =>
    Some("replay checks every completed file scan against the entries it keeps"),
("retirement_drain.rs", "SELECT device FROM _coven_retirement_finality", "_coven_retirement_finality") =>
    Some("finality recording skips every retired device that already has its F"),
("snapshot_witnesses.rs", "SELECT audience,device,number,landed FROM _coven_snapshot_witnesses ORDER BY audience,device,number", "_coven_snapshot_witnesses") =>
    Some("retention reads every audience's witness staircase, bounded by the snapshots of the last day"),
("retirement_drain.rs", "SELECT device FROM _coven_drained_devices", "_coven_drained_devices") =>
    Some("discovery skips every drained device"),
("retirement_drain.rs", sql, "f") if sql.starts_with("SELECT f.device,f.entry_device,f.entry_number,f.final_at FROM _coven_retirement_finality f") =>
    Some("each pass checks every retired device still waiting for its drain"),
("retirement_drain.rs", sql, "_coven_retirement_finality") if sql.starts_with("DELETE FROM _coven_retirement_finality WHERE NOT EXISTS(") =>
    Some("replay checks every drain basis against the entries it keeps"),
("entry_effects.rs", "SELECT audience FROM _coven_data_reloads", "_coven_data_reloads") =>
    Some("sync and replay read every audience awaiting replacement data"),
("sqlite.rs", "PRAGMA integrity_check", "database") =>
    Some("corruption detection checks every stored row and index"),
("download.rs", "SELECT device,number FROM _coven_positions ORDER BY device", "_coven_positions") =>
    Some("positions() returns every device's consumed position"),
("store_log.rs" | "snapshot_write.rs", "SELECT device,max(number) FROM _coven_store_log GROUP BY device ORDER BY device", "_coven_store_log") =>
    Some("compute the complete store-log frontier for all devices"),
("database_snapshots.rs", "SELECT device,number FROM _coven_uploads ORDER BY device,number", "_coven_uploads") =>
    Some("snapshot coverage includes every waiting write"),
("database_sync.rs", "SELECT EXISTS(SELECT 1 FROM _coven_uploads)", "_coven_uploads") =>
    Some("unfiltered EXISTS stops after the first queue row"),
("download.rs", "SELECT (SELECT publication FROM _coven_snapshot_schema WHERE EXISTS(SELECT 1 FROM _coven_members))", "_coven_snapshot_schema" | "_coven_members") =>
    Some("the schema row is singular; unfiltered membership EXISTS stops after its first row"),
("file_queue.rs", "SELECT u.id,u.reference,u.queued_at,u.session,r.failures,r.not_before,r.wait FROM _coven_file_uploads AS u LEFT JOIN _coven_retries AS r ON r.upload=u.id ORDER BY u.id", "u") =>
    Some("the file queue reader returns every queued attachment"),
("cache_progress.rs", "INSERT INTO _coven_eager_cancel(cancelled) SELECT 1 WHERE EXISTS(SELECT 1 FROM _coven_eager_requests) ON CONFLICT DO NOTHING", "_coven_eager_requests") =>
    Some("unfiltered EXISTS stops after the first requested file"),
("cache_progress.rs", "SELECT EXISTS(SELECT 1 FROM _coven_eager_cancel)", "_coven_eager_cancel") =>
    Some("the cancellation table holds at most one row"),
("eager_work.rs", "SELECT reference FROM _coven_eager_work ORDER BY namespace,file_id", "_coven_eager_work") =>
    Some("the eager fill reads every pending changed reference"),
("cache_progress.rs", "SELECT reference FROM _coven_eager_requests ORDER BY namespace,file_id", "_coven_eager_requests") =>
    Some("eager progress covers the whole requested set"),
("file_queue.rs", "SELECT EXISTS(SELECT 1 FROM _coven_upload_pause)", "_coven_upload_pause") =>
    Some("the pause table holds at most one row"),
("retry.rs", "SELECT scope,operation,upload,download,failures,not_before,wait FROM _coven_retries", "_coven_retries") =>
    Some("reopening arms every persisted retry wait"),
("file_removals.rs", "SELECT path,area FROM _coven_file_removals ORDER BY area,path", "_coven_file_removals") =>
    Some("file removal visits every queued filesystem deletion"),
("file_removals.rs", "SELECT EXISTS(SELECT 1 FROM _coven_cache_writes)", "_coven_cache_writes") =>
    Some("opening stops at the first cache range recorded as being written"),
("file_removals.rs", "INSERT INTO _coven_file_removals(area,path) SELECT 'cache',path FROM _coven_cache_writes", "_coven_cache_writes") =>
    Some("opening makes every abandoned cache range write a removal"),
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
("snapshot_state.rs", "SELECT EXISTS(SELECT 1 FROM temp._coven_snapshot_values v WHERE matched=0 OR (matched=2) != EXISTS(SELECT 1 FROM _coven_lost l WHERE l.table_name=v.table_name AND l.key=v.key AND l.audience=v.audience AND l.frozen=0 AND l.replacement_kind='rules'))", "v") =>
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
("store_log_tables.rs", "SELECT member,signing,sealing,role,removed,access FROM _coven_members", "_coven_members") =>
    Some("store_log() returns the complete member map"),
("store_log_tables.rs", "SELECT signing,sealing FROM _coven_bound_keys", "_coven_bound_keys") =>
    Some("store_log() returns every pair replay bound"),
("store_log_tables.rs", "SELECT device,member,name,state FROM _coven_devices", "_coven_devices") =>
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
("raise_capture.rs", "DELETE FROM _coven_raise_captures", "_coven_raise_captures") =>
    Some("once per breaking migration: replace the previous unpublished capture"),
("raise_capture.rs", "SELECT audience,version,through,writes FROM _coven_raise_captures ORDER BY audience", "_coven_raise_captures") =>
    Some("one capture per readable audience; scheduling lists them all"),
("migration_snapshot.rs", "SELECT DISTINCT lower(table_name) FROM _coven_columns", "_coven_columns") =>
    Some("once per migration: discover every table with retained column metadata"),
("migration_receive.rs", "SELECT version,before_schema,after_schema,tables,columns FROM _coven_conversion_schemas ORDER BY version", "_coven_conversion_schemas") =>
    Some("opening retains every breaking migration's conversion schema"),
("migration_hidden.rs", "SELECT id,table_name,key,audience FROM _coven_lost WHERE frozen=0 AND replacement_kind='rules'", "_coven_lost") =>
    Some("once per migration: read every hidden row before its identity moves"),
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
("migration_state.rs", "DELETE FROM _coven_lost WHERE table_name=?1 AND frozen=0", "_coven_lost") =>
    Some("once per migration: remove active losses of a dropped app table"),
("migration_state.rs", "DELETE FROM _coven_lost WHERE column_id=?1 AND frozen=0 AND replacement_kind='write'"
    | "SELECT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=?1 AND frozen=1)"
    | "UPDATE _coven_lost SET column_id=?2 WHERE column_id=?1 AND frozen=0 AND replacement_kind='write'", "_coven_lost") =>
    Some("once per migration: separate active and frozen losses when moving columns"),
_ => None,
    }
}
