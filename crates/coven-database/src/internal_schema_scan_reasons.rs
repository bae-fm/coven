//! Full scans the internal query plans may contain, each with its reason.
/// One internal statement as the plan checks explain it, and the reasons
/// name it: its source file and line, its SQL as written, and as run.
pub(super) struct Statement {
    pub file: String,
    pub line: usize,
    pub template: String,
    pub sql: String,
}
/// Exceptions name the exact source statement and scanned relation. A nested
/// lookup in a whole-table enumeration still has to use its own index. Each
/// domain with its own tables names its statements' scans in its tests.
pub(super) fn reason(statement: &Statement, relation: &str) -> Option<&'static str> {
    let (file, template, sql) = (&statement.file, &statement.template, &statement.sql);
    database_reason(statement, relation)
        .or_else(|| crate::store_state_tables::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::published::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::snapshot_install::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::write_commit::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::write_commit_store::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::merge_store::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::file_queue::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::file_moves::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::row_queries::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::write_sources::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::forget_records::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::file_removals::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::file_protection::tests::scan_reason(file, template, sql, relation))
        .or_else(|| {
            crate::database::file_retention::tests::scan_reason(file, template, sql, relation)
        })
        .or_else(|| crate::migration_run::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::pending::tests::scan_reason(file, template, sql, relation))
        .or_else(|| crate::raise_capture::tests::scan_reason(file, template, sql, relation))
}

fn database_reason(statement: &Statement, relation: &str) -> Option<&'static str> {
    let file = statement.file.as_str();
    let sql = statement.template.as_str();
    match (file, sql, relation) {
("coven_uploads", "SELECT * FROM coven_uploads", "l" | "z" | "w") =>
    Some("the view lists every queued file, which its link to its upload enumerates; the pause and setting tables hold at most one row each"),
("coven_files", "SELECT * FROM coven_files", "coven_files") =>
    Some("the table lists every file this device has a record of"),
("file_moves.rs", "SELECT m.id FROM _coven_moves AS m WHERE m.state='moving' AND NOT EXISTS(SELECT 1 FROM _coven_file_uploads AS u WHERE u.move=m.id AND u.stored=0 AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id))", "m") =>
    Some("the moves still moving, each with its queued uploads; a finished, failed or cancelled move is in no index entry"),
("file_pins.rs", "SELECT id,file,copy FROM _coven_pin_requests ORDER BY file", "_coven_pin_requests") =>
    Some("the pins coven keeps, each until it is made: the work the upload worker does"),
("coven_user_files", "SELECT * FROM coven_user_files", "u") =>
    Some("the view lists every user original this device records"),
("coven_sending", "SELECT * FROM coven_sending", "j") =>
    Some("the files being sent, one per upload in flight, from this process's memory"),
("coven_downloading", "SELECT * FROM coven_downloading", "j") =>
    Some("the files being downloaded to keep whole, one per download in flight, from this process's memory"),
("coven_upload_pause", "SELECT * FROM coven_upload_pause", "one" | "z") =>
    Some("one constant row, and the pause table holds at most one row"),
("coven_network", "SELECT * FROM coven_network", "one" | "w") =>
    Some("one constant row, and the setting's table holds at most one row"),
("coven_pins", "SELECT * FROM coven_pins", "coven_pins") =>
    Some("the table lists every pinned file"),
("coven_pin_requests", "SELECT * FROM coven_pin_requests", "coven_pin_requests") =>
    Some("the table lists every file with a pin request"),
("coven_moves", "SELECT * FROM coven_moves", "coven_moves") =>
    Some("the table lists every file of every moving or failed move"),
("coven_upload_queue", "SELECT * FROM coven_upload_queue", "coven_upload_queue") =>
    Some("the table lists every queued file"),
("coven_sync_status", "SELECT * FROM coven_sync_status", "coven_sync_status")
| ("coven_retry_effect", "SELECT * FROM coven_retry_effect", "coven_retry_effect") =>
    Some("the table holds one row"),
("coven_pending", "SELECT * FROM coven_pending", "e") =>
    Some("the view lists every pending record"),
("database_local_bytes.rs", "SELECT namespace,bytes,cached,pinned FROM _coven_cache_budgets ORDER BY namespace", "_coven_cache_budgets") =>
    Some("every namespace's budget and counters, one row per namespace the app declared"),
("database_local_bytes.rs", "SELECT coalesce(sum(size),0) FROM _coven_cache_partials", "_coven_cache_partials") =>
    Some("the sum over every partial download, which is the figure asked for"),
("operation.rs", "SELECT operation FROM _coven_app_operations WHERE outcome IS NULL ORDER BY operation", "_coven_app_operations") =>
    Some("only once this device's removal applies, ending every operation the app's writes journaled that still runs"),
("operation.rs", "SELECT EXISTS(SELECT 1 FROM _coven_held_keys)", "_coven_held_keys") =>
    Some("an unfiltered EXISTS stops after its first row"),
("coven_operations", "SELECT * FROM coven_operations", "a") =>
    Some("the view lists every operation the app's writes journaled, until the app acknowledges it"),
("coven_members", "SELECT * FROM coven_members", "m") =>
    Some("the view lists every member, former ones included"),
("coven_devices", "SELECT * FROM coven_devices", "d") =>
    Some("the view lists every device"),
("coven_circle_members", "SELECT * FROM coven_circle_members", "m" | "s") =>
    Some("the view lists every circle's members, reached from the memberships or from the members they name"),
("database_sync.rs", "SELECT slot,record FROM _coven_slot_records ORDER BY slot", "_coven_slot_records") =>
    Some("opening reconstructs authenticated administrative history once; passes retain the result"),
("members.rs", "SELECT member,signing,sealing,role,removed,access FROM _coven_members WHERE removed=0 ORDER BY member", "_coven_members") =>
    Some("the member list returns every current member"),
("download.rs", "SELECT (SELECT publication FROM _coven_snapshot_schema WHERE EXISTS(SELECT 1 FROM _coven_members))", "_coven_snapshot_schema" | "_coven_members") =>
    Some("the schema row is singular; unfiltered membership EXISTS stops after its first row"),
("file_queue.rs", "SELECT u.id,u.location,u.size,u.hash,u.queued_at,u.session,u.bytes_confirmed, u.bytes_total,r.failures,r.not_before,r.wait,u.move,u.stored,u.holds FROM _coven_file_uploads AS u LEFT JOIN _coven_retries AS r ON r.upload=u.id WHERE EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id) ORDER BY u.id", "u") =>
    Some("the file queue reader returns every upload a file waits for"),
("file_queue.rs", "SELECT u.id,u.location,u.size,u.hash,u.queued_at,u.session,u.bytes_confirmed, u.bytes_total,r.failures,r.not_before,r.wait,u.move,u.stored,u.holds FROM _coven_file_uploads AS u LEFT JOIN _coven_retries AS r ON r.upload=u.id ORDER BY u.id", "u") =>
    Some("tests read every queued upload, those no file waits for included"),
("file_queue.rs", "SELECT u.id,u.location FROM _coven_file_uploads AS u WHERE u.session IS NULL AND NOT EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id) ORDER BY u.id", "u") =>
    Some("each upload worker run retires every upload no file waits for and no session holds"),
("file_queue.rs", "SELECT u.id,u.location,u.size,u.hash,u.queued_at,u.session,u.bytes_confirmed, u.bytes_total,r.failures,r.not_before,r.wait,u.move,u.stored,u.holds FROM _coven_file_uploads AS u LEFT JOIN _coven_retries AS r ON r.upload=u.id WHERE u.session IS NOT NULL AND NOT EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id) ORDER BY u.id", "u") =>
    Some("each upload worker run aborts the session of every upload no file waits for"),
("file_table.rs", "SELECT table_name,column_name,write_once FROM _coven_file_columns ORDER BY table_name,column_name", "_coven_file_columns") =>
    Some("opening and each migration compare every registered file column with the declarations"),
("file_write_uploads.rs", "SELECT file FROM _coven_unreferenced_files ORDER BY file", "_coven_unreferenced_files") =>
    Some("files other devices' writes left for this device's next write, which concurrent removals leave; normally none"),
("file_database.rs", "SELECT EXISTS(SELECT 1 FROM _coven_unreferenced_files) AND NOT EXISTS(SELECT 1 FROM _coven_write_pause)", "_coven_unreferenced_files" | "_coven_write_pause") =>
    Some("unfiltered EXISTS stops at the first row; the pause table holds at most one row"),
("file_queue.rs", "SELECT (SELECT count(*) FROM _coven_uploads),(SELECT count(*) FROM _coven_file_uploads)", "_coven_uploads" | "_coven_file_uploads") =>
    Some("deleting a store counts every waiting write and file, once"),
("sync_facts.rs", "SELECT u.id,u.stored,u.holds IS NOT NULL,r.not_before,e.id FROM _coven_file_uploads AS u LEFT JOIN _coven_retries AS r ON r.upload=u.id LEFT JOIN _coven_pending AS e ON e.work='upload' AND e.device=u.device AND e.object=u.object WHERE u.id IN ( SELECT f.id FROM _coven_pending AS p JOIN _coven_file_uploads AS f ON f.device=p.device AND f.object=p.object WHERE p.work='upload' UNION SELECT MIN(position) FROM coven_sending UNION SELECT * FROM ( SELECT f.id FROM _coven_file_uploads AS f WHERE NOT f.stored AND f.id NOT IN (SELECT position FROM coven_sending) AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=f.id) AND NOT EXISTS(SELECT 1 FROM _coven_pending AS p WHERE p.work='upload' AND p.device=f.device AND p.object=f.object) ORDER BY f.id LIMIT 1)) AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id) ORDER BY u.id", "p") =>
    Some("the pending records with an object, which the sync state lists with their uploads"),
("sync_facts.rs", "SELECT u.id,u.stored,u.holds IS NOT NULL,r.not_before,e.id FROM _coven_file_uploads AS u LEFT JOIN _coven_retries AS r ON r.upload=u.id LEFT JOIN _coven_pending AS e ON e.work='upload' AND e.device=u.device AND e.object=u.object WHERE u.id IN ( SELECT f.id FROM _coven_pending AS p JOIN _coven_file_uploads AS f ON f.device=p.device AND f.object=p.object WHERE p.work='upload' UNION SELECT MIN(position) FROM coven_sending UNION SELECT * FROM ( SELECT f.id FROM _coven_file_uploads AS f WHERE NOT f.stored AND f.id NOT IN (SELECT position FROM coven_sending) AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=f.id) AND NOT EXISTS(SELECT 1 FROM _coven_pending AS p WHERE p.work='upload' AND p.device=f.device AND p.object=f.object) ORDER BY f.id LIMIT 1)) AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id) ORDER BY u.id", "f") =>
    Some("the walk to the first queued upload with no record, in id order over the unstored ones, stops at its first match"),
("sync_facts.rs", "SELECT u.id,u.stored,u.holds IS NOT NULL,r.not_before,e.id FROM _coven_file_uploads AS u LEFT JOIN _coven_retries AS r ON r.upload=u.id LEFT JOIN _coven_pending AS e ON e.work='upload' AND e.device=u.device AND e.object=u.object WHERE u.id IN ( SELECT f.id FROM _coven_pending AS p JOIN _coven_file_uploads AS f ON f.device=p.device AND f.object=p.object WHERE p.work='upload' UNION SELECT MIN(position) FROM coven_sending UNION SELECT * FROM ( SELECT f.id FROM _coven_file_uploads AS f WHERE NOT f.stored AND f.id NOT IN (SELECT position FROM coven_sending) AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=f.id) AND NOT EXISTS(SELECT 1 FROM _coven_pending AS p WHERE p.work='upload' AND p.device=f.device AND p.object=f.object) ORDER BY f.id LIMIT 1)) AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id) ORDER BY u.id", "j") =>
    Some("the files being sent, one per upload in flight, from this process's memory"),
("sync_facts.rs", "SELECT EXISTS(SELECT 1 FROM _coven_file_uploads AS u WHERE EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id))", "u") =>
    Some("unfiltered EXISTS stops at the first upload a file waits for"),
("sync_facts.rs", "SELECT EXISTS(SELECT 1 FROM _coven_uploads)", "_coven_uploads") =>
    Some("unfiltered EXISTS stops at the first row"),
("file_queue.rs", "SELECT EXISTS(SELECT 1 FROM _coven_upload_pause)", "_coven_upload_pause") =>
    Some("the pause table holds at most one row"),
("file_queue.rs", "SELECT EXISTS(SELECT 1 FROM _coven_unmetered_only)", "_coven_unmetered_only") =>
    Some("the setting's table holds at most one row"),
("retry.rs", "SELECT scope,operation,upload,pin,failures,not_before,wait FROM _coven_retries", "_coven_retries") =>
    Some("reopening arms every persisted retry wait"),
("database_operations.rs", "SELECT device FROM _coven_requests_ended", "_coven_requests_ended") =>
    Some("each pass reads every device gone for good whose join requests this device ended, once"),
("database_operations.rs", "SELECT device FROM _coven_gone_storage_cleared", "_coven_gone_storage_cleared") =>
    Some("a pass that clears storage for devices gone for good reads every one it cleared"),
("operation.rs", "SELECT id,kind,last_step,data,started_by FROM _coven_operations ORDER BY id", "_coven_operations") =>
    Some("operations() returns every unfinished journal entry"),
("operation.rs", "SELECT id FROM _coven_operations WHERE kind=?1 OR started_by=?1 ORDER BY id", "_coven_operations") =>
    Some("the transaction that stops this device for good ends every invite and every operation an invite started, among the unfinished journal entries"),
("snapshot_conflicts.rs", _, "i" | "b") =>
    Some("a reload visits each row it installed, or had before, once; each lookup of the device's own records is indexed"),
("snapshot_files.rs", _, "c" | "p" | "d" | "temp._coven_install_stored" | "temp._coven_install_references") =>
    Some("a reload visits each row it changed once, and each file reference and stored file those rows hold; each lookup of the device's own records is indexed"),
("snapshot_conflicts.rs", sql, "_coven_lost")
    if sql.contains("FROM _coven_lost WHERE frozen=0 AND replacement_kind='rules'") =>
    Some("a reload visits every row the rules take out, through the index of rules losses: its snapshots' authors took some out, and a replaced row can bring back any other"),
("sqlite.rs", "{} ORDER BY l.id", "l") =>
    Some("the public losses reader returns all losses"),
("upload.rs", "SELECT u.rowid,u.device,u.number,k.sealing_keys FROM _coven_uploads u LEFT JOIN _coven_upload_keys k ON k.device=u.device AND k.number=u.number ORDER BY u.rowid LIMIT 1", "u") =>
    Some("rowid order with LIMIT 1 reads only the oldest queued write"),
("upload.rs", "SELECT rowid,device,number,NULL FROM _coven_uploads ORDER BY rowid", "_coven_uploads") =>
    Some("the queue reader lists every waiting write in insertion order"),
("migration_snapshot.rs", "SELECT DISTINCT lower(table_name) FROM _coven_columns WHERE synced=1", "_coven_columns") =>
    Some("once per migration: discover every table with synced column metadata"),
("migration_receive.rs", "SELECT version,before_schema,after_schema,tables,columns FROM _coven_conversion_schemas ORDER BY version", "_coven_conversion_schemas") =>
    Some("opening retains every breaking migration's conversion schema"),
("migration_hidden.rs", "SELECT table_name,key,audience FROM _coven_lost WHERE frozen=0 AND replacement_kind='rules'", "_coven_lost") =>
    Some("once per migration: read every hidden row before its identity moves"),
("migration_state.rs", "SELECT id,table_name,column_name FROM _coven_columns WHERE synced=1", "_coven_columns") =>
    Some("once per migration: carry every synced column definition through renames and drops"),
("migration_state.rs", "SELECT id,table_name,identity FROM _coven_foreign_keys", "_coven_foreign_keys") =>
    Some("once per migration: carry all foreign-key definitions through schema changes"),
("migration_state.rs", "SELECT id,table_name,identity FROM _coven_constraints", "_coven_constraints") =>
    Some("once per migration: carry all unique constraints through schema changes"),
("reload_gate.rs", "SELECT audience FROM _coven_data_reloads", "_coven_data_reloads") =>
    Some("sync schedules every audience awaiting replacement data"),
("sequence.rs", "SELECT device,number,digest FROM _coven_last_claims ORDER BY device", "_coven_last_claims") =>
    Some("snapshot coverage records each device's last consumed claim"),
("merge_store.rs", "SELECT timestamp,number FROM _coven_writes INDEXED BY sqlite_autoindex__coven_writes_1 WHERE (slot<=?1 OR ({WRITE_DEVICE_SQL}=?2 AND number<?3)) ORDER BY timestamp DESC LIMIT 1", "_coven_writes") =>
    Some("the timestamp index walks from the newest write down and stops at the first one the applied write had read"),
("snapshot_write.rs", "SELECT timestamp,number,read_through,slot FROM _coven_writes WHERE slot<=?1 ORDER BY {WRITE_DEVICE_SQL},number", "_coven_writes") =>
    Some("the snapshot emits the counted write history in canonical identity order"),
("write_boundary.rs", "SELECT b.kind,b.version,b.audience,b.snapshot_device,b.snapshot_number,b.slot,b.included,b.applied FROM _coven_boundaries b ORDER BY b.slot", "b") =>
    Some("boundary selection folds each retained raise and reset in slot order"),
("sqlite_settings.rs", "SELECT count(*) FROM (SELECT 1 FROM {table} LIMIT ?1)", _) =>
    Some("reads at most SMALL_TABLE rows of each table PRAGMA optimize names, to tell whether gathering would keep statistics for it"),
_ => None,
    }
}

#[cfg(test)]
#[path = "internal_schema_scan_reasons_tests.rs"]
mod tests;
