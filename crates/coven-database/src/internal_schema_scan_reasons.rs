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
/// lookup in a whole-table enumeration still has to use its own index.
pub(super) fn reason(statement: &Statement, relation: &str) -> Option<&'static str> {
    let file = statement.file.as_str();
    let sql = statement.template.as_str();
    match (file, sql, relation) {
("write_commit.rs", "INSERT INTO _coven_cells(column_id,row_id,write_id) VALUES{values} ON CONFLICT(column_id,row_id) DO UPDATE SET write_id=excluded.write_id", "2-ROW") =>
    Some("a row's cells are the statement's own values"),
("coven_uploads", "SELECT * FROM coven_uploads", "l" | "z") =>
    Some("the view lists every queued file, which its link to its upload enumerates; the pause table holds at most one row"),
("coven_files", "SELECT * FROM coven_files", "coven_files") =>
    Some("the table lists every file this device has a record of"),
("file_moves.rs", "SELECT m.id FROM _coven_moves AS m WHERE m.state='moving' AND NOT EXISTS(SELECT 1 FROM _coven_file_uploads AS u WHERE u.move=m.id AND u.stored=0 AND EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id))", "m") =>
    Some("the moves still moving, each with its queued uploads; a finished, failed or cancelled move is in no index entry"),
("snapshot_load.rs", "UPDATE _coven_file_candidates SET since=?1", "_coven_file_candidates") =>
    Some("a reload skips the slots it replaces, so every candidate counts from its target"),
("file_pins.rs", "SELECT file,copy FROM _coven_pin_requests ORDER BY file", "_coven_pin_requests") =>
    Some("the pins coven keeps, each until it is made: the work the upload worker does"),
("coven_user_files", "SELECT * FROM coven_user_files", "u") =>
    Some("the view lists every user original this device records"),
("coven_sending", "SELECT * FROM coven_sending", "j")
| ("file_queue.rs", "SELECT file FROM coven_sending", "j") =>
    Some("the files being sent, one per upload in flight, from this process's memory"),
("coven_downloading", "SELECT * FROM coven_downloading", "j") =>
    Some("the files being downloaded to keep whole, one per download in flight, from this process's memory"),
("coven_upload_pause", "SELECT * FROM coven_upload_pause", "one" | "z") =>
    Some("one constant row, and the pause table holds at most one row"),
("coven_pins", "SELECT * FROM coven_pins", "coven_pins") =>
    Some("the table lists every pinned file"),
("coven_upload_queue", "SELECT * FROM coven_upload_queue", "coven_upload_queue") =>
    Some("the table lists every queued file"),
("coven_pending", "SELECT * FROM coven_pending", "e") =>
    Some("the view lists every pending record"),
("coven_members", "SELECT * FROM coven_members", "m") =>
    Some("the view lists every member, former ones included"),
("coven_devices", "SELECT * FROM coven_devices", "d") =>
    Some("the view lists every device"),
("coven_circle_members", "SELECT * FROM coven_circle_members", "m" | "s") =>
    Some("the view lists every circle's members, reached from the memberships or from the members they name"),
("database_sync.rs", "SELECT slot,record FROM _coven_slot_records ORDER BY slot", "_coven_slot_records") =>
    Some("opening reconstructs authenticated administrative history once; passes retain the result"),
("sql_versions.rs", "SELECT kind,name,version FROM _coven_sql_versions WHERE kind IN ('collation','function')", "_coven_sql_versions") =>
    Some("opening compares every recorded version with the app's registered ones"),
("members.rs", "SELECT member,role,signing,sealing,access FROM _coven_members WHERE removed=0 ORDER BY member", "_coven_members") =>
    Some("the member list returns every current member"),
("download.rs", "SELECT (SELECT publication FROM _coven_snapshot_schema WHERE EXISTS(SELECT 1 FROM _coven_members))", "_coven_snapshot_schema" | "_coven_members") =>
    Some("the schema row is singular; unfiltered membership EXISTS stops after its first row"),
("file_queue.rs", "SELECT u.id,u.location,u.size,u.hash,u.queued_at,u.session,u.bytes_confirmed, u.bytes_total,r.failures,r.not_before,r.wait,u.move,u.stored,u.holds FROM _coven_file_uploads AS u LEFT JOIN _coven_retries AS r ON r.upload=u.id WHERE EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id) ORDER BY u.id", "u") =>
    Some("the file queue reader returns every upload a file waits for"),
("file_queue.rs", "SELECT u.id,u.location,u.size,u.hash,u.queued_at,u.session,u.bytes_confirmed, u.bytes_total,r.failures,r.not_before,r.wait,u.move,u.stored,u.holds FROM _coven_file_uploads AS u LEFT JOIN _coven_retries AS r ON r.upload=u.id ORDER BY u.id", "u") =>
    Some("tests read every queued upload, those no file waits for included"),
("file_queue.rs", "SELECT u.id,u.location FROM _coven_file_uploads AS u WHERE NOT EXISTS(SELECT 1 FROM _coven_upload_files AS l WHERE l.upload=u.id) ORDER BY u.id", "u") =>
    Some("each upload worker run retires every upload no file waits for"),
("file_table.rs", "SELECT table_name,column_name,write_once FROM _coven_file_columns ORDER BY table_name,column_name", "_coven_file_columns") =>
    Some("opening and each migration compare every registered file column with the declarations"),
("file_protection.rs", "SELECT DISTINCT table_name,key FROM _coven_file_references", "_coven_file_references") =>
    Some("once per migration: rebuild every referencing row's file references"),
("file_write.rs", "DELETE FROM _coven_unreferenced_files WHERE EXISTS( SELECT 1 FROM _coven_file_references AS r WHERE r.file=_coven_unreferenced_files.file) OR NOT EXISTS(SELECT 1 FROM main.coven_files AS f WHERE f.id=_coven_unreferenced_files.file)", "_coven_unreferenced_files")
| ("file_write_uploads.rs", "SELECT file FROM _coven_unreferenced_files ORDER BY file", "_coven_unreferenced_files")=>
    Some("files other devices' writes left for this device's next write, which concurrent removals leave; normally none"),
("file_database.rs", "SELECT EXISTS(SELECT 1 FROM _coven_unreferenced_files) AND NOT EXISTS(SELECT 1 FROM _coven_write_pause)", "_coven_unreferenced_files" | "_coven_write_pause") =>
    Some("unfiltered EXISTS stops at the first row; the pause table holds at most one row"),
("file_queue.rs", "SELECT (SELECT count(*) FROM _coven_uploads),(SELECT count(*) FROM _coven_file_uploads)", "_coven_uploads" | "_coven_file_uploads") =>
    Some("deleting a store counts every waiting write and file, once"),
("file_queue.rs", "SELECT EXISTS(SELECT 1 FROM _coven_upload_pause)", "_coven_upload_pause") =>
    Some("the pause table holds at most one row"),
("retry.rs", "SELECT scope,operation,upload,pin,failures,not_before,wait FROM _coven_retries", "_coven_retries") =>
    Some("reopening arms every persisted retry wait"),
("file_removals.rs", "SELECT path,area FROM _coven_file_removals ORDER BY area,path", "_coven_file_removals") =>
    Some("file removal visits every queued filesystem deletion"),
("file_removals.rs", "SELECT path,temporary,state,size,modified_at FROM _coven_copies WHERE state IN ('writing','placed','lost') ORDER BY path", "_coven_copies") =>
    Some("the copies no call is writing that a lost switch or a failed, dropped or crashed call left; normally none"),
("file_removals.rs", "SELECT EXISTS(SELECT 1 FROM _coven_cache_writes)", "_coven_cache_writes") =>
    Some("opening stops at the first cache range recorded as being written"),
("file_removals.rs", "INSERT INTO _coven_file_removals(area,path) SELECT 'cache',path FROM _coven_cache_writes", "_coven_cache_writes") =>
    Some("opening makes every abandoned cache range write a removal"),
("operation.rs", "SELECT id,kind,last_step,data,started_by FROM _coven_operations ORDER BY id", "_coven_operations") =>
    Some("operations() returns every unfinished journal entry"),
("snapshot_conflicts.rs" | "snapshot_materialize.rs", _, "i" | "b") =>
    Some("a reload visits each row it installed, or had before, once; each lookup of the device's own records is indexed"),
("snapshot_files.rs", _, "c" | "p" | "d" | "temp._coven_install_stored" | "temp._coven_install_references") =>
    Some("a reload visits each row it changed once, and each file reference and stored file those rows hold; each lookup of the device's own records is indexed"),
("snapshot_conflicts.rs", sql, "_coven_lost")
    if sql.contains("FROM _coven_lost WHERE frozen=0 AND replacement_kind='rules'") =>
    Some("a reload visits every row the rules take out in the audiences it keeps, through the index of rules losses, since a replaced row can bring any back"),
("snapshot_install.rs", sql, "g")
    if sql.starts_with("INSERT INTO _coven_rows(") =>
    Some("installing writes each earlier generation of the snapshot's rows; its write lookup is indexed"),
("snapshot_install.rs", sql, "i")
    if sql.starts_with("INSERT INTO _coven_rows(")
        || sql.starts_with("INSERT INTO _coven_cells(")
        || sql.starts_with("INSERT INTO _coven_references(") =>
    Some("installing writes the merge records of every row of the snapshot, one statement per table, column and reference; each lookup is indexed"),
("snapshot_install.rs", sql, "i")
    if sql.starts_with("SELECT EXISTS(SELECT 1 FROM {installed} AS i") =>
    Some("installing checks every row of the snapshot once; its loss lookup is indexed"),
("snapshot_install.rs", sql, relation)
    if (relation == "s" || relation.starts_with("temp._coven_install_"))
        && (sql.contains("temp._coven_install_parts") || sql.contains("temp._coven_install_authored")) =>
    Some("installing checks and writes every source record of the snapshot once; each lookup of the device's own is indexed"),
("sqlite.rs", "{} ORDER BY l.id", "l") =>
    Some("the public losses reader returns all losses"),
("store_state_tables.rs", "SELECT member,signing,sealing,role,removed,access FROM _coven_members", "_coven_members") =>
    Some("store_state() returns the complete member map"),
("store_state_tables.rs", "SELECT signing,sealing FROM _coven_bound_keys", "_coven_bound_keys") =>
    Some("store_state() returns every registered key pair"),
("store_state_tables.rs", "SELECT device,member,name,state FROM _coven_devices", "_coven_devices") =>
    Some("store_state() returns the complete device map"),
("store_state_tables.rs", "SELECT circle,name FROM _coven_circles", "_coven_circles") =>
    Some("store_state() returns the complete circle map"),
("store_state_tables.rs", "SELECT circle,member FROM _coven_circle_members", "_coven_circle_members") =>
    Some("store_state() returns every circle membership"),
("store_state_tables.rs", "SELECT circle,slot FROM _coven_circle_deletions", "_coven_circle_deletions") =>
    Some("store_state() returns every circle deletion and its cause"),
("store_state_tables.rs", "DELETE FROM {table}", table)
    if statement.sql == format!("DELETE FROM {table}") =>
    Some("applying a slot replaces the entire derived table; foreign-key lookups must be indexed"),
("upload.rs", "SELECT rowid,device,number,sealing_keys FROM _coven_uploads ORDER BY rowid LIMIT 1", "_coven_uploads") =>
    Some("rowid order with LIMIT 1 reads only the oldest queued write"),
("upload.rs", "SELECT rowid,device,number FROM _coven_uploads ORDER BY rowid", "_coven_uploads") =>
    Some("the queue reader lists every waiting write in insertion order"),
("raise_capture.rs", "DELETE FROM _coven_raise_captures", "_coven_raise_captures") =>
    Some("once per breaking migration: replace the previous unpublished capture"),
("raise_capture.rs", "SELECT audience,version,through,covers FROM _coven_raise_captures ORDER BY audience", "_coven_raise_captures") =>
    Some("one capture per readable audience; scheduling lists them all"),
("migration_snapshot.rs", "SELECT DISTINCT lower(table_name) FROM _coven_columns", "_coven_columns") =>
    Some("once per migration: discover every table with retained column metadata"),
("migration_receive.rs", "SELECT version,before_schema,after_schema,tables,columns FROM _coven_conversion_schemas ORDER BY version", "_coven_conversion_schemas") =>
    Some("opening retains every breaking migration's conversion schema"),
("migration_hidden.rs", "SELECT table_name,key,audience FROM _coven_lost WHERE frozen=0 AND replacement_kind='rules'", "_coven_lost") =>
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
("database_file_retention.rs", "SELECT device,file FROM _coven_row_file_refs UNION SELECT device,file FROM _coven_published_file_refs UNION SELECT device,file FROM _coven_queued_file_refs UNION SELECT device,file FROM _coven_file_uploads", "_coven_row_file_refs" | "_coven_published_file_refs" | "_coven_queued_file_refs" | "_coven_file_uploads") =>
    Some("retention enumerates every locally protected file"),
("migration_run.rs", "INSERT INTO _coven_migration_losses(write_id,audience,record) SELECT ?1,audience,record FROM temp._coven_migration_frozen", "temp._coven_migration_frozen") =>
    Some("the marker retains every loss frozen by this migration"),
("pending.rs", "{SELECT} ORDER BY id", "_coven_pending") =>
    Some("pending returns every retained condition"),
("pending.rs", "SELECT subject,value,operation,object,work,device,file,coven_version,move,pin FROM _coven_pending", "_coven_pending") =>
    Some("pending returns every retained condition"),
("pending.rs", "DELETE FROM _coven_pending AS p WHERE NOT EXISTS(SELECT 1 FROM _coven_provider_access a WHERE a.operation=p.operation)", "p") =>
    Some("reset visits every condition; provider-obligation lookups remain indexed"),
("published.rs", "SELECT EXISTS(SELECT 1 FROM _coven_published)", "_coven_published") =>
    Some("the counted projection marker has at most one row"),
("published.rs", "SELECT table_name,key,audience FROM _coven_rows UNION SELECT table_name,key,audience FROM _coven_lost", "_coven_rows" | "_coven_lost") =>
    Some("preserving the counted projection visits all current row identities"),
("published.rs", "DELETE FROM {table}", table) if statement.sql == format!("DELETE FROM {table}") =>
    Some("releasing the counted projection clears all of its retained records"),
("published.rs", "SELECT table_name,key,audience FROM _coven_published_rows UNION SELECT table_name,key,audience FROM _coven_published_losses", "_coven_published_rows" | "_coven_published_losses") =>
    Some("restoration visits every saved row identity"),
("published.rs", "SELECT record FROM _coven_published_rows ORDER BY audience,table_name,key", "_coven_published_rows") =>
    Some("restoration reads every preserved row"),
("published.rs", "SELECT record FROM _coven_published_losses ORDER BY id", "_coven_published_losses") =>
    Some("restoration reads every preserved loss"),
("published.rs", "INSERT INTO _coven_creations SELECT * FROM _coven_published_creations", "_coven_published_creations") =>
    Some("restoration installs every preserved creation"),
("published_migration.rs", "CREATE TEMP TABLE _coven_carried_rows AS SELECT * FROM _coven_published_rows; DELETE FROM _coven_published_rows", "_coven_published_rows") =>
    Some("a migration carries every preserved row through schema changes"),
("raise_capture.rs", "UPDATE _coven_raise_captures SET version=?1,through=?2,covers=?3", "_coven_raise_captures") =>
    Some("the new migration prefix replaces every audience capture"),
("raise_capture.rs", "SELECT max(through) FROM _coven_raise_captures", "database") =>
    Some("the eligible queue prefix includes every captured audience"),
("raise_capture.rs", "SELECT audience,version,covers FROM _coven_raise_captures ORDER BY audience", "_coven_raise_captures") =>
    Some("judgement reads every audience's unpublished raise"),
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
_ => None,
    }
}

#[cfg(test)]
#[path = "internal_schema_scan_reasons_tests.rs"]
mod tests;
