/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_device_files SELECT 'file_' || i,'path_' || i,zeroblob(32) FROM fixture;
         INSERT INTO _coven_copy_chunks SELECT 'path_' || i,0,zeroblob(32) FROM fixture;
         INSERT INTO _coven_file_uploads(id,location,device,object,size,hash,copy,move,stored,queued_at)
           SELECT i,'location_' || i,device,substr(key,17,16),1,zeroblob(32),'copy_' || i,i,i%2,zeroblob(13) FROM fixture;
         INSERT INTO _coven_upload_files SELECT 'file_' || i,i,i%2 FROM fixture;
         INSERT INTO _coven_file_chunks SELECT device,substr(key,17,16),0,key FROM fixture;
         INSERT INTO _coven_upload_pause VALUES(1);
         INSERT INTO _coven_unmetered_only VALUES(1);",
    )
    .unwrap();
}

/// The statement pausing uploads, for a test of what a transaction records.
pub(crate) const PAUSE: &str = "INSERT INTO _coven_upload_pause VALUES(1)";

/// Lose every queued file's chunk hashes, as a damaged database would.
pub(crate) fn lose_chunk_hashes(db: &crate::sqlite::DatabaseConnection) {
    db.internal_execute("DELETE FROM _coven_file_chunks", []).unwrap();
}

/// Give every queued upload a stored time no decoder accepts, as a damaged
/// database would.
pub(crate) fn damage_queued_times(db: &crate::sqlite::DatabaseConnection) {
    db.internal_execute("UPDATE _coven_file_uploads SET queued_at=x'00'", []).unwrap();
}

/// Queue an upload of coven's copy of each row's file of the app table
/// `tracks`, in one move, inside the caller's transaction.
pub(crate) fn queue_track_files(db: &crate::sqlite::DatabaseConnection) {
    crate::file_moves::tests::moving(db, 1);
    db.batch(
        "INSERT INTO _coven_file_uploads(id,location,device,object,size,hash,copy,move,queued_at)
           SELECT rowid,'file 0 ' || file || ' ' || hex(zeroblob(32)),zeroblob(8),
                  unhex(printf('%032x',rowid)),0,zeroblob(32),'copy ' || rowid,1,zeroblob(13)
           FROM tracks;
         INSERT INTO _coven_upload_files(file,upload) SELECT file,rowid FROM tracks;",
    )
    .unwrap();
}
