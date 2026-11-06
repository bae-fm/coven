/// Record a cache entry of `size` bytes at `path`, for chunk 0 of the stored
/// object at `object_path` in the `files` namespace, last read at `read`.
pub(crate) fn cached(
    db: &crate::sqlite::DatabaseConnection,
    object_path: &str,
    path: &str,
    size: i64,
    read: i64,
) -> Result<usize, crate::DbError> {
    db.internal_execute(
        "INSERT INTO _coven_cache(namespace,object_path,chunk,path,size,last_read,pinned)
         VALUES('files',?1,0,?2,?3,?4,0)",
        (object_path, path, size, read),
    )
}

/// Record a cache range at `path` as being written, as a crash mid-write
/// leaves it.
pub(crate) fn writing(db: &crate::sqlite::DatabaseConnection, path: &str) {
    db.internal_execute("INSERT INTO _coven_cache_writes(path) VALUES(?1)", [path]).unwrap();
}

/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_cache(namespace,object_path,chunk,path,size,last_read,pinned,checked_hash)
           SELECT audience,'file_' || i,CASE WHEN (i/64)%4=0 THEN -2 ELSE 0 END,'cache_' || i,
                  65536,i,CASE WHEN (i/64)%4=0 THEN 1 ELSE 0 END,
                  CASE WHEN (i/64)%4=0 THEN key END FROM fixture;
         INSERT INTO _coven_cache_budgets(namespace,bytes) SELECT 'audience_' || i,counter(1048576) FROM fixture WHERE true ON CONFLICT(namespace) DO UPDATE SET bytes=excluded.bytes;
         INSERT INTO _coven_cache_writes SELECT 'writing_' || i FROM fixture;",
    )
    .unwrap();
}
