//! The bytes in coven's files and cache areas recorded for removal once the
//! writer is free (Appendix C.3).

macro_rules! coven_file_removals_tables {
    ($visit:ident) => {
        $visit!(_coven_file_removals, "
            CREATE TABLE _coven_file_removals (
                path TEXT NOT NULL,
                area TEXT NOT NULL DEFAULT 'files' CHECK(area IN ('files','cache')),
                PRIMARY KEY(path,area)
            ) STRICT, WITHOUT ROWID;
            CREATE TRIGGER _coven_file_removal_unnamed BEFORE INSERT ON _coven_file_removals BEGIN
                SELECT RAISE(ABORT,'a file recorded for removal is named by a row')
                WHERE EXISTS(SELECT 1 FROM _coven_device_files WHERE path=NEW.path)
                    OR EXISTS(SELECT 1 FROM _coven_file_uploads WHERE copy=NEW.path)
                    OR EXISTS(SELECT 1 FROM _coven_pin_requests WHERE copy=NEW.path)
                    OR EXISTS(SELECT 1 FROM _coven_cache WHERE path=NEW.path)
                    OR EXISTS(SELECT 1 FROM _coven_cache_partials WHERE path=NEW.path);
            END;
        ");
    };
}
