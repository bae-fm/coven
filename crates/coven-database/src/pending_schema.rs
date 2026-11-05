//! One record per thing that can't proceed (§19.1).

macro_rules! coven_pending_tables {
    ($visit:ident) => {
        // A record's cause is the text of the error behind it, written with
        // the record by the run that met the error. Opening clears every
        // cause: one from an earlier run names a failure nothing has met
        // since (§19.1).
        // `build` is the build of coven that recorded it: a refusal is
        // rechecked once by each other build (§19.1).
        $visit!(_coven_pending, "
            CREATE TABLE _coven_pending (
                id INTEGER PRIMARY KEY,
                subject BLOB NOT NULL UNIQUE,
                value BLOB NOT NULL,
                operation INTEGER REFERENCES _coven_operations(id),
                object BLOB CHECK(object IS NULL OR length(object)=9),
                work TEXT CHECK(work IN ('upload','download')),
                device BLOB CHECK(device IS NULL OR length(device)=8),
                file BLOB CHECK(file IS NULL OR length(file)=16),
                build BLOB NOT NULL CHECK(length(build)=16),
                move INTEGER REFERENCES _coven_moves(id),
                pin TEXT REFERENCES _coven_pin_requests(file) ON DELETE CASCADE,
                cause TEXT,
                CHECK((device IS NULL)=(file IS NULL)),
                CHECK((work IS NULL)=(device IS NULL))
            ) STRICT;
            CREATE INDEX _coven_pending_object ON _coven_pending(object);
            CREATE INDEX _coven_pending_file ON _coven_pending(device,file) WHERE device IS NOT NULL;
            CREATE UNIQUE INDEX _coven_pending_operation ON _coven_pending(operation) WHERE operation IS NOT NULL;
            CREATE UNIQUE INDEX _coven_pending_move ON _coven_pending(move) WHERE move IS NOT NULL;
            CREATE UNIQUE INDEX _coven_pending_pin ON _coven_pending(pin) WHERE pin IS NOT NULL;
            -- A download's record ends with the last file over its bytes,
            -- in the transaction that removes it or moves it off them
            -- (§19.1).
            CREATE TRIGGER _coven_stored_files_removed AFTER DELETE ON _coven_stored_files
            WHEN NOT EXISTS(SELECT 1 FROM _coven_stored_files AS s WHERE s.device=OLD.device AND s.object=OLD.object)
            BEGIN
                DELETE FROM _coven_pending WHERE work='download' AND device=OLD.device AND file=OLD.object;
            END;
            CREATE TRIGGER _coven_stored_files_moved AFTER UPDATE OF device,object ON _coven_stored_files
            WHEN NOT EXISTS(SELECT 1 FROM _coven_stored_files AS s WHERE s.device=OLD.device AND s.object=OLD.object)
            BEGIN
                DELETE FROM _coven_pending WHERE work='download' AND device=OLD.device AND file=OLD.object;
            END;
        ");
    };
}
