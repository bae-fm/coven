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
                slot_key BLOB CHECK(slot_key IS NULL OR length(slot_key)=9),
                work TEXT CHECK(work IN ('upload','download')),
                device BLOB CHECK(device IS NULL OR length(device)=8),
                object BLOB CHECK(object IS NULL OR length(object)=16),
                build BLOB NOT NULL CHECK(length(build)=16),
                move INTEGER REFERENCES _coven_moves(id),
                pin TEXT REFERENCES _coven_pin_requests(file) ON DELETE CASCADE,
                cause TEXT,
                -- The number of this device's write a raise wait holds: a
                -- capture that takes the write into a raise's prefix ends
                -- the wait in its own commit (§18.5).
                raise_write BLOB CHECK(raise_write IS NULL OR length(raise_write)=8),
                CHECK((device IS NULL)=(object IS NULL)),
                CHECK((work IS NULL)=(device IS NULL))
            ) STRICT;
            CREATE INDEX _coven_pending_slot_key ON _coven_pending(slot_key);
            CREATE INDEX _coven_pending_object ON _coven_pending(device,object) WHERE device IS NOT NULL;
            CREATE UNIQUE INDEX _coven_pending_operation ON _coven_pending(operation) WHERE operation IS NOT NULL;
            CREATE UNIQUE INDEX _coven_pending_move ON _coven_pending(move) WHERE move IS NOT NULL;
            CREATE UNIQUE INDEX _coven_pending_pin ON _coven_pending(pin) WHERE pin IS NOT NULL;
            CREATE INDEX _coven_pending_raise_write ON _coven_pending(raise_write) WHERE raise_write IS NOT NULL;
            -- A download's record ends with the last file over its bytes,
            -- in the transaction that removes it or moves it off them
            -- (§19.1).
            CREATE TRIGGER _coven_stored_files_removed AFTER DELETE ON _coven_stored_files
            WHEN NOT EXISTS(SELECT 1 FROM _coven_stored_files AS s WHERE s.device=OLD.device AND s.object=OLD.object)
            BEGIN
                DELETE FROM _coven_pending WHERE work='download' AND device=OLD.device AND object=OLD.object;
            END;
            CREATE TRIGGER _coven_stored_files_moved AFTER UPDATE OF device,object ON _coven_stored_files
            WHEN NOT EXISTS(SELECT 1 FROM _coven_stored_files AS s WHERE s.device=OLD.device AND s.object=OLD.object)
            BEGIN
                DELETE FROM _coven_pending WHERE work='download' AND device=OLD.device AND object=OLD.object;
            END;
        ");
    };
}
