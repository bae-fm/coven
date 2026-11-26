//! Durable retry deadlines (§6.7).

macro_rules! coven_retries_tables {
    ($visit:ident) => {
        // One persisted retry wait, T and W, per waiting operation, queued
        // upload, pin request, joiner poll or provider cooldown (§6.7). Work rows take
        // their waits with them.
        $visit!(
            _coven_retries,
            "
            CREATE TABLE _coven_retries (
                id INTEGER PRIMARY KEY,
                scope TEXT UNIQUE CHECK(scope IN ('join','sync','provider')),
                operation INTEGER UNIQUE REFERENCES _coven_operations(id) ON DELETE CASCADE,
                upload INTEGER UNIQUE REFERENCES _coven_file_uploads(id) ON DELETE CASCADE,
                pin INTEGER UNIQUE REFERENCES _coven_pin_requests(id) ON DELETE CASCADE,
                failures INTEGER NOT NULL CHECK(failures>=0),
                not_before BLOB NOT NULL CHECK(length(not_before)=13),
                wait INTEGER NOT NULL CHECK(wait>=0),
                CHECK((scope IS NOT NULL)+(operation IS NOT NULL)+(upload IS NOT NULL)
                    +(pin IS NOT NULL)=1)
            ) STRICT;
        "
        );
    };
}
