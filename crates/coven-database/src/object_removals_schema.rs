//! The objects this device put in storage for a while and still owes the
//! deletion of (§13.2).

macro_rules! coven_object_removals_tables {
    ($visit:ident) => {
        // Objects this device put in storage for a while, each with the
        // time, in seconds since the Unix epoch, after which a pass deletes
        // it: an ended invite's marker (§13.2). Nothing references them.
        $visit!(
            _coven_object_removals,
            "
            CREATE TABLE _coven_object_removals (
                path TEXT PRIMARY KEY,
                after INTEGER NOT NULL CHECK(after>=0)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_object_removals_after ON _coven_object_removals(after);
        "
        );
    };
}
