//! How far this device has looked for kept records and applied-write entries
//! no write still to be judged can name, and the entries something stopped
//! naming since (§16.7).

macro_rules! coven_forgetting_tables {
    ($visit:ident) => {
        // The slot through which this device has looked at every kept record
        // that stopped being current and every applied-write entry, and
        // forgotten those it may: each slot looks only past it. `newest` is
        // the latest stamped entry at or below it, which stays.
        $visit!(
            _coven_forgotten,
            "
            CREATE TABLE _coven_forgotten (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                through BLOB NOT NULL CHECK(length(through)=8),
                newest INTEGER REFERENCES _coven_writes(id)
            ) STRICT;
        "
        );
        // The applied writes something stopped naming since the last applied
        // slot, which the next one looks at again.
        $visit!(
            _coven_unnamed_writes,
            "
            CREATE TABLE _coven_unnamed_writes (
                write_id INTEGER PRIMARY KEY NOT NULL REFERENCES _coven_writes(id)
            ) STRICT;
        "
        );
    };
}
