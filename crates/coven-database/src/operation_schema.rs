//! The keys a running operation holds, its snapshot reservation and the write
//! pause it sets (§11).

macro_rules! coven_operation_state_tables {
    ($visit:ident) => {
        // Every member pair this installation holds or has held, its kept
        // candidate included, recorded before custody keeps it: a binding of
        // its member to a pair not here is one it can't sign with, which
        // stops it for good (§12.3, §14.1).
        $visit!(
            _coven_held_keys,
            "
            CREATE TABLE _coven_held_keys (
                signing BLOB NOT NULL CHECK(length(signing)=32),
                sealing BLOB NOT NULL CHECK(length(sealing)=32),
                PRIMARY KEY(signing,sealing)
            ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_snapshot_reservation,
            "
            CREATE TABLE _coven_snapshot_reservation (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                number INTEGER NOT NULL CHECK(number>=0)
            ) STRICT;
            INSERT INTO _coven_snapshot_reservation VALUES(1,0);
        "
        );
        // The one operation pausing app writes while it replaces this
        // member's keys (§12.3). Its row goes with the operation's.
        $visit!(
            _coven_write_pause,
            "
            CREATE TABLE _coven_write_pause (
                paused INTEGER PRIMARY KEY CHECK(paused=1),
                operation INTEGER NOT NULL UNIQUE
                    REFERENCES _coven_operations(id) ON DELETE CASCADE
            ) STRICT;
        "
        );
    };
}
