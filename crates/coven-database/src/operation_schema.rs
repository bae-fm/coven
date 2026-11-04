//! The journal of durable operations, the app's operations among them, and
//! the keys a running operation holds, its snapshot reservation and the write
//! pause it sets (§11).

macro_rules! coven_operations_tables {
    ($visit:ident) => {
        $visit!(
            _coven_operations,
            "
            CREATE TABLE _coven_operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                last_step INTEGER NOT NULL,
                data BLOB NOT NULL,
                started_by TEXT NOT NULL
            );
            CREATE INDEX _coven_operations_kind ON _coven_operations(kind);
        "
        );
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
        // The operations the app's writes journaled, each with its kind as
        // coven-sync names it and, once it has finished, its outcome as
        // coven-sync encodes it, until the app acknowledges it
        // (`coven_operations`, §11.4). The journal row of an app operation
        // without its outcome can't be deleted: the outcome is written
        // first, in the same transaction.
        $visit!(
            _coven_app_operations,
            "
            CREATE TABLE _coven_app_operations (
                operation INTEGER PRIMARY KEY,
                kind TEXT NOT NULL,
                outcome BLOB
            ) STRICT;
            CREATE TRIGGER _coven_app_operation_ends BEFORE DELETE ON _coven_operations
            WHEN EXISTS(SELECT 1 FROM _coven_app_operations
                WHERE operation=OLD.id AND outcome IS NULL)
            BEGIN
                SELECT RAISE(ABORT,'an app operation ends with its outcome');
            END;
        "
        );
    };
}
