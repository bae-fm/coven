//! A breaking migration's raise: the queue prefix captured for each audience
//! until its snapshot publishes and the oldest schema version whose writes
//! this installation reads (§18.5).

macro_rules! coven_raises_tables {
    ($visit:ident) => {
        $visit!(
            _coven_raise_captures,
            "
            CREATE TABLE _coven_raise_captures (
                audience TEXT PRIMARY KEY NOT NULL,
                version INTEGER NOT NULL,
                through BLOB NOT NULL CHECK(length(through)=8),
                covers BLOB NOT NULL CHECK(length(covers)=8)
            ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_snapshot_schema,
            "
            CREATE TABLE _coven_snapshot_schema (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                minimum INTEGER NOT NULL CHECK(minimum>=0)
            ) STRICT;
            INSERT INTO _coven_snapshot_schema VALUES(1,0);
        "
        );
    };
}
