//! The oldest schema version whose writes this installation reads, and the
//! breaking raise it waits to publish (§18.5).

macro_rules! coven_snapshot_schema_tables {
    ($visit:ident) => {
        $visit!(
            _coven_snapshot_schema,
            "
            CREATE TABLE _coven_snapshot_schema (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                minimum INTEGER NOT NULL CHECK(minimum>=0),
                publication INTEGER NOT NULL CHECK(publication>=0)
            ) STRICT;
            INSERT INTO _coven_snapshot_schema VALUES(1,0,0);
        "
        );
    };
}
