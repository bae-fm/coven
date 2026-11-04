//! The sequence prefix this installation has applied (§5).

macro_rules! coven_applied_sequence_tables {
    ($visit:ident) => {
        $visit!(
            _coven_sequence,
            "
            CREATE TABLE _coven_sequence (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                applied BLOB NOT NULL CHECK(length(applied)=8)
            ) STRICT;
            INSERT INTO _coven_sequence VALUES(1,x'0000000000000000');
        "
        );
    };
}
