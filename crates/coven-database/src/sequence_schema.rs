//! The sequence prefix this installation has applied (§5), each device's last
//! claim in it, and the cut-offs its admin slots set (§5.2).

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
        $visit!(
            _coven_last_claims,
            "
            CREATE TABLE _coven_last_claims (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8),
                digest BLOB CHECK(digest IS NULL OR length(digest)=32),
                through BLOB NOT NULL CHECK(length(through)=8),
                CHECK((number=x'0000000000000000')=(digest IS NULL))
            ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_cutoffs,
            "
            CREATE TABLE _coven_cutoffs (
                slot BLOB PRIMARY KEY NOT NULL CHECK(length(slot)=8),
                device BLOB CHECK(device IS NULL OR length(device)=8),
                member BLOB CHECK(member IS NULL OR length(member)=16),
                signing BLOB CHECK(signing IS NULL OR length(signing)=32),
                CHECK((device IS NULL)<>(member IS NULL)),
                CHECK(signing IS NULL OR member IS NOT NULL)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_cutoffs_device ON _coven_cutoffs(device,slot);
            CREATE INDEX _coven_cutoffs_member ON _coven_cutoffs(member,slot);
        "
        );
    };
}
