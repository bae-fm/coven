//! The applied slots' records, and the slots a write landed in or was refused
//! at (§5).

macro_rules! coven_sequence_application_tables {
    ($visit:ident) => {
        $visit!(
            _coven_slot_records,
            "
            CREATE TABLE _coven_slot_records (
                slot BLOB PRIMARY KEY NOT NULL CHECK(length(slot)=8),
                kind TEXT NOT NULL CHECK(kind IN ('admin','key','announcement')),
                record BLOB NOT NULL
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_slot_kind ON _coven_slot_records(kind,slot);
        "
        );
        // The sealed size of each applied writes slot: what a new device
        // reads to replay it. Snapshot requests sum it (§16.2).
        $visit!(
            _coven_write_slots,
            "
            CREATE TABLE _coven_write_slots (
                slot BLOB PRIMARY KEY NOT NULL CHECK(length(slot)=8),
                size INTEGER NOT NULL CHECK(size>0)
            ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_write_refusals,
            "
            CREATE TABLE _coven_write_refusals (
                slot BLOB PRIMARY KEY NOT NULL CHECK(length(slot)=8),
                cutoff BLOB NOT NULL CHECK(length(cutoff)=8)
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
