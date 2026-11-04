//! The snapshots found absent and the slots each device's files were scanned
//! through, which prove storage deletion strands no row (§17.10).

macro_rules! coven_file_retention_tables {
    ($visit:ident) => {
        $visit!(
            _coven_absent_snapshots,
            "
            CREATE TABLE _coven_absent_snapshots (
                audience TEXT NOT NULL,
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8),
                PRIMARY KEY(audience,device,number)
            ) STRICT, WITHOUT ROWID;
        "
        );
        // The file folder of an uploader gone for good that this device
        // listed when the entry stopping it applied, and that entry's slot.
        // A later entry stopping the same device again, as its member's
        // removal does, lists it again; the paths found are candidates
        // (§17.10).
        $visit!(
            _coven_file_scans,
            "
            CREATE TABLE _coven_file_scans (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                slot BLOB NOT NULL CHECK(length(slot)=8)
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
