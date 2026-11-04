//! The fixed file paths awaiting a storage-deletion decision, the triggers
//! that add each as its last local protection ends, and the snapshots found
//! absent and the slots each device's files were scanned through, which
//! prove storage deletion strands no row (§17.10).

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
        // Fixed file paths awaiting a storage-deletion decision (§17.10): the
        // transaction ending a path's last local protection adds it, so
        // retention never lists files/. `since` is the applied slot when it
        // last lost its protection: every other device must have published a
        // write that read through it before the path goes. Applying a slot
        // records it as applied before its changes reach these tables, so a
        // path a slot leaves unnamed counts from that slot.
        $visit!(_coven_file_candidates, "
            CREATE TABLE _coven_file_candidates (
                device BLOB NOT NULL CHECK(length(device)=8),
                file BLOB NOT NULL CHECK(length(file)=16),
                since BLOB NOT NULL CHECK(length(since)=8),
                PRIMARY KEY(device,file)
            ) STRICT, WITHOUT ROWID;
            CREATE VIEW _coven_file_protection(device,file) AS
                SELECT device,file FROM _coven_row_file_refs UNION ALL SELECT device,file FROM _coven_published_file_refs
                UNION ALL SELECT device,file FROM _coven_queued_file_refs UNION ALL SELECT device,file FROM _coven_file_uploads;
            CREATE TRIGGER _coven_row_file_ref_released AFTER DELETE ON _coven_row_file_refs BEGIN
                INSERT INTO _coven_file_candidates SELECT OLD.device,OLD.file,(SELECT applied FROM _coven_sequence WHERE singleton=1) WHERE NOT EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=OLD.device AND file=OLD.file) ON CONFLICT DO UPDATE SET since=max(since,excluded.since);
            END;
            CREATE TRIGGER _coven_published_file_ref_released AFTER DELETE ON _coven_published_file_refs BEGIN
                INSERT INTO _coven_file_candidates SELECT OLD.device,OLD.file,(SELECT applied FROM _coven_sequence WHERE singleton=1) WHERE NOT EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=OLD.device AND file=OLD.file) ON CONFLICT DO UPDATE SET since=max(since,excluded.since);
            END;
            CREATE TRIGGER _coven_queued_file_ref_released AFTER DELETE ON _coven_queued_file_refs BEGIN
                INSERT INTO _coven_file_candidates SELECT OLD.device,OLD.file,(SELECT applied FROM _coven_sequence WHERE singleton=1) WHERE NOT EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=OLD.device AND file=OLD.file) ON CONFLICT DO UPDATE SET since=max(since,excluded.since);
            END;
            CREATE TRIGGER _coven_file_upload_released AFTER DELETE ON _coven_file_uploads BEGIN
                INSERT INTO _coven_file_candidates SELECT OLD.device,OLD.file,(SELECT applied FROM _coven_sequence WHERE singleton=1) WHERE NOT EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=OLD.device AND file=OLD.file) ON CONFLICT DO UPDATE SET since=max(since,excluded.since);
            END;
        ");
    };
}
