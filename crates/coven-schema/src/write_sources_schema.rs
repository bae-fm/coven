//! Each original change this device holds, the part header it came in, the
//! slot at which it stopped being current, and this device's verdict on it
//! (§9.5, §16.7, D7).

macro_rules! coven_authored_sources_tables {
    ($visit:ident) => {
        $visit!(_coven_authored_parts, "
            CREATE TABLE _coven_authored_parts (
                write_id BLOB NOT NULL CHECK(length(write_id)=16),
                audience TEXT NOT NULL,
                header BLOB NOT NULL,
                PRIMARY KEY(write_id,audience)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_authored_changes, "
            CREATE TABLE _coven_authored_changes (
                write_id BLOB NOT NULL,
                audience TEXT NOT NULL,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                record BLOB NOT NULL,
                stop BLOB CHECK(stop IS NULL OR length(stop)=8),
                PRIMARY KEY(write_id,audience,table_name,key),
                FOREIGN KEY(write_id,audience) REFERENCES _coven_authored_parts(write_id,audience)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_authored_changes_audience ON _coven_authored_changes(audience,write_id,table_name,key);
            CREATE INDEX _coven_authored_changes_stop ON _coven_authored_changes(stop) WHERE stop IS NOT NULL;
        ");
        // This device's verdict on an original change, beside its record for
        // the reason `_coven_upload_keys` is beside an upload's.
        $visit!(_coven_authored_admissions, "
            CREATE TABLE _coven_authored_admissions (
                write_id BLOB NOT NULL,
                audience TEXT NOT NULL,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                admission BLOB NOT NULL,
                PRIMARY KEY(write_id,audience,table_name,key),
                FOREIGN KEY(write_id,audience,table_name,key) REFERENCES _coven_authored_changes(write_id,audience,table_name,key) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
    };
}
