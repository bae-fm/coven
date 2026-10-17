//! Each row's selected original creation and the authorship evidence of
//! other members' writes (§9.6, D7, D9), kept apart from merge state and
//! dismissible losses.

macro_rules! coven_ownership_tables {
    ($visit:ident) => {
        $visit!(
            _coven_creations,
            "
            CREATE TABLE _coven_creations (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                write_id BLOB NOT NULL CHECK(length(write_id)=16),
                creator BLOB NOT NULL,
                PRIMARY KEY(table_name,key,audience)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_creations_audience ON _coven_creations(audience,table_name,key);
        "
        );
        $visit!(
            _coven_write_evidence,
            "
            CREATE TABLE _coven_write_evidence (
                write_id BLOB PRIMARY KEY NOT NULL CHECK(length(write_id)=16),
                prefix BLOB NOT NULL,
                header BLOB NOT NULL,
                roots BLOB NOT NULL,
                signature BLOB NOT NULL CHECK(length(signature)=64)
            ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_part_leaves,
            "
            CREATE TABLE _coven_part_leaves (
                write_id BLOB NOT NULL CHECK(length(write_id)=16),
                audience TEXT NOT NULL,
                hashes BLOB NOT NULL CHECK(length(hashes)%32=0),
                PRIMARY KEY(write_id,audience)
            ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_part_proofs,
            "
            CREATE TABLE _coven_part_proofs (
                write_id BLOB NOT NULL CHECK(length(write_id)=16),
                audience TEXT NOT NULL,
                proof BLOB NOT NULL,
                hashes BLOB NOT NULL CHECK(length(hashes)%32=0),
                PRIMARY KEY(write_id,audience)
            ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_owner_tables,
            "
            CREATE TABLE _coven_owner_tables (
                table_name TEXT PRIMARY KEY NOT NULL
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
