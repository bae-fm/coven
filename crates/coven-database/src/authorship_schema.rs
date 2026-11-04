//! The authorship evidence of other members' writes (§9.6, D7, D9): each
//! write's signed header and roots, and each audience part's leaves and proof.

macro_rules! coven_authorship_tables {
    ($visit:ident) => {
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
    };
}
