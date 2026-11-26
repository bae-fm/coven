//! Each write's checked plaintext header, kept after its body leaves.

macro_rules! coven_write_headers_tables {
    ($visit:ident) => {
        $visit!(
            _coven_write_headers,
            "
            CREATE TABLE _coven_write_headers (
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000'),
                header BLOB NOT NULL,
                PRIMARY KEY(device,number)
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
