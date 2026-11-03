//! The sequence claims' tables (§5.1): each claim's durable identity and
//! exact attempted bytes, and the claim each queued upload made.

macro_rules! coven_claim_tables {
    ($visit:ident) => {
        $visit!(_coven_claim_counters, "
            CREATE TABLE _coven_claim_counters (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_claim_journal, "
            CREATE TABLE _coven_claim_journal (
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8),
                through BLOB CHECK(through IS NULL OR length(through)=8),
                operation INTEGER,
                settlement TEXT NOT NULL CHECK(settlement IN ('unknown','retry','not-landed','landed','device-id-in-use')),
                singleton INTEGER NOT NULL UNIQUE CHECK(singleton=1),
                CHECK((through IS NULL)<>(operation IS NULL)),
                PRIMARY KEY(device,number)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_claim_attempts, "
            CREATE TABLE _coven_claim_attempts (
                device BLOB NOT NULL,
                number BLOB NOT NULL,
                attempt INTEGER NOT NULL,
                path TEXT NOT NULL,
                digest BLOB CHECK(digest IS NULL OR length(digest)=32),
                size BLOB CHECK(size IS NULL OR length(size)=8),
                session BLOB,
                PRIMARY KEY(device,number,attempt),
                FOREIGN KEY(device,number) REFERENCES _coven_claim_journal(device,number) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_claim_paths ON _coven_claim_attempts(path);
        ");
        $visit!(_coven_claim_chunks, "
            CREATE TABLE _coven_claim_chunks (
                device BLOB NOT NULL,
                number BLOB NOT NULL,
                attempt INTEGER NOT NULL,
                chunk INTEGER NOT NULL,
                bytes BLOB NOT NULL CHECK(length(bytes)<=65536),
                PRIMARY KEY(device,number,attempt,chunk),
                FOREIGN KEY(device,number,attempt) REFERENCES _coven_claim_attempts(device,number,attempt) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_upload_claims, "
            CREATE TABLE _coven_upload_claims (
                device BLOB NOT NULL,
                number BLOB NOT NULL,
                claim BLOB NOT NULL CHECK(length(claim)=8),
                PRIMARY KEY(device, number),
                FOREIGN KEY(device,number) REFERENCES _coven_uploads(device,number) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_upload_claims_claim ON _coven_upload_claims(device,claim);
        ");
    };
}
