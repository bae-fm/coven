//! The applied sequence prefix and durable claim attempts.

macro_rules! coven_sequence_tables {
    ($visit:ident) => {
        $visit!(_coven_store, "
            CREATE TABLE _coven_store (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                store BLOB NOT NULL CHECK(length(store)=16),
                name TEXT NOT NULL,
                created INTEGER NOT NULL CHECK(created IN (0,1))
            ) STRICT;
        ");
        $visit!(_coven_migration_prefix, "
            CREATE TABLE _coven_migration_prefix (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                device BLOB NOT NULL CHECK(length(device)=8),
                through BLOB NOT NULL CHECK(length(through)=8),
                version INTEGER NOT NULL
            ) STRICT;
        ");
        $visit!(_coven_migration_losses, "
            CREATE TABLE _coven_migration_losses (
                write_id BLOB NOT NULL CHECK(length(write_id)=16),
                audience TEXT NOT NULL,
                record BLOB NOT NULL,
                PRIMARY KEY(write_id,audience,record)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_migration_losses_audience ON _coven_migration_losses(audience);
        ");
        $visit!(_coven_published, "
            CREATE TABLE _coven_published (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1)
            ) STRICT;
        ");
        $visit!(_coven_published_rows, "
            CREATE TABLE _coven_published_rows (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                record BLOB NOT NULL,
                visible INTEGER NOT NULL CHECK(visible IN (0,1)),
                PRIMARY KEY(table_name,key,audience)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_published_rows_audience ON _coven_published_rows(audience,table_name,key);
        ");
        $visit!(_coven_published_losses, "
            CREATE TABLE _coven_published_losses (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                record BLOB NOT NULL
            ) STRICT;
            CREATE INDEX _coven_published_losses_audience ON _coven_published_losses(audience,table_name,key,id);
        ");
        $visit!(_coven_published_file_refs, "
            CREATE TABLE _coven_published_file_refs (
                device BLOB NOT NULL CHECK(length(device)=8),
                file BLOB NOT NULL CHECK(length(file)=16),
                PRIMARY KEY(device,file)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_published_creations, "
            CREATE TABLE _coven_published_creations (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                write_id BLOB NOT NULL,
                creator BLOB NOT NULL,
                PRIMARY KEY(table_name,key,audience)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_published_creations_audience ON _coven_published_creations(audience,table_name,key);
        ");
        $visit!(_coven_sequence, "
            CREATE TABLE _coven_sequence (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                applied BLOB NOT NULL CHECK(length(applied)=8)
            ) STRICT;
            INSERT INTO _coven_sequence VALUES(1,x'0000000000000000');
        ");
        $visit!(_coven_absent_snapshots, "
            CREATE TABLE _coven_absent_snapshots (
                audience TEXT NOT NULL,
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8),
                PRIMARY KEY(audience,device,number)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_last_claims, "
            CREATE TABLE _coven_last_claims (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8),
                digest BLOB CHECK(digest IS NULL OR length(digest)=32),
                through BLOB NOT NULL CHECK(length(through)=8),
                CHECK((number=x'0000000000000000')=(digest IS NULL))
            ) STRICT, WITHOUT ROWID;
        ");
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
        $visit!(_coven_slot_records, "
            CREATE TABLE _coven_slot_records (
                slot BLOB PRIMARY KEY NOT NULL CHECK(length(slot)=8),
                kind TEXT NOT NULL CHECK(kind IN ('admin','key','announcement')),
                record BLOB NOT NULL
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_slot_kind ON _coven_slot_records(kind,slot);
        ");
        $visit!(_coven_write_refusals, "
            CREATE TABLE _coven_write_refusals (
                slot BLOB PRIMARY KEY NOT NULL CHECK(length(slot)=8),
                cutoff BLOB NOT NULL CHECK(length(cutoff)=8)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_cutoffs, "
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
        ");
    };
}
