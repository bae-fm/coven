//! The applied sequence prefix, the store, the migration's losses and what
//! the sequence settles.

macro_rules! coven_sequence_tables {
    ($visit:ident) => {
        $visit!(_coven_store, "
            CREATE TABLE _coven_store (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                store BLOB NOT NULL CHECK(length(store)=16),
                name TEXT NOT NULL,
                named_at BLOB NOT NULL CHECK(length(named_at)=8 AND named_at>x'0000000000000000'),
                created INTEGER NOT NULL CHECK(created IN (0,1))
            ) STRICT;
        ");
        coven_migration_prefix_tables!($visit);
        $visit!(_coven_migration_losses, "
            CREATE TABLE _coven_migration_losses (
                write_id BLOB NOT NULL CHECK(length(write_id)=16),
                audience TEXT NOT NULL,
                record BLOB NOT NULL,
                PRIMARY KEY(write_id,audience,record)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_migration_losses_audience ON _coven_migration_losses(audience);
        ");
        // The freezes a breaking migration made of the state kept aside beside
        // the queue, over the counted prefix: its migration write's own
        // losses, over the whole prefix, replace them wherever they are put.
        $visit!(_coven_migration_counted_losses, "
            CREATE TABLE _coven_migration_counted_losses (
                write_id BLOB NOT NULL CHECK(length(write_id)=16),
                audience TEXT NOT NULL,
                record BLOB NOT NULL,
                PRIMARY KEY(write_id,audience,record)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_migration_counted_losses_audience ON _coven_migration_counted_losses(audience);
        ");
        coven_applied_sequence_tables!($visit);
        coven_file_retention_tables!($visit);
        $visit!(_coven_last_claims, "
            CREATE TABLE _coven_last_claims (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8),
                digest BLOB CHECK(digest IS NULL OR length(digest)=32),
                through BLOB NOT NULL CHECK(length(through)=8),
                CHECK((number=x'0000000000000000')=(digest IS NULL))
            ) STRICT, WITHOUT ROWID;
        ");
        coven_sequence_application_tables!($visit);
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
