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
        coven_sequence_application_tables!($visit);
    };
}
