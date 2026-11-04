//! The freezes a breaking migration made, which its migration write puts as
//! losses once it applies (§18.5).

macro_rules! coven_migration_losses_tables {
    ($visit:ident) => {
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
    };
}
