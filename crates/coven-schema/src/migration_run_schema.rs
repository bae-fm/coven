//! What a breaking migration's write carries only on its own device: the
//! freezes it made, which it puts as losses once it applies, and its row
//! changes whole, which its marker applies from (§18.4, §18.5).

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
        // The migration write's original records, whole: its marker has no
        // row changes in its slot, so this device applies them from here when
        // the marker's slot applies (§18.4). Its kept records are in
        // `_coven_authored_changes` with every other write's.
        $visit!(_coven_migration_records, "
            CREATE TABLE _coven_migration_records (
                write_id BLOB NOT NULL CHECK(length(write_id)=16),
                audience TEXT NOT NULL,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                record BLOB NOT NULL,
                PRIMARY KEY(write_id,audience,table_name,key)
            ) STRICT, WITHOUT ROWID;
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
