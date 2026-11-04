//! The applied sequence prefix and what the sequence settles: a migration's
//! captured prefix and losses, file retention and the applied slots' records.

macro_rules! coven_sequence_tables {
    ($visit:ident) => {
        coven_migration_prefix_tables!($visit);
        coven_migration_losses_tables!($visit);
        coven_applied_sequence_tables!($visit);
        coven_file_retention_tables!($visit);
        coven_sequence_application_tables!($visit);
    };
}
