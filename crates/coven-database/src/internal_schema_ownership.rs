//! Each row's selected original creation (§9.6), kept apart from merge state
//! and dismissible losses.

macro_rules! coven_ownership_tables {
    ($visit:ident) => {
        coven_creations_tables!($visit);
        coven_authorship_tables!($visit);
        coven_schema_definitions_tables!($visit);
    };
}
