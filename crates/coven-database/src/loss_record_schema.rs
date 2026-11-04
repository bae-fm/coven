//! The values merges, removal rules and schema changes took out of the app's
//! rows (§18.7, D7).

macro_rules! coven_losses_tables {
    ($visit:ident) => {
        $visit!(_coven_lost, "
            CREATE TABLE _coven_lost (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                generation BLOB NOT NULL CHECK(length(generation) = 8),
                column_id INTEGER REFERENCES _coven_columns(id),
                value BLOB NOT NULL,
                set_by BLOB NOT NULL,
                frozen INTEGER NOT NULL DEFAULT 0 CHECK(frozen IN (0,1)),
                replacement_kind TEXT NOT NULL CHECK(replacement_kind IN ('write', 'rules', 'excluded', 'dependency', 'unauthorized')),
                replaced_by BLOB NOT NULL,
                CHECK((replacement_kind='write') = (column_id IS NOT NULL)),
                CHECK(replacement_kind NOT IN ('excluded','dependency','unauthorized') OR frozen=1)
            ) STRICT;
            CREATE INDEX _coven_lost_row ON _coven_lost(audience,table_name,key,generation,column_id);
            CREATE INDEX _coven_lost_app_key ON _coven_lost(table_name,key);
            CREATE UNIQUE INDEX _coven_lost_live_cell ON _coven_lost(table_name,key,audience,column_id,set_by) WHERE frozen=0 AND replacement_kind='write';
            CREATE INDEX _coven_lost_removed ON _coven_lost(table_name,key,audience) WHERE frozen=0 AND replacement_kind='rules';
            CREATE INDEX _coven_lost_column ON _coven_lost(column_id);
        ");
    };
}
