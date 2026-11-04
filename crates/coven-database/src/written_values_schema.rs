//! The written values an app table displays differently.

macro_rules! coven_written_values_tables {
    ($visit:ident) => {
        $visit!(_coven_written_values, "
            CREATE TABLE _coven_written_values (
                column_id INTEGER NOT NULL,
                row_id INTEGER NOT NULL,
                value ANY,
                PRIMARY KEY(row_id,column_id),
                FOREIGN KEY(column_id,row_id) REFERENCES _coven_cells(column_id,row_id) ON DELETE CASCADE ON UPDATE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
    };
}
