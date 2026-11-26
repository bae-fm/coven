//! The app's columns that reference files (§17.2).

macro_rules! coven_file_columns_tables {
    ($visit:ident) => {
        // The app's file reference columns as last registered, which a later
        // open or migration compares against (§18.1).
        $visit!(
            _coven_file_columns,
            "
            CREATE TABLE _coven_file_columns (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                write_once INTEGER NOT NULL CHECK(write_once IN (0,1)),
                PRIMARY KEY(table_name,column_name)
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
