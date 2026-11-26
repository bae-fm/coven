//! The published merge state's tables: the counted data beneath this
//! installation's provisional write queue (`published.rs`).

macro_rules! coven_published_tables {
    ($visit:ident) => {
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
        // A kept row whose table a migration dropped: the counted state holds
        // nothing of it but its kept losses, whatever the app's row left.
        $visit!(_coven_published_dropped, "
            CREATE TABLE _coven_published_dropped (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                PRIMARY KEY(table_name,key,audience)
            ) STRICT, WITHOUT ROWID;
        ");
        // The stored files each kept row's app row referenced when it was
        // kept, under the kept row, so putting one row back releases only its.
        $visit!(_coven_published_file_refs, "
            CREATE TABLE _coven_published_file_refs (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                device BLOB NOT NULL CHECK(length(device)=8),
                object BLOB NOT NULL CHECK(length(object)=16),
                PRIMARY KEY(table_name,key,audience,device,object)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_published_file_refs_object ON _coven_published_file_refs(device,object);
        ");
        // The applied writes each kept row's record and active lost values
        // name, by the row: the counted state reads their entries when it is
        // put back, so this device keeps them (§16.7).
        $visit!(_coven_published_writes, "
            CREATE TABLE _coven_published_writes (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                write_id INTEGER NOT NULL REFERENCES _coven_writes(id),
                PRIMARY KEY(table_name,key,audience,write_id)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_published_writes_write ON _coven_published_writes(write_id);
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
    };
}
