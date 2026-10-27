//! The retained-input database's schema: independently committed remote
//! input, owned by the same store lifetime.

macro_rules! coven_input_tables {
    ($visit:ident) => {
        $visit!(
            _coven_input_objects,
            "
    CREATE TABLE _coven_input_objects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT NOT NULL UNIQUE,
        identity BLOB NOT NULL,
        observation BLOB NOT NULL,
        released INTEGER NOT NULL DEFAULT 0 CHECK(released IN (0,1)),
        received INTEGER NOT NULL DEFAULT 0 CHECK(received IN (0,1)),
        files_complete INTEGER CHECK(files_complete IN (0,1)),
        files_schema INTEGER CHECK(files_schema IS NULL OR files_complete=0)
    ) STRICT;
        "
        );
        $visit!(
            _coven_input_ranges,
            "
    CREATE TABLE _coven_input_ranges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        object INTEGER NOT NULL REFERENCES _coven_input_objects(id) ON DELETE CASCADE,
        start INTEGER NOT NULL CHECK(start>=0),
        end INTEGER NOT NULL CHECK(end>start),
        bytes BLOB NOT NULL CHECK(length(bytes)=end-start),
        UNIQUE(object,start)
    ) STRICT;
        "
        );
        $visit!(
            _coven_input_files,
            "
    CREATE TABLE _coven_input_files (
        object INTEGER NOT NULL REFERENCES _coven_input_objects(id) ON DELETE CASCADE,
        device BLOB NOT NULL CHECK(length(device)=8),
        file BLOB NOT NULL CHECK(length(file)=16),
        PRIMARY KEY(object,device,file)
    ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_input_unread,
            "
    CREATE TABLE _coven_input_unread (
        object INTEGER NOT NULL REFERENCES _coven_input_objects(id) ON DELETE CASCADE,
        audience TEXT NOT NULL,
        key BLOB NOT NULL CHECK(length(key)=16),
        PRIMARY KEY(object,audience,key)
    ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_input_budget,
            "
    CREATE TABLE _coven_input_budget (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        maximum INTEGER NOT NULL CHECK(maximum>=0),
        retained INTEGER NOT NULL CHECK(retained>=0),
        evicted INTEGER NOT NULL CHECK(evicted>=0)
    ) STRICT;
    INSERT INTO _coven_input_budget VALUES(1,67108864,0,0);
    CREATE TRIGGER _coven_input_range_added AFTER INSERT ON _coven_input_ranges BEGIN
        UPDATE _coven_input_budget SET retained=retained+length(NEW.bytes) WHERE singleton=1;
    END;
    CREATE TRIGGER _coven_input_range_removed AFTER DELETE ON _coven_input_ranges BEGIN
        UPDATE _coven_input_budget SET retained=retained-length(OLD.bytes) WHERE singleton=1;
    END;
    PRAGMA application_id=1;
        "
        );
    };
}

pub(crate) fn initial_input_schema() -> String {
    let mut sql = String::new();
    macro_rules! append {
        ($table:ident, $sql:literal) => {
            sql.push_str($sql);
        };
    }
    coven_input_tables!(append);
    sql
}
