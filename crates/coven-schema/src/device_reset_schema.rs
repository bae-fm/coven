//! This installation's completed replacement notice, and the stop of a copy
//! that found another installation using its device id (§19.3).

macro_rules! coven_device_reset_tables {
    ($visit:ident) => {
        $visit!(
            _coven_device_reset,
            "
            CREATE TABLE _coven_device_reset (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                old_device BLOB NOT NULL CHECK(length(old_device)=8),
                new_device BLOB NOT NULL CHECK(length(new_device)=8)
            ) STRICT;
        "
        );
        $visit!(
            _coven_device_id_in_use,
            "
            CREATE TABLE _coven_device_id_in_use (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                slot BLOB NOT NULL CHECK(length(slot)=8)
            ) STRICT;
        "
        );
    };
}
