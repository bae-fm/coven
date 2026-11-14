//! The tables of the store's committed state: its identity and name (§10.3),
//! membership and audiences (§10, §15).

macro_rules! coven_store_state_tables {
    ($visit:ident) => {
        $visit!(_coven_store, "
            CREATE TABLE _coven_store (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                store BLOB NOT NULL CHECK(length(store)=16),
                name TEXT NOT NULL,
                named_at BLOB NOT NULL CHECK(length(named_at)=8 AND named_at>x'0000000000000000'),
                created INTEGER NOT NULL CHECK(created IN (0,1)),
                joined INTEGER NOT NULL CHECK(joined IN (0,1))
            ) STRICT;
        ");
        $visit!(
            _coven_members,
            "
            CREATE TABLE _coven_members (
                member TEXT PRIMARY KEY NOT NULL CHECK(length(member)=36),
                signing BLOB NOT NULL CHECK(length(signing)=32),
                sealing BLOB NOT NULL CHECK(length(sealing)=32),
                access BLOB,
                role TEXT NOT NULL CHECK(role IN ('admin','member')),
                removed INTEGER NOT NULL CHECK(removed IN (0,1)),
                joined_circle TEXT,
                joined_person TEXT CHECK(joined_person IS NULL OR joined_circle IS NOT NULL)
            ) STRICT, WITHOUT ROWID;
        "
        );
        // Every public key pair bound by a counted administrative slot (§10).
        $visit!(
            _coven_bound_keys,
            "
            CREATE TABLE _coven_bound_keys (
                signing BLOB NOT NULL CHECK(length(signing)=32),
                sealing BLOB NOT NULL CHECK(length(sealing)=32),
                PRIMARY KEY(signing,sealing)
            ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_devices,
            "
            CREATE TABLE _coven_devices (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                member TEXT NOT NULL REFERENCES _coven_members(member),
                name TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('active','removed','replaced'))
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_devices_member ON _coven_devices(member);
        "
        );
        $visit!(
            _coven_circles,
            "
            CREATE TABLE _coven_circles (
                circle TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                base INTEGER NOT NULL CHECK(base BETWEEN 0 AND 4294967295)
            ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_circle_deletions,
            "
            CREATE TABLE _coven_circle_deletions (
                circle TEXT PRIMARY KEY NOT NULL,
                slot BLOB NOT NULL CHECK(length(slot)=8 AND slot>x'0000000000000000')
            ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_circle_members,
            "
            CREATE TABLE _coven_circle_members (
                circle TEXT NOT NULL REFERENCES _coven_circles(circle),
                member TEXT NOT NULL REFERENCES _coven_members(member),
                PRIMARY KEY(circle,member)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_circle_members_member ON _coven_circle_members(member);
        "
        );
    };
}
