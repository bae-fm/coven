//! The devices whose requests ended and whose gone storage was cleared.

macro_rules! coven_ended_requests_tables {
    ($visit:ident) => {
        // The devices gone for good whose storage this device cleared once:
        // it ended the uploads each left, then listed its snapshot folders,
        // deleting each body there that no announcement names (§16.6,
        // §17.10). Their announced snapshots it deletes as its own, by their
        // announcements.
        $visit!(
            _coven_gone_storage_cleared,
            "
            CREATE TABLE _coven_gone_storage_cleared (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8)
            ) STRICT, WITHOUT ROWID;
        "
        );
        // The devices gone for good whose join requests this device ended:
        // it created each one's gone marker, then deleted everything else
        // under the device's join requests (§13.2). A request that arrives
        // later is its own joining device's to delete.
        $visit!(
            _coven_requests_ended,
            "
            CREATE TABLE _coven_requests_ended (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8)
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
