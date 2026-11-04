//! The app's upload pause and its unmetered-only setting (§17.9).

macro_rules! coven_upload_settings_tables {
    ($visit:ident) => {
        // Present while the app has paused uploads (E7).
        $visit!(
            _coven_upload_pause,
            "
            CREATE TABLE _coven_upload_pause (
                paused INTEGER PRIMARY KEY CHECK(paused=1)
            ) STRICT;
        "
        );
        // Present while the app moves files only on unmetered networks (E7).
        $visit!(
            _coven_unmetered_only,
            "
            CREATE TABLE _coven_unmetered_only (
                unmetered_only INTEGER PRIMARY KEY CHECK(unmetered_only=1)
            ) STRICT;
        "
        );
    };
}
