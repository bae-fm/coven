//! The files a switch to the cloud asked this device to keep whole (§17.8).

macro_rules! coven_pin_requests_tables {
    ($visit:ident) => {
        // Each file a switch to the cloud asked this device to keep whole,
        // which coven downloads itself until it is (§17.8): from coven's
        // copy of an app file, which the request keeps until then, or from
        // storage. A user's original is never read for the cache. The
        // triggers keep `coven_pin_requests` (`internal_schema_pin_requests.rs`)
        // listing each request, for the app's triggers to follow (§9.4).
        $visit!(_coven_pin_requests, "
            CREATE TABLE _coven_pin_requests (
                file TEXT PRIMARY KEY NOT NULL,
                copy TEXT
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_pin_requests_copy ON _coven_pin_requests(copy) WHERE copy IS NOT NULL;
            CREATE TRIGGER _coven_pin_requests_added AFTER INSERT ON _coven_pin_requests BEGIN
                INSERT INTO coven_pin_requests(file) VALUES(NEW.file);
            END;
            CREATE TRIGGER _coven_pin_requests_ended AFTER DELETE ON _coven_pin_requests BEGIN
                DELETE FROM coven_pin_requests WHERE file=OLD.file;
            END;
        ");
    };
}
