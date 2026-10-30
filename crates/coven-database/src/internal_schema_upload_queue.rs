//! The files waiting in this device's upload queue (§4, api.md E4), kept as
//! a table so the app's local triggers can follow them (§9.4), as they
//! follow `coven_pins`.
//!
//! A file is queued while an upload will switch it to the cloud, as
//! `coven_uploads` lists it. Coven's triggers on the uploads' files insert a
//! file's entry as it is queued and delete it as its upload ends, in the same
//! statement. Its place is its upload's, which files sharing one upload share.

/// The table of queued files, which the app's triggers may follow.
pub(crate) const UPLOAD_QUEUE: &str = "coven_upload_queue";

pub(super) const SCHEMA: &str = "CREATE TABLE coven_upload_queue (
        file TEXT PRIMARY KEY NOT NULL,
        position INTEGER NOT NULL
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX _coven_upload_queue_position ON coven_upload_queue(position);
    CREATE TRIGGER _coven_upload_queue_queued AFTER INSERT ON _coven_upload_files BEGIN
        INSERT INTO coven_upload_queue(file,position) VALUES(NEW.file,NEW.upload);
    END;
    CREATE TRIGGER _coven_upload_queue_ended AFTER DELETE ON _coven_upload_files BEGIN
        DELETE FROM coven_upload_queue WHERE file=OLD.file;
    END;";

#[cfg(test)]
#[path = "internal_schema_upload_queue_tests.rs"]
mod tests;
