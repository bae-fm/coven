//! The files waiting in this device's upload queue (§4, api.md E4), kept as
//! a table so the app's local triggers can follow them (§9.4), as they
//! follow `coven_pins`.
//!
//! A file is queued while its row's pending attachment names its upload, as
//! `coven_uploads` lists it. Coven's triggers on the pending attachments and
//! the uploads insert a row's entry as its attachment is queued, and delete
//! it as the attachment ends, in the same statement. A newer file for the
//! same row replaces the entry: its file and place are the newer upload's.

/// The table of queued files, which the app's triggers may follow.
pub(crate) const UPLOAD_QUEUE: &str = "coven_upload_queue";

/// Insert the entries of the attachments `$attachments` names, a condition
/// on `p`, the pending attachments, each with its upload.
macro_rules! queue {
    ($attachments:literal) => {
        concat!(
            "INSERT INTO coven_upload_queue(table_name,key,file,position)
                 SELECT p.table_name,p.key,",
            uuid_text!("u.file"),
            ",u.id FROM _coven_pending_attachments AS p
                 JOIN _coven_file_uploads AS u ON u.id=p.upload WHERE ",
            $attachments,
            ";"
        )
    };
}

pub(super) const SCHEMA: &str = concat!(
    "CREATE TABLE coven_upload_queue (
        table_name TEXT NOT NULL,
        key BLOB NOT NULL,
        file TEXT NOT NULL,
        position INTEGER PRIMARY KEY,
        UNIQUE(table_name,key)
    ) STRICT;
    CREATE TRIGGER _coven_upload_queue_attached AFTER INSERT ON _coven_pending_attachments BEGIN ",
    queue!("p.upload=NEW.upload"),
    " END;
    CREATE TRIGGER _coven_upload_queue_attachment_changed
        AFTER UPDATE OF upload,table_name,key ON _coven_pending_attachments BEGIN
        DELETE FROM coven_upload_queue WHERE position=OLD.upload; ",
    queue!("p.upload=NEW.upload"),
    " END;
    CREATE TRIGGER _coven_upload_queue_attachment_ended AFTER DELETE ON _coven_pending_attachments BEGIN
        DELETE FROM coven_upload_queue WHERE position=OLD.upload;
    END;
    CREATE TRIGGER _coven_upload_queue_upload_changed AFTER UPDATE OF id,file ON _coven_file_uploads BEGIN
        DELETE FROM coven_upload_queue WHERE position=OLD.id; ",
    queue!("p.upload=NEW.id"),
    " END;",
);

#[cfg(test)]
#[path = "internal_schema_upload_queue_tests.rs"]
mod tests;
