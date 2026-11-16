//! Every pending record (§19.1, api.md E4), kept as a table so the app's
//! local triggers can follow them (§9.4), as they follow `coven_moves`.
//!
//! `_coven_pending_source` says what the table holds: each record of
//! `_coven_pending`, once for each file it names on this device, or once
//! with no file. Coven's triggers on the records, the uploads, the uploads'
//! files and the stored files bring each record they change up to date in
//! the same statement: a row is inserted as its record arises or comes to
//! name its file, changed as what its record says changes, and deleted as
//! its record ends or stops naming its file. A row whose record and file
//! stayed the same is left alone.

/// The table of pending records, which the app's triggers may follow.
pub(crate) const PENDING: &str = "coven_pending";

/// Delete the rows of the records `$records`, a SELECT of record ids, whose
/// file `_coven_pending_source` no longer says the record names.
macro_rules! dropped {
    ($($records:tt)+) => {
        concat!(
            "DELETE FROM coven_pending WHERE position IN (",
            $($records)+,
            ") AND NOT EXISTS(SELECT 1 FROM _coven_pending_source AS s
                 WHERE s.position=coven_pending.position AND s.file IS coven_pending.file);"
        )
    };
}

/// Insert a row for each file `_coven_pending_source` says one of the
/// records `$records` names that the table doesn't list yet.
macro_rules! named {
    ($($records:tt)+) => {
        concat!(
            "INSERT INTO coven_pending(record,file,position,cause,move,operation,subject_kind,
                 reason_kind,original_path)
             SELECT s.record,s.file,s.position,s.cause,s.move,s.operation,s.subject_kind,
                 s.reason_kind,s.original_path
             FROM _coven_pending_source AS s WHERE s.position IN (",
            $($records)+,
            ") AND NOT EXISTS(SELECT 1 FROM coven_pending AS p
                 WHERE p.position=s.position AND p.file IS s.file);"
        )
    };
}

/// Bring the files the records `$records` name up to date.
macro_rules! refresh {
    ($($records:tt)+) => {
        concat!(dropped!($($records)+), named!($($records)+))
    };
}

/// The records about the stored bytes the row `$row` names by its device
/// and object.
macro_rules! about {
    ($row:literal) => {
        concat!(
            "SELECT e.id FROM _coven_pending AS e WHERE e.device=",
            $row,
            ".device AND e.object=",
            $row,
            ".object"
        )
    };
}

/// The records about the bytes of the upload whose file the row `$row` of
/// `_coven_upload_files` lists.
macro_rules! uploaded {
    ($row:literal) => {
        concat!(
            "SELECT e.id FROM _coven_file_uploads AS u
             JOIN _coven_pending AS e ON e.device=u.device AND e.object=u.object
             WHERE u.id=",
            $row,
            ".upload"
        )
    };
}

/// The table and its index, as every store holds them. A record names each
/// file once, or no file, so `position` and `file` together name a row.
pub(super) const TABLE: &str = "CREATE TABLE coven_pending (
        record BLOB NOT NULL,
        file TEXT,
        position INTEGER NOT NULL,
        cause TEXT,
        move INTEGER,
        operation INTEGER,
        subject_kind TEXT,
        reason_kind TEXT,
        original_path BLOB
    ) STRICT;
    CREATE UNIQUE INDEX _coven_pending_files ON coven_pending(position,ifnull(file,x''));";

/// The view saying what the table holds, and the triggers keeping the table
/// to it.
///
/// The view lists every record in the order the records arose; a record
/// about stored bytes also names each file over them on this device: the
/// files an upload of them switches, or, while it switches none, the stored
/// files, and a record about a pin coven keeps names its file. A switched
/// file is stored before its upload stops listing it, so the record names
/// it throughout its switch. Its cause is the text of the error behind
/// it, when this run met that error and no app call received it. A record
/// about a move names it in `move`, the id `coven_uploads.move` holds.
/// `operation` is the journal row a record describes. `subject_kind` and
/// `reason_kind` name the variants of the record's subject and reason in
/// snake case, as `PendingSubject` and `PendingReason` name them;
/// `original_path` is the path, as the bytes `coven_user_files.path` holds,
/// of a reason that an original is missing or changed, and NULL for any
/// other.
pub(super) const SOURCE: &str = concat!(
    "CREATE VIEW _coven_pending_source(record,file,position,cause,move,operation,subject_kind,
            reason_kind,original_path) AS
        SELECT e.value,coalesce(l.file,f.file,e.pin),e.id,e.cause,e.move,e.operation,
            _coven_variant_kind(CAST(e.value AS TEXT)->'$.subject'),
            _coven_variant_kind(CAST(e.value AS TEXT)->'$.reason'),
            _coven_json_bytes(CAST(e.value AS TEXT)->'$.reason.Original.path')
        FROM _coven_pending AS e
        LEFT JOIN _coven_file_uploads AS u ON u.device=e.device AND u.object=e.object
        LEFT JOIN _coven_upload_files AS l ON l.upload=u.id
        LEFT JOIN _coven_stored_files AS f ON l.file IS NULL
            AND f.device=e.device AND f.object=e.object;
    CREATE TRIGGER _coven_pending_arose AFTER INSERT ON _coven_pending BEGIN ",
    named!("SELECT NEW.id"),
    " END;
    CREATE TRIGGER _coven_pending_changed
        AFTER UPDATE OF value,cause,move,operation,device,object,pin ON _coven_pending BEGIN ",
    dropped!("SELECT NEW.id"),
    "UPDATE coven_pending SET (record,cause,move,operation,subject_kind,reason_kind,original_path)=
         (SELECT s.record,s.cause,s.move,s.operation,s.subject_kind,s.reason_kind,s.original_path
          FROM _coven_pending_source AS s WHERE s.position=NEW.id LIMIT 1)
     WHERE position=NEW.id
       AND (record,cause,move,operation,subject_kind,reason_kind,original_path) IS NOT
         (SELECT s.record,s.cause,s.move,s.operation,s.subject_kind,s.reason_kind,s.original_path
          FROM _coven_pending_source AS s WHERE s.position=NEW.id LIMIT 1);",
    named!("SELECT NEW.id"),
    " END;
    CREATE TRIGGER _coven_pending_ended AFTER DELETE ON _coven_pending BEGIN
        DELETE FROM coven_pending WHERE position=OLD.id;
    END;
    CREATE TRIGGER _coven_pending_upload_added AFTER INSERT ON _coven_file_uploads BEGIN ",
    refresh!(about!("NEW")),
    " END;
    CREATE TRIGGER _coven_pending_upload_changed
        AFTER UPDATE OF device,object ON _coven_file_uploads BEGIN ",
    refresh!(about!("OLD")),
    refresh!(about!("NEW")),
    " END;
    CREATE TRIGGER _coven_pending_upload_removed AFTER DELETE ON _coven_file_uploads BEGIN ",
    refresh!(about!("OLD")),
    " END;
    CREATE TRIGGER _coven_pending_upload_file_added AFTER INSERT ON _coven_upload_files BEGIN ",
    refresh!(uploaded!("NEW")),
    " END;
    CREATE TRIGGER _coven_pending_upload_file_changed
        AFTER UPDATE OF file,upload ON _coven_upload_files BEGIN ",
    refresh!(uploaded!("OLD")),
    refresh!(uploaded!("NEW")),
    " END;
    CREATE TRIGGER _coven_pending_upload_file_removed AFTER DELETE ON _coven_upload_files BEGIN ",
    refresh!(uploaded!("OLD")),
    " END;
    CREATE TRIGGER _coven_pending_stored_file_added AFTER INSERT ON _coven_stored_files BEGIN ",
    refresh!(about!("NEW")),
    " END;
    CREATE TRIGGER _coven_pending_stored_file_changed
        AFTER UPDATE OF file,device,object ON _coven_stored_files BEGIN ",
    refresh!(about!("OLD")),
    refresh!(about!("NEW")),
    " END;
    CREATE TRIGGER _coven_pending_stored_file_removed AFTER DELETE ON _coven_stored_files BEGIN ",
    refresh!(about!("OLD")),
    " END;"
);

#[cfg(test)]
#[path = "internal_schema_pending_tests.rs"]
mod tests;
