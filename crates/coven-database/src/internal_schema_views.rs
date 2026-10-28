//! Coven's state as the app's SQL reads it (Appendix C.1, api.md E4): views
//! over coven's own tables, so they are true in every transaction that
//! changes those tables. App SQL reads them and nothing writes them.
//!
//! Each view reads its tables only through joins, never through a subquery:
//! SQLite names the view to the authorizer for each column its joins read,
//! which is how a read through a view is told from a read of coven's tables
//! themselves. A table whose columns a statement then leaves unused, as in
//! `SELECT count(*) FROM coven_uploads`, SQLite reports once more with no
//! column and no view; that read is allowed only for a table the same
//! statement reached through a view.

/// What app SQL may read of coven's: these views, and the tables coven keeps
/// for the app's triggers to follow (`internal_schema_pins.rs`,
/// `internal_schema_upload_queue.rs`).
const READABLE: [&str; 10] = [
    "coven_uploads",
    "coven_upload_pause",
    "coven_sending",
    "coven_downloading",
    super::UPLOAD_QUEUE,
    super::PINS,
    "coven_pending",
    "coven_abandoned",
    "coven_members",
    "coven_devices",
];

pub(super) const SCHEMA: &str = concat!(
    // The files waiting in this device's upload queue, as `uploads()` lists
    // them, with what each waits for and the bytes the provider confirmed.
    // `sending` is this process's, which no table holds (`transfers_in_flight.rs`).
    "CREATE VIEW coven_uploads(table_name,key,position,paused,retry_at,pending,file,
            bytes_confirmed,bytes_total,sending) AS
        SELECT p.table_name,p.key,u.id,z.paused IS NOT NULL,_coven_unix_ms(r.not_before),e.value,",
    uuid_text!("u.file"),
    ",u.bytes_confirmed,u.bytes_total,_coven_sending(u.file)
        FROM _coven_file_uploads AS u
        JOIN _coven_pending_attachments AS p ON p.upload=u.id
        LEFT JOIN _coven_upload_pause AS z
        LEFT JOIN _coven_retries AS r ON r.upload=u.id
        LEFT JOIN _coven_pending AS e ON e.device=u.device AND e.file=u.file;",
    // Whether the app paused uploads, as one row whether or not anything is
    // queued. The pause table's one row, if any, joins a constant one.
    "CREATE VIEW coven_upload_pause(paused) AS
        SELECT z.paused IS NOT NULL FROM (SELECT 1) AS one
        LEFT JOIN _coven_upload_pause AS z;",
    // The queued files this process is sending now, one per upload in
    // flight, each found by its uploader and id: counting a window's or an
    // album's takes a lookup per file. CROSS JOIN keeps SQLite from planning
    // the few files innermost, behind a scan of the queue.
    "CREATE VIEW coven_sending(table_name,key,file,position) AS
        SELECT p.table_name,p.key,",
    uuid_text!("u.file"),
    ",u.id
        FROM json_each(_coven_sending_files()) AS j
        CROSS JOIN _coven_file_uploads AS u
            ON u.device=unhex(substr(j.value,1,16)) AND u.file=unhex(substr(j.value,17))
        CROSS JOIN _coven_pending_attachments AS p ON p.upload=u.id;",
    // The rows whose stored file this process is downloading now to keep
    // whole, one per download in flight, each found by its file: a row that
    // no longer names the file isn't listed.
    "CREATE VIEW coven_downloading(table_name,key,file) AS
        SELECT f.table_name,f.key,",
    uuid_text!("f.file"),
    "
        FROM json_each(_coven_downloading_files()) AS j
        CROSS JOIN _coven_row_files AS f
            ON f.device=unhex(substr(j.value,1,16)) AND f.file=unhex(substr(j.value,17));",
    // Every pending record (§19.1), in the order the records arose; a
    // file's record also names the row on this device whose file it is.
    "CREATE VIEW coven_pending(record,table_name,key,position) AS
        SELECT e.value,coalesce(p.table_name,f.table_name),coalesce(p.key,f.key),e.id
        FROM _coven_pending AS e
        LEFT JOIN _coven_file_uploads AS u ON u.device=e.device AND u.file=e.file
        LEFT JOIN _coven_pending_attachments AS p ON p.upload=u.id
        LEFT JOIN _coven_row_files AS f ON p.upload IS NULL
            AND f.device=e.device AND f.file=e.file;",
    // Each attachment this device abandoned for a newer change to its row's
    // file (§17.2), as `abandoned_attachments()` lists them, until the app
    // dismisses it.
    "CREATE VIEW coven_abandoned(table_name,key,file,reason,position) AS
        SELECT a.table_name,a.key,",
    uuid_text!("a.file"),
    ",a.reason,a.id
        FROM _coven_abandoned_attachments AS a;",
    // Every member this device's applied slots name, former ones included.
    "CREATE VIEW coven_members(member,role,removed,is_self) AS
        SELECT ",
    uuid_text!("m.member"),
    ",m.role,m.removed,d.device IS NOT NULL
        FROM _coven_members AS m
        LEFT JOIN _coven_devices AS d ON d.member=m.member AND d.device=_coven_device();",
    "CREATE VIEW coven_devices(device,member,name,state,is_self) AS
        SELECT d.device,",
    uuid_text!("d.member"),
    ",d.name,d.state,d.device=_coven_device()
        FROM _coven_devices AS d;",
);

/// Whether `name` is one of coven's views or tables app SQL may read.
pub(crate) fn app_readable(name: &str) -> bool {
    READABLE
        .iter()
        .any(|object| object.eq_ignore_ascii_case(name))
}

#[cfg(test)]
#[path = "internal_schema_views_tests.rs"]
mod tests;
