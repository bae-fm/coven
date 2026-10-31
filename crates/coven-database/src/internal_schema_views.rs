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

/// What app SQL may read of coven's: these views, the files table, and the
/// tables coven keeps for the app's triggers to follow
/// (`internal_schema_pins.rs`, `internal_schema_upload_queue.rs`).
const READABLE: [&str; 13] = [
    super::FILES,
    "coven_user_files",
    "coven_uploads",
    "coven_upload_pause",
    "coven_sending",
    "coven_downloading",
    super::UPLOAD_QUEUE,
    super::PINS,
    "coven_pending",
    "coven_members",
    "coven_devices",
    "coven_circles",
    "coven_circle_members",
];

pub(super) const SCHEMA: &str = concat!(
    // The user's originals this device records, by file: the path, as the
    // operating system's bytes, which `coven_path_text` shows, and the size
    // and exact modification time recorded when each was staged, as whole
    // seconds since the Unix epoch rounded down and the nanoseconds past
    // that second (§17.3).
    "CREATE VIEW coven_user_files(file,path,size,modified_seconds,modified_nanos) AS
        SELECT u.file,u.path,u.size,_coven_unix_seconds(u.modified_at),
            _coven_unix_nanos(u.modified_at)
        FROM _coven_user_files AS u;",
    // The files waiting in this device's upload queue, as `uploads()` lists
    // them, with what each waits for and the bytes the provider confirmed.
    // Files sharing one upload share its place. The pause holds only
    // trailing uploads (§17.4). `sending` is this process's, which no table
    // holds (`transfers_in_flight.rs`). `move` is the upload's move, for the
    // app to cancel, but never a bound upload's, which can't be cancelled.
    "CREATE VIEW coven_uploads(file,position,paused,retry_at,pending,
            bytes_confirmed,bytes_total,sending,move) AS
        SELECT l.file,u.id,z.paused IS NOT NULL AND u.holds IS NULL,_coven_unix_ms(r.not_before),e.value,
            u.bytes_confirmed,u.bytes_total,_coven_sending(u.file),
            CASE WHEN u.holds IS NULL THEN u.move END
        FROM _coven_upload_files AS l
        JOIN _coven_file_uploads AS u ON u.id=l.upload
        LEFT JOIN _coven_upload_pause AS z
        LEFT JOIN _coven_retries AS r ON r.upload=u.id
        LEFT JOIN _coven_pending AS e ON e.device=u.device AND e.file=u.file;",
    // Whether the app paused uploads, as one row whether or not anything is
    // queued. The pause table's one row, if any, joins a constant one.
    "CREATE VIEW coven_upload_pause(paused) AS
        SELECT z.paused IS NOT NULL FROM (SELECT 1) AS one
        LEFT JOIN _coven_upload_pause AS z;",
    // The queued files this process is sending now, by each upload in
    // flight, each found by its uploader and object: counting a window's or
    // an album's takes a lookup per file. CROSS JOIN keeps SQLite from
    // planning the few files innermost, behind a scan of the queue.
    "CREATE VIEW coven_sending(file,position) AS
        SELECT l.file,u.id
        FROM json_each(_coven_sending_files()) AS j
        CROSS JOIN _coven_file_uploads AS u
            ON u.device=unhex(substr(j.value,1,16)) AND u.file=unhex(substr(j.value,17))
        CROSS JOIN _coven_upload_files AS l ON l.upload=u.id;",
    // The stored files this process is downloading now to keep whole, by
    // each download in flight, each found by its stored bytes.
    "CREATE VIEW coven_downloading(file) AS
        SELECT f.file
        FROM json_each(_coven_downloading_files()) AS j
        CROSS JOIN _coven_stored_files AS f
            ON f.device=unhex(substr(j.value,1,16)) AND f.object=unhex(substr(j.value,17));",
    // Every pending record (§19.1), in the order the records arose; a
    // record about stored bytes also names each file over them on this
    // device: the files an upload switches, or the stored files, and a
    // record about a pin coven keeps names its file.
    "CREATE VIEW coven_pending(record,file,position) AS
        SELECT e.value,coalesce(l.file,f.file,e.pin),e.id
        FROM _coven_pending AS e
        LEFT JOIN _coven_file_uploads AS u ON u.device=e.device AND u.file=e.file
        LEFT JOIN _coven_upload_files AS l ON l.upload=u.id
        LEFT JOIN _coven_stored_files AS f ON u.id IS NULL
            AND f.device=e.device AND f.object=e.file;",
    // Every member this device's applied slots name, former ones included,
    // with their public keys and how they reach storage: the account or the
    // S3 key id the stored access names, whichever it is.
    "CREATE VIEW coven_members(member,role,removed,is_self,signing,sealing,account,
            access_key_id) AS
        SELECT ",
    uuid_text!("m.member"),
    ",m.role,m.removed,d.device IS NOT NULL,m.signing,m.sealing,
            json_extract(CAST(m.access AS TEXT),'$.ProviderAccount'),
            json_extract(CAST(m.access AS TEXT),'$.S3AccessKey.access_key_id')
        FROM _coven_members AS m
        LEFT JOIN _coven_devices AS d ON d.member=m.member AND d.device=_coven_device();",
    "CREATE VIEW coven_devices(device,member,name,state,is_self) AS
        SELECT d.device,",
    uuid_text!("d.member"),
    ",d.name,d.state,d.device=_coven_device()
        FROM _coven_devices AS d;",
    // The circles this device's member is in, as `Circles::list()` lists
    // them: a deleted circle isn't listed.
    "CREATE VIEW coven_circles(circle,name) AS
        SELECT c.circle,c.name
        FROM _coven_devices AS me
        JOIN _coven_circle_members AS m ON m.member=me.member
        JOIN _coven_circles AS c ON c.circle=m.circle
        LEFT JOIN _coven_circle_deletions AS x ON x.circle=c.circle
        WHERE me.device=_coven_device() AND x.circle IS NULL;",
    // Each circle's members who remain in the store, as
    // `Circles::members()` lists them: none for a deleted circle.
    "CREATE VIEW coven_circle_members(circle,member,is_self) AS
        SELECT m.circle,",
    uuid_text!("m.member"),
    ",me.device IS NOT NULL
        FROM _coven_circle_members AS m
        JOIN _coven_members AS s ON s.member=m.member
        LEFT JOIN _coven_circle_deletions AS x ON x.circle=m.circle
        LEFT JOIN _coven_devices AS me ON me.member=m.member AND me.device=_coven_device()
        WHERE s.removed=0 AND x.circle IS NULL;",
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
