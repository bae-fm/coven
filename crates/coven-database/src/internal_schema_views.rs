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

/// The views app SQL may read.
const VIEWS: [&str; 6] = [
    "coven_uploads",
    "coven_pins",
    "coven_pending",
    "coven_abandoned",
    "coven_members",
    "coven_devices",
];

/// Canonical lowercase hyphenated text of a 16-byte UUID, as `UuidText`
/// binds one.
macro_rules! uuid_text {
    ($blob:literal) => {
        concat!(
            "lower(substr(hex(",
            $blob,
            "),1,8)||'-'||substr(hex(",
            $blob,
            "),9,4)||'-'||",
            "substr(hex(",
            $blob,
            "),13,4)||'-'||substr(hex(",
            $blob,
            "),17,4)||'-'||",
            "substr(hex(",
            $blob,
            "),21))"
        )
    };
}

pub(super) const SCHEMA: &str = concat!(
    // The files waiting in this device's upload queue, as `uploads()` lists
    // them, with what each waits for.
    "CREATE VIEW coven_uploads(table_name,key,position,paused,retry_at,pending) AS
        SELECT p.table_name,p.key,u.id,z.paused IS NOT NULL,_coven_unix_ms(r.not_before),e.value
        FROM _coven_file_uploads AS u
        JOIN _coven_pending_attachments AS p ON p.upload=u.id
        LEFT JOIN _coven_upload_pause AS z
        LEFT JOIN _coven_retries AS r ON r.upload=u.id
        LEFT JOIN _coven_pending AS e ON e.device=u.device AND e.file=u.file;",
    // The rows whose file this device keeps pinned (§17.6): a pinned file
    // still queued here, which is whole here, or a stored file with a pinned
    // checked whole copy, or a pin waiting for one.
    "CREATE VIEW coven_pins(table_name,key,kept) AS
        SELECT p.table_name,p.key,1
        FROM _coven_pending_attachments AS p
        JOIN _coven_file_uploads AS u ON u.id=p.upload
        WHERE u.pinned=1
        UNION ALL
        SELECT f.table_name,f.key,c.file_id IS NOT NULL
        FROM _coven_row_files AS f
        LEFT JOIN _coven_pending_attachments AS p ON p.table_name=f.table_name AND p.key=f.key
        LEFT JOIN _coven_cache AS c ON c.namespace=f.namespace
            AND c.file_id=_coven_cache_id(f.device,f.file) AND c.chunk=-2
            AND c.pinned=1 AND c.checked_hash=f.hash
        LEFT JOIN _coven_pin_requests AS q ON q.namespace=f.namespace
            AND q.file_id=_coven_cache_id(f.device,f.file)
        WHERE p.upload IS NULL AND (c.file_id IS NOT NULL OR q.file_id IS NOT NULL);",
    // Every pending record (§19.1); a file's record also names the row on
    // this device whose file it is.
    "CREATE VIEW coven_pending(record,table_name,key) AS
        SELECT e.value,coalesce(p.table_name,f.table_name),coalesce(p.key,f.key)
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

/// Whether `name` is one of the views app SQL may read.
pub(crate) fn app_view(name: &str) -> bool {
    VIEWS.iter().any(|view| view.eq_ignore_ascii_case(name))
}

#[cfg(test)]
#[path = "internal_schema_views_tests.rs"]
mod tests;
