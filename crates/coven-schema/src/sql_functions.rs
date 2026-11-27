//! The SQL functions coven registers on every connection to a store's database, which its views,
//! triggers and migrations call and an app's SQL may call.

/// One SQL function coven registers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqlFunction {
    /// The function's name.
    pub name: &'static str,
    /// How many arguments it takes, or -1 for any number.
    pub arguments: i32,
    /// Whether SQLite may treat its result as a function of its arguments alone, which an index,
    /// a CHECK or a generated column needs.
    pub deterministic: bool,
}

const fn function(name: &'static str, arguments: i32, deterministic: bool) -> SqlFunction {
    SqlFunction { name, arguments, deterministic }
}

/// Every SQL function coven registers on a store's connection, by name. A test in coven-database
/// compares this list with the functions a connection holds after `make_readable_objects`, so a
/// function added, renamed or changed there fails that test until this list agrees.
pub const SQL_FUNCTIONS: [SqlFunction; 21] = [
    function("_coven_device", 0, false),
    function("_coven_file_id", 1, true),
    function("_coven_file_of_key", 1, true),
    function("_coven_json_bytes", 1, true),
    function("_coven_loss_order", 2, true),
    function("_coven_request_joiner", 1, true),
    function("_coven_request_member", 1, true),
    function("_coven_rowid_changed", 0, false),
    function("_coven_stored_device", 1, true),
    function("_coven_stored_object", 1, true),
    function("_coven_stored_object_path", 1, true),
    function("_coven_sync_state", 14, false),
    function("_coven_unix_ms", 1, true),
    function("_coven_unix_nanos", 1, true),
    function("_coven_unix_seconds", 1, true),
    function("_coven_upload_phase", 16, false),
    function("_coven_variant_kind", 1, true),
    function("coven_applying", 0, false),
    function("coven_file_device", 1, true),
    function("coven_key", -1, true),
    function("coven_path_text", 1, true),
];
