//! coven's bookkeeping schema.
//!
//! coven owns its device-local bookkeeping tables — `protocol_state`,
//! `materialized_commits`, `snapshot_coverage`, `store_writes`,
//! `outbound_membership_mutation`, `outbound_store_snapshot`,
//! `local_blob_refs`, `local_cleanup_intents`, exact prepared Store objects, and
//! row-bound blob locators — all
//! created STRICT by [`apply_coven_schema`], which coven
//! runs against the connection it owns during open. The host does not implement
//! any of this; app SQL goes through [`crate::CovenHandle::sql`] or
//! [`crate::CovenHandle::write`].

macro_rules! coven_tables {
    ($visit:ident) => {
        $visit!(
            protocol_state,
            "
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
"
        );
        $visit!(
            materialized_commits,
            "
    device_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    commit_ref TEXT NOT NULL CHECK (json_valid(commit_ref)),
    PRIMARY KEY (device_id, seq)
"
        );
        $visit!(
            snapshot_coverage,
            "
    device_id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL CHECK (seq > 0),
    commit_ref TEXT NOT NULL CHECK (json_valid(commit_ref)),
    snapshot_hash TEXT NOT NULL CHECK (length(snapshot_hash) = 64)
"
        );
        $visit!(
            local_blob_refs,
            "
    table_name TEXT NOT NULL,
    row_id TEXT NOT NULL,
    column_name TEXT NOT NULL,
    row_stamp TEXT NOT NULL,
    namespace TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    path TEXT NOT NULL,
    plaintext_size INTEGER NOT NULL CHECK (plaintext_size >= 0),
    plaintext_hash TEXT NOT NULL CHECK (length(plaintext_hash) = 64),
    PRIMARY KEY (table_name, row_id, column_name, row_stamp)
"
        );
        $visit!(
            cloud_outbox,
            "
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation TEXT NOT NULL CHECK (operation IN ('upload', 'delete')),
    table_name TEXT,
    row_id TEXT,
    column_name TEXT,
    row_stamp TEXT,
    root_table TEXT,
    root_id TEXT,
    row_ref TEXT CHECK (row_ref IS NULL OR json_valid(row_ref)),
    upload_state TEXT CHECK (upload_state IS NULL OR json_valid(upload_state)),
    stored_ref TEXT CHECK (stored_ref IS NULL OR json_valid(stored_ref)),
    source_path TEXT,
    retain_pinned INTEGER CHECK (retain_pinned IS NULL OR retain_pinned IN (0, 1)),
    created_at TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    last_attempt_at TEXT,
    CHECK (
        (operation = 'upload' AND table_name IS NOT NULL AND row_id IS NOT NULL
         AND column_name IS NOT NULL AND row_stamp IS NOT NULL
         AND root_table IS NOT NULL AND root_id IS NOT NULL AND row_ref IS NOT NULL
         AND stored_ref IS NULL AND source_path IS NOT NULL AND retain_pinned IS NOT NULL
         AND upload_state IS NOT NULL)
        OR
        (operation = 'delete' AND table_name IS NULL AND row_id IS NULL
         AND column_name IS NULL AND row_stamp IS NULL
         AND root_table IS NULL AND root_id IS NULL AND row_ref IS NULL
         AND stored_ref IS NOT NULL AND source_path IS NULL AND retain_pinned IS NULL
         AND upload_state IS NULL)
    ),
    UNIQUE (operation, table_name, row_id, column_name, row_stamp),
    UNIQUE (stored_ref)
"
        );
        $visit!(
            blob_make_remote_intents,
            "
    root_table TEXT NOT NULL,
    root_id TEXT NOT NULL,
    retain_pinned INTEGER NOT NULL CHECK (retain_pinned IN (0, 1)),
    state TEXT NOT NULL CHECK (state IN ('uploading', 'cancelling', 'publishing')),
    write_id TEXT UNIQUE,
    CHECK ((state = 'publishing') = (write_id IS NOT NULL)),
    PRIMARY KEY (root_table, root_id)
"
        );
        $visit!(
            local_cleanup_intents,
            "
    namespace TEXT NOT NULL,
    blob_id   TEXT NOT NULL,
    copy_identity TEXT NOT NULL CHECK (copy_identity = 'local' OR length(copy_identity) = 64),
    PRIMARY KEY (namespace, blob_id, copy_identity)
"
        );
        $visit!(
            published_blob_drop_intents,
            "
    seq INTEGER NOT NULL CHECK (seq > 0),
    namespace TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    size INTEGER NOT NULL CHECK (size >= 0),
    plaintext_hash TEXT NOT NULL,
    locator_hash TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('drop', 'cache', 'pin')),
    PRIMARY KEY (seq, namespace, blob_id, locator_hash)
"
        );
        $visit!(
            store_writes,
            "
    ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
    write_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (json_valid(status)),
    affected_rows TEXT NOT NULL CHECK (json_valid(affected_rows)),
    changeset BLOB NOT NULL,
    inverse_changeset BLOB NOT NULL,
    base TEXT NOT NULL CHECK (json_valid(base)),
    blob_facts TEXT NOT NULL CHECK (json_valid(blob_facts)),
    prepared TEXT CHECK (prepared IS NULL OR json_valid(prepared))
"
        );
        $visit!(
            store_write_blob_leases,
            "
    write_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    blob_id TEXT NOT NULL,
    PRIMARY KEY (write_id, namespace, blob_id),
    FOREIGN KEY (write_id) REFERENCES store_writes(write_id)
"
        );
        $visit!(
            store_write_partitions,
            "
    write_id TEXT NOT NULL,
    audience TEXT NOT NULL,
    control_coord TEXT,
    changeset BLOB NOT NULL,
    PRIMARY KEY (write_id, audience),
    FOREIGN KEY (write_id) REFERENCES store_writes(write_id) ON DELETE CASCADE,
    CHECK (
        (audience = 'store' AND control_coord IS NULL)
        OR
        (audience NOT IN ('store', 'local') AND json_valid(control_coord))
    )
"
        );
        $visit!(
            remote_objects,
            "
    object_id TEXT PRIMARY KEY CHECK (length(object_id) = 64),
    state TEXT NOT NULL CHECK (json_valid(state))
"
        );
        $visit!(
            protocol_inert_objects,
            "
    object_id TEXT PRIMARY KEY CHECK (length(object_id) = 64),
    state TEXT NOT NULL CHECK (json_valid(state))
"
        );
        $visit!(
            reclaimed_store_packages,
            "
    object_id TEXT PRIMARY KEY CHECK (length(object_id) = 64),
    authorization_hash TEXT NOT NULL UNIQUE CHECK (length(authorization_hash) = 64),
    state TEXT NOT NULL CHECK (json_valid(state)),
    FOREIGN KEY (authorization_hash) REFERENCES store_reclaim_operations(authorization_hash)
"
        );
        $visit!(
            store_write_packages,
            "
    write_id TEXT NOT NULL,
    audience TEXT NOT NULL,
    remote_object_id TEXT NOT NULL CHECK (length(remote_object_id) = 64),
    PRIMARY KEY (write_id, audience),
    FOREIGN KEY (write_id) REFERENCES store_writes(write_id) ON DELETE CASCADE,
    FOREIGN KEY (remote_object_id) REFERENCES remote_objects(object_id)
"
        );
        $visit!(
            store_write_blobs,
            "
    write_id TEXT NOT NULL,
    audience TEXT NOT NULL,
    locator_hash TEXT NOT NULL CHECK (length(locator_hash) = 64),
    remote_object_id TEXT NOT NULL CHECK (length(remote_object_id) = 64),
    spool_path TEXT,
    PRIMARY KEY (write_id, audience, remote_object_id),
    FOREIGN KEY (write_id) REFERENCES store_writes(write_id) ON DELETE CASCADE,
    FOREIGN KEY (remote_object_id) REFERENCES remote_objects(object_id)
"
        );
        $visit!(
            blob_locators,
            "
    remote_object_id TEXT PRIMARY KEY CHECK (length(remote_object_id) = 64),
    locator_hash TEXT NOT NULL CHECK (length(locator_hash) = 64),
    FOREIGN KEY (remote_object_id) REFERENCES remote_objects(object_id)
"
        );
        $visit!(
            row_blob_locators,
            "
    table_name TEXT NOT NULL,
    row_id TEXT NOT NULL,
    column_name TEXT NOT NULL,
    row_stamp TEXT NOT NULL,
    audience_authority TEXT NOT NULL CHECK (json_valid(audience_authority)),
    remote_object_id TEXT NOT NULL CHECK (length(remote_object_id) = 64),
    PRIMARY KEY (table_name, row_id, column_name, row_stamp),
    FOREIGN KEY (remote_object_id) REFERENCES blob_locators(remote_object_id)
"
        );
        $visit!(
            outbound_membership_mutation,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    intent_hash TEXT NOT NULL CHECK (length(intent_hash) = 64),
    plan_bytes BLOB NOT NULL,
    progress_bytes BLOB NOT NULL
"
        );
        $visit!(
            outbound_store_snapshot,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    snapshot_ref TEXT NOT NULL CHECK (json_valid(snapshot_ref)),
    meta_prepared TEXT NOT NULL CHECK (json_valid(meta_prepared)),
    image_ref TEXT NOT NULL CHECK (json_valid(image_ref)),
    image_prepared TEXT NOT NULL CHECK (json_valid(image_prepared)),
    image_bytes BLOB NOT NULL,
    meta_bytes BLOB NOT NULL,
    blobs TEXT NOT NULL CHECK (json_valid(blobs))
"
        );
        $visit!(
            published_store_snapshot,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    snapshot_ref TEXT NOT NULL CHECK (json_valid(snapshot_ref)),
    successor_slot TEXT NOT NULL CHECK (json_valid(successor_slot)),
    meta_bytes BLOB NOT NULL
"
        );
        $visit!(
            snapshot_blob_spool_cleanup,
            "
    path TEXT PRIMARY KEY
"
        );
        $visit!(
            outbound_store_acks,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    ack_ref TEXT NOT NULL CHECK (json_valid(ack_ref)),
    ack_bytes BLOB NOT NULL,
    prepared_object TEXT NOT NULL CHECK (json_valid(prepared_object)),
    activation TEXT NOT NULL CHECK (json_valid(activation))
"
        );
        $visit!(
            published_store_acks,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    ack_ref TEXT NOT NULL CHECK (json_valid(ack_ref)),
    successor_slot TEXT NOT NULL CHECK (json_valid(successor_slot))
"
        );
        $visit!(
            activated_store_acks,
            "
    device_id TEXT PRIMARY KEY,
    ack_ref TEXT NOT NULL CHECK (json_valid(ack_ref)),
    activating_commit TEXT NOT NULL CHECK (json_valid(activating_commit))
"
        );
        $visit!(
            store_device_exclusion_freezes,
            "
    proposal_id TEXT PRIMARY KEY CHECK (length(proposal_id) = 64),
    proposal_ref TEXT NOT NULL CHECK (json_valid(proposal_ref)),
    target_cut TEXT NOT NULL CHECK (json_valid(target_cut))
"
        );
        $visit!(
            outbound_store_device_exclusion,
            "
    operation_id TEXT PRIMARY KEY CHECK (length(operation_id) = 64),
    active_key INTEGER UNIQUE CHECK (active_key IS NULL OR active_key = 1),
    state TEXT NOT NULL CHECK (json_valid(state))
"
        );
        $visit!(
            store_reclaim_operations,
            "
    authorization_hash TEXT PRIMARY KEY CHECK (length(authorization_hash) = 64),
    state TEXT NOT NULL CHECK (json_valid(state))
"
        );
        $visit!(
            store_protocol_root_authority,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    store_root_hash TEXT NOT NULL CHECK (length(store_root_hash) = 64),
    store_protocol_root_bytes BLOB NOT NULL,
    store_root_object TEXT NOT NULL CHECK (json_valid(store_root_object))
"
        );
        $visit!(
            local_store_protocol_root,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    store_root_hash TEXT NOT NULL CHECK (length(store_root_hash) = 64),
    store_protocol_root_bytes BLOB NOT NULL,
    prepared_object TEXT NOT NULL CHECK (json_valid(prepared_object))
"
        );
        $visit!(
            local_store_device_registration,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    device_id TEXT NOT NULL UNIQUE,
    registration_hash TEXT NOT NULL UNIQUE CHECK (length(registration_hash) = 64),
    registration_bytes BLOB NOT NULL,
    prepared_object TEXT NOT NULL CHECK (json_valid(prepared_object)),
    initial_ack_ref TEXT NOT NULL CHECK (json_valid(initial_ack_ref)),
    initial_ack_bytes BLOB NOT NULL,
    initial_ack_prepared TEXT NOT NULL CHECK (json_valid(initial_ack_prepared)),
    state TEXT NOT NULL CHECK (json_valid(state))
"
        );
        $visit!(
            local_store_device_retirement,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    payload BLOB NOT NULL,
    published INTEGER NOT NULL CHECK (published IN (0, 1))
"
        );
        $visit!(
            local_store_founder_graph,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    membership_graph TEXT NOT NULL CHECK (json_valid(membership_graph))
"
        );
        $visit!(
            serial_head_receipt,
            "
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    head_bytes BLOB NOT NULL,
    version_token TEXT NOT NULL CHECK (json_valid(version_token)),
    commit_ref TEXT NOT NULL CHECK (json_valid(commit_ref))
"
        );
        $visit!(
            store_device_registration_activations,
            "
    device_id TEXT PRIMARY KEY,
    registration_hash TEXT NOT NULL CHECK (length(registration_hash) = 64),
    author_pubkey TEXT NOT NULL,
    device_signing_pubkey TEXT NOT NULL,
    registration_bytes BLOB NOT NULL,
    registration_object TEXT NOT NULL CHECK (json_valid(registration_object)),
    activation_authority TEXT NOT NULL CHECK (json_valid(activation_authority)),
    UNIQUE (device_id, registration_hash),
    UNIQUE (registration_object)
"
        );
        $visit!(
            store_device_state_snapshots,
            "
    commit_ref TEXT PRIMARY KEY CHECK (json_valid(commit_ref)),
    state TEXT NOT NULL CHECK (json_valid(state))
"
        );
        $visit!(
            circle_control_activations,
            "
    circle_id TEXT NOT NULL,
    control_coord TEXT NOT NULL CHECK (json_valid(control_coord)),
    stream_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    commit_hash TEXT NOT NULL CHECK (length(commit_hash) = 64),
    control_bytes BLOB NOT NULL,
    PRIMARY KEY (circle_id, control_coord),
    UNIQUE (circle_id, stream_id, seq)
"
        );
        $visit!(
            circle_operations,
            "
    operation_id TEXT PRIMARY KEY,
    circle_id TEXT NOT NULL UNIQUE,
    payload BLOB NOT NULL
"
        );
        $visit!(
            circle_access_cache,
            "
    circle_id TEXT NOT NULL,
    control_coord TEXT NOT NULL CHECK (json_valid(control_coord)),
    owner_pubkey TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('active', 'inactive')),
    access_bytes BLOB NOT NULL,
    PRIMARY KEY (circle_id, control_coord, owner_pubkey),
    FOREIGN KEY (circle_id, control_coord)
        REFERENCES circle_control_activations(circle_id, control_coord)
"
        );
        $visit!(
            circle_roster_cache,
            "
    circle_id TEXT NOT NULL,
    control_coord TEXT NOT NULL CHECK (json_valid(control_coord)),
    roster_bytes BLOB NOT NULL,
    PRIMARY KEY (circle_id, control_coord),
    FOREIGN KEY (circle_id, control_coord)
        REFERENCES circle_control_activations(circle_id, control_coord)
"
        );
        $visit!(
            circle_metadata_cache,
            "
    circle_id TEXT NOT NULL,
    control_coord TEXT NOT NULL CHECK (json_valid(control_coord)),
    metadata_bytes BLOB NOT NULL,
    PRIMARY KEY (circle_id, control_coord),
    FOREIGN KEY (circle_id, control_coord)
        REFERENCES circle_control_activations(circle_id, control_coord)
"
        );
        $visit!(
            circle_current_state,
            "
    circle_id TEXT PRIMARY KEY,
    state BLOB NOT NULL
"
        );
        $visit!(
            circle_snapshot_coverage,
            "
    circle_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    commit_hash TEXT NOT NULL CHECK (length(commit_hash) = 64),
    snapshot_hash TEXT NOT NULL CHECK (length(snapshot_hash) = 64),
    PRIMARY KEY (circle_id, device_id)
"
        );
        $visit!(
            circle_acks,
            "
    circle_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    ack_hash TEXT NOT NULL CHECK (length(ack_hash) = 64),
    ack_bytes BLOB NOT NULL,
    published INTEGER NOT NULL CHECK (published IN (0, 1)),
    PRIMARY KEY (circle_id, device_id, revision),
    UNIQUE (circle_id, ack_hash)
"
        );
    };
}

macro_rules! coven_routing_tables {
    ($visit:ident) => {
        $visit!(
            _coven_audience,
            "
    routing_id TEXT PRIMARY KEY,
    circle_id TEXT,
    _updated_at TEXT NOT NULL
"
        );
        $visit!(
            _coven_row_routes,
            "
    routing_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    row_id TEXT NOT NULL,
    _updated_at TEXT NOT NULL,
    UNIQUE (table_name, row_id)
"
        );
    };
}

const OBJECT_OWNERSHIP_TRIGGERS: &str = "
CREATE TRIGGER IF NOT EXISTS remote_object_identity_must_not_be_inert_on_insert
BEFORE INSERT ON remote_objects
WHEN EXISTS (
    SELECT 1 FROM protocol_inert_objects WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'remote object identity is protocol-inert');
END;
CREATE TRIGGER IF NOT EXISTS remote_object_identity_must_not_be_inert_on_update
BEFORE UPDATE OF object_id ON remote_objects
WHEN EXISTS (
    SELECT 1 FROM protocol_inert_objects WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'remote object identity is protocol-inert');
END;
CREATE TRIGGER IF NOT EXISTS inert_object_identity_must_not_be_remote_on_insert
BEFORE INSERT ON protocol_inert_objects
WHEN EXISTS (
    SELECT 1 FROM remote_objects WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'protocol-inert object identity has active ownership');
END;
CREATE TRIGGER IF NOT EXISTS inert_object_identity_must_not_be_remote_on_update
BEFORE UPDATE OF object_id ON protocol_inert_objects
WHEN EXISTS (
    SELECT 1 FROM remote_objects WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'protocol-inert object identity has active ownership');
END;
CREATE TRIGGER IF NOT EXISTS remote_object_identity_must_not_be_reclaimed_on_insert
BEFORE INSERT ON remote_objects
WHEN EXISTS (
    SELECT 1 FROM reclaimed_store_packages WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'remote object identity is a reclaimed Store package');
END;
CREATE TRIGGER IF NOT EXISTS remote_object_identity_must_not_be_reclaimed_on_update
BEFORE UPDATE OF object_id ON remote_objects
WHEN EXISTS (
    SELECT 1 FROM reclaimed_store_packages WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'remote object identity is a reclaimed Store package');
END;
CREATE TRIGGER IF NOT EXISTS inert_object_identity_must_not_be_reclaimed_on_insert
BEFORE INSERT ON protocol_inert_objects
WHEN EXISTS (
    SELECT 1 FROM reclaimed_store_packages WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'protocol-inert object identity is a reclaimed Store package');
END;
CREATE TRIGGER IF NOT EXISTS inert_object_identity_must_not_be_reclaimed_on_update
BEFORE UPDATE OF object_id ON protocol_inert_objects
WHEN EXISTS (
    SELECT 1 FROM reclaimed_store_packages WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'protocol-inert object identity is a reclaimed Store package');
END;
CREATE TRIGGER IF NOT EXISTS reclaimed_store_package_identity_must_be_closed_on_insert
BEFORE INSERT ON reclaimed_store_packages
WHEN EXISTS (
    SELECT 1 FROM remote_objects WHERE object_id = NEW.object_id
    UNION ALL
    SELECT 1 FROM protocol_inert_objects WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'reclaimed Store package identity has another ownership state');
END;
CREATE TRIGGER IF NOT EXISTS reclaimed_store_package_identity_must_be_closed_on_update
BEFORE UPDATE OF object_id ON reclaimed_store_packages
WHEN EXISTS (
    SELECT 1 FROM remote_objects WHERE object_id = NEW.object_id
    UNION ALL
    SELECT 1 FROM protocol_inert_objects WHERE object_id = NEW.object_id
)
BEGIN
    SELECT RAISE(ABORT, 'reclaimed Store package identity has another ownership state');
END;
";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CovenSchemaManifest {
    objects: Vec<CovenSchemaObject>,
    tables: Vec<CovenTableShape>,
}

impl CovenSchemaManifest {
    pub(crate) fn is_empty(&self) -> bool {
        self.objects.is_empty() && self.tables.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CovenSchemaObject {
    kind: String,
    name: String,
    table_name: String,
    sql: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CovenTableShape {
    name: String,
    columns: i64,
    without_rowid: bool,
    strict: bool,
}

fn normalize_schema_sql(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut normalized = String::with_capacity(sql.len());
    let mut index = 0;
    let mut pending_separator = false;
    let mut quote = None;

    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(delimiter) = quote {
            normalized.push(char::from(byte));
            if byte == delimiter {
                if bytes.get(index + 1) == Some(&delimiter) {
                    normalized.push(char::from(delimiter));
                    index += 1;
                } else {
                    quote = None;
                }
            }
            index += 1;
            continue;
        }

        if byte == b'-' && bytes.get(index + 1) == Some(&b'-') {
            index += 2;
            while index < bytes.len() && !matches!(bytes[index], b'\n' | b'\r') {
                index += 1;
            }
            pending_separator = true;
            continue;
        }
        if byte.is_ascii_whitespace() {
            pending_separator = true;
            index += 1;
            continue;
        }
        if matches!(byte, b'\'' | b'"' | b'`') {
            if pending_separator
                && normalized
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
            {
                normalized.push(' ');
            }
            pending_separator = false;
            quote = Some(byte);
            normalized.push(char::from(byte));
            index += 1;
            continue;
        }
        if pending_separator
            && byte.is_ascii_alphanumeric()
            && normalized
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
        {
            normalized.push(' ');
        }
        pending_separator = false;
        normalized.push(char::from(byte.to_ascii_lowercase()));
        index += 1;
    }

    normalized
}

fn all_coven_table_names() -> std::collections::BTreeSet<&'static str> {
    let mut names = std::collections::BTreeSet::new();
    macro_rules! collect_name {
        ($name:ident, $columns:literal) => {
            names.insert(stringify!($name));
        };
    }

    coven_tables!(collect_name);
    coven_routing_tables!(collect_name);
    names
}

pub(crate) fn live_coven_schema_manifest(
    conn: &rusqlite::Connection,
) -> rusqlite::Result<CovenSchemaManifest> {
    let names = all_coven_table_names();
    let mut objects = conn
        .prepare(
            "SELECT type, name, tbl_name, sql
             FROM main.sqlite_schema
             WHERE type IN ('table', 'index', 'trigger')
             ORDER BY type, name",
        )?
        .query_map([], |row| {
            Ok(CovenSchemaObject {
                kind: row.get(0)?,
                name: row.get(1)?,
                table_name: row.get(2)?,
                sql: row
                    .get::<_, Option<String>>(3)?
                    .map(|sql| normalize_schema_sql(&sql)),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    objects.retain(|object| names.contains(object.table_name.as_str()));

    let mut tables = conn
        .prepare("PRAGMA main.table_list")?
        .query_map([], |row| {
            Ok(CovenTableShape {
                name: row.get(1)?,
                columns: row.get(3)?,
                without_rowid: row.get::<_, i64>(4)? != 0,
                strict: row.get::<_, i64>(5)? != 0,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    tables.retain(|table| names.contains(table.name.as_str()));
    tables.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(CovenSchemaManifest { objects, tables })
}

pub(crate) fn expected_coven_schema_manifest(
    include_routing: bool,
) -> rusqlite::Result<CovenSchemaManifest> {
    let conn = rusqlite::Connection::open_in_memory()?;
    apply_coven_schema(&conn)?;
    if include_routing {
        apply_coven_routing_schema(&conn)?;
    }
    live_coven_schema_manifest(&conn)
}

/// Creates Coven's bookkeeping tables after the fresh host schema has passed
/// sync-routing validation, inside the same open transaction. Idempotent (`IF
/// NOT EXISTS`). STRICT: every column here is already TEXT/INTEGER/BLOB, so
/// STRICT only forecloses a future column drifting off its declared affinity.
pub(crate) fn apply_coven_schema(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    macro_rules! apply_table {
        ($name:ident, $columns:literal) => {
            conn.execute_batch(concat!(
                "CREATE TABLE IF NOT EXISTS ",
                stringify!($name),
                " (",
                $columns,
                ") STRICT;"
            ))?;
        };
    }

    coven_tables!(apply_table);
    conn.execute_batch(OBJECT_OWNERSHIP_TRIGGERS)?;
    Ok(())
}

/// Create the MergeConcurrent audience mirror and private route map. The
/// initializer calls this only when the routing contract has a scoped graph.
pub(crate) fn apply_coven_routing_schema(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    macro_rules! apply_table {
        ($name:ident, $columns:literal) => {
            conn.execute_batch(concat!(
                "CREATE TABLE ",
                stringify!($name),
                " (",
                $columns,
                ") STRICT, WITHOUT ROWID;"
            ))?;
        };
    }

    coven_routing_tables!(apply_table);
    Ok(())
}

/// Whether `name` is a table coven owns for sync bookkeeping. Hosts may not
/// declare these as synced tables.
pub(crate) fn is_reserved_table_name(name: &str) -> bool {
    macro_rules! matches_table {
        ($table:ident, $columns:literal) => {
            if name == stringify!($table) {
                return true;
            }
        };
    }

    coven_tables!(matches_table);
    coven_routing_tables!(matches_table);
    false
}

/// The name of every table [`apply_coven_schema`] creates, for a test to assert a
/// schema property (STRICT) holds across all of them without re-listing the set
/// by hand.
#[cfg(test)]
pub(crate) fn table_names() -> Vec<&'static str> {
    let mut names = Vec::new();
    macro_rules! collect_name {
        ($name:ident, $columns:literal) => {
            names.push(stringify!($name));
        };
    }

    coven_tables!(collect_name);
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_sql_normalization_ignores_formatting_but_keeps_constraints() {
        let formatted = normalize_schema_sql(
            "CREATE TABLE t (
                id INTEGER PRIMARY KEY, -- explanatory text
                value TEXT CHECK (length(value) = 64)
             ) STRICT",
        );
        let compact = normalize_schema_sql(
            "create table t(id integer primary key,value text check(length(value)=64)) strict",
        );
        let changed = normalize_schema_sql(
            "create table t(id integer primary key,value text check(length(value)=32)) strict",
        );

        assert_eq!(formatted, compact);
        assert_ne!(formatted, changed);
    }

    /// Every bookkeeping table [`apply_coven_schema`] creates is declared STRICT
    /// — the same guarantee the synced-table contract now requires of the host's
    /// own tables, so coven does not exempt itself from the invariant it enforces
    /// on the host.
    #[test]
    fn every_bookkeeping_table_is_strict() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        apply_coven_schema(&conn).expect("apply coven schema");
        for name in table_names() {
            let sql = format!(
                "PRAGMA table_list({})",
                crate::sync::session::quote_ident(name)
            );
            let mut stmt = conn.prepare(&sql).expect("prepare table_list");
            let strict: i64 = stmt
                .query_row([], |row| row.get(5))
                .unwrap_or_else(|e| panic!("PRAGMA table_list({name}): {e}"));
            assert_eq!(strict, 1, "{name} must be STRICT");
        }
    }

    #[test]
    fn active_and_protocol_inert_object_identities_are_disjoint() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        apply_coven_schema(&conn).expect("apply coven schema");
        let active_first = "a".repeat(64);
        conn.execute(
            "INSERT INTO remote_objects (object_id, state) VALUES (?1, '{}')",
            [&active_first],
        )
        .expect("insert active object");
        assert!(
            conn.execute(
                "INSERT INTO protocol_inert_objects (object_id, state) VALUES (?1, '{}')",
                [&active_first],
            )
            .is_err(),
            "an active object identity must not also become protocol-inert"
        );

        let inert_first = "b".repeat(64);
        conn.execute(
            "INSERT INTO protocol_inert_objects (object_id, state) VALUES (?1, '{}')",
            [&inert_first],
        )
        .expect("insert protocol-inert object");
        assert!(
            conn.execute(
                "INSERT INTO remote_objects (object_id, state) VALUES (?1, '{}')",
                [&inert_first],
            )
            .is_err(),
            "a protocol-inert object identity must not return to active ownership"
        );
        assert!(
            conn.execute(
                "UPDATE remote_objects SET object_id = ?1 WHERE object_id = ?2",
                [&inert_first, &active_first],
            )
            .is_err(),
            "an active identity update must not collide with protocol-inert ownership"
        );
        assert!(
            conn.execute(
                "UPDATE protocol_inert_objects SET object_id = ?1 WHERE object_id = ?2",
                [&active_first, &inert_first],
            )
            .is_err(),
            "a protocol-inert identity update must not collide with active ownership"
        );
    }

    #[test]
    fn circle_operation_journal_has_one_progress_representation() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        apply_coven_schema(&conn).expect("apply coven schema");
        let mut statement = conn
            .prepare("PRAGMA table_info(circle_operations)")
            .expect("prepare circle_operations table_info");
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))
            .expect("query circle_operations columns")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("read circle_operations columns");

        assert_eq!(columns, ["operation_id", "circle_id", "payload"]);
    }

    #[test]
    fn circle_operation_journal_allows_one_pending_operation_per_circle() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        apply_coven_schema(&conn).expect("apply coven schema");
        conn.execute(
            "INSERT INTO circle_operations (operation_id, circle_id, payload)
             VALUES ('operation-a', 'circle-a', x'7b7d')",
            [],
        )
        .expect("insert first pending Circle operation");

        let error = conn
            .execute(
                "INSERT INTO circle_operations (operation_id, circle_id, payload)
                 VALUES ('operation-b', 'circle-a', x'7b7d')",
                [],
            )
            .expect_err("a Circle cannot have a second pending operation");

        assert!(error.to_string().contains("circle_operations.circle_id"));
    }

    #[test]
    fn prepared_blob_identity_is_the_exact_remote_object() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        apply_coven_schema(&conn).expect("apply coven schema");
        let primary_key = conn
            .prepare("PRAGMA table_info(store_write_blobs)")
            .expect("prepare store_write_blobs table_info")
            .query_map([], |row| {
                Ok((row.get::<_, String>(1)?, row.get::<_, i64>(5)?))
            })
            .expect("query store_write_blobs columns")
            .filter_map(|row| {
                let (name, position) = row.expect("read store_write_blobs column");
                (position != 0).then_some((position, name))
            })
            .collect::<std::collections::BTreeMap<_, _>>();

        assert_eq!(
            primary_key.into_values().collect::<Vec<_>>(),
            ["write_id", "audience", "remote_object_id"]
        );
    }

    #[test]
    fn routing_tables_are_strict_without_rowid() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        apply_coven_routing_schema(&conn).expect("apply routing schema");
        for name in ["_coven_audience", "_coven_row_routes"] {
            let sql = format!(
                "PRAGMA table_list({})",
                crate::sync::session::quote_ident(name)
            );
            let (wr, strict): (i64, i64) = conn
                .query_row(&sql, [], |row| Ok((row.get(4)?, row.get(5)?)))
                .expect("table_list");
            assert_eq!((wr, strict), (1, 1), "{name}");
        }
    }
}

/// An external user-owned file a blob id resolves to, read back from a
/// `local_blob_refs` row. The blob's plaintext lives at `path` (an absolute file
/// coven references but does not own); `size` is its registered plaintext length,
/// combined with the row's signed content hash to validate the exact file. The `namespace`
/// stays on the row but is not part of the read shape, so it is not carried here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalBlob {
    /// Absolute path to the external file coven reads but does not own.
    pub path: std::path::PathBuf,
    /// The file's plaintext length at registration. A read fails loud if the
    /// file's current length differs (truncated, replaced) — validate-on-read.
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEntry {
    pub id: i64,
    pub attempt_count: i64,
    pub last_attempt_at: Option<String>,
    pub operation: OutboxOperation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxOperation {
    Upload {
        root_table: String,
        root_id: String,
        row: crate::blob::RowBlobRef,
        source_path: std::path::PathBuf,
        retain_pinned: bool,
        state: OutboxUploadState,
    },
    Delete {
        stored: crate::blob::locator::StoredBlobRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OutboxUploadState {
    Pending,
    Prepared {
        authority: crate::sync::audience_package::PackageAudience,
        stored: crate::blob::locator::StoredBlobRef,
        spool_path: std::path::PathBuf,
    },
    Created {
        authority: crate::sync::audience_package::PackageAudience,
        stored: crate::blob::locator::StoredBlobRef,
        spool_path: std::path::PathBuf,
    },
}
