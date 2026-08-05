use super::*;
use crate::database::query_mapped_rows;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurablePreparedProtocolObject {
    pub(super) semantic_bytes: Vec<u8>,
    pub(super) prepared: PreparedExactObject,
}

impl DurablePreparedProtocolObject {
    pub(crate) fn new(semantic_bytes: Vec<u8>, prepared: PreparedExactObject) -> Self {
        Self {
            semantic_bytes,
            prepared,
        }
    }

    pub(crate) fn semantic_bytes(&self) -> &[u8] {
        &self.semantic_bytes
    }

    pub(crate) fn prepared(&self) -> &PreparedExactObject {
        &self.prepared
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreBatchLocalCleanup {
    pub drops: Vec<crate::protocol::blob::DeferredLocalBlobDrop>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreBatchCompletion {}

pub(super) fn validate_host_synced_tables(
    conn: &Connection,
    synced_tables: &[SyncedTable],
) -> Result<(), DbError> {
    validate_host_trigger_names(conn)?;

    // Which table owns each declared blob namespace, so a second table claiming an
    // already-owned namespace is caught here. A blob's namespace is part of its
    // address; two tables sharing one makes `row_for_blob_in_namespace` resolve to
    // whichever the hash map iterates first.
    let mut namespace_owner: HashMap<&str, &str> = HashMap::new();
    let mut table_by_sqlite_name: HashMap<String, &str> = HashMap::new();
    for table in synced_tables {
        let name = table.name();
        if name.is_empty() {
            return Err(DbError::Message(
                "synced table name must not be empty".to_string(),
            ));
        }
        if is_reserved_table_name(name) {
            return Err(DbError::Message(format!(
                "synced table {name:?} is reserved by coven"
            )));
        }
        let sqlite_name = name.to_ascii_lowercase();
        if let Some(prior) = table_by_sqlite_name.insert(sqlite_name, name) {
            return Err(DbError::Message(format!(
                "synced tables {prior:?} and {name:?} are declared as the same SQLite table more than once"
            )));
        }
        if let Some(live_name) = canonical_table_name(conn, name)? {
            if live_name != name {
                return Err(DbError::Message(format!(
                    "synced table {name:?} does not use the live schema's exact spelling {live_name:?}"
                )));
            }
        }
        validate_synced_table_contract(conn, name)?;
        validate_existing_row_identities(conn, table)?;
        if let Some(decl) = table.blob() {
            let namespace = decl.namespace.as_str();
            if let Some(prior) = namespace_owner.insert(namespace, name) {
                return Err(DbError::Message(format!(
                    "synced tables {prior:?} and {name:?} both declare blob namespace \
                     {namespace:?}; a namespace must be owned by exactly one table"
                )));
            }
        }
    }
    Ok(())
}

fn validate_host_trigger_names(conn: &Connection) -> Result<(), DbError> {
    let names = query_mapped_rows(
        conn,
        "SELECT name FROM main.sqlite_schema WHERE type = 'trigger'
             UNION ALL
             SELECT name FROM temp.sqlite_schema WHERE type = 'trigger'",
        [],
        |row| row.get::<_, String>(0),
    )?;
    for name in names {
        if is_coven_cleanup_guard_name(&name) {
            return Err(DbError::Message(format!(
                "host trigger {name:?} uses a name reserved for Coven blob cleanup guards"
            )));
        }
    }
    Ok(())
}

/// Return the live `main`-schema spelling SQLite resolves for `table`.
/// SQLite table identifiers compare case-insensitively, while coven dispatches
/// changesets by their exact table name, so open requires declarations to use
/// this canonical spelling.
pub(super) fn canonical_table_name(
    conn: &Connection,
    table: &str,
) -> Result<Option<String>, DbError> {
    conn.query_row(
        "SELECT name FROM main.sqlite_schema \
         WHERE type = 'table' AND name = ?1 COLLATE NOCASE",
        [table],
        |row| row.get(0),
    )
    .optional()
    .map_err(DbError::from)
}

pub(super) fn validate_existing_row_identities(
    conn: &Connection,
    table: &SyncedTable,
) -> Result<(), DbError> {
    if table.row_identity() == crate::protocol::synced_schema::RowIdentity::SharedKey {
        return Ok(());
    }
    let sql = format!(
        "SELECT id FROM {}",
        crate::database::quote_ident(table.name())
    );
    let ids = query_mapped_rows(conn, &sql, [], |row| row.get::<_, String>(0))?;
    for id in ids {
        table
            .row_identity()
            .validate(table.name(), &id)
            .map_err(|error| DbError::Message(error.to_string()))?;
    }
    Ok(())
}

/// One column of a table's `PRAGMA table_info`. `position` is the column ordinal
/// — the index a session changeset reports for that column, so the pk's position
/// is what the by-position apply path reads. `pk` is 0 for a non-key column or its
/// 1-based rank within the primary key.
pub(super) struct ColumnInfo {
    position: i64,
    name: String,
    declared_type: String,
    not_null: bool,
    pk: i64,
}

/// Enforce the synced-table contract ([`crate::protocol::synced_schema::SyncedTable`]) on
/// `table`'s live schema: the table declared STRICT; a single primary key
/// column, named `id`, declared TEXT, at column 0; and an `_updated_at` column
/// declared TEXT NOT NULL. A violation is an open error naming the table and the
/// requirement it broke, so the integrator learns it on their own device instead
/// of a peer's pull failing on the row.
pub(super) fn validate_synced_table_contract(
    conn: &Connection,
    table: &str,
) -> Result<(), DbError> {
    match table_is_strict(conn, table)? {
        None => {
            return Err(DbError::Message(format!(
                "synced table {table:?} is declared in `synced_tables` but no migration \
                 creates it — add a `CREATE TABLE {table} (...) STRICT` to the schema \
                 migrations, or remove the declaration"
            )));
        }
        Some(false) => {
            return Err(DbError::Message(format!(
                "synced table {table:?} is not declared STRICT; the sync contract assumes typed \
                 columns (apply preserves storage classes peer-to-peer, LWW arbitration renders \
                 values to strings for comparison), which STRICT enforces at the insert — declare \
                 it STRICT: `CREATE TABLE {table} (...) STRICT`"
            )));
        }
        Some(true) => {}
    }

    let sql = format!("PRAGMA table_info({})", crate::database::quote_ident(table));
    let mut stmt = conn.prepare(&sql).map_err(DbError::from)?;
    let mut columns = Vec::new();
    let rows = stmt
        .query_map([], |row| {
            Ok(ColumnInfo {
                position: row.get::<_, i64>(0)?,
                name: row.get::<_, String>(1)?,
                declared_type: row.get::<_, String>(2)?,
                not_null: row.get::<_, i64>(3)? != 0,
                pk: row.get::<_, i64>(5)?,
            })
        })
        .map_err(DbError::from)?;
    for row in rows {
        columns.push(row.map_err(DbError::from)?);
    }

    let pk_columns: Vec<&ColumnInfo> = columns.iter().filter(|c| c.pk > 0).collect();
    let pk = match pk_columns.as_slice() {
        [single] => *single,
        [] => {
            return Err(DbError::Message(format!(
                "synced table {table:?} has no primary key; the contract requires a single \
                 `id` TEXT primary key at column 0"
            )))
        }
        _ => {
            let names: Vec<&str> = pk_columns.iter().map(|c| c.name.as_str()).collect();
            return Err(DbError::Message(format!(
                "synced table {table:?} has a composite primary key {names:?}; the contract \
                 requires a single `id` TEXT primary key at column 0"
            )));
        }
    };
    if pk.name != "id" {
        return Err(DbError::Message(format!(
            "synced table {table:?} primary key is {:?}, not `id`; the contract requires the \
             primary key to be the `id` column",
            pk.name
        )));
    }
    if pk.position != 0 {
        return Err(DbError::Message(format!(
            "synced table {table:?} primary key `id` is at column {}, not column 0; the \
             contract requires `id` to be the first column",
            pk.position
        )));
    }
    if !declared_as_text(&pk.declared_type) {
        return Err(DbError::Message(format!(
            "synced table {table:?} primary key `id` is declared {:?}, not TEXT; the contract \
             requires an `id` TEXT primary key",
            pk.declared_type
        )));
    }

    let updated_at = columns
        .iter()
        .find(|c| c.name == "_updated_at")
        .ok_or_else(|| {
            DbError::Message(format!(
                "synced table {table:?} has no `_updated_at` column; the contract requires \
                 `_updated_at TEXT NOT NULL`"
            ))
        })?;
    if !declared_as_text(&updated_at.declared_type) {
        return Err(DbError::Message(format!(
            "synced table {table:?} column `_updated_at` is declared {:?}, not TEXT; the \
             contract requires `_updated_at TEXT NOT NULL`",
            updated_at.declared_type
        )));
    }
    if !updated_at.not_null {
        return Err(DbError::Message(format!(
            "synced table {table:?} column `_updated_at` is nullable; the contract requires \
             `_updated_at TEXT NOT NULL`"
        )));
    }

    Ok(())
}

/// Whether a `PRAGMA table_info` declared type is TEXT, case-insensitively. SQL
/// keywords are case-insensitive, so `text` and `TEXT` both satisfy the contract;
/// any other declared type (or none) does not.
pub(super) fn declared_as_text(declared_type: &str) -> bool {
    declared_type.eq_ignore_ascii_case("TEXT")
}

/// Whether `table` (in the `main` schema) is declared STRICT, via `PRAGMA
/// table_list`'s `strict` column (SQLite 3.37+) — the schema-level flag itself,
/// not `sqlite_master.sql` text, which a hand-formatted `CREATE TABLE` could spell
/// many ways. `None` means the table doesn't exist in `main` at all — a declared
/// synced table no migration created — which the caller reports as its own
/// contract error rather than folding into "not STRICT".
pub(super) fn table_is_strict(conn: &Connection, table: &str) -> Result<Option<bool>, DbError> {
    let sql = format!("PRAGMA table_list({})", crate::database::quote_ident(table));
    let rows = query_mapped_rows(conn, &sql, [], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(5)?))
    })?;
    for (schema, strict) in rows {
        if schema == "main" {
            return Ok(Some(strict != 0));
        }
    }
    Ok(None)
}
