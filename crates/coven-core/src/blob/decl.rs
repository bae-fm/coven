//! The resolved blob-declaration model: coven's own derivation of which rows
//! carry blobs and where each blob's columns live.
//!
//! The gate-sibling of [`Gates`](crate::sync::gate::Gates). A host declares per
//! table, via [`SyncedTable::carries_blob`], the columns that locate a blob (its
//! id, optional readable cloud path, and encryption-scope column) plus the
//! namespace and retention class. [`BlobDecls::from_tables`] resolves those column
//! *names* to indices against the live schema for the database handle — the same
//! `PRAGMA table_info` name→index resolution the gate runs — so coven reads a
//! row's blob straight off a changeset row or a live `SELECT` with no per-row host
//! callback.
//!
//! From that one model coven derives every blob set it needs:
//! [`BlobDecls::ref_from_change`] over a changeset row (push upload / pull
//! download / apply-side local-copy drop), [`BlobDecls::refs_in_db`] over the whole
//! DB (snapshot-bootstrap backfill), [`BlobDecls::refs_for_root`] over a gated
//! root's subtree (the make-Remote / make-Local transitions), and
//! [`BlobDecls::row_for_blob_in_namespace`] to map a blob back to its row by namespace
//! (the read-path locality dispatch and the make-Remote completion check).
//!
//! A declaration's two blob properties — [`Provenance`] (the Local story) and
//! [`CacheFill`] (the Remote story) — are described by the [blob concept
//! tree](crate::blob).

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use crate::blob::{BlobRef, BlobScope, CacheFill, Provenance};
use crate::changeset::RowChange;
use crate::sync::gate::Gates;
use crate::sync::session::{
    quote_ident, table_columns as session_table_columns, BlobScopeSpec, SyncedTable,
};

/// Why building the blob-declaration model failed.
#[derive(Debug)]
pub enum BlobDeclError {
    /// A declared blob column is absent from the table's live schema.
    MissingColumn { table: String, column: String },
    /// A schema read (`PRAGMA table_info`) failed.
    Sqlite(rusqlite::Error),
    /// Walking the gate's FK graph for [`BlobDecls::refs_for_root`] failed.
    Gate(String),
}

impl std::fmt::Display for BlobDeclError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobDeclError::MissingColumn { table, column } => {
                write!(
                    f,
                    "blob declaration names column {column:?} absent from {table:?}"
                )
            }
            BlobDeclError::Sqlite(e) => write!(f, "blob declaration schema read failed: {e}"),
            BlobDeclError::Gate(e) => write!(f, "blob declaration FK walk failed: {e}"),
        }
    }
}

impl std::error::Error for BlobDeclError {}

impl From<rusqlite::Error> for BlobDeclError {
    fn from(e: rusqlite::Error) -> Self {
        BlobDeclError::Sqlite(e)
    }
}

/// A blob-bearing table's columns resolved to indices in the live schema (the
/// same order a changeset reports its columns, so an index reads either source).
struct TableBlob {
    namespace: String,
    provenance: Provenance,
    fill: CacheFill,
    /// Index of the blob-id column.
    id_col: usize,
    /// Name of the blob-id column. The index reads a row top-to-bottom; the name
    /// keys a lookup the other way ([`BlobDecls::row_for_blob_in_namespace`]: which row
    /// carries a given blob id), so both directions resolve off the same declaration.
    id_col_name: String,
    /// Index of the readable cloud-path column, if declared.
    cloud_path_col: Option<usize>,
    /// Name of the readable cloud-path column, if declared.
    cloud_path_col_name: Option<String>,
    /// The encryption scope, with any column reference resolved to an index.
    scope: ResolvedScopeSpec,
}

/// [`BlobScopeSpec`] with its column reference resolved to a schema index.
enum ResolvedScopeSpec {
    Master,
    Derived(String),
    /// The item id is the value of this column in the blob's row.
    ItemColumn(usize),
}

impl TableBlob {
    /// Build the [`BlobRef`] for one of this table's rows from the per-row `id`,
    /// `scope`, and `cloud_path` plus this table's fixed namespace, provenance, and
    /// cache fill. Shared by [`BlobDecls::ref_from_change`] (changeset row) and
    /// [`BlobDecls::refs_in_db`] (live row), which differ only in how they read
    /// those per-row values.
    fn blob_ref(&self, id: String, scope: BlobScope, cloud_path: Option<String>) -> BlobRef {
        BlobRef {
            namespace: self.namespace.clone(),
            id,
            scope,
            cloud_path,
            provenance: self.provenance,
            fill: self.fill,
        }
    }

    /// Build the [`BlobRef`] for a live `SELECT *` row of this table, or `None` when
    /// the row's blob id (or an `ItemColumn` scope value) is NULL. The resolved
    /// indices address a `SELECT *` row in schema order, exactly as they address a
    /// changeset row. Shared by [`BlobDecls::refs_in_db`] (whole DB) and
    /// [`BlobDecls::refs_for_root`] (one root's subtree).
    fn ref_from_row(&self, row: &rusqlite::Row<'_>) -> rusqlite::Result<Option<BlobRef>> {
        let Some(id) = row.get::<_, Option<String>>(self.id_col)? else {
            return Ok(None);
        };
        let cloud_path = match self.cloud_path_col {
            Some(i) => row.get::<_, Option<String>>(i)?,
            None => None,
        };
        let scope = match &self.scope {
            ResolvedScopeSpec::Master => BlobScope::Master,
            ResolvedScopeSpec::Derived(s) => BlobScope::Derived(s.clone()),
            ResolvedScopeSpec::ItemColumn(i) => match row.get::<_, Option<String>>(*i)? {
                Some(item) => BlobScope::Item(item),
                None => return Ok(None),
            },
        };
        Ok(Some(self.blob_ref(id, scope, cloud_path)))
    }
}

/// The blob declarations for a database handle, resolved from the declared set +
/// the live schema at open. A synced table absent from this map carries no blob.
pub struct BlobDecls {
    tables: HashMap<String, TableBlob>,
}

#[cfg(test)]
thread_local! {
    static FROM_TABLES_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_from_tables_call_count() {
    FROM_TABLES_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(crate) fn from_tables_call_count() -> usize {
    FROM_TABLES_CALLS.with(std::cell::Cell::get)
}

impl BlobDecls {
    /// Resolve every [`SyncedTable::carries_blob`] declaration's column names to
    /// indices against the live schema, mirroring
    /// [`Gates::from_tables`](crate::sync::gate::Gates::from_tables). A declared
    /// column absent from the table is a host error surfaced here, never a silent
    /// drop.
    pub fn from_tables(conn: &Connection, tables: &[SyncedTable]) -> Result<Self, BlobDeclError> {
        #[cfg(test)]
        FROM_TABLES_CALLS.with(|calls| calls.set(calls.get() + 1));

        let mut map = HashMap::new();
        for t in tables {
            let Some(decl) = t.blob() else {
                continue;
            };
            let cols = table_columns(conn, t.name())?;
            let index_of = |column: &str| -> Result<usize, BlobDeclError> {
                cols.iter()
                    .position(|c| c == column)
                    .ok_or_else(|| BlobDeclError::MissingColumn {
                        table: t.name().to_string(),
                        column: column.to_string(),
                    })
            };

            let id_col = index_of(&decl.id_column)?;
            let cloud_path_col = match &decl.cloud_path_column {
                Some(c) => Some(index_of(c)?),
                None => None,
            };
            let scope = match &decl.scope {
                BlobScopeSpec::Master => ResolvedScopeSpec::Master,
                BlobScopeSpec::Derived(s) => ResolvedScopeSpec::Derived(s.clone()),
                BlobScopeSpec::ItemColumn(c) => ResolvedScopeSpec::ItemColumn(index_of(c)?),
            };

            map.insert(
                t.name().to_string(),
                TableBlob {
                    namespace: decl.namespace.clone(),
                    provenance: decl.provenance,
                    fill: decl.fill,
                    id_col,
                    id_col_name: decl.id_column.clone(),
                    cloud_path_col,
                    cloud_path_col_name: decl.cloud_path_column.clone(),
                    scope,
                },
            );
        }
        Ok(BlobDecls { tables: map })
    }

    /// The blob a single changeset row references, or `None` when the row's table
    /// carries no blob or the blob id is absent/NULL. Reads the declared columns
    /// off the changeset row (which reports columns in schema order, the order the
    /// resolved indices address).
    pub fn ref_from_change(&self, change: &RowChange) -> Option<BlobRef> {
        let tb = self.tables.get(&change.table)?;
        let id = change.col(tb.id_col)?.to_string();
        let cloud_path = tb
            .cloud_path_col
            .and_then(|i| change.col(i))
            .map(str::to_string);
        let scope = match &tb.scope {
            ResolvedScopeSpec::Master => BlobScope::Master,
            ResolvedScopeSpec::Derived(s) => BlobScope::Derived(s.clone()),
            ResolvedScopeSpec::ItemColumn(i) => BlobScope::Item(change.col(*i)?.to_string()),
        };
        Some(tb.blob_ref(id, scope, cloud_path))
    }

    /// Every blob the rows currently in `conn` reference — the snapshot-bootstrap
    /// analogue of [`Self::ref_from_change`], reading the declared columns from a
    /// live `SELECT` instead of a changeset row. A row whose blob id is NULL is
    /// skipped.
    pub fn refs_in_db(&self, conn: &Connection) -> Result<Vec<BlobRef>, BlobDeclError> {
        let mut out = Vec::new();
        for (table, tb) in &self.tables {
            // `SELECT *` returns columns in schema order, so the resolved indices
            // address this row exactly as they address a changeset row.
            let sql = format!("SELECT * FROM {}", quote_ident(table));
            let mut stmt = conn.prepare(&sql)?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                if let Some(blob) = tb.ref_from_row(row)? {
                    out.push(blob);
                }
            }
        }
        Ok(out)
    }

    /// Every blob the blob-bearing rows in the gated subtree rooted at
    /// `(root_table, root_id)` reference. The transition analogue of
    /// [`Self::refs_in_db`], scoped to one root: it asks the gate for the subtree's
    /// rows ([`Gates::subtree_rows`] — a pure down-walk, so a release's files but
    /// not a sibling release's) and reads each blob-bearing row by primary key.
    ///
    /// `make_remote` enqueues an upload per user-provided blob here; `make_local`
    /// materializes each blob here back to a local file. A row in the subtree whose
    /// table carries no blob (a `tracks` join row) contributes nothing.
    pub fn refs_for_root(
        &self,
        conn: &Connection,
        gates: &Gates,
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<BlobRef>, BlobDeclError> {
        let rows = gates
            .subtree_rows(conn, root_table, root_id)
            .map_err(|e| BlobDeclError::Gate(e.to_string()))?;
        let mut out = Vec::new();
        for (table, pk) in rows {
            let Some(tb) = self.tables.get(&table) else {
                continue; // a non-blob-bearing subtree row (e.g. a join row).
            };
            // The subtree keys rows by primary key (`id`), the same column the gate
            // walks; read that row's declared blob columns by `SELECT *`.
            let sql = format!("SELECT * FROM {} WHERE id = ?1", quote_ident(&table));
            let blob = conn.query_row(&sql, [&pk], |row| tb.ref_from_row(row))?;
            if let Some(blob) = blob {
                out.push(blob);
            }
        }
        Ok(out)
    }

    /// The `(table, primary key)` of the row carrying `blob_id` in the table declared
    /// for `namespace` — the carrying table resolved from the blob's own namespace
    /// (part of its address), not by scanning every blob-bearing table. `None` when no
    /// declared table owns `namespace`, or that table has no row with the id. Both the
    /// read path (locality dispatch) and the make-Remote completion check use this, so
    /// a blob id that collides across namespaces always reads the right table's gate,
    /// never the first id match.
    pub fn row_for_blob_in_namespace(
        &self,
        conn: &Connection,
        namespace: &str,
        blob_id: &str,
    ) -> Result<Option<(String, String)>, BlobDeclError> {
        let Some((table, tb)) = self.tables.iter().find(|(_, tb)| tb.namespace == namespace) else {
            return Ok(None);
        };
        Ok(pk_carrying_blob(conn, table, tb, blob_id)?.map(|pk| (table.clone(), pk)))
    }

    /// The `(table, primary key)` of a live row whose declared blob resolves to
    /// `cloud_key`. Hashed homes encode namespace + blob id in the key itself;
    /// readable homes encode namespace + declared `cloud_path`. This is the GC-side
    /// lookup: before honoring a tombstone for `cloud_key`, ask whether current DB
    /// state still names that exact cloud object.
    pub fn row_for_blob_cloud_key(
        &self,
        conn: &Connection,
        cloud_key: &str,
    ) -> Result<Option<(String, String)>, BlobDeclError> {
        if let Some((namespace, blob_id)) = hashed_blob_key_parts(cloud_key) {
            if let Some(row) = self.row_for_blob_in_namespace(conn, &namespace, &blob_id)? {
                return Ok(Some(row));
            }
        }

        for (table, tb) in &self.tables {
            let Some(cloud_path_col_name) = &tb.cloud_path_col_name else {
                continue;
            };
            let Some(cloud_path) = cloud_key
                .strip_prefix(&tb.namespace)
                .and_then(|rest| rest.strip_prefix('/'))
            else {
                continue;
            };
            if let Some(pk) = pk_carrying_cloud_path(conn, table, cloud_path_col_name, cloud_path)?
            {
                return Ok(Some((table.clone(), pk)));
            }
        }

        Ok(None)
    }
}

/// The primary key (`id`) of the row in `table` whose declared blob-id column equals
/// `blob_id`, or `None`. The per-table lookup [`BlobDecls::row_for_blob_in_namespace`]
/// runs against the one namespace-resolved table.
fn pk_carrying_blob(
    conn: &Connection,
    table: &str,
    tb: &TableBlob,
    blob_id: &str,
) -> Result<Option<String>, BlobDeclError> {
    let sql = format!(
        "SELECT id FROM {} WHERE {} = ?1",
        quote_ident(table),
        quote_ident(&tb.id_col_name),
    );
    conn.query_row(&sql, [blob_id], |row| row.get::<_, String>(0))
        .optional()
        .map_err(BlobDeclError::from)
}

fn pk_carrying_cloud_path(
    conn: &Connection,
    table: &str,
    cloud_path_col_name: &str,
    cloud_path: &str,
) -> Result<Option<String>, BlobDeclError> {
    let sql = format!(
        "SELECT id FROM {} WHERE {} = ?1",
        quote_ident(table),
        quote_ident(cloud_path_col_name),
    );
    conn.query_row(&sql, [cloud_path], |row| row.get::<_, String>(0))
        .optional()
        .map_err(BlobDeclError::from)
}

fn hashed_blob_key_parts(cloud_key: &str) -> Option<(String, String)> {
    let mut parts = cloud_key.split('/');
    let namespace = parts.next()?;
    let _first = parts.next()?;
    let _second = parts.next()?;
    let id = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    match crate::library_dir::LibraryDir::hashed_path(namespace, id) {
        Ok(rebuilt) if rebuilt == cloud_key => Some((namespace.to_string(), id.to_string())),
        _ => None,
    }
}

/// Column names of `table`, in declared (schema) order, via `PRAGMA table_info` —
/// the safe-rusqlite sibling of the gate's FFI `column_names`. The index of a name
/// here is the index a changeset reports for that column.
pub(crate) fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>, BlobDeclError> {
    session_table_columns(conn, table).map_err(BlobDeclError::from)
}
