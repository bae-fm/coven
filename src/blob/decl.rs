//! The resolved blob-declaration model: coven's own derivation of which rows
//! carry blobs and where each blob's columns live.
//!
//! The gate-sibling of [`Gates`](crate::sync::gate::Gates). A host declares per
//! table, via [`SyncedTable::carries_blob`], the columns that locate a blob (its
//! id, optional readable cloud path, and encryption-scope column) plus the
//! namespace and retention class. [`BlobDecls::from_tables`] resolves those column
//! *names* to indices against the live schema once per cycle — the same
//! `PRAGMA table_info` name→index resolution the gate runs — so coven reads a
//! row's blob straight off a changeset row or a live `SELECT` with no per-row host
//! callback.
//!
//! From that one model coven derives every blob set it needs:
//! [`BlobDecls::ref_from_change`] over a changeset row (push upload / pull
//! download / apply-side cache drop), and [`BlobDecls::refs_in_db`] over the whole
//! DB (snapshot-bootstrap backfill).

use std::collections::HashMap;

use rusqlite::Connection;

use crate::blob::{BlobRef, BlobScope, BlobSync};
use crate::changeset::RowChange;
use crate::sync::session::{quote_ident, BlobScopeSpec, SyncedTable};

/// Why building the blob-declaration model failed.
#[derive(Debug)]
pub enum BlobDeclError {
    /// A declared blob column is absent from the table's live schema.
    MissingColumn { table: String, column: String },
    /// A schema read (`PRAGMA table_info`) failed.
    Sqlite(rusqlite::Error),
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
    sync: BlobSync,
    /// Index of the blob-id column.
    id_col: usize,
    /// Index of the readable cloud-path column, if declared.
    cloud_path_col: Option<usize>,
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

/// The blob declarations for a sync cycle, resolved once from the declared set +
/// the live schema. A synced table absent from this map carries no blob.
pub struct BlobDecls {
    tables: HashMap<String, TableBlob>,
}

impl BlobDecls {
    /// Resolve every [`SyncedTable::carries_blob`] declaration's column names to
    /// indices against the live schema, mirroring
    /// [`Gates::from_tables`](crate::sync::gate::Gates::from_tables). A declared
    /// column absent from the table is a host error surfaced here, never a silent
    /// drop.
    pub fn from_tables(conn: &Connection, tables: &[SyncedTable]) -> Result<Self, BlobDeclError> {
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
                    sync: decl.sync,
                    id_col,
                    cloud_path_col,
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
        Some(BlobRef {
            namespace: tb.namespace.clone(),
            id,
            scope,
            cloud_path,
            sync: tb.sync,
        })
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
                let Some(id) = row.get::<_, Option<String>>(tb.id_col)? else {
                    continue;
                };
                let cloud_path = match tb.cloud_path_col {
                    Some(i) => row.get::<_, Option<String>>(i)?,
                    None => None,
                };
                let scope = match &tb.scope {
                    ResolvedScopeSpec::Master => BlobScope::Master,
                    ResolvedScopeSpec::Derived(s) => BlobScope::Derived(s.clone()),
                    ResolvedScopeSpec::ItemColumn(i) => match row.get::<_, Option<String>>(*i)? {
                        Some(item) => BlobScope::Item(item),
                        None => continue,
                    },
                };
                out.push(BlobRef {
                    namespace: tb.namespace.clone(),
                    id,
                    scope,
                    cloud_path,
                    sync: tb.sync,
                });
            }
        }
        Ok(out)
    }
}

/// Column names of `table`, in declared (schema) order, via `PRAGMA table_info` —
/// the safe-rusqlite sibling of the gate's FFI `column_names`. The index of a name
/// here is the index a changeset reports for that column.
fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>, BlobDeclError> {
    let sql = format!("PRAGMA table_info({})", quote_ident(table));
    let mut stmt = conn.prepare(&sql)?;
    let names = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(names)
}
