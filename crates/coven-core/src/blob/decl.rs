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
//! tree](crate::blob). Immutable generated cloud locations include the blob id, so a
//! consumer's readable path never determines object identity.

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use crate::blob::{BlobRef, BlobScope, CacheFill, Provenance};
use crate::changeset::RowChange;
use crate::sync::gate::Gates;
use crate::sync::session::{quote_ident, table_columns as session_table_columns, SyncedTable};

/// Why building the blob-declaration model failed.
#[derive(Debug)]
pub enum BlobDeclError {
    /// A declared blob column is absent from the table's live schema.
    MissingColumn { table: String, column: String },
    /// A schema read (`PRAGMA table_info`) failed.
    Sqlite(rusqlite::Error),
    /// A row's declared size column is not an integer.
    InvalidSizeValue { table: String, value: String },
    /// A row's declared size column is negative.
    InvalidSize { table: String, value: i64 },
    /// New and old changeset walks produced different row counts.
    ChangesetWalkMismatch { old_count: usize, new_count: usize },
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
            BlobDeclError::InvalidSizeValue { table, value } => {
                write!(
                    f,
                    "blob declaration found non-integer size in {table}: {value:?}"
                )
            }
            BlobDeclError::InvalidSize { table, value } => {
                write!(f, "blob declaration found invalid size in {table}: {value}")
            }
            BlobDeclError::ChangesetWalkMismatch {
                old_count,
                new_count,
            } => write!(
                f,
                "blob declaration changeset walk mismatch: old={old_count}, new={new_count}"
            ),
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

/// A blob a changeset row references, paired with the row's declared plaintext
/// size and content hash when those columns rode with the change — what the eager
/// pull needs to download and verify a blob before its row is applied.
pub type ChangesetBlobDownload = (BlobRef, Option<u64>, Option<String>);

/// A blob-bearing table's columns resolved to indices in the live schema (the
/// same order a changeset reports its columns, so an index reads either source).
struct TableBlob {
    namespace: String,
    provenance: Provenance,
    fill: CacheFill,
    /// Index of the blob-id column.
    id_col: usize,
    /// Index of the plaintext-size column.
    size_col: usize,
    /// Index of the content-hash column.
    hash_col: usize,
    /// Name of the blob-id column. The index reads a row top-to-bottom; the name
    /// keys a lookup the other way ([`BlobDecls::row_for_blob_in_namespace`]: which row
    /// carries a given blob id), so both directions resolve off the same declaration.
    id_col_name: String,
    /// Name of the plaintext-size column.
    size_col_name: String,
    /// Name of the content-hash column.
    hash_col_name: String,
    /// Index of the readable cloud-path column, if declared.
    cloud_path_col: Option<usize>,
    /// Name of the readable cloud-path column, if declared.
    cloud_path_col_name: Option<String>,
    /// The encryption scope, fixed per table by the declaration.
    scope: BlobScope,
}

impl TableBlob {
    /// Build the [`BlobRef`] for one of this table's rows from the per-row `id`,
    /// `scope`, and `cloud_path` plus this table's fixed namespace, provenance, and
    /// cache fill. Shared by [`BlobDecls::ref_from_change`] (changeset row) and
    /// [`BlobDecls::refs_in_db`] (live row), which differ only in how they read
    /// those per-row values.
    ///
    fn blob_ref(
        &self,
        id: String,
        scope: BlobScope,
        cloud_path: Option<String>,
    ) -> Result<BlobRef, BlobDeclError> {
        Ok(BlobRef {
            namespace: self.namespace.clone(),
            id,
            scope,
            cloud_path,
            provenance: self.provenance,
            fill: self.fill,
        })
    }

    /// The blob a changeset row references.
    ///
    fn ref_from_change(&self, change: &RowChange) -> Result<Option<BlobRef>, BlobDeclError> {
        let Some(id) = change.col(self.id_col).map(str::to_string) else {
            return Ok(None);
        };
        let cloud_path = self
            .cloud_path_col
            .and_then(|i| change.col(i))
            .map(str::to_string);
        self.blob_ref(id, self.scope.clone(), cloud_path).map(Some)
    }

    fn size_from_change(
        &self,
        table: &str,
        change: &RowChange,
    ) -> Result<Option<u64>, BlobDeclError> {
        let Some(raw) = change.col(self.size_col) else {
            return Ok(None);
        };
        let value = raw
            .parse::<i64>()
            .map_err(|_| BlobDeclError::InvalidSizeValue {
                table: table.to_string(),
                value: raw.to_string(),
            })?;
        Ok(Some(u64::try_from(value).map_err(|_| {
            BlobDeclError::InvalidSize {
                table: table.to_string(),
                value,
            }
        })?))
    }

    /// The blob's content hash as carried in a changeset row, or `None` when the
    /// hash column is absent from the row (an update that did not touch it). Read
    /// off the changeset row the same way the size is, so the eager pull can carry
    /// the author-signed hash forward to the download's verification without
    /// querying DB state that does not exist locally yet.
    fn hash_from_change(&self, change: &RowChange) -> Option<String> {
        change.col(self.hash_col).map(str::to_string)
    }

    /// Build the [`BlobRef`] for a live `SELECT *` row of this table, or `None` when
    /// the row's blob id is NULL. The resolved
    /// indices address a `SELECT *` row in schema order, exactly as they address a
    /// changeset row. Shared by [`BlobDecls::refs_in_db`] (whole DB) and
    /// [`BlobDecls::refs_for_root`] (one root's subtree).
    fn ref_from_row(&self, row: &rusqlite::Row<'_>) -> Result<Option<BlobRef>, BlobDeclError> {
        let Some(id) = row.get::<_, Option<String>>(self.id_col)? else {
            return Ok(None);
        };
        let cloud_path = match self.cloud_path_col {
            Some(i) => row.get::<_, Option<String>>(i)?,
            None => None,
        };
        self.blob_ref(id, self.scope.clone(), cloud_path).map(Some)
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
            let size_col = index_of(&decl.size_column)?;
            let hash_col = index_of(&decl.hash_column)?;
            let cloud_path_col = match &decl.cloud_path_column {
                Some(c) => Some(index_of(c)?),
                None => None,
            };

            map.insert(
                t.name().to_string(),
                TableBlob {
                    namespace: decl.namespace.clone(),
                    provenance: decl.provenance,
                    fill: decl.fill,
                    id_col,
                    size_col,
                    hash_col,
                    id_col_name: decl.id_column.clone(),
                    size_col_name: decl.size_column.clone(),
                    hash_col_name: decl.hash_column.clone(),
                    cloud_path_col,
                    cloud_path_col_name: decl.cloud_path_column.clone(),
                    scope: decl.scope.clone(),
                },
            );
        }
        Ok(BlobDecls { tables: map })
    }

    /// Install connection-local guards that keep a blob cleanup intent exclusive
    /// until its filesystem deletion finishes. A cleanup intent is committed
    /// before the database releases the row; while it exists, no INSERT or UPDATE
    /// may make the same `(namespace, blob id, content hash)` live again. TEMP
    /// triggers keep this runtime guard out of snapshots and use each declaration's
    /// resolved blob-id and content-hash columns rather than assuming the row primary
    /// key carries the blob id.
    pub(crate) fn install_cleanup_guards(&self, conn: &Connection) -> Result<(), BlobDeclError> {
        for (table, blob) in &self.tables {
            let table_ident = quote_ident(table);
            let id_ident = quote_ident(&blob.id_col_name);
            let hash_ident = quote_ident(&blob.hash_col_name);
            let namespace_literal: String =
                conn.query_row("SELECT quote(?1)", [&blob.namespace], |row| row.get(0))?;
            for (trigger_kind, event_clause) in [
                ("insert", "BEFORE INSERT".to_string()),
                (
                    "update",
                    format!("BEFORE UPDATE OF {id_ident}, {hash_ident}"),
                ),
            ] {
                let trigger = quote_ident(&format!("coven_cleanup_guard_{trigger_kind}_{table}"));
                conn.execute_batch(&format!(
                    "CREATE TEMP TRIGGER {trigger} \
                     {event_clause} ON main.{table_ident} \
                     WHEN NEW.{id_ident} IS NOT NULL AND EXISTS (\
                         SELECT 1 FROM local_cleanup_intents \
                         WHERE namespace = {namespace_literal} \
                           AND blob_id = NEW.{id_ident}\
                           AND content_hash = NEW.{hash_ident}\
                     ) \
                     BEGIN \
                         SELECT RAISE(ABORT, 'blob local cleanup in progress'); \
                     END;"
                ))?;
            }
        }
        Ok(())
    }

    /// The blob a single changeset row references, or `None` when the row's table
    /// carries no blob or the blob id is absent/NULL. Reads the declared columns
    /// off the changeset row (which reports columns in schema order, the order the
    /// resolved indices address).
    pub fn ref_from_change(&self, change: &RowChange) -> Result<Option<BlobRef>, BlobDeclError> {
        let Some(tb) = self.tables.get(&change.table) else {
            return Ok(None);
        };
        tb.ref_from_change(change)
    }

    /// The blob a changeset row references plus the row's declared plaintext size
    /// and content hash when those are present in the changeset row.
    /// Used by eager pull before the row is applied, so the downloader can stream
    /// the cloud object into the cache and verify the exact length and hash without
    /// querying DB state that does not exist locally yet.
    pub fn ref_size_hash_from_change(
        &self,
        change: &RowChange,
    ) -> Result<Option<ChangesetBlobDownload>, BlobDeclError> {
        let Some(tb) = self.tables.get(&change.table) else {
            return Ok(None);
        };
        let Some(blob) = tb.ref_from_change(change)? else {
            return Ok(None);
        };
        let size = tb.size_from_change(&change.table, change)?;
        let hash = tb.hash_from_change(change);
        Ok(Some((blob, size, hash)))
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
            let mut stmt = conn.prepare(&sql)?;
            let mut rows = stmt.query([&pk])?;
            if let Some(row) = rows.next()? {
                if let Some(blob) = tb.ref_from_row(row)? {
                    out.push(blob);
                }
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

    /// The plaintext byte length from the row carrying `blob_id` in `namespace`.
    pub fn size_for_blob_in_namespace(
        &self,
        conn: &Connection,
        namespace: &str,
        blob_id: &str,
    ) -> Result<Option<u64>, BlobDeclError> {
        let Some((table, tb)) = self.tables.iter().find(|(_, tb)| tb.namespace == namespace) else {
            return Ok(None);
        };
        pk_carrying_blob_size(conn, table, tb, blob_id)
    }

    /// The author-signed content hash from the row carrying `blob_id` in
    /// `namespace` — the value a whole-blob download verifies the decrypted
    /// plaintext against. `None` when no declared table owns `namespace`, that
    /// table has no row with the id, or the row's hash column is NULL.
    pub fn hash_for_blob_in_namespace(
        &self,
        conn: &Connection,
        namespace: &str,
        blob_id: &str,
    ) -> Result<Option<String>, BlobDeclError> {
        let Some((table, tb)) = self.tables.iter().find(|(_, tb)| tb.namespace == namespace) else {
            return Ok(None);
        };
        pk_carrying_blob_hash(conn, table, tb, blob_id)
    }

    /// The readable cloud path from the row carrying `blob_id` in `namespace` — the key
    /// a browsable home stores the blob at. `None` when no declared table owns
    /// `namespace`, that table declares no cloud-path column (an opaque home's blob is
    /// keyed by id), that table has no row with the id, or the row's value is NULL.
    pub fn cloud_path_for_blob_in_namespace(
        &self,
        conn: &Connection,
        namespace: &str,
        blob_id: &str,
    ) -> Result<Option<String>, BlobDeclError> {
        let Some((table, tb)) = self.tables.iter().find(|(_, tb)| tb.namespace == namespace) else {
            return Ok(None);
        };
        let Some(cloud_path_col_name) = &tb.cloud_path_col_name else {
            return Ok(None);
        };
        pk_carrying_blob_cloud_path(conn, table, tb, cloud_path_col_name, blob_id)
    }

    /// The plaintext byte length and content hash on row `pk` of `table` — the values a
    /// changeset UPDATE omitted because they did not change.
    ///
    /// Keyed by the row's primary key, which is the only handle a change always carries and
    /// a device can always resolve. The blob id would seem the natural key — it is how the
    /// size and hash are read everywhere else — but "the row carrying blob X" has no answer
    /// on a device that already applied a concurrent repointing of that very row: no row
    /// carries X any more, though the row itself is sitting right there under its `pk`.
    ///
    /// `None` when `table` carries no blob, has no such row, or the column is NULL.
    pub fn size_for_row(
        &self,
        conn: &Connection,
        table: &str,
        pk: &str,
    ) -> Result<Option<u64>, BlobDeclError> {
        let Some(tb) = self.tables.get(table) else {
            return Ok(None);
        };
        let size = column_on_row::<i64>(conn, table, &tb.size_col_name, pk)?;
        size.map(|value| {
            u64::try_from(value).map_err(|_| BlobDeclError::InvalidSize {
                table: table.to_string(),
                value,
            })
        })
        .transpose()
    }

    /// The content hash on row `pk` of `table`. The sibling of [`Self::size_for_row`], and
    /// keyed the same way and for the same reason.
    pub fn hash_for_row(
        &self,
        conn: &Connection,
        table: &str,
        pk: &str,
    ) -> Result<Option<String>, BlobDeclError> {
        let Some(tb) = self.tables.get(table) else {
            return Ok(None);
        };
        column_on_row::<String>(conn, table, &tb.hash_col_name, pk)
    }

    /// The `(table, primary key)` of a live row whose declared blob resolves to
    /// `cloud_key`. Both current generated layouts encode namespace + blob id. This
    /// is the GC-side lookup: before honoring a tombstone for `cloud_key`, ask
    /// whether current DB state still names that exact cloud object.
    pub fn row_for_blob_cloud_key(
        &self,
        conn: &Connection,
        cloud_key: &str,
    ) -> Result<Option<(String, String)>, BlobDeclError> {
        if let Some((namespace, blob_id)) =
            hashed_blob_key_parts(cloud_key).or_else(|| plain_blob_key_parts(cloud_key))
        {
            if let Some(row) = self.row_for_blob_in_namespace(conn, &namespace, &blob_id)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    pub fn ref_for_row(
        &self,
        conn: &Connection,
        table: &str,
        pk: &str,
    ) -> Result<Option<BlobRef>, BlobDeclError> {
        let Some(tb) = self.tables.get(table) else {
            return Ok(None);
        };
        let sql = format!("SELECT * FROM {} WHERE id = ?1", quote_ident(table));
        conn.query_row(&sql, [pk], |row| {
            tb.ref_from_row(row).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        })
        .optional()
        .map(Option::flatten)
        .map_err(BlobDeclError::from)
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

fn pk_carrying_blob_size(
    conn: &Connection,
    table: &str,
    tb: &TableBlob,
    blob_id: &str,
) -> Result<Option<u64>, BlobDeclError> {
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = ?1",
        quote_ident(&tb.size_col_name),
        quote_ident(table),
        quote_ident(&tb.id_col_name),
    );
    let size = conn
        .query_row(&sql, [blob_id], |row| row.get::<_, i64>(0))
        .optional()
        .map_err(BlobDeclError::from)?;
    size.map(|value| {
        u64::try_from(value).map_err(|_| BlobDeclError::InvalidSize {
            table: table.to_string(),
            value,
        })
    })
    .transpose()
}

fn pk_carrying_blob_hash(
    conn: &Connection,
    table: &str,
    tb: &TableBlob,
    blob_id: &str,
) -> Result<Option<String>, BlobDeclError> {
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = ?1",
        quote_ident(&tb.hash_col_name),
        quote_ident(table),
        quote_ident(&tb.id_col_name),
    );
    conn.query_row(&sql, [blob_id], |row| row.get::<_, Option<String>>(0))
        .optional()
        .map(Option::flatten)
        .map_err(BlobDeclError::from)
}

fn pk_carrying_blob_cloud_path(
    conn: &Connection,
    table: &str,
    tb: &TableBlob,
    cloud_path_col_name: &str,
    blob_id: &str,
) -> Result<Option<String>, BlobDeclError> {
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = ?1",
        quote_ident(cloud_path_col_name),
        quote_ident(table),
        quote_ident(&tb.id_col_name),
    );
    conn.query_row(&sql, [blob_id], |row| row.get::<_, Option<String>>(0))
        .optional()
        .map(Option::flatten)
        .map_err(BlobDeclError::from)
}

/// One column's value off the row with primary key `pk`. The row is named by the key every
/// changeset change carries, so this resolves on any device holding the row.
fn column_on_row<T: rusqlite::types::FromSql>(
    conn: &Connection,
    table: &str,
    column: &str,
    pk: &str,
) -> Result<Option<T>, BlobDeclError> {
    let sql = format!(
        "SELECT {} FROM {} WHERE id = ?1",
        quote_ident(column),
        quote_ident(table),
    );
    conn.query_row(&sql, [pk], |row| row.get::<_, Option<T>>(0))
        .optional()
        .map(Option::flatten)
        .map_err(BlobDeclError::from)
}

fn hashed_blob_key_parts(cloud_key: &str) -> Option<(String, String)> {
    // Map a key back to its DB row, which is keyed by namespace + id, not by who
    // uploaded it — so the uploader segment is parsed and dropped.
    crate::store_dir::StoreDir::parse_generated_blob_key(cloud_key)
        .map(|(namespace, _uploader, id, _generation)| (namespace, id))
}

fn plain_blob_key_parts(cloud_key: &str) -> Option<(String, String)> {
    let mut parts = cloud_key.splitn(6, '/');
    let namespace = parts.next()?;
    if parts.next()? != ".coven-generations" {
        return None;
    }
    let uploader = parts.next()?;
    let generation = parts.next()?;
    let id = parts.next()?;
    let cloud_path = parts.next()?;
    crate::store_dir::validate_path_token(namespace).ok()?;
    crate::store_dir::validate_path_token(uploader).ok()?;
    crate::store_dir::validate_path_token(id).ok()?;
    crate::store_dir::validate_cloud_path(cloud_path).ok()?;
    if generation.len() != 64
        || !generation
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    Some((namespace.to_string(), id.to_string()))
}

/// Column names of `table`, in declared (schema) order, via `PRAGMA table_info`.
/// The index of a name here is the index a changeset reports for that column.
pub(crate) fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>, BlobDeclError> {
    session_table_columns(conn, table).map_err(BlobDeclError::from)
}
