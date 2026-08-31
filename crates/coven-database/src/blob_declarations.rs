//! The resolved blob-declaration model: coven's own derivation of which rows
//! carry blobs and where each blob's columns live.
//!
//! The gate-sibling of [`Gates`](crate::Gates). A host declares per
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
//! download / apply-side local-copy drop),
//! [`BlobDecls::publication_blobs_in_db`] over the whole database, and
//! [`BlobDecls::row_for_blob_in_namespace`] to map a blob back to its row by namespace
//! (the read-path locality dispatch and the make-Remote completion check).
//!
//! A declaration's three blob properties — [`Provenance`] (the Local story),
//! [`CacheFill`] (the Remote story), and [`BlobReplacement`] (whether the row may be
//! repointed at a different blob) — are described by the blob concept tree in
//! the replication layer. The last of them is enforced here, because it is a rule about a
//! row's `(blob id, cloud path)` pair and this is the one place coven reads that pair off
//! a row: a replaceable blob's readable path must name its blob, and a write-once row may
//! never be repointed. Together they are what keeps a cloud object from ever being
//! rewritten with different bytes.

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use crate::{quote_ident, table_columns as session_table_columns};
use coven_foundation::changeset::{ChangeOp, RowChange};
use coven_protocol::blob::{
    cloud_path_names_blob, BlobRef, BlobReplacement, BlobScope, CacheFill, Provenance,
};
use coven_protocol::synced_schema::SyncedTable;

/// Why building the blob-declaration model failed.
#[derive(Debug)]
pub enum BlobDeclError {
    /// A declared blob column is absent from the table's live schema.
    MissingColumn { table: String, column: String },
    /// A schema read (`PRAGMA table_info`) failed.
    Sqlite(rusqlite::Error),
    /// A captured host changeset could not be read.
    Changeset(crate::ChangesetError),
    /// A row's declared size column is negative.
    InvalidSize { table: String, value: i64 },
    /// A row names a blob but has no content hash.
    MissingHash { table: String, row_id: String },
    /// New and old changeset walks produced different row counts.
    ChangesetWalkMismatch { old_count: usize, new_count: usize },
    /// A blob-bearing INSERT or UPDATE has no primary key.
    MissingPublicationPrimaryKey { table: String },
    /// The transaction row named by a blob-bearing change is absent.
    MissingPublicationRow { table: String, primary_key: String },
    /// The transaction row named by a blob-bearing change no longer carries a blob.
    MissingPublicationBlob { table: String, primary_key: String },
    /// The transaction row carries a different blob than its change introduced.
    PublicationBlobMismatch {
        table: String,
        primary_key: String,
        changed_blob_id: String,
        row_blob_id: String,
    },
    /// A [`Replaceable`](BlobReplacement::Replaceable) blob's readable cloud path does not
    /// name the blob it carries, so the row's next blob would be keyed at this blob's
    /// cloud object and overwrite it. See `cloud_path_names_blob`.
    CloudPathNotKeyedByBlob {
        table: String,
        blob_id: String,
        cloud_path: String,
    },
    /// A [`WriteOnce`](BlobReplacement::WriteOnce) row was repointed at a different blob.
    /// Its cloud path is a stable readable name — that is what write-once buys — so the
    /// new blob would be keyed at the old blob's cloud object and overwrite it.
    WriteOnceBlobRepointed { table: String, blob_id: String },
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
            BlobDeclError::Changeset(error) => {
                write!(f, "blob declaration changeset read failed: {error}")
            }
            BlobDeclError::InvalidSize { table, value } => {
                write!(f, "blob declaration found invalid size in {table}: {value}")
            }
            BlobDeclError::MissingHash { table, row_id } => write!(
                f,
                "blob-bearing row {table:?}/{row_id:?} has no content hash"
            ),
            BlobDeclError::ChangesetWalkMismatch {
                old_count,
                new_count,
            } => write!(
                f,
                "blob declaration changeset walk mismatch: old={old_count}, new={new_count}"
            ),
            BlobDeclError::MissingPublicationPrimaryKey { table } => {
                write!(f, "blob-bearing Store write row in {table:?} has no primary key")
            }
            BlobDeclError::MissingPublicationRow { table, primary_key } => write!(
                f,
                "blob-bearing Store write row {table:?}/{primary_key:?} is absent before commit"
            ),
            BlobDeclError::MissingPublicationBlob { table, primary_key } => write!(
                f,
                "blob-bearing Store write row {table:?}/{primary_key:?} no longer carries a blob"
            ),
            BlobDeclError::PublicationBlobMismatch {
                table,
                primary_key,
                changed_blob_id,
                row_blob_id,
            } => write!(
                f,
                "blob-bearing Store write row {table:?}/{primary_key:?} changed from introduced blob \
                 {changed_blob_id:?} to {row_blob_id:?} before commit"
            ),
            BlobDeclError::CloudPathNotKeyedByBlob {
                table,
                blob_id,
                cloud_path,
            } => write!(
                f,
                "replaceable blob {blob_id} in {table} has cloud path {cloud_path:?}, which does \
                 not name it: the path's file name must be the blob id, or end with -{blob_id} \
                 before its extension, so that replacing the blob moves its cloud key rather \
                 than overwriting its cloud object"
            ),
            BlobDeclError::WriteOnceBlobRepointed { table, blob_id } => write!(
                f,
                "write-once row in {table} was repointed at blob {blob_id}: a write-once blob's \
                 cloud path is a stable readable name, so the new blob would overwrite the cloud \
                 object of the blob it replaced. Declare the table replaceable (and key its path \
                 by its blob id) if its rows are meant to be repointed"
            ),
        }
    }
}

impl std::error::Error for BlobDeclError {}

impl From<rusqlite::Error> for BlobDeclError {
    fn from(e: rusqlite::Error) -> Self {
        BlobDeclError::Sqlite(e)
    }
}

/// Exact row facts captured with a durable Store write for one blob-bearing row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationBlob {
    pub table: String,
    pub row_id: String,
    pub row_stamp: String,
    pub column: String,
    pub blob: BlobRef,
    pub plaintext_size: u64,
    pub plaintext_hash: String,
}

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
    /// Index of the readable cloud-path column, if declared.
    cloud_path_col: Option<usize>,
    /// The encryption scope, fixed per table by the declaration.
    scope: BlobScope,
    /// Whether this table's row may be repointed at a different blob, and so which rule
    /// keeps its cloud object from ever being rewritten. See [`TableBlob::blob_ref`] and
    /// [`TableBlob::ref_from_change`].
    replacement: BlobReplacement,
}

impl TableBlob {
    /// Build the [`BlobRef`] for one of this table's rows from the per-row `id`,
    /// `scope`, and `cloud_path` plus this table's fixed namespace, provenance, and
    /// cache fill. Shared by changeset and live-row readers.
    ///
    /// The gate a [`Replaceable`](BlobReplacement::Replaceable) blob's readable cloud path
    /// passes through: it must name the blob ([`cloud_path_names_blob`]), so that a row
    /// repointed at a new blob keys it at a *new* cloud object rather than over the one it
    /// replaced. Every blob set coven derives — the push scan, the pull scan, the snapshot
    /// backfill, the transitions — is built here, so there is no path around it. A
    /// [`WriteOnce`](BlobReplacement::WriteOnce) blob is exempt: its row is never
    /// repointed ([`TableBlob::ref_from_change`] refuses that), so its object is written
    /// once and its path is free to be a stable readable name.
    fn blob_ref(
        &self,
        table: &str,
        id: String,
        scope: BlobScope,
        cloud_path: Option<String>,
    ) -> Result<BlobRef, BlobDeclError> {
        if self.replacement == BlobReplacement::Replaceable {
            if let Some(path) = cloud_path.as_deref() {
                if !cloud_path_names_blob(path, &id) {
                    return Err(BlobDeclError::CloudPathNotKeyedByBlob {
                        table: table.to_string(),
                        blob_id: id,
                        cloud_path: path.to_string(),
                    });
                }
            }
        }
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
    /// The gate a [`WriteOnce`](BlobReplacement::WriteOnce) row passes through. A
    /// changeset UPDATE reports only the columns whose values CHANGED, so the blob-id
    /// column appearing in one *is* the repointing: the row now names a different blob.
    /// That is what write-once forbids — its cloud path is a stable readable name, so the
    /// new blob would be keyed at the old blob's object and overwrite it. Refused here,
    /// where the change is read, rather than discovered as a corrupted bucket later.
    fn ref_from_change(
        &self,
        table: &str,
        change: &RowChange,
    ) -> Result<Option<BlobRef>, BlobDeclError> {
        let Some(id) = change.col(self.id_col).map(str::to_string) else {
            return Ok(None);
        };
        if self.replacement == BlobReplacement::WriteOnce && change.op == ChangeOp::Update {
            return Err(BlobDeclError::WriteOnceBlobRepointed {
                table: table.to_string(),
                blob_id: id,
            });
        }
        let cloud_path = self
            .cloud_path_col
            .and_then(|i| change.col(i))
            .map(str::to_string);
        self.blob_ref(table, id, self.scope.clone(), cloud_path)
            .map(Some)
    }

    /// Build the [`BlobRef`] for a live `SELECT *` row of this table, or `None` when
    /// the row's blob id is NULL. The resolved
    /// indices address a `SELECT *` row in schema order, exactly as they address a
    /// changeset row.
    fn ref_from_row(
        &self,
        table: &str,
        row: &rusqlite::Row<'_>,
    ) -> Result<Option<BlobRef>, BlobDeclError> {
        let Some(id) = row.get::<_, Option<String>>(self.id_col)? else {
            return Ok(None);
        };
        let cloud_path = match self.cloud_path_col {
            Some(i) => row.get::<_, Option<String>>(i)?,
            None => None,
        };
        self.blob_ref(table, id, self.scope.clone(), cloud_path)
            .map(Some)
    }

    fn size_from_row(&self, table: &str, row: &rusqlite::Row<'_>) -> Result<u64, BlobDeclError> {
        let value = row.get::<_, i64>(self.size_col)?;
        u64::try_from(value).map_err(|_| BlobDeclError::InvalidSize {
            table: table.to_string(),
            value,
        })
    }

    fn hash_from_row(
        &self,
        table: &str,
        row_id: &str,
        row: &rusqlite::Row<'_>,
    ) -> Result<String, BlobDeclError> {
        row.get::<_, Option<String>>(self.hash_col)?
            .ok_or_else(|| BlobDeclError::MissingHash {
                table: table.to_string(),
                row_id: row_id.to_string(),
            })
    }
}

/// The blob declarations for a database handle, resolved from the declared set +
/// the live schema at open. A synced table absent from this map carries no blob.
pub struct BlobDecls {
    tables: HashMap<String, TableBlob>,
}

#[cfg(any(test, feature = "test-utils"))]
thread_local! {
    static FROM_TABLES_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, feature = "test-utils"))]
pub fn reset_from_tables_call_count() {
    FROM_TABLES_CALLS.with(|calls| calls.set(0));
}

#[cfg(any(test, feature = "test-utils"))]
pub fn from_tables_call_count() -> usize {
    FROM_TABLES_CALLS.with(std::cell::Cell::get)
}

impl BlobDecls {
    /// Resolve every [`SyncedTable::carries_blob`] declaration's column names to
    /// indices against the live schema, mirroring
    /// [`Gates::from_tables`](crate::Gates::from_tables). A declared
    /// column absent from the table is a host error surfaced here, never a silent
    /// drop.
    pub(crate) fn from_tables(
        conn: &Connection,
        tables: &[SyncedTable],
    ) -> Result<Self, BlobDeclError> {
        #[cfg(any(test, feature = "test-utils"))]
        FROM_TABLES_CALLS.with(|calls| calls.set(calls.get() + 1));

        let mut map = HashMap::new();
        for t in tables {
            let Some(decl) = t.blob() else {
                continue;
            };
            // Column names in declared (schema) order — the index of a name here
            // is the index a changeset reports for that column.
            let cols = session_table_columns(conn, t.name()).map_err(BlobDeclError::from)?;
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
                    cloud_path_col,
                    scope: decl.scope.clone(),
                    replacement: decl.replacement,
                },
            );
        }
        Ok(BlobDecls { tables: map })
    }

    /// Install connection-local guards that keep a blob cleanup intent exclusive
    /// until its filesystem deletion finishes. A cleanup intent is committed
    /// before the database releases the row; while either cleanup queue holds it,
    /// no INSERT or UPDATE may make the same `(namespace, blob id)` live again.
    /// TEMP triggers keep this runtime guard out of snapshots and use each
    /// declaration's resolved blob-id column rather than assuming the row primary
    /// key carries the blob id.
    pub(crate) fn install_cleanup_guards(&self, conn: &Connection) -> Result<(), BlobDeclError> {
        for (table, blob) in &self.tables {
            let table_ident = quote_ident(table);
            let id_ident = quote_ident(&blob.id_col_name);
            let namespace_literal: String =
                conn.query_row("SELECT quote(?1)", [&blob.namespace], |row| row.get(0))?;
            for (trigger_kind, event_clause) in [
                ("insert", "BEFORE INSERT".to_string()),
                ("update", format!("BEFORE UPDATE OF {id_ident}")),
            ] {
                let trigger = quote_ident(&format!(
                    "{}{trigger_kind}_{table}",
                    super::COVEN_CLEANUP_GUARD_PREFIX
                ));
                conn.execute_batch(&format!(
                    "CREATE TEMP TRIGGER {trigger} \
                     {event_clause} ON main.{table_ident} \
                     WHEN NEW.{id_ident} IS NOT NULL AND (\
                         EXISTS (\
                             SELECT 1 FROM local_cleanup_intents \
                             WHERE namespace = {namespace_literal} \
                               AND blob_id = NEW.{id_ident}\
                         ) OR EXISTS (\
                             SELECT 1 FROM published_blob_drop_intents \
                             WHERE namespace = {namespace_literal} \
                               AND blob_id = NEW.{id_ident}\
                         )\
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
        tb.ref_from_change(&change.table, change)
    }

    /// The exact blob reference and declared size owned by an INSERT or UPDATE,
    /// completed from that row inside the transaction that produced the change.
    /// Changesets omit unchanged columns, so the change identifies whether this
    /// write introduced a blob while the live transaction row supplies its full
    /// cloud path and size before a later write can repoint or delete it.
    pub(crate) fn publication_blob_from_change(
        &self,
        conn: &Connection,
        change: &RowChange,
    ) -> Result<Option<PublicationBlob>, BlobDeclError> {
        if !matches!(change.op, ChangeOp::Insert | ChangeOp::Update) {
            return Ok(None);
        }
        let Some(tb) = self.tables.get(&change.table) else {
            return Ok(None);
        };
        let Some(changed_blob) = tb.ref_from_change(&change.table, change)? else {
            return Ok(None);
        };
        let pk = change
            .pk()
            .ok_or_else(|| BlobDeclError::MissingPublicationPrimaryKey {
                table: change.table.clone(),
            })?;
        let sql = format!("SELECT * FROM {} WHERE id = ?1", quote_ident(&change.table));
        let mut statement = conn.prepare(&sql)?;
        let mut rows = statement.query([pk])?;
        let row = rows
            .next()?
            .ok_or_else(|| BlobDeclError::MissingPublicationRow {
                table: change.table.clone(),
                primary_key: pk.to_string(),
            })?;
        let publication = publication_blob_from_row(&change.table, tb, row)?;
        let blob = &publication.blob;
        if blob.id != changed_blob.id {
            return Err(BlobDeclError::PublicationBlobMismatch {
                table: change.table.clone(),
                primary_key: pk.to_string(),
                changed_blob_id: changed_blob.id,
                row_blob_id: blob.id.clone(),
            });
        }
        Ok(Some(publication))
    }

    pub(crate) fn publication_blob_for_row(
        &self,
        conn: &Connection,
        table: &str,
        row_id: &str,
    ) -> Result<Option<PublicationBlob>, BlobDeclError> {
        let Some(blob) = self.tables.get(table) else {
            return Ok(None);
        };
        let sql = format!("SELECT * FROM {} WHERE id = ?1", quote_ident(table));
        let mut statement = conn.prepare(&sql)?;
        let mut rows = statement.query([row_id])?;
        rows.next()?
            .map(|row| publication_blob_from_row(table, blob, row))
            .transpose()
    }

    /// Require every inserted or updated blob-bearing row in `changeset` to
    /// carry complete final content facts before its transaction can commit.
    pub(crate) fn validate_changed_rows(
        &self,
        conn: &Connection,
        changeset: &[u8],
    ) -> Result<(), BlobDeclError> {
        let changes = crate::walk_changeset(changeset).map_err(BlobDeclError::Changeset)?;
        for change in changes {
            if !matches!(change.op, ChangeOp::Insert | ChangeOp::Update)
                || !self.tables.contains_key(&change.table)
            {
                continue;
            }
            let row_id =
                change
                    .pk()
                    .ok_or_else(|| BlobDeclError::MissingPublicationPrimaryKey {
                        table: change.table.clone(),
                    })?;
            match self.publication_blob_for_row(conn, &change.table, row_id) {
                Ok(_) | Err(BlobDeclError::MissingPublicationBlob { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Every exact blob-bearing row version currently present in `conn`.
    pub(crate) fn publication_blobs_in_db(
        &self,
        conn: &Connection,
    ) -> Result<Vec<PublicationBlob>, BlobDeclError> {
        let mut out = Vec::new();
        for (table, blob) in &self.tables {
            let sql = format!("SELECT * FROM {}", quote_ident(table));
            let mut statement = conn.prepare(&sql)?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let Some(reference) = blob.ref_from_row(table, row)? else {
                    continue;
                };
                let row_id = row.get::<_, String>("id")?;
                out.push(PublicationBlob {
                    table: table.clone(),
                    row_id: row_id.clone(),
                    row_stamp: row.get("_updated_at")?,
                    column: blob.id_col_name.clone(),
                    blob: reference,
                    plaintext_size: blob.size_from_row(table, row)?,
                    plaintext_hash: blob.hash_from_row(table, &row_id, row)?,
                });
            }
        }
        out.sort_by(|left, right| {
            (&left.table, &left.row_id, &left.column, &left.row_stamp).cmp(&(
                &right.table,
                &right.row_id,
                &right.column,
                &right.row_stamp,
            ))
        });
        Ok(out)
    }

    /// The `(table, primary key)` of the row carrying `blob_id` in the table declared
    /// for `namespace` — the carrying table resolved from the blob's own namespace
    /// (part of its address), not by scanning every blob-bearing table. `None` when no
    /// declared table owns `namespace`, or that table has no row with the id. Both the
    /// read path (locality dispatch) and the make-Remote completion check use this, so
    /// a blob id that collides across namespaces always reads the right table's gate,
    /// never the first id match.
    pub(crate) fn row_for_blob_in_namespace(
        &self,
        conn: &Connection,
        namespace: &str,
        blob_id: &str,
    ) -> Result<Option<(String, String)>, BlobDeclError> {
        let Some((table, tb)) = self.table_for_namespace(namespace) else {
            return Ok(None);
        };
        let sql = format!(
            "SELECT id FROM {} WHERE {} = ?1",
            quote_ident(table),
            quote_ident(&tb.id_col_name),
        );
        conn.query_row(&sql, [blob_id], |row| row.get::<_, String>(0))
            .optional()
            .map(|primary_key| primary_key.map(|primary_key| (table.clone(), primary_key)))
            .map_err(BlobDeclError::from)
    }

    /// The one `(table, declaration)` whose blob namespace is `namespace`, or
    /// `None` when no declared table owns it. The namespace is part of a blob's
    /// address, so this resolves the carrying table without scanning every
    /// blob-bearing table's rows.
    fn table_for_namespace(&self, namespace: &str) -> Option<(&String, &TableBlob)> {
        self.tables.iter().find(|(_, tb)| tb.namespace == namespace)
    }

    /// Whether a live row still needs the logical-id-keyed local source for this
    /// blob. A row needs that source exactly when its current stamp has no installed
    /// remote locator binding.
    pub(crate) fn local_copy_is_referenced(
        &self,
        conn: &Connection,
        namespace: &str,
        blob_id: &str,
    ) -> Result<bool, BlobDeclError> {
        let Some((table, blob)) = self.table_for_namespace(namespace) else {
            return Ok(false);
        };
        let sql = format!(
            "SELECT EXISTS(
                 SELECT 1 FROM {table} AS live
                 WHERE CAST(live.{blob_column} AS TEXT) = ?1
                   AND NOT EXISTS (
                       SELECT 1 FROM row_blob_locators AS binding
                       WHERE binding.table_name = ?2
                         AND binding.row_id = CAST(live.id AS TEXT)
                         AND binding.column_name = ?3
                         AND binding.row_stamp = CAST(live._updated_at AS TEXT)
                   )
             )",
            table = quote_ident(table),
            blob_column = quote_ident(&blob.id_col_name),
        );
        conn.query_row(
            &sql,
            rusqlite::params![blob_id, table, blob.id_col_name],
            |row| row.get(0),
        )
        .map_err(BlobDeclError::from)
    }

    /// Whether any live row still carries this logical blob ID, independent of
    /// whether that row currently resolves to a local source or an exact remote
    /// locator.
    pub(crate) fn blob_id_is_referenced(
        &self,
        conn: &Connection,
        namespace: &str,
        blob_id: &str,
    ) -> Result<bool, BlobDeclError> {
        let Some((table, blob)) = self.table_for_namespace(namespace) else {
            return Ok(false);
        };
        let sql = format!(
            "SELECT EXISTS(
                 SELECT 1 FROM {table}
                 WHERE CAST({blob_column} AS TEXT) = ?1
             )",
            table = quote_ident(table),
            blob_column = quote_ident(&blob.id_col_name),
        );
        conn.query_row(&sql, [blob_id], |row| row.get(0))
            .map_err(BlobDeclError::from)
    }

    /// Whether a live row's current stamp still names one exact locator. A row
    /// that merely reuses the same logical blob id under another locator does not
    /// retain this locator's cache or pinned file.
    pub(crate) fn exact_copy_is_referenced(
        &self,
        conn: &Connection,
        namespace: &str,
        blob_id: &str,
        locator_hash: coven_protocol::store_commit::ObjectHash,
    ) -> Result<bool, BlobDeclError> {
        let Some((table, blob)) = self.table_for_namespace(namespace) else {
            return Ok(false);
        };
        let sql = format!(
            "SELECT EXISTS(
                 SELECT 1
                 FROM {table} AS live
                 JOIN row_blob_locators AS binding
                   ON binding.table_name = ?2
                  AND binding.row_id = CAST(live.id AS TEXT)
                  AND binding.column_name = ?3
                  AND binding.row_stamp = CAST(live._updated_at AS TEXT)
                 JOIN blob_locators AS locator
                   ON locator.remote_object_id = binding.remote_object_id
                 WHERE CAST(live.{blob_column} AS TEXT) = ?1
                   AND locator.locator_hash = ?4
             )",
            table = quote_ident(table),
            blob_column = quote_ident(&blob.id_col_name),
        );
        conn.query_row(
            &sql,
            rusqlite::params![blob_id, table, blob.id_col_name, locator_hash.to_string()],
            |row| row.get(0),
        )
        .map_err(BlobDeclError::from)
    }
}

fn publication_blob_from_row(
    table: &str,
    blob: &TableBlob,
    row: &rusqlite::Row<'_>,
) -> Result<PublicationBlob, BlobDeclError> {
    let row_id = row.get::<_, String>("id")?;
    let reference =
        blob.ref_from_row(table, row)?
            .ok_or_else(|| BlobDeclError::MissingPublicationBlob {
                table: table.to_string(),
                primary_key: row_id.clone(),
            })?;
    let plaintext_hash = blob.hash_from_row(table, &row_id, row)?;
    Ok(PublicationBlob {
        table: table.to_string(),
        row_id,
        row_stamp: row.get("_updated_at")?,
        column: blob.id_col_name.clone(),
        blob: reference,
        plaintext_size: blob.size_from_row(table, row)?,
        plaintext_hash,
    })
}
