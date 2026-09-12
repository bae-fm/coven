//! The receive side of a Circle bootstrap: one payload decoded onto the
//! receiver's own schema, checked against the signed reference, and held as
//! the source the install copies from.

use std::collections::BTreeSet;

use rusqlite::Connection;

use super::snapshot_image::{circle_projection_tables, SnapshotImageError};
use crate::DbError;
use coven_protocol::remote_object;
use coven_protocol::synced_schema::SyncedTable;

pub(super) fn verify_circle_bootstrap_image(
    receiver: &Connection,
    rows: &[u8],
    reference: &coven_protocol::circle::CircleBootstrapRef,
    circle_id: coven_protocol::circle::CircleId,
    tables: &[SyncedTable],
    routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
) -> Result<(), SnapshotImageError> {
    if coven_protocol::store_commit::ObjectHash::digest(rows) != reference.image.image_hash {
        return Err(SnapshotImageError::Projection(
            "Circle bootstrap image differs from its signed hash".to_string(),
        ));
    }
    verify_circle_bootstrap_rows(receiver, rows, reference, circle_id, tables, routing_key)
        .map(|_| ())
}

/// One Circle bootstrap changeset decoded onto the receiver's own schema: the
/// projection tables and nothing else, holding exactly the rows the payload
/// states. Verification reads it and installation copies out of it; it is
/// never serialized.
pub(crate) struct StagedCircleRows {
    scratch: Connection,
}

impl StagedCircleRows {
    /// Decode `rows` onto the projection tables as the receiver declares them.
    /// Every change must insert a row of a projection table, host rows carry
    /// their declared identities, and no row is stated twice.
    pub(super) fn stage(
        receiver: &Connection,
        rows: &[u8],
        tables: &[SyncedTable],
    ) -> Result<Self, SnapshotImageError> {
        let projection_tables = circle_projection_tables(receiver, tables)?;
        let mut stated = BTreeSet::new();
        for change in changeset_rows(rows)? {
            if change.op != rusqlite::hooks::Action::SQLITE_INSERT {
                return Err(SnapshotImageError::Projection(format!(
                    "Circle bootstrap changeset carries a non-insert change to {}",
                    change.table
                )));
            }
            if !projection_tables.contains(&change.table) {
                return Err(SnapshotImageError::Projection(format!(
                    "Circle bootstrap changeset names undeclared table {}",
                    change.table
                )));
            }
            if !stated.insert((change.table.clone(), change.row_id.clone())) {
                return Err(SnapshotImageError::Projection(format!(
                    "Circle bootstrap changeset repeats row {}.{}",
                    change.table, change.row_id
                )));
            }
        }
        crate::changeset_identity::validate_changeset_row_identities(
            &crate::gate::recorded_host_changeset(rows)?,
            tables,
        )
        .map_err(|error| {
            SnapshotImageError::Projection(format!("Circle bootstrap changeset identity: {error}"))
        })?;

        let scratch = Connection::open_in_memory().map_err(DbError::from)?;
        scratch
            .pragma_update(None, "foreign_keys", "OFF")
            .map_err(SnapshotImageError::from)?;
        for table in &projection_tables {
            let create = crate::create_table_sql(receiver, table).map_err(|error| {
                SnapshotImageError::Projection(format!(
                    "Circle bootstrap projection table {table}: {error}"
                ))
            })?;
            scratch.execute_batch(&create).map_err(|error| {
                SnapshotImageError::ProjectionSqlite {
                    operation: format!("create Circle bootstrap table {table}"),
                    source: error,
                }
            })?;
        }
        scratch
            .apply_strm(
                &mut &rows[..],
                None::<fn(&str) -> bool>,
                |_conflict, _item| rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT,
            )
            .map_err(|error| SnapshotImageError::ProjectionSqlite {
                operation: "apply Circle bootstrap rows".to_string(),
                source: error,
            })?;
        Ok(Self { scratch })
    }

    /// Install these rows, their routes, and their blob graph onto `conn`
    /// directly — no transaction of its own. `conn` is the caller's active
    /// transaction: the pull replay wraps this in a fresh throwaway
    /// transaction; the snapshot-restore installer runs it inside the single
    /// install transaction alongside the Store image, so the whole set commits
    /// or rolls back together. Foreign keys are deferred to that outer commit,
    /// matching the final foreign-key validation the install runs over the
    /// installed union.
    pub(crate) fn install_on(
        &self,
        conn: &Connection,
        synced_tables: &[SyncedTable],
        activation_commit: &coven_protocol::store_commit::StoreBatchCommitRef,
        circle_id: coven_protocol::circle::CircleId,
        reference: &coven_protocol::circle::CircleBootstrapRef,
    ) -> Result<(), DbError> {
        let source = &self.scratch;
        let projection_tables = circle_projection_tables(conn, synced_tables)
            .map_err(|error| DbError::context("Circle bootstrap projection tables", error))?;
        conn.pragma_update(None, "defer_foreign_keys", "ON")
            .map_err(DbError::from)?;
        // The bootstrap is this Circle's whole state at its coverage, so every row
        // it names it also supersedes. The target can already hold one: a restore
        // carries the routing tables wholesale, and a replay base built from a
        // baseline image holds the rows that stood before the bootstrap — including
        // a row that was in the Store audience and has since moved into the Circle.
        // Clearing what the image restates makes the install the same operation
        // whatever it lands on, rather than one that only works onto an empty base.
        let superseded = if projection_tables
            .iter()
            .any(|table| table == "_coven_row_routes")
        {
            crate::query_mapped_rows(
                source,
                "SELECT table_name, row_id FROM _coven_row_routes",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?
        } else {
            Vec::new()
        };
        for (table, row_id) in &superseded {
            if !projection_tables.contains(table) {
                return Err(DbError::Message(format!(
                    "Circle {circle_id} bootstrap routes a row in unprojected table {table:?}"
                )));
            }
            conn.execute(
                &format!("DELETE FROM {} WHERE id = ?1", crate::quote_ident(table)),
                [row_id],
            )
            .map_err(DbError::from)?;
            // The audience row is keyed by the routing id, which only the route
            // names, so it goes before the route that finds it.
            conn.execute(
                "DELETE FROM _coven_audience WHERE routing_id IN (
                     SELECT routing_id FROM _coven_row_routes
                     WHERE table_name = ?1 AND row_id = ?2
                 )",
                rusqlite::params![table, row_id],
            )
            .map_err(DbError::from)?;
            conn.execute(
                "DELETE FROM _coven_row_routes WHERE table_name = ?1 AND row_id = ?2",
                rusqlite::params![table, row_id],
            )
            .map_err(DbError::from)?;
        }
        for table in &projection_tables {
            // The routing tables are deterministic in the row they describe, so a
            // target that already holds an entry holds the same one — a restore
            // carries them wholesale. Skipping a re-insert there is not papering
            // over a conflict; the data tables above have had everything this
            // bootstrap restates cleared, so they insert exactly once.
            let ignore_existing = table == "_coven_audience" || table == "_coven_row_routes";
            crate::copy_table_with_conflicts(source, conn, table, ignore_existing).map_err(
                |error| {
                    DbError::context(
                        format!("install exact Circle {} bootstrap table {table}", circle_id),
                        error,
                    )
                },
            )?;
        }
        super::pull_replay::install_circle_bootstrap_remote_objects_from_reference_on(
            conn,
            activation_commit,
            reference,
        )?;
        for binding in &reference.blobs {
            let stored = binding.stored().ok_or_else(|| {
                DbError::Message("Circle bootstrap row blob has no exact locator".to_string())
            })?;
            let object_id = remote_object::remote_object_id(stored.object());
            let coven_protocol::blob::RowBlobAuthority::Remote(authority) = binding.authority()
            else {
                return Err(DbError::Message(
                    "Circle bootstrap row blob lacks remote package authority".to_string(),
                ));
            };
            crate::blob_records::validate_stored_locator_on(conn, stored)?;
            let encoded_authority = serde_json::to_string(authority).map_err(|error| {
                DbError::context("serialize Circle bootstrap blob authority", error)
            })?;
            let binding_inserted = conn
                .execute(
                    "INSERT INTO row_blob_locators
                 (table_name, row_id, column_name, row_stamp, audience_authority, remote_object_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(table_name, row_id, column_name, row_stamp) DO NOTHING",
                    rusqlite::params![
                        binding.table(),
                        binding.row_id(),
                        binding.column(),
                        binding.row_stamp(),
                        &encoded_authority,
                        object_id.to_string(),
                    ],
                )
                .map_err(DbError::from)?;
            if binding_inserted == 0 {
                let (retained_authority, retained_object): (String, String) = conn
                    .query_row(
                        "SELECT audience_authority, remote_object_id
                         FROM row_blob_locators
                         WHERE table_name = ?1 AND row_id = ?2
                           AND column_name = ?3 AND row_stamp = ?4",
                        rusqlite::params![
                            binding.table(),
                            binding.row_id(),
                            binding.column(),
                            binding.row_stamp(),
                        ],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                if retained_authority != encoded_authority
                    || retained_object != object_id.to_string()
                {
                    return Err(DbError::Message(format!(
                        "Circle bootstrap row blob binding conflicts for {}.{}.{} at {}",
                        binding.table(),
                        binding.row_id(),
                        binding.column(),
                        binding.row_stamp(),
                    )));
                }
            }
        }
        Ok(())
    }

    /// These rows as a database image, so a test reads what the payload states
    /// through the ordinary image readers.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn database_bytes(&self) -> Result<Vec<u8>, DbError> {
        crate::connection_io::serialize_database_image(&self.scratch)
    }
}

/// The bootstrap payload's changes, each as its operation, table, and the
/// primary key the row states.
pub(crate) struct StagedChange {
    pub(crate) table: String,
    pub(crate) op: rusqlite::hooks::Action,
    pub(crate) row_id: String,
}

pub(crate) fn changeset_rows(rows: &[u8]) -> Result<Vec<StagedChange>, SnapshotImageError> {
    use fallible_streaming_iterator::FallibleStreamingIterator;

    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let input: &mut dyn std::io::Read = &mut &rows[..];
    let mut iterator =
        rusqlite::session::ChangesetIter::start_strm(&input).map_err(SnapshotImageError::from)?;
    let mut changes = Vec::new();
    while let Some(item) = iterator.next().map_err(SnapshotImageError::from)? {
        let operation = item.op().map_err(SnapshotImageError::from)?;
        let table = operation.table_name().to_string();
        let op = operation.code();
        // An insert states its primary key on the new side, an update or a
        // delete on the old one. A bootstrap refuses those two below, but it
        // still names the row they touch.
        let stated = match op {
            rusqlite::hooks::Action::SQLITE_INSERT => item.new_value(0),
            _ => item.old_value(0),
        };
        let row_id = match stated {
            Ok(value) => crate::value_ref_to_string(value),
            Err(rusqlite::Error::InvalidColumnIndex(_)) => None,
            Err(error) => return Err(SnapshotImageError::from(error)),
        }
        .ok_or_else(|| {
            SnapshotImageError::Projection(format!(
                "Circle bootstrap changeset states a row of {table} without a primary key"
            ))
        })?;
        changes.push(StagedChange { table, op, row_id });
    }
    Ok(changes)
}

/// Verify one Circle bootstrap payload against the receiver's own schema and
/// routing contract, and stage its rows for installation.
pub(crate) fn verify_circle_bootstrap_rows(
    receiver: &Connection,
    rows: &[u8],
    reference: &coven_protocol::circle::CircleBootstrapRef,
    circle_id: coven_protocol::circle::CircleId,
    tables: &[SyncedTable],
    routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
) -> Result<StagedCircleRows, SnapshotImageError> {
    let schema_version: u32 = receiver
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(SnapshotImageError::from)?;
    if reference.schema_version != schema_version {
        return Err(SnapshotImageError::Projection(format!(
            "Circle bootstrap schema is {}, this database is {schema_version}",
            reference.schema_version
        )));
    }
    let routing_contract = crate::SyncRoutingContract::from_connection(receiver, tables)
        .map_err(SnapshotImageError::from)?;
    if routing_contract.hash() != reference.sync_routing_hash {
        return Err(SnapshotImageError::Projection(
            "Circle bootstrap routing contract differs from its signed hash".to_string(),
        ));
    }
    let staged = StagedCircleRows::stage(receiver, rows, tables)?;
    let scratch = &staged.scratch;
    let gates = crate::Gates::from_tables(scratch, tables).map_err(SnapshotImageError::from)?;
    if gates.has_scoped_graph() {
        let routing_key = routing_key.ok_or_else(|| {
            SnapshotImageError::Projection(
                "scoped Circle bootstrap verification requires Store routing authentication"
                    .to_string(),
            )
        })?;
        crate::validate_snapshot_routing_state(
            scratch,
            &gates,
            routing_key,
            &coven_protocol::circle::Audience::Circle(circle_id),
        )
        .map_err(SnapshotImageError::from)?;
    }
    let declarations =
        crate::BlobDecls::from_tables(scratch, tables).map_err(SnapshotImageError::from)?;
    let bound = declarations
        .publication_blobs_in_db(scratch)
        .map_err(SnapshotImageError::from)?;
    if bound.len() != reference.blobs.len() {
        return Err(SnapshotImageError::Projection(
            "Circle bootstrap blob closure does not exactly cover its image rows".to_string(),
        ));
    }
    for row in &bound {
        let mut matching = reference.blobs.iter().filter(|binding| {
            row.table == binding.table()
                && row.row_id == binding.row_id()
                && row.row_stamp == binding.row_stamp()
                && row.column == binding.column()
        });
        let binding = matching.next().ok_or_else(|| {
            SnapshotImageError::Projection(
                "Circle bootstrap image row has no exact signed blob binding".to_string(),
            )
        })?;
        if matching.next().is_some()
            || &row.blob != binding.blob()
            || row.plaintext_size != binding.plaintext_size()
            || row.plaintext_hash != binding.plaintext_hash().to_string()
            || !matches!(
                binding.authority(),
                coven_protocol::blob::RowBlobAuthority::Remote(
                    coven_protocol::audience_package::PackageAudience::Circle {
                        circle_id: binding_circle,
                        ..
                    }
                ) if *binding_circle == circle_id
            )
            || binding.stored().is_none()
        {
            return Err(SnapshotImageError::Projection(
                "Circle bootstrap blob closure differs from an exact image row".to_string(),
            ));
        }
    }
    Ok(staged)
}

#[cfg(test)]
#[path = "circle_bootstrap_rows_tests.rs"]
mod tests;
