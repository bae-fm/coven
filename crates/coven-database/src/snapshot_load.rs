//! Install prepared records with row replacement and local replay in one commit.

use crate::merge_store::MergeStore;
use crate::snapshot_error::{invalid, SnapshotError};
use crate::snapshot_metadata::SnapshotMetadata;
use crate::snapshot_preparation::PreparedSnapshot;
use crate::sqlite::DatabaseConnection;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::snapshot::{SnapshotHeader, SnapshotRecord};
use coven_merge::RowId;
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn load<P: crate::PendingValue>(
    database: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    reload: crate::SnapshotReload<PreparedSnapshot<'_>, crate::download_stream::PreparedWrite<'_>>,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<Vec<crate::LoadedSnapshot>, DbError> {
    database.stream_transaction(|database| {
        let crate::SnapshotReload {
            snapshots,
            slots,
            target,
            state,
            provider,
            cutoffs,
            boundaries,
            operation,
            authorship: _,
        } = reload;
        let current = crate::sequence::applied(database)?;
        if target < current {
            return Err(invalid("reload target precedes committed applied slot"));
        }
        crate::store_state_tables::replace(database, &state)?;
        provider.record::<P>(database)?;
        database.internal_execute("DELETE FROM _coven_cutoffs", [])?;
        for (slot, change) in cutoffs {
            if slot.0 > target {
                return Err(invalid("cut-off exceeds reload target"));
            }
            crate::sequence::cutoff(database, slot, &change)?;
        }
        let resets = crate::BoundaryCoverage::replace(database, boundaries)?;
        if !resets.is_empty() {
            crate::pending::reset(database)?;
        }
        let mut coverage = crate::snapshot_coverage::SnapshotCoverage::new(target);
        crate::download_stage::begin(database)?;
        let mut touched = crate::published::restore(database, schema)?;
        crate::snapshot_state::create_tables(database)?;
        let mut loaded = Vec::new();
        let mut selected = BTreeSet::new();
        for source in snapshots {
            match source {
                crate::SnapshotSource::Current(audience) => {
                    coverage.loaded(audience, current)?;
                }
                crate::SnapshotSource::Stored { id, input, author } => {
                    if !selected.insert(id.audience.clone()) {
                        return Err(invalid("reload repeats an audience"));
                    }
                    let (header, rows) =
                        read(database, schema, input, Some(author)).map_err(|error| {
                            SnapshotError::Body {
                                snapshot: id.clone(),
                                error: Box::new(error),
                            }
                        })?;
                    let files = crate::database::file_retention::snapshot_references(
                        database,
                        &id.audience,
                        schema,
                    )?;
                    coverage.loaded(id.audience.clone(), header.covers)?;
                    database.internal_execute(
                        "DELETE FROM _coven_raise_captures WHERE audience=?1 AND version<=?2",
                        params![
                            crate::write_encoding::audience_text(&id.audience),
                            header.schema_version
                        ],
                    )?;
                    database.internal_execute(
                        "DELETE FROM _coven_migration_losses WHERE audience=?1",
                        [crate::write_encoding::audience_text(&id.audience)],
                    )?;
                    touched.extend(rows);
                    loaded.push(crate::LoadedSnapshot { id, files });
                }
                crate::SnapshotSource::Empty(audience) => {
                    if !selected.insert(audience.clone()) {
                        return Err(invalid("reload repeats an audience"));
                    }
                    touched.extend(crate::snapshot_state::begin(database, schema, &audience)?);
                    crate::ownership::clear_audience(database, &audience)?;
                    coverage.loaded(audience, 0)?;
                }
            }
        }
        for audience in &selected {
            database.internal_execute(
                "DELETE FROM temp._coven_download_rows WHERE audience=?1",
                [crate::write_encoding::audience_text(audience)],
            )?;
        }
        // Move each loaded merge row's raw values into the common staging view.
        let stored =
            MergeStore::from_snapshot(database, &schema.schema, &selected).without_row_cache();
        for audience in &selected {
            database.for_each(
                "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE audience=?1",
                [crate::write_encoding::audience_text(audience)],
                |r| {
                    let row = crate::row_queries::read_identity(r)?;
                    crate::download_stage::put_values(
                        database,
                        &row,
                        &stored.row_without_losses(&row)?.state,
                    )
                },
            )?;
        }
        crate::snapshot_state::drop_tables(database)?;
        crate::snapshot_writes::apply::<P>(database, schema, slots, &coverage)?;
        for audience in &selected {
            crate::reload_gate::clear_reload(database, audience)?;
        }
        crate::queued_writes::overlay(database, schema)?;
        let deleted = crate::store_state_tables::deleted_circles(database)?;
        crate::download_stage::materialize(database, before, schema, &deleted, touched, files)?;
        crate::pending::clear_covered_slots(database)?;
        files.before_commit()?;
        if let Some(operation) = operation {
            crate::operation::advance(database, &operation)?;
        }
        Ok(loaded)
    })
}

pub(crate) fn read(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    prepared: PreparedSnapshot<'_>,
    author: Option<coven_foundation::id_source::MemberId>,
) -> Result<(SnapshotHeader, BTreeSet<RowId>), DbError> {
    let header = prepared.header.clone();
    let expected = &header.id;
    let local_version = *schema.versions.end();
    let mut touched = crate::snapshot_state::begin(database, schema, &expected.audience)?;
    crate::ownership::clear_audience(database, &expected.audience)?;
    let mut authenticated = BTreeMap::new();
    let metadata = SnapshotMetadata::new(database);
    let mut loss_state = None;
    let mut complete = BTreeMap::new();
    let mut carried = BTreeSet::new();
    prepared.each(&metadata, |record| {
        match record {
            SnapshotRecord::Synced(row) => crate::snapshot_state::synced(
                database,
                schema,
                row,
                header.schema_version < local_version,
            )?,
            SnapshotRecord::Write(write) => metadata.put(&write)?,
            SnapshotRecord::Column(column) => {
                // A synced column is registered; any other column can be
                // named only by frozen losses, which keep dropped columns.
                let synced = schema
                    .declarations
                    .iter()
                    .any(|declaration| declaration.name == column.table)
                    && schema
                        .table(&column.table)
                        .columns
                        .iter()
                        .any(|c| c.name == column.column);
                if !synced {
                    crate::write_commit::frozen_column(database, &column.table, &column.column)?;
                }
                database.internal_execute(
                            "INSERT INTO temp._coven_snapshot_columns(table_name,column_name) VALUES(?1,?2)",
                            params![column.table, column.column],
                        )?;
            }
            SnapshotRecord::Merge(row) => {
                touched.insert(crate::snapshot_state::merged(database, schema, row)?);
            }
            SnapshotRecord::Loss(loss) => {
                crate::snapshot_state::loss(database, schema, loss, &metadata, &mut loss_state)?
            }
            SnapshotRecord::Creation(creation) => {
                schema.row_table(&creation.row)?;
                crate::ownership::put(
                    database,
                    &creation.row,
                    &crate::ownership::Creation {
                        write: creation.write,
                        creator: creation.creator,
                    },
                )?;
            }
            SnapshotRecord::Evidence(evidence) => {
                crate::authorship::load(
                    database,
                    &expected.audience,
                    &evidence.original,
                    &evidence.verified,
                )?;
                authenticated.insert(evidence.original.write, evidence.verified);
            }
            SnapshotRecord::Admin(original) => {
                crate::sequence_apply::retain(database, "admin", &original)?
            }
            SnapshotRecord::KeyCopy(original) => {
                crate::sequence_apply::retain(database, "key", &original)?
            }
            SnapshotRecord::Announcement(original) => {
                crate::sequence_apply::retain(database, "announcement", &original)?
            }
            SnapshotRecord::Claim(claim) => {
                crate::sequence::note_record(database, &claim, header.covers)?;
            }
            SnapshotRecord::Authored(authored) => {
                crate::write_sources::load_evidence(database, &authored)?;
                carried.insert((
                    authored.header.position,
                    authored.change.row().table.clone(),
                    authored.change.row().key.clone(),
                ));
                if authored.complete {
                    complete.insert(
                        (
                            authored.header.position,
                            authored.change.row().audience.clone(),
                        ),
                        authored.header,
                    );
                }
            }
        }
        Ok(())
    })?;
    for ((_, audience), header) in complete {
        crate::write_sources::complete(database, &header, &audience)?;
    }
    crate::write_sources::forget_own_migration(
        database,
        &expected.audience,
        header.covers,
        header.schema_version,
        &carried,
    )?;
    crate::snapshot_state::finish(database)?;
    crate::authorship::check(
        database,
        schema,
        &expected.audience,
        crate::authorship::Author {
            device: expected.device,
            member: author,
        },
        &authenticated,
    )?;
    database.batch(
        "DELETE FROM temp._coven_snapshot_writes; DELETE FROM temp._coven_snapshot_columns",
    )?;
    Ok((header, touched))
}

#[cfg(test)]
#[path = "snapshot_load_tests.rs"]
pub(crate) mod tests;
