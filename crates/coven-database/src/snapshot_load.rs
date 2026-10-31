//! Install prepared records with row replacement and local replay in one commit.

use crate::snapshot_error::{invalid, SnapshotError};
use crate::snapshot_layout::SnapshotLayout;
use crate::snapshot_metadata::SnapshotMetadata;
use crate::snapshot_preparation::PreparedSnapshot;
use crate::sqlite::DatabaseConnection;
use crate::write_schema::WriteSchema;
use crate::{DbError, LoadStep};
use coven_format::snapshot::{SnapshotHeader, SnapshotRecord};
use coven_foundation::progress::ProgressReport;
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn load<P: crate::PendingValue>(
    database: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    reload: crate::SnapshotReload<PreparedSnapshot<'_>, crate::download_stream::PreparedWrite<'_>>,
    files: &crate::file_write::FileWrite<'_>,
    progress: &mut ProgressReport<LoadStep>,
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
        crate::snapshot_install::create_tables(database, schema)?;
        let mut loaded = Vec::new();
        let mut selected = BTreeSet::new();
        let stored = snapshots
            .iter()
            .filter(|source| matches!(source, crate::SnapshotSource::Stored { .. }))
            .count();
        begin_installing(schema, stored, progress);
        for source in snapshots {
            match source {
                crate::SnapshotSource::Current(audience) => {
                    coverage.loaded(audience, current)?;
                }
                crate::SnapshotSource::Stored { id, input, author } => {
                    if !selected.insert(id.audience.clone()) {
                        return Err(invalid("reload repeats an audience"));
                    }
                    let header =
                        read(database, schema, input, Some(author), progress).map_err(|error| {
                            SnapshotError::Body { snapshot: id.clone(), error: Box::new(error) }
                        })?;
                    crate::download_stage::replace(database, &id.audience)?;
                    let files = crate::database::file_retention::snapshot_references(
                        database,
                        schema,
                        &id.audience,
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
                    loaded.push(crate::LoadedSnapshot { id, files });
                }
                crate::SnapshotSource::Empty(audience) => {
                    if !selected.insert(audience.clone()) {
                        return Err(invalid("reload repeats an audience"));
                    }
                    crate::snapshot_state::begin(database, &audience)?;
                    crate::ownership::clear_audience(database, &audience)?;
                    crate::download_stage::replace(database, &audience)?;
                    coverage.loaded(audience, 0)?;
                }
            }
        }
        // A published row the reload replaces is read from its snapshot now.
        for audience in &selected {
            database.internal_execute(
                "DELETE FROM temp._coven_download_rows WHERE audience=?1",
                [crate::write_encoding::audience_text(audience)],
            )?;
        }
        crate::snapshot_state::drop_tables(database)?;
        crate::snapshot_writes::apply::<P>(database, schema, slots, &coverage, progress)?;
        for audience in &selected {
            crate::reload_gate::clear_reload(database, audience)?;
        }
        // The removal rules run only where they can reach; every other
        // installed row shows its merged values (§8.4).
        let deleted = crate::store_state_tables::deleted_circles(database)?;
        touched.extend(crate::snapshot_conflicts::rows(database, schema, &deleted)?);
        crate::queued_writes::overlay(database, schema, &touched)?;
        let region = crate::download_stage::rules(database, before, schema, &deleted, touched)?;
        crate::snapshot_materialize::materialize(database, schema, &region, files)?;
        files.retain_rows(region.into_iter().map(|row| (row.table, row.key)).collect())?;
        crate::download_stage::finish(database)?;
        crate::snapshot_install::drop_tables(database, schema)?;
        crate::pending::clear_covered_slots(database)?;
        // A reload skips the slots it replaces, and with them any change that
        // named a candidate's path and left it unnamed again, so every
        // candidate counts from the reload's target (§17.10).
        database.internal_execute(
            "UPDATE _coven_file_candidates SET since=?1",
            [target.to_be_bytes()],
        )?;
        files.before_commit()?;
        if let Some(operation) = operation {
            crate::operation::advance(database, &operation)?;
        }
        Ok(loaded)
    })
}

/// Begin the `Installing` step for `stored` snapshots, each counting every
/// table once in each of installing's passes, when there are any (§13.4).
pub(crate) fn begin_installing(
    schema: &WriteSchema,
    stored: usize,
    progress: &mut ProgressReport<LoadStep>,
) {
    let tables = SnapshotLayout::new(schema).tables.len() as u64;
    let passes = stored as u64 * tables * crate::snapshot_install::TABLE_PASSES;
    if passes > 0 {
        progress.begin(LoadStep::Installing, passes);
    }
}

/// Install one staged snapshot's rows, counting each table once in each
/// pass toward the `Installing` step `progress` is on.
pub(crate) fn read(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    mut prepared: PreparedSnapshot<'_>,
    author: Option<coven_foundation::id_source::MemberId>,
    progress: &mut ProgressReport<LoadStep>,
) -> Result<SnapshotHeader, DbError> {
    let header = prepared.header.clone();
    let expected = &header.id;
    let audience = &expected.audience;
    prepared.refuse(0)?;
    let layout = SnapshotLayout::new(schema);
    crate::snapshot_state::begin(database, audience)?;
    crate::ownership::clear_audience(database, audience)?;
    let metadata = SnapshotMetadata::new(database);
    prepared.each(
        "SELECT device,number,timestamp,read_through,slot FROM temp._coven_snapshot_writes WHERE source=?1 ORDER BY device,number",
        [prepared.staging],
        |r| metadata.put(&crate::snapshot_metadata::applied(r)?),
    )?;
    prepared.each(
        &format!(
            "SELECT table_name,column_name FROM {} ORDER BY table_name,column_name",
            prepared.table("columns")
        ),
        [],
        |r| {
            let (table, column): (String, String) = (r.get(0)?, r.get(1)?);
            // A synced column is registered; any other column can be
            // named only by frozen losses, which keep dropped columns.
            let synced = schema.declarations.iter().any(|declaration| declaration.name == table)
                && schema.table(&table).columns.iter().any(|c| c.name == column);
            if !synced {
                crate::write_commit::frozen_column(database, &table, &column)?;
            }
            database.internal_execute(
                "INSERT INTO temp._coven_snapshot_columns(table_name,column_name) VALUES(?1,?2)",
                params![table, column],
            )?;
            Ok(())
        },
    )?;
    crate::snapshot_install::copy_rows(database, &prepared, &layout, progress)?;
    crate::snapshot_install::check_synced(database, schema, &layout, audience, progress)?;
    prepared.refuse(3)?;
    crate::snapshot_install::compare_rows(&prepared, &layout, progress)?;
    crate::snapshot_install::check_merged(database, schema, &layout, audience, progress)?;
    crate::snapshot_install::persist_rows(database, &layout, audience, progress)?;
    let mut loss_state = None;
    prepared.frames(4..=4, &metadata, |record| match record {
        SnapshotRecord::Loss(loss) => {
            crate::snapshot_state::loss(database, schema, loss, &metadata, &mut loss_state)
        }
        _ => Err(invalid("staged loss section holds another record")),
    })?;
    prepared.refuse(5)?;
    crate::snapshot_install::sources(database, &prepared, audience)?;
    prepared.refuse(6)?;
    crate::snapshot_install::creations(database, &prepared)?;
    let mut authenticated = BTreeMap::new();
    prepared.frames(7..=11, &metadata, |record| {
        match record {
            SnapshotRecord::Evidence(evidence) => {
                crate::authorship::load(
                    database,
                    audience,
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
            _ => return Err(invalid("staged history section holds another record")),
        }
        Ok(())
    })?;
    crate::snapshot_install::complete_parts(database, audience)?;
    crate::write_sources::forget_own_migration(
        database,
        audience,
        header.covers,
        header.schema_version,
        |write, audience, table, key| {
            crate::snapshot_install::carried(database, write, audience, table, key)
        },
    )?;
    crate::snapshot_install::finish(database, &layout, audience, progress)?;
    crate::authorship::check(
        database,
        schema,
        audience,
        crate::authorship::Author { device: expected.device, member: author },
        &authenticated,
    )?;
    database.batch(
        "DELETE FROM temp._coven_snapshot_writes; DELETE FROM temp._coven_snapshot_columns",
    )?;
    Ok(header)
}

#[cfg(test)]
#[path = "snapshot_load_tests.rs"]
pub(crate) mod tests;
