//! Install streamed plaintext with row replacement and local replay in one commit.

use crate::merge_store::MergeStore;
use crate::removal_view::DatabaseRemovalView;
use crate::snapshot_error::{invalid, SnapshotError};
use crate::snapshot_metadata::SnapshotMetadata;
use crate::sqlite::DatabaseConnection;
use crate::write_rows::AppView;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::snapshot::{SnapshotHeader, SnapshotRecord};
use coven_format::snapshot_stream::SnapshotChunkDecoder;
use coven_format::store_log::SnapshotId;
use coven_merge::RowId;
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

pub(crate) fn load<P: crate::PendingValue, R: Read, W: Read>(
    database: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    reload: crate::SnapshotReload<R, W>,
    files: &crate::file_write::FileWrite<'_>,
    device: coven_foundation::id_source::DeviceId,
) -> Result<(), DbError> {
    database.transaction(|database| {
        let crate::SnapshotReload {
            snapshots,
            writes,
            absent,
            boundaries,
            expected_entries,
            operation,
        } = reload;
        if let Some(expected) = expected_entries {
            let current = database.query(
                "SELECT device,number FROM _coven_store_log ORDER BY device,number",
                [],
                |row| crate::store_log_tables::entry_id(row, 0),
            )?;
            if current != expected {
                return Err(DbError::StoreLogEntriesChanged);
            }
        }
        if snapshots.is_empty() {
            return Err(invalid("reload has no snapshots"));
        }
        let mut selected = BTreeSet::new();
        for source in &snapshots {
            let audience = match source {
                crate::SnapshotSource::Stored { id, .. } => &id.audience,
                crate::SnapshotSource::Empty(audience) => audience,
            };
            if !selected.insert(audience.clone()) {
                return Err(invalid("reload repeats an audience"));
            }
            // The rows being replaced, under the boundaries they were applied with.
            crate::entry_inputs::original(database, schema, audience)?;
        }
        let applied = crate::download::positions(database)?;
        let mut causes = BTreeSet::new();
        for audience in &selected {
            causes.extend(crate::entry_inputs::applied(database, audience)?);
        }
        if let Some(boundaries) = boundaries {
            let resets = crate::BoundaryCoverage::replace(database, boundaries)?;
            crate::pending::suppress(database, &resets)?;
            crate::pending::refresh(database)?;
        }
        let mut coverage = crate::snapshot_coverage::SnapshotCoverage::new(database, device)?;
        let visible = AppView::after(before, schema);
        let old_store = MergeStore::from_schema(before, &schema.schema);
        let deleted = crate::store_log_tables::deleted_circles(database)?;
        let empty = BTreeMap::new();
        let old =
            DatabaseRemovalView::new(before, &old_store, schema, &visible, &empty, &deleted, None)?;
        crate::snapshot_state::create_tables(database)?;
        let mut touched = BTreeSet::new();
        for source in snapshots {
            match source {
                crate::SnapshotSource::Stored { id, prefix, input } => {
                    let (header, rows) = read(database, schema, &id, prefix, input)?;
                    // Writes covered here but never applied individually are not
                    // retained, so earlier inputs cannot reproduce this audience.
                    if !header.writes.0.iter().all(|write| applied.covers(*write)) {
                        crate::entry_inputs::forget(database, &id.audience, None)?;
                    }
                    let store = MergeStore::from_snapshot(database, &schema.schema, &selected);
                    let retained = crate::entry_inputs::retain(
                        database,
                        crate::entry_inputs::Origin::Snapshot(&id),
                        header.schema_version,
                        &header.writes,
                        &store,
                        schema,
                    )?;
                    crate::entry_inputs::selected(database, &id.audience, retained)?;
                    coverage.loaded(id.audience, header.writes);
                    touched.extend(rows);
                }
                crate::SnapshotSource::Empty(audience) => {
                    touched.extend(crate::snapshot_state::begin(database, schema, &audience)?);
                    let store = MergeStore::from_snapshot(database, &schema.schema, &selected);
                    let retained = crate::entry_inputs::retain(
                        database,
                        crate::entry_inputs::Origin::Empty(&audience),
                        database.schema_version()?,
                        &coven_format::value::WritePositions(Vec::new()),
                        &store,
                        schema,
                    )?;
                    crate::entry_inputs::selected(database, &audience, retained)?;
                    coverage.loaded(audience, coven_format::value::WritePositions(Vec::new()));
                }
            }
        }
        // Rows the loaded data replaces depend on the boundaries applied before
        // and on those selecting it now; their local children are kept (§8.4).
        causes.extend(
            crate::Boundary::load_selected(database)?
                .iter()
                .filter(|boundary| selected.contains(&boundary.snapshot().audience))
                .map(crate::Boundary::entry),
        );
        let current = MergeStore::from_snapshot(database, &schema.schema, &selected);
        let mut affected = crate::write_apply::WriteApply::new(
            database, schema, &current, &visible, &visible, &deleted,
        )
        .caused_by(causes)
        .replace(&old, touched)?;
        crate::snapshot_state::drop_tables(database)?;
        affected.extend(crate::snapshot_writes::apply::<P, _>(
            database, schema, writes, absent, coverage, &deleted,
        )?);
        // Waits for writes these positions now cover end with this commit.
        crate::pending::clear_covered_writes(database)?;
        // Retention sees the final state, including every replayed waiting write.
        files.retain_rows(affected)?;
        files.before_commit()?;
        for audience in &selected {
            crate::entry_effects::clear_reload(database, audience)?;
        }
        crate::entry_inputs::release(database)?;
        if let Some(operation) = operation {
            crate::operation::advance(database, &operation)?;
        }
        Ok(())
    })
}

pub(crate) fn read(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    expected: &SnapshotId,
    prefix: coven_format::sealed_snapshot::SnapshotObjectPrefix,
    mut input: impl Read,
) -> Result<(SnapshotHeader, BTreeSet<RowId>), DbError> {
    let local_version = *schema.versions.end();
    let mut touched = crate::snapshot_state::begin(database, schema, &expected.audience)?;
    let metadata = SnapshotMetadata::new(database);
    let mut decoder = SnapshotChunkDecoder::new(prefix);
    let mut checked_header = false;
    let mut loss_state = None;
    let mut complete = BTreeSet::new();
    let mut carried = BTreeSet::new();
    let mut chunk = [0; coven_format::chunks::CHUNK_SIZE];
    loop {
        let length = match input.read(&mut chunk) {
            Ok(length) => length,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(SnapshotError::Read(error).into()),
        };
        if length == 0 {
            break;
        }
        let mut bytes = &chunk[..length];
        loop {
            let record = decoder.next_record(&mut bytes, &metadata);
            metadata.check()?;
            let record = record.map_err(SnapshotError::Format)?;
            if !checked_header {
                if let Some(header) = decoder.header() {
                    check_header(database, schema, expected, header)?;
                    checked_header = true;
                }
            }
            let Some(record) = record else { break };
            match record {
                SnapshotRecord::Synced(row) => crate::snapshot_state::synced(
                    database,
                    schema,
                    row,
                    decoder
                        .header()
                        .expect("record follows header")
                        .schema_version
                        < local_version,
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
                        crate::write_commit::frozen_column(
                            database,
                            &column.table,
                            &column.column,
                        )?;
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
                SnapshotRecord::Authored(authored) => {
                    crate::write_landing::agrees(database, &authored)?;
                    crate::write_sources::load_evidence(database, &authored)?;
                    carried.insert((
                        authored.header.position,
                        authored.change.row.table.clone(),
                        authored.change.row.key.clone(),
                    ));
                    if authored.complete {
                        complete.insert((
                            coven_format::merge_fields::encode_write_header(&authored.header)
                                .map_err(DbError::from)?,
                            authored.change.row.audience.clone(),
                        ));
                    }
                }
            }
        }
    }
    decoder.finish().map_err(SnapshotError::Format)?;
    for (header, audience) in complete {
        let header = coven_format::merge_fields::decode_write_header(&header)?;
        crate::write_sources::complete(database, &header, &audience)?;
    }
    let header = decoder
        .header()
        .ok_or_else(|| invalid("snapshot has no header"))?;
    check_header(database, schema, expected, header)?;
    metadata.validate(&header.writes)?;
    crate::write_sources::forget_own_migration(
        database,
        &expected.audience,
        &header.writes,
        &carried,
    )?;
    crate::snapshot_state::finish(database)?;
    database.batch(
        "DELETE FROM temp._coven_snapshot_writes; DELETE FROM temp._coven_snapshot_columns",
    )?;
    Ok((header.clone(), touched))
}

fn check_header(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    expected: &SnapshotId,
    header: &SnapshotHeader,
) -> Result<(), DbError> {
    if header.id != *expected {
        return Err(invalid(
            "snapshot identity differs from the requested object",
        ));
    }
    if !schema.versions.contains(&header.schema_version) {
        return Err(SnapshotError::Schema {
            snapshot: header.schema_version,
            database: *schema.versions.end(),
        }
        .into());
    }
    let mut missing = Vec::new();
    for entry in &header.store_log.0 {
        let number: Option<Vec<u8>> = database.query_row(
            "SELECT max(number) FROM _coven_store_log WHERE device=?1",
            [entry.device.0.to_be_bytes().as_slice()],
            |r| r.get(0),
        )?;
        if number.map_or(0, crate::write_encoding::counter) < entry.number {
            missing.push(*entry);
        }
    }
    if !missing.is_empty() {
        return Err(SnapshotError::StoreLog { missing }.into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_load_tests.rs"]
pub(crate) mod tests;
