//! Install streamed plaintext with row replacement and local replay in one commit.

use crate::merge_store::MergeStore;
use crate::snapshot_error::{invalid, SnapshotError};
use crate::snapshot_metadata::SnapshotMetadata;
use crate::sqlite::DatabaseConnection;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::admin::SnapshotId;
use coven_format::snapshot::{SnapshotHeader, SnapshotRecord};
use coven_format::snapshot_stream::SnapshotChunkDecoder;
use coven_merge::RowId;
use rusqlite::params;
use std::collections::BTreeSet;
use std::io::Read;

/// Who signed a snapshot being loaded, and how to authenticate the other
/// members' writes it carries (§8.8, §15).
#[derive(Clone, Copy)]
pub(crate) struct SnapshotSigner<'a> {
    pub(crate) authorship: &'a dyn crate::Authorship,
    /// The snapshot author's member, unknown only before this device's own.
    pub(crate) author: Option<coven_foundation::id_source::MemberId>,
}

pub(crate) fn load<P: crate::PendingValue, R: Read, W: Read>(
    database: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    reload: crate::SnapshotReload<R, W>,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<Vec<crate::LoadedSnapshot>, DbError> {
    database.stream_transaction(|database| {
        let crate::SnapshotReload {
            snapshots,
            slots,
            target,
            state,
            cutoffs,
            boundaries,
            operation,
            authorship,
        } = reload;
        if target < crate::sequence::applied(database)? {
            return Err(invalid("reload target precedes committed applied slot"));
        }
        crate::store_state_tables::replace(database, &state)?;
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
                crate::SnapshotSource::Stored {
                    id,
                    prefix,
                    input,
                    author,
                } => {
                    if !selected.insert(id.audience.clone()) {
                        return Err(invalid("reload repeats an audience"));
                    }
                    let (header, rows) = read(
                        database,
                        schema,
                        &id,
                        prefix,
                        input,
                        &SnapshotSigner {
                            authorship: authorship.as_ref(),
                            author: Some(author),
                        },
                    )
                    .map_err(|error| SnapshotError::Body {
                        snapshot: id.clone(),
                        error: Box::new(error),
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
        crate::snapshot_writes::apply::<P, _>(database, schema, slots, &coverage)?;
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
    expected: &SnapshotId,
    prefix: coven_format::sealed_snapshot::SnapshotObjectPrefix,
    mut input: impl Read,
    signer: &SnapshotSigner<'_>,
) -> Result<(SnapshotHeader, BTreeSet<RowId>), DbError> {
    #[cfg(any(test, feature = "test-utils"))]
    database.record_work(|work| work.snapshot_reads += 1);
    let SnapshotSigner { authorship, author } = *signer;
    let local_version = *schema.versions.end();
    let mut touched = crate::snapshot_state::begin(database, schema, &expected.audience)?;
    crate::ownership::clear_audience(database, &expected.audience)?;
    crate::authorship::begin_load(database)?;
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
                    check_header(schema, expected, header)?;
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
                    crate::authorship::load(database, authorship, &expected.audience, &evidence)?
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
                    crate::sequence::note_record(
                        database,
                        &claim,
                        decoder.header().expect("record follows header").covers,
                    )?;
                }
                SnapshotRecord::Authored(authored) => {
                    crate::write_sources::load_evidence(database, &authored)?;
                    carried.insert((
                        authored.header.position,
                        authored.change.row().table.clone(),
                        authored.change.row().key.clone(),
                    ));
                    if authored.complete {
                        complete.insert((
                            coven_format::merge_fields::encode_write_header(&authored.header)
                                .map_err(DbError::from)?,
                            authored.change.row().audience.clone(),
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
    check_header(schema, expected, header)?;
    metadata.validate(header.covers)?;
    crate::write_sources::forget_own_migration(
        database,
        &expected.audience,
        header.covers,
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
    )?;
    crate::authorship::end_load(database)?;
    database.batch(
        "DELETE FROM temp._coven_snapshot_writes; DELETE FROM temp._coven_snapshot_columns",
    )?;
    Ok((header.clone(), touched))
}

fn check_header(
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
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_load_tests.rs"]
pub(crate) mod tests;
