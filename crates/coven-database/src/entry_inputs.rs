//! Retained merge inputs used when an entry-dependent projection changes (§9).
//!
//! An audience's data under a set of boundary entries is one retained input
//! plus every write part kept after that input's positions. The *origin* is the
//! materialized state captured before a non-final boundary first replaced it,
//! labelled with the applied boundary entries it reflects (its basis). A loaded
//! boundary snapshot is the input of the boundary that selected it. Inputs are
//! released once every boundary entry of their audience is final.
use crate::{
    sqlite::DatabaseConnection,
    write_encoding::{audience_text, decoded, encoded},
    write_schema::WriteSchema,
    Boundary, DbError, EntryId,
};
use coven_format::{
    merge_fields,
    snapshot_rows::{MergeRow, SyncedRow},
    store_log::SnapshotId,
    value::WritePositions,
};
use coven_merge::{Audience, RowId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) enum Origin<'a> {
    Local(&'a Audience),
    Snapshot(&'a SnapshotId),
    Empty(&'a Audience),
}
impl Origin<'_> {
    fn audience(&self) -> &Audience {
        match self {
            Self::Local(a) | Self::Empty(a) => a,
            Self::Snapshot(id) => &id.audience,
        }
    }
}

fn id_bytes(entry: EntryId) -> [Vec<u8>; 2] {
    [
        entry.device.0.to_be_bytes().to_vec(),
        entry.number.to_be_bytes().to_vec(),
    ]
}

/// Boundary entries as sorted 16-byte identities: device then number.
fn basis(entries: impl IntoIterator<Item = EntryId>) -> Vec<u8> {
    let sorted: BTreeSet<_> = entries
        .into_iter()
        .map(|e| (e.device.0, e.number))
        .collect();
    sorted
        .into_iter()
        .flat_map(|(device, number)| device.to_be_bytes().into_iter().chain(number.to_be_bytes()))
        .collect()
}

/// Boundary entries whose data this audience's materialized rows reflect.
pub(crate) fn applied(
    db: &DatabaseConnection,
    audience: &Audience,
) -> Result<BTreeSet<EntryId>, DbError> {
    Ok(db
        .query(
            "SELECT entry_device,entry_number FROM _coven_boundaries WHERE audience=?1 AND applied=1",
            [audience_text(audience)],
            |r| crate::store_log_tables::entry_id(r, 0),
        )?
        .into_iter()
        .collect())
}

/// Whether this device reads the audience's parts: the store, or a circle its
/// member belongs to. A deleted circle's member still holds its hidden rows.
pub(crate) fn reads(db: &DatabaseConnection, audience: &Audience) -> Result<bool, DbError> {
    let Audience::Circle(circle) = audience else {
        return Ok(true);
    };
    let device = crate::pending::own_device(db)?;
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_devices d JOIN _coven_circle_members m ON m.member=d.member
         WHERE d.device=?1 AND d.state<>'removed' AND m.circle=?2)",
        params![device.0.to_be_bytes().as_slice(), circle.to_string()],
        |r| r.get(0),
    )
}

/// Capture the materialized audience before a boundary change can replace it.
/// Without a complete materialization (a pending reload, or parts this device
/// does not read) there is nothing to restore later; a reversal then reloads.
pub(crate) fn original(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    audience: &Audience,
) -> Result<(), DbError> {
    let exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_data_origins WHERE audience=?1)",
        [audience_text(audience)],
        |r| r.get(0),
    )?;
    if exists || crate::entry_effects::reloading(db, audience)? || !reads(db, audience)? {
        return Ok(());
    }
    let store = crate::merge_store::MergeStore::from_schema(db, &schema.schema);
    let input = retain(
        db,
        Origin::Local(audience),
        db.schema_version()?,
        &crate::download::positions(db)?,
        &store,
        schema,
    )?;
    db.internal_execute(
        "INSERT INTO _coven_data_origins(audience,input,basis) VALUES(?1,?2,?3)",
        params![
            audience_text(audience),
            input,
            basis(applied(db, audience)?)
        ],
    )?;
    Ok(())
}

/// Store the audience's current rows and losses as an input. A snapshot or
/// empty input already retained under the same identity is replaced.
pub(crate) fn retain(
    db: &DatabaseConnection,
    origin: Origin<'_>,
    version: u32,
    positions: &WritePositions,
    store: &crate::merge_store::MergeStore<'_>,
    schema: &WriteSchema,
) -> Result<i64, DbError> {
    let audience = audience_text(origin.audience());
    let positions = encoded(merge_fields::encode_write_positions(positions))?;
    let existing = match &origin {
        Origin::Local(_) => None,
        Origin::Snapshot(id) => db.query_row(
            "SELECT (SELECT id FROM _coven_data_inputs WHERE audience=?1 AND kind='snapshot' AND device=?2 AND number=?3)",
            params![audience, id.device.0.to_be_bytes().as_slice(), id.number.to_be_bytes().as_slice()],
            |r| r.get::<_, Option<i64>>(0),
        )?,
        Origin::Empty(_) => db.query_row(
            "SELECT (SELECT id FROM _coven_data_inputs WHERE audience=?1 AND kind='empty' AND device IS NULL AND number IS NULL AND version=?2)",
            params![audience, version],
            |r| r.get::<_, Option<i64>>(0),
        )?,
    };
    let id = match existing {
        Some(id) => {
            db.internal_execute("DELETE FROM _coven_input_file_refs WHERE input=?1", [id])?;
            db.internal_execute("DELETE FROM _coven_data_rows WHERE input=?1", [id])?;
            db.internal_execute("DELETE FROM _coven_data_losses WHERE input=?1", [id])?;
            db.internal_execute(
                "UPDATE _coven_data_inputs SET version=?2,positions=?3 WHERE id=?1",
                params![id, version, positions],
            )?;
            id
        }
        None => {
            let (kind, device, number) = match &origin {
                Origin::Local(_) => ("local", None, None),
                Origin::Empty(_) => ("empty", None, None),
                Origin::Snapshot(id) => (
                    "snapshot",
                    Some(id.device.0.to_be_bytes().to_vec()),
                    Some(id.number.to_be_bytes().to_vec()),
                ),
            };
            db.query_row(
                "INSERT INTO _coven_data_inputs(audience,kind,device,number,version,positions) VALUES(?1,?2,?3,?4,?5,?6) RETURNING id",
                params![audience, kind, device, number, version, positions],
                |r| r.get(0),
            )?
        }
    };
    let mut references = BTreeSet::new();
    db.for_each(
        "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE audience=?1 ORDER BY table_name,key",
        [&audience],
        |r| {
            let row = crate::row_queries::read_identity(r)?;
            let state = store.row_without_losses(&row)?.state;
            if let Some(location) = crate::file_protection::location(schema, &row.table) {
                if let Some(cell) = state.cells().get(location) {
                    crate::file_protection::value(&cell.value.value, &mut references)?;
                }
            }
            db.internal_execute(
                "INSERT INTO _coven_data_rows(input,table_name,key,record) VALUES(?1,?2,?3,?4)",
                params![id, row.table, row.key, encoded(merge_fields::encode_merge_row(&MergeRow { state }))?],
            )?;
            Ok::<_, DbError>(())
        },
    )?;
    let mut ordinal = 0i64;
    crate::loss_record::visit(db, origin.audience(), |loss| {
        crate::file_protection::loss(schema, &loss, &mut references)?;
        db.internal_execute(
            "INSERT INTO _coven_data_losses(input,ordinal,record) VALUES(?1,?2,?3)",
            params![id, ordinal, encoded(merge_fields::encode_loss(&loss))?],
        )?;
        ordinal += 1;
        Ok::<_, DbError>(())
    })?;
    crate::file_protection::retain_input(db, id, &references)?;
    Ok(id)
}

/// Associate freshly loaded data with the boundary selecting this audience, or
/// make it the audience's origin when no boundary selects it.
pub(crate) fn selected(
    db: &DatabaseConnection,
    audience: &Audience,
    input: i64,
) -> Result<(), DbError> {
    let last = Boundary::load_selected(db)?
        .into_iter()
        .rfind(|b| b.snapshot().audience == *audience);
    match last {
        Some(boundary) => {
            let [device, number] = id_bytes(boundary.entry());
            db.internal_execute(
                "UPDATE _coven_boundaries SET input=?3 WHERE entry_device=?1 AND entry_number=?2",
                params![device, number, input],
            )?;
        }
        None => {
            db.internal_execute(
                "INSERT INTO _coven_data_origins(audience,input,basis) VALUES(?1,?2,x'')
                 ON CONFLICT(audience) DO UPDATE SET input=excluded.input,basis=excluded.basis",
                params![audience_text(audience), input],
            )?;
        }
    }
    collect(db, audience)
}

/// The retained input for a selection: the origin when it reflects exactly
/// these boundaries, otherwise the input loaded for the last selected one.
pub(crate) fn selected_input(
    db: &DatabaseConnection,
    audience: &Audience,
    boundaries: &[Boundary],
) -> Result<Option<i64>, DbError> {
    let wanted = basis(boundaries.iter().map(Boundary::entry));
    let origin: Option<(i64, Vec<u8>)> = db
        .query(
            "SELECT input,basis FROM _coven_data_origins WHERE audience=?1",
            [audience_text(audience)],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?
        .into_iter()
        .next();
    if let Some((input, basis)) = origin {
        if basis == wanted {
            return Ok(Some(input));
        }
    }
    let Some(last) = boundaries.last() else {
        return Ok(None);
    };
    let [device, number] = id_bytes(last.entry());
    db.query_row(
        "SELECT input FROM _coven_boundaries WHERE entry_device=?1 AND entry_number=?2",
        params![device, number],
        |r| r.get(0),
    )
}

/// Forget every retained input and write part of this audience except `keep`.
/// Used when data arrived that the remaining inputs cannot reproduce, and when
/// no replay can need them any more.
pub(crate) fn forget(
    db: &DatabaseConnection,
    audience: &Audience,
    keep: Option<i64>,
) -> Result<(), DbError> {
    let text = audience_text(audience);
    db.internal_execute(
        "DELETE FROM _coven_data_origins WHERE audience=?1 AND input IS NOT ?2",
        params![text, keep],
    )?;
    db.internal_execute(
        "UPDATE _coven_boundaries SET input=NULL WHERE audience=?1 AND input IS NOT NULL AND input IS NOT ?2",
        params![text, keep],
    )?;
    collect(db, audience)?;
    if keep.is_none() {
        crate::retained_writes::release(db, audience)?;
    }
    Ok(())
}

/// Delete inputs that neither the origin nor a boundary names.
fn collect(db: &DatabaseConnection, audience: &Audience) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM _coven_data_inputs WHERE audience=?1
         AND NOT EXISTS(SELECT 1 FROM _coven_data_origins o WHERE o.input=_coven_data_inputs.id)
         AND NOT EXISTS(SELECT 1 FROM _coven_boundaries b WHERE b.input=_coven_data_inputs.id)",
        [audience_text(audience)],
    )?;
    Ok(())
}

/// Entries stored strictly before the persisted horizon (§9); no horizon means none.
pub(crate) const FINAL: &str =
    "(l.stored_at < coalesce((SELECT horizon FROM _coven_finality WHERE singleton=1),x''))";

/// Release every audience whose boundary entries are all final (§9): no later
/// arrival can change its selection, so its current rows are its only input.
/// A later boundary captures a fresh origin. A final, kept and applied reset's
/// suppressed observations are cleared for good; other final resets stop
/// suppressing. A final reset still awaiting its reload keeps suppressing.
pub(crate) fn release(db: &DatabaseConnection) -> Result<(), DbError> {
    let audiences = db.query(
        "SELECT DISTINCT audience FROM _coven_data_inputs",
        [],
        |r| crate::write_encoding::audience(&r.get::<_, String>(0)?),
    )?;
    // A retired write's verdict that can still change keeps its audience's inputs.
    let windows = crate::write_landing::open_audiences(db)?;
    for audience in audiences.into_iter().filter(|a| !windows.contains(a)) {
        let open: bool = db.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM _coven_boundaries b JOIN _coven_store_log l ON l.device=b.entry_device AND l.number=b.entry_number
                 WHERE b.audience=?1 AND NOT {FINAL})"
            ),
            [audience_text(&audience)],
            |r| r.get(0),
        )?;
        if !open {
            forget(db, &audience, None)?;
        }
    }
    crate::local_effects::release(db)?;
    crate::write_landing::release(db)?;
    let resets = db.query(
        "SELECT DISTINCT entry_device,entry_number FROM _coven_pending_resets",
        [],
        |r| crate::store_log_tables::entry_id(r, 0),
    )?;
    for entry in resets {
        let [device, number] = id_bytes(entry);
        let (finished, cleared, awaiting): (bool, bool, bool) = db.query_row(
            &format!(
                "SELECT {FINAL}, l.outcome='kept' AND b.applied=1,
                        l.outcome='kept' AND b.applied=0 AND EXISTS(SELECT 1 FROM _coven_data_reloads r WHERE r.audience=b.audience)
                 FROM _coven_boundaries b JOIN _coven_store_log l ON l.device=b.entry_device AND l.number=b.entry_number
                 WHERE b.entry_device=?1 AND b.entry_number=?2"
            ),
            params![device, number],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if !finished || awaiting {
            continue;
        }
        db.pending_change(|| {
            if cleared {
                db.internal_execute(
                    "DELETE FROM _coven_pending WHERE suppressed=1 AND (subject,reporter) IN
                     (SELECT subject,reporter FROM _coven_pending_resets WHERE entry_device=?1 AND entry_number=?2)",
                    params![device, number],
                )?;
            }
            db.internal_execute(
                "DELETE FROM _coven_pending_resets WHERE entry_device=?1 AND entry_number=?2",
                params![device, number],
            )?;
            Ok(())
        })?;
    }
    Ok(())
}

/// Rebuild selected audiences against a pinned before view. Metadata and
/// consumed positions remain available while original changes are rejudged.
pub(crate) fn restore(
    db: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    inputs: &BTreeMap<Audience, i64>,
    deleted: &BTreeMap<coven_foundation::id_source::CircleId, EntryId>,
    causes: BTreeSet<EntryId>,
) -> Result<BTreeSet<crate::write_rows::AppKey>, DbError> {
    if inputs.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut coverage = BTreeMap::new();
    let selected: BTreeSet<_> = inputs.keys().cloned().collect();
    let visible = crate::write_rows::AppView::after(before, schema);
    let old_store = crate::merge_store::MergeStore::from_schema(before, &schema.schema);
    let empty = BTreeMap::new();
    let old = crate::removal_view::DatabaseRemovalView::new(
        before, &old_store, schema, &visible, &empty, deleted, None,
    )?;
    crate::snapshot_state::create_tables(db)?;
    db.batch("INSERT INTO temp._coven_snapshot_writes SELECT substr(timestamp,9,8),number,timestamp,had_read FROM _coven_writes")?;
    let mut touched = BTreeSet::<RowId>::new();
    for (audience, input) in inputs {
        let (version, positions, stored_audience) = db.query_row(
            "SELECT version,positions,audience FROM _coven_data_inputs WHERE id=?1",
            [input],
            |r| {
                Ok((
                    r.get::<_, u32>(0)?,
                    decoded(merge_fields::decode_write_positions(
                        &r.get::<_, Vec<u8>>(1)?,
                    ))?,
                    r.get::<_, String>(2)?,
                ))
            },
        )?;
        if stored_audience != audience_text(audience) {
            return Err(DbError::DamagedDatabase);
        }
        if !schema.versions.contains(&version) {
            return Err(crate::SnapshotError::Schema {
                snapshot: version,
                database: *schema.versions.end(),
            }
            .into());
        }
        for position in &positions.0 {
            db.internal_execute("INSERT INTO _coven_positions(device,number) VALUES(?1,?2) ON CONFLICT(device) DO UPDATE SET number=max(number,excluded.number)", (position.device.0.to_be_bytes().as_slice(), position.number.to_be_bytes().as_slice()))?;
        }
        coverage.insert(audience.clone(), positions);
        touched.extend(crate::snapshot_state::begin(db, schema, audience)?);
        let metadata = crate::snapshot_metadata::SnapshotMetadata::new(db);
        db.for_each("SELECT record FROM _coven_data_rows WHERE input=?1 ORDER BY table_name,key", [input], |r| {
            let merged = merge_fields::decode_merge_row(&r.get::<_, Vec<u8>>(0)?, &metadata);
            metadata.check()?;
            let merged = merged?;
            for name in merged.state.cells().keys() {
                db.internal_execute("INSERT INTO temp._coven_snapshot_columns(table_name,column_name) VALUES(?1,?2) ON CONFLICT DO NOTHING", params![merged.state.row().table, name])?;
            }
            // App values come from the retained merge cells; a column no write
            // set takes its SQLite default, as when a snapshot crosses additions.
            if merged.state.present() {
                crate::snapshot_state::synced(db, schema, SyncedRow { row: merged.state.row().clone(), columns: merged.state.cells().iter().map(|(name, cell)| (name.clone(), cell.value.clone())).collect() }, true)?;
            }
            touched.insert(crate::snapshot_state::merged(db, schema, merged)?);
            Ok::<_, DbError>(())
        })?;
        db.for_each(
            "SELECT record FROM _coven_data_losses WHERE input=?1 ORDER BY ordinal",
            [input],
            |r| {
                let loss = merge_fields::decode_loss(&r.get::<_, Vec<u8>>(0)?)?;
                crate::loss_record::put(db, &loss, None)?;
                crate::file_protection::retain_loss(db, schema, &loss)
            },
        )?;
    }
    let current = crate::merge_store::MergeStore::from_snapshot(db, &schema.schema, &selected);
    let mut affected =
        crate::write_apply::WriteApply::new(db, schema, &current, &visible, &visible, deleted)
            .caused_by(causes)
            .replace(&old, touched)?;
    crate::snapshot_state::drop_tables(db)?;
    // Each retained write applies once, with every rebuilt part its input
    // has not taken in, so a foreign-key action and its cause in different
    // audiences are judged together.
    crate::retained_writes::visit(db, &selected, |mut write| {
        let position = write.header.position;
        write.parts.retain(|part| match part {
            crate::DownloadedPart::Opened(part) => !coverage[&part.audience].covers(position),
            crate::DownloadedPart::Skipped(_) => false,
        });
        if !write.parts.is_empty() {
            affected.extend(crate::download::apply_opened(db, schema, write, deleted)?);
        }
        Ok(())
    })?;
    Ok(affected)
}

#[cfg(test)]
#[path = "entry_inputs_tests.rs"]
pub(crate) mod tests;
