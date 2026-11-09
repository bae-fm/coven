//! Forgetting the applied-write entries no check reads (§16.7).
//!
//! An entry's slot and timestamp are read only while something names the
//! write by them: a merge row's generation or cell, an active lost value, or
//! the counted state kept aside beside the queue. Every other name is an
//! identity, which a write's device's later landed writes cover, since a
//! device's writes land in number order. So an entry goes once nothing
//! names it that way and its slot is at or below the slot every write still
//! to be judged has read past, unless it is its device's last landed write,
//! or the latest stamped entry at or below that slot.
//!
//! An entry can go only when the last of those holds, so it is looked at
//! again then: when that slot passes its own, when something stops naming
//! it, when its device lands a later write, and when a later stamped entry
//! takes its place. No pass reads every entry.

use crate::write_encoding::{counter, decoded, encoded, slot_counter};
use crate::{sqlite::DatabaseConnection, DbError};
use coven_format::{merge_fields, value::Value};
use coven_merge::{RowId, RowState, WriteId};
use rusqlite::params;
use std::collections::BTreeSet;

/// Look again at `write`'s entry in the next applied slot.
fn look_again(db: &DatabaseConnection, ordinal: i64) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_unnamed_writes(write_id) VALUES(?1) ON CONFLICT DO NOTHING",
        [ordinal],
    )?;
    Ok(())
}

/// The writes a row's merge state names by their entries: the writes that
/// moved it to each generation, set each cell, and set or replaced each
/// active lost value.
fn names(state: &RowState<Value>) -> BTreeSet<WriteId> {
    state
        .generations()
        .values()
        .copied()
        .chain(state.cells().values().map(|cell| cell.write))
        .chain(state.lost().iter().flat_map(|(key, value)| [key.write, value.replaced_by]))
        .collect()
}

/// A landed write moved a row from `before` to `after`: the writes the row no
/// longer names may have lost their last name.
pub(crate) fn changed(
    db: &DatabaseConnection,
    before: &RowState<Value>,
    after: &RowState<Value>,
) -> Result<(), DbError> {
    let after = names(after);
    for write in names(before).difference(&after) {
        look_again(db, crate::snapshot_state::ordinal(db, *write)?)?;
    }
    Ok(())
}

/// The writes a lost value names by their entries: an active one's setters
/// and the write that replaced it, which merges compare. A frozen one names
/// its writes only as identities.
pub(crate) fn active_writes(loss: &coven_format::loss::Loss) -> Vec<WriteId> {
    use coven_format::loss::{LossCause, LossValues};
    if loss.frozen {
        return Vec::new();
    }
    let mut writes: Vec<WriteId> = match &loss.values {
        LossValues::Cell { cell, .. } => vec![cell.write],
        LossValues::Row(cells) => cells.values().map(|cell| cell.write).collect(),
    };
    if let LossCause::Write(write) = loss.cause {
        writes.push(write);
    }
    writes
}

/// A dismissal removed or changed a lost value: the writes it named may have
/// lost their last name.
pub(crate) fn dismissed(
    db: &DatabaseConnection,
    loss: &coven_format::loss::Loss,
) -> Result<(), DbError> {
    for write in active_writes(loss) {
        look_again(db, crate::snapshot_state::ordinal(db, write)?)?;
    }
    Ok(())
}

/// The counted state kept aside for `row`, or for every row, is put back or
/// let go: the writes it named may have lost their last name.
pub(crate) fn unnamed_by_published(
    db: &DatabaseConnection,
    row: Option<&RowId>,
) -> Result<(), DbError> {
    match row {
        Some(row) => db.internal_execute(
            "INSERT INTO _coven_unnamed_writes(write_id) SELECT write_id FROM _coven_published_writes
             WHERE table_name=?1 AND key=?2 AND audience=?3 ON CONFLICT DO NOTHING",
            params![row.table, row.key, &row.audience],
        )?,
        // SQLite reads an upsert after a bare SELECT's FROM as a join's ON.
        None => db.internal_execute(
            "INSERT INTO _coven_unnamed_writes(write_id) SELECT write_id FROM _coven_published_writes WHERE true
             ON CONFLICT DO NOTHING",
            [],
        )?,
    };
    Ok(())
}

/// `write` lands: the last write its device landed before is no longer its
/// last.
pub(crate) fn superseded(db: &DatabaseConnection, write: WriteId) -> Result<(), DbError> {
    if let Some(previous) = crate::write_metadata::landed_below(db, write)? {
        look_again(db, previous.ordinal)?;
    }
    Ok(())
}

/// The slot every write still to be judged has read past moved from `from`
/// to `through`: look again at the entries in the slots between, and find
/// the latest stamped entry at or below `through` from `newest`, the one at
/// or below `from`, and those. A replaced one is looked at again.
pub(crate) fn passed(
    db: &DatabaseConnection,
    from: u64,
    through: u64,
    newest: Option<i64>,
) -> Result<Option<i64>, DbError> {
    let (from, through) = (from.to_be_bytes(), through.to_be_bytes());
    let range = params![from.as_slice(), through.as_slice()];
    db.internal_execute(
        "INSERT INTO _coven_unnamed_writes(write_id) SELECT id FROM _coven_writes WHERE slot>?1 AND slot<=?2
         ON CONFLICT DO NOTHING",
        range,
    )?;
    let Some((latest, stamp)) = db
        .query(
            "SELECT id,timestamp FROM _coven_writes WHERE slot>?1 AND slot<=?2 ORDER BY timestamp DESC LIMIT 1",
            range,
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
        )?
        .pop()
    else {
        return Ok(newest);
    };
    let Some(newest) = newest else {
        return Ok(Some(latest));
    };
    let previous: Vec<u8> =
        db.query_row("SELECT timestamp FROM _coven_writes WHERE id=?1", [newest], |r| r.get(0))?;
    // Timestamps are encoded big-endian, so their bytes order as they do.
    if stamp > previous {
        look_again(db, newest)?;
        return Ok(Some(latest));
    }
    Ok(Some(newest))
}

/// Forget each entry looked at again that nothing reads any more: one at or
/// below `through`, that no merge row, active lost value or kept counted
/// state names, that is not its device's last landed write, and is not
/// `newest`, the latest stamped one at or below `through`. Its plaintext
/// header goes with it. An entry above `through` is looked at again once the
/// slot passes it.
pub(crate) fn forget(
    db: &DatabaseConnection,
    through: u64,
    newest: Option<i64>,
) -> Result<(), DbError> {
    let candidates =
        db.query("SELECT write_id FROM _coven_unnamed_writes", [], |r| r.get::<_, i64>(0))?;
    db.internal_execute("DELETE FROM _coven_unnamed_writes", [])?;
    for ordinal in candidates.into_iter().filter(|ordinal| Some(*ordinal) != newest) {
        let (write, slot) = db.query_row(
            "SELECT timestamp,number,slot FROM _coven_writes WHERE id=?1",
            [ordinal],
            |r| {
                let stamp = decoded(merge_fields::decode_timestamp(&r.get::<_, Vec<u8>>(0)?))?;
                let write = WriteId { device: stamp.device(), number: counter(r.get(1)?)? };
                Ok((write, r.get::<_, Option<Vec<u8>>>(2)?.map(slot_counter).transpose()?))
            },
        )?;
        if slot.is_none_or(|slot| slot.get() > through)
            || last(db, write)?
            || named(db, ordinal, write)?
        {
            continue;
        }
        crate::write_headers::forget(db, write)?;
        crate::write_commit::forget_entry(db, ordinal)?;
    }
    Ok(())
}

/// Whether `write` is its device's last landed write: no write of its
/// device numbered above it landed.
fn last(db: &DatabaseConnection, write: WriteId) -> Result<bool, DbError> {
    let Some(next) = write.number.checked_add(1) else {
        return Ok(true);
    };
    Ok(!crate::write_metadata::landed(db, WriteId { device: write.device, number: next })?)
}

/// Whether something reads `write`'s entry, `ordinal`: a merge row's
/// generation or cell, the counted state kept aside for a row, or an active
/// lost value's setter or replacer.
fn named(db: &DatabaseConnection, ordinal: i64, write: WriteId) -> Result<bool, DbError> {
    let id = encoded(merge_fields::encode_write_id(&write))?;
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_rows WHERE write_id=?1)
             OR EXISTS(SELECT 1 FROM _coven_cells WHERE write_id=?1)
             OR EXISTS(SELECT 1 FROM _coven_published_writes WHERE write_id=?1)
             OR EXISTS(SELECT 1 FROM _coven_lost WHERE set_by=?2 AND frozen=0 AND replacement_kind='write')
             OR EXISTS(SELECT 1 FROM _coven_lost WHERE replaced_by=?2 AND frozen=0 AND replacement_kind='write')",
        params![ordinal, id],
        |r| r.get(0),
    )
}
