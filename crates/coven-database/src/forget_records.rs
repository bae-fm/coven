//! The slot at which each kept change stopped being current, and forgetting
//! the records no write still to be judged can name (§16.7).
//!
//! A change is current while it supplies its row's generation or sets one of
//! its present cells. A change that stopped never becomes current again, so a
//! genuine write names it only if it read less than the slot where it
//! stopped; once every device that can still write has written past that
//! slot, and this device's queued writes read past it, its record goes.

use crate::write_encoding::{counter, decoded, encoded};
use crate::{sqlite::DatabaseConnection, DbError};
use coven_format::merge_fields;
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, RowId, RowState, Slot, WriteId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

/// The other devices that can still write: every device besides this one
/// not gone for good (§14.1), each with the slot whose entry added it, when
/// known. Stored files (§17.10) and kept records (§16.7) wait for the same
/// devices.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Writers(BTreeMap<DeviceId, Option<u64>>);

impl Writers {
    /// The devices, each with the slot that added it, when known.
    pub fn new(devices: BTreeMap<DeviceId, Option<u64>>) -> Self {
        Self(devices)
    }

    /// The devices.
    pub fn devices(&self) -> impl Iterator<Item = DeviceId> + '_ {
        self.0.keys().copied()
    }

    /// The greatest slot every one of these devices has seen: each has landed
    /// a write that read through it, or was added after it, so writes nothing
    /// that read less. `reads` holds the `read_through` of each device's last
    /// landed write, for those that landed one. With no other device, every
    /// slot.
    pub fn through(&self, reads: &BTreeMap<DeviceId, u64>) -> u64 {
        self.0
            .iter()
            .map(|(device, added)| {
                // A device added at slot `a` writes nothing that read less
                // than `a`; one that landed nothing and whose adding slot
                // isn't known has seen nothing.
                let seen = added.map(|added| added.saturating_sub(1));
                reads.get(device).copied().max(seen).unwrap_or(0)
            })
            .min()
            .unwrap_or(u64::MAX)
    }
}

/// The writes current in a row's merge state: the one that supplied its
/// generation, and each one setting a present cell.
fn current(state: &RowState<coven_format::value::Value>) -> BTreeSet<WriteId> {
    state
        .generations()
        .get(&state.generation())
        .copied()
        .into_iter()
        .chain(state.cells().values().map(|cell| cell.write))
        .collect()
}

/// `write`, landing in `slot`, moved `before` to `after`: every other write
/// current before and not after stopped being current at `slot`, and its
/// kept record says so.
pub(crate) fn stopped(
    db: &DatabaseConnection,
    write: WriteId,
    before: &RowState<coven_format::value::Value>,
    after: &RowState<coven_format::value::Value>,
    slot: Slot,
) -> Result<(), DbError> {
    let after = current(after);
    for writer in current(before) {
        if writer != write && !after.contains(&writer) {
            stop(db, writer, before.row(), slot)?;
        }
    }
    Ok(())
}

/// Record that `writer`'s change at `row`, named as the row is now, stopped
/// being current at `slot`.
fn stop(db: &DatabaseConnection, writer: WriteId, row: &RowId, slot: Slot) -> Result<(), DbError> {
    let Some((_, table)) = crate::write_sources::kept_part(db, writer, row)? else {
        // A migration write another raise replaced, or one a loaded
        // snapshot covers without its part: no history holds its changes,
        // so there is no record to mark (§9.5).
        tracing::debug!(?writer, ?row, "a write stopped being current with no record held");
        return Ok(());
    };
    stopped_at(db, writer, &row.audience, &table, &row.key, slot)
}

/// Whether `write` is current in a row it just changed, as `updates` leave
/// the rows.
pub(crate) fn applied<'a>(
    write: WriteId,
    mut updates: impl Iterator<Item = &'a RowState<coven_format::value::Value>>,
) -> bool {
    updates.any(|state| current(state).contains(&write))
}

/// `write`'s change at `table` and `key` in `audience`, as its write named
/// them, stopped being current at `slot`, unless it had already stopped.
pub(crate) fn stopped_at(
    db: &DatabaseConnection,
    write: WriteId,
    audience: &Audience,
    table: &str,
    key: &[u8],
    slot: Slot,
) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_authored_changes SET stop=?5 WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4 AND stop IS NULL",
        params![
            encoded(merge_fields::encode_write_id(&write))?,
            audience,
            table,
            key,
            slot.get().to_be_bytes().as_slice()
        ],
    )?;
    Ok(())
}

/// `write` landed in `slot` and was consumed in `audiences` without being
/// staged, as a write the cut-off refuses: none of its changes there ever
/// became current, so each stops at its own slot.
pub(crate) fn none_current(
    db: &DatabaseConnection,
    write: WriteId,
    audiences: &[Audience],
    slot: Slot,
) -> Result<(), DbError> {
    let id = encoded(merge_fields::encode_write_id(&write))?;
    for audience in audiences {
        db.internal_execute(
            "UPDATE _coven_authored_changes SET stop=?3 WHERE write_id=?1 AND audience=?2 AND stop IS NULL",
            params![id, audience, slot.get().to_be_bytes().as_slice()],
        )?;
    }
    Ok(())
}

/// Forget every kept record no write still to be judged here can name: those
/// that stopped being current at or below the slot every other device that
/// can still write has seen, and this device's queued writes have read
/// (§16.7). Run in the transaction that applies a slot, after its writes are
/// judged; it deletes only what nothing can name, so no state is wrong before
/// it runs.
pub(crate) fn forget(db: &DatabaseConnection, writers: &Writers) -> Result<(), DbError> {
    let mut reads = BTreeMap::new();
    for device in writers.devices() {
        if let Some(read) = crate::write_metadata::last_landed_read(db, device)? {
            reads.insert(device, read);
        }
    }
    let own = db
        .local_device()
        .ok_or(DbError::Invariant { what: "a writer connection observes this device" })?;
    // No record stopped after the slot this device has applied, so looking
    // past it would mark slots forgotten through before they come.
    let seen = writers.through(&reads).min(crate::sequence::applied(db)?);
    let through = match crate::write_metadata::queued_read(db, own)? {
        Some(queued) => seen.min(queued),
        None => seen,
    };
    let from = forgotten_through(db)?;
    if through <= from {
        return Ok(());
    }
    let candidates = db.query(
        "SELECT write_id,audience,table_name,key FROM _coven_authored_changes WHERE stop>?1 AND stop<=?2 ORDER BY write_id,audience",
        params![from.to_be_bytes().as_slice(), through.to_be_bytes().as_slice()],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                crate::write_encoding::audience(r, 1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Vec<u8>>(3)?,
            ))
        },
    )?;
    let mut parts = BTreeMap::<(Vec<u8>, Audience), Vec<(String, Vec<u8>)>>::new();
    for (write, audience, table, key) in candidates {
        parts.entry((write, audience)).or_default().push((table, key));
    }
    for ((write, audience), records) in parts {
        forget_in_part(db, &write, &audience, records, through)?;
    }
    db.internal_execute(
        "INSERT INTO _coven_forgotten(singleton,through) VALUES(1,?1)
         ON CONFLICT(singleton) DO UPDATE SET through=excluded.through",
        [through.to_be_bytes().as_slice()],
    )?;
    Ok(())
}

/// Forget the `candidates` of one part that may go once every write still to
/// be judged has read `through`. Another member's creation that this device
/// lists proves that member's authorship in its snapshots (format D7.5), so
/// it stays; a part received whole opens its leaves by their place among all
/// its records, so its records go together, with its leaf hashes, or stay.
fn forget_in_part(
    db: &DatabaseConnection,
    write: &[u8],
    audience: &Audience,
    candidates: Vec<(String, Vec<u8>)>,
    through: u64,
) -> Result<(), DbError> {
    let id = decoded(merge_fields::decode_write_id(write))?;
    let header = db.authored_part(write, audience)?.ok_or(DbError::DamagedDatabase)?;
    let version = merge_fields::decode_write_header(&header)?.schema_version;
    let evidence = crate::authorship::keeps_evidence(db, id)?;
    let protected = |table: &str, key: &[u8]| -> Result<bool, DbError> {
        if !evidence {
            return Ok(false);
        }
        let Some(table) = crate::source_names::table(db, version, table, db.schema_version()?)?
        else {
            return Ok(false);
        };
        let row = RowId { table, key: key.to_vec(), audience: audience.clone() };
        Ok(crate::ownership::creation(db, &row)?.is_some_and(|creation| creation.write == id))
    };
    let leaves = crate::authorship::holds_leaves(db, id, audience)?;
    let records = if leaves {
        let mut records = Vec::new();
        for (table, key, stop) in db.query(
            "SELECT table_name,key,stop FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2",
            params![write, audience],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Option<Vec<u8>>>(2)?.map(counter).transpose()?,
                ))
            },
        )? {
            if stop.is_none_or(|stop| stop > through) || protected(&table, &key)? {
                return Ok(());
            }
            records.push((table, key));
        }
        records
    } else {
        let mut records = Vec::new();
        for (table, key) in candidates {
            if !protected(&table, &key)? {
                records.push((table, key));
            }
        }
        records
    };
    for (table, key) in records {
        db.internal_execute(
            "DELETE FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4",
            params![write, audience, table, key],
        )?;
    }
    let emptied = db.internal_execute(
        "DELETE FROM _coven_authored_parts WHERE write_id=?1 AND audience=?2
         AND NOT EXISTS(SELECT 1 FROM _coven_authored_changes c WHERE c.write_id=?1 AND c.audience=?2)",
        params![write, audience],
    )?;
    if emptied > 0 {
        db.forget_authored_part(write, audience);
        if leaves {
            crate::authorship::forget_part(db, id, audience)?;
        }
    }
    Ok(())
}

/// The stop slot through which this device has forgotten every record it
/// may: zero before it forgot any.
fn forgotten_through(db: &DatabaseConnection) -> Result<u64, DbError> {
    let through: Option<Vec<u8>> =
        db.query_row("SELECT (SELECT through FROM _coven_forgotten WHERE singleton=1)", [], |r| {
            r.get(0)
        })?;
    Ok(through.map(counter).transpose()?.unwrap_or(0))
}

/// Records that stopped at or below the slot forgotten through may have
/// become forgettable, or come back: a loaded snapshot brings the records its
/// writer still held, and a dropped table's creations no longer protect
/// theirs. The next slot looks at every stopped record again.
pub(crate) fn look_again(db: &DatabaseConnection) -> Result<(), DbError> {
    db.internal_execute("DELETE FROM _coven_forgotten", [])?;
    Ok(())
}

#[cfg(test)]
#[path = "forget_records_tests.rs"]
mod tests;
