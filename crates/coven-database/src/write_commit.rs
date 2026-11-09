//! Persist exactly the row updates produced by coven-merge, with their record.
use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::encoded;
use crate::DbError;
use coven_format::{merge_fields, value::Value, write::WriteRecord, write_stream::WriteEncoder};
use coven_foundation::id_source::{DeviceId, ObjectId};
use coven_merge::{Audience, LostChange, RowId, RowUpdate, WriteId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

/// Store a write and the row updates merging it produced. Returns each
/// present row's current generation, as `persist` does.
pub(crate) fn commit(
    database: &DatabaseConnection,
    record: &WriteRecord,
    store: &MergeStore<'_>,
    updates: &BTreeMap<RowId, RowUpdate<Value>>,
) -> Result<BTreeMap<RowId, StoredGeneration>, DbError> {
    let write_id = retain_original(database, &record.header, None)?;
    persist(
        database,
        updates,
        |row| store.row(row),
        |id| Ok(if id == record.header.position { write_id } else { store.write_ordinal(id) }),
    )
}

pub(crate) fn retain_metadata(
    database: &DatabaseConnection,
    write: &coven_format::snapshot_rows::AppliedWrite,
) -> Result<i64, DbError> {
    retain(database, write.id, write.timestamp, write.read_through, Some(write.slot))
}

pub(crate) fn retain_original(
    database: &DatabaseConnection,
    header: &coven_format::write::WriteHeader,
    slot: Option<coven_merge::Slot>,
) -> Result<i64, DbError> {
    retain(database, header.position, header.timestamp, header.read_through, slot)
}

/// Refuse a landing write that read less than its device's last landed
/// write (§7.1): a genuine device's applied slot never goes back, and
/// forgetting kept records relies on it (§16.7). A write already stored is
/// this device's own, queued here, or one that landed before, checked then.
pub(crate) fn check_reads(
    database: &DatabaseConnection,
    header: &coven_format::write::WriteHeader,
) -> Result<(), DbError> {
    if crate::write_metadata::stored_write(database, header.position)?.is_some() {
        return Ok(());
    }
    let last = crate::write_metadata::last_landed_read(database, header.position.device)?;
    if last.is_some_and(|last| header.read_through < last) {
        return Err(DbError::InvalidWrite {
            write: header.position,
            error: coven_merge::MergeError::CausalClosure(header.position),
        });
    }
    Ok(())
}

fn retain(
    database: &DatabaseConnection,
    id: WriteId,
    timestamp: coven_merge::Timestamp,
    read_through: u64,
    slot: Option<coven_merge::Slot>,
) -> Result<i64, DbError> {
    let stamp = encoded(merge_fields::encode_timestamp(&timestamp))?;
    if let Some(known) = crate::write_metadata::stored_write(database, id)? {
        if known.timestamp != timestamp
            || known.read_through != read_through
            || slot.zip(known.slot).is_some_and(|(slot, old)| slot != old)
        {
            return Err(DbError::InvalidWrite {
                write: id,
                error: coven_merge::MergeError::DuplicateWrite(id),
            });
        }
        if let Some(slot) = slot.filter(|_| known.slot.is_none()) {
            database.internal_execute(
                "UPDATE _coven_writes SET slot=?1 WHERE id=?2",
                params![slot.get().to_be_bytes().as_slice(), known.ordinal],
            )?;
        }
        return Ok(known.ordinal);
    }
    database.query_row(
        "INSERT INTO _coven_writes(timestamp,number,read_through,slot) VALUES(?1,?2,?3,?4) RETURNING id",
        params![stamp, id.number.to_be_bytes().as_slice(), read_through.to_be_bytes().as_slice(), slot.map(|s|s.get().to_be_bytes().to_vec())],
        |r| r.get(0))
}

/// Which stored rows [`forget`] deletes, with their cells, references and
/// claims.
pub(crate) enum StoredRows<'a> {
    /// Every row an audience holds.
    InAudience(&'a Audience),
    /// Every generation of one row.
    Row(&'a RowId),
    /// Every row of a table, in every audience.
    InTable(&'a str),
    /// One stored generation.
    Generation(i64),
}

/// Remove the stored `rows` and their cells, references and claims.
pub(crate) fn forget(database: &DatabaseConnection, rows: &StoredRows<'_>) -> Result<(), DbError> {
    delete_children(database, rows)?;
    match rows {
        StoredRows::InAudience(audience) => {
            database.internal_execute("DELETE FROM _coven_rows WHERE audience=?1", [audience])
        }
        StoredRows::Row(row) => database.internal_execute(
            "DELETE FROM _coven_rows WHERE table_name=?1 AND key=?2 AND audience=?3",
            params![row.table, row.key, &row.audience],
        ),
        StoredRows::InTable(name) => {
            database.internal_execute("DELETE FROM _coven_rows WHERE table_name=?1", [name])
        }
        StoredRows::Generation(id) => {
            database.internal_execute("DELETE FROM _coven_rows WHERE id=?1", [id])
        }
    }?;
    Ok(())
}

/// Remove the cells, references and claims of `rows` in `_coven_rows`. The
/// rows themselves stay for the caller to remove or keep.
fn delete_children(database: &DatabaseConnection, rows: &StoredRows<'_>) -> Result<(), DbError> {
    for table in ["_coven_references", "_coven_cells", "_coven_claims"] {
        match rows {
            StoredRows::InAudience(audience) => database.internal_execute(
                &format!(
                    "DELETE FROM {table} WHERE row_id IN (SELECT id FROM _coven_rows WHERE audience=?1)"
                ),
                [audience],
            ),
            StoredRows::Row(row) => database.internal_execute(
                &format!("DELETE FROM {table} WHERE row_id IN (SELECT id FROM _coven_rows WHERE table_name=?1 AND key=?2 AND audience=?3)"),
                params![row.table, row.key, &row.audience],
            ),
            StoredRows::InTable(name) => database.internal_execute(
                &format!(
                    "DELETE FROM {table} WHERE row_id IN (SELECT id FROM _coven_rows WHERE table_name=?1)"
                ),
                [name],
            ),
            StoredRows::Generation(id) => database
                .internal_execute(&format!("DELETE FROM {table} WHERE row_id=?1"), [id]),
        }?;
    }
    Ok(())
}

/// A present row's current generation as `persist` stored it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StoredGeneration {
    /// The generation's `_coven_rows` id.
    pub(crate) ordinal: i64,
    /// Whether `persist` inserted the generation's `_coven_rows` row, rather
    /// than finding it already stored. Nothing deletes a `_coven_rows` row
    /// without its cells, written values and claims (`forget`), so an inserted
    /// row has none until the transaction stores them.
    pub(crate) fresh: bool,
}

/// Store each row's update over its prior record. Returns each present row's
/// current generation.
pub(crate) fn persist(
    database: &DatabaseConnection,
    updates: &BTreeMap<RowId, RowUpdate<Value>>,
    prior: impl Fn(&RowId) -> Result<crate::merge_store::StoredRow, DbError>,
    ordinal: impl Fn(WriteId) -> Result<i64, DbError>,
) -> Result<BTreeMap<RowId, StoredGeneration>, DbError> {
    let mut present = BTreeMap::new();
    let mut olds = Vec::with_capacity(updates.len());
    // Each generation this write stores, as a row, a generation and the
    // writer's ordinal.
    let mut stores = Vec::new();
    for (row, update) in updates {
        let old = prior(row)?;
        for (generation, writer) in update.state.generations() {
            if old.state.generations().get(generation) != Some(writer) {
                stores.push((row, *generation, ordinal(*writer)?));
            }
        }
        olds.push(old);
    }
    let mut generations = crate::write_commit_store::store_generations(database, &stores)?;
    let mut all_cells = Vec::new();
    let mut all_references = Vec::new();
    for ((row, update), old) in updates.iter().zip(olds) {
        let audience = &row.audience;
        // The row's current generation, when this write stores it.
        let current = generations.remove(&(row, update.state.generation()));
        let advanced = old.state.generation() != update.state.generation();
        if advanced {
            if let Some(ordinal) = old.ordinal {
                delete_children(database, &StoredRows::Generation(ordinal))?;
            }
            if let Some(loss) = old.loss {
                crate::loss_record::delete(database, loss)?;
            }
        }
        if update.state.present() {
            let stored = match current {
                Some(stored) => stored,
                None => StoredGeneration {
                    ordinal: database.query_row("SELECT id FROM _coven_rows WHERE table_name=?1 AND key=?2 AND audience=?3 AND generation=?4",params![row.table,row.key,audience,update.state.generation().to_be_bytes().as_slice()],|r| r.get(0))?,
                    fresh: false,
                },
            };
            let row_ordinal = stored.ordinal;
            present.insert(row.clone(), stored);
            let mut cells = Vec::new();
            for (name, cell) in update.state.cells() {
                if !advanced && old.state.cells().get(name) == Some(cell) {
                    continue;
                }
                cells.push((database.column_id(&row.table, name)?, ordinal(cell.write)?));
            }
            all_cells.extend(cells.into_iter().map(|(column, write)| (column, row_ordinal, write)));
            for (name, cell) in update.state.cells() {
                if !advanced && old.state.cells().get(name) == Some(cell) {
                    continue;
                }
                let column = database.column_id(&row.table, name)?;
                let previous = old.state.cells().get(name).map(|c| &c.value.parents);
                if !advanced {
                    if let Some(previous) = previous {
                        for key in
                            previous.keys().filter(|key| !cell.value.parents.contains_key(*key))
                        {
                            let key = crate::row_queries::foreign_key(database, &row.table, key)?;
                            database.internal_execute("DELETE FROM _coven_references WHERE row_id=?1 AND column_id=?2 AND foreign_key_id=?3",params![row_ordinal,column,key])?;
                        }
                    }
                }
                for (key, parent) in &cell.value.parents {
                    if !advanced && previous.and_then(|p| p.get(key)) == Some(parent) {
                        continue;
                    }
                    let key = crate::row_queries::foreign_key(database, &row.table, key)?;
                    all_references.push((row_ordinal, column, key, parent));
                }
            }
        }
        for change in &update.lost_changes {
            match change {
                LostChange::Remove(key) => crate::loss_record::delete(database, old.lost_ids[key])?,
                LostChange::Put(key, value) => crate::loss_record::put_write_loss(
                    database,
                    row,
                    key,
                    value,
                    old.lost_ids.get(key).copied(),
                )?,
            }
        }
    }
    crate::write_commit_store::store_cells(database, &all_cells)?;
    crate::write_commit_store::store_references(database, &all_references)?;
    Ok(present)
}

/// Replace the unique-constraint claims of the stored generation `row`,
/// which holds `audience`'s row, with `claims`, each a constraint's id and
/// the value the row claims. Unchanged claims are left alone. A `fresh`
/// generation (see [`StoredGeneration`]) has none to read.
pub(crate) fn store_claims(
    database: &DatabaseConnection,
    row: i64,
    fresh: bool,
    audience: &Audience,
    claims: &BTreeMap<i64, Vec<u8>>,
) -> Result<(), DbError> {
    let previous: BTreeMap<i64, Vec<u8>> = if fresh {
        BTreeMap::new()
    } else {
        database
            .query("SELECT constraint_id,value FROM _coven_claims WHERE row_id=?1", [row], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?
            .into_iter()
            .collect()
    };
    for constraint in previous.keys().filter(|c| !claims.contains_key(c)) {
        database.internal_execute(
            "DELETE FROM _coven_claims WHERE row_id=?1 AND constraint_id=?2",
            params![row, constraint],
        )?;
    }
    for (constraint, value) in claims {
        if previous.get(constraint) != Some(value) {
            database.internal_execute("INSERT INTO _coven_claims(row_id,constraint_id,audience,value) VALUES(?1,?2,?3,?4) ON CONFLICT(row_id,constraint_id) DO UPDATE SET value=excluded.value", params![row,constraint,audience,value])?;
        }
    }
    Ok(())
}

/// Table `old`'s stored rows take the name `new`.
pub(crate) fn rename_table(
    database: &DatabaseConnection,
    old: &str,
    new: &str,
) -> Result<(), DbError> {
    database.internal_execute(
        "UPDATE _coven_rows SET table_name=?2 WHERE table_name=?1",
        params![old, new],
    )?;
    Ok(())
}

/// Delete the cells and references of column `id`, which a migration dropped.
pub(crate) fn forget_column(database: &DatabaseConnection, id: i64) -> Result<(), DbError> {
    database.internal_execute("DELETE FROM _coven_references WHERE column_id=?1", [id])?;
    database.internal_execute("DELETE FROM _coven_cells WHERE column_id=?1", [id])?;
    Ok(())
}

/// The cells and references of column `old` move to column `new`.
pub(crate) fn move_column(
    database: &DatabaseConnection,
    old: i64,
    new: i64,
) -> Result<(), DbError> {
    database.internal_execute(
        "UPDATE _coven_cells SET column_id=?2 WHERE column_id=?1",
        params![old, new],
    )?;
    database.internal_execute(
        "UPDATE _coven_references SET column_id=?2 WHERE column_id=?1",
        params![old, new],
    )?;
    Ok(())
}

/// Delete the references through foreign key `id`, which a migration dropped.
pub(crate) fn forget_foreign_key(database: &DatabaseConnection, id: i64) -> Result<(), DbError> {
    database.internal_execute("DELETE FROM _coven_references WHERE foreign_key_id=?1", [id])?;
    Ok(())
}

/// The references through foreign key `id` name their parent table `parent`.
pub(crate) fn repoint_references(
    database: &DatabaseConnection,
    id: i64,
    parent: &str,
) -> Result<(), DbError> {
    database.internal_execute(
        "UPDATE _coven_references SET parent_table=?2 WHERE foreign_key_id=?1 AND parent_table<>?2",
        params![id, parent],
    )?;
    Ok(())
}

/// Delete the claims on unique constraint `id`, which a migration dropped.
pub(crate) fn forget_constraint(database: &DatabaseConnection, id: i64) -> Result<(), DbError> {
    database.internal_execute("DELETE FROM _coven_claims WHERE constraint_id=?1", [id])?;
    Ok(())
}

/// Queue a new write only after authorizing every circle part and every row
/// change. The caller's write transaction holds the writer lock through these
/// checks and commit, so a membership change cannot race the authorization.
/// `replaced` is [`retain_queue`]'s.
pub(crate) fn queue(
    database: &DatabaseConnection,
    schema: &crate::write_schema::WriteSchema,
    record: &WriteRecord,
    replaced: BTreeSet<(DeviceId, ObjectId)>,
) -> Result<(), DbError> {
    let mut encoded = Vec::with_capacity(record.parts.len());
    for part in &record.parts {
        if crate::reload_gate::reloading(database, &part.audience)? {
            return Err(DbError::AudienceReloading(part.audience.clone()));
        }
        check_membership(database, record.header.position.device, &part.audience)?;
        // Each record is encoded, and so validated, once for the write: the
        // queued plaintext holds these bytes.
        let frames = crate::write_sources::encode_records(&record.header, part)?;
        for row in part.records() {
            crate::write_sources::validate(database, &record.header, row)?;
        }
        encoded.push(frames);
    }
    retain_queue(database, schema, record, &encoded, replaced)?;
    for part in &record.parts {
        crate::ownership::offer_part(database, &record.header, part)?;
    }
    if record.parts.is_empty() {
        return Ok(());
    }
    crate::author_judgement::judge_own_write(database, schema, record)
}

/// Check a write's rows, its file records among them, against the same
/// committed membership. A deleted circle refuses every write into it, as its
/// deletion, before any question of who is in it (§15.9). This is the
/// deletion check for a local write's rows and file records; a migration
/// write's rows, which never reach it, are checked in `migration_run`.
/// The caller must keep its write transaction open until the authorized work commits.
pub(crate) fn check_membership(
    database: &DatabaseConnection,
    device: DeviceId,
    audience: &Audience,
) -> Result<(), DbError> {
    if let Audience::Circle(circle) = audience {
        let deleted: bool = database.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_circle_deletions WHERE circle=?1)",
            params![circle],
            |row| row.get(0),
        )?;
        if deleted {
            return Err(DbError::DeletedCircle(*circle));
        }
        let member: bool = database.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM _coven_circle_members cm
                JOIN _coven_circles c ON c.circle=cm.circle
                JOIN _coven_devices d ON d.member=cm.member
                JOIN _coven_members m ON m.member=d.member
                WHERE cm.circle=?1 AND d.device=?2
                  AND d.state<>'removed' AND m.removed=0
            )",
            params![circle, device],
            |row| row.get(0),
        )?;
        if !member {
            return Err(DbError::NotInCircle(*circle));
        }
    }
    Ok(())
}

/// Retain already committed work even after its author leaves a circle (§15.8).
/// The header is followed by its parts' plaintext frame streams; sync records
/// sealing keys before the first upload attempt. `replaced` holds the stored
/// paths the write moves its file records off, which it protects until it
/// is published (§17.10).
pub(crate) fn retain_queue(
    database: &DatabaseConnection,
    schema: &crate::write_schema::WriteSchema,
    record: &WriteRecord,
    frames: &[Vec<Vec<u8>>],
    replaced: BTreeSet<(DeviceId, ObjectId)>,
) -> Result<(), DbError> {
    let form = crate::write_sources::KeptForm::new(schema, record.header.schema_version);
    for part in &record.parts {
        for row in part.records() {
            crate::write_sources::retain_authored(database, &form, &record.header, row)?;
        }
    }
    let encoder = encoded(WriteEncoder::with_records(record, frames))?;
    let bytes = plaintext(database, encoder)?;
    crate::upload::queue(
        database,
        record.header.position,
        &bytes,
        record.parts.iter().flat_map(|part| part.dismissals.iter().map(|d| &d.row)),
    )?;
    crate::file_protection::queued(database, record, replaced)
}

/// Check the encoder's exact plaintext length against SQLite's connection limit
/// before allocating the queue buffer, reserving room for its sealing keys.
pub(crate) fn plaintext(
    database: &DatabaseConnection,
    encoder: WriteEncoder<'_>,
) -> Result<Vec<u8>, DbError> {
    let length = database.check_value_length("write plaintext", encoder.plaintext_length())?;
    // D9's prefix records one store key and the counted part keys. SQLite's
    // row header and the two eight-byte identity fields need at most 32 bytes.
    let keys_length = coven_format::sealed_write::WriteObjectPrefix::encoded_length(
        encoder.header().parts.len(),
    )? as u64;
    database.check_value_length("write queue row", length as u64 + keys_length + 32)?;
    let mut bytes = vec![0; length];
    encoded(encoder.encode_plaintext(&mut bytes))?;
    Ok(bytes)
}

#[cfg(test)]
#[path = "write_commit_tests.rs"]
pub(crate) mod tests;
