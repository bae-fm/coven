//! Persist exactly the row updates produced by coven-merge, with their record.
use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, encoded};
use crate::DbError;
use coven_format::{merge_fields, value::Value, write::WriteRecord, write_stream::WriteEncoder};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, LostChange, RowId, RowUpdate, WriteId};
use rusqlite::params;
use std::collections::BTreeMap;

/// Store a write and the row updates merging it produced. Returns the id of
/// each present row's current generation, as `persist` does.
pub(crate) fn commit(
    database: &DatabaseConnection,
    record: &WriteRecord,
    store: &MergeStore<'_>,
    updates: &BTreeMap<RowId, RowUpdate<Value>>,
) -> Result<BTreeMap<RowId, i64>, DbError> {
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
                params![slot.0.to_be_bytes().as_slice(), known.ordinal],
            )?;
        }
        return Ok(known.ordinal);
    }
    database.query_row(
        "INSERT INTO _coven_writes(timestamp,number,read_through,slot) VALUES(?1,?2,?3,?4) RETURNING id",
        params![stamp, id.number.to_be_bytes().as_slice(), read_through.to_be_bytes().as_slice(), slot.map(|s|s.0.to_be_bytes().to_vec())],
        |r| r.get(0))
}

/// Which stored rows [`forget`] deletes, with their cells, references and
/// claims.
pub(crate) enum StoredRows<'a> {
    /// Every row an audience holds.
    InAudience(&'a str),
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
            params![row.table, row.key, audience_text(&row.audience)],
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
                params![row.table, row.key, audience_text(&row.audience)],
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

/// Store each row's update over its prior record. Returns the id of each
/// present row's current generation.
pub(crate) fn persist(
    database: &DatabaseConnection,
    updates: &BTreeMap<RowId, RowUpdate<Value>>,
    prior: impl Fn(&RowId) -> Result<crate::merge_store::StoredRow, DbError>,
    ordinal: impl Fn(WriteId) -> Result<i64, DbError>,
) -> Result<BTreeMap<RowId, i64>, DbError> {
    let mut present = BTreeMap::new();
    for (row, update) in updates {
        let old = prior(row)?;
        let audience = audience_text(&row.audience);
        // The id of the row's current generation, when this write stores it.
        let mut current = None;
        for (generation, writer) in update.state.generations() {
            if old.state.generations().get(generation) != Some(writer) {
                let id: i64 = database.query_row("INSERT INTO _coven_rows(table_name,key,audience,generation,write_id) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(table_name,key,audience,generation) DO UPDATE SET write_id=excluded.write_id RETURNING id",params![row.table,row.key,audience,generation.to_be_bytes().as_slice(),ordinal(*writer)?],|r| r.get(0))?;
                if *generation == update.state.generation() {
                    current = Some(id);
                }
            }
        }
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
            let row_ordinal: i64 = match current {
                Some(id) => id,
                None => database.query_row("SELECT id FROM _coven_rows WHERE table_name=?1 AND key=?2 AND audience=?3 AND generation=?4",params![row.table,row.key,audience,update.state.generation().to_be_bytes().as_slice()],|r| r.get(0))?,
            };
            present.insert(row.clone(), row_ordinal);
            let mut cells = Vec::new();
            for (name, cell) in update.state.cells() {
                if !advanced && old.state.cells().get(name) == Some(cell) {
                    continue;
                }
                cells.push((database.column_id(&row.table, name)?, ordinal(cell.write)?));
            }
            store_cells(database, row_ordinal, &cells)?;
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
                    database.internal_execute("INSERT INTO _coven_references(row_id,column_id,foreign_key_id,parent_table,parent_key,parent_audience,parent_generation) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(row_id,column_id,foreign_key_id) DO UPDATE SET parent_table=excluded.parent_table,parent_key=excluded.parent_key,parent_audience=excluded.parent_audience,parent_generation=excluded.parent_generation",params![row_ordinal,column,key,parent.row.table,parent.row.key,audience_text(&parent.row.audience),parent.generation.to_be_bytes().as_slice()])?;
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
    Ok(present)
}

/// Replace the unique-constraint claims of the stored generation `row`,
/// which holds `audience`'s row, with `claims`, each a constraint's id and
/// the value the row claims. Unchanged claims are left alone.
pub(crate) fn store_claims(
    database: &DatabaseConnection,
    row: i64,
    audience: &Audience,
    claims: &BTreeMap<i64, Vec<u8>>,
) -> Result<(), DbError> {
    let previous: BTreeMap<i64, Vec<u8>> = database
        .query("SELECT constraint_id,value FROM _coven_claims WHERE row_id=?1", [row], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?
        .into_iter()
        .collect();
    for constraint in previous.keys().filter(|c| !claims.contains_key(c)) {
        database.internal_execute(
            "DELETE FROM _coven_claims WHERE row_id=?1 AND constraint_id=?2",
            params![row, constraint],
        )?;
    }
    for (constraint, value) in claims {
        if previous.get(constraint) != Some(value) {
            database.internal_execute("INSERT INTO _coven_claims(row_id,constraint_id,audience,value) VALUES(?1,?2,?3,?4) ON CONFLICT(row_id,constraint_id) DO UPDATE SET value=excluded.value", params![row,constraint,audience_text(audience),value])?;
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
pub(crate) fn queue(
    database: &DatabaseConnection,
    schema: &crate::write_schema::WriteSchema,
    record: &WriteRecord,
) -> Result<(), DbError> {
    for part in &record.parts {
        if crate::reload_gate::reloading(database, &part.audience)? {
            return Err(DbError::AudienceReloading(part.audience.clone()));
        }
        check_membership(database, record.header.position.device, &part.audience)?;
        for row in part.records() {
            crate::write_sources::validate(database, &record.header, row)?;
        }
    }
    retain_queue(database, record)?;
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
            params![circle.to_string()],
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
            params![circle.to_string(), device],
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
/// sealing keys before the first upload attempt.
pub(crate) fn retain_queue(
    database: &DatabaseConnection,
    record: &WriteRecord,
) -> Result<(), DbError> {
    for part in &record.parts {
        for row in part.records() {
            crate::write_sources::retain(database, &record.header, row)?;
        }
        crate::write_sources::complete(database, &record.header, &part.audience)?;
    }
    let encoder = encoded(WriteEncoder::new(record))?;
    let bytes = plaintext(database, encoder)?;
    crate::upload::queue(database, record.header.position, &bytes)?;
    crate::file_protection::queued(database, record)
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

/// The cells a row's write sets, as one statement: each is a column id and
/// the write's ordinal. SQLite takes up to 32,766 parameters a statement.
fn store_cells(
    database: &DatabaseConnection,
    row: i64,
    cells: &[(i64, i64)],
) -> Result<(), DbError> {
    for chunk in cells.chunks(10_000) {
        let values = vec!["(?,?,?)"; chunk.len()].join(",");
        database.internal_execute(
            &format!("INSERT INTO _coven_cells(column_id,row_id,write_id) VALUES{values} ON CONFLICT(column_id,row_id) DO UPDATE SET write_id=excluded.write_id"),
            rusqlite::params_from_iter(chunk.iter().flat_map(|(column, write)| [*column, row, *write])),
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "write_commit_tests.rs"]
pub(crate) mod tests;
