//! Telling damage to a snapshot's staged records on this device from the
//! snapshot's own (§19.1): the digest of what a staging holds, and, for
//! tests, damage to it.

#[cfg(any(test, feature = "test-utils"))]
use super::table;
use super::{SnapshotStaging, StagedTables};
use crate::snapshot_error::SnapshotError;
use crate::sqlite::DatabaseConnection;
use crate::DbError;

/// A digest of one staging's records as its tables hold them. Staging is a
/// function of the snapshot's bytes, so two stagings of the same snapshot
/// hold the same records, and differ only where this device's disk changed
/// one (§19.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagedDigest([u8; 32]);

/// Read back every record `tables` hold and digest them, each table's rows
/// in the order of their encodings, so the digest is of their values alone.
fn digest(db: &DatabaseConnection, tables: &StagedTables) -> Result<StagedDigest, DbError> {
    let mut hasher = coven_crypto::ContentHasher::new();
    let mut table = |name: &str, select: &str, params: &[&dyn rusqlite::ToSql]| {
        let mut rows = Vec::new();
        db.for_each(select, params, |row| {
            rows.push(encoded(row)?);
            Ok::<_, DbError>(())
        })?;
        rows.sort_unstable();
        hasher.update(&(name.len() as u64).to_be_bytes());
        hasher.update(name.as_bytes());
        hasher.update(&(rows.len() as u64).to_be_bytes());
        for row in rows {
            hasher.update(&row);
        }
        Ok::<_, DbError>(())
    };
    // A table is named for its staging; the digest names it without.
    let staging = format!("_coven_staged_{}_", tables.id);
    for name in &tables.tables {
        table(&name.replacen(&staging, "", 1), &format!("SELECT * FROM {name}"), &[])?;
    }
    table(
        "_coven_snapshot_writes",
        "SELECT device,number,timestamp,read_through,slot FROM temp._coven_snapshot_writes WHERE source=?1",
        &[&tables.id],
    )?;
    Ok(StagedDigest(*hasher.finish().as_bytes()))
}

/// One row's values, each tagged with its type and text or blob lengths
/// written out, so no two rows share an encoding.
fn encoded(row: &rusqlite::Row<'_>) -> Result<Vec<u8>, DbError> {
    use rusqlite::types::ValueRef;
    let mut bytes = Vec::new();
    for i in 0..row.as_ref().column_count() {
        match row.get_ref(i)? {
            ValueRef::Null => bytes.push(0),
            ValueRef::Integer(value) => {
                bytes.push(1);
                bytes.extend_from_slice(&value.to_be_bytes());
            }
            ValueRef::Real(value) => {
                bytes.push(2);
                bytes.extend_from_slice(&value.to_bits().to_be_bytes());
            }
            ValueRef::Text(value) | ValueRef::Blob(value) => {
                bytes.push(3);
                bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
                bytes.extend_from_slice(value);
            }
        }
    }
    Ok(bytes)
}

/// A failure of a staged snapshot's body carries the digest of its staging
/// as the failed read left it, for its caller to tell damage on this device
/// from the snapshot's own (§19.1). When the staging can't be read back,
/// that failure is the one reported: the input database failed.
pub(super) fn with_staged_digest(
    db: &DatabaseConnection,
    stagings: &[SnapshotStaging],
    error: DbError,
) -> DbError {
    let DbError::Snapshot(SnapshotError::Body { snapshot, error, staged: None }) = error else {
        return error;
    };
    let tables = stagings
        .iter()
        .find(|staging| staging.expected == snapshot)
        .and_then(|staging| staging.tables.as_ref());
    match tables.map(|tables| digest(db, tables)) {
        Some(Ok(staged)) => SnapshotError::Body { snapshot, error, staged: Some(staged) }.into(),
        Some(Err(failure)) => failure,
        None => SnapshotError::Body { snapshot, error, staged: None }.into(),
    }
}

/// Damage a test makes to a staging, as damage on this device's disk would
/// (§19.1).
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StagedDamage {
    /// Its first staged record is cut to one byte, which a load or file
    /// check refuses.
    Record,
    /// Its staged writes claim to have read through their own slots, which
    /// finishing the staging refuses.
    Writes,
}

/// Make `damage` to the staging `staging`.
#[cfg(any(test, feature = "test-utils"))]
pub(super) fn damage(
    db: &DatabaseConnection,
    staging: i64,
    damage: StagedDamage,
) -> Result<(), DbError> {
    let changed = match damage {
        StagedDamage::Record => {
            let records = table(staging, "records");
            db.internal_execute(
                &format!(
                    "UPDATE {records} SET record=substr(record,1,1) WHERE rowid=(SELECT min(rowid) FROM {records})"
                ),
                [],
            )?
        }
        StagedDamage::Writes => db.internal_execute(
            "UPDATE temp._coven_snapshot_writes SET read_through=slot WHERE source=?1",
            [staging],
        )?,
    };
    match changed {
        0 => {
            Err(DbError::Invariant { what: "a test damages a staging that holds what it changes" })
        }
        _ => Ok(()),
    }
}

impl SnapshotStaging {
    /// The digest of the records it holds now (§19.1).
    pub async fn digest(&self) -> Result<StagedDigest, DbError> {
        self.id()?;
        let tables = self.tables.as_ref().map(|tables| (tables.id, tables.tables.clone()));
        let (id, tables) = tables.expect("a staging with an id holds its tables");
        self.database
            .call(move |inner| inner.inputs.write(|db| digest(db, &StagedTables { id, tables })))
            .await
    }
}
