//! An additive set hash of agreement state, committed with each affected region.
//!
//! Durable leaves and their sum modulo 2^256 do not depend on key availability.
//! Only the keyed sum leaves the database; unlocking or rotating a key never
//! scans app rows.
use crate::removal_view::DatabaseRemovalView;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, encoded};
use crate::DbError;
use coven_crypto::{ContentHasher, FingerprintHasher};
use coven_format::{loss::Loss, merge_fields};
use coven_merge::{Audience, ColumnValue, RemovalResult};
use rusqlite::params;
use std::collections::BTreeMap;

fn hash(fields: &[&[u8]]) -> [u8; 32] {
    let mut hasher = ContentHasher::new();
    for field in fields {
        hasher.update(&(field.len() as u64).to_be_bytes());
        hasher.update(field);
    }
    *hasher.finish().as_bytes()
}

pub(crate) fn update(
    database: &DatabaseConnection,
    view: &DatabaseRemovalView<'_>,
    result: &RemovalResult,
) -> Result<(), DbError> {
    for id in &result.region {
        let state = view.state(id)?;
        if state.generations().is_empty() {
            continue;
        }
        let mut fields = vec![
            b"coven/agreement/row/v1".to_vec(),
            id.table.as_bytes().to_vec(),
            id.key.clone(),
            (state.generations().len() as u64).to_be_bytes().to_vec(),
        ];
        for (generation, write) in state.generations() {
            fields.push(generation.to_be_bytes().to_vec());
            fields.push(encoded(merge_fields::encode_write_id(write))?);
        }
        // Length framing separates generations, cells and visible values.
        let setters = state
            .cells()
            .iter()
            .map(|(name, cell)| (name.clone(), cell.write))
            .collect();
        fields.push(encoded(merge_fields::encode_setters(&setters))?);
        let written_references = state
            .cells()
            .iter()
            .filter(|(_, cell)| !cell.value.parents.is_empty())
            .map(|(name, cell)| (name.clone(), cell.value.clone()))
            .collect();
        fields.push(encoded(merge_fields::encode_columns(&written_references))?);
        let row = view.evaluated(id)?;
        let values = if state.present() && !result.removed.contains_key(id) {
            row.values
                .iter()
                .map(|(name, value)| {
                    (
                        name.clone(),
                        ColumnValue {
                            value: value.clone(),
                            parents: BTreeMap::new(),
                        },
                    )
                })
                .collect()
        } else {
            BTreeMap::new()
        };
        fields.push(encoded(merge_fields::encode_columns(&values))?);
        let leaf = hash(&fields.iter().map(Vec::as_slice).collect::<Vec<_>>());
        let key = hash(&[b"row", id.table.as_bytes(), &id.key]);
        put(database, &id.audience, key, leaf, None)?;
        forget_merge_losses(database, id)?;
        for (lost_key, value) in state.lost() {
            loss(
                database,
                &Loss::cell(id.clone(), lost_key.clone(), value.clone()),
            )?;
        }
        if let Some(rules) = result.removed.get(id) {
            loss(database, &Loss::removed(&state, rules.clone()))?;
        }
    }
    Ok(())
}

pub(crate) fn loss(database: &DatabaseConnection, loss: &Loss) -> Result<(), DbError> {
    let identity = encoded(merge_fields::encode_loss_identity(loss))?;
    let value = encoded(merge_fields::encode_loss(loss))?;
    put(
        database,
        &loss.row.audience,
        hash(&[b"loss", &identity]),
        hash(&[&value]),
        (!loss.retired).then(|| hash(&[b"row", loss.row.table.as_bytes(), &loss.row.key])),
    )
}

pub(crate) fn forget_loss(database: &DatabaseConnection, loss: &Loss) -> Result<(), DbError> {
    let identity = encoded(merge_fields::encode_loss_identity(loss))?;
    forget(database, &loss.row.audience, hash(&[b"loss", &identity]))
}

fn forget_merge_losses(
    database: &DatabaseConnection,
    row: &coven_merge::RowId,
) -> Result<(), DbError> {
    let key = hash(&[b"row", row.table.as_bytes(), &row.key]);
    let audience = audience_text(&row.audience);
    // Finish reading the leaf cursor before deleting from its table.
    database.for_each::<_, crate::DbError>(
        "SELECT hash FROM _coven_fingerprint_leaves WHERE audience=?1 AND merge_row=?2",
        params![audience, key.as_slice()],
        |r| {
            let hash: [u8; 32] = r.get(0)?;
            database.internal_execute("UPDATE _coven_fingerprint_sums SET sum=_coven_fingerprint_replace(sum,?2,zeroblob(32)) WHERE audience=?1",params![audience,hash.as_slice()])?;
            Ok(())
        },
    )?;
    database.internal_execute(
        "DELETE FROM _coven_fingerprint_leaves WHERE audience=?1 AND merge_row=?2",
        params![audience, key.as_slice()],
    )?;
    Ok(())
}

fn forget(
    database: &DatabaseConnection,
    audience: &Audience,
    key: [u8; 32],
) -> Result<(), DbError> {
    let audience = audience_text(audience);
    database.internal_execute("UPDATE _coven_fingerprint_sums SET sum=_coven_fingerprint_replace(sum,(SELECT hash FROM _coven_fingerprint_leaves WHERE audience=?1 AND key=?2),zeroblob(32)) WHERE audience=?1", params![audience,key.as_slice()])?;
    database.internal_execute(
        "DELETE FROM _coven_fingerprint_leaves WHERE audience=?1 AND key=?2",
        params![audience, key.as_slice()],
    )?;
    Ok(())
}

pub(crate) fn forget_rows<'a>(
    database: &DatabaseConnection,
    rows: impl Iterator<Item = &'a coven_merge::RowId>,
) -> Result<(), DbError> {
    for row in rows {
        let key = hash(&[b"row", row.table.as_bytes(), &row.key]);
        forget(database, &row.audience, key)?;
        forget_merge_losses(database, row)?;
    }
    Ok(())
}

pub(crate) fn read(
    database: &DatabaseConnection,
    audience: &Audience,
    mut key: FingerprintHasher,
) -> Result<coven_crypto::Fingerprint, DbError> {
    key.update(b"coven/agreement/root/v1");
    key.update(audience_text(audience).as_bytes());
    let sum: Option<[u8; 32]> = database.query_row(
        "SELECT (SELECT sum FROM _coven_fingerprint_sums WHERE audience=?1)",
        [audience_text(audience)],
        |row| row.get(0),
    )?;
    // The empty set has sum zero, including an audience with no leaves yet.
    key.update(&sum.unwrap_or_default());
    Ok(key.finish())
}

/// Big-endian addition and subtraction, discarding overflow modulo 2^256.
/// SQLite calls this while updating the sum so no separate read is needed.
pub(crate) fn replace_sum(mut sum: [u8; 32], old: [u8; 32], new: [u8; 32]) -> [u8; 32] {
    let mut carry = 0i16;
    for i in (0..32).rev() {
        let digit = i16::from(sum[i]) - i16::from(old[i]) + i16::from(new[i]) + carry;
        sum[i] = digit as u8;
        carry = digit >> 8;
    }
    sum
}

fn put(
    database: &DatabaseConnection,
    audience: &Audience,
    key: [u8; 32],
    leaf: [u8; 32],
    merge_row: Option<[u8; 32]>,
) -> Result<(), DbError> {
    let audience = audience_text(audience);
    // The sum must bind each leaf to its identity, including excluded writes.
    let leaf = hash(&[b"coven/agreement/leaf/v1", &key, &leaf]);
    // The old leaf is read by primary key inside the sum update. Both statements
    // share the enclosing writer transaction, including rollback on either error.
    database.internal_execute(
        "INSERT INTO _coven_fingerprint_sums(audience,sum) VALUES(?1,?3)
         ON CONFLICT(audience) DO UPDATE SET sum=_coven_fingerprint_replace(sum,
             (SELECT hash FROM _coven_fingerprint_leaves WHERE audience=?1 AND key=?2),excluded.sum)",
        params![audience, key.as_slice(), leaf.as_slice()],
    )?;
    database.internal_execute(
        "INSERT INTO _coven_fingerprint_leaves(audience,key,hash,merge_row) VALUES(?1,?2,?3,?4)
         ON CONFLICT(audience,key) DO UPDATE SET hash=excluded.hash,merge_row=excluded.merge_row",
        params![
            audience,
            key.as_slice(),
            leaf.as_slice(),
            merge_row.as_ref().map(|key| key.as_slice())
        ],
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "fingerprint_tests.rs"]
mod tests;
