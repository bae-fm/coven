//! Indexed relations between synced rows, independent of app-table indexes.
use crate::schema::TableSchema;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience, audience_text, encoded};
use crate::DbError;
use coven_format::merge_fields;
use coven_merge::{ForeignKey, RowId, UniqueConstraint};
use rusqlite::params;
use std::collections::BTreeSet;

pub(crate) fn children(
    database: &DatabaseConnection,
    table: &TableSchema,
    key: &ForeignKey,
    parent: &RowId,
) -> Result<BTreeSet<RowId>, DbError> {
    Ok(database.query("SELECT DISTINCT r.table_name,r.key,r.audience FROM _coven_references v JOIN _coven_rows r ON r.id=v.row_id WHERE v.parent_table=?1 AND v.parent_key=?2 AND v.parent_audience=?3 AND v.foreign_key_id=(SELECT id FROM _coven_foreign_keys WHERE table_name=?4 AND identity=?5)", params![parent.table,parent.key,audience_text(&parent.audience),table.name,encoded(merge_fields::encode_foreign_key(key))?], read_identity)?.into_iter().collect())
}

pub(crate) fn foreign_key(
    database: &DatabaseConnection,
    table: &str,
    key: &ForeignKey,
) -> Result<i64, DbError> {
    let identity = encoded(merge_fields::encode_foreign_key(key))?;
    database.internal_execute(
        "INSERT INTO _coven_foreign_keys(table_name,identity) VALUES(?1,?2) ON CONFLICT DO NOTHING",
        params![table, identity],
    )?;
    database.query_row(
        "SELECT id FROM _coven_foreign_keys WHERE table_name=?1 AND identity=?2",
        params![table, identity],
        |r| r.get(0),
    )
}

pub(crate) fn constraint(
    database: &DatabaseConnection,
    table: &str,
    constraint: &UniqueConstraint,
) -> Result<i64, DbError> {
    let identity = encoded(merge_fields::encode_unique_constraint(constraint))?;
    database.internal_execute(
        "INSERT INTO _coven_constraints(table_name,identity) VALUES(?1,?2) ON CONFLICT DO NOTHING",
        params![table, identity],
    )?;
    database.query_row(
        "SELECT id FROM _coven_constraints WHERE table_name=?1 AND identity=?2",
        params![table, identity],
        |r| r.get(0),
    )
}

pub(crate) fn claimants(
    database: &DatabaseConnection,
    table: &str,
    constraint: &UniqueConstraint,
    audience: &coven_merge::Audience,
    value: &[u8],
) -> Result<Vec<RowId>, DbError> {
    database.query("SELECT r.table_name,r.key,r.audience FROM _coven_claims c JOIN _coven_rows r ON r.id=c.row_id WHERE c.constraint_id=(SELECT id FROM _coven_constraints WHERE table_name=?1 AND identity=?2) AND c.audience=?3 AND c.value=?4", params![table,encoded(merge_fields::encode_unique_constraint(constraint))?,audience_text(audience),value], read_identity)
}

pub(crate) fn read_identity(r: &rusqlite::Row<'_>) -> rusqlite::Result<RowId> {
    Ok(RowId {
        table: r.get(0)?,
        key: r.get(1)?,
        audience: audience(&r.get::<_, String>(2)?)?,
    })
}
