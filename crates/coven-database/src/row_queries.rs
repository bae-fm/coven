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

/// The registered row of a synced table's foreign key. Preparing the schema
/// registers every one, so a reference can only name a registered key (Appendix B.1).
pub(crate) fn foreign_key(
    database: &DatabaseConnection,
    table: &str,
    key: &ForeignKey,
) -> Result<i64, DbError> {
    let id = database.foreign_key_id(table, &encoded(merge_fields::encode_foreign_key(key))?)?;
    Ok(id.unwrap_or_else(|| panic!("foreign key {key:?} of {table} was never registered")))
}

/// Register one `_coven_columns` row per column and one `_coven_foreign_keys`
/// row per foreign key of each synced table (Appendix B.1). Runs after a migration has
/// carried renamed identities over and removed vanished ones, and on every
/// writable open.
pub(crate) fn register_schema(
    database: &DatabaseConnection,
    schema: &crate::schema::Schema,
    tables: &[crate::SyncedTable],
) -> Result<(), DbError> {
    // The declarations a later open or migration compares against (§18.3).
    database.internal_execute("DELETE FROM _coven_owner_tables", [])?;
    for declaration in tables.iter().filter(|d| d.owner_only) {
        let table = &schema.tables[&declaration.name.to_ascii_lowercase()];
        database.internal_execute(
            "INSERT INTO _coven_owner_tables(table_name) VALUES(?1)",
            [&table.name],
        )?;
    }
    crate::file_table::register(database, schema, tables)?;
    for declaration in &crate::file_table::synced(tables) {
        let table = &schema.tables[&declaration.name.to_ascii_lowercase()];
        for column in &table.columns {
            // A dropped column a frozen loss still names takes up the name
            // again when a migration brings it back (§18.7).
            database.internal_execute(
                "INSERT INTO _coven_columns(table_name,column_name,synced) VALUES(?1,?2,1)
                 ON CONFLICT(table_name,column_name) DO UPDATE SET synced=1 WHERE synced=0",
                rusqlite::params![table.name, column.name],
            )?;
        }
        for key in &table.foreign_keys {
            definition_id(
                database,
                FOREIGN_KEYS,
                &table.name,
                &encoded(merge_fields::encode_foreign_key(&schema.foreign_key(table, key)))?,
            )?;
        }
    }
    Ok(())
}

/// The first registered synced table whose `owner_only` declaration differs
/// from the one last registered. A table never registered has no earlier
/// declaration to change.
pub(crate) fn owner_declaration_changed(
    database: &DatabaseConnection,
    schema: &crate::schema::Schema,
    tables: &[crate::SyncedTable],
) -> Result<Option<String>, DbError> {
    for declaration in tables {
        let Some(table) = schema.tables.get(&declaration.name.to_ascii_lowercase()) else {
            continue;
        };
        let (registered, owner): (bool, bool) = database.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_columns WHERE table_name=?1 AND synced=1),
                    EXISTS(SELECT 1 FROM _coven_owner_tables WHERE table_name=?1)",
            [&table.name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if registered && owner != declaration.owner_only {
            return Ok(Some(table.name.clone()));
        }
    }
    Ok(None)
}

pub(crate) fn constraint(
    database: &DatabaseConnection,
    table: &str,
    constraint: &UniqueConstraint,
) -> Result<i64, DbError> {
    definition_id(
        database,
        CONSTRAINTS,
        table,
        &encoded(merge_fields::encode_unique_constraint(constraint))?,
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
    Ok(RowId { table: r.get(0)?, key: r.get(1)?, audience: audience(&r.get::<_, String>(2)?)? })
}

/// A table-scoped metadata definition's statements: record it once, and
/// read its stable row id.
struct Definitions {
    insert: &'static str,
    id: &'static str,
}

const FOREIGN_KEYS: Definitions = Definitions {
    insert:
        "INSERT INTO _coven_foreign_keys(table_name,identity) VALUES(?1,?2) ON CONFLICT DO NOTHING",
    id: "SELECT id FROM _coven_foreign_keys WHERE table_name=?1 AND identity=?2",
};

const CONSTRAINTS: Definitions = Definitions {
    insert:
        "INSERT INTO _coven_constraints(table_name,identity) VALUES(?1,?2) ON CONFLICT DO NOTHING",
    id: "SELECT id FROM _coven_constraints WHERE table_name=?1 AND identity=?2",
};

/// Insert a table-scoped metadata definition once and return its stable row id.
fn definition_id(
    database: &DatabaseConnection,
    definitions: Definitions,
    table: &str,
    identity: &dyn rusqlite::ToSql,
) -> Result<i64, DbError> {
    let parameters = rusqlite::params![table, identity];
    database.internal_execute(definitions.insert, parameters)?;
    database.query_row(definitions.id, parameters, |r| r.get(0))
}

#[cfg(test)]
#[path = "row_queries_tests.rs"]
pub(crate) mod tests;
