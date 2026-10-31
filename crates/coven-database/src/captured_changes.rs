//! The local captured prefix follows schema identities before its raise publishes.
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::write::{WriteHeader, WritePart};
use coven_merge::{Audience, ColumnValue, ConstraintColumns, ForeignKey, Operation};
use std::collections::BTreeMap;

/// All local originals through the migration marker follow the migrated schema,
/// whether this installation has joined a store yet or not.
pub(crate) fn capture(
    db: &DatabaseConnection,
    marker: coven_merge::WriteId,
    version: u32,
) -> Result<(), DbError> {
    db.internal_execute("INSERT INTO _coven_migration_prefix(singleton,device,through,version) VALUES(1,?1,?2,?3) ON CONFLICT(singleton) DO UPDATE SET device=excluded.device,through=excluded.through,version=excluded.version",rusqlite::params![marker.device.0.to_be_bytes(),marker.number.to_be_bytes(),version])?;
    Ok(())
}

pub(crate) fn includes(db: &DatabaseConnection, header: &WriteHeader) -> Result<bool, DbError> {
    db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_migration_prefix WHERE singleton=1 AND device=?1 AND through>=?2 AND version>=?3)",rusqlite::params![header.position.device.0.to_be_bytes(),header.position.number.to_be_bytes(),header.schema_version],|r|r.get(0))
}

pub(crate) fn audiences(
    db: &DatabaseConnection,
    header: &WriteHeader,
) -> Result<Vec<Audience>, DbError> {
    if !includes(db, header)? {
        return Ok(vec![]);
    }
    db.query(
        "SELECT audience FROM _coven_authored_parts WHERE write_id=?1 ORDER BY audience",
        [coven_format::merge_fields::encode_write_id(&header.position)?],
        |r| crate::write_encoding::audience(&r.get::<_, String>(0)?),
    )
}

fn column(
    db: &DatabaseConnection,
    from: u32,
    to: u32,
    old: &str,
    new: &str,
    name: String,
    mut value: ColumnValue<coven_format::value::Value>,
) -> Result<Option<(String, ColumnValue<coven_format::value::Value>)>, DbError> {
    let Some(name) = crate::source_names::column(db, from, old, &name, to, new)? else {
        return Ok(None);
    };
    let mut parents = BTreeMap::new();
    for (key, mut parent) in value.parents {
        let Some(target) = crate::source_names::table(db, from, &key.parent, to)? else {
            continue;
        };
        let own = key
            .columns
            .0
            .iter()
            .map(|name| crate::source_names::column(db, from, old, name, to, new))
            .collect::<Result<Option<Vec<_>>, DbError>>()?;
        let other = key
            .parent_columns
            .0
            .iter()
            .map(|name| crate::source_names::column(db, from, &key.parent, name, to, &target))
            .collect::<Result<Option<Vec<_>>, DbError>>()?;
        let (Some(own), Some(other)) = (own, other) else {
            continue;
        };
        parent.row.table = target.clone();
        parents.insert(
            ForeignKey::new(ConstraintColumns(own), target, ConstraintColumns(other)),
            parent,
        );
    }
    value.parents = parents;
    Ok(Some((name, value)))
}

pub(crate) fn project(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    part: &mut WritePart,
) -> Result<(), DbError> {
    let current = *schema.versions.end();
    let from = header.schema_version;
    let mut rows = Vec::new();
    for mut row in std::mem::take(&mut part.rows) {
        let old = row.row.table.clone();
        let Some(new) = crate::source_names::table(db, from, &old, current)? else {
            continue;
        };
        if let Operation::Insert(values) | Operation::Update(values) = &mut row.change.operation {
            let mut mapped = BTreeMap::new();
            for (name, value) in std::mem::take(values) {
                if let Some((name, mut value)) = column(db, from, current, &old, &new, name, value)?
                {
                    let table = schema.table(&new);
                    value.parents.retain(|key, _| {
                        table.foreign_keys.iter().any(|fk| schema.foreign_key(table, fk) == *key)
                    });
                    mapped.insert(name, value);
                }
            }
            *values = mapped;
        }
        row.row.table = new.clone();
        row.old = row
            .old
            .into_iter()
            .map(|(name, value)| {
                Ok(crate::source_names::column(db, from, &old, &name, current, &new)?
                    .map(|name| (name, value)))
            })
            .collect::<Result<Vec<_>, DbError>>()?
            .into_iter()
            .flatten()
            .collect();
        row.sources.setters = row
            .sources
            .setters
            .into_iter()
            .map(|(name, value)| {
                Ok(crate::source_names::column(db, from, &old, &name, current, &new)?
                    .map(|name| (name, value)))
            })
            .collect::<Result<Vec<_>, DbError>>()?
            .into_iter()
            .flatten()
            .collect();
        rows.push(row);
    }
    part.rows = rows;
    Ok(())
}

pub(crate) fn losses(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
) -> Result<(), DbError> {
    if header.disposition != coven_format::write::WriteDisposition::Migration {
        return Ok(());
    }
    db.for_each(
        "SELECT audience,record FROM _coven_migration_losses WHERE write_id=?1",
        [coven_format::merge_fields::encode_write_id(&header.position)?],
        |r| {
            if includes(db, header)? {
                let loss = coven_format::merge_fields::decode_loss(&r.get::<_, Vec<u8>>(1)?)?;
                crate::loss_record::put(db, &loss, None)?;
                crate::file_protection::retain_loss(db, schema, &loss)?;
            }
            Ok::<_, DbError>(())
        },
    )
}
