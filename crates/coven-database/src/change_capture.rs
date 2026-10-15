//! Convert a transaction's session changes into read dependencies before commit.

use crate::{observation::RowChange, sql_value::SqlValue, sqlite::DatabaseConnection, DbError};
use rusqlite::{
    fallible_streaming_iterator::FallibleStreamingIterator, hooks::Action, session::ChangesetIter,
};
use std::collections::BTreeMap;

struct TableColumns {
    names: Vec<String>,
    generated: Vec<String>,
    virtual_parent: Option<String>,
}

/// Tables whose changes need an update hook in addition to the session. An
/// INTEGER PRIMARY KEY alias has no primary-key index and cannot contain NULL,
/// even though table_xinfo reports its declared NOT NULL flag as false.
pub(crate) fn supplemental_tables(
    database: &DatabaseConnection,
) -> Result<BTreeMap<String, Option<String>>, DbError> {
    let names = database.query(
        "SELECT name FROM pragma_table_list AS t
         WHERE schema='main' AND type IN ('table','shadow') AND wr=0
         AND name NOT GLOB 'sqlite_*'
         AND (NOT EXISTS (SELECT 1 FROM pragma_table_xinfo(t.name,'main') WHERE pk>0)
              OR (EXISTS (SELECT 1 FROM pragma_table_xinfo(t.name,'main') WHERE pk>0 AND \"notnull\"=0)
                  AND EXISTS (SELECT 1 FROM pragma_index_list(t.name,'main') WHERE origin='pk')))",
        [],
        |row| row.get::<_, String>(0).map(|name| name.to_ascii_lowercase()),
    )?;
    names
        .into_iter()
        .map(|table| {
            let parent = shadow_parent(database, &table)?;
            Ok((table, parent))
        })
        .collect()
}

pub(crate) fn shadow_parent(
    database: &DatabaseConnection,
    table: &str,
) -> Result<Option<String>, DbError> {
    let shadow: bool = database.query_row(
        "SELECT type='shadow' FROM pragma_table_list WHERE schema='main' AND name=?1 COLLATE NOCASE",
        [table],
        |r| r.get(0),
    )?;
    if !shadow {
        return Ok(None);
    }
    // SQLite's built-in virtual modules name shadow tables with the owning
    // table's name and a suffix. Longest prefix disambiguates underscores.
    let parents = database.query("SELECT lower(name) FROM pragma_table_list WHERE schema='main' AND type='virtual' ORDER BY length(name) DESC", [], |r| r.get::<_,String>(0))?;
    Ok(Some(
        parents
            .into_iter()
            .find(|parent| table.starts_with(&format!("{parent}_")))
            .expect("shadow table has a virtual-table owner"),
    ))
}

pub(crate) fn changes(
    database: &DatabaseConnection,
    bytes: &[u8],
) -> Result<Vec<RowChange>, DbError> {
    let mut input = bytes;
    let input: &mut dyn std::io::Read = &mut input;
    let mut items = ChangesetIter::start_strm(&input)?;
    let mut tables = BTreeMap::<String, TableColumns>::new();
    let mut changes = Vec::new();
    while let Some(item) = items.next()? {
        let operation = item.op()?;
        let table = operation.table_name().to_ascii_lowercase();
        if !tables.contains_key(&table) {
            let fields = database.query(
                "SELECT name,pk,hidden FROM pragma_table_xinfo(?1,'main') ORDER BY cid",
                [&table],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?.to_ascii_lowercase(),
                        r.get::<_, bool>(1)?,
                        r.get::<_, u32>(2)?,
                    ))
                },
            )?;
            let mut names: Vec<_> = fields
                .iter()
                .filter(|(_, _, hidden)| *hidden == 0)
                .map(|(name, _, _)| name.clone())
                .collect();
            if !fields.iter().any(|(_, pk, _)| *pk) {
                // SQLITE_SESSION_OBJCONFIG_ROWID adds the implicit key first.
                names.insert(0, "rowid".into());
            }
            assert_eq!(
                names.len(),
                operation.number_of_columns() as usize,
                "session columns match table metadata"
            );
            let virtual_parent = shadow_parent(database, &table)?;
            tables.insert(
                table.clone(),
                TableColumns {
                    names,
                    generated: fields
                        .into_iter()
                        .filter(|(_, _, hidden)| *hidden != 0)
                        .map(|(name, _, _)| name)
                        .collect(),
                    virtual_parent,
                },
            );
        }
        let columns = &tables[&table];
        if let Some(parent) = &columns.virtual_parent {
            changes.push(RowChange {
                table: parent.clone(),
                column: String::new(),
                keys: None,
            });
        }
        let mut old = BTreeMap::new();
        let mut new = BTreeMap::new();
        let pk = item.pk()?;
        let mut changed = Vec::new();
        for (index, name) in columns.names.iter().enumerate() {
            let before = if operation.code() == Action::SQLITE_INSERT {
                None
            } else {
                value(item.old_value(index), index)?
            };
            let after = if operation.code() == Action::SQLITE_DELETE {
                None
            } else {
                value(item.new_value(index), index)?
            };
            if pk[index] != 0 {
                // An UPDATE includes the old key and omits unchanged new keys.
                let after = match (operation.code(), after) {
                    (Action::SQLITE_UPDATE, None) => before.clone(),
                    (_, after) => after,
                };
                old.insert(name.clone(), before.unwrap_or(SqlValue::Null));
                new.insert(name.clone(), after.unwrap_or(SqlValue::Null));
            } else if before.is_some() || after.is_some() {
                changed.push(name.clone());
            }
        }
        if operation.code() == Action::SQLITE_UPDATE {
            // Sessions omit generated columns; they can change with their inputs.
            changed.extend(columns.generated.iter().cloned());
        } else {
            changed = vec![String::new()];
        }
        for column in changed {
            changes.push(RowChange {
                table: table.clone(),
                column,
                keys: Some((old.clone(), new.clone())),
            });
        }
    }
    Ok(changes)
}

fn value(
    value: rusqlite::Result<rusqlite::types::ValueRef<'_>>,
    index: usize,
) -> rusqlite::Result<Option<SqlValue>> {
    match value {
        Ok(value) => Ok(Some(value.into())),
        Err(rusqlite::Error::InvalidColumnIndex(i)) if i == index => Ok(None),
        Err(error) => Err(error),
    }
}

/// Read dependencies of a local write's app phase, from its row operations.
/// Each operation counts as a session change of its row would: inserts and
/// deletes change the whole row, updates each column whose value changed,
/// and a primary-key update deletes the old key's row and inserts the new.
/// Tables a session cannot key report table-wide changes.
pub(crate) fn event_changes(
    database: &DatabaseConnection,
    events: &[crate::sqlite::Event],
    supplemental: &BTreeMap<String, Option<String>>,
) -> Result<Vec<RowChange>, DbError> {
    struct Fields {
        names: Vec<(String, bool, bool)>,
        keyed: bool,
        virtual_parent: Option<String>,
    }
    let mut tables = BTreeMap::<String, Fields>::new();
    let mut changes = Vec::new();
    for event in events {
        let table = event.table.to_ascii_lowercase();
        if !tables.contains_key(&table) {
            let names = database.query(
                "SELECT name,pk,hidden FROM pragma_table_xinfo(?1,'main') ORDER BY cid",
                [&table],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?.to_ascii_lowercase(),
                        r.get::<_, bool>(1)?,
                        r.get::<_, u32>(2)? != 0,
                    ))
                },
            )?;
            let keyed = names.iter().any(|(_, pk, _)| *pk) && !supplemental.contains_key(&table);
            tables.insert(
                table.clone(),
                Fields {
                    names,
                    keyed,
                    virtual_parent: shadow_parent(database, &table)?,
                },
            );
        }
        let fields = &tables[&table];
        if let Some(parent) = &fields.virtual_parent {
            changes.push(RowChange {
                table: parent.clone(),
                column: String::new(),
                keys: None,
            });
        }
        if !fields.keyed {
            changes.push(RowChange {
                table,
                column: String::new(),
                keys: None,
            });
            continue;
        }
        let key = |values: &[Option<SqlValue>]| {
            fields
                .names
                .iter()
                .zip(values.iter().map(Some).chain(std::iter::repeat(None)))
                .filter(|((_, pk, _), _)| *pk)
                .map(|((name, _, _), value)| {
                    let value = value.and_then(Option::clone).unwrap_or(SqlValue::Null);
                    (name.clone(), value)
                })
                .collect::<BTreeMap<_, _>>()
        };
        let absent = key(&[]);
        let old = key(&event.old);
        let new = key(&event.new);
        let whole = |old: BTreeMap<_, _>, new: BTreeMap<_, _>| RowChange {
            table: table.clone(),
            column: String::new(),
            keys: Some((old, new)),
        };
        match event.action {
            Action::SQLITE_INSERT => changes.push(whole(absent, new)),
            Action::SQLITE_DELETE => changes.push(whole(old, absent)),
            _ if old != new => {
                changes.push(whole(old, absent.clone()));
                changes.push(whole(absent, new));
            }
            _ => {
                for (index, (name, pk, hidden)) in fields.names.iter().enumerate() {
                    // Sessions omit generated columns; they can change with their inputs.
                    if *pk || (!*hidden && event.old.get(index) == event.new.get(index)) {
                        continue;
                    }
                    changes.push(RowChange {
                        table: table.clone(),
                        column: name.clone(),
                        keys: Some((old.clone(), new.clone())),
                    });
                }
            }
        }
    }
    Ok(changes)
}
