//! SQLite's actual schema, declaration checks, and per-migration comparison.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::types::ValueRef;

use crate::declaration::AudienceSource;
use crate::sql::{guarded_trigger, identifier, table_parts, tokens};
use crate::sqlite::DatabaseConnection;
use crate::{DbError, MigrationChange, RowIdentity, SchemaError, SyncedTable};

pub(crate) struct Schema {
    objects: BTreeMap<(String, String), SchemaObject>,
    pub(crate) tables: BTreeMap<String, TableSchema>,
}

#[derive(Eq, PartialEq)]
struct SchemaObject {
    table: String,
    sql: Option<String>,
}

pub(crate) struct TableSchema {
    pub(crate) name: String,
    pub(crate) sql: String,
    pub(crate) columns: Vec<SchemaColumn>,
    pub(crate) indices: Vec<SchemaIndex>,
    pub(crate) foreign_keys: Vec<SchemaForeignKey>,
    pub(crate) without_rowid: bool,
}

pub(crate) struct SchemaColumn {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) not_null: bool,
    pub(crate) default: Option<String>,
    pub(crate) primary_key: u32,
    pub(crate) generated: bool,
}

pub(crate) struct SchemaIndex {
    pub(crate) name: String,
    pub(crate) sql: Option<String>,
    pub(crate) primary: bool,
    pub(crate) columns: Vec<Option<String>>,
    pub(crate) collations: Vec<String>,
}

pub(crate) struct SchemaForeignKey {
    pub(crate) target: String,
    pub(crate) columns: Vec<String>,
    pub(crate) target_columns: Vec<Option<String>>,
    on_update: String,
    pub(crate) on_delete: String,
}

impl Schema {
    pub(crate) fn read(db: &DatabaseConnection) -> Result<Self, DbError> {
        let objects = db.query(
            "SELECT type, name, tbl_name, sql FROM main.sqlite_schema WHERE name NOT LIKE 'sqlite_%' AND substr(lower(name),1,6) != 'coven_' ORDER BY type, name",
            [], |row| Ok(((row.get::<_, String>(0)?, row.get::<_, String>(1)?), SchemaObject { table: row.get(2)?, sql: row.get(3)? })),
        )?.into_iter().collect::<BTreeMap<_, _>>();
        let mut tables = BTreeMap::new();
        for ((kind, name), object) in &objects {
            if kind == "table" {
                let columns = db.query("SELECT name, type, \"notnull\", dflt_value, pk, hidden != 0 FROM pragma_table_xinfo(?1, 'main') ORDER BY cid", [name], |r| {
                    Ok(SchemaColumn { name: r.get(0)?, kind: r.get(1)?, not_null: r.get(2)?, default: r.get(3)?, primary_key: r.get(4)?, generated: r.get(5)? })
                })?;
                let mut indices = Vec::new();
                for (name, primary) in db.query("SELECT name, origin = 'pk' FROM pragma_index_list(?1, 'main') WHERE \"unique\" ORDER BY seq", [name], |r| Ok((r.get::<_, String>(0)?, r.get::<_, bool>(1)?)))? {
                    let (columns, collations) = db.query("SELECT name, coll FROM pragma_index_xinfo(?1, 'main') WHERE key ORDER BY seqno", [&name], |r| Ok((r.get::<_, Option<String>>(0)?, r.get::<_, String>(1)?)))?.into_iter().unzip();
                    let sql = objects.get(&("index".into(), name.clone())).and_then(|object| object.sql.clone());
                    indices.push(SchemaIndex { name, sql, primary, columns, collations });
                }
                let mut foreign_keys = BTreeMap::new();
                for (id, target, column, target_column, on_update, on_delete) in db.query("SELECT id, \"table\", \"from\", \"to\", on_update, on_delete FROM pragma_foreign_key_list(?1, 'main') ORDER BY id, seq", [name], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, Option<String>>(3)?, r.get::<_, String>(4)?, r.get::<_, String>(5)?)))? {
                    let key = foreign_keys.entry(id).or_insert_with(|| SchemaForeignKey { target, columns: Vec::new(), target_columns: Vec::new(), on_update, on_delete });
                    key.columns.push(column);
                    key.target_columns.push(target_column);
                }
                let without_rowid = object.sql.as_ref().is_some_and(|sql| {
                    tokens(sql)
                        .windows(2)
                        .any(|p| p[0].word("without") && p[1].word("rowid"))
                });
                tables.insert(
                    name.to_ascii_lowercase(),
                    TableSchema {
                        name: name.clone(),
                        sql: object.sql.clone().expect("table has CREATE SQL"),
                        columns,
                        indices,
                        foreign_keys: foreign_keys.into_values().collect(),
                        without_rowid,
                    },
                );
            }
        }
        Ok(Self { objects, tables })
    }

    pub(crate) fn validate(
        &self,
        db: &DatabaseConnection,
        declarations: &[SyncedTable],
    ) -> Result<(), DbError> {
        let mut declared = BTreeSet::new();
        for declaration in declarations {
            if declaration.name.to_ascii_lowercase().starts_with("coven_") {
                return Err(DbError::InternalTable {
                    table: declaration.name.clone(),
                });
            }
            if !declared.insert(declaration.name.to_ascii_lowercase()) {
                return Err(SchemaError::DuplicateTable {
                    table: declaration.name.clone(),
                }
                .into());
            }
        }
        let mut audience_parents = BTreeMap::new();
        for declaration in declarations {
            let error_table = || declaration.name.clone();
            let table = self
                .tables
                .get(&declaration.name.to_ascii_lowercase())
                .ok_or_else(|| SchemaError::MissingTable {
                    table: error_table(),
                })?;
            let mut key: Vec<_> = table.columns.iter().filter(|c| c.primary_key > 0).collect();
            key.sort_by_key(|c| c.primary_key);
            if key.is_empty() {
                return Err(SchemaError::NoPrimaryKey {
                    table: error_table(),
                }
                .into());
            }
            // INTEGER PRIMARY KEY aliases the rowid only when there is no
            // separate primary-key index. INTEGER PRIMARY KEY DESC has an index.
            let rowid_key = !table.without_rowid
                && key.len() == 1
                && key[0].kind.eq_ignore_ascii_case("integer")
                && !table.indices.iter().any(|i| i.primary);
            if rowid_key || key.iter().any(|c| c.default.is_some()) {
                return Err(SchemaError::GeneratedPrimaryKey {
                    table: error_table(),
                }
                .into());
            }
            if key.len() != declaration.key.len()
                || key
                    .iter()
                    .zip(&declaration.key)
                    .any(|(column, name)| !column.name.eq_ignore_ascii_case(name))
            {
                return Err(SchemaError::KeyColumns {
                    table: error_table(),
                }
                .into());
            }
            if let Some(column) = key.iter().find(|c| !c.not_null) {
                return Err(SchemaError::NullableKey {
                    table: error_table(),
                    column: column.name.clone(),
                }
                .into());
            }
            if declaration.identity == RowIdentity::IndependentUuid {
                check_uuid(db, table, &key).map_err(|error| match error {
                    DbError::Schema(SchemaError::IndependentKeyNotUuid { .. }) => {
                        SchemaError::IndependentKeyNotUuid {
                            table: error_table(),
                        }
                        .into()
                    }
                    error => error,
                })?;
            }
            for foreign_key in &table.foreign_keys {
                if foreign_key.replaces_reference() {
                    for column in &foreign_key.columns {
                        if key.iter().any(|k| k.name.eq_ignore_ascii_case(column)) {
                            return Err(SchemaError::PrimaryKeyAction {
                                table: error_table(),
                                column: column.clone(),
                            }
                            .into());
                        }
                    }
                }
            }
            let audience_key = match &declaration.audience {
                AudienceSource::Both { .. } => {
                    return Err(SchemaError::TwoAudiences {
                        table: error_table(),
                    }
                    .into())
                }
                AudienceSource::Store => None,
                AudienceSource::Column(column) => {
                    if !table.columns.iter().any(|c| {
                        c.name.eq_ignore_ascii_case(column) && c.not_null && text_affinity(&c.kind)
                    }) {
                        return Err(SchemaError::AudienceColumn {
                            table: error_table(),
                            column: column.clone(),
                        }
                        .into());
                    }
                    Some(column)
                }
                AudienceSource::ForeignKey(column) => {
                    let keys: Vec<_> = table
                        .foreign_keys
                        .iter()
                        .filter(|f| f.columns.iter().any(|c| c.eq_ignore_ascii_case(column)))
                        .collect();
                    if keys.iter().any(|f| f.columns.len() != 1) {
                        return Err(SchemaError::AudienceForeignKeyColumns {
                            table: error_table(),
                        }
                        .into());
                    }
                    let [foreign_key] = keys.as_slice() else {
                        return Err(SchemaError::AudienceForeignKeyTarget {
                            table: error_table(),
                            column: column.clone(),
                        }
                        .into());
                    };
                    if !declared.contains(&foreign_key.target.to_ascii_lowercase()) {
                        return Err(SchemaError::AudienceForeignKeyTarget {
                            table: error_table(),
                            column: column.clone(),
                        }
                        .into());
                    }
                    if foreign_key.replaces_reference() {
                        return Err(SchemaError::AudienceForeignKeyAction {
                            table: error_table(),
                            column: column.clone(),
                        }
                        .into());
                    }
                    audience_parents.insert(
                        declaration.name.to_ascii_lowercase(),
                        foreign_key.target.to_ascii_lowercase(),
                    );
                    Some(column)
                }
            };
            if let Some(audience_key) = audience_key {
                if declaration.identity == RowIdentity::SharedKey
                    && !key
                        .iter()
                        .any(|c| c.name.eq_ignore_ascii_case(audience_key))
                {
                    return Err(SchemaError::AudienceConstraint {
                        table: error_table(),
                        constraint: "PRIMARY KEY".into(),
                    }
                    .into());
                }
                for index in &table.indices {
                    if !index.primary
                        && !index.columns.iter().any(|c| {
                            c.as_ref()
                                .is_some_and(|c| c.eq_ignore_ascii_case(audience_key))
                        })
                    {
                        return Err(SchemaError::AudienceConstraint {
                            table: error_table(),
                            constraint: index.name.clone(),
                        }
                        .into());
                    }
                }
            }
            for trigger in &declaration.shared_triggers {
                let found = self.objects.iter().find(|((kind, name), obj)| {
                    kind == "trigger"
                        && name.eq_ignore_ascii_case(trigger)
                        && obj.table.eq_ignore_ascii_case(&table.name)
                });
                let (_, object) = found.ok_or_else(|| SchemaError::MissingTrigger {
                    table: error_table(),
                    trigger: trigger.clone(),
                })?;
                if !object.sql.as_deref().is_some_and(guarded_trigger) {
                    return Err(SchemaError::SharedTriggerGuard {
                        table: error_table(),
                        trigger: trigger.clone(),
                    }
                    .into());
                }
            }
            if let Some(file) = &declaration.files {
                // The spec assigns column meanings, not SQLite storage classes.
                // Check each named column against the actual table.
                for column in file.columns() {
                    if !table
                        .columns
                        .iter()
                        .any(|c| c.name.eq_ignore_ascii_case(column))
                    {
                        return Err(SchemaError::FileColumn {
                            table: error_table(),
                            column: column.into(),
                        }
                        .into());
                    }
                }
            }
        }
        self.validate_local_children(&declared)?;
        for table in self.tables.values() {
            for key in &table.foreign_keys {
                for action in [&key.on_delete, &key.on_update] {
                    if action != "SET NULL" && action != "SET DEFAULT" {
                        continue;
                    }
                    for source in &key.columns {
                        let column = table
                            .columns
                            .iter()
                            .find(|c| c.name.eq_ignore_ascii_case(source))
                            .expect("foreign key column");
                        if !column.not_null {
                            continue;
                        }
                        let null = if action == "SET NULL" {
                            true
                        } else {
                            match &column.default {
                                None => true,
                                Some(default) => {
                                    db.query_row(&format!("SELECT ({default}) IS NULL"), [], |r| {
                                        r.get::<_, bool>(0)
                                    })?
                                }
                            }
                        };
                        if null {
                            return Err(SchemaError::ImpossibleAction {
                                table: table.name.clone(),
                                column: column.name.clone(),
                            }
                            .into());
                        }
                    }
                }
            }
        }
        for declaration in declarations {
            let mut path = BTreeSet::new();
            let mut table = declaration.name.to_ascii_lowercase();
            while let Some(parent) = audience_parents.get(&table) {
                if !path.insert(table) {
                    return Err(SchemaError::AudienceCycle {
                        table: declaration.name.clone(),
                    }
                    .into());
                }
                table = parent.clone();
            }
        }
        Ok(())
    }

    fn validate_local_children(&self, synced: &BTreeSet<String>) -> Result<(), DbError> {
        let mut reached = synced.clone();
        let mut pending: Vec<_> = synced.iter().cloned().collect();
        while let Some(parent) = pending.pop() {
            for (name, table) in &self.tables {
                if synced.contains(name) {
                    continue;
                }
                for key in &table.foreign_keys {
                    if !key.target.eq_ignore_ascii_case(&parent) {
                        continue;
                    }
                    let invalid = key.columns.iter().find(|name| {
                        key.on_delete != "CASCADE"
                            && (key.on_delete != "SET NULL"
                                || table.columns.iter().any(|column| {
                                    column.name.eq_ignore_ascii_case(name) && column.not_null
                                }))
                    });
                    if let Some(column) = invalid {
                        return Err(SchemaError::LocalChildAction {
                            table: table.name.clone(),
                            column: column.clone(),
                        }
                        .into());
                    }
                    if reached.insert(name.clone()) {
                        pending.push(name.clone());
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn change_to(&self, next: &Self) -> MigrationChange {
        for (identity, before) in &self.objects {
            let Some(after) = next.objects.get(identity) else {
                return MigrationChange::Breaking;
            };
            if before == after {
                continue;
            }
            if identity.0 != "table" || before.table != after.table {
                return MigrationChange::Breaking;
            }
            let parts = before
                .sql
                .as_deref()
                .and_then(table_parts)
                .zip(after.sql.as_deref().and_then(table_parts));
            let Some(((old, old_suffix), (new, new_suffix))) = parts else {
                return MigrationChange::Breaking;
            };
            if old_suffix != new_suffix {
                return MigrationChange::Breaking;
            }
            // Existing clauses must remain verbatim in token form. Additional
            // clauses must name new columns and introduce no constraints.
            let old_names: BTreeSet<_> = self.tables[&identity.1.to_ascii_lowercase()]
                .columns
                .iter()
                .map(|c| c.name.to_ascii_lowercase())
                .collect();
            let table_key = identity.1.to_ascii_lowercase();
            let retained = next.tables[&table_key]
                .columns
                .iter()
                .filter(|column| old_names.contains(&column.name.to_ascii_lowercase()))
                .map(|column| &column.name);
            if !self.tables[&table_key]
                .columns
                .iter()
                .map(|column| &column.name)
                .eq(retained)
            {
                return MigrationChange::Breaking;
            }
            let mut remaining = new;
            for clause in old {
                let Some(index) = remaining.iter().position(|c| *c == clause) else {
                    return MigrationChange::Breaking;
                };
                remaining.remove(index);
            }
            for clause in remaining {
                let column = clause.first().and_then(|token| match token {
                    crate::sql::Token::Word(name) | crate::sql::Token::Quoted(name) => {
                        Some(name.to_ascii_lowercase())
                    }
                    _ => None,
                });
                if column.is_none_or(|name| old_names.contains(&name))
                    || clause.iter().any(|t| {
                        [
                            "constraint",
                            "primary",
                            "unique",
                            "references",
                            "check",
                            "not",
                            "collate",
                            "generated",
                        ]
                        .iter()
                        .any(|w| t.word(w))
                    })
                {
                    return MigrationChange::Breaking;
                }
            }
        }
        for ((kind, name), object) in &next.objects {
            if !self.objects.contains_key(&(kind.clone(), name.clone())) && kind != "table" {
                let table = object.table.to_ascii_lowercase();
                let on_new_table = matches!(kind.as_str(), "index" | "trigger")
                    && !self.tables.contains_key(&table)
                    && next.tables.contains_key(&table);
                if !on_new_table {
                    return MigrationChange::Breaking;
                }
            }
        }
        MigrationChange::Addition
    }
}

impl SchemaForeignKey {
    fn replaces_reference(&self) -> bool {
        [&self.on_update, &self.on_delete]
            .iter()
            .any(|s| s.eq_ignore_ascii_case("SET NULL") || s.eq_ignore_ascii_case("SET DEFAULT"))
    }
}

fn text_affinity(kind: &str) -> bool {
    let kind = kind.to_ascii_uppercase();
    !kind.contains("INT") && ["CHAR", "CLOB", "TEXT"].iter().any(|s| kind.contains(s))
}

fn check_uuid(
    db: &DatabaseConnection,
    table: &TableSchema,
    key: &[&SchemaColumn],
) -> Result<(), DbError> {
    let mut candidates: Vec<_> = key
        .iter()
        .map(|c| text_affinity(&c.kind) || c.kind.eq_ignore_ascii_case("uuid"))
        .collect();
    let sql = format!(
        "SELECT {} FROM main.{}",
        key.iter()
            .map(|c| identifier(&c.name))
            .collect::<Vec<_>>()
            .join(","),
        identifier(&table.name)
    );
    db.scan(&sql, [], |row| {
        for (i, candidate) in candidates.iter_mut().enumerate() {
            if *candidate {
                *candidate = match row.get_ref(i)? {
                    ValueRef::Text(bytes) => std::str::from_utf8(bytes).is_ok_and(|text| {
                        uuid::Uuid::parse_str(text).is_ok_and(|id| {
                            matches!(id.get_version_num(), 4 | 7)
                                && id.get_variant() == uuid::Variant::RFC4122
                                && id.to_string() == text
                        })
                    }),
                    _ => false,
                };
            }
        }
        Ok(())
    })?;
    if !candidates.iter().any(|v| *v) {
        return Err(SchemaError::IndependentKeyNotUuid {
            table: table.name.clone(),
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
