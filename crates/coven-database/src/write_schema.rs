//! Schema-derived lookups and one-row SQLite evaluators, prepared when opening.

use crate::removal_schema::TableRules;
use crate::schema::{Schema, SchemaForeignKey, TableSchema};
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::{DbError, SyncedTable};
use coven_merge::ForeignKey;
use std::collections::BTreeMap;

pub(crate) struct WriteSchema {
    pub(crate) schema: Schema,
    /// Versions whose row layout the declarations can interpret. Breaking
    /// migrations change this range only while opening the database.
    pub(crate) versions: std::ops::RangeInclusive<u32>,
    pub(crate) declarations: Vec<SyncedTable>,
    pub(crate) rules: BTreeMap<String, TableRules>,
    pub(crate) conversions: BTreeMap<u32, crate::migration_receive::ReceivedMigration>,
    /// The file namespaces this open declares (§17.1).
    pub(crate) namespaces: Vec<crate::Namespace>,
}

impl WriteSchema {
    pub(crate) fn read(
        db: &DatabaseConnection,
        declarations: Vec<SyncedTable>,
    ) -> Result<Self, DbError> {
        let schema = Schema::read(db, &crate::file_table::declared(&declarations))?;
        let mut declarations = crate::file_table::synced(&declarations);
        for declaration in &mut declarations {
            let table = &schema.tables[&declaration.name.to_ascii_lowercase()];
            declaration.name = table.name.clone();
            for file in &mut declaration.files {
                file.column = crate::write_rows::column_name(table, &file.column).to_owned();
            }
        }
        let mut rules = BTreeMap::new();
        for declaration in &declarations {
            let table = &schema.tables[&declaration.name.to_ascii_lowercase()];
            rules.insert(table.name.clone(), TableRules::read(db, table)?);
        }
        let result = Self {
            schema,
            versions: db.query_row(
                "SELECT minimum FROM _coven_snapshot_schema WHERE singleton=1",
                [],
                |r| r.get(0),
            )?..=db.schema_version()?,
            declarations,
            rules,
            conversions: BTreeMap::new(),
            namespaces: Vec::new(),
        };
        Ok(result)
    }

    /// The table's file reference columns, as the schema spells them.
    pub(crate) fn file_columns(&self, table: &str) -> Vec<&str> {
        self.declarations
            .iter()
            .find(|d| d.name.eq_ignore_ascii_case(table))
            .map(|d| d.files.iter().map(|f| f.column.as_str()).collect())
            .unwrap_or_default()
    }

    /// The table's file reference column of this name, if it is one.
    pub(crate) fn file_column(&self, table: &str, column: &str) -> Option<&crate::FileColumn> {
        self.declarations
            .iter()
            .find(|d| d.name.eq_ignore_ascii_case(table))?
            .files
            .iter()
            .find(|f| f.column.eq_ignore_ascii_case(column))
    }

    /// A declared namespace by name.
    pub(crate) fn namespace(&self, name: &str) -> Option<&crate::Namespace> {
        self.namespaces.iter().find(|n| n.name == name)
    }

    pub(crate) fn table(&self, name: &str) -> &TableSchema {
        &self.schema.tables[&name.to_ascii_lowercase()]
    }
    /// Validate an incoming row's table, key shape and declared audience.
    #[expect(
        clippy::map_err_ignore,
        reason = "the key check refuses only a key that isn't a declared independent UUID"
    )]
    pub(crate) fn row_table(&self, row: &coven_merge::RowId) -> Result<&TableSchema, DbError> {
        use crate::snapshot_error::invalid;
        if !self
            .declarations
            .iter()
            .any(|declaration| declaration.name == row.table)
        {
            return Err(invalid("row names a table that does not sync"));
        }
        let table = self.table(&row.table);
        let key = coven_format::key::decode_key(&row.key)?;
        if key.len() != crate::write_rows::key_columns(table).len() {
            return Err(invalid("row key has the wrong number of columns"));
        }
        let values = crate::write_rows::key_columns(table)
            .into_iter()
            .map(|column| column.name.clone())
            .zip(key)
            .collect();
        crate::write_capture::validate_key(self, table, &values)
            .map_err(|_| invalid("row key is not a declared independent UUID"))?;
        if matches!(
            self.declaration(&row.table).audience,
            crate::declaration::AudienceSource::Store
        ) && row.audience != coven_merge::Audience::Store
        {
            return Err(invalid("store table is in a circle audience"));
        }
        Ok(table)
    }

    /// Validate incoming cells, including historical snapshot losses, against
    /// the same column and reference declarations used for downloaded writes.
    pub(crate) fn row_column(
        &self,
        table: &TableSchema,
        audience: &coven_merge::Audience,
        name: &str,
        value: &coven_merge::ColumnValue<coven_format::value::Value>,
    ) -> Result<(), DbError> {
        use crate::snapshot_error::invalid;
        match table.columns.iter().find(|column| column.name == name) {
            None => return Err(invalid("row names an unknown column")),
            // SQLite computes a generated column on every device, so no
            // record or snapshot carries one (format.md D5).
            Some(column) if column.generated => {
                return Err(invalid("row names a generated column"))
            }
            Some(_) => {}
        }
        for (key, parent) in &value.parents {
            if !key.columns.0.iter().any(|column| column == name)
                || parent.row.table != key.parent
                || !table
                    .foreign_keys
                    .iter()
                    .any(|foreign| self.foreign_key(table, foreign) == *key)
            {
                return Err(invalid(
                    "reference does not match the application foreign key",
                ));
            }
            self.row_table(&parent.row)?;
            // A file lives in one audience, and only its rows reference it (§17.2).
            if crate::file_table::is_files(&key.parent) && parent.row.audience != *audience {
                return Err(invalid("file reference names another audience's file"));
            }
        }
        Ok(())
    }

    /// Inserts and present snapshot rows supply their key and reference cells;
    /// updates can omit unchanged cells outside a touched reference group.
    /// Both obey identical identity checks.
    pub(crate) fn row_columns<'a>(
        &self,
        row: &coven_merge::RowId,
        complete: bool,
        columns: impl IntoIterator<
            Item = (
                &'a String,
                &'a coven_merge::ColumnValue<coven_format::value::Value>,
            ),
        >,
    ) -> Result<&TableSchema, DbError> {
        use crate::{
            declaration::AudienceSource,
            snapshot_error::invalid,
            write_rows::{column_name, key_columns, row_key, AppValues},
        };
        use coven_format::value::Value;
        let table = self.row_table(row)?;
        let columns: BTreeMap<_, _> = columns
            .into_iter()
            .map(|(name, value)| (name.as_str(), value))
            .collect();
        for (name, value) in &columns {
            self.row_column(table, &row.audience, name, value)?;
        }
        if crate::file_table::is_files(&table.name) {
            crate::file_table::check_cells(&columns, complete)?;
        }
        if complete
            && key_columns(table)
                .iter()
                .map(|column| column.name.as_str())
                .chain(
                    table
                        .foreign_keys
                        .iter()
                        .flat_map(|key| &key.columns)
                        .map(|name| column_name(table, name)),
                )
                .any(|name| !columns.contains_key(name))
        {
            return Err(invalid("present row is missing a key or reference cell"));
        }
        for foreign in table
            .foreign_keys
            .iter()
            .filter(|key| key.columns.len() > 1)
        {
            let name = self.foreign_key(table, foreign);
            let present = name
                .columns
                .0
                .iter()
                .filter(|column| columns.contains_key(column.as_str()))
                .count();
            if present == 0 {
                continue;
            }
            if present < name.columns.0.len() {
                return Err(invalid("partial multi-column reference"));
            }
            let first = &columns[name.columns.0[0].as_str()];
            let parent = first.parents.get(&name);
            let mut null = false;
            for column in &name.columns.0 {
                let value = columns.get(column.as_str()).expect("complete reference");
                if value.parents.get(&name) != parent {
                    return Err(invalid("multi-column reference parents disagree"));
                }
                null |= matches!(value.value, Value::Null);
            }
            if null != parent.is_none() {
                return Err(invalid(
                    "multi-column reference parent disagrees with NULL values",
                ));
            }
        }
        let mut keys: AppValues = key_columns(table)
            .into_iter()
            .map(|column| column.name.clone())
            .zip(coven_format::key::decode_key(&row.key)?)
            .collect();
        for (name, value) in &mut keys {
            if let Some(incoming) = columns.get(name.as_str()) {
                *value = incoming.value.clone();
            }
        }
        if row_key(table, &keys)? != row.key {
            return Err(invalid("row cells disagree with their key"));
        }
        let audience = &self.declaration(&row.table).audience;
        let name = match audience {
            AudienceSource::Store => return Ok(table),
            AudienceSource::Column(name) | AudienceSource::ForeignKey(name) => {
                column_name(table, name)
            }
            AudienceSource::Both { .. } => unreachable!("validated declaration"),
        };
        let Some(value) = columns.get(name) else {
            return if complete {
                Err(invalid("present row is missing its audience cell"))
            } else {
                Ok(table)
            };
        };
        match audience {
            AudienceSource::Column(_) => {
                if value.value != Value::Text(crate::write_encoding::audience_text(&row.audience)) {
                    return Err(invalid("row cells disagree with their audience"));
                }
            }
            AudienceSource::ForeignKey(_) => {
                let foreign = table
                    .foreign_keys
                    .iter()
                    .find(|key| {
                        key.columns
                            .iter()
                            .any(|column| column_name(table, column) == name)
                    })
                    .expect("declared audience reference");
                let parent = value
                    .parents
                    .get(&self.foreign_key(table, foreign))
                    .ok_or_else(|| invalid("audience reference has no parent"))?;
                if parent.row.audience != row.audience {
                    return Err(invalid("inherited audience differs from its parent"));
                }
            }
            _ => unreachable!("audience cell declaration"),
        }
        Ok(table)
    }

    pub(crate) fn declaration(&self, name: &str) -> &SyncedTable {
        self.declarations
            .iter()
            .find(|d| d.name == name)
            .expect("synced table declaration")
    }

    pub(crate) fn foreign_key(&self, table: &TableSchema, key: &SchemaForeignKey) -> ForeignKey {
        self.schema.foreign_key(table, key)
    }

    /// Run within the caller's transaction. Failure rolls back its temporary
    /// tables with that transaction; success removes them before returning.
    pub(crate) fn with_evaluators<R>(
        &self,
        db: &DatabaseConnection,
        run: impl FnOnce() -> Result<R, DbError>,
    ) -> Result<R, DbError> {
        self.prepare(db)?;
        let result = run()?;
        for declaration in &self.declarations {
            let table = self.table(&declaration.name);
            db.batch(&format!(
                "DROP TABLE temp.{}; DROP TABLE temp.{}",
                identifier(&evaluation_name(table)),
                identifier(&affinity_name(table))
            ))?;
        }
        Ok(result)
    }

    pub(crate) fn prepare(&self, db: &DatabaseConnection) -> Result<(), DbError> {
        for declaration in &self.declarations {
            let table = self.table(&declaration.name);
            let rules = &self.rules[&table.name];
            db.batch(&format!(
                "CREATE TEMP TABLE {} ({} INTEGER PRIMARY KEY, {})",
                identifier(&evaluation_name(table)),
                identifier(&ordinal(table)),
                rules.columns.join(",")
            ))?;
            db.batch(&format!(
                "CREATE TEMP TABLE {} ({} INTEGER PRIMARY KEY, {})",
                identifier(&affinity_name(table)),
                identifier(&ordinal(table)),
                table
                    .columns
                    .iter()
                    .map(|c| format!("{} {}", identifier(&c.name), identifier(&c.kind)))
                    .collect::<Vec<_>>()
                    .join(",")
            ))?;
        }
        Ok(())
    }
}

pub(crate) fn evaluation_name(table: &TableSchema) -> String {
    format!("_coven_eval_{}", table.name)
}
pub(crate) fn affinity_name(table: &TableSchema) -> String {
    format!("_coven_affinity_{}", table.name)
}
pub(crate) fn ordinal(table: &TableSchema) -> String {
    let mut name = "coven_ordinal".to_owned();
    while table
        .columns
        .iter()
        .any(|c| c.name.eq_ignore_ascii_case(&name))
    {
        name.push('_');
    }
    name
}

/// Whether a record names one of `table`'s generated columns anywhere: in its
/// net change's columns or old values, a replaced setter, or a step's columns.
/// SQLite computes a generated column on every device, so no record carries
/// one (format.md D5).
pub(crate) fn names_generated(
    table: &TableSchema,
    record: coven_format::write::PartRecord<'_>,
) -> bool {
    use coven_format::write::PartRecord;
    use coven_merge::Operation;
    let generated = |name: &String| table.generated(name);
    let columns = |change: &coven_merge::Change<coven_format::value::Value>| match &change.operation
    {
        Operation::Insert(columns) | Operation::Update(columns) => columns.keys().any(generated),
        Operation::Delete => false,
    };
    let execution = record.execution();
    let own = match record {
        PartRecord::Change(change) => {
            columns(&change.change)
                || change.old.keys().any(generated)
                || change.sources.setters.keys().any(generated)
        }
        PartRecord::Execution(_) => false,
    };
    own || execution.sources.setters.keys().any(generated)
        || execution.steps.iter().any(|step| columns(&step.change))
}

#[cfg(test)]
#[path = "write_schema_tests.rs"]
mod tests;
