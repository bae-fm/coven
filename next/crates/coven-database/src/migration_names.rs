//! Identity across a migration: explicit renames first, surviving names second.

use crate::removal_schema::name;
use crate::schema::Schema;
use coven_merge::{ConstraintColumns, ForeignKey};
use rusqlite::fallible_iterator::FallibleIterator;
use sqlite3_parser::{
    ast::{AlterTableBody, Cmd, Stmt},
    lexer::sql::Parser,
    Bump,
};
use std::collections::{BTreeMap, BTreeSet};

struct Origin {
    table: String,
    columns: BTreeMap<String, String>,
}

pub(crate) struct MigrationNames {
    live: BTreeMap<String, Origin>,
    statements: Vec<String>,
    dropped: BTreeSet<String>,
    renamed_tables: BTreeMap<String, String>,
    renamed_columns: BTreeMap<(String, String), String>,
}

pub(crate) struct MigrationEffects {
    pub(crate) names: MigrationMatch,
    pub(crate) dropped: BTreeSet<String>,
    pub(crate) statements: Vec<String>,
}

/// Old names to their surviving names; absence means the object was dropped.
pub(crate) struct MigrationMatch {
    pub(crate) tables: BTreeMap<String, String>,
    pub(crate) columns: BTreeMap<(String, String), String>,
}

impl MigrationNames {
    pub(crate) fn new(before: &Schema) -> Self {
        Self {
            statements: Vec::new(),
            dropped: BTreeSet::new(),
            renamed_tables: BTreeMap::new(),
            renamed_columns: BTreeMap::new(),
            live: before
                .tables
                .iter()
                .map(|(name, table)| {
                    (
                        name.clone(),
                        Origin {
                            table: table.name.clone(),
                            columns: table
                                .columns
                                .iter()
                                .map(|c| (c.name.to_ascii_lowercase(), c.name.clone()))
                                .collect(),
                        },
                    )
                })
                .collect(),
        }
    }

    /// Called for an executed statement, never a failed or merely prepared rename.
    pub(crate) fn record(&mut self, sql: &str) -> rusqlite::Result<()> {
        let bump = Bump::new();
        let mut parser = Parser::new(&bump, sql.as_bytes());
        let command = parser
            .next()
            .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?;
        match command {
            Some(Cmd::Stmt(Stmt::AlterTable(table, body))) if main(&table.db_name) => {
                let table = name(table.name.0).to_ascii_lowercase();
                match body {
                    AlterTableBody::RenameTo(to) => {
                        if let Some(origin) = self.live.remove(&table) {
                            let target = name(to.0).to_ascii_lowercase();
                            self.renamed_tables.retain(|_, old| *old != origin.table);
                            self.renamed_tables
                                .insert(target.clone(), origin.table.clone());
                            self.live.insert(target, origin);
                        }
                    }
                    AlterTableBody::RenameColumn { old, new } => {
                        if let Some(origin) = self.live.get_mut(&table) {
                            if let Some(source) =
                                origin.columns.remove(&name(old.0).to_ascii_lowercase())
                            {
                                let target = name(new.0).to_ascii_lowercase();
                                self.renamed_columns
                                    .retain(|(t, _), c| *t != origin.table || *c != source);
                                self.renamed_columns
                                    .insert((origin.table.clone(), target.clone()), source.clone());
                                origin.columns.insert(target, source);
                            }
                        }
                    }
                    AlterTableBody::DropColumn(column) => {
                        if let Some(origin) = self.live.get_mut(&table) {
                            origin.columns.remove(&name(column.0).to_ascii_lowercase());
                        }
                    }
                    _ => {}
                }
            }
            Some(Cmd::Stmt(Stmt::DropTable { tbl_name, .. })) if main(&tbl_name.db_name) => {
                let table = name(tbl_name.name.0).to_ascii_lowercase();
                let original = self
                    .live
                    .remove(&table)
                    .map(|o| o.table.to_ascii_lowercase());
                self.dropped.insert(original.unwrap_or(table));
            }
            _ => {}
        }
        self.statements.push(sql.to_owned());
        Ok(())
    }

    pub(crate) fn through<'a>(
        before: &Schema,
        after: &Schema,
        statements: impl Iterator<Item = &'a str>,
    ) -> rusqlite::Result<MigrationMatch> {
        let mut names = Self::new(before);
        for statement in statements {
            names.record(statement)?;
        }
        Ok(names.finish(before, after).names)
    }

    pub(crate) fn finish(self, before: &Schema, after: &Schema) -> MigrationEffects {
        let mut tables = BTreeMap::new();
        let mut columns = BTreeMap::new();
        let mut destinations = BTreeSet::new();
        for (target, source) in &self.renamed_tables {
            if let Some(table) = after.tables.get(target) {
                tables.insert(source.clone(), table.name.clone());
                destinations.insert(target.clone());
            }
        }
        for (name, table) in &after.tables {
            if !destinations.contains(name) {
                if let Some(old) = before.tables.get(name) {
                    if !tables.contains_key(&old.name) {
                        tables.insert(old.name.clone(), table.name.clone());
                    }
                }
            }
        }
        for (old_name, new_name) in &tables {
            let old = &before.tables[&old_name.to_ascii_lowercase()];
            let new = &after.tables[&new_name.to_ascii_lowercase()];
            let mut claimed = BTreeSet::new();
            for column in &new.columns {
                if let Some(source) = self
                    .renamed_columns
                    .get(&(old_name.clone(), column.name.to_ascii_lowercase()))
                {
                    columns.insert((old_name.clone(), source.clone()), column.name.clone());
                    claimed.insert(column.name.to_ascii_lowercase());
                }
            }
            for column in &new.columns {
                if !claimed.contains(&column.name.to_ascii_lowercase()) {
                    if let Some(source) = old
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(&column.name))
                    {
                        columns
                            .entry((old_name.clone(), source.name.clone()))
                            .or_insert_with(|| column.name.clone());
                    }
                }
            }
        }
        MigrationEffects {
            names: MigrationMatch { tables, columns },
            dropped: self.dropped,
            statements: self.statements,
        }
    }
}

impl MigrationMatch {
    pub(crate) fn reference_columns(
        &self,
        before: &Schema,
        after: &Schema,
    ) -> BTreeMap<String, BTreeSet<String>> {
        let mut changes: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for ((table, column), new_column) in &self.columns {
            let old_table = &before.tables[&table.to_ascii_lowercase()];
            let new_table = &after.tables[&self.tables[table].to_ascii_lowercase()];
            let old: Option<BTreeSet<_>> = old_table
                .foreign_keys
                .iter()
                .filter(|fk| fk.columns.iter().any(|c| c.eq_ignore_ascii_case(column)))
                .map(|fk| self.foreign_key(table, &before.foreign_key(old_table, fk)))
                .collect();
            let new: BTreeSet<_> = new_table
                .foreign_keys
                .iter()
                .filter(|fk| {
                    fk.columns
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(new_column))
                })
                .map(|fk| after.foreign_key(new_table, fk))
                .collect();
            if old.as_ref() != Some(&new) {
                changes
                    .entry(new_table.name.clone())
                    .or_default()
                    .insert(new_column.clone());
            }
        }
        changes
    }

    pub(crate) fn foreign_key(&self, table: &str, key: &ForeignKey) -> Option<ForeignKey> {
        Some(ForeignKey::new(
            ConstraintColumns(
                key.columns
                    .0
                    .iter()
                    .map(|c| self.columns.get(&(table.to_owned(), c.clone())).cloned())
                    .collect::<Option<Vec<_>>>()?,
            ),
            self.tables.get(&key.parent)?.clone(),
            ConstraintColumns(
                key.parent_columns
                    .0
                    .iter()
                    .map(|c| self.columns.get(&(key.parent.clone(), c.clone())).cloned())
                    .collect::<Option<Vec<_>>>()?,
            ),
        ))
    }
}

fn main(schema: &Option<sqlite3_parser::ast::Name<'_>>) -> bool {
    schema
        .as_ref()
        .is_none_or(|s| name(s.0).eq_ignore_ascii_case("main"))
}
