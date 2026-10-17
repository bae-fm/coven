//! Synced keys compare text with the same byte identities as the merge.

use super::{Schema, TableSchema};
use crate::{DbError, SchemaError};
use std::collections::BTreeSet;

impl Schema {
    /// Coven compares key text by bytes, as the merge model compares rows.
    /// A collation that lets SQLite hold two spellings as one key would give one
    /// SQLite row two identities in the merge, so every column a synced key,
    /// unique constraint or foreign key compares, and every unique index term,
    /// must be BINARY.
    pub(super) fn validate_key_collations(
        &self,
        declared: &BTreeSet<String>,
    ) -> Result<(), DbError> {
        let binary = |table: &TableSchema, column: &str, collation: &str| {
            if collation.eq_ignore_ascii_case("BINARY") {
                return Ok(());
            }
            Err(SchemaError::KeyCollation {
                table: table.name.clone(),
                column: column.to_owned(),
                collation: collation.to_owned(),
            })
        };
        let declared_binary = |table: &TableSchema, name: &str| match table
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
        {
            Some(column) => binary(table, &column.name, &column.collation),
            None => Ok(()),
        };
        for table in self.tables.values() {
            let synced = declared.contains(&table.name.to_ascii_lowercase());
            if synced {
                for column in table.columns.iter().filter(|c| c.primary_key > 0) {
                    binary(table, &column.name, &column.collation)?;
                }
                for index in &table.indices {
                    for (column, collation) in index.columns.iter().zip(&index.collations) {
                        binary(table, column.as_deref().unwrap_or(&index.name), collation)?;
                        if let Some(column) = column {
                            declared_binary(table, column)?;
                        }
                    }
                }
            }
            for key in &table.foreign_keys {
                let parent = self.tables.get(&key.target.to_ascii_lowercase());
                if !synced && !declared.contains(&key.target.to_ascii_lowercase()) {
                    continue;
                }
                for column in &key.columns {
                    declared_binary(table, column)?;
                }
                let Some(parent) = parent else { continue };
                let primary = crate::write_rows::key_columns(parent);
                for (position, column) in key.target_columns.iter().enumerate() {
                    // A key that names no parent columns references the primary key.
                    let column = column
                        .as_deref()
                        .or_else(|| primary.get(position).map(|c| c.name.as_str()));
                    if let Some(column) = column {
                        declared_binary(parent, column)?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "schema_collation_tests.rs"]
mod tests;
