//! INSERT names its columns in SQL; SQLite's authorizer supplies only its table.

use crate::{DbError, SyncedTable};
use sqlite3_parser::{
    ast::{Cmd, DistinctNames, InsertBody, QualifiedName, Stmt, TriggerCmd},
    lexer::sql::Parser,
    Bump, FallibleIterator,
};
use std::collections::BTreeMap;

pub(crate) struct FileAuthorization {
    columns: BTreeMap<String, [String; 2]>,
    inserts: BTreeMap<String, String>,
    triggers: BTreeMap<String, BTreeMap<String, String>>,
}

impl FileAuthorization {
    pub(crate) fn new(tables: &[SyncedTable]) -> Self {
        Self {
            columns: tables
                .iter()
                .filter_map(|t| {
                    t.files.as_ref().map(|f| {
                        (
                            t.name.to_ascii_lowercase(),
                            [f.hash.clone(), f.location.clone()],
                        )
                    })
                })
                .collect(),
            inserts: BTreeMap::new(),
            triggers: BTreeMap::new(),
        }
    }

    pub(crate) fn statement(&mut self, sql: &str) -> rusqlite::Result<()> {
        self.inserts = self.assignments(sql)?;
        Ok(())
    }

    pub(crate) fn triggers(&mut self, triggers: Vec<(String, String)>) -> rusqlite::Result<()> {
        self.triggers.clear();
        for (name, sql) in triggers {
            self.triggers
                .insert(name.to_ascii_lowercase(), self.assignments(&sql)?);
        }
        Ok(())
    }

    pub(crate) fn update(&self, table: &str, column: &str) -> Option<DbError> {
        self.columns
            .get(&table.to_ascii_lowercase())
            .and_then(|columns| columns.iter().find(|c| c.eq_ignore_ascii_case(column)))
            .map(|column| DbError::FileColumnWrite {
                table: table.into(),
                column: column.clone(),
            })
    }

    pub(crate) fn insert(&self, table: &str, trigger: Option<&str>) -> Option<DbError> {
        let assignments = match trigger {
            Some(trigger) => self.triggers.get(&trigger.to_ascii_lowercase())?,
            None => &self.inserts,
        };
        assignments
            .get(&table.to_ascii_lowercase())
            .map(|column| DbError::FileColumnWrite {
                table: table.into(),
                column: column.clone(),
            })
    }

    fn assignments(&self, sql: &str) -> rusqlite::Result<BTreeMap<String, String>> {
        let mut result = BTreeMap::new();
        if self.columns.is_empty() {
            return Ok(result);
        }
        let bump = Bump::new();
        let mut parser = Parser::new(&bump, sql.as_bytes());
        while let Some(command) = parser
            .next()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
        {
            let Cmd::Stmt(statement) = command else {
                continue;
            };
            match statement {
                Stmt::Insert {
                    tbl_name,
                    columns,
                    body,
                    ..
                } if !matches!(body, InsertBody::DefaultValues) => {
                    self.record(&tbl_name, columns.as_ref(), &mut result)
                }
                Stmt::CreateTrigger { commands, .. } => {
                    for command in commands {
                        if let TriggerCmd::Insert {
                            tbl_name,
                            col_names,
                            ..
                        } = command
                        {
                            self.record(tbl_name, col_names.as_ref(), &mut result);
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(result)
    }

    fn record(
        &self,
        table: &QualifiedName<'_>,
        columns: Option<&DistinctNames<'_>>,
        result: &mut BTreeMap<String, String>,
    ) {
        for (name, managed) in &self.columns {
            if table.name != name.as_str() {
                continue;
            }
            for column in managed {
                if columns.is_none_or(|columns| columns.iter().any(|c| c == column.as_str())) {
                    result.insert(name.clone(), column.clone());
                    break;
                }
            }
        }
    }
}
