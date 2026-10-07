//! SQLite owns the pre-migration rows; Rust retains only table and column names.

use crate::migration_names::MigrationMatch;
use crate::schema::{Schema, TableSchema};
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience, value};
use crate::write_rows::{key_columns, AppKey, AppRow, AppValues};
use crate::{DbError, SyncedTable};
use rusqlite::{params, params_from_iter};
use sqlite3_parser::{
    ast::{AlterTableBody, Cmd, ColumnConstraint, CreateTableBody, Stmt},
    lexer::sql::Parser,
    Bump, FallibleIterator,
};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct MigrationCapture {
    pub(crate) synced: BTreeSet<String>,
    pub(crate) snapshot: Option<MigrationSnapshot>,
}

pub(crate) struct MigrationSnapshot {
    tables: BTreeMap<String, SnapshotTable>,
}

struct SnapshotTable {
    temporary: String,
    columns: BTreeMap<String, String>,
}

impl MigrationCapture {
    pub(crate) fn new(db: &DatabaseConnection, tables: &[SyncedTable]) -> Result<Self, DbError> {
        // Column identities survive an empty table, and identify its old synced name
        // even when the app declares only the renamed, final schema.
        let mut synced: BTreeSet<String> = db
            .query(
                "SELECT DISTINCT lower(table_name) FROM _coven_columns",
                [],
                |r| r.get(0),
            )?
            .into_iter()
            .collect();
        synced.extend(tables.iter().map(|t| t.name.to_ascii_lowercase()));
        Ok(Self {
            synced,
            snapshot: None,
        })
    }

    pub(crate) fn before(
        &mut self,
        db: &DatabaseConnection,
        before: &Schema,
        sql: &str,
    ) -> rusqlite::Result<()> {
        if self.snapshot.is_some() {
            return Ok(());
        }
        let bump = Bump::new();
        let command = Parser::new(&bump, sql.as_bytes())
            .next()
            .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?;
        let main = |name: &Option<sqlite3_parser::ast::Name<'_>>| {
            name.as_ref()
                .is_none_or(|n| crate::removal_schema::name(n.0).eq_ignore_ascii_case("main"))
        };
        let breaking = match command {
            Some(Cmd::Stmt(Stmt::AlterTable(table, AlterTableBody::AddColumn(column)))) => {
                main(&table.db_name)
                    && self
                        .synced
                        .contains(&crate::removal_schema::name(table.name.0).to_ascii_lowercase())
                    && column.constraints.iter().any(|c| {
                        c.name.is_some()
                            || !matches!(
                                c.constraint,
                                ColumnConstraint::Default(_)
                                    | ColumnConstraint::NotNull { nullable: true, .. }
                            )
                    })
            }
            Some(Cmd::Stmt(Stmt::AlterTable(table, body))) => {
                main(&table.db_name)
                    && before.tables.contains_key(
                        &crate::removal_schema::name(table.name.0).to_ascii_lowercase(),
                    )
                    && (matches!(body, AlterTableBody::RenameTo(_))
                        || self.synced.contains(
                            &crate::removal_schema::name(table.name.0).to_ascii_lowercase(),
                        ))
            }
            Some(Cmd::Stmt(Stmt::DropTable { tbl_name, .. })) => {
                main(&tbl_name.db_name)
                    && before.tables.contains_key(
                        &crate::removal_schema::name(tbl_name.name.0).to_ascii_lowercase(),
                    )
            }
            Some(Cmd::Stmt(
                Stmt::CreateTable {
                    body: CreateTableBody::ColumnsAndConstraints { .. },
                    ..
                }
                | Stmt::CreateView { .. }
                | Stmt::DropView { .. },
            )) => false,
            Some(Cmd::Stmt(Stmt::CreateIndex { tbl_name, .. })) => {
                before
                    .tables
                    .contains_key(&crate::removal_schema::name(tbl_name.0).to_ascii_lowercase())
                    && db
                        .migration_targets(sql)?
                        .iter()
                        .any(|t| self.synced.contains(t))
            }
            Some(Cmd::Stmt(Stmt::CreateTrigger { tbl_name, .. })) => {
                before.tables.contains_key(
                    &crate::removal_schema::name(tbl_name.name.0).to_ascii_lowercase(),
                ) && db
                    .migration_targets(sql)?
                    .iter()
                    .any(|t| self.synced.contains(t))
            }
            _ => db
                .migration_targets(sql)?
                .iter()
                .any(|t| self.synced.contains(t)),
        };
        if breaking {
            self.snapshot = Some(
                MigrationSnapshot::capture(db, before, &self.synced)
                    .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?,
            );
        }
        Ok(())
    }

    pub(crate) fn discard(&mut self, db: &DatabaseConnection) -> Result<(), DbError> {
        if let Some(snapshot) = self.snapshot.take() {
            snapshot.drop(db)?;
        }
        Ok(())
    }
}

impl MigrationSnapshot {
    fn capture(
        db: &DatabaseConnection,
        schema: &Schema,
        synced: &BTreeSet<String>,
    ) -> Result<Self, DbError> {
        let mut tables = BTreeMap::new();
        for table in schema
            .tables
            .values()
            .filter(|t| synced.contains(&t.name.to_ascii_lowercase()))
        {
            if !table.indices.iter().any(|i| i.primary)
                || key_columns(table).iter().any(|c| !c.not_null)
            {
                continue;
            }
            let temporary = format!("_coven_migration_before_{}", tables.len());
            let columns: BTreeMap<_, _> = table
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name.clone(), format!("v{i}")))
                .collect();
            let projection = table
                .columns
                .iter()
                .map(|c| {
                    format!(
                        "{} AS {}",
                        identifier(&c.name),
                        identifier(&columns[&c.name])
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            db.batch(&format!("CREATE TEMP TABLE {} AS SELECT {} AS coven_key,{projection} FROM main.{}; CREATE UNIQUE INDEX temp.{} ON {}(coven_key)",identifier(&temporary),key_sql(table,""),identifier(&table.name),identifier(&format!("{temporary}_key")),identifier(&temporary)))?;
            tables.insert(table.name.clone(), SnapshotTable { temporary, columns });
        }
        Ok(Self { tables })
    }

    pub(crate) fn rename(
        &mut self,
        db: &DatabaseConnection,
        names: &MigrationMatch,
    ) -> Result<(), DbError> {
        for (old, table) in &self.tables {
            if !names.tables.contains_key(old) {
                db.batch(&format!("DROP TABLE temp.{}", identifier(&table.temporary)))?;
            }
        }
        self.tables = std::mem::take(&mut self.tables)
            .into_iter()
            .filter_map(|(old, mut table)| {
                let new = names.tables.get(&old)?;
                table.columns = table
                    .columns
                    .into_iter()
                    .filter_map(|(column, slot)| {
                        names
                            .columns
                            .get(&(old.clone(), column))
                            .map(|name| (name.clone(), slot))
                    })
                    .collect();
                Some((new.clone(), table))
            })
            .collect();
        Ok(())
    }

    pub(crate) fn row(
        &self,
        db: &DatabaseConnection,
        key: &AppKey,
    ) -> Result<Option<AppRow>, DbError> {
        let Some(table) = self.tables.get(&key.0) else {
            return Ok(None);
        };
        let mut rows = Vec::new();
        db.visit(
            &format!(
                "SELECT {} FROM temp.{} WHERE coven_key=?1",
                table.projection(),
                identifier(&table.temporary)
            ),
            [&key.1],
            |r| {
                rows.push(table.values(r)?);
                Ok(())
            },
        )?;
        let Some(values) = rows.pop() else {
            return Ok(None);
        };
        let owners = db.query("SELECT r.audience,r.generation FROM _coven_rows r WHERE r.table_name=?1 AND r.key=?2 AND NOT EXISTS(SELECT 1 FROM _coven_rows newer WHERE newer.table_name=r.table_name AND newer.key=r.key AND newer.audience=r.audience AND newer.generation>r.generation) AND NOT EXISTS(SELECT 1 FROM _coven_lost l WHERE l.table_name=r.table_name AND l.key=r.key AND l.audience=r.audience AND l.generation=r.generation AND l.retired=0 AND l.replacement_kind='rules')",params![key.0,key.1],|r|Ok((r.get::<_,String>(0)?,crate::write_encoding::counter(r.get(1)?))))?;
        let mut owners = owners
            .into_iter()
            .filter(|(_, generation)| generation % 2 == 1);
        let owner = owners.next().map(|(audience, _)| audience);
        assert!(
            owners.next().is_none(),
            "one visible audience per migration key"
        );
        // Rows without merge records were not synced before this migration.
        Ok(owner
            .map(|owner| {
                Ok::<_, rusqlite::Error>(AppRow {
                    values,
                    audience: audience(&owner)?,
                    parents: BTreeMap::new(),
                })
            })
            .transpose()?)
    }

    pub(crate) fn find(
        &self,
        db: &DatabaseConnection,
        table: &TableSchema,
        columns: &[String],
        values: &[coven_format::value::Value],
    ) -> Result<Vec<AppValues>, DbError> {
        let Some(snapshot) = self.tables.get(&table.name) else {
            return Ok(Vec::new());
        };
        let Some(slots) = columns
            .iter()
            .map(|c| snapshot.columns.get(c))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(Vec::new());
        };
        let values = crate::removal_sql::reference_values(db, table, columns, values)?;
        let index = table
            .indices
            .iter()
            .find(|i| {
                i.columns.len() == columns.len()
                    && i.columns
                        .iter()
                        .all(|c| c.as_ref().is_some_and(|c| columns.contains(c)))
            })
            .expect("reference targets a unique index");
        let collations: Vec<_> = columns
            .iter()
            .map(|c| {
                let position = index
                    .columns
                    .iter()
                    .position(|n| n.as_ref() == Some(c))
                    .expect("target column");
                &index.collations[position]
            })
            .collect();
        let terms: Vec<_> = slots
            .iter()
            .zip(&collations)
            .map(|(c, collation)| format!("{} COLLATE {}", identifier(c), identifier(collation)))
            .collect();
        let lookup = format!(
            "{}_lookup_{}",
            snapshot.temporary,
            slots
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join("_")
        );
        db.batch(&format!(
            "CREATE INDEX IF NOT EXISTS temp.{} ON {}({})",
            identifier(&lookup),
            identifier(&snapshot.temporary),
            terms.join(",")
        ))?;
        db.query(
            &format!(
                "SELECT {} FROM temp.{} WHERE {}",
                snapshot.projection(),
                identifier(&snapshot.temporary),
                terms
                    .iter()
                    .map(|term| format!("{term}=?"))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            params_from_iter(values.iter().map(crate::write_encoding::sql_value)),
            |r| snapshot.values(r),
        )
    }

    pub(crate) fn differences(
        &self,
        db: &DatabaseConnection,
        schema: &crate::write_schema::WriteSchema,
        references: &BTreeMap<String, BTreeSet<String>>,
    ) -> Result<BTreeMap<AppKey, BTreeSet<String>>, DbError> {
        let mut changed = BTreeMap::new();
        for declaration in &schema.declarations {
            let table = schema.table(&declaration.name);
            let key = key_sql(table, "a.");
            let snapshot = self.tables.get(&table.name);
            let tests: Vec<_> = table
                .columns
                .iter()
                .map(|column| {
                    if references
                        .get(&table.name)
                        .is_some_and(|cs| cs.contains(&column.name))
                    {
                        return "1".to_owned();
                    }
                    match snapshot.and_then(|s| s.columns.get(&column.name)) {
                        Some(slot) => format!(
                            "(typeof(a.{0})<>typeof(b.{1}) OR NOT (a.{0} IS b.{1} COLLATE BINARY))",
                            identifier(&column.name),
                            identifier(slot)
                        ),
                        None => "1".into(),
                    }
                })
                .collect();
            let join = snapshot
                .map(|s| {
                    format!(
                        "LEFT JOIN temp.{} b ON b.coven_key={key}",
                        identifier(&s.temporary)
                    )
                })
                .unwrap_or_default();
            let absent = if snapshot.is_some() {
                "b.coven_key IS NULL"
            } else {
                "1"
            };
            let sql = format!(
                "SELECT {key},{} FROM main.{} a {join} WHERE {absent} OR {}",
                tests.join(","),
                identifier(&table.name),
                tests.join(" OR ")
            );
            db.visit(&sql, [], |r| {
                let columns = table
                    .columns
                    .iter()
                    .enumerate()
                    .filter_map(|(i, c)| match r.get::<_, bool>(i + 1) {
                        Ok(true) => Some(Ok(c.name.clone())),
                        Ok(false) => None,
                        Err(e) => Some(Err(e)),
                    })
                    .collect::<rusqlite::Result<BTreeSet<_>>>()?;
                changed.insert((table.name.clone(), r.get(0)?), columns);
                Ok(())
            })?;
            if let Some(snapshot) = snapshot {
                db.batch(&format!("CREATE TEMP TABLE _coven_migration_keys(coven_key BLOB PRIMARY KEY) WITHOUT ROWID; INSERT INTO _coven_migration_keys SELECT {key} FROM main.{} a",identifier(&table.name)))?;
                db.visit(&format!("SELECT b.coven_key FROM temp.{} b LEFT JOIN temp._coven_migration_keys a USING(coven_key) WHERE a.coven_key IS NULL",identifier(&snapshot.temporary)),[],|r| { changed.insert((table.name.clone(),r.get(0)?),BTreeSet::new()); Ok(()) })?;
                db.batch("DROP TABLE temp._coven_migration_keys")?;
            }
        }
        Ok(changed)
    }

    pub(crate) fn drop(self, db: &DatabaseConnection) -> Result<(), DbError> {
        for table in self.tables.into_values() {
            db.batch(&format!("DROP TABLE temp.{}", identifier(&table.temporary)))?;
        }
        Ok(())
    }
}

impl SnapshotTable {
    fn projection(&self) -> String {
        if self.columns.is_empty() {
            return "NULL".into();
        }
        self.columns
            .values()
            .map(|s| identifier(s))
            .collect::<Vec<_>>()
            .join(",")
    }
    fn values(&self, row: &rusqlite::Row<'_>) -> rusqlite::Result<AppValues> {
        self.columns
            .keys()
            .enumerate()
            .map(|(i, c)| Ok((c.clone(), value(row.get_ref(i)?)?)))
            .collect()
    }
}

fn key_sql(table: &TableSchema, prefix: &str) -> String {
    let primary = table
        .indices
        .iter()
        .find(|i| i.primary)
        .expect("synced primary index");
    format!(
        "coven_migration_key({})",
        key_columns(table)
            .iter()
            .zip(&primary.collations)
            .map(|(c, collation)| format!(
                "'{}',{prefix}{}",
                collation.replace('\'', "''"),
                identifier(&c.name)
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
}
