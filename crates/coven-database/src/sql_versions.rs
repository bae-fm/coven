//! The version of each app collation and deterministic function the store's
//! indexes were built with, and the rebuild a new version takes (§4).

use std::collections::{BTreeMap, BTreeSet};

use sqlite3_parser::ast::{ColumnConstraint, CreateTableBody, Stmt, TableConstraint};
use sqlite3_parser::Bump;

use crate::sql::{identifier, stored_statement, tokens, Token};
use crate::sqlite::DatabaseConnection;
use crate::DbError;

/// What a version belongs to. SQLite keeps collations and functions apart,
/// so one name can be both.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Versioned {
    Collation,
    Function,
}

impl Versioned {
    fn kind(self) -> &'static str {
        match self {
            Self::Collation => "collation",
            Self::Function => "function",
        }
    }
}

/// A registered collation's or deterministic function's version. The name
/// is ASCII lowercase, as SQLite compares names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlVersion {
    pub(crate) kind: Versioned,
    pub(crate) name: String,
    pub(crate) version: u32,
}

/// At a writable open, before the app's migrations: rebuild the indexes
/// that use a collation or deterministic function registered at another
/// version than the one recorded, or at none recorded, and record the
/// registered versions, in one transaction (§4). What decides which rows
/// every device keeps, or keeps a computed value, refuses the change, since
/// coven rebuilds only indexes; so does SQLite, when a unique index rebuilt
/// in a new order holds two equal values. Either way nothing changes.
pub(crate) fn rebuild(
    db: &DatabaseConnection,
    synced: &[crate::SyncedTable],
    registered: &[SqlVersion],
) -> Result<(), DbError> {
    let recorded = recorded(db)?;
    let changed: Vec<&SqlVersion> = registered
        .iter()
        .filter(|v| recorded.get(&(v.kind, v.name.clone())) != Some(&v.version))
        .collect();
    if changed.is_empty() {
        return Ok(());
    }
    let changes = |(kind, name): &(Versioned, String)| {
        changed
            .iter()
            .any(|version| version.kind == *kind && version.name == *name)
    };
    let synced = |table: &str| synced.iter().any(|t| t.name.eq_ignore_ascii_case(table));
    db.transaction(|db| {
        let mut rebuilt = Vec::new();
        for object in app_objects(db)? {
            let parsed = ObjectSql::parse(&object);
            let mut uses = BTreeSet::new();
            for expression in &parsed.expressions {
                uses.extend(expression_uses(db, &object.table, expression)?);
            }
            // A CHECK or a synced table's unique index decides which rows
            // every device keeps (§9.2, §9.3): a device at the old version
            // would decide differently. A generated column keeps what the
            // old version computed.
            let decides = match parsed.kind {
                ObjectKind::Table => true,
                ObjectKind::Index { unique } => unique && synced(&object.table),
            };
            if decides {
                if let Some((_, name)) = uses.iter().find(|used| changes(used)) {
                    return Err(DbError::VersionChangeRefused {
                        name: name.clone(),
                        table: object.table,
                    });
                }
            }
            // A collation's indexes are rebuilt by its name, below.
            if matches!(parsed.kind, ObjectKind::Index { .. })
                && uses
                    .iter()
                    .any(|used| used.0 == Versioned::Function && changes(used))
            {
                rebuilt.push(object.name);
            }
        }
        // SQLite rebuilds every index in a collation, a table's own order
        // included when its primary key is in it.
        for collation in changed.iter().filter(|v| v.kind == Versioned::Collation) {
            db.batch(&format!("REINDEX {}", identifier(&collation.name)))?;
        }
        for index in rebuilt {
            // Qualified, so SQLite never reads the name as a collation's.
            db.batch(&format!("REINDEX main.{}", identifier(&index)))?;
        }
        for version in &changed {
            db.internal_execute(
                "INSERT INTO _coven_sql_versions(kind,name,version) VALUES(?1,?2,?3)
                 ON CONFLICT(kind,name) DO UPDATE SET version=excluded.version",
                (version.kind.kind(), &version.name, version.version),
            )?;
        }
        Ok(())
    })
}

/// At a read-only open, which can't rebuild: every registered collation
/// and deterministic function the store's indexes were built with is at
/// the version they were built with. A name with no recorded version is
/// one nothing in the store uses yet (§4).
pub(crate) fn require_built(
    db: &DatabaseConnection,
    registered: &[SqlVersion],
) -> Result<(), DbError> {
    let recorded = recorded(db)?;
    for version in registered {
        if let Some(&built) = recorded.get(&(version.kind, version.name.clone())) {
            if built != version.version {
                return Err(DbError::VersionNotRebuilt {
                    name: version.name.clone(),
                    built,
                    registered: version.version,
                });
            }
        }
    }
    Ok(())
}

fn recorded(db: &DatabaseConnection) -> Result<BTreeMap<(Versioned, String), u32>, DbError> {
    db.query(
        "SELECT kind,name,version FROM _coven_sql_versions",
        [],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get(2)?)),
    )?
    .into_iter()
    .map(|(kind, name, version)| {
        let kind = match kind.as_str() {
            "collation" => Versioned::Collation,
            "function" => Versioned::Function,
            _ => return Err(DbError::DamagedDatabase),
        };
        Ok(((kind, name), version))
    })
    .collect()
}

/// One of the app's tables or indexes, as SQLite stores it.
struct AppObject {
    name: String,
    table: String,
    sql: String,
}

/// The app's tables and the indexes it declared; SQLite's own and coven's
/// use none of the app's collations or functions.
fn app_objects(db: &DatabaseConnection) -> Result<Vec<AppObject>, DbError> {
    Ok(db
        .query(
            "SELECT name,tbl_name,sql FROM main.sqlite_schema
             WHERE type IN ('table','index') AND sql IS NOT NULL",
            [],
            |r| {
                Ok(AppObject {
                    name: r.get(0)?,
                    table: r.get(1)?,
                    sql: r.get(2)?,
                })
            },
        )?
        .into_iter()
        .filter(|object| {
            !crate::internal_schema::reserved_name(&object.table)
                && !object.table.to_ascii_lowercase().starts_with("sqlite_")
        })
        .collect())
}

enum ObjectKind {
    Table,
    Index { unique: bool },
}

/// The expressions of a table or index that can use the app's collations
/// and functions: a table's CHECKs and generated columns, and an index's
/// terms and condition.
struct ObjectSql {
    kind: ObjectKind,
    expressions: Vec<String>,
}

impl ObjectSql {
    fn parse(object: &AppObject) -> Self {
        let bump = Bump::new();
        let mut expressions = Vec::new();
        let kind = match stored_statement(&bump, &object.sql) {
            Stmt::CreateTable {
                body:
                    CreateTableBody::ColumnsAndConstraints {
                        columns,
                        constraints,
                        ..
                    },
                ..
            } => {
                for definition in &columns {
                    for constraint in definition.constraints {
                        if let ColumnConstraint::Check(expr)
                        | ColumnConstraint::Generated { expr, .. } = &constraint.constraint
                        {
                            expressions.push(expr.to_string());
                        }
                    }
                }
                for constraint in constraints.into_iter().flatten() {
                    if let TableConstraint::Check(expr, _) = &constraint.constraint {
                        expressions.push(expr.to_string());
                    }
                }
                ObjectKind::Table
            }
            // A table made from a SELECT declares neither, and a virtual
            // table's module keeps its own.
            Stmt::CreateTable { .. } | Stmt::CreateVirtualTable { .. } => ObjectKind::Table,
            Stmt::CreateIndex {
                unique,
                columns,
                where_clause,
                ..
            } => {
                expressions.extend(columns.iter().map(|term| term.expr.to_string()));
                expressions.extend(where_clause.map(|condition| condition.to_string()));
                ObjectKind::Index { unique }
            }
            _ => panic!("stored table or index is neither CREATE TABLE nor CREATE INDEX"),
        };
        Self { kind, expressions }
    }
}

/// The collations and functions an expression on `table` uses, as SQLite
/// resolves them while preparing it: each function it calls (`x REGEXP y`
/// calls `regexp`), each collation it names, and the collation of each
/// column it reads.
fn expression_uses(
    db: &DatabaseConnection,
    table: &str,
    expression: &str,
) -> Result<BTreeSet<(Versioned, String)>, DbError> {
    let reads = db.reads(&format!(
        "SELECT ({expression}) FROM main.{}",
        identifier(table)
    ))?;
    let mut uses: BTreeSet<_> = reads
        .functions
        .into_iter()
        .map(|name| (Versioned::Function, name))
        .collect();
    for column in &reads.columns {
        let collation = db.column_collation(table, column)?;
        uses.insert((Versioned::Collation, collation.to_ascii_lowercase()));
    }
    for pair in tokens(expression)?.windows(2) {
        if let [collate, Token::Word(name) | Token::Quoted(name)] = pair {
            if collate.word("collate") {
                uses.insert((Versioned::Collation, name.to_ascii_lowercase()));
            }
        }
    }
    Ok(uses)
}

#[cfg(test)]
#[path = "sql_versions_tests.rs"]
mod tests;
