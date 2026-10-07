//! Removal constraints from SQLite's schema AST, with original expression text.

use crate::schema::{SchemaIndex, TableSchema};
use crate::schema_source::SchemaSource;
use crate::sql::identifier;
use sqlite3_parser::{
    ast::{
        Cmd, ColumnConstraint, CreateTableBody, Expr, Name, SortedColumn, Stmt, TableConstraint,
    },
    lexer::sql::Parser,
    Bump, FallibleIterator,
};

pub(crate) struct TableRules {
    pub(crate) columns: Vec<String>,
    pub(crate) checks: Vec<(String, String)>,
    pub(crate) unique: Vec<UniqueSql>,
}

pub(crate) struct UniqueSql {
    pub(crate) identity: coven_merge::UniqueConstraint,
    pub(crate) collations: Vec<String>,
    pub(crate) dependencies: Vec<String>,
    pub(crate) expressions: Vec<String>,
    pub(crate) predicate: String,
}

struct AutomaticUnique {
    terms: Vec<String>,
    columns: Vec<String>,
    collations: Vec<String>,
}

impl TableRules {
    pub(crate) fn read(
        db: &crate::sqlite::DatabaseConnection,
        table: &TableSchema,
    ) -> Result<Self, crate::DbError> {
        let bump = Bump::new();
        let Stmt::CreateTable {
            body:
                CreateTableBody::ColumnsAndConstraints {
                    columns: definitions,
                    constraints,
                    ..
                },
            ..
        } = parse(&bump, &table.sql)
        else {
            panic!("stored table is not CREATE TABLE")
        };
        let sources = SchemaSource::new(&table.sql).body().parts();
        let mut sources = sources.into_iter();
        let mut columns = Vec::new();
        let mut checks = Vec::new();
        let mut automatic = Vec::new();
        let mut column_collations = std::collections::BTreeMap::new();
        for definition in &definitions {
            let column = table
                .columns
                .iter()
                .find(|c| definition.col_name == c.name.as_str())
                .expect("schema column");
            let mut source = sources.next().expect("column source");
            let mut sql = format!("{} {}", identifier(&column.name), identifier(&column.kind));
            let mut collation = "BINARY".to_owned();
            let mut unique = false;
            for constraint in definition.constraints {
                match &constraint.constraint {
                    ColumnConstraint::Check(_) => {
                        let expression = source.node_body(&constraint.constraint).text().to_owned();
                        checks.push((check_name(&constraint.name, &expression), expression));
                    }
                    ColumnConstraint::Collate { collation_name } => {
                        collation = name(collation_name.0);
                        sql.push_str(&format!(" COLLATE {}", identifier(&collation)));
                    }
                    ColumnConstraint::Generated { .. } => {
                        let expression = source.node_body(&constraint.constraint);
                        sql.push_str(&format!(" AS ({})", expression.text()));
                    }
                    ColumnConstraint::Unique(_) => unique = true,
                    _ => {}
                }
            }
            if unique {
                automatic.push(AutomaticUnique {
                    terms: vec![column.name.clone()],
                    columns: vec![column.name.clone()],
                    collations: vec![collation.clone()],
                });
            }
            column_collations.insert(column.name.clone(), collation);
            columns.push(sql);
        }
        // SQLite also accepts table constraints without commas between them.
        let remainder = sources.map(|s| s.text()).collect::<Vec<_>>().join(",");
        let mut source = SchemaSource::new(&remainder);
        for constraint in constraints.into_iter().flatten() {
            match &constraint.constraint {
                TableConstraint::Check(_, _) => {
                    let expression = source.node_body(&constraint.constraint).text().to_owned();
                    checks.push((check_name(&constraint.name, &expression), expression));
                }
                TableConstraint::Unique { columns, .. } => {
                    let body = source.node_body(&constraint.constraint);
                    let terms = terms(table, columns, &body);
                    let (columns, collations) = columns
                        .iter()
                        .map(|term| {
                            let (column, explicit) = automatic_column(table, &term.expr);
                            let collation =
                                explicit.unwrap_or_else(|| column_collations[&column].clone());
                            (column, collation)
                        })
                        .unzip();
                    automatic.push(AutomaticUnique {
                        terms,
                        columns,
                        collations,
                    });
                }
                _ => {}
            }
        }
        // Distinct declarations can share one SQLite autoindex. Keep their
        // identities separately, using the index only to find visible claims.
        let mut indexed = std::collections::BTreeMap::new();
        for unique in automatic {
            let index = table
                .indices
                .iter()
                .find(|index| index.sql.is_none() && unique.matches(index))
                .expect("UNIQUE declaration's automatic index");
            indexed.insert(
                coven_merge::UniqueConstraint {
                    terms: unique.terms,
                    partial: None,
                },
                index,
            );
        }
        for index in &table.indices {
            let Some(sql) = &index.sql else { continue };
            let Stmt::CreateIndex {
                columns,
                where_clause,
                ..
            } = parse(&bump, sql)
            else {
                panic!("stored index is not CREATE INDEX")
            };
            let mut source = SchemaSource::new(sql);
            let body = source.body();
            indexed.insert(
                coven_merge::UniqueConstraint {
                    terms: terms(table, columns, &body),
                    partial: where_clause.map(|_| source.trailing_expression().to_owned()),
                },
                index,
            );
        }
        let unique = indexed
            .into_iter()
            .map(|(identity, index)| {
                let coven_merge::UniqueConstraint { terms, partial } = identity;
                let expressions: Vec<_> = terms
                    .iter()
                    .zip(&index.columns)
                    .zip(&index.collations)
                    .map(|((term, column), collation)| {
                        let expression = match column {
                            Some(column) => identifier(column),
                            None => term.clone(),
                        };
                        format!("({expression}) COLLATE {}", identifier(collation))
                    })
                    .collect();
                let sql = format!(
                    "SELECT {} FROM main.{}",
                    expressions.join(","),
                    identifier(&table.name)
                );
                let predicate = partial.clone().unwrap_or_else(|| "1".into());
                Ok(UniqueSql {
                    identity: coven_merge::UniqueConstraint { terms, partial },
                    dependencies: db.columns_read(&format!("{sql} WHERE {predicate}"))?,
                    collations: index.collations.clone(),
                    expressions,
                    predicate,
                })
            })
            .collect::<Result<_, crate::DbError>>()?;
        Ok(Self {
            columns,
            checks,
            unique,
        })
    }
}

impl AutomaticUnique {
    fn matches(&self, index: &SchemaIndex) -> bool {
        self.columns.len() == index.columns.len()
            && self
                .columns
                .iter()
                .zip(&index.columns)
                .all(|(a, b)| b.as_ref().is_some_and(|b| a.eq_ignore_ascii_case(b)))
            && self
                .collations
                .iter()
                .zip(&index.collations)
                .all(|(a, b)| a.eq_ignore_ascii_case(b))
    }
}

fn parse<'a>(bump: &'a Bump, sql: &'a str) -> Stmt<'a> {
    let mut parser = Parser::new(bump, sql.as_bytes());
    let Cmd::Stmt(stmt) = parser
        .next()
        .unwrap_or_else(|e| panic!("sqlite3-parser rejected SQLite's accepted schema {sql:?}: {e}"))
        .expect("stored schema statement")
    else {
        panic!("stored schema contains EXPLAIN")
    };
    assert!(parser.next().expect("end of stored schema").is_none());
    stmt
}

fn terms(
    table: &TableSchema,
    columns: &[SortedColumn<'_>],
    source: &SchemaSource<'_>,
) -> Vec<String> {
    let parts = source.parts();
    assert_eq!(
        columns.len(),
        parts.len(),
        "AST index terms and source ranges"
    );
    columns
        .iter()
        .zip(parts)
        .map(|(term, source)| match column(table, &term.expr) {
            Some(column) => column,
            None => source
                .without_suffix(
                    usize::from(term.order.is_some()) + if term.nulls.is_some() { 2 } else { 0 },
                )
                .to_owned(),
        })
        .collect()
}

fn column(table: &TableSchema, expr: &Expr<'_>) -> Option<String> {
    let id = match expr {
        Expr::Id(id) => id.0,
        Expr::Name(name) => name.0,
        _ => return None,
    };
    // TRUE/FALSE are parsed as identifiers; SQLite resolves them as literals
    // unless the table actually has a column with that name.
    table
        .columns
        .iter()
        .find(|c| Name(id) == c.name.as_str())
        .map(|c| c.name.clone())
}

fn automatic_column(table: &TableSchema, mut expr: &Expr<'_>) -> (String, Option<String>) {
    let mut collation = None;
    loop {
        match expr {
            Expr::Collate(inner, name_text) => {
                if collation.is_none() {
                    collation = Some(name(name_text));
                }
                expr = inner;
            }
            Expr::Parenthesized(inner) if inner.len() == 1 => expr = &inner[0],
            _ => {
                return (
                    column(table, expr).expect("SQLite automatic unique index column"),
                    collation,
                )
            }
        }
    }
}

fn check_name(named: &Option<Name<'_>>, expression: &str) -> String {
    match named {
        Some(named) => name(named.0),
        None => expression.to_owned(),
    }
}

pub(crate) fn name(raw: &str) -> String {
    match raw.as_bytes()[0] {
        b'[' => raw[1..raw.len() - 1].to_owned(),
        quote @ (b'\'' | b'"' | b'`') => raw[1..raw.len() - 1].replace(
            &format!("{0}{0}", quote as char),
            &(quote as char).to_string(),
        ),
        _ => raw.to_owned(),
    }
}

#[cfg(test)]
#[path = "removal_schema_tests.rs"]
mod tests;
