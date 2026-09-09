use super::*;
use sqlite3_parser::ast::{fmt::ToTokens, Cmd, ColumnConstraint, CreateTableBody, Stmt, TabFlags};
use sqlite3_parser::{lexer::sql::Parser, Bump, FallibleIterator as _};

/// SQLite evaluates assignment affinity, generated expressions and defaults.
/// The private evaluator carries column semantics only; the complete result is
/// subsequently inserted into the original constrained schema atomically.
/// Defaults run on the receiver so evaluator writes cannot change their context.
pub(super) struct NativeProjectionValues<'connection> {
    receiver: &'connection Connection,
    evaluator: Connection,
    tables: BTreeMap<String, ValueTable>,
}

struct ValueTable {
    name: String,
    insert: String,
    select: String,
    defaults: BTreeMap<usize, String>,
}

struct Sql<'value, T>(&'value T);

impl<T: ToTokens> std::fmt::Display for Sql<'_, T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.to_fmt(formatter)
    }
}

impl<'connection> NativeProjectionValues<'connection> {
    pub(super) fn new(receiver: &'connection Connection) -> Result<Self, DbError> {
        Ok(Self {
            receiver,
            evaluator: Connection::open_in_memory()?,
            tables: BTreeMap::new(),
        })
    }

    pub(super) fn include(&mut self, table: &ProjectionTableRows) -> Result<(), DbError> {
        let connection = self.receiver;
        let name = format!("coven_projection_values_{}", self.tables.len());
        let create = crate::schema_introspection::create_table_sql(connection, &table.table)
            .map_err(|error| {
                DbError::context(
                    "read local relationship schema",
                    rusqlite::Error::ToSqlConversionFailure(Box::new(error)),
                )
            })?;
        let bump = Bump::new();
        let mut parser = Parser::new(&bump, create.as_bytes());
        let command = parser.next().map_err(|error| {
            DbError::context(
                "parse local relationship schema",
                rusqlite::Error::ToSqlConversionFailure(Box::new(error)),
            )
        })?;
        let Some(Cmd::Stmt(Stmt::CreateTable {
            body: CreateTableBody::ColumnsAndConstraints { columns, flags, .. },
            ..
        })) = command
        else {
            return Err(DbError::Message(format!(
                "local relationship table {:?} has no ordinary column schema",
                table.table
            )));
        };
        if parser
            .next()
            .map_err(|error| {
                DbError::context(
                    "parse local relationship schema",
                    rusqlite::Error::ToSqlConversionFailure(Box::new(error)),
                )
            })?
            .is_some()
        {
            return Err(DbError::Message(
                "local relationship schema contains multiple statements".into(),
            ));
        }
        let mut definitions = Vec::new();
        let mut defaults = BTreeMap::new();
        for (column_index, column) in columns.iter().enumerate() {
            let mut definition = column.col_name.to_string();
            if let Some(declared_type) = &column.col_type {
                definition.push(' ');
                definition.push_str(&Sql(declared_type).to_string());
            }
            for constraint in column.constraints {
                match &constraint.constraint {
                    ColumnConstraint::Collate { .. } | ColumnConstraint::Generated { .. } => {
                        definition.push(' ');
                        definition.push_str(&Sql(&constraint.constraint).to_string());
                    }
                    ColumnConstraint::Default(expression) => {
                        defaults.insert(column_index, expression.to_string());
                    }
                    ColumnConstraint::PrimaryKey { .. }
                    | ColumnConstraint::NotNull { .. }
                    | ColumnConstraint::Unique(_)
                    | ColumnConstraint::Check(_)
                    | ColumnConstraint::Defer(_)
                    | ColumnConstraint::ForeignKey { .. } => {}
                }
            }
            definitions.push(definition);
        }
        // A rowid table without an INTEGER PRIMARY KEY carries its
        // live rowid explicitly through synthetic replacement.
        if table.columns.len() == columns.len() + 1 {
            definitions.push(format!(
                "{} INTEGER",
                crate::quote_ident(&table.columns[columns.len()])
            ));
        } else if table.columns.len() != columns.len() {
            return Err(DbError::Message(
                "local relationship columns differ from their schema".into(),
            ));
        }
        let strict = if flags.contains(TabFlags::Strict) {
            " STRICT"
        } else {
            ""
        };
        self.evaluator.execute_batch(&format!(
            "CREATE TEMP TABLE {} ({}){strict}",
            crate::quote_ident(&name),
            definitions.join(", ")
        ))?;
        let writable = table
            .writable_columns
            .iter()
            .map(|index| crate::quote_ident(&table.columns[*index]))
            .collect::<Vec<_>>();
        let placeholders = (1..=writable.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert = format!(
            "INSERT INTO temp.{} ({}) VALUES ({placeholders})",
            crate::quote_ident(&name),
            writable.join(", ")
        );
        let select = format!(
            "SELECT {} FROM temp.{}",
            table
                .columns
                .iter()
                .map(|column| crate::quote_ident(column))
                .collect::<Vec<_>>()
                .join(", "),
            crate::quote_ident(&name)
        );
        self.tables.insert(
            table.table.clone(),
            ValueTable {
                name,
                insert,
                select,
                defaults,
            },
        );
        Ok(())
    }

    pub(super) fn normalize(
        &self,
        table: &ProjectionTableRows,
        row: &[Value],
    ) -> Result<Vec<Value>, DbError> {
        let value_table = &self.tables[&table.table];
        self.evaluator.execute(
            &format!("DELETE FROM temp.{}", crate::quote_ident(&value_table.name)),
            [],
        )?;
        self.evaluator.execute(
            &value_table.insert,
            rusqlite::params_from_iter(table.writable_columns.iter().map(|index| &row[*index])),
        )?;
        Ok(self.evaluator.query_row(&value_table.select, [], |row| {
            (0..table.columns.len())
                .map(|index| row.get(index))
                .collect::<rusqlite::Result<Vec<Value>>>()
        })?)
    }

    pub(super) fn default_value(
        &self,
        table: &ProjectionTableRows,
        column: usize,
    ) -> Result<Value, DbError> {
        match self.tables[&table.table].defaults.get(&column) {
            Some(expression) => {
                Ok(self
                    .receiver
                    .query_row(&format!("SELECT ({expression})"), [], |row| row.get(0))?)
            }
            None => Ok(Value::Null),
        }
    }
}
