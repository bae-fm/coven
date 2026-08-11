use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read};
use std::sync::{Arc, Mutex};

use fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::hooks::{Action, AuthAction, AuthContext};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OptionalExtension};
use sqlite3_parser::ast::{
    As, Cmd, Expr, Literal, OneSelect, Operator, ResultColumn, SelectTable, Stmt,
};
use sqlite3_parser::lexer::sql::Parser;
use sqlite3_parser::{Bump, FallibleIterator as _};

use crate::{is_reserved_table_name, DbError};

#[path = "live_query_change_capture.rs"]
mod change_capture;
pub(crate) use change_capture::ChangeCapture;

thread_local! {
    static AUTHORIZER_READ_CAPTURE: std::cell::RefCell<Option<ReadDependencyCapture>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) fn begin_authorizer_read_capture(capture: ReadDependencyCapture) {
    AUTHORIZER_READ_CAPTURE.with(|slot| {
        let replaced = slot.borrow_mut().replace(capture);
        assert!(replaced.is_none(), "read dependency authorizer nested");
    });
}

pub(crate) fn authorize_tracked_host_sql(
    context: AuthContext<'_>,
) -> rusqlite::hooks::Authorization {
    AUTHORIZER_READ_CAPTURE.with(|slot| {
        if let Some(capture) = slot.borrow().as_ref() {
            capture.authorize(context);
        }
    });
    crate::authorize_host_sql(context)
}

pub(crate) fn end_authorizer_read_capture() {
    AUTHORIZER_READ_CAPTURE.with(|slot| {
        slot.borrow_mut()
            .take()
            .expect("read dependency authorizer was not active");
    });
}

#[derive(Clone, Debug, PartialEq)]
enum SqlValue {
    Null,
    Integer(i64),
    Real(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl SqlValue {
    fn from_ref(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Real(value) => Self::Real(value.to_bits()),
            ValueRef::Text(value) => Self::Text(value.to_vec()),
            ValueRef::Blob(value) => Self::Blob(value.to_vec()),
        }
    }

    fn compare(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Integer(left), Self::Integer(right)) => Some(left.cmp(right)),
            (Self::Text(left), Self::Text(right)) => Some(left.cmp(right)),
            (Self::Blob(left), Self::Blob(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RowOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug)]
struct RowMutation {
    table: String,
    operation: RowOperation,
    old_key: BTreeMap<String, SqlValue>,
    new_key: BTreeMap<String, SqlValue>,
    changed_columns: BTreeSet<String>,
}

#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct CommittedChanges {
    rows: Vec<RowMutation>,
    schema_changed: bool,
    unknown: bool,
}

impl CommittedChanges {
    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty() && !self.schema_changed && !self.unknown
    }

    pub(crate) fn unknown() -> Self {
        Self {
            unknown: true,
            ..Self::default()
        }
    }

    pub(crate) fn mark_schema_changed(&mut self) {
        self.schema_changed = true;
    }
}

#[derive(Debug)]
struct TableColumns {
    names: Vec<String>,
}

fn table_columns(
    connection: &Connection,
    table: &str,
    number_of_columns: usize,
    primary_key: &[u8],
) -> Result<TableColumns, DbError> {
    let mut statement = connection
        .prepare("SELECT name FROM pragma_table_xinfo(?1) ORDER BY cid")
        .map_err(DbError::from)?;
    let mut names = statement
        .query_map([table], |row| row.get::<_, String>(0))
        .map_err(DbError::from)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(DbError::from)?;
    if number_of_columns == names.len() + 1 && primary_key.first() == Some(&1) {
        names.insert(0, "_rowid_".to_string());
    }
    if names.len() != number_of_columns {
        return Err(DbError::Message(format!(
            "live-query changeset for table {table:?} has {number_of_columns} columns but its schema has {}",
            names.len()
        )));
    }
    Ok(TableColumns {
        names: names.into_iter().map(|name| normalize(&name)).collect(),
    })
}

pub(crate) fn decode_changeset(
    connection: &Connection,
    bytes: &[u8],
) -> Result<CommittedChanges, DbError> {
    if bytes.is_empty() {
        return Ok(CommittedChanges::default());
    }
    let mut cursor = Cursor::new(bytes);
    let input: &mut dyn Read = &mut cursor;
    let mut iterator =
        rusqlite::session::ChangesetIter::start_strm(&input).map_err(DbError::from)?;
    let mut committed = CommittedChanges::default();
    let mut schemas = BTreeMap::<String, TableColumns>::new();
    while let Some(item) = iterator.next().map_err(DbError::from)? {
        let operation = item.op().map_err(DbError::from)?;
        let table = normalize(operation.table_name());
        if is_reserved_table_name(&table) || table.starts_with("sqlite_") {
            continue;
        }
        let primary_key = item.pk().map_err(DbError::from)?;
        let column_count = usize::try_from(operation.number_of_columns())?;
        if primary_key.len() != column_count {
            return Err(DbError::Message(format!(
                "live-query changeset for table {table:?} has {column_count} columns but {} primary-key flags",
                primary_key.len()
            )));
        }
        if !schemas.contains_key(&table) {
            schemas.insert(
                table.clone(),
                table_columns(connection, &table, column_count, primary_key)?,
            );
        }
        let columns = &schemas[&table];
        let row_operation = match operation.code() {
            Action::SQLITE_INSERT => RowOperation::Insert,
            Action::SQLITE_UPDATE => RowOperation::Update,
            Action::SQLITE_DELETE => RowOperation::Delete,
            action => {
                return Err(DbError::Message(format!(
                    "unexpected live-query changeset action {action:?}"
                )))
            }
        };
        let mut old_key = BTreeMap::new();
        let mut new_key = BTreeMap::new();
        let mut changed_columns = BTreeSet::new();
        for (index, name) in columns.names.iter().enumerate() {
            let is_key = primary_key[index] != 0;
            let old = match row_operation {
                RowOperation::Insert => None,
                RowOperation::Update | RowOperation::Delete => {
                    optional_changeset_value(item.old_value(index))?
                }
            };
            let mut new = match row_operation {
                RowOperation::Delete => None,
                RowOperation::Insert | RowOperation::Update => {
                    optional_changeset_value(item.new_value(index))?
                }
            };
            if is_key && row_operation == RowOperation::Update && new.is_none() {
                new = old.clone();
            }
            if is_key {
                if let Some(value) = old.clone() {
                    old_key.insert(name.clone(), value);
                }
                if let Some(value) = new.clone() {
                    new_key.insert(name.clone(), value);
                }
            }
            let changed = match row_operation {
                RowOperation::Insert | RowOperation::Delete => true,
                RowOperation::Update if is_key => old != new,
                RowOperation::Update => old.is_some() || new.is_some(),
            };
            if changed {
                changed_columns.insert(name.clone());
            }
        }
        committed.rows.push(RowMutation {
            table,
            operation: row_operation,
            old_key,
            new_key,
            changed_columns,
        });
    }
    Ok(committed)
}

fn optional_changeset_value(
    value: rusqlite::Result<ValueRef<'_>>,
) -> Result<Option<SqlValue>, DbError> {
    match value {
        Ok(value) => Ok(Some(SqlValue::from_ref(value))),
        Err(rusqlite::Error::InvalidColumnIndex(_)) => Ok(None),
        Err(error) => Err(DbError::from(error)),
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReadDependencyCapture {
    state: Arc<Mutex<ReadCaptureState>>,
}

#[derive(Debug, Default)]
struct ReadCaptureState {
    current: BTreeMap<String, TableRead>,
    statements: Vec<StatementRead>,
}

#[derive(Clone, Debug, Default)]
struct TableRead {
    columns: BTreeSet<String>,
    reads_existence: bool,
}

#[derive(Debug)]
struct StatementRead {
    tables: BTreeMap<String, TableRead>,
    expanded_sql: Option<String>,
}

impl ReadDependencyCapture {
    pub(crate) fn begin_statement(&self) {
        let mut state = self.state.lock().expect("read dependency mutex poisoned");
        if !state.current.is_empty() {
            let tables = std::mem::take(&mut state.current);
            state.statements.push(StatementRead {
                tables,
                expanded_sql: None,
            });
        }
    }

    pub(crate) fn finish_statement(&self, expanded_sql: Option<String>) {
        let mut state = self.state.lock().expect("read dependency mutex poisoned");
        let tables = std::mem::take(&mut state.current);
        state.statements.push(StatementRead {
            tables,
            expanded_sql,
        });
    }

    pub(crate) fn authorize(&self, context: AuthContext<'_>) {
        let AuthAction::Read {
            table_name,
            column_name,
        } = context.action
        else {
            return;
        };
        if context
            .database_name
            .is_some_and(|database| !database.eq_ignore_ascii_case("main"))
            || is_reserved_table_name(table_name)
            || table_name.starts_with("sqlite_")
        {
            return;
        }
        let mut state = self.state.lock().expect("read dependency mutex poisoned");
        let table = state.current.entry(normalize(table_name)).or_default();
        if column_name.is_empty() {
            table.reads_existence = true;
        } else {
            table.columns.insert(normalize(column_name));
        }
    }

    pub(crate) fn dependencies(
        self,
        connection: &Connection,
    ) -> Result<QueryDependencies, DbError> {
        let mut state = self.state.lock().expect("read dependency mutex poisoned");
        if !state.current.is_empty() {
            let tables = std::mem::take(&mut state.current);
            state.statements.push(StatementRead {
                tables,
                expanded_sql: None,
            });
        }
        let statements = std::mem::take(&mut state.statements);
        drop(state);
        QueryDependencies::from_statements(connection, statements)
    }
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct QueryDependencies {
    tables: BTreeMap<String, TableDependency>,
    unknown: bool,
}

#[derive(Clone, Debug)]
struct TableDependency {
    columns: BTreeSet<String>,
    reads_existence: bool,
    key: KeyScope,
}

#[derive(Clone, Debug)]
enum KeyScope {
    All,
    Predicate(KeyPredicate),
}

#[derive(Clone, Debug)]
enum KeyPredicate {
    Compare {
        column: String,
        operator: KeyOperator,
        value: SqlValue,
    },
    In {
        column: String,
        values: Vec<SqlValue>,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

#[derive(Clone, Copy, Debug)]
enum KeyOperator {
    Equal,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

impl QueryDependencies {
    pub fn unknown() -> Self {
        Self {
            tables: BTreeMap::new(),
            unknown: true,
        }
    }

    fn from_statements(
        connection: &Connection,
        statements: Vec<StatementRead>,
    ) -> Result<Self, DbError> {
        if statements.is_empty() {
            return Ok(Self::unknown());
        }
        let mut tables = BTreeMap::<String, TableDependency>::new();
        for statement in statements {
            for table in statement.tables.keys() {
                reject_virtual_table(connection, table)?;
            }
            let statement_scope = match statement.expanded_sql.as_deref() {
                Some(sql) => key_scope_for_statement(connection, sql, &statement.tables)?,
                None => None,
            };
            for (name, read) in statement.tables {
                let scope = statement_scope
                    .as_ref()
                    .filter(|(table, _)| table == &name)
                    .map(|(_, scope)| scope.clone())
                    .unwrap_or(KeyScope::All);
                match tables.entry(name) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(TableDependency {
                            columns: read.columns,
                            reads_existence: read.reads_existence,
                            key: scope,
                        });
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        let dependency = entry.get_mut();
                        dependency.columns.extend(read.columns);
                        dependency.reads_existence |= read.reads_existence;
                        dependency.key = union_scope(dependency.key.clone(), scope);
                    }
                }
            }
        }
        Ok(Self {
            unknown: tables.is_empty(),
            tables,
        })
    }

    pub fn is_affected_by(&self, changes: &CommittedChanges) -> bool {
        if self.unknown || changes.unknown || changes.schema_changed {
            return true;
        }
        changes.rows.iter().any(|change| {
            let Some(dependency) = self.tables.get(&change.table) else {
                return false;
            };
            if !dependency.key.matches(change) {
                return false;
            }
            match change.operation {
                RowOperation::Insert | RowOperation::Delete => true,
                RowOperation::Update => {
                    dependency.reads_existence && change.old_key != change.new_key
                        || !dependency.columns.is_disjoint(&change.changed_columns)
                }
            }
        })
    }
}

impl KeyScope {
    fn matches(&self, change: &RowMutation) -> bool {
        match self {
            Self::All => true,
            Self::Predicate(predicate) => {
                predicate.matches(&change.old_key) || predicate.matches(&change.new_key)
            }
        }
    }
}

impl KeyPredicate {
    fn matches(&self, key: &BTreeMap<String, SqlValue>) -> bool {
        match self {
            Self::Compare {
                column,
                operator,
                value,
            } => key.get(column).is_some_and(|actual| match operator {
                KeyOperator::Equal => actual == value,
                KeyOperator::Less => actual.compare(value).is_some_and(|o| o.is_lt()),
                KeyOperator::LessEqual => actual.compare(value).is_some_and(|o| o.is_le()),
                KeyOperator::Greater => actual.compare(value).is_some_and(|o| o.is_gt()),
                KeyOperator::GreaterEqual => actual.compare(value).is_some_and(|o| o.is_ge()),
            }),
            Self::In { column, values } => key
                .get(column)
                .is_some_and(|actual| values.iter().any(|value| value == actual)),
            Self::And(left, right) => left.matches(key) && right.matches(key),
            Self::Or(left, right) => left.matches(key) || right.matches(key),
        }
    }
}

fn union_scope(left: KeyScope, right: KeyScope) -> KeyScope {
    match (left, right) {
        (KeyScope::Predicate(left), KeyScope::Predicate(right)) => {
            KeyScope::Predicate(KeyPredicate::Or(Box::new(left), Box::new(right)))
        }
        _ => KeyScope::All,
    }
}

#[derive(Clone, Copy)]
enum KeyAffinity {
    Integer,
    Text,
    Blob,
}

fn key_scope_for_statement(
    connection: &Connection,
    sql: &str,
    reads: &BTreeMap<String, TableRead>,
) -> Result<Option<(String, KeyScope)>, DbError> {
    if reads.len() != 1 {
        return Ok(None);
    }
    let Some(table) = reads.keys().next().cloned() else {
        return Ok(None);
    };
    let Some(key_columns) = primary_key_columns(connection, &table)? else {
        return Ok(None);
    };
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    let first = match parser.next() {
        Ok(first) => first,
        Err(_) => return Ok(None),
    };
    let Some(Cmd::Stmt(Stmt::Select(select))) = first else {
        return Ok(None);
    };
    let has_another_statement = match parser.next() {
        Ok(statement) => statement.is_some(),
        Err(_) => return Ok(None),
    };
    if has_another_statement || select.with.is_some() || select.body.compounds.is_some() {
        return Ok(None);
    }
    let OneSelect::Select {
        columns,
        from,
        where_clause,
        group_by,
        having,
        window_clause,
        ..
    } = &select.body.select
    else {
        return Ok(None);
    };
    if columns.iter().any(|column| match column {
        ResultColumn::Expr(expression, _) => !expression_is_subquery_free(expression),
        ResultColumn::Star | ResultColumn::TableStar(_) => false,
    }) || where_clause.is_some_and(|expression| !expression_is_subquery_free(expression))
        || group_by.is_some_and(|expressions| {
            expressions
                .iter()
                .any(|expr| !expression_is_subquery_free(expr))
        })
        || having.is_some_and(|expr| !expression_is_subquery_free(expr))
        || window_clause.is_some()
        || select.order_by.is_some_and(|columns| {
            columns
                .iter()
                .any(|column| !expression_is_subquery_free(&column.expr))
        })
        || select.limit.is_some_and(|limit| {
            !expression_is_subquery_free(&limit.expr)
                || limit
                    .offset
                    .as_ref()
                    .is_some_and(|expr| !expression_is_subquery_free(expr))
        })
    {
        return Ok(None);
    }
    let Some(from) = from.as_ref() else {
        return Ok(None);
    };
    if from.joins.is_some() {
        return Ok(None);
    }
    let Some(SelectTable::Table(name, alias, _)) = from.select else {
        return Ok(None);
    };
    if normalize(&name.name.to_string()) != table {
        return Ok(None);
    }
    let alias = alias.as_ref().map(alias_name);
    let Some(where_clause) = where_clause.as_ref() else {
        return Ok(None);
    };
    let Some(predicate) = predicate_from_expr(where_clause, &table, alias.as_deref(), &key_columns)
    else {
        return Ok(None);
    };
    Ok(Some((table, KeyScope::Predicate(predicate))))
}

fn expression_is_subquery_free(expression: &Expr<'_>) -> bool {
    match expression {
        Expr::Between {
            lhs, start, end, ..
        } => {
            expression_is_subquery_free(lhs)
                && expression_is_subquery_free(start)
                && expression_is_subquery_free(end)
        }
        Expr::Binary(left, _, right) => {
            expression_is_subquery_free(left) && expression_is_subquery_free(right)
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            base.is_none_or(|expr| expression_is_subquery_free(expr))
                && when_then_pairs.iter().all(|(when, then)| {
                    expression_is_subquery_free(when) && expression_is_subquery_free(then)
                })
                && else_expr.is_none_or(|expr| expression_is_subquery_free(expr))
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr) => expression_is_subquery_free(expr),
        Expr::Exists(_) | Expr::InSelect { .. } | Expr::Subquery(_) | Expr::InTable { .. } => false,
        Expr::FunctionCall { .. } | Expr::FunctionCallStar { .. } => false,
        Expr::InList { lhs, rhs, .. } => {
            expression_is_subquery_free(lhs)
                && rhs.is_none_or(|values| values.iter().all(expression_is_subquery_free))
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            expression_is_subquery_free(lhs)
                && expression_is_subquery_free(rhs)
                && escape.is_none_or(|expr| expression_is_subquery_free(expr))
        }
        Expr::Parenthesized(expressions) => expressions.iter().all(expression_is_subquery_free),
        Expr::Raise(_, expression) => expression.is_none(),
        Expr::DoublyQualified(_, _, _)
        | Expr::Id(_)
        | Expr::Literal(_)
        | Expr::Name(_)
        | Expr::Qualified(_, _)
        | Expr::Variable(_) => true,
    }
}

fn alias_name(alias: &As<'_>) -> String {
    let name = match alias {
        As::As(name) | As::Elided(name) => name,
    };
    normalize(&name.to_string())
}

fn primary_key_columns(
    connection: &Connection,
    table: &str,
) -> Result<Option<BTreeMap<String, KeyAffinity>>, DbError> {
    let schema_sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )
        .optional()
        .map_err(DbError::from)?;
    let Some(schema_sql) = schema_sql else {
        return Ok(None);
    };
    reject_virtual_table_sql(table, &schema_sql)?;
    let mut statement = connection
        .prepare("SELECT name, type, pk FROM pragma_table_xinfo(?1) WHERE pk > 0 ORDER BY pk")
        .map_err(DbError::from)?;
    let rows = statement
        .query_map([table], |row| {
            let name: String = row.get(0)?;
            let declared: String = row.get(1)?;
            Ok((name, declared))
        })
        .map_err(DbError::from)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(DbError::from)?;
    if rows.is_empty() {
        return Ok(Some(BTreeMap::from([(
            "_rowid_".to_string(),
            KeyAffinity::Integer,
        )])));
    }
    if !primary_key_uses_binary_collation(connection, table)? {
        return Ok(None);
    }
    let mut columns = BTreeMap::new();
    for (name, declared) in rows {
        let affinity = match declared.trim().to_ascii_uppercase().as_str() {
            "INTEGER" => KeyAffinity::Integer,
            "TEXT" => KeyAffinity::Text,
            "BLOB" | "" => KeyAffinity::Blob,
            _ => return Ok(None),
        };
        columns.insert(normalize(&name), affinity);
    }
    Ok(Some(columns))
}

fn primary_key_uses_binary_collation(
    connection: &Connection,
    table: &str,
) -> Result<bool, DbError> {
    let index: Option<String> = connection
        .query_row(
            "SELECT name FROM pragma_index_list(?1) WHERE origin = 'pk'",
            [table],
            |row| row.get(0),
        )
        .optional()
        .map_err(DbError::from)?;
    let Some(index) = index else {
        // An INTEGER PRIMARY KEY aliases the rowid and has no separate index.
        return Ok(true);
    };
    let has_non_binary: bool = connection
        .query_row(
            "SELECT EXISTS(\
                 SELECT 1 FROM pragma_index_xinfo(?1) \
                 WHERE key = 1 AND upper(coll) <> 'BINARY'\
             )",
            [index],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    Ok(!has_non_binary)
}

fn reject_virtual_table(connection: &Connection, table: &str) -> Result<(), DbError> {
    let schema_sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )
        .optional()
        .map_err(DbError::from)?;
    if let Some(schema_sql) = schema_sql {
        reject_virtual_table_sql(table, &schema_sql)?;
    }
    Ok(())
}

fn reject_virtual_table_sql(table: &str, schema_sql: &str) -> Result<(), DbError> {
    if schema_sql
        .trim_start()
        .to_ascii_uppercase()
        .starts_with("CREATE VIRTUAL TABLE")
    {
        Err(DbError::Message(format!(
            "live queries cannot safely track virtual table {table:?}"
        )))
    } else {
        Ok(())
    }
}

fn predicate_from_expr(
    expression: &Expr<'_>,
    table: &str,
    alias: Option<&str>,
    keys: &BTreeMap<String, KeyAffinity>,
) -> Option<KeyPredicate> {
    match expression {
        Expr::Parenthesized(expressions) if expressions.len() == 1 => {
            predicate_from_expr(&expressions[0], table, alias, keys)
        }
        Expr::Binary(left, Operator::And, right) => {
            let left = predicate_from_expr(left, table, alias, keys);
            let right = predicate_from_expr(right, table, alias, keys);
            match (left, right) {
                (Some(left), Some(right)) => {
                    Some(KeyPredicate::And(Box::new(left), Box::new(right)))
                }
                (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
                (None, None) => None,
            }
        }
        Expr::Binary(left, Operator::Or, right) => Some(KeyPredicate::Or(
            Box::new(predicate_from_expr(left, table, alias, keys)?),
            Box::new(predicate_from_expr(right, table, alias, keys)?),
        )),
        Expr::Binary(left, operator, right) => {
            comparison_predicate(left, *operator, right, table, alias, keys).or_else(|| {
                comparison_predicate(
                    right,
                    reverse_operator(*operator)?,
                    left,
                    table,
                    alias,
                    keys,
                )
            })
        }
        Expr::InList {
            lhs,
            not: false,
            rhs: Some(values),
        } => {
            let column = key_column(lhs, table, alias, keys)?;
            let affinity = keys.get(&column)?;
            let values = values
                .iter()
                .map(|value| literal_value(value, *affinity))
                .collect::<Option<Vec<_>>>()?;
            Some(KeyPredicate::In { column, values })
        }
        Expr::Between {
            lhs,
            not: false,
            start,
            end,
        } => {
            let column = key_column(lhs, table, alias, keys)?;
            let affinity = *keys.get(&column)?;
            Some(KeyPredicate::And(
                Box::new(KeyPredicate::Compare {
                    column: column.clone(),
                    operator: KeyOperator::GreaterEqual,
                    value: literal_value(start, affinity)?,
                }),
                Box::new(KeyPredicate::Compare {
                    column,
                    operator: KeyOperator::LessEqual,
                    value: literal_value(end, affinity)?,
                }),
            ))
        }
        _ => None,
    }
}

fn comparison_predicate(
    column: &Expr<'_>,
    operator: Operator,
    value: &Expr<'_>,
    table: &str,
    alias: Option<&str>,
    keys: &BTreeMap<String, KeyAffinity>,
) -> Option<KeyPredicate> {
    let operator = match operator {
        Operator::Equals | Operator::Is => KeyOperator::Equal,
        Operator::Less => KeyOperator::Less,
        Operator::LessEquals => KeyOperator::LessEqual,
        Operator::Greater => KeyOperator::Greater,
        Operator::GreaterEquals => KeyOperator::GreaterEqual,
        _ => return None,
    };
    let column = key_column(column, table, alias, keys)?;
    let value = literal_value(value, *keys.get(&column)?)?;
    Some(KeyPredicate::Compare {
        column,
        operator,
        value,
    })
}

fn reverse_operator(operator: Operator) -> Option<Operator> {
    match operator {
        Operator::Equals | Operator::Is => Some(operator),
        Operator::Less => Some(Operator::Greater),
        Operator::LessEquals => Some(Operator::GreaterEquals),
        Operator::Greater => Some(Operator::Less),
        Operator::GreaterEquals => Some(Operator::LessEquals),
        _ => None,
    }
}

fn key_column(
    expression: &Expr<'_>,
    table: &str,
    alias: Option<&str>,
    keys: &BTreeMap<String, KeyAffinity>,
) -> Option<String> {
    let name = match expression {
        Expr::Id(name) => normalize(name.0),
        Expr::Name(name) => normalize(&name.to_string()),
        Expr::Qualified(qualifier, name)
            if {
                let qualifier = normalize(&qualifier.to_string());
                qualifier == table || alias == Some(qualifier.as_str())
            } =>
        {
            normalize(&name.to_string())
        }
        Expr::DoublyQualified(database, qualifier, name)
            if database == "main" && {
                let qualifier = normalize(&qualifier.to_string());
                qualifier == table || alias == Some(qualifier.as_str())
            } =>
        {
            normalize(&name.to_string())
        }
        _ => return None,
    };
    keys.contains_key(&name).then_some(name)
}

fn literal_value(expression: &Expr<'_>, affinity: KeyAffinity) -> Option<SqlValue> {
    match (expression, affinity) {
        (Expr::Literal(Literal::Numeric(value)), KeyAffinity::Integer) => match value.parse() {
            Ok(value) => Some(SqlValue::Integer(value)),
            Err(_) => None,
        },
        (
            Expr::Unary(
                sqlite3_parser::ast::UnaryOperator::Negative,
                Expr::Literal(Literal::Numeric(value)),
            ),
            KeyAffinity::Integer,
        ) => match value.parse::<i64>() {
            Ok(value) => value.checked_neg().map(SqlValue::Integer),
            Err(_) => None,
        },
        (Expr::Literal(Literal::String(value)), KeyAffinity::Text) => {
            decode_sql_string(value).map(|value| SqlValue::Text(value.into_bytes()))
        }
        (Expr::Literal(Literal::Blob(value)), KeyAffinity::Blob) => {
            decode_sql_blob(value).map(SqlValue::Blob)
        }
        _ => None,
    }
}

fn decode_sql_string(value: &str) -> Option<String> {
    let value = value.strip_prefix('\'')?.strip_suffix('\'')?;
    Some(value.replace("''", "'"))
}

fn decode_sql_blob(value: &str) -> Option<Vec<u8>> {
    hex::decode(value).ok()
}

fn normalize(identifier: &str) -> String {
    identifier.to_ascii_lowercase()
}

#[cfg(test)]
#[path = "live_query_tests.rs"]
mod tests;
