//! What a live query reads on each request: compiled statements alone, so no
//! statement takes its values from another's rows in Rust (§4, api.md E4).

use crate::{DbError, Writer};
use sqlive::rusqlite::Row;
use sqlive::{LiveStatement, Values};
use sqlive_compose::Read;

/// What a live query reads on each request and returns: a read the app's
/// build compiled (`read!`), which returns its rows, two that read one
/// snapshot (`Both`), which return each one's rows, one whose rows read as
/// another type (`ReadAs`), a read of coven's own tables (`CovenRead`), or
/// one of these only when there is one (`Maybe`).
/// coven implements it for those alone, and for its own tests' SQL
/// (`LiveSql`).
pub trait LiveReads: sealed::Sealed + Send + Sync + 'static {
    /// The rows the reads return.
    type Rows: Send + 'static;

    /// Run the reads through `run`, which runs statements on one snapshot
    /// and hands each row to `each` with its statement's place in the list.
    #[doc(hidden)]
    fn read(&self, run: Run<'_>) -> Result<Self::Rows, DbError>;
}

/// How a read makes a `T` of each row it reads.
type RowReader<T> = Box<dyn Fn(&Row<'_>) -> sqlive::rusqlite::Result<T> + Send + Sync>;

/// What runs a live query's statements on one snapshot (`LiveReads::read`).
pub(crate) type Run<'a> = &'a mut dyn FnMut(
    &[LiveStatement<'_>],
    &mut dyn FnMut(usize, &Row<'_>) -> sqlive::rusqlite::Result<()>,
) -> Result<(), DbError>;

/// The statement of `read` with the values it binds.
fn statement<Q: Read<Handle = Writer>>(read: &Q) -> Result<LiveStatement<'static>, DbError> {
    let values = Values::named(&read.params()).map_err(DbError::from)?;
    Ok(LiveStatement::compiled(Q::compiled(), values))
}

impl<Q> LiveReads for Q
where
    Q: Read<Handle = Writer> + Send + Sync + 'static,
    Q::Row: Send + 'static,
{
    type Rows = Vec<Q::Row>;

    fn read(&self, run: Run<'_>) -> Result<Self::Rows, DbError> {
        let mut rows = Vec::new();
        run(&[statement(self)?], &mut |_, row| {
            rows.push(Q::read(row)?);
            Ok(())
        })?;
        Ok(rows)
    }
}

/// Two reads the app's build compiled, which a live query reads on one
/// snapshot: a concern's read that is too large for SQLite to prepare as one
/// statement.
#[derive(Clone, Debug, PartialEq)]
pub struct Both<A, B>(pub A, pub B);

impl<A, B> LiveReads for Both<A, B>
where
    A: Read<Handle = Writer> + Send + Sync + 'static,
    B: Read<Handle = Writer> + Send + Sync + 'static,
    A::Row: Send + 'static,
    B::Row: Send + 'static,
{
    type Rows = (Vec<A::Row>, Vec<B::Row>);

    fn read(&self, run: Run<'_>) -> Result<Self::Rows, DbError> {
        let (mut first, mut second) = (Vec::new(), Vec::new());
        run(&[statement(&self.0)?, statement(&self.1)?], &mut |index, row| {
            match index {
                0 => first.push(A::read(row)?),
                _ => second.push(B::read(row)?),
            }
            Ok(())
        })?;
        Ok((first, second))
    }
}

/// `values`, bound in order, for a `CovenRead`.
#[doc(hidden)]
pub fn bound(values: &[&dyn sqlive::rusqlite::ToSql]) -> Result<Values, DbError> {
    Ok(Values::positional(values)?)
}

/// No values, for a `CovenRead` whose SELECT binds none.
#[doc(hidden)]
pub fn unbound() -> Result<Values, DbError> {
    Ok(Values::Positional(Vec::new()))
}

/// One of coven's own SELECTs, which may read its tables that app SQL can't:
/// its SQL, the values it binds, how each row reads, and what the rows make,
/// which a one-shot read and a live query (`subscribe`) both read. Only
/// coven's crates make one, as `Invite::live` does for `coven_invites`.
pub struct CovenRead<T, U> {
    sql: String,
    values: Box<dyn Fn() -> Result<Values, DbError> + Send + Sync>,
    row: RowReader<T>,
    finish: Box<dyn Fn(Vec<T>) -> Result<U, DbError> + Send + Sync>,
}

impl<T: 'static> CovenRead<T, Vec<T>> {
    /// `sql`, bound to what `values` gives, whose rows `row` reads.
    #[doc(hidden)]
    pub fn new(
        sql: impl Into<String>,
        values: impl Fn() -> Result<Values, DbError> + Send + Sync + 'static,
        row: impl Fn(&Row<'_>) -> sqlive::rusqlite::Result<T> + Send + Sync + 'static,
    ) -> Self {
        Self { sql: sql.into(), values: Box::new(values), row: Box::new(row), finish: Box::new(Ok) }
    }

    /// The read of the first row alone, which fails with `QueryReturnedNoRows`
    /// when there is none, as `query_row` does.
    #[doc(hidden)]
    pub fn one(self) -> CovenRead<T, T> {
        self.then(first)
    }
}

impl<T: 'static, U: 'static> CovenRead<T, U> {
    /// The read whose result `then` makes of this one's.
    #[doc(hidden)]
    pub fn then<V>(
        self,
        then: impl Fn(U) -> Result<V, DbError> + Send + Sync + 'static,
    ) -> CovenRead<T, V> {
        let finish = self.finish;
        CovenRead {
            sql: self.sql,
            values: self.values,
            row: self.row,
            finish: Box::new(move |rows| then(finish(rows)?)),
        }
    }
}

/// What `reads` returns, read once on `db` inside its transaction, as a live
/// query reads it on each run.
pub(crate) fn once<S: LiveReads>(
    reads: &S,
    db: &crate::sqlite::StoreSql,
) -> Result<S::Rows, DbError> {
    reads.read(&mut |statements, each| {
        for (index, statement) in statements.iter().enumerate() {
            db.once_rows(statement, |row| each(index, row))?;
        }
        Ok(())
    })
}

impl<T: Send + 'static, U: Send + 'static> LiveReads for CovenRead<T, U> {
    type Rows = U;

    fn read(&self, run: Run<'_>) -> Result<Self::Rows, DbError> {
        let mut rows = Vec::new();
        let statement = LiveStatement::host(&self.sql, (self.values)()?);
        run(&[statement], &mut |_, row| {
            rows.push((self.row)(row)?);
            Ok(())
        })?;
        (self.finish)(rows)
    }
}

/// A read the app's build compiled whose rows read as `R`: a list that one of
/// several statements reads, as its sort picks, each with its own row type,
/// reads its rows as one type whichever statement the request picks.
pub struct ReadAs<R> {
    statement: LiveStatement<'static>,
    read: RowReader<R>,
}

impl<R: 'static> ReadAs<R> {
    /// `read`'s rows, each made an `R` by `row`.
    ///
    /// # Errors
    ///
    /// Fails when one of `read`'s values doesn't bind.
    pub fn new<Q>(
        read: &Q,
        row: impl Fn(Q::Row) -> R + Send + Sync + 'static,
    ) -> Result<Self, DbError>
    where
        Q: Read<Handle = Writer> + 'static,
    {
        Ok(Self {
            statement: statement(read)?,
            read: Box::new(move |found| Ok(row(Q::read(found)?))),
        })
    }
}

impl<R: Send + 'static> LiveReads for ReadAs<R> {
    type Rows = Vec<R>;

    fn read(&self, run: Run<'_>) -> Result<Self::Rows, DbError> {
        let mut rows = Vec::new();
        run(std::slice::from_ref(&self.statement), &mut |_, row| {
            rows.push((self.read)(row)?);
            Ok(())
        })?;
        Ok(rows)
    }
}

/// A read that runs only when there is one: a request with nothing to read,
/// such as a detail pane with nothing selected, reads nothing and returns
/// `None`, and the query runs again only for a new request.
#[derive(Clone, Debug, PartialEq)]
pub struct Maybe<S>(pub Option<S>);

impl<S: LiveReads> LiveReads for Maybe<S> {
    type Rows = Option<S::Rows>;

    fn read(&self, run: Run<'_>) -> Result<Self::Rows, DbError> {
        self.0.as_ref().map(|reads| reads.read(run)).transpose()
    }
}

/// The app's SELECT `sql`, which sqlive plans as it first runs it, with the
/// values it binds, how its rows read and what the read returns of them: a
/// live query for coven's own tests of its live queries, which compile no
/// statement.
#[cfg(any(test, feature = "test-utils"))]
pub struct LiveSql<T, U = Vec<T>> {
    statement: TestStatement,
    values: Values,
    row: fn(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    finish: fn(Vec<T>) -> Result<U, DbError>,
}

#[cfg(any(test, feature = "test-utils"))]
impl<T> LiveSql<T> {
    /// `sql`, bound to `values` by position, whose rows `row` reads.
    ///
    /// # Panics
    ///
    /// Panics when a value's `ToSql` fails, as a test's own value doesn't.
    pub fn new(
        sql: impl Into<String>,
        values: &[&dyn sqlive::rusqlite::ToSql],
        row: fn(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    ) -> Self {
        let values = Values::positional(values).expect("a test's values bind");
        Self { statement: TestStatement::App(sql.into()), values, row, finish: Ok }
    }

    /// `statement`, which a build compiled and binds no value, whose rows
    /// `row` reads.
    pub fn compiled(
        statement: sqlive::CompiledStatement<Writer>,
        row: fn(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    ) -> Self {
        let values = Values::Positional(Vec::new());
        Self { statement: TestStatement::Compiled(statement), values, row, finish: Ok }
    }

    /// `sql`, which binds no value, whose rows `row` reads.
    pub fn of(sql: impl Into<String>, row: fn(&Row<'_>) -> sqlive::rusqlite::Result<T>) -> Self {
        Self::new(sql, &[], row)
    }

    /// The read of the first row alone, which fails with `QueryReturnedNoRows`
    /// when there is none, as `query_row` does.
    pub fn one(self) -> LiveSql<T, T> {
        LiveSql { statement: self.statement, values: self.values, row: self.row, finish: first }
    }
}

/// The statement of a `LiveSql`: the app's SQL, which sqlive plans as it
/// first runs it, or a statement a build compiled.
#[cfg(any(test, feature = "test-utils"))]
enum TestStatement {
    App(String),
    Compiled(sqlive::CompiledStatement<Writer>),
}

/// The first of `rows`, or `QueryReturnedNoRows` when there is none.
fn first<T>(rows: Vec<T>) -> Result<T, DbError> {
    Ok(rows.into_iter().next().ok_or(sqlive::rusqlite::Error::QueryReturnedNoRows)?)
}

#[cfg(any(test, feature = "test-utils"))]
impl<T: Send + 'static, U: Send + 'static> LiveReads for LiveSql<T, U> {
    type Rows = U;

    fn read(&self, run: Run<'_>) -> Result<Self::Rows, DbError> {
        let mut rows = Vec::new();
        let values = self.values.clone();
        let statement = match &self.statement {
            TestStatement::App(sql) => LiveStatement::app(sql, values),
            TestStatement::Compiled(statement) => LiveStatement::compiled(*statement, values),
        };
        run(&[statement], &mut |_, row| {
            rows.push((self.row)(row)?);
            Ok(())
        })?;
        (self.finish)(rows)
    }
}

mod sealed {
    /// Keeps `LiveReads` to the reads coven implements it for.
    pub trait Sealed {}

    impl<Q: sqlive_compose::Read> Sealed for Q {}
    impl<A, B> Sealed for super::Both<A, B> {}
    impl<S> Sealed for super::Maybe<S> {}
    impl<R> Sealed for super::ReadAs<R> {}
    impl<T, U> Sealed for super::CovenRead<T, U> {}
    #[cfg(any(test, feature = "test-utils"))]
    impl<T, U> Sealed for super::LiveSql<T, U> {}
}

#[cfg(test)]
#[path = "live_reads_tests.rs"]
mod tests;
