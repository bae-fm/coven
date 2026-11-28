//! The statements a live query's run reads, each with the values it binds
//! (SPEC.md sections 3 and 4).

use super::HostSql;
use crate::authorization::{Policy, PolicyError};
use crate::{CompiledStatement, Values};
use rusqlite::Row;
use std::sync::Arc;

/// One statement of a live query's run and the values it binds: the app's
/// SQL, whose scope plan sqlive builds; a compiled read of the app's, which
/// brings its plan; or the host's own SELECT, which may read tables the
/// app's SQL cannot. Each narrows by the same rules.
#[derive(Clone, Debug)]
pub struct LiveStatement<'a> {
    source: Source<'a>,
    values: Values,
}

#[derive(Clone, Copy, Debug)]
enum Source<'a> {
    App(&'a str),
    Compiled { sql: &'static str, plan: &'static str },
    Host(&'a str),
}

impl<'a> LiveStatement<'a> {
    /// The app's SELECT `sql`, bound to `values`.
    pub fn app(sql: &'a str, values: Values) -> Self {
        Self { source: Source::App(sql), values }
    }

    /// The app's compiled read `statement`, bound to `values`. A schema
    /// that shows a table, view or function the plan read otherwise than
    /// the schema it was built against fails the run with
    /// `CompiledSchemaDiffers`, as `app_query_compiled` does.
    pub fn compiled<H>(statement: CompiledStatement<H>, values: Values) -> Self {
        Self { source: Source::Compiled { sql: statement.sql, plan: statement.plan }, values }
    }

    /// The host's own SELECT `sql`, bound to `values`.
    pub fn host(sql: &'a str, values: Values) -> Self {
        Self { source: Source::Host(sql), values }
    }
}

/// How a statement runs: as a statement of a live query's run, which records
/// what it read, or once, in a one-shot read, which records nothing.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Run {
    Live,
    Once,
}

impl<P: Policy> HostSql<P> {
    /// Hand `each` every row of `statement` in order, as a statement of a live
    /// query's run: the run records each table it read in the scope that the
    /// statement's plan gives the values it binds.
    pub(super) fn live_rows(
        &self,
        statement: &LiveStatement<'_>,
        each: impl FnMut(&Row<'_>) -> rusqlite::Result<()>,
    ) -> Result<(), PolicyError<P>> {
        self.statement_rows(statement, Run::Live, each)
    }

    /// Hand `each` every row of `statement` in order, read once: a live
    /// query's statement run in a one-shot read, which builds no plan. A
    /// compiled read whose plan doesn't fit the schema still fails.
    pub fn once_rows(
        &self,
        statement: &LiveStatement<'_>,
        each: impl FnMut(&Row<'_>) -> rusqlite::Result<()>,
    ) -> Result<(), PolicyError<P>> {
        self.statement_rows(statement, Run::Once, each)
    }

    fn statement_rows(
        &self,
        statement: &LiveStatement<'_>,
        run: Run,
        mut each: impl FnMut(&Row<'_>) -> rusqlite::Result<()>,
    ) -> Result<(), PolicyError<P>> {
        let mut every = |row: &Row<'_>| each(row).map(|()| true);
        match statement.source {
            Source::Host(sql) => {
                let _scope = self.authorization.internal();
                self.authorization.begin_read_statement();
                self.with_statement(sql, |prepared| {
                    let plan = match run {
                        Run::Live => self.host_plan(sql, prepared)?,
                        Run::Once => None,
                    };
                    let values = statement.values.of_statement(prepared)?;
                    Ok(self.run_bound(
                        crate::authorization::Owner::Host,
                        prepared,
                        plan,
                        &values,
                        &mut every,
                    )?)
                })
            }
            Source::App(sql) | Source::Compiled { sql, .. } => {
                let sql = self.authorization.check_sql(sql)?;
                self.begin_app_statement()?;
                self.authorization.begin_read_statement();
                let result = (|| {
                    let mut prepared = self.app_statement(sql)?;
                    let schema = prepared.schema();
                    let plan = match (statement.source, run) {
                        (Source::Compiled { plan, .. }, Run::Live) => {
                            Some(self.compiled_plan(plan, schema)?)
                        }
                        (Source::Compiled { plan, .. }, Run::Once) => {
                            self.compiled_plan(plan, schema)?;
                            None
                        }
                        (_, Run::Live) => self.live_scope_plan(sql, &prepared, schema)?,
                        (_, Run::Once) => None,
                    };
                    let values = statement.values.of_statement(&prepared)?;
                    self.run_bound(
                        crate::authorization::Owner::App,
                        &mut prepared,
                        plan,
                        &values,
                        &mut every,
                    )
                })();
                Ok(self.authorization.app_result(result)?)
            }
        }
    }

    /// The scope plan of the host's statement `sql`, built against the
    /// schema the connection reads now.
    fn host_plan(
        &self,
        sql: &str,
        prepared: &rusqlite::Statement<'_>,
    ) -> Result<Option<Arc<crate::ScopePlan>>, PolicyError<P>> {
        let schema = self.app_schema_cookie().map_err(crate::Error::from)?;
        Ok(self.live_scope_plan(sql, prepared, Some(schema))?)
    }
}

#[cfg(test)]
#[path = "live_statement_tests.rs"]
mod tests;
