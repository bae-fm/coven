//! A walk of the SQL SQLite runs for a statement or one of the app's views:
//! each SELECT, its CTEs, its arms and their FROM items, the views those
//! read, and the queries inside every expression (§4). The walk resolves
//! each FROM item's name as SQLite does, to the innermost CTE of that name
//! or else to the main schema's table or view, reads each view's SQL once,
//! and notes what each query reads. A `Visit` adds what one use of the walk
//! does at a SELECT, at a core and at a FROM clause.

use std::collections::{BTreeMap, BTreeSet};

use crate::schema_objects::{main_object, read_view};
use crate::select_syntax::{parts, window_parts, Part};
use rusqlite::Connection;
use sqlite3_parser::ast::{
    Expr, FromClause, JoinConstraint, OneSelect, QualifiedName, ResultColumn, Select, SelectTable,
    SortedColumn,
};

/// The CTE or view whose SQL the walk is reading, the innermost one.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Within {
    /// A CTE, by its place in the CTEs the walk met.
    Cte(usize),
    /// A view, by its name, lowercase.
    View(String),
}

/// The tables a query reads, in its FROM, in the views it reads and in the
/// queries inside it, and the CTEs it reads, whose own tables count too.
#[derive(Clone, Default)]
pub(crate) struct Reads {
    /// Tables of the main schema, by their stored names.
    pub(crate) tables: BTreeSet<String>,
    /// CTEs, by their places in the CTEs the walk met.
    pub(crate) ctes: BTreeSet<usize>,
    /// Views, by their names, lowercase, whose tables `tables` holds.
    pub(crate) views: BTreeSet<String>,
}

impl Reads {
    fn extend(&mut self, other: Self) {
        self.tables.extend(other.tables);
        self.ctes.extend(other.ctes);
        self.views.extend(other.views);
    }
}

/// What a use of the walk does at each SELECT, core and FROM clause, after
/// the walk has read what it holds, with `scope` where the node is.
pub(crate) trait Visit {
    /// At `select`, which reads `reads`: a subquery whose first row an
    /// expression takes when `first_row`.
    fn select(
        &mut self,
        _scope: &Scope<'_>,
        _select: &Select<'_>,
        _first_row: bool,
        _reads: &Reads,
    ) -> rusqlite::Result<()> {
        Ok(())
    }

    /// At `core`, which reads `reads`, sorted by `order_by` when it is its
    /// query's only one.
    fn core(
        &mut self,
        _scope: &Scope<'_>,
        _core: &OneSelect<'_>,
        _order_by: &[SortedColumn<'_>],
        _reads: &Reads,
    ) -> rusqlite::Result<()> {
        Ok(())
    }

    /// At `from`, whose FROM items, joined ones included, are `items`.
    fn from(
        &mut self,
        _scope: &Scope<'_>,
        _from: &FromClause<'_>,
        _items: &[&SelectTable<'_>],
    ) -> rusqlite::Result<()> {
        Ok(())
    }
}

/// A walk that only notes what each query reads.
impl Visit for () {}

/// Where the walk is, and what it has met so far.
pub(crate) struct Scope<'c> {
    pub(crate) connection: &'c Connection,
    /// What each view read so far reads, by its name, lowercase, so a view
    /// read twice is read once.
    views: BTreeMap<String, Reads>,
    /// The CTEs where the walk is: each one's name, lowercase, which a FROM
    /// item's name means before any table or view of that name, and its place
    /// in `defined`.
    ctes: Vec<(String, usize)>,
    /// Each CTE the walk has met, by its name, lowercase, with what it reads.
    /// A CTE can name another defined after it in the same WITH, so the walk
    /// reads every CTE's SELECT before it follows the CTEs a query names.
    defined: Vec<(String, Reads)>,
    /// The CTE or view whose SQL the walk is reading now.
    within: Option<Within>,
    /// Why the parser couldn't read a view the walk met.
    unread: Option<String>,
}

impl Scope<'_> {
    /// The CTE or view whose SQL the walk is reading now.
    pub(crate) fn within(&self) -> Option<Within> {
        self.within.clone()
    }

    /// Why the parser couldn't read a view the walk met, if it couldn't.
    pub(crate) fn unread(&self) -> Option<&str> {
        self.unread.as_deref()
    }

    /// The names, lowercase, of the CTEs and the views the walk met.
    pub(crate) fn names(&self) -> BTreeSet<String> {
        self.defined
            .iter()
            .map(|(name, _)| name.clone())
            .chain(self.views.keys().cloned())
            .collect()
    }

    /// The place of the CTE that `name` names where the walk is, the
    /// innermost of that name.
    fn cte(&self, name: &QualifiedName<'_>) -> Option<usize> {
        if name.db_name.is_some() {
            return None;
        }
        self.ctes.iter().rev().find(|(cte, _)| name.name == cte.as_str()).map(|(_, place)| *place)
    }

    /// The stored name of the main schema's object that `name` names, when
    /// its type is one of `kinds`, `table` or `view`.
    pub(crate) fn object(
        &self,
        name: &QualifiedName<'_>,
        kinds: &[&str],
    ) -> rusqlite::Result<Option<String>> {
        Ok(self
            .main(name)?
            .filter(|(_, found)| kinds.contains(&found.as_str()))
            .map(|(stored, _)| stored))
    }

    /// The stored name and the type, `table` or `view`, of the main schema's
    /// object that `name` names. A name of another schema reads no table of
    /// the app's, and a CTE's name none of the schema's objects.
    fn main(&self, name: &QualifiedName<'_>) -> rusqlite::Result<Option<(String, String)>> {
        if name.db_name.as_ref().is_some_and(|schema| *schema != "main") {
            return Ok(None);
        }
        if self.cte(name).is_some() {
            return Ok(None);
        }
        main_object(self.connection, &name.name)
    }

    /// The CTEs and views a query that reads `reads` runs: those it reads,
    /// and those each of them reads.
    pub(crate) fn run(&self, reads: &Reads) -> BTreeSet<Within> {
        let mut run = BTreeSet::new();
        let mut waiting: Vec<Within> = reads
            .ctes
            .iter()
            .map(|place| Within::Cte(*place))
            .chain(reads.views.iter().map(|view| Within::View(view.clone())))
            .collect();
        while let Some(within) = waiting.pop() {
            if run.contains(&within) {
                continue;
            }
            let read = match &within {
                Within::Cte(place) => &self.defined[*place].1,
                Within::View(view) => &self.views[view],
            };
            waiting.extend(read.ctes.iter().map(|place| Within::Cte(*place)));
            waiting.extend(read.views.iter().map(|view| Within::View(view.clone())));
            run.insert(within);
        }
        run
    }

    /// The tables `reads` reads, with those of the CTEs it reads and of the
    /// CTEs those read.
    pub(crate) fn tables(&self, reads: &Reads) -> BTreeSet<String> {
        let mut tables = reads.tables.clone();
        let mut followed = BTreeSet::new();
        let mut waiting: Vec<usize> = reads.ctes.iter().copied().collect();
        while let Some(place) = waiting.pop() {
            if !followed.insert(place) {
                continue;
            }
            let (_, cte) = &self.defined[place];
            tables.extend(cte.tables.iter().cloned());
            waiting.extend(cte.ctes.iter().copied());
        }
        tables
    }
}

pub(crate) struct Walk<'c, V> {
    pub(crate) scope: Scope<'c>,
    pub(crate) visit: V,
}

impl<'c, V: Visit> Walk<'c, V> {
    pub(crate) fn new(connection: &'c Connection, visit: V) -> Self {
        Self {
            scope: Scope {
                connection,
                views: BTreeMap::new(),
                ctes: Vec::new(),
                defined: Vec::new(),
                within: None,
                unread: None,
            },
            visit,
        }
    }

    /// Read `select`, a subquery whose first row an expression takes when
    /// `first_row`.
    pub(crate) fn select(
        &mut self,
        select: &Select<'_>,
        first_row: bool,
    ) -> rusqlite::Result<Reads> {
        let outer = self.scope.ctes.len();
        let ctes = select.with.iter().flat_map(|with| with.ctes.iter());
        for cte in ctes.clone() {
            let name = crate::sql::name(cte.tbl_name.0).to_ascii_lowercase();
            self.scope.ctes.push((name.clone(), self.scope.defined.len()));
            self.scope.defined.push((name, Reads::default()));
        }
        for (position, cte) in ctes.enumerate() {
            let place = self.scope.ctes[outer + position].1;
            let around = self.scope.within.replace(Within::Cte(place));
            let reads = self.select(cte.select, false);
            self.scope.within = around;
            self.scope.defined[place].1 = reads?;
        }
        let mut reads = Reads::default();
        // A compound's ORDER BY sorts by its result columns; a single
        // SELECT's can name its core's columns too.
        let order_by = match select.body.compounds {
            None => select.order_by.unwrap_or_default(),
            Some(_) => &[],
        };
        let arms = std::iter::once(&select.body.select)
            .chain(select.body.compounds.iter().flatten().map(|compound| &compound.select));
        for arm in arms {
            reads.extend(self.core(arm, order_by)?);
        }
        for term in select.order_by.iter().flat_map(|terms| terms.iter()) {
            reads.extend(self.expr(&term.expr)?);
        }
        if let Some(limit) = select.limit {
            reads.extend(self.expr(&limit.expr)?);
            if let Some(offset) = &limit.offset {
                reads.extend(self.expr(offset)?);
            }
        }
        self.visit.select(&self.scope, select, first_row, &reads)?;
        self.scope.ctes.truncate(outer);
        Ok(reads)
    }

    /// Read one core, sorted by `order_by` when it is the query's only one.
    fn core(
        &mut self,
        core: &OneSelect<'_>,
        order_by: &[SortedColumn<'_>],
    ) -> rusqlite::Result<Reads> {
        let mut reads = Reads::default();
        match core {
            OneSelect::Values(rows) => {
                for expr in rows.iter().flat_map(|row| row.iter()) {
                    reads.extend(self.expr(expr)?);
                }
            }
            OneSelect::Select {
                columns,
                from,
                where_clause,
                group_by,
                having,
                window_clause,
                ..
            } => {
                for column in columns.iter() {
                    if let ResultColumn::Expr(expr, _) = column {
                        reads.extend(self.expr(expr)?);
                    }
                }
                if let Some(from) = from {
                    reads.extend(self.from(from)?);
                }
                for expr in where_clause.iter().copied().chain(having.iter().copied()) {
                    reads.extend(self.expr(expr)?);
                }
                for expr in group_by.iter().flat_map(|exprs| exprs.iter()) {
                    reads.extend(self.expr(expr)?);
                }
                let mut found = Vec::new();
                for definition in window_clause.iter().flat_map(|windows| windows.iter()) {
                    window_parts(&definition.window, &mut found);
                }
                reads.extend(self.parts(&found)?);
            }
        }
        self.visit.core(&self.scope, core, order_by, &reads)?;
        Ok(reads)
    }

    /// Each FROM item, then each join's ON.
    fn from(&mut self, from: &FromClause<'_>) -> rusqlite::Result<Reads> {
        let items: Vec<&SelectTable<'_>> = from
            .select
            .into_iter()
            .chain(from.joins.iter().flatten().map(|join| &join.table))
            .collect();
        let mut reads = Reads::default();
        for item in &items {
            reads.extend(self.item(item)?);
        }
        for join in from.joins.iter().flatten() {
            if let Some(JoinConstraint::On(expr)) = &join.constraint {
                reads.extend(self.expr(expr)?);
            }
        }
        self.visit.from(&self.scope, from, &items)?;
        Ok(reads)
    }

    fn item(&mut self, item: &SelectTable<'_>) -> rusqlite::Result<Reads> {
        match item {
            SelectTable::Table(name, ..) => self.named(name),
            SelectTable::TableCall(_, args, _) => {
                let mut reads = Reads::default();
                for arg in args.iter().flat_map(|args| args.iter()) {
                    reads.extend(self.expr(arg)?);
                }
                Ok(reads)
            }
            SelectTable::Select(select, _) => self.select(select, false),
            SelectTable::Sub(from, _) => self.from(from),
        }
    }

    /// The CTE, table or view `name` names, as a FROM item or as the table
    /// of `x IN table` names it.
    fn named(&mut self, name: &QualifiedName<'_>) -> rusqlite::Result<Reads> {
        if let Some(place) = self.scope.cte(name) {
            return Ok(Reads { ctes: BTreeSet::from([place]), ..Reads::default() });
        }
        match self.scope.main(name)? {
            Some((table, kind)) if kind == "table" => {
                Ok(Reads { tables: BTreeSet::from([table]), ..Reads::default() })
            }
            Some((view, _)) => self.view(&view),
            None => Ok(Reads::default()),
        }
    }

    /// Read the main schema's view `stored`, once.
    pub(crate) fn view(&mut self, stored: &str) -> rusqlite::Result<Reads> {
        let name = stored.to_ascii_lowercase();
        if let Some(reads) = self.scope.views.get(&name) {
            return Ok(reads.clone());
        }
        let connection = self.scope.connection;
        // A view's SELECT names no CTE of the statement.
        let ctes = std::mem::take(&mut self.scope.ctes);
        let around = self.scope.within.replace(Within::View(name.clone()));
        let read = read_view(connection, stored, |_, select| self.select(select, false));
        self.scope.within = around;
        self.scope.ctes = ctes;
        match read? {
            Ok(mut reads) => {
                reads.views.insert(name.clone());
                self.scope.views.insert(name, reads.clone());
                Ok(reads)
            }
            Err(why) => {
                self.scope.unread.get_or_insert(why);
                Ok(Reads::default())
            }
        }
    }

    fn expr(&mut self, expr: &Expr<'_>) -> rusqlite::Result<Reads> {
        let mut found = Vec::new();
        parts(expr, &mut found);
        self.parts(&found)
    }

    fn parts(&mut self, found: &[Part<'_>]) -> rusqlite::Result<Reads> {
        let mut reads = Reads::default();
        for part in found {
            match part {
                Part::Select(select) => reads.extend(self.select(select, false)?),
                Part::Subquery(select) => reads.extend(self.select(select, true)?),
                Part::Table(name) => reads.extend(self.named(name)?),
                Part::Name(_) | Part::Collation | Part::Function(..) | Part::Clock => {}
            }
        }
        Ok(reads)
    }
}
