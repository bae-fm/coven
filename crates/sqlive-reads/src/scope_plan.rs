//! What one app statement's SQL lets a live query narrow, read from SQLite's
//! parsed SELECT syntax once per statement and schema (SPEC.md section 4). A
//! shape this doesn't understand depends on whole tables rather than guessing.
//!
//! A statement is a tree of SELECT cores: the statement itself, each arm of
//! a compound, and each subquery, in any clause, sitting in the core whose
//! clause holds it. A core's sources are the tables and other FROM items it
//! joins. Each table's rows are narrowed by the conditions its own core
//! forces on the rows that can reach the result: constants compared with its
//! columns, and equalities with earlier sources of the core or of a core it
//! sits in (`scope_edge.rs`), which a write that changes the table looks up
//! (`scope_lookup.rs`). An equality also carries what one side's conditions
//! say of its compared column to the other side's column. Values pass
//! into a subquery: whether a subquery finds a row doesn't narrow the core
//! around it, and only the column a subquery returns as it is does come out
//! of one, as an equality with a column around it (`scope_subquery.rs`). A
//! condition on one table's row that none of these reads, a function of a
//! column or a CASE, narrows that table by asking SQLite about each changed
//! row (`scope_row.rs`). A subquery in FROM, a CTE and one of the app's
//! views are read as cores too, and conditions on their rows pass into them
//! (`derived.rs`).

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::derived::Ctes;
use crate::from_item::{Columns, FromItem};
use crate::read_report::ReadPlace;
use crate::reference_columns::ReferenceColumns;
use crate::row_scope::Scopes;
use crate::schema_objects::{MainSchema, SchemaLookups};
use crate::scope_condition::read_condition;
use crate::scope_edge::{Edge, MemberValue};
use crate::scope_narrowing::Narrowing;
use crate::scope_order::{column_sort_key, sorted_limit, LastRow, SortedLimit};
use crate::scope_term::{Affinity, RunValues, ScopeTerm};
use crate::scope_window::{read_window, Window};
use crate::select_syntax::{
    conjuncts, number_parameters, one_select, parts, window_parts, Functions, Part,
};
use crate::sql::unparenthesized;
use crate::sql_value::SqlValue as Value;
use crate::table_places::Places;
use crate::unread_columns::{ColumnRef, OutputRead};
use crate::unreported_columns::{unreported_columns, Node, UnreportedColumns};
use crate::view_trees::ViewTrees;
use rusqlite::{Connection, Statement};
use sqlite3_parser::{
    ast::{As, Expr, Name, OneSelect, ResultColumn, Select},
    Bump,
};

/// The rows of each table a statement can depend on, before its values are
/// bound.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ScopePlan {
    /// Why the planner couldn't read the statement, when its shape keeps
    /// every table it reads whole.
    pub(crate) sources: Result<Sources, String>,
    /// The columns its rows depend on that SQLite's authorizer doesn't
    /// report, or unknown when the parser can't read the statement
    /// (`unreported_columns.rs`).
    pub(crate) unreported: Arc<UnreportedColumns>,
    /// How much of the statement the planner read.
    pub size: ScopePlanSize,
}

/// How much of a statement the planner read to build its plan, up to where
/// it stopped, if it did: what it holds before it passes edges into
/// subqueries, CTEs and views as it orders the sources.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScopePlanSize {
    /// Sources: the items of each FROM, those of the subqueries, CTEs and
    /// views read as their own included.
    pub sources: usize,
    /// SELECT cores.
    pub cores: usize,
    /// Edges between the sources: each an equality that the rows of one
    /// source meet with a value of another's, by which the planner narrows
    /// it. One equality can give an edge each way, and an OR's sides give
    /// none.
    pub edges: usize,
}

#[cfg(any(test, feature = "test-utils"))]
impl std::ops::AddAssign for ScopePlanSize {
    fn add_assign(&mut self, other: Self) {
        self.sources += other.sources;
        self.cores += other.cores;
        self.edges += other.edges;
    }
}

impl ScopePlan {
    /// The plan for `sql`, prepared as `statement`, up to the first NUL
    /// (`sql::up_to_nul`). `schema` is the main schema as `connection` sees
    /// it.
    pub fn build(
        connection: &Connection,
        schema: &MainSchema,
        sql: &str,
        statement: &Statement<'_>,
    ) -> rusqlite::Result<Self> {
        Ok(Self::read(connection, schema, sql, statement, Places::Skip)?.0)
    }

    /// The plan `build` builds, with the names it looked up.
    pub(crate) fn read(
        connection: &Connection,
        schema: &MainSchema,
        sql: &str,
        statement: &Statement<'_>,
        places: Places,
    ) -> rusqlite::Result<(Self, Found)> {
        let schema = &SchemaLookups::new(schema);
        let unread = |why: &str| -> rusqlite::Result<(Self, Found)> {
            let plan = Self {
                sources: Err(why.to_owned()),
                unreported: Arc::new(UnreportedColumns::unknown(schema, why.to_owned())?),
                size: ScopePlanSize::default(),
            };
            Ok((plan, Found::default()))
        };
        let written = sql;
        let sql = number_parameters(sql, statement)?;
        let bump = Bump::new();
        // A SQL form the parser can't represent depends on whole tables.
        let select = match one_select(&bump, &sql) {
            Ok(select) => select,
            Err(why) => return unread(&why.to_string()),
        };
        let functions = Functions::read(connection)?;
        let views = ViewTrees::new(&bump);
        let (unreported, picked) =
            unreported_columns(connection, schema, &functions, &views, select)?;
        let written = (places == Places::Find).then_some(written);
        let (sources, size) =
            Sources::read(connection, schema, select, &functions, &views, &picked, written)?;
        let plan = Self {
            sources: sources.map_err(|Unread(why)| why.to_owned()),
            unreported: Arc::new(unreported),
            size,
        };
        Ok((plan, Found { objects: schema.looked_up(), functions: functions.asked() }))
    }

    /// Where a run bound to `parameters` finds the row its scope ends at, or
    /// `None` when no row of the run bounds it (`scope_order.rs`).
    pub fn last_row(&self, parameters: &[Value]) -> Option<LastRow> {
        self.sources.as_ref().ok()?.sorted.as_ref()?.last_row(RunValues::Run(parameters))
    }

    /// The scope of each table the statement reads as one of its sources,
    /// for a run bound to `parameters` that returned `last`, the ORDER BY's
    /// values of the row `last_row` names, when it returned that many rows. A
    /// table the plan doesn't name depends on every row.
    pub fn scopes(&self, parameters: &[Value], last: Option<&[Value]>) -> Scopes {
        let mut scopes = match &self.sources {
            Ok(sources) => crate::scope_lookup::scopes(sources, parameters, last),
            Err(_) => Scopes::default(),
        };
        scopes.unreported = Arc::clone(&self.unreported);
        scopes
    }
}

/// What building a plan looked up: the names of the main schema's objects,
/// ASCII lowercased, and the names of functions.
#[derive(Default)]
pub(crate) struct Found {
    pub(crate) objects: BTreeSet<String>,
    pub(crate) functions: BTreeSet<String>,
}

/// A statement's sources, in the order their scopes are found.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct Sources {
    pub(crate) sources: Vec<Source>,
    /// Positions in `sources`, each after the sources its memberships use.
    pub(crate) order: Vec<usize>,
    /// The app's views and the CTEs read as sources of their own, by the
    /// names, lowercase, SQLite reports reads through them by, but none that
    /// SQLite can also report a read through inside a view read whole: such
    /// a read could be the view's, whose tables no source narrows. `None`
    /// when it reads a view whole that the parser can't read, whose SQL could
    /// use any name.
    pub(crate) inlined: Option<BTreeSet<String>>,
    /// The shadow tables of the contentful FTS5 tables it reads, lowercase.
    /// FTS5 reads them for the statement as it runs, and SQLite reports
    /// those reads, but they read what the FTS5 table's rows show: sqlive
    /// records a change of the content table's rows as the FTS5 table's,
    /// and a change of its configuration as a change of every row of it,
    /// and the rest of its index changes no row the table gives.
    pub(crate) fts5_shadows: BTreeSet<String>,
    /// The order and LIMIT a full run's last row bounds its source by.
    pub(crate) sorted: Option<SortedLimit>,
    /// The subqueries that keep the first rows of each window, which
    /// narrow their tables' own scopes (`scope_window.rs`).
    pub(crate) windows: Vec<Window>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct Source {
    pub(crate) object: Object,
    /// The FROM item as sqlive's own SQL names it, with its qualifier;
    /// `None` for a table function of another source's columns, or a
    /// subquery or CTE whose SQL names a row around it, whose rows exist
    /// only beside that row and so can't be read alone.
    pub(crate) from: Option<String>,
    /// How the statement names this source's columns.
    pub(crate) qualifier: String,
    /// Conditions on its rows' columns alone: those its core's conditions
    /// put on it, and those passed into a subquery, CTE or view.
    pub(crate) term: Option<ScopeTerm>,
    /// What the equalities between it and the other sources of its core
    /// carry to it from their terms (`Planner::carry_terms`), and what a
    /// subquery, CTE or view around it passes in of what they carry to it
    /// (`derived::push_carried`). It narrows the source's scope, and decides
    /// nothing of the order the sources take values in.
    pub(crate) carried: Option<ScopeTerm>,
    /// Where the SQL names its table or view: the CTE or view that writes
    /// it, when one does, or the statement's own SQL, in a plan built to
    /// report its places.
    pub(crate) place: Option<ReadPlace>,
    /// Whether only a column nothing reads reads it, so the statement
    /// depends on none of its rows (`unread_columns.rs`).
    pub(crate) unread: bool,
    /// The columns of the table it reads that the statement names through it,
    /// those it reads and those it doesn't (`reference_columns.rs`).
    pub(crate) columns: ReferenceColumns,
    /// Columns that must equal a value an earlier source holds.
    pub(crate) memberships: Vec<Membership>,
    /// The ORs that narrow it, each a list of sides whose memberships take
    /// values from sources placed before it (`scope_or.rs`).
    pub(crate) either: Vec<Vec<crate::scope_or::EitherSide>>,
}

impl Source {
    /// The table whose rows this source reads, lowercase; `None` for a FROM
    /// item sqlive reads only for its values.
    pub(crate) fn table(&self) -> Option<&str> {
        match &self.object {
            Object::Table { name, .. } => Some(name),
            Object::View { .. } | Object::Recursive { .. } | Object::Function | Object::Derived => {
                None
            }
        }
    }
}

/// What a source reads its rows from.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) enum Object {
    /// A table, by its name, lowercase.
    Table {
        name: String,
        /// The innermost CTE or app view the source sits in, lowercase, which
        /// SQLite reports its reads through.
        within: Option<String>,
    },
    /// A view read whole, by its name, lowercase, why it is, and the names
    /// SQLite can report a read through inside it (`whole_view_names.rs`),
    /// unless the parser can't read them.
    View { name: String, why: String, inside: Option<BTreeSet<String>> },
    /// A CTE whose SQL names itself, read whole: the tables, lowercase, its
    /// SQL reads, through the CTEs and views it reads too, and the names
    /// SQLite can report a read through while it runs the CTE
    /// (`recursive_cte.rs`), unless the parser can't read them.
    Recursive { tables: BTreeSet<String>, inside: Option<BTreeSet<String>> },
    /// A table function.
    Function,
    /// A subquery, CTE or view whose own tables sqlive reads as sources of
    /// their own.
    Derived,
}

/// Why the planner can't read a statement's, a subquery's, a CTE's or a
/// view's tables as sources: the shape that stops it.
#[derive(Clone, Copy)]
pub(crate) struct Unread(pub(crate) &'static str);

/// `member` equals `value` over some row of `partner` in its scope, a NULL
/// matching a NULL when `nulls`.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Membership {
    pub(crate) member: MemberValue,
    pub(crate) affinity: Affinity,
    pub(crate) partner: usize,
    pub(crate) value: PartnerValue,
    pub(crate) nulls: bool,
    /// Whether the partner sits in a core inside the member's
    /// (`Nesting::nested`).
    pub(crate) nested: bool,
    /// Whether the planner passed the edge into a derived item's arm.
    pub(crate) pushed: bool,
}

impl Membership {
    /// The membership `edge` gives its member.
    pub(crate) fn of(edge: &Edge) -> Self {
        Self {
            member: edge.member.value.clone(),
            affinity: edge.member.affinity,
            partner: edge.partner,
            value: edge.value.clone(),
            nulls: edge.nulls,
            nested: edge.nesting.nested(),
            pushed: edge.pushed,
        }
    }
}

/// What a membership compares with.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum PartnerValue {
    /// One of the partner's columns, lowercase, of the same affinity.
    Column(String),
    /// An expression over the partner's columns, as SQL, of no affinity
    /// sqlive knows: a lookup gives the member column's to the value it
    /// compares.
    Expression(String),
}

/// One SELECT core: the statement itself, an arm of a compound, or a
/// subquery anywhere in either.
pub(crate) struct Core {
    /// Which core of the statement's or a view's syntax it reads.
    pub(crate) node: Node,
    /// Whether a `*` of its result columns spans columns sqlive doesn't
    /// know.
    pub(crate) unknown_star: bool,
    /// The core a subquery sits in, whose names its SQL can name too.
    parent: Option<usize>,
    /// Its FROM items, positions in the planner's items.
    pub(crate) items: Vec<usize>,
    /// The names its result columns are given, which SQLite resolves a bare
    /// name to when no FROM item has that column.
    aliases: Vec<String>,
    /// The item each name in its clauses resolves to, `None` for a name
    /// sqlive can't place.
    names: Vec<Option<usize>>,
    /// The columns its clauses but its result columns name, and what each
    /// result column reads (`unread_columns.rs`).
    pub(crate) clause_refs: Vec<ColumnRef>,
    pub(crate) outputs: Vec<OutputRead>,
    /// The window of the subquery whose one core this is
    /// (`scope_window.rs`).
    pub(crate) window: Option<Window>,
    /// Whether a lookup that evaluates the core's own clauses again at a
    /// write could get another answer than the query did
    /// (`Planner::volatile`), a subquery in them counting by its SELECT's, or
    /// it reads a view read whole.
    pub(crate) volatile: bool,
}

impl Core {
    /// The core it sits in.
    pub(crate) fn parent(&self) -> Option<usize> {
        self.parent
    }
}

/// Where a condition was written: the WHERE or an inner join's ON, or the
/// ON of the LEFT JOIN that adds the item at this position.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Place {
    Inner,
    LeftOn(usize),
}

impl Sources {
    /// The sources of `select`, whose views are read in `views` and whose
    /// SELECTs and cores `picked` keep rows SQLite picks, and how much of it
    /// the planner read.
    fn read(
        connection: &Connection,
        schema: &SchemaLookups<'_>,
        select: &Select<'_>,
        functions: &Functions,
        views: &ViewTrees<'_>,
        picked: &BTreeSet<Node>,
        written: Option<&str>,
    ) -> rusqlite::Result<(Result<Self, Unread>, ScopePlanSize)> {
        let mut planner = Planner {
            items: Vec::new(),
            cores: Vec::new(),
            edges: Vec::new(),
            schema,
            functions,
            inlined: Vec::new(),
            fts5_shadows: Vec::new(),
            within: None,
            declared_ctes: 0,
            selects: Vec::new(),
            views,
            whole_names: Some(BTreeSet::new()),
        };
        let read = read_select(connection, &mut planner, select, None, &Ctes::default())?;
        let size = ScopePlanSize {
            sources: planner.items.len(),
            cores: planner.cores.len(),
            edges: planner.edges.len(),
        };
        let cores = match read {
            Ok(cores) => cores,
            Err(unread) => return Ok((Err(unread), size)),
        };
        let sorted = match cores[..] {
            [core] => sorted_limit(connection, &planner, select, core)?,
            _ => None,
        };
        let live = planner.live();
        for item in planner.unread_items(&live) {
            planner.items[item].source.unread = true;
        }
        if let Some(written) = written {
            planner.place_items(written, select);
        }
        planner.set_sort_keys();
        let columns = planner.reference_columns(&live, picked)?;
        for (item, columns) in planner.items.iter_mut().zip(columns) {
            item.source.columns = columns;
        }
        Ok((Ok(planner.ordered(sorted)), size))
    }
}

/// One SELECT the planner has read.
pub(crate) struct ReadSelect {
    pub(crate) node: Node,
    /// Its arms' cores.
    pub(crate) cores: Vec<usize>,
    /// Whether it is volatile (`Planner::select_volatile`).
    volatile: bool,
}

/// A statement's cores, their items and the edges between them, as the
/// `read_` functions find them; each of those gives the reason for a shape
/// that keeps every table whole.
pub(crate) struct Planner<'a, 'v> {
    pub(crate) items: Vec<FromItem>,
    /// Each core after the core it sits in.
    pub(crate) cores: Vec<Core>,
    pub(crate) edges: Vec<Edge>,
    pub(crate) schema: &'a SchemaLookups<'a>,
    /// The connection's functions, which tell an aggregate call from a
    /// scalar one.
    pub(crate) functions: &'a Functions,
    /// The app's views and the CTEs read as sources of their own, by the
    /// names, lowercase, SQLite reports reads through them by.
    pub(crate) inlined: Vec<String>,
    /// The shadow tables of the contentful FTS5 tables it reads, lowercase
    /// (`Sources::fts5_shadows`).
    pub(crate) fts5_shadows: Vec<String>,
    /// The innermost CTE or app view whose SQL is being read, lowercase.
    pub(crate) within: Option<String>,
    /// How many CTEs the WITHs read so far declare, which numbers each.
    pub(crate) declared_ctes: usize,
    /// Each SELECT the planner has read, in the order it finished reading
    /// them.
    pub(crate) selects: Vec<ReadSelect>,
    /// Where the planner reads each view's syntax.
    pub(crate) views: &'a ViewTrees<'v>,
    /// The names, lowercase, SQLite can report a read through inside the
    /// views the statement reads whole (`whole_view_names.rs`); `None` once
    /// the parser can't read one of those, whose SQL could use any name.
    pub(crate) whole_names: Option<BTreeSet<String>>,
}

impl Planner<'_, '_> {
    /// Give each ordering of a column in each source's terms how that
    /// column sorts.
    fn set_sort_keys(&mut self) {
        for item in 0..self.items.len() {
            let from = &mut self.items[item];
            let mut term = from.source.term.take();
            let mut carried = from.source.carried.take();
            let mut ors = std::mem::take(&mut from.ors);
            let sides = ors.iter_mut().flatten().flat_map(|side| &mut side.terms);
            for found in term.iter_mut().chain(&mut carried).chain(sides) {
                found.set_keys(&|column| column_sort_key(self, item, column));
            }
            let from = &mut self.items[item];
            (from.source.term, from.source.carried, from.ors) = (term, carried, ors);
        }
    }

    /// Note `found`, the names SQLite can report a read through inside a
    /// view the statement reads whole (`whole_view_names.rs`), or a CTE that
    /// names itself (`recursive_cte.rs`).
    pub(crate) fn note_read_whole(&mut self, found: Result<BTreeSet<String>, String>) {
        match (found, &mut self.whole_names) {
            (Ok(found), Some(names)) => names.extend(found),
            (Ok(_), None) => {}
            (Err(why), _) => {
                tracing::debug!(
                    %why,
                    "sqlive can't read the names inside a view or CTE read whole, so no read \
                     through a view or CTE narrows"
                );
                self.whole_names = None;
            }
        }
    }

    /// The narrowing `JOIN right USING (column)` gives, as the ON it stands
    /// for: `left.column = right.column`, with `left` the leftmost item
    /// before `right` that has the column (`using_left`).
    fn using_narrowing(&self, left: (usize, String), right: (usize, String)) -> Narrowing {
        let edges = [(left.clone(), right.clone(), false), (right, left, true)].map(
            |((member, column), partner, first)| {
                let member = self.narrowed_column(member, column)?;
                self.column_edge(member.into(), partner, first)
            },
        );
        Narrowing { edges: edges.into_iter().flatten().collect(), ..Narrowing::default() }
    }

    /// The cores whose items a name written in `core` can name: `core`, then
    /// each core it sits in, outward.
    pub(crate) fn scope_of(&self, core: usize) -> impl Iterator<Item = usize> + '_ {
        let cores = &self.cores;
        std::iter::successors(Some(core), move |current| cores[*current].parent)
    }

    /// The item a column name written in `core` belongs to, and the column,
    /// lowercase, as SQLite resolves it: among the core's own items first,
    /// then those of each core it sits in. `None` when sqlive can't be sure.
    pub(crate) fn resolve(&self, core: usize, expr: &Expr<'_>) -> Option<(usize, String)> {
        let (qualifier, name) = match unparenthesized(expr) {
            Expr::Id(name) => (None, Name(name.0)),
            Expr::Qualified(table, name) => (Some(table), name.clone()),
            Expr::DoublyQualified(db, table, name) if *db == "main" => (Some(table), name.clone()),
            _ => return None,
        };
        for current in self.scope_of(core) {
            let items = self.cores[current]
                .items
                .iter()
                .map(|position| (*position, &self.items[*position]));
            match qualifier {
                Some(qualifier) => {
                    // The first item of that name decides: SQLite looks
                    // further out only when it lacks the column.
                    if let Some((position, item)) =
                        items.clone().find(|(_, item)| *qualifier == Name(&item.source.qualifier))
                    {
                        return Some((position, item.columns.named(&name)?));
                    }
                }
                None => {
                    // A function's columns are unknown: one of them could be it.
                    if items
                        .clone()
                        .any(|(_, item)| matches!(item.columns, Columns::Unknown { .. }))
                    {
                        return None;
                    }
                    // A USING join's right side leaves its compared
                    // columns' bare names to the item on the left.
                    let mut found = items.clone().filter_map(|(position, item)| {
                        let column = item.columns.named(&name)?;
                        (!item.using.contains(&column)).then_some((position, column))
                    });
                    if let Some(first) = found.next() {
                        return found.next().is_none().then_some(first);
                    }
                    // A column whose name sqlive doesn't know, a result
                    // column's name, or a rowid, which SQLite finds here
                    // before looking further out.
                    if items.clone().any(|(_, item)| !item.columns.complete())
                        || self.cores[current].aliases.iter().any(|alias| name == Name(alias))
                        || crate::schema_objects::ROWID_NAMES.iter().any(|rowid| name == *rowid)
                    {
                        return None;
                    }
                }
            }
        }
        None
    }

    /// Note the items the names among `found`, written in `core`, resolve
    /// to.
    fn note_names(&mut self, core: usize, found: &[Part<'_>]) {
        let names: Vec<Option<usize>> = found
            .iter()
            .filter_map(|part| match part {
                Part::Name(name) => Some(self.resolve(core, name).map(|(item, _)| item)),
                _ => None,
            })
            .collect();
        self.cores[core].names.extend(names);
    }

    /// Whether `core` reads a view or CTE read whole, which can show what
    /// the host holds in memory as well as its tables, itself or through a
    /// subquery, CTE or view in its FROM.
    pub(crate) fn reads_whole(&self, core: usize) -> bool {
        self.cores[core].items.iter().any(|item| {
            let item = &self.items[*item];
            item.is_read_whole()
                || item
                    .derived
                    .as_ref()
                    .is_some_and(|derived| derived.cores.iter().any(|arm| self.reads_whole(*arm)))
        })
    }

    /// Whether `select`, which the planner has read, is volatile: its cores'
    /// clauses, ORDER BY and LIMIT, or any SELECT it read (its subqueries, and
    /// the subqueries, CTEs and views in its FROM).
    pub(crate) fn select_volatile(&self, select: &Select<'_>) -> bool {
        let node = Node::query(select);
        self.selects
            .iter()
            .rev()
            .find(|read| read.node == node)
            .expect("the planner reads a SELECT before it asks whether it is volatile")
            .volatile
    }

    /// Whether a FROM subquery or CTE read in a core that sits in `parent`,
    /// as the items from `items` on and the cores from `cores` on, can be
    /// read alone: its core sits in no other, or every name in its cores'
    /// clauses names one of its own items and each CTE it reads whole reads
    /// alone, so it returns the same rows wherever the statement reads it.
    pub(crate) fn alone(&self, parent: Option<usize>, items: usize, cores: usize) -> bool {
        parent.is_none()
            || (self.cores[cores..]
                .iter()
                .all(|core| core.names.iter().all(|item| item.is_some_and(|item| item >= items)))
                && self.items[items..].iter().all(|item| {
                    !matches!(item.source.object, Object::Recursive { .. })
                        || item.source.from.is_some()
                }))
    }

    /// The numbers of the CTEs that the items from `items` on read.
    pub(crate) fn ctes_read(&self, items: usize) -> Vec<usize> {
        self.items[items..].iter().flat_map(|item| item.ctes.iter().copied()).collect()
    }
}

/// Why a statement that names a contentful FTS5 table's hidden column stops
/// (`Planner::names_hidden`).
const HIDDEN_COLUMN: Unread = Unread("an FTS5 table's hidden column");

/// Read `select`, which sits in the core `parent`, if any, where its FROM
/// items can name `ctes`: the cores of its arms.
pub(crate) fn read_select<'a>(
    connection: &Connection,
    planner: &mut Planner,
    select: &'a Select<'a>,
    parent: Option<usize>,
    ctes: &Ctes<'a>,
) -> rusqlite::Result<Result<Vec<usize>, Unread>> {
    let inner;
    let ctes = match &select.with {
        Some(with) => {
            inner = ctes.with(with, parent.is_none(), &mut planner.declared_ctes);
            &inner
        }
        None => ctes,
    };
    let arms = std::iter::once(&select.body.select)
        .chain(select.body.compounds.iter().flatten().map(|compound| &compound.select));
    let read_before = planner.selects.len();
    let mut cores = Vec::new();
    for arm in arms {
        match read_core(connection, planner, arm, parent, ctes)? {
            Ok(core) => cores.push(core),
            Err(unread) => return Ok(Err(unread)),
        }
    }
    let mut found = Vec::new();
    for term in select.order_by.iter().flat_map(|terms| terms.iter()) {
        parts(&term.expr, &mut found);
    }
    // A single core's ORDER BY can name its sources. SQLite matches each
    // term of a compound's to a result column instead, and refuses one that
    // holds a SELECT or `x IN table`.
    let order = match cores[..] {
        [core] => {
            if planner.names_hidden(core, &found) {
                return Ok(Err(HIDDEN_COLUMN));
            }
            planner.note_names(core, &found);
            let refs = planner.column_refs(core, &found);
            planner.cores[core].clause_refs.extend(refs);
            Some(core)
        }
        _ => None,
    };
    if let Err(unread) = read_nested(connection, planner, &found, order, ctes)? {
        return Ok(Err(unread));
    }
    let mut volatile = planner.volatile(cores[0], &found);
    // SQLite resolves LIMIT and OFFSET with no names around them.
    let mut found = Vec::new();
    for limit in select.limit.iter() {
        parts(&limit.expr, &mut found);
        if let Some(offset) = &limit.offset {
            parts(offset, &mut found);
        }
    }
    if let Err(unread) = read_nested(connection, planner, &found, None, ctes)? {
        return Ok(Err(unread));
    }
    volatile |= planner.volatile(cores[0], &found)
        || cores.iter().any(|core| planner.cores[*core].volatile)
        || planner.selects[read_before..].iter().any(|read| read.volatile);
    planner.selects.push(ReadSelect { node: Node::query(select), cores: cores.clone(), volatile });
    Ok(Ok(cores))
}

/// The SELECTs read in one core's clauses, each with its arms' cores.
pub(crate) type Subqueries<'a> = Vec<(&'a Select<'a>, Vec<usize>)>;

/// Read the SELECTs among `found`, each sitting in `parent`.
fn read_nested<'a>(
    connection: &Connection,
    planner: &mut Planner,
    found: &[Part<'a>],
    parent: Option<usize>,
    ctes: &Ctes<'a>,
) -> rusqlite::Result<Result<Subqueries<'a>, Unread>> {
    let mut read = Vec::new();
    for part in found {
        match part {
            Part::Name(_) | Part::Collation | Part::Function(..) | Part::Clock => {}
            Part::Select(select) | Part::Subquery(select) => {
                let cores = match read_select(connection, planner, select, parent, ctes)? {
                    Ok(cores) => cores,
                    Err(unread) => return Ok(Err(unread)),
                };
                if let (Some(_), [core]) = (parent, &cores[..]) {
                    planner.cores[*core].window = read_window(connection, planner, select, *core)?;
                }
                read.push((*select, cores));
            }
            // Its table is read outside any FROM, and so in every row.
            Part::Table(_) => return Ok(Err(Unread("`x IN table`"))),
        }
    }
    Ok(Ok(read))
}

/// Read one core, which sits in `parent`: its items, then the SELECTs in any
/// of its clauses, then its conditions, which a comparison with a subquery's
/// value reads the subquery's cores for.
fn read_core<'a>(
    connection: &Connection,
    planner: &mut Planner,
    select: &'a OneSelect<'a>,
    parent: Option<usize>,
    ctes: &Ctes<'a>,
) -> rusqlite::Result<Result<usize, Unread>> {
    let core = planner.cores.len();
    planner.cores.push(Core {
        node: Node::core(select),
        unknown_star: false,
        parent,
        items: Vec::new(),
        aliases: Vec::new(),
        names: Vec::new(),
        clause_refs: Vec::new(),
        outputs: Vec::new(),
        window: None,
        volatile: false,
    });
    let mut found = Vec::new();
    let (columns, from, where_clause, group_by, having, window_clause) = match select {
        OneSelect::Values(rows) => {
            for expr in rows.iter().flat_map(|row| row.iter()) {
                parts(expr, &mut found);
            }
            if planner.names_hidden(core, &found) {
                return Ok(Err(HIDDEN_COLUMN));
            }
            planner.note_names(core, &found);
            // VALUES reads every column it writes.
            planner.cores[core].clause_refs = planner.column_refs(core, &found);
            let read = read_nested(connection, planner, &found, Some(core), ctes)?;
            return Ok(read.map(|_| {
                planner.cores[core].volatile = planner.volatile(core, &found);
                core
            }));
        }
        OneSelect::Select {
            columns, from, where_clause, group_by, having, window_clause, ..
        } => (columns, from, where_clause, group_by, having, window_clause),
    };
    for column in columns.iter() {
        if let ResultColumn::Expr(expr, alias) = column {
            parts(expr, &mut found);
            if let Some(As::As(alias) | As::Elided(alias)) = alias {
                planner.cores[core].aliases.push(alias.0.to_owned());
            }
        }
    }
    let result_parts = found.len();
    let mut conditions: Vec<(Written<'_>, Place)> = Vec::new();
    if let Some(from) = from {
        if let Err(unread) = crate::from_item::read_from(
            connection,
            planner,
            from,
            None,
            (core, parent, ctes),
            &mut conditions,
            &mut found,
        )? {
            return Ok(Err(unread));
        }
    }
    if let Some(predicate) = where_clause {
        parts(predicate, &mut found);
        let mut joined = Vec::new();
        conjuncts(predicate, &mut joined);
        conditions.extend(joined.into_iter().map(|expr| (Written::Expr(expr), Place::Inner)));
    }
    for expr in group_by.iter().flat_map(|exprs| exprs.iter()) {
        parts(expr, &mut found);
    }
    if let Some(having) = having {
        parts(having, &mut found);
    }
    for definition in window_clause.iter().flat_map(|windows| windows.iter()) {
        window_parts(&definition.window, &mut found);
    }
    if planner.names_hidden(core, &found) {
        return Ok(Err(HIDDEN_COLUMN));
    }
    planner.note_names(core, &found);
    let subqueries = match read_nested(connection, planner, &found, Some(core), ctes)? {
        Ok(subqueries) => subqueries,
        Err(unread) => return Ok(Err(unread)),
    };
    planner.cores[core].volatile = planner.volatile(core, &found) || planner.reads_whole(core);
    // A `*` over columns sqlive doesn't know reads whatever it spans, and
    // every result column reads what it names then.
    let (clause_parts, outputs) = match crate::derived::arm_columns(planner, core, select) {
        Some(outputs) => (result_parts, planner.output_reads(core, &outputs, &subqueries)),
        None => {
            planner.cores[core].unknown_star =
                columns.iter().any(|column| !matches!(column, ResultColumn::Expr(..)));
            (0, Vec::new())
        }
    };
    let refs = planner.column_refs(core, &found[clause_parts..]);
    planner.cores[core].clause_refs.extend(refs);
    planner.cores[core].outputs = outputs;
    let mut read = Vec::new();
    for (condition, place) in conditions {
        let found = match condition {
            Written::Expr(condition) => {
                read_condition(connection, planner, core, condition, &subqueries)?
            }
            Written::Using { left, right } => {
                for (item, column) in [&left, &right] {
                    let column = ColumnRef::Of(*item, column.clone());
                    planner.cores[core].clause_refs.push(column);
                }
                planner.using_narrowing(left, right)
            }
        };
        read.push((found, place));
    }
    // The conditions a row of NULLs fails come first, as they make LEFT JOINs
    // inner, which the others see.
    let (others, rejecting): (Vec<_>, Vec<_>) =
        read.into_iter().partition(|(found, _)| found.may_hold_on_nulls());
    for (found, place) in rejecting.into_iter().chain(others) {
        match place {
            Place::Inner => planner.narrow(found),
            Place::LeftOn(item) => planner.narrow_on(item, found),
        }
    }
    planner.carry_terms(core);
    // What its conditions say of a subquery's, CTE's or view's rows alone
    // narrows that one's own cores. Its equalities with other sources do
    // once the order says which it takes values from.
    for item in planner.cores[core].items.clone() {
        let term = planner.items[item].source.term.clone();
        crate::derived::push_down(planner, item, term, Vec::new());
    }
    for item in planner.cores[core].items.clone() {
        if let Some(carried) = planner.items[item].source.carried.clone() {
            crate::derived::push_carried(planner, item, &carried);
        }
    }
    Ok(Ok(core))
}

/// A condition of a core as its SQL writes it.
pub(crate) enum Written<'a> {
    Expr(&'a Expr<'a>),
    /// `JOIN right USING (column)`: the item and column to its left that
    /// SQLite compares the right item's column with, and that one.
    Using {
        left: (usize, String),
        right: (usize, String),
    },
}

/// Read one condition of `core` into the terms and edges of the items
/// it narrows: only the core's own. A condition holding a SELECT doesn't
/// narrow, except a column compared with the value a subquery returns,
/// `subqueries` being the SELECTs the core's clauses hold, and an OR whose
/// sides narrow one source (`scope_or.rs`).
pub(crate) fn is_name(expr: &Expr<'_>) -> bool {
    matches!(unparenthesized(expr), Expr::Id(_) | Expr::Qualified(..) | Expr::DoublyQualified(..))
}
