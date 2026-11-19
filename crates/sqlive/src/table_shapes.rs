//! Each table of a connection's main schema as sqlive's change captures read
//! it: its columns, whether a key names its rows, and what a shadow table's
//! change means for the virtual table it belongs to. A contentful FTS5
//! table's content table holds its rows, so the table has a shape of its
//! own, the content table's renamed, under which a capture records each of
//! the content table's row changes a second time. A connection reads them
//! once for each schema, which SQLite's schema cookie names, so no capture
//! reads a table by the shape an earlier schema gave it, and no commit reads
//! them again.

use super::HostSql;
use crate::authorization::Policy;
use crate::authorization::PolicyError;
use std::collections::HashMap;
use std::sync::Arc;

/// One table of the main schema.
pub(crate) struct TableShape {
    /// Lowercase, as live queries record what they read.
    pub(crate) name: String,
    /// Each column at its pre-update position, in declared order: lowercase
    /// name, whether it is part of the key, and whether it is hidden or
    /// generated.
    pub(crate) columns: Vec<(String, bool, bool)>,
    /// No key can name its rows' changes: the table has no primary key.
    pub(crate) whole: bool,
    /// What a change to it means for the virtual table it belongs to, when
    /// it is a shadow table.
    pub(crate) shadow: Option<Shadow>,
}

/// What a change to a shadow table means for the virtual table it belongs
/// to.
pub(crate) enum Shadow {
    /// A change to every column of every row of the virtual table, named
    /// here in lowercase: SQLite's built-in modules keep their tables' rows
    /// in ways sqlive doesn't read.
    Whole(String),
    /// A contentful FTS5 table's content table: its rows are the virtual
    /// table's, the rowid in `id` and each column at its place after it, so
    /// each change is the same change of the virtual table's shape at this
    /// position. FTS5 rewrites a row an UPDATE changes, as a delete and an
    /// insert.
    Rows(usize),
    /// A contentful FTS5 table's index of its content table's values: FTS5
    /// changes it as the content table's rows change, or as it merges or
    /// rebuilds the index, which changes no row the table gives.
    Index,
}

/// Every table of the main schema but SQLite's own.
pub(crate) struct TableShapes {
    positions: HashMap<String, usize>,
    shapes: Vec<TableShape>,
}

impl TableShapes {
    fn read<P: Policy>(database: &HostSql<P>) -> Result<Self, PolicyError<P>> {
        let tables = database.query(crate::schema_objects::MAIN_TABLES, [], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        let columns = |table: &str| {
            database.query(
                "SELECT name,pk,hidden FROM pragma_table_xinfo(?1,'main') ORDER BY cid",
                [table],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?.to_ascii_lowercase(),
                        r.get::<_, bool>(1)?,
                        r.get::<_, u32>(2)? != 0,
                    ))
                },
            )
        };
        let mut shapes = Vec::new();
        // Each contentful FTS5 table's own shape, its rowid first, as its
        // content table's `id` is, and its content table, lowercase.
        let mut contents = HashMap::new();
        for (table, content) in database.query(crate::schema_objects::FTS5_CONTENT, [], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })? {
            let own = columns(&table)?.into_iter().filter(|(_, _, hidden)| !hidden);
            let columns = std::iter::once(("rowid".to_owned(), true, false))
                .chain(own.map(|(name, _, _)| (name, false, false)))
                .collect();
            contents.insert(content.to_ascii_lowercase(), shapes.len());
            shapes.push(TableShape {
                name: table.to_ascii_lowercase(),
                columns,
                whole: false,
                shadow: None,
            });
        }
        // SQLite's built-in virtual modules name shadow tables with the owning
        // table's name and a suffix. Longest prefix disambiguates underscores.
        let mut virtuals: Vec<String> = tables
            .iter()
            .filter(|(_, kind)| kind == "virtual")
            .map(|(name, _)| name.to_ascii_lowercase())
            .collect();
        virtuals.sort_by_key(|name| std::cmp::Reverse(name.len()));
        let mut positions = HashMap::new();
        for (table, kind) in tables {
            if kind == "virtual" {
                continue;
            }
            let name = table.to_ascii_lowercase();
            let columns = columns(&table)?;
            let shadow = (kind == "shadow").then(|| {
                let parent = virtuals
                    .iter()
                    .find(|parent| name.starts_with(&format!("{parent}_")))
                    .expect("shadow table has a virtual-table owner");
                if let Some(position) = contents.get(&name) {
                    return Shadow::Rows(*position);
                }
                let contentful = contents.contains_key(&format!("{parent}_content"));
                if contentful && name != format!("{parent}_config") {
                    Shadow::Index
                } else {
                    Shadow::Whole(parent.clone())
                }
            });
            positions.insert(table, shapes.len());
            shapes.push(TableShape {
                whole: !columns.iter().any(|(_, key, _)| *key),
                name,
                columns,
                shadow,
            });
        }
        Ok(Self { positions, shapes })
    }

    /// The position of `table`, as SQLite names it to a pre-update hook and
    /// in a changeset.
    pub(crate) fn position(&self, table: &str) -> Option<usize> {
        self.positions.get(table).copied()
    }

    pub(crate) fn shape(&self, position: usize) -> &TableShape {
        &self.shapes[position]
    }

    /// Each table with its position.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (usize, &TableShape)> {
        self.shapes.iter().enumerate()
    }

    /// The virtual table that a change to every row of `table`, named in
    /// lowercase, changes every row of too: none for a table that isn't a
    /// shadow table, SQLite's own among them, or an FTS5 table's index.
    pub(crate) fn parent(&self, table: &str) -> Option<&str> {
        self.shapes.iter().find(|shape| shape.name == table)?.parent(self)
    }
}

impl TableShape {
    /// The virtual table that a change to every row of this table changes
    /// every row of too, lowercase.
    pub(crate) fn parent<'a>(&'a self, shapes: &'a TableShapes) -> Option<&'a str> {
        match self.shadow.as_ref()? {
            Shadow::Whole(parent) => Some(parent),
            Shadow::Rows(position) => Some(&shapes.shape(*position).name),
            Shadow::Index => None,
        }
    }
}

impl<P: Policy> HostSql<P> {
    /// The shapes of the main schema's tables as the connection has the
    /// schema now: those read before, while SQLite's schema cookie still
    /// names the schema they were read in, else read afresh.
    pub(crate) fn table_shapes(&self) -> Result<Arc<TableShapes>, PolicyError<P>> {
        let cookie = self.app_schema_cookie()?;
        if let Some((read_in, shapes)) = self.shapes.borrow().as_ref() {
            if *read_in == cookie {
                return Ok(Arc::clone(shapes));
            }
        }
        let shapes = Arc::new(TableShapes::read(self)?);
        self.shapes.replace(Some((cookie, Arc::clone(&shapes))));
        Ok(shapes)
    }

    /// Run `change`, which moves SQLite's schema cookie without changing any
    /// table, so the shapes stay those of the schema it leaves: the next
    /// capture reads no table again.
    pub(crate) fn keeping_table_shapes<T, E: From<PolicyError<P>>>(
        &mut self,
        change: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        self.table_shapes()?;
        let changed = change()?;
        let cookie = self.schema_cookie().map_err(crate::Error::from)?;
        if let Some((read_in, _)) = self.shapes.borrow_mut().as_mut() {
            *read_in = cookie;
        }
        Ok(changed)
    }
}
