//! Each table of a connection's main schema as coven's change captures read
//! it: its columns, whether a key names its rows, and the virtual table a
//! shadow table belongs to. A connection reads them once for each schema,
//! which SQLite's schema cookie names, so no capture reads a table by the
//! shape an earlier schema gave it, and no commit reads them again.

use super::DatabaseConnection;
use crate::DbError;
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
    /// The virtual table a shadow table belongs to, lowercase.
    pub(crate) parent: Option<String>,
}

/// Every table of the main schema but SQLite's own, as the schema was when
/// SQLite's schema cookie held `cookie`.
pub(crate) struct TableShapes {
    cookie: i64,
    positions: HashMap<String, usize>,
    shapes: Vec<TableShape>,
}

impl TableShapes {
    fn read(database: &DatabaseConnection, cookie: i64) -> Result<Self, DbError> {
        let tables = database.query(
            "SELECT name,type FROM pragma_table_list
             WHERE schema='main' AND type IN ('table','shadow','virtual') AND name NOT GLOB 'sqlite_*'",
            [],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )?;
        // SQLite's built-in virtual modules name shadow tables with the owning
        // table's name and a suffix. Longest prefix disambiguates underscores.
        let mut virtuals: Vec<String> = tables
            .iter()
            .filter(|(_, kind)| kind == "virtual")
            .map(|(name, _)| name.to_ascii_lowercase())
            .collect();
        virtuals.sort_by_key(|name| std::cmp::Reverse(name.len()));
        let mut positions = HashMap::new();
        let mut shapes = Vec::new();
        for (table, kind) in tables {
            if kind == "virtual" {
                continue;
            }
            let name = table.to_ascii_lowercase();
            let columns = database.query(
                "SELECT name,pk,hidden FROM pragma_table_xinfo(?1,'main') ORDER BY cid",
                [&table],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?.to_ascii_lowercase(),
                        r.get::<_, bool>(1)?,
                        r.get::<_, u32>(2)? != 0,
                    ))
                },
            )?;
            let parent = (kind == "shadow").then(|| {
                virtuals
                    .iter()
                    .find(|parent| name.starts_with(&format!("{parent}_")))
                    .expect("shadow table has a virtual-table owner")
                    .clone()
            });
            positions.insert(table, shapes.len());
            shapes.push(TableShape {
                whole: !columns.iter().any(|(_, key, _)| *key),
                name,
                columns,
                parent,
            });
        }
        Ok(Self { cookie, positions, shapes })
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

    /// The virtual table `table`, named in lowercase, belongs to: none for a
    /// table that isn't a shadow table, SQLite's own among them.
    pub(crate) fn parent(&self, table: &str) -> Option<&str> {
        self.shapes.iter().find(|shape| shape.name == table)?.parent.as_deref()
    }
}

impl DatabaseConnection {
    /// The shapes of the main schema's tables as the connection has the
    /// schema now: those read before, while SQLite's schema cookie still
    /// names the schema they were read in, else read afresh.
    pub(crate) fn table_shapes(&self) -> Result<Arc<TableShapes>, DbError> {
        let cookie = self.app_schema_cookie()?;
        if let Some(shapes) = self.shapes.borrow().as_ref().filter(|shapes| shapes.cookie == cookie)
        {
            return Ok(Arc::clone(shapes));
        }
        let shapes = Arc::new(TableShapes::read(self, cookie)?);
        self.shapes.replace(Some(Arc::clone(&shapes)));
        Ok(shapes)
    }
}
