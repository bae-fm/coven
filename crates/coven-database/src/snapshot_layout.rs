//! Where a snapshot's rows wait: one table per synced table, with a column
//! for each of the row's values, its cells' setters and their references'
//! parents, so one row of the snapshot is one SQL row. Staging writes them
//! on the input connection as the body streams; installing copies each to
//! the writer once and works on them there with SQL (§16.2, §21.2).

use crate::schema::TableSchema;
use crate::sql::identifier;
use crate::write_schema::WriteSchema;
use coven_merge::ForeignKey;

/// One synced table's columns as both connections lay them out.
pub(crate) struct TableLayout {
    pub(crate) table: String,
    /// The start of every column of coven's own, which no app column of the
    /// table starts with.
    prefix: String,
    /// The app's columns, in the table's order.
    pub(crate) columns: Vec<String>,
    pub(crate) generated: Vec<bool>,
    pub(crate) defaults: Vec<Option<String>>,
    /// Each reference a cell can carry: the column's position and the key.
    pub(crate) pairs: Vec<(usize, ForeignKey)>,
}

impl TableLayout {
    pub(crate) fn new(schema: &crate::schema::Schema, table: &TableSchema) -> Self {
        let mut prefix = String::from("coven_");
        while table
            .columns
            .iter()
            .any(|column| column.name.to_ascii_lowercase().starts_with(&prefix))
        {
            prefix.push('_');
        }
        let mut pairs = Vec::new();
        for foreign in &table.foreign_keys {
            let key = schema.foreign_key(table, foreign);
            for column in &key.columns.0 {
                let at = table
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(column))
                    .expect("a reference column is a column of its table");
                pairs.push((at, key.clone()));
            }
        }
        Self {
            table: table.name.clone(),
            prefix,
            columns: table.columns.iter().map(|c| c.name.clone()).collect(),
            generated: table.columns.iter().map(|c| c.generated).collect(),
            defaults: table.columns.iter().map(|c| c.default.clone()).collect(),
            pairs,
        }
    }

    /// One of coven's columns, by its role.
    pub(crate) fn column(&self, name: &str) -> String {
        identifier(&format!("{}{name}", self.prefix))
    }
    pub(crate) fn value(&self, column: usize) -> String {
        self.column(&format!("v{column}"))
    }
    pub(crate) fn setter_device(&self, column: usize) -> String {
        self.column(&format!("d{column}"))
    }
    pub(crate) fn setter_number(&self, column: usize) -> String {
        self.column(&format!("n{column}"))
    }
    pub(crate) fn parent_key(&self, pair: usize) -> String {
        self.column(&format!("k{pair}"))
    }
    pub(crate) fn parent_audience(&self, pair: usize) -> String {
        self.column(&format!("a{pair}"))
    }
    pub(crate) fn parent_generation(&self, pair: usize) -> String {
        self.column(&format!("g{pair}"))
    }

    /// Coven's columns in the order both connections read and write them:
    /// the row's identity; `synced`, which columns its synced record has,
    /// as a `1` or `0` each, or NULL without one; `merged`, whether a merge
    /// record names it; its current generation and that generation's write;
    /// how many earlier generations it has; `keyed`, whether its synced
    /// record has every key column; `present`, whether its merge record is
    /// at an odd generation; then its values, its cells' setters and its
    /// cells' parents.
    pub(crate) fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = [
            "key",
            "audience",
            "synced",
            "merged",
            "generation",
            "device",
            "number",
            "earlier",
            "keyed",
            "present",
        ]
        .iter()
        .map(|name| self.column(name))
        .collect();
        names.extend((0..self.columns.len()).map(|j| self.value(j)));
        for j in 0..self.columns.len() {
            names.push(self.setter_device(j));
            names.push(self.setter_number(j));
        }
        for q in 0..self.pairs.len() {
            names.push(self.parent_key(q));
            names.push(self.parent_audience(q));
            names.push(self.parent_generation(q));
        }
        names
    }

    /// Positions in [`Self::names`].
    pub(crate) const KEY: usize = 0;
    pub(crate) const AUDIENCE: usize = 1;
    pub(crate) const SYNCED: usize = 2;
    pub(crate) const MERGED: usize = 3;
    pub(crate) const GENERATION: usize = 4;
    pub(crate) const DEVICE: usize = 5;
    pub(crate) const NUMBER: usize = 6;
    pub(crate) const EARLIER: usize = 7;
    pub(crate) const KEYED: usize = 8;
    pub(crate) const PRESENT: usize = 9;
    const FIELDS: usize = 10;
    pub(crate) fn value_at(&self, column: usize) -> usize {
        Self::FIELDS + column
    }
    pub(crate) fn setter_at(&self, column: usize) -> usize {
        Self::FIELDS + self.columns.len() + 2 * column
    }
    pub(crate) fn parent_at(&self, pair: usize) -> usize {
        Self::FIELDS + 3 * self.columns.len() + 3 * pair
    }
    /// How many columns [`Self::names`] gives.
    pub(crate) fn width(&self) -> usize {
        self.parent_at(self.pairs.len())
    }

    /// A staging table of `staging` on the input connection: its synced
    /// rows, or its merge rows.
    pub(crate) fn staged(&self, staging: i64, rows: Staged) -> String {
        let rows = match rows {
            Staged::Synced => "synced",
            Staged::Merged => "merged",
        };
        format!(
            "temp.{}",
            identifier(&format!("_coven_staged_{staging}_{rows}_{}", self.table))
        )
    }
    /// The writer's table of the snapshots being installed.
    pub(crate) fn installed(&self) -> String {
        format!(
            "temp.{}",
            identifier(&format!("_coven_install_{}", self.table))
        )
    }

    /// The input connection's tables: coven's columns only, untyped, so each
    /// value keeps the storage class the snapshot gave it.
    pub(crate) fn create_staged(&self, staging: i64, rows: Staged) -> String {
        format!(
            "CREATE TEMP TABLE {}({},UNIQUE({},{}));",
            self.staged(staging, rows).trim_start_matches("temp."),
            self.names().join(","),
            self.column("key"),
            self.column("audience")
        )
    }

    /// The writer's table: coven's columns, then the app's as SQLite would
    /// store them, with their declared types, collations and generated
    /// expressions, which the table's CHECK and unique expressions read.
    pub(crate) fn create_installed(&self, schema: &WriteSchema) -> String {
        format!(
            "CREATE TEMP TABLE {}({},{},UNIQUE({},{}));",
            self.installed().trim_start_matches("temp."),
            self.names().join(","),
            schema.rules[&self.table].columns.join(","),
            self.column("key"),
            self.column("audience")
        )
    }
}

/// The two kinds of row a snapshot stages, each in a table of its own.
#[derive(Clone, Copy)]
pub(crate) enum Staged {
    /// Synced records: identity, which columns, values and parents.
    Synced,
    /// Merge records: identity, generations, cell values, setters and
    /// parents.
    Merged,
}

/// The layout of every synced table, in declaration order.
pub(crate) struct SnapshotLayout {
    pub(crate) tables: Vec<TableLayout>,
}

impl SnapshotLayout {
    pub(crate) fn new(schema: &WriteSchema) -> Self {
        Self {
            tables: schema
                .declarations
                .iter()
                .map(|declaration| {
                    TableLayout::new(&schema.schema, schema.table(&declaration.name))
                })
                .collect(),
        }
    }

    /// The layout of a synced table; the caller checked that it syncs.
    pub(crate) fn table(&self, name: &str) -> &TableLayout {
        &self.tables[self.position(name)]
    }
    /// Where a synced table is in [`Self::tables`]; the caller checked that
    /// it syncs.
    pub(crate) fn position(&self, name: &str) -> usize {
        self.tables
            .iter()
            .position(|table| table.table == name)
            .expect("a synced table's layout")
    }
}
