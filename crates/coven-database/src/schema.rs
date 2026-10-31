//! SQLite's actual schema, declaration checks, and per-migration comparison.

use std::collections::{BTreeMap, BTreeSet};

use crate::declaration::AudienceSource;
use crate::sql::{guarded_trigger, identifier, table_parts, tokens};
use crate::sqlite::DatabaseConnection;
use crate::{DbError, MigrationChange, RowIdentity, SchemaError, SyncedTable};

#[path = "schema_collation.rs"]
mod collation;

pub(crate) struct Schema {
    objects: BTreeMap<(String, String), SchemaObject>,
    pub(crate) tables: BTreeMap<String, TableSchema>,
}

#[derive(Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct SchemaObject {
    table: String,
    sql: Option<String>,
}

impl SchemaObject {
    /// Whether this is the app's trigger on one of coven's tables it may
    /// follow (§9.4): a local object, which syncs nothing and changes no
    /// row of the table it follows, so a migration comparing schemas passes
    /// over it as it does a local table's (§18.1).
    fn follows(&self) -> bool {
        crate::internal_schema::followed(&self.table)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct TableSchema {
    pub(crate) name: String,
    pub(crate) sql: String,
    pub(crate) columns: Vec<SchemaColumn>,
    pub(crate) indices: Vec<SchemaIndex>,
    pub(crate) foreign_keys: Vec<SchemaForeignKey>,
    pub(crate) without_rowid: bool,
    /// The SQL reading one row by its key, built the first time it's read.
    #[serde(skip)]
    pub(crate) row_lookup: std::sync::OnceLock<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct SchemaColumn {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) not_null: bool,
    pub(crate) default: Option<String>,
    pub(crate) primary_key: u32,
    pub(crate) generated: bool,
    /// The collation the column declares, BINARY when it names none.
    pub(crate) collation: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct SchemaIndex {
    pub(crate) name: String,
    pub(crate) sql: Option<String>,
    pub(crate) primary: bool,
    pub(crate) columns: Vec<Option<String>>,
    pub(crate) collations: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct SchemaForeignKey {
    pub(crate) target: String,
    pub(crate) columns: Vec<String>,
    pub(crate) target_columns: Vec<Option<String>>,
    pub(crate) on_update: String,
    pub(crate) on_delete: String,
    /// A file reference coven checks, not a key SQLite declares (§17.2).
    pub(crate) file: bool,
}

impl Schema {
    pub(crate) fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(&(self.objects.iter().collect::<Vec<_>>(), &self.tables))
            .expect("schema contains only JSON-compatible fields")
    }

    pub(crate) fn decode(bytes: &[u8]) -> rusqlite::Result<Self> {
        type Objects = Vec<((String, String), SchemaObject)>;
        let (objects, tables): (Objects, BTreeMap<String, TableSchema>) =
            serde_json::from_slice(bytes).map_err(|error| {
                crate::write_encoding::decode_failure(0, rusqlite::types::Type::Blob, error)
            })?;
        Ok(Self {
            objects: objects.into_iter().collect(),
            tables,
        })
    }

    /// The app's schema and coven's files table, with each of `files`, a
    /// table and column, read as a reference to a file (§17.2): a foreign key
    /// into `coven_files` that takes its row out, as restrict does, when its
    /// file is gone. SQLite knows nothing of these keys; coven checks them.
    pub(crate) fn read(
        db: &DatabaseConnection,
        files: &[(String, String)],
    ) -> Result<Self, DbError> {
        let objects = db.query(
            "SELECT type, name, tbl_name, sql FROM main.sqlite_schema WHERE name NOT GLOB 'sqlite_*' AND substr(lower(name),1,7) != '_coven_' ORDER BY type, name",
            [], |row| Ok(((row.get::<_, String>(0)?, row.get::<_, String>(1)?), SchemaObject { table: row.get(2)?, sql: row.get(3)? })),
        )?.into_iter()
            // Coven's views of its own state, its files and its table of
            // pins are coven's, not the app's; the app's triggers on that
            // table are the app's.
            .filter(|((kind, name), _)| {
                !((kind == "view" || kind == "table") && crate::internal_schema::app_readable(name))
            })
            .collect::<BTreeMap<_, _>>();
        let mut tables = BTreeMap::new();
        for ((kind, name), object) in &objects {
            if kind == "table" {
                let sql = object.sql.clone().expect("table has CREATE SQL");
                tables.insert(name.to_ascii_lowercase(), table(db, name, sql, &objects)?);
            }
        }
        let sql = db.query_row(
            "SELECT sql FROM main.sqlite_schema WHERE type='table' AND name=?1",
            [crate::internal_schema::FILES],
            |r| r.get::<_, String>(0),
        )?;
        tables.insert(
            crate::internal_schema::FILES.into(),
            table(db, crate::internal_schema::FILES, sql, &objects)?,
        );
        for (table_name, column) in files {
            let Some(table) = tables.get_mut(&table_name.to_ascii_lowercase()) else {
                continue;
            };
            let Some(column) = table
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(column))
                .map(|c| c.name.clone())
            else {
                continue;
            };
            table.foreign_keys.push(SchemaForeignKey::file(column));
        }
        Ok(Self { objects, tables })
    }

    pub(crate) fn foreign_key(
        &self,
        table: &TableSchema,
        key: &SchemaForeignKey,
    ) -> coven_merge::ForeignKey {
        let parent = &self.tables[&key.target.to_ascii_lowercase()];
        coven_merge::ForeignKey::new(
            coven_merge::ConstraintColumns(
                key.columns
                    .iter()
                    .map(|c| crate::write_rows::column_name(table, c).to_owned())
                    .collect(),
            ),
            parent.name.clone(),
            coven_merge::ConstraintColumns(crate::write_rows::target_columns(parent, key)),
        )
    }

    pub(crate) fn validate(&self, declarations: &[SyncedTable]) -> Result<(), DbError> {
        let mut declared = BTreeSet::new();
        for declaration in declarations {
            if crate::internal_schema::reserved_name(&declaration.name) {
                return Err(DbError::InternalTable {
                    table: declaration.name.clone(),
                });
            }
            if !declared.insert(declaration.name.to_ascii_lowercase()) {
                return Err(SchemaError::DuplicateTable {
                    table: declaration.name.clone(),
                }
                .into());
            }
        }
        for table in self.tables.values() {
            for key in &table.foreign_keys {
                // Coven deletes file records itself, when the last reference
                // goes (§17.10); a key SQLite enforces would restrict, cascade
                // or set its rows as coven does, and coven wouldn't see it as
                // a reference. A file column is how a row references a file.
                if !key.file
                    && key
                        .target
                        .eq_ignore_ascii_case(crate::internal_schema::FILES)
                {
                    return Err(SchemaError::FilesForeignKey {
                        table: table.name.clone(),
                        key: key.description(),
                    }
                    .into());
                }
                if (declared.contains(&table.name.to_ascii_lowercase())
                    || declared.contains(&key.target.to_ascii_lowercase()))
                    && [&key.on_delete, &key.on_update]
                        .iter()
                        .any(|action| *action == "SET DEFAULT")
                {
                    return Err(SchemaError::SetDefault {
                        table: table.name.clone(),
                        key: key.description(),
                    }
                    .into());
                }
            }
        }
        // A trigger on a table the app may follow, coven_files, coven_pins
        // or coven_upload_queue, follows coven's changes to it; run before
        // one, it could skip it with RAISE(IGNORE) and leave the table wrong.
        for ((kind, name), object) in &self.objects {
            if kind == "trigger"
                && object.follows()
                && !object.sql.as_deref().is_some_and(crate::sql::after_trigger)
            {
                return Err(SchemaError::TriggerTiming {
                    trigger: name.clone(),
                    table: object.table.to_ascii_lowercase(),
                }
                .into());
            }
        }
        let mut audience_parents = BTreeMap::new();
        for declaration in declarations {
            let error_table = || declaration.name.clone();
            let table = self
                .tables
                .get(&declaration.name.to_ascii_lowercase())
                .ok_or_else(|| SchemaError::MissingTable {
                    table: error_table(),
                })?;
            let mut key: Vec<_> = table.columns.iter().filter(|c| c.primary_key > 0).collect();
            key.sort_by_key(|c| c.primary_key);
            if key.is_empty() {
                return Err(SchemaError::NoPrimaryKey {
                    table: error_table(),
                }
                .into());
            }
            // INTEGER PRIMARY KEY aliases the rowid only when there is no
            // separate primary-key index. INTEGER PRIMARY KEY DESC has an index.
            let rowid_key = !table.without_rowid
                && key.len() == 1
                && key[0].kind.eq_ignore_ascii_case("integer")
                && !table.indices.iter().any(|i| i.primary);
            if rowid_key || key.iter().any(|c| c.default.is_some()) {
                return Err(SchemaError::GeneratedPrimaryKey {
                    table: error_table(),
                }
                .into());
            }
            if key.len() != declaration.key.len()
                || key
                    .iter()
                    .zip(&declaration.key)
                    .any(|(column, name)| !column.name.eq_ignore_ascii_case(name))
            {
                return Err(SchemaError::KeyColumns {
                    table: error_table(),
                }
                .into());
            }
            if let Some(column) = key.iter().find(|c| !c.not_null) {
                return Err(SchemaError::NullableKey {
                    table: error_table(),
                    column: column.name.clone(),
                }
                .into());
            }
            if declaration.identity == RowIdentity::IndependentUuid
                && !key.iter().any(|column| text_affinity(&column.kind))
            {
                return Err(SchemaError::IndependentKeyNotUuid {
                    table: error_table(),
                }
                .into());
            }
            for foreign_key in &table.foreign_keys {
                if foreign_key.replaces_reference() {
                    for column in &foreign_key.columns {
                        if key.iter().any(|k| k.name.eq_ignore_ascii_case(column)) {
                            return Err(SchemaError::PrimaryKeyAction {
                                table: error_table(),
                                column: column.clone(),
                            }
                            .into());
                        }
                    }
                }
            }
            for foreign_key in &table.foreign_keys {
                let targets_primary = self
                    .tables
                    .get(&foreign_key.target.to_ascii_lowercase())
                    .is_some_and(|parent| {
                        let primary = crate::write_rows::key_columns(parent);
                        !primary.is_empty()
                            && foreign_key.target_columns.len() == primary.len()
                            && (foreign_key.target_columns.iter().all(Option::is_none)
                                || primary.iter().all(|column| {
                                    foreign_key
                                        .target_columns
                                        .iter()
                                        .flatten()
                                        .filter(|target| target.eq_ignore_ascii_case(&column.name))
                                        .count()
                                        == 1
                                }))
                    });
                if !targets_primary {
                    return Err(SchemaError::ForeignKeyNotPrimaryKey {
                        table: table.name.clone(),
                        key: foreign_key.description(),
                    }
                    .into());
                }
            }
            let audience_key = match &declaration.audience {
                AudienceSource::Both { .. } => {
                    return Err(SchemaError::TwoAudiences {
                        table: error_table(),
                    }
                    .into())
                }
                AudienceSource::Store => None,
                AudienceSource::Column(column) => {
                    if !table.columns.iter().any(|c| {
                        c.name.eq_ignore_ascii_case(column) && c.not_null && text_affinity(&c.kind)
                    }) {
                        return Err(SchemaError::AudienceColumn {
                            table: error_table(),
                            column: column.clone(),
                        }
                        .into());
                    }
                    Some(column)
                }
                AudienceSource::ForeignKey(column) => {
                    let keys: Vec<_> = table
                        .foreign_keys
                        .iter()
                        .filter(|f| f.columns.iter().any(|c| c.eq_ignore_ascii_case(column)))
                        .collect();
                    if keys.iter().any(|f| f.columns.len() != 1) {
                        return Err(SchemaError::AudienceForeignKeyColumns {
                            table: error_table(),
                        }
                        .into());
                    }
                    let [foreign_key] = keys.as_slice() else {
                        return Err(SchemaError::AudienceForeignKeyTarget {
                            table: error_table(),
                            column: column.clone(),
                        }
                        .into());
                    };
                    if !declared.contains(&foreign_key.target.to_ascii_lowercase()) {
                        return Err(SchemaError::AudienceForeignKeyTarget {
                            table: error_table(),
                            column: column.clone(),
                        }
                        .into());
                    }
                    if foreign_key.replaces_reference() {
                        return Err(SchemaError::AudienceForeignKeyAction {
                            table: error_table(),
                            column: column.clone(),
                        }
                        .into());
                    }
                    audience_parents.insert(
                        declaration.name.to_ascii_lowercase(),
                        foreign_key.target.to_ascii_lowercase(),
                    );
                    Some(column)
                }
            };
            if let Some(audience_key) = audience_key {
                if declaration.identity == RowIdentity::SharedKey
                    && !key
                        .iter()
                        .any(|c| c.name.eq_ignore_ascii_case(audience_key))
                {
                    return Err(SchemaError::AudienceConstraint {
                        table: error_table(),
                        constraint: "PRIMARY KEY".into(),
                    }
                    .into());
                }
                for index in &table.indices {
                    if !index.primary
                        && !index.columns.iter().any(|c| {
                            c.as_ref()
                                .is_some_and(|c| c.eq_ignore_ascii_case(audience_key))
                        })
                    {
                        return Err(SchemaError::AudienceConstraint {
                            table: error_table(),
                            constraint: index.name.clone(),
                        }
                        .into());
                    }
                }
            }
            for trigger in &declaration.shared_triggers {
                let found = self.objects.iter().find(|((kind, name), obj)| {
                    kind == "trigger"
                        && name.eq_ignore_ascii_case(trigger)
                        && obj.table.eq_ignore_ascii_case(&table.name)
                });
                let (_, object) = found.ok_or_else(|| SchemaError::MissingTrigger {
                    table: error_table(),
                    trigger: trigger.clone(),
                })?;
                if !object.sql.as_deref().is_some_and(guarded_trigger) {
                    return Err(SchemaError::SharedTriggerGuard {
                        table: error_table(),
                        trigger: trigger.clone(),
                    }
                    .into());
                }
            }
            let mut files = BTreeSet::new();
            for file in &declaration.files {
                let column = table
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&file.column))
                    .ok_or_else(|| SchemaError::FileColumn {
                        table: error_table(),
                        column: file.column.clone(),
                    })?;
                if !files.insert(column.name.to_ascii_lowercase()) {
                    return Err(SchemaError::FileColumnRepeated {
                        table: error_table(),
                        column: column.name.clone(),
                    }
                    .into());
                }
                // Coven writes the column when its row moves to another
                // audience (§17.2), which a generated column or one SQLite
                // also checks against another table would refuse.
                if column.generated {
                    return Err(SchemaError::FileColumnGenerated {
                        table: error_table(),
                        column: column.name.clone(),
                    }
                    .into());
                }
                if table.foreign_keys.iter().any(|key| {
                    !key.is_file()
                        && key
                            .columns
                            .iter()
                            .any(|c| c.eq_ignore_ascii_case(&column.name))
                }) {
                    return Err(SchemaError::FileColumnForeignKey {
                        table: error_table(),
                        column: column.name.clone(),
                    }
                    .into());
                }
            }
        }
        self.validate_key_collations(&declared)?;
        self.validate_local_children(&declared)?;
        self.validate_local_updates(&declared)?;
        for table in self.tables.values() {
            for key in &table.foreign_keys {
                for source in &key.columns {
                    let column = table
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(source))
                        .expect("foreign key column");
                    let sets_null = key.on_delete == "SET NULL" || key.on_update == "SET NULL";
                    // A reference computed from several stored columns could
                    // merge into a parent no write named, and an action can't
                    // assign it (§9.1).
                    if column.generated {
                        return Err(SchemaError::GeneratedForeignKey {
                            table: table.name.clone(),
                            column: column.name.clone(),
                        }
                        .into());
                    }
                    if column.not_null && sets_null {
                        return Err(SchemaError::ImpossibleAction {
                            table: table.name.clone(),
                            column: column.name.clone(),
                        }
                        .into());
                    }
                }
            }
        }
        for declaration in declarations {
            let mut path = BTreeSet::new();
            let mut table = declaration.name.to_ascii_lowercase();
            while let Some(parent) = audience_parents.get(&table) {
                if !path.insert(table) {
                    return Err(SchemaError::AudienceCycle {
                        table: declaration.name.clone(),
                    }
                    .into());
                }
                table = parent.clone();
            }
        }
        Ok(())
    }

    pub(crate) fn changed_tables(&self, next: &Self) -> BTreeSet<String> {
        self.objects
            .iter()
            .filter(|(key, object)| next.objects.get(*key) != Some(*object))
            .chain(
                next.objects
                    .iter()
                    .filter(|(key, object)| self.objects.get(*key) != Some(*object)),
            )
            .filter(|((kind, _), object)| kind != "view" && !object.follows())
            .map(|(_, object)| object.table.to_ascii_lowercase())
            .collect()
    }

    pub(crate) fn change_to(&self, next: &Self, synced: &BTreeSet<String>) -> MigrationChange {
        // Final declarations cannot tell us whether a vanished table used to sync.
        if self
            .objects
            .keys()
            .any(|identity| identity.0 == "table" && !next.objects.contains_key(identity))
        {
            return MigrationChange::Breaking;
        }
        let mut change = MigrationChange::NoChange;
        for (identity, before) in &self.objects {
            let after = next.objects.get(identity);
            if identity.0 == "view"
                || before.follows()
                || (!synced.contains(&before.table.to_ascii_lowercase())
                    && !after
                        .is_some_and(|object| synced.contains(&object.table.to_ascii_lowercase())))
            {
                continue;
            }
            let Some(after) = after else {
                return MigrationChange::Breaking;
            };
            if before == after {
                continue;
            }
            if identity.0 != "table" || before.table != after.table {
                return MigrationChange::Breaking;
            }
            let parts = before
                .sql
                .as_deref()
                .and_then(table_parts)
                .zip(after.sql.as_deref().and_then(table_parts));
            let Some(((old, old_suffix), (new, new_suffix))) = parts else {
                return MigrationChange::Breaking;
            };
            if old_suffix != new_suffix {
                return MigrationChange::Breaking;
            }
            // Existing clauses must remain verbatim in token form. Additional
            // clauses must name new columns and introduce no constraints.
            let old_names: BTreeSet<_> = self.tables[&identity.1.to_ascii_lowercase()]
                .columns
                .iter()
                .map(|c| c.name.to_ascii_lowercase())
                .collect();
            let table_key = identity.1.to_ascii_lowercase();
            let retained = next.tables[&table_key]
                .columns
                .iter()
                .filter(|column| old_names.contains(&column.name.to_ascii_lowercase()))
                .map(|column| &column.name);
            if !self.tables[&table_key]
                .columns
                .iter()
                .map(|column| &column.name)
                .eq(retained)
            {
                return MigrationChange::Breaking;
            }
            let mut remaining = new;
            for clause in old {
                let Some(index) = remaining.iter().position(|c| *c == clause) else {
                    return MigrationChange::Breaking;
                };
                remaining.remove(index);
            }
            for clause in remaining {
                let column = clause.first().and_then(|token| match token {
                    crate::sql::Token::Word(name) | crate::sql::Token::Quoted(name) => {
                        Some(name.to_ascii_lowercase())
                    }
                    _ => None,
                });
                if column.is_none_or(|name| old_names.contains(&name))
                    || clause.iter().any(|t| {
                        [
                            "constraint",
                            "primary",
                            "unique",
                            "references",
                            "check",
                            "not",
                            "collate",
                            "generated",
                        ]
                        .iter()
                        .any(|w| t.word(w))
                    })
                {
                    return MigrationChange::Breaking;
                }
            }
            change = MigrationChange::Addition;
        }
        for ((kind, name), object) in &next.objects {
            if kind == "view"
                || object.follows()
                || !synced.contains(&object.table.to_ascii_lowercase())
            {
                continue;
            }
            if !self.objects.contains_key(&(kind.clone(), name.clone())) {
                if kind == "table" {
                    change = MigrationChange::Addition;
                    continue;
                }
                let table = object.table.to_ascii_lowercase();
                let on_new_table = matches!(kind.as_str(), "index" | "trigger")
                    && !self.tables.contains_key(&table)
                    && next.tables.contains_key(&table);
                if !on_new_table {
                    return MigrationChange::Breaking;
                }
            }
        }
        change
    }
}

/// One table's columns, unique indexes and foreign keys, as SQLite reports
/// them.
fn table(
    db: &DatabaseConnection,
    name: &str,
    sql: String,
    objects: &BTreeMap<(String, String), SchemaObject>,
) -> Result<TableSchema, DbError> {
    let mut columns = db.query("SELECT name, type, \"notnull\", dflt_value, pk, hidden != 0 FROM pragma_table_xinfo(?1, 'main') ORDER BY cid", [name], |r| {
        Ok(SchemaColumn { name: r.get(0)?, kind: r.get(1)?, not_null: r.get(2)?, default: r.get(3)?, primary_key: r.get(4)?, generated: r.get(5)?, collation: String::new() })
    })?;
    for column in &mut columns {
        column.collation = db.column_collation(name, &column.name)?;
    }
    let mut indices = Vec::new();
    for (index, primary) in db.query("SELECT name, origin = 'pk' FROM pragma_index_list(?1, 'main') WHERE \"unique\" ORDER BY seq", [name], |r| Ok((r.get::<_, String>(0)?, r.get::<_, bool>(1)?)))? {
        let (columns, collations) = db.query("SELECT name, coll FROM pragma_index_xinfo(?1, 'main') WHERE key ORDER BY seqno", [&index], |r| Ok((r.get::<_, Option<String>>(0)?, r.get::<_, String>(1)?)))?.into_iter().unzip();
        let sql = objects.get(&("index".into(), index.clone())).and_then(|object| object.sql.clone());
        indices.push(SchemaIndex { name: index, sql, primary, columns, collations });
    }
    let mut foreign_keys = BTreeMap::new();
    for (id, target, column, target_column, on_update, on_delete) in db.query("SELECT id, \"table\", \"from\", \"to\", on_update, on_delete FROM pragma_foreign_key_list(?1, 'main') ORDER BY id, seq", [name], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, Option<String>>(3)?, r.get::<_, String>(4)?, r.get::<_, String>(5)?)))? {
        let key = foreign_keys.entry(id).or_insert_with(|| SchemaForeignKey { target, columns: Vec::new(), target_columns: Vec::new(), on_update, on_delete, file: false });
        key.columns.push(column);
        key.target_columns.push(target_column);
    }
    let without_rowid = tokens(&sql)?
        .windows(2)
        .any(|p| p[0].word("without") && p[1].word("rowid"));
    Ok(TableSchema {
        name: name.to_owned(),
        sql,
        columns,
        indices,
        foreign_keys: foreign_keys.into_values().collect(),
        without_rowid,
        row_lookup: std::sync::OnceLock::new(),
    })
}

impl SchemaForeignKey {
    /// A file reference held in `column` (§17.2).
    fn file(column: String) -> Self {
        Self {
            target: crate::internal_schema::FILES.into(),
            columns: vec![column],
            target_columns: vec![Some("id".into())],
            on_update: "NO ACTION".into(),
            on_delete: "RESTRICT".into(),
            file: true,
        }
    }

    /// Whether this key is a file reference rather than one SQLite declares.
    pub(crate) fn is_file(&self) -> bool {
        self.file
    }

    fn description(&self) -> String {
        let columns = self
            .columns
            .iter()
            .map(|c| identifier(c))
            .collect::<Vec<_>>()
            .join(", ");
        let target_columns = self
            .target_columns
            .iter()
            .flatten()
            .map(|c| identifier(c))
            .collect::<Vec<_>>();
        let target_columns = if target_columns.is_empty() {
            String::new()
        } else {
            format!(" ({})", target_columns.join(", "))
        };
        format!(
            "({columns}) REFERENCES {}{target_columns}",
            identifier(&self.target)
        )
    }

    fn replaces_reference(&self) -> bool {
        [&self.on_update, &self.on_delete]
            .iter()
            .any(|s| s.eq_ignore_ascii_case("SET NULL"))
    }
}

fn text_affinity(kind: &str) -> bool {
    let kind = kind.to_ascii_uppercase();
    !kind.contains("INT") && ["CHAR", "CLOB", "TEXT"].iter().any(|s| kind.contains(s))
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
