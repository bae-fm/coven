//! Lazy indexed removal views before and after the merge's row updates.
use crate::merge_store::MergeStore;
use crate::schema::{SchemaForeignKey, TableSchema};
use crate::sql::identifier;
use crate::write_encoding::{audience, decoded, encoded, sql_value};
use crate::write_rows::{
    column_list, column_name, equality_key, key_columns, read_row, row_id, row_key, target_columns,
    AppValues, AppView,
};
use crate::write_schema::WriteSchema;
use crate::{declaration::AudienceSource, DbError};
use coven_format::value::Value;
use coven_foundation::id_source::CircleId;
use coven_merge::{
    Audience, Constraints, ForeignKey, Group, MergeError, OnDelete, Reference, ReferenceValue,
    RemovalRow, RemovalView, RowId, RowState, RowUpdate, Timestamp, WriteId,
};
use rusqlite::{params, params_from_iter};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) enum RemovalFailure {
    Database(DbError),
    Merge(MergeError),
}
impl RemovalFailure {
    pub(crate) fn into_db_error(self, write: Option<WriteId>) -> DbError {
        match self {
            Self::Database(error) => error,
            Self::Merge(error) => match write {
                Some(write) => DbError::InvalidWrite { write, error },
                None => panic!("stored removal view violates merge invariant: {error}"),
            },
        }
    }
}
impl From<DbError> for RemovalFailure {
    fn from(error: DbError) -> Self {
        Self::Database(error)
    }
}
impl From<MergeError> for RemovalFailure {
    fn from(error: MergeError) -> Self {
        Self::Merge(error)
    }
}

#[derive(Clone)]
pub(crate) struct EvaluatedRow {
    pub(crate) facts: RemovalRow,
    pub(crate) values: AppValues,
    pub(crate) constraints: Constraints,
    pub(crate) readings: BTreeMap<ForeignKey, ReferenceValue>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ReferenceTarget {
    table: String,
    columns: Vec<String>,
    value: Vec<u8>,
}

pub(crate) struct DatabaseRemovalView<'a> {
    database: &'a crate::sqlite::DatabaseConnection,
    store: &'a MergeStore<'a>,
    schema: &'a WriteSchema,
    app: &'a AppView<'a>,
    updates: &'a BTreeMap<RowId, RowUpdate<Value>>,
    deleted: &'a BTreeSet<CircleId>,
    arriving: Option<(WriteId, Timestamp)>,
    rows: RefCell<BTreeMap<RowId, EvaluatedRow>>,
    edges: BTreeMap<RowId, BTreeSet<RowId>>,
    extra_groups: RefCell<BTreeMap<Group, BTreeSet<RowId>>>,
    lookups: RefCell<BTreeMap<ReferenceTarget, BTreeSet<RowId>>>,
}

impl<'a> DatabaseRemovalView<'a> {
    pub(crate) fn new(
        database: &'a crate::sqlite::DatabaseConnection,
        store: &'a MergeStore<'a>,
        schema: &'a WriteSchema,
        app: &'a AppView<'a>,
        updates: &'a BTreeMap<RowId, RowUpdate<Value>>,
        deleted: &'a BTreeSet<CircleId>,
        arriving: Option<(WriteId, Timestamp)>,
    ) -> Result<Self, DbError> {
        let mut edges = BTreeMap::<RowId, BTreeSet<RowId>>::new();
        for (id, update) in updates {
            for value in update
                .state
                .cells()
                .values()
                .map(|c| &c.value)
                .chain(update.state.lost().values().map(|l| &l.value))
            {
                for parent in value.parents.values() {
                    edges
                        .entry(parent.row.clone())
                        .or_default()
                        .insert(id.clone());
                }
            }
        }
        Ok(Self {
            database,
            store,
            schema,
            app,
            updates,
            deleted,
            arriving,
            rows: RefCell::new(BTreeMap::new()),
            edges,
            extra_groups: RefCell::new(BTreeMap::new()),
            lookups: RefCell::new(BTreeMap::new()),
        })
    }

    pub(crate) fn prior(&self, id: &RowId) -> Result<crate::merge_store::StoredRow, DbError> {
        self.store.row(id)
    }

    pub(crate) fn state(&self, id: &RowId) -> Result<RowState<Value>, DbError> {
        match self.updates.get(id) {
            Some(update) => Ok(update.state.clone()),
            None => Ok(self.store.row(id)?.state),
        }
    }

    fn index(&self, id: &RowId) -> Result<(), DbError> {
        let state = self.state(id)?;
        if !state.present() {
            return Ok(());
        }
        let table = self.schema.table(&id.table);
        let values = state
            .cells()
            .iter()
            .map(|(name, cell)| (name.clone(), cell.value.value.clone()))
            .collect();
        let values = crate::removal_sql::evaluate_values(self.database, table, &values)?;
        for index in &table.indices {
            if let Some(columns) = index.columns.iter().cloned().collect::<Option<Vec<_>>>() {
                let values: Vec<_> = columns
                    .iter()
                    .map(|column| values[column].clone())
                    .collect();
                if !values.iter().any(|value| matches!(value, Value::Null)) {
                    self.lookups
                        .borrow_mut()
                        .entry(ReferenceTarget {
                            table: table.name.clone(),
                            columns,
                            value: equality_key(&values, &index.collations)?,
                        })
                        .or_default()
                        .insert(id.clone());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn prime(&self, changed: impl IntoIterator<Item = RowId>) -> Result<(), DbError> {
        // Changes are indexed by their old/new claims so either view can find
        // them even while the physical app table already holds the new values.
        let changed: Vec<_> = changed.into_iter().collect();
        for row in &changed {
            self.index(row)?;
        }
        for row in changed {
            for group in self.row_groups(&row)? {
                self.extra_groups
                    .borrow_mut()
                    .entry(group)
                    .or_default()
                    .insert(row.clone());
            }
        }
        Ok(())
    }

    fn stamp(&self, write: WriteId) -> Timestamp {
        match self.arriving {
            Some((id, stamp)) if write == id => stamp,
            _ => self.store.stamp(write),
        }
    }

    pub(crate) fn evaluated(&self, id: &RowId) -> Result<EvaluatedRow, DbError> {
        if let Some(row) = self.rows.borrow().get(id) {
            return Ok(row.clone());
        }
        let state = self.state(id)?;
        let generation = state.generation();
        if generation.is_multiple_of(2) {
            let row = EvaluatedRow {
                facts: RemovalRow::Absent { generation },
                values: BTreeMap::new(),
                constraints: Constraints::default(),
                readings: BTreeMap::new(),
            };
            self.rows.borrow_mut().insert(id.clone(), row.clone());
            return Ok(row);
        }
        let table = self.schema.table(&id.table);
        let rules = &self.schema.rules[&table.name];
        let mut values: AppValues = state
            .cells()
            .iter()
            .map(|(n, c)| (n.clone(), c.value.value.clone()))
            .collect();
        let mut references = BTreeMap::new();
        let mut unbound = BTreeMap::new();
        for fk in &table.foreign_keys {
            let name = self.schema.foreign_key(table, fk);
            let Some(setter) = fk
                .columns
                .iter()
                .filter_map(|c| state.cells().get(column_name(table, c)))
                .max_by_key(|c| self.stamp(c.write))
            else {
                // The before view of an added reference has no cells yet.
                continue;
            };
            let Some(parent) = setter.value.parents.get(&name) else {
                continue;
            };
            let on_delete = match fk.on_delete.as_str() {
                "CASCADE" => OnDelete::Cascade,
                "RESTRICT" => OnDelete::Restrict,
                "NO ACTION" => OnDelete::NoAction,
                "SET NULL" | "SET DEFAULT" => {
                    let defaults = fk.on_delete == "SET DEFAULT";
                    let replacement =
                        crate::removal_sql::replacement(self.database, table, fk, defaults)?;
                    let permitted = crate::removal_sql::permits(
                        self.database,
                        table,
                        rules,
                        &values,
                        &replacement,
                    )?;
                    if defaults {
                        let parent = self.default_parent(table, &id.audience, fk, &replacement)?;
                        let missing = parent.is_none()
                            && replacement.values().all(|v| !matches!(v, Value::Null));
                        if permitted && missing {
                            unbound.insert(name.clone(), replacement);
                        }
                        OnDelete::SetDefault {
                            parent,
                            permitted: permitted && !missing,
                        }
                    } else {
                        OnDelete::SetNull { permitted }
                    }
                }
                action => panic!("unknown SQLite ON DELETE action {action}"),
            };
            references.insert(
                name,
                Reference {
                    parent: parent.clone(),
                    on_delete,
                },
            );
        }
        let mut readings = BTreeMap::new();
        for (name, reference) in &references {
            let parent_generation = self.state(&reference.parent.row)?.generation();
            let default_generation = match &reference.on_delete {
                OnDelete::SetDefault {
                    parent: Some(parent),
                    ..
                } => Some(self.state(parent)?.generation()),
                _ => None,
            };
            let reading = coven_merge::resolve_reference(
                id,
                reference,
                parent_generation,
                default_generation,
            )
            .map_err(|error| {
                RemovalFailure::Merge(error).into_db_error(self.arriving.map(|(write, _)| write))
            })?;
            let defaults = match reading {
                ReferenceValue::Null => Some(false),
                ReferenceValue::Default(_) => Some(true),
                ReferenceValue::Original { .. } => None,
            };
            if let Some(defaults) = defaults {
                let fk = table
                    .foreign_keys
                    .iter()
                    .find(|fk| self.schema.foreign_key(table, fk) == *name)
                    .expect("reference columns");
                values.extend(crate::removal_sql::replacement(
                    self.database,
                    table,
                    fk,
                    defaults,
                )?);
            } else if matches!(reading, ReferenceValue::Original { stale: true, .. }) {
                if let Some(replacement) = unbound.get(name) {
                    values.extend(replacement.clone());
                }
            }
            readings.insert(name.clone(), reading);
        }
        values = crate::removal_sql::evaluate_values(self.database, table, &values)?;
        let constraints = crate::removal_sql::constraints(
            self.database,
            table,
            rules,
            &state,
            &values,
            |write| self.stamp(write),
        )?;
        let facts = RemovalRow::Present {
            generation,
            started: self.stamp(state.generations()[&generation]),
            references,
            deleted_circle: matches!(id.audience,Audience::Circle(circle) if self.deleted.contains(&circle)),
        };
        let row = EvaluatedRow {
            facts,
            values,
            constraints,
            readings,
        };
        self.rows.borrow_mut().insert(id.clone(), row.clone());
        Ok(row)
    }

    pub(crate) fn constraints_for_values(
        &self,
        id: &RowId,
        values: &AppValues,
    ) -> Result<Constraints, DbError> {
        let table = self.schema.table(&id.table);
        crate::removal_sql::constraints(
            self.database,
            table,
            &self.schema.rules[&table.name],
            &self.state(id)?,
            values,
            |write| self.stamp(write),
        )
    }

    pub(crate) fn lost_value(
        &self,
        id: &RowId,
        key: &coven_merge::LostKey,
        lost: &coven_merge::LostValue<Value>,
    ) -> Result<coven_merge::ColumnValue<Value>, DbError> {
        let mut value = lost.value.clone();
        let table = self.schema.table(&id.table);
        for (name, parent) in &lost.value.parents {
            if self.state(&parent.row)?.generation() == parent.generation {
                continue;
            }
            let fk = table
                .foreign_keys
                .iter()
                .find(|fk| self.schema.foreign_key(table, fk) == *name)
                .expect("lost reference constraint");
            if fk.on_delete == "SET NULL" || fk.on_delete == "SET DEFAULT" {
                let replacement = crate::removal_sql::replacement(
                    self.database,
                    table,
                    fk,
                    fk.on_delete == "SET DEFAULT",
                )?;
                // A lost cell is not an app row. It retains its original parent
                // metadata while reading the stale reference's replacement.
                let values = crate::removal_sql::reference_values(
                    self.database,
                    table,
                    std::slice::from_ref(&key.column),
                    std::slice::from_ref(&replacement[&key.column]),
                )?;
                value.value = values.into_iter().next().expect("one column");
            }
        }
        Ok(value)
    }

    pub(crate) fn default_parent(
        &self,
        child: &TableSchema,
        child_audience: &Audience,
        fk: &SchemaForeignKey,
        replacement: &AppValues,
    ) -> Result<Option<RowId>, DbError> {
        if replacement.values().any(|v| matches!(v, Value::Null)) {
            return Ok(None);
        }
        let target = self.schema.table(&fk.target);
        let columns = target_columns(target, fk);
        let parameters: Vec<_> = fk
            .columns
            .iter()
            .map(|c| replacement[column_name(child, c)].clone())
            .collect();
        let parameters =
            crate::removal_sql::reference_values(self.database, target, &columns, &parameters)?;
        let index = target
            .indices
            .iter()
            .find(|i| {
                i.columns.len() == columns.len()
                    && i.columns
                        .iter()
                        .all(|c| c.as_ref().is_some_and(|c| columns.contains(c)))
            })
            .expect("reference unique target");
        let mut parents = BTreeSet::new();
        for values in self.app.find(target, &columns, &parameters)? {
            let key = (target.name.clone(), row_key(target, &values)?);
            if let Some(app) = self.app.row(&key)? {
                parents.insert(row_id(&key, &app));
            }
        }
        let ordered_columns: Vec<_> = index
            .columns
            .iter()
            .map(|c| c.clone().expect("target column"))
            .collect();
        let ordered_values: Vec<_> = ordered_columns
            .iter()
            .map(|c| parameters[columns.iter().position(|n| n == c).expect("column")].clone())
            .collect();
        let equality = equality_key(&ordered_values, &index.collations)?;
        if index.primary {
            parents.extend(self.database.query("SELECT DISTINCT table_name,key,audience FROM coven_rows WHERE table_name=?1 AND key=?2", params![target.name,equality], crate::row_queries::read_identity)?);
        } else {
            let unique = self.schema.rules[&target.name]
                .unique
                .iter()
                .find(|u| u.index == index.name)
                .expect("reference unique target");
            for audience in BTreeSet::from([Audience::Store, child_audience.clone()]) {
                parents.extend(crate::row_queries::claimants(
                    self.database,
                    &target.name,
                    &unique.identity,
                    &audience,
                    &equality,
                )?);
            }
        }
        if let Some(added) = self.lookups.borrow().get(&ReferenceTarget {
            table: target.name.clone(),
            columns: ordered_columns,
            value: equality_key(&ordered_values, &index.collations)?,
        }) {
            parents.extend(added.iter().cloned());
        }
        let wanted = equality_key(&ordered_values, &index.collations)?;
        let mut current = BTreeSet::new();
        for parent in parents {
            if parent.audience != Audience::Store && &parent.audience != child_audience {
                continue;
            }
            let state = self.state(&parent)?;
            if !state.present() {
                continue;
            }
            let values: Vec<_> = index
                .columns
                .iter()
                .map(|c| {
                    state.cells()[c.as_ref().expect("target column")]
                        .value
                        .value
                        .clone()
                })
                .collect();
            if values.iter().any(|v| matches!(v, Value::Null)) {
                continue;
            }
            if equality_key(&values, &index.collations)? == wanted {
                current.insert(parent);
            }
        }
        if let Some(parent) = current
            .into_iter()
            .min_by_key(|p| p.audience != Audience::Store)
        {
            return Ok(Some(parent));
        }
        let primary = key_columns(target);
        if !primary.iter().all(|c| columns.contains(&c.name)) {
            return Ok(None);
        }
        let values: AppValues = columns.into_iter().zip(parameters).collect();
        let audience = match &self.schema.declaration(&target.name).audience {
            AudienceSource::Store => Audience::Store,
            AudienceSource::Column(c) => match values.get(column_name(target, c)) {
                Some(Value::Text(text)) => audience(text)?,
                _ => child_audience.clone(),
            },
            AudienceSource::ForeignKey(_) => child_audience.clone(),
            AudienceSource::Both { .. } => unreachable!(),
        };
        Ok(Some(RowId {
            table: target.name.clone(),
            key: row_key(target, &values)?,
            audience,
        }))
    }

    fn row_groups(&self, id: &RowId) -> Result<BTreeSet<Group>, DbError> {
        let row = self.evaluated(id)?;
        let mut groups = BTreeSet::from([Group::Key {
            table: id.table.clone(),
            key: id.key.clone(),
        }]);
        groups.extend(
            row.constraints
                .unique
                .into_iter()
                .map(|(constraint, claim)| Group::Claim {
                    table: id.table.clone(),
                    audience: id.audience.clone(),
                    constraint,
                    value: claim.value,
                }),
        );
        Ok(groups)
    }

    fn neighbors(&self, id: &RowId) -> Result<BTreeSet<RowId>, DbError> {
        let mut related = BTreeSet::new();
        if let RemovalRow::Present { references, .. } = self.evaluated(id)?.facts {
            for reference in references.into_values() {
                related.insert(reference.parent.row);
                if let OnDelete::SetDefault {
                    parent: Some(parent),
                    ..
                } = reference.on_delete
                {
                    related.insert(parent);
                }
            }
        }
        for lost in self.state(id)?.lost().values() {
            related.extend(lost.value.parents.values().map(|parent| parent.row.clone()));
        }
        related.extend(self.database.query("SELECT DISTINCT l.table_name,l.key,l.audience FROM coven_lost_references v JOIN coven_lost l ON l.id=v.loss_id WHERE v.parent_table=?1 AND v.parent_key=?2 AND v.parent_audience=?3", params![id.table,id.key,crate::write_encoding::audience_text(&id.audience)], crate::row_queries::read_identity)?);
        if let Some(children) = self.edges.get(id) {
            related.extend(children.iter().cloned());
        }
        for declaration in &self.schema.declarations {
            let table = self.schema.table(&declaration.name);
            for fk in table
                .foreign_keys
                .iter()
                .filter(|fk| fk.target.eq_ignore_ascii_case(&id.table))
            {
                related.extend(crate::row_queries::children(
                    self.database,
                    table,
                    &self.schema.foreign_key(table, fk),
                    id,
                )?);
                if fk.on_delete == "SET DEFAULT" {
                    let replacement =
                        crate::removal_sql::replacement(self.database, table, fk, true)?;
                    if self
                        .default_parent(table, &id.audience, fk, &replacement)?
                        .as_ref()
                        == Some(id)
                    {
                        let name = self.schema.foreign_key(table, fk);
                        related.extend(self.database.query("SELECT DISTINCT r.table_name,r.key,r.audience FROM coven_references v JOIN coven_rows r ON r.id=v.row_id WHERE v.foreign_key_id=(SELECT id FROM coven_foreign_keys WHERE table_name=?1 AND identity=?2)",params![table.name,encoded(coven_format::merge_fields::encode_foreign_key(&name))?],crate::row_queries::read_identity)?.into_iter().filter(|r| id.audience==Audience::Store || id.audience==r.audience));
                    }
                }
            }
        }
        Ok(related)
    }

    fn competitors(&self, group: &Group) -> Result<BTreeSet<RowId>, DbError> {
        let mut members = match self.extra_groups.borrow().get(group) {
            Some(rows) => rows.clone(),
            None => BTreeSet::new(),
        };
        match group {
            Group::Key { table, key } => {
                members.extend(self.database.query("SELECT DISTINCT table_name,key,audience FROM coven_rows WHERE table_name=?1 AND key=?2",params![table,key],crate::row_queries::read_identity)?);
            }
            Group::Claim {
                table,
                audience,
                constraint,
                value,
            } => {
                let table = self.schema.table(table);
                let unique = self.schema.rules[&table.name]
                    .unique
                    .iter()
                    .find(|c| c.identity == *constraint)
                    .expect("unique claim");
                let parameters = decoded(coven_format::key::decode_key(value))?;
                let condition = unique
                    .expressions
                    .iter()
                    .map(|e| format!("({e})=?"))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                for values in self.database.query(
                    &format!(
                        "SELECT {} FROM main.{} WHERE ({}) AND {condition}",
                        column_list(table),
                        identifier(&table.name),
                        unique.predicate
                    ),
                    params_from_iter(parameters.iter().map(sql_value)),
                    |r| read_row(table, r),
                )? {
                    let key = (table.name.clone(), row_key(table, &values)?);
                    if let Some(app) = self.app.row(&key)? {
                        if &app.audience == audience {
                            members.insert(row_id(&key, &app));
                        }
                    }
                }
                members.extend(crate::row_queries::claimants(
                    self.database,
                    &table.name,
                    constraint,
                    audience,
                    value,
                )?);
            }
        }
        Ok(members)
    }
}

impl RemovalView for DatabaseRemovalView<'_> {
    type Error = RemovalFailure;
    fn rows(&self) -> Result<Vec<RowId>, Self::Error> {
        let mut rows: BTreeSet<_> = self
            .database
            .query(
                "SELECT DISTINCT table_name,key,audience FROM coven_rows WHERE table_name>=''",
                [],
                crate::row_queries::read_identity,
            )?
            .into_iter()
            .collect();
        rows.extend(self.updates.keys().cloned());
        Ok(rows.into_iter().collect())
    }
    fn row(&self, row: &RowId) -> Result<RemovalRow, Self::Error> {
        Ok(self.evaluated(row)?.facts)
    }
    fn constraints(
        &self,
        row: &RowId,
        references: &BTreeMap<ForeignKey, ReferenceValue>,
    ) -> Result<Constraints, Self::Error> {
        let evaluated = self.evaluated(row)?;
        assert_eq!(
            &evaluated.readings, references,
            "SQLite and merge resolved different references"
        );
        Ok(evaluated.constraints)
    }
    fn related(&self, row: &RowId) -> Result<BTreeSet<RowId>, Self::Error> {
        Ok(self.neighbors(row)?)
    }
    fn groups(&self, row: &RowId) -> Result<BTreeSet<Group>, Self::Error> {
        Ok(self.row_groups(row)?)
    }
    fn members(&self, group: &Group) -> Result<BTreeSet<RowId>, Self::Error> {
        Ok(self.competitors(group)?)
    }
}
