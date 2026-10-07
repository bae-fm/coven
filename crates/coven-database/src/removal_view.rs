//! Lazy indexed removal views before and after the merge's row updates.
use crate::merge_store::MergeStore;
use crate::sql::identifier;
use crate::write_encoding::{decoded, sql_value};
use crate::write_rows::{column_list, column_name, read_row, row_id, row_key, AppValues, AppView};
use crate::write_schema::WriteSchema;
use crate::DbError;
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

pub(crate) struct DatabaseRemovalView<'a> {
    database: &'a crate::sqlite::DatabaseConnection,
    store: &'a MergeStore<'a>,
    schema: &'a WriteSchema,
    app: &'a AppView<'a>,
    updates: &'a BTreeMap<RowId, RowUpdate<Value>>,
    deleted: &'a BTreeSet<CircleId>,
    arriving: Option<(WriteId, Timestamp)>,
    rows: Option<RefCell<BTreeMap<RowId, EvaluatedRow>>>,
    edges: BTreeMap<RowId, BTreeSet<RowId>>,
    extra_groups: RefCell<BTreeMap<Group, BTreeSet<RowId>>>,
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
            for value in update.state.cells().values().map(|c| &c.value) {
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
            rows: store.caches_rows().then(|| RefCell::new(BTreeMap::new())),
            edges,
            extra_groups: RefCell::new(BTreeMap::new()),
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

    pub(crate) fn prime(&self, changed: impl IntoIterator<Item = RowId>) -> Result<(), DbError> {
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

    fn remember(&self, id: &RowId, row: EvaluatedRow) -> EvaluatedRow {
        if let Some(rows) = &self.rows {
            rows.borrow_mut().insert(id.clone(), row.clone());
        }
        row
    }

    pub(crate) fn evaluated(&self, id: &RowId) -> Result<EvaluatedRow, DbError> {
        if let Some(rows) = &self.rows {
            if let Some(row) = rows.borrow().get(id) {
                return Ok(row.clone());
            }
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
            return Ok(self.remember(id, row));
        }
        let table = self.schema.table(&id.table);
        let rules = &self.schema.rules[&table.name];
        let mut values: AppValues = state
            .cells()
            .iter()
            .map(|(n, c)| (n.clone(), c.value.value.clone()))
            .collect();
        let mut references = BTreeMap::new();
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
                "SET NULL" => {
                    let replacement = crate::removal_sql::null_reference(table, fk);
                    let permitted = crate::removal_sql::permits(
                        self.database,
                        table,
                        rules,
                        &values,
                        &replacement,
                    )?;
                    OnDelete::SetNull { permitted }
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
            let reading = coven_merge::resolve_reference(id, reference, parent_generation)
                .map_err(|error| {
                    RemovalFailure::Merge(error)
                        .into_db_error(self.arriving.map(|(write, _)| write))
                })?;
            if reading == ReferenceValue::Null {
                let fk = table
                    .foreign_keys
                    .iter()
                    .find(|fk| self.schema.foreign_key(table, fk) == *name)
                    .expect("reference columns");
                values.extend(crate::removal_sql::null_reference(table, fk));
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
        Ok(self.remember(id, row))
    }

    pub(crate) fn unique_values(
        &self,
        id: &RowId,
        values: &AppValues,
    ) -> Result<BTreeMap<coven_merge::UniqueConstraint, Vec<u8>>, DbError> {
        let table = self.schema.table(&id.table);
        crate::removal_sql::unique_values(
            self.database,
            table,
            &self.schema.rules[&table.name],
            values,
        )
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
            }
        }
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
                members.extend(self.database.query("SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE table_name=?1 AND key=?2",params![table,key],crate::row_queries::read_identity)?);
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
                "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE table_name>=''",
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
