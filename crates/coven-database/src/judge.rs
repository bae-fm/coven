//! The permission check on each row change of one write (§8.8, D5). Every
//! device runs it, the author on its own write before queueing it.
//!
//! A write's steps never say what produced them. The receiver orders the
//! steps of every part it opened by sequence, and calls a step a foreign-key
//! action exactly when it is the write's schema's action on a child of an
//! earlier step that deleted the child's parent, or replaced its key: the
//! child still references that parent's old key in the authoring view, and
//! the step's operation, columns, values and parents are exactly the action's.
//! The authoring view is rebuilt from the write's own earlier steps and the
//! authenticated original records of the cells the steps replaced, never from
//! the receiver's current rows. Every other step is direct, as a statement or
//! a shared trigger is.
//!
//! A row change passes when the author may change the row of every direct
//! step, every action reaches a direct step on such a row, a row only actions
//! changed has exactly their combined effect as its net change, and a row
//! whose steps cause actions has its net change where its steps end (Lean
//! `Admission.checkSteps`, `applied_root_ends_where_steps_end`). A failure names the row's owner when the author
//! may not change the row itself, and an invalid foreign-key effect otherwise
//! (`Admission.guard`).

use crate::judge_view::{Authoring, ChildView};
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::audience_text;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::loss::AuthorizationFailure;
use coven_format::merge_fields;
use coven_format::snapshot_rows::OriginalRow;
use coven_format::value::Value;
use coven_format::write::{Step, WriteHeader};
use coven_format::write_sources::ChangeId;
use coven_foundation::id_source::{DeviceId, MemberId};
use coven_merge::{Change, Operation, RowId};
use rusqlite::params;
use std::collections::BTreeMap;

/// One step as the index keeps it: enough to find a parent's last operation
/// and a key update's other half without holding column values.
#[derive(Clone, Debug)]
pub(crate) struct Indexed {
    pub(crate) sequence: u32,
    pub(crate) delete: bool,
    pub(crate) insert: bool,
    pub(crate) key_update: bool,
    pub(crate) generation: u64,
}

impl Indexed {
    fn of(step: &Step) -> Self {
        Self {
            sequence: step.sequence,
            delete: matches!(step.change.operation, Operation::Delete),
            insert: matches!(step.change.operation, Operation::Insert(_)),
            key_update: step.key_update,
            generation: step.change.generation,
        }
    }

    /// The row's generation after this step.
    pub(crate) fn after(&self) -> u64 {
        if self.delete || self.insert {
            self.generation.saturating_add(1)
        } else {
            self.generation
        }
    }
}

/// Every step of the write's opened parts, by row and by sequence.
#[derive(Default)]
pub(crate) struct StepIndex {
    rows: BTreeMap<RowId, Vec<Indexed>>,
    sequences: BTreeMap<u32, Vec<RowId>>,
    /// The first delete of each table's rows: no reference into a table can
    /// act before it.
    deletes: BTreeMap<String, u32>,
}

impl StepIndex {
    /// Every step of the write's retained parts.
    pub(crate) fn of(db: &DatabaseConnection, header: &WriteHeader) -> Result<Self, DbError> {
        let mut index = Self::default();
        visit_write(db, header, |record| {
            let row = record.row().clone();
            let steps: Vec<_> = record.execution().steps.iter().map(Indexed::of).collect();
            for step in &steps {
                index
                    .sequences
                    .entry(step.sequence)
                    .or_default()
                    .push(row.clone());
                if step.delete {
                    let first = index
                        .deletes
                        .entry(row.table.clone())
                        .or_insert(step.sequence);
                    *first = (*first).min(step.sequence);
                }
            }
            index.rows.insert(row, steps);
            Ok(())
        })?;
        Ok(index)
    }

    /// Whether any row of `table` was deleted before `sequence`.
    pub(crate) fn deleted_before(&self, table: &str, sequence: u32) -> bool {
        self.deletes
            .iter()
            .any(|(name, first)| name.eq_ignore_ascii_case(table) && *first < sequence)
    }

    /// The row's steps before `sequence`, the nearest first.
    pub(crate) fn before<'a>(
        &'a self,
        row: &RowId,
        sequence: u32,
    ) -> impl Iterator<Item = &'a Indexed> + 'a {
        self.rows
            .get(row)
            .into_iter()
            .flat_map(|steps| steps.iter().rev())
            .filter(move |step| step.sequence < sequence)
    }

    /// The row's last step before `sequence`.
    pub(crate) fn last_before(&self, row: &RowId, sequence: u32) -> Option<&Indexed> {
        self.before(row, sequence).next()
    }

    /// The new key's half of the primary-key update whose old half is
    /// `old` at `sequence`: the other row of the same table at that position.
    pub(crate) fn new_half(&self, old: &RowId, sequence: u32) -> Option<(&RowId, &Indexed)> {
        self.sequences.get(&sequence)?.iter().find_map(|row| {
            if row == old || row.table != old.table {
                return None;
            }
            let step = self.rows[row].iter().find(|s| s.sequence == sequence)?;
            (!step.delete).then_some((row, step))
        })
    }
}

/// One row change's permission check (Lean `Admission.check`) and, for its
/// foreign-key actions, the changes that caused them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Checked {
    /// Whether the change passes, and why not.
    pub(crate) check: Result<(), AuthorizationFailure>,
    /// For each foreign-key action on the row, in execution order, its own
    /// sequence and the steps that caused it: its root direct change, then
    /// each intermediate action down to its parent, by row and sequence.
    pub(crate) actions: Vec<(u32, Vec<(RowId, u32)>)>,
    /// Whether foreign-key actions alone changed the row.
    pub(crate) derived: bool,
}

impl Checked {
    /// Every action's causes in execution order, as changes of `write`.
    pub(crate) fn causes(&self, write: coven_merge::WriteId) -> Vec<ChangeId> {
        self.actions
            .iter()
            .flat_map(|(_, chain)| chain)
            .map(|(row, _)| ChangeId {
                write,
                row: row.clone(),
            })
            .collect()
    }
}

/// Every opened row change's check.
#[derive(Clone)]
pub(crate) struct Judgement {
    pub(crate) rows: BTreeMap<RowId, Checked>,
}

/// Facts a record contributes beyond its steps.
struct Shape {
    steps: Vec<u32>,
    /// A copy, or a change coven made without SQL: a direct target.
    normalized: bool,
    /// Every step is an action and their combined effect is the net change,
    /// or the row has no net change.
    exact: bool,
    /// The steps end where the net change does (Lean `Capture`).
    consistent: bool,
    /// A delete's old values are the cells it replaced.
    began: bool,
}

/// Judge every row change of the write's parts this device holds. The
/// original records come from `_coven_authored_changes`, so every caller
/// retains them first.
pub(crate) fn judge(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
) -> Result<Judgement, DbError> {
    let authoring = Authoring::new(db, schema, header.schema_version)?;
    let index = StepIndex::of(db, header)?;
    let mut actions = BTreeMap::new();
    let mut shapes = BTreeMap::new();
    visit_write(db, header, |record| {
        let row = record.row().clone();
        let execution = record.execution();
        let mut view = ChildView::new(&authoring, &row, &record, header.schema_version);
        // A delete's old values are the cells it replaced (D5).
        let began = match &record {
            OriginalRow::Change(change) if change.change.operation == Operation::Delete => {
                view.began_with(db, &change.old)?
            }
            _ => true,
        };
        let mut derived = 0;
        for step in &execution.steps {
            if let Some(cause) = view.action(db, &index, step)? {
                actions.insert((row.clone(), step.sequence), cause);
                derived += 1;
            }
            view.apply(step);
        }
        let all = !execution.steps.is_empty() && derived == execution.steps.len();
        let exact = match &record {
            OriginalRow::Change(change) if all => {
                combined(&execution.steps).is_some_and(|effect| effect == change.change)
            }
            _ => true,
        };
        let normalized = match &record {
            OriginalRow::Change(change) => execution.steps.is_empty() || change.creator.is_some(),
            OriginalRow::Execution(_) => false,
        };
        shapes.insert(
            row,
            Shape {
                steps: execution.steps.iter().map(|s| s.sequence).collect(),
                normalized,
                exact,
                consistent: consistent(&record),
                began,
            },
        );
        Ok(())
    })?;
    // Each action's chain of causes, root first.
    let chain = |row: &RowId, sequence: u32| {
        let mut chain = Vec::new();
        let mut at = actions[&(row.clone(), sequence)].clone();
        loop {
            chain.push(at.clone());
            match actions.get(&at) {
                Some(parent) => at = parent.clone(),
                None => break,
            }
        }
        chain.reverse();
        chain
    };
    let causes: std::collections::BTreeSet<RowId> = actions
        .keys()
        .flat_map(|(row, sequence)| chain(row, *sequence))
        .map(|(row, _)| row)
        .collect();
    let mut owners = Owners::new(db, schema, header)?;
    let mut rows = BTreeMap::new();
    for (row, shape) in &shapes {
        let derived: Vec<u32> = shape
            .steps
            .iter()
            .copied()
            .filter(|sequence| actions.contains_key(&(row.clone(), *sequence)))
            .collect();
        let direct = shape.normalized || derived.len() < shape.steps.len();
        let owner = owners.foreign_owner(db, row)?;
        let refuse = || match owner {
            Some(owner) => AuthorizationFailure::NotOwner { owner },
            None => AuthorizationFailure::InvalidForeignKeyEffect,
        };
        let mut check = Ok(());
        // A direct target needs the author's own permission (`statement_authorized`).
        if direct && owner.is_some() {
            check = Err(refuse());
        }
        // An action's steps justify effects on other rows only when the row's
        // net change is where they end.
        if check.is_ok() && causes.contains(row) && !shape.consistent {
            check = Err(refuse());
        }
        let mut chains = Vec::new();
        for sequence in derived {
            let chain = chain(row, sequence);
            // Every action reaches an authorized direct root (`derived_roots_authorized`).
            if check.is_ok() && owners.foreign_owner(db, &chain[0].0)?.is_some() {
                check = Err(refuse());
            }
            chains.push((sequence, chain));
        }
        // Actions alone must combine to exactly the net change (`derived_exact`),
        // and a delete they make shows the values its setters wrote, not
        // values its author chose (Lean `originalCell`).
        if check.is_ok() && !direct && (!shape.exact || !shape.began) {
            check = Err(refuse());
        }
        rows.insert(
            row.clone(),
            Checked {
                check,
                actions: chains,
                derived: !direct,
            },
        );
    }
    Ok(Judgement { rows })
}

/// Whether a row's steps end where its net change does: the same starting
/// generation, the same presence at the end, and every net column's value as
/// the steps last set it. A row inserted and deleted again has no net change.
pub(crate) fn consistent(record: &OriginalRow) -> bool {
    let steps = &record.execution().steps;
    let Some(first) = steps.first() else {
        return true;
    };
    let initially = !matches!(first.change.operation, Operation::Insert(_));
    let mut present = initially;
    let mut set = BTreeMap::new();
    for step in steps {
        match &step.change.operation {
            Operation::Insert(columns) => {
                present = true;
                set = columns.clone();
            }
            Operation::Update(columns) => set.extend(columns.clone()),
            Operation::Delete => {
                present = false;
                set.clear();
            }
        }
    }
    let OriginalRow::Change(change) = record else {
        return initially == present;
    };
    if change.creator.is_some() {
        return true;
    }
    if change.change.generation != first.change.generation {
        return false;
    }
    match (&change.change.operation, initially, present) {
        (Operation::Delete, true, false) => true,
        (Operation::Insert(columns), false, true) | (Operation::Update(columns), true, true) => {
            columns
                .iter()
                .all(|(name, cell)| set.get(name).is_some_and(|last| last.value == cell.value))
        }
        _ => false,
    }
}

/// Visit the write's retained original records, part by part in row order.
pub(crate) fn visit_write(
    db: &DatabaseConnection,
    header: &WriteHeader,
    mut visit: impl FnMut(OriginalRow) -> Result<(), DbError>,
) -> Result<(), DbError> {
    db.for_each(
        "SELECT record FROM _coven_authored_changes WHERE write_id=?1 ORDER BY audience,table_name,key",
        params![merge_fields::encode_write_id(&header.position)?],
        |r| visit(OriginalRow::decode(&r.get::<_, Vec<u8>>(0)?)?),
    )
}

/// The combined effect of successive actions on one row (Lean
/// `combineEffects`): later columns replace earlier ones, a delete replaces
/// everything, and actions on different incarnations never combine.
fn combined(steps: &[Step]) -> Option<Change<Value>> {
    let mut steps = steps.iter();
    let mut effect = steps.next()?.change.clone();
    for step in steps {
        if step.change.generation != effect.generation {
            return None;
        }
        effect.operation = match (effect.operation, &step.change.operation) {
            (Operation::Delete, _) | (_, Operation::Delete) => Operation::Delete,
            (Operation::Update(mut columns), Operation::Update(next)) => {
                columns.extend(next.clone());
                Operation::Update(columns)
            }
            _ => return None,
        };
    }
    Some(effect)
}

/// Each row's owner, as the selected creations and the counted membership name them.
struct Owners<'a> {
    schema: &'a WriteSchema,
    version: u32,
    current: u32,
    device: DeviceId,
    member: Option<MemberId>,
    known: BTreeMap<RowId, Option<MemberId>>,
}

impl<'a> Owners<'a> {
    fn new(
        db: &DatabaseConnection,
        schema: &'a WriteSchema,
        header: &WriteHeader,
    ) -> Result<Self, DbError> {
        Ok(Self {
            schema,
            version: header.schema_version,
            current: db.schema_version()?,
            device: header.position.device,
            member: crate::ownership::member(db, header.position.device)?,
            known: BTreeMap::new(),
        })
    }

    /// The member who owns this row when it is not the author (Lean
    /// `!allowed`): an owner table's row another member created. A row no
    /// write has created yet is new, and its creation is the author's.
    fn foreign_owner(
        &mut self,
        db: &DatabaseConnection,
        row: &RowId,
    ) -> Result<Option<MemberId>, DbError> {
        if let Some(owner) = self.known.get(row) {
            return Ok(*owner);
        }
        let owner = self.lookup(db, row)?;
        self.known.insert(row.clone(), owner);
        Ok(owner)
    }

    fn lookup(&self, db: &DatabaseConnection, row: &RowId) -> Result<Option<MemberId>, DbError> {
        let Some(table) = crate::source_names::table(db, self.version, &row.table, self.current)?
        else {
            return Ok(None);
        };
        if !crate::ownership::owner_table(self.schema, &table) {
            return Ok(None);
        }
        let current = RowId {
            table,
            ..row.clone()
        };
        let Some(creation) = crate::ownership::creation(db, &current)? else {
            return Ok(None);
        };
        // A device's own creations are its member's, even before its
        // registration acquires a slot.
        if creation.creator.write.device == self.device {
            return Ok(None);
        }
        // Every applied write's device is registered before it applies.
        let owner = crate::ownership::member(db, creation.creator.write.device)?.ok_or(
            DbError::Invariant {
                what: "an applied write's device has a member",
            },
        )?;
        Ok((self.member != Some(owner)).then_some(owner))
    }
}

/// The admission stored for one original row change, if judged.
pub(crate) fn stored(
    db: &DatabaseConnection,
    header: &WriteHeader,
    row: &RowId,
) -> Result<coven_format::snapshot_rows::ChangeVerdict, DbError> {
    let admission: Option<Option<Vec<u8>>> = db
        .query(
            "SELECT admission FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4",
            params![
                merge_fields::encode_write_id(&header.position)?,
                audience_text(&row.audience),
                row.table,
                row.key
            ],
            |r| r.get(0),
        )?
        .pop();
    match admission.flatten() {
        None => Ok(coven_format::snapshot_rows::ChangeVerdict::Unjudged),
        Some(bytes) => {
            merge_fields::decode_change_verdict(&bytes).map_err(|_| DbError::DamagedDatabase)
        }
    }
}

#[cfg(test)]
#[path = "judge_tests.rs"]
pub(crate) mod tests;
