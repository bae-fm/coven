use crate::input::{validate_audience, validate_parent};
use crate::{Audience, MergeError, Parent, RowId, Timestamp};
use std::collections::{BTreeMap, BTreeSet};

/// The foreign key's ON DELETE action (§8.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnDelete {
    /// A stale reference takes the child out.
    Cascade,
    /// A stale reference takes the child out.
    Restrict,
    /// A stale reference takes the child out.
    NoAction,
    /// Read null after a parent deletion if SQLite accepts it.
    SetNull {
        /// Whether the column permits the substitution, e.g. is nullable.
        permitted: bool,
    },
    /// Read the default after a parent deletion if SQLite accepts it.
    SetDefault {
        /// The default parent, or `None` for a null default.
        parent: Option<RowId>,
        /// Whether SQLite permits the substitution.
        permitted: bool,
    },
}

/// One reference in the merged values, before null/default substitution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reference {
    /// The parent generation carried by the winning reference's write.
    pub parent: Parent,
    /// The schema's ON DELETE action and substitution constraints.
    pub on_delete: OnDelete,
}

/// What a reference reads as. Substitution changes neither its winning write
/// nor its timestamp, in the app's table or in lost values (§8.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReferenceValue {
    /// The original parent; stale references take their children out.
    Original {
        /// The original parent and generation.
        parent: Parent,
        /// The parent's carried generation has been deleted since.
        stale: bool,
    },
    /// A set-null reference whose parent's generation was deleted.
    Null,
    /// A set-default reference, always pointing at the default parent's
    /// current generation and never stale. `None` is a null default.
    Default(Option<Parent>),
}

impl ReferenceValue {
    fn dependency(&self) -> Option<(&RowId, bool)> {
        match self {
            Self::Original { parent, stale } => Some((&parent.row, *stale)),
            Self::Default(Some(parent)) => Some((&parent.row, false)),
            Self::Null | Self::Default(None) => None,
        }
    }
}

/// Resolve a reference against parent generations, before CHECK and unique
/// evaluation. `default_generation` is required only for a substituted,
/// non-null default. Use this for retained lost values as well as winning
/// cells: their references read null/default under the same rule (§8.4).
/// Deleted/removed default parents take the child out;
/// re-adding the default parent lets it return.
pub fn resolve_reference(
    child: &RowId,
    reference: &Reference,
    parent_generation: u64,
    default_generation: Option<u64>,
) -> Result<ReferenceValue, MergeError> {
    validate_parent(child, &reference.parent)?;
    let stale = parent_generation != reference.parent.generation;
    if stale {
        match &reference.on_delete {
            OnDelete::SetNull { permitted: true } => return Ok(ReferenceValue::Null),
            OnDelete::SetDefault {
                parent,
                permitted: true,
            } => {
                let parent = match parent {
                    Some(row) => {
                        validate_audience(child, row)?;
                        Some(Parent {
                            row: row.clone(),
                            generation: default_generation
                                .ok_or_else(|| MergeError::RegionNotClosed(row.clone()))?,
                        })
                    }
                    None => None,
                };
                return Ok(ReferenceValue::Default(parent));
            }
            _ => {}
        }
    }
    Ok(ReferenceValue::Original {
        parent: reference.parent.clone(),
        stale,
    })
}

/// The row facts removal reads from merged state, never from prior removals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemovalRow {
    /// An even generation, including zero for a row never inserted.
    Absent {
        /// The row's current even generation.
        generation: u64,
    },
    /// An odd generation, whether or not a rule currently removes the row.
    Present {
        /// The row's current odd generation.
        generation: u64,
        /// The timestamp recorded for this generation in `coven_rows`.
        started: Timestamp,
        /// Foreign-key names and their merged references.
        references: BTreeMap<String, Reference>,
        /// Whether the store log has deleted this row's circle.
        deleted_circle: bool,
    },
}

impl RemovalRow {
    /// The generation in merged state, independent of removal rules.
    pub fn generation(&self) -> u64 {
        match self {
            Self::Absent { generation } | Self::Present { generation, .. } => *generation,
        }
    }
    /// Whether the row exists before the removal rules run.
    pub fn present(&self) -> bool {
        matches!(self, Self::Present { .. })
    }
}

/// One unique value and the latest timestamp of any column in its constraint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueClaim {
    /// Canonical equality encoding of the whole value, with SQL collation and
    /// null semantics supplied by the database. Non-conflicting nulls make no claim.
    pub value: Vec<u8>,
    /// The latest winning setter of any of the constraint's columns (§8.5).
    pub timestamp: Timestamp,
}

/// CHECK and unique results on merged values after reference substitution.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Constraints {
    /// Names of all CHECK constraints the merged values fail.
    pub failed_checks: BTreeSet<String>,
    /// Unique constraint names and the row's claims.
    pub unique: BTreeMap<String, UniqueClaim>,
}

/// Abstract database view for the removal rules. All answers are functions of
/// the merged state and store log, independent of which rows are removed.
/// An adapter can prepare this view in memory; the merge itself performs no I/O.
pub trait RemovalView {
    /// Every known row, including rows absent by their own generation.
    fn rows(&self) -> Result<Vec<RowId>, MergeError>;
    /// Facts for a row. Unknown keys are `Absent { generation: 0 }`.
    fn row(&self, row: &RowId) -> Result<RemovalRow, MergeError>;
    /// Evaluate constraints on merged values with these resolved references.
    /// Substitution permission is supplied with the reference; CHECK results
    /// here describe the merged values that remain after that decision.
    fn constraints(
        &self,
        row: &RowId,
        references: &BTreeMap<String, ReferenceValue>,
    ) -> Result<Constraints, MergeError>;
    /// Indexed neighbors in both directions: parents, children, default
    /// parents, every row sharing a unique claim, and the same key in other
    /// audiences. Include absent and removed rows, and all rivals regardless
    /// of rank. These edges must include changes caused by reference
    /// substitution (e.g. a default changing a unique value).
    fn related(&self, row: &RowId) -> Result<BTreeSet<RowId>, MergeError>;
}

/// A rule recorded for a removed row. Every rule that holds at the end is
/// included; unique/other-audience rules retain the once-judged decision.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rule {
    /// The named foreign key is stale or its parent is absent/removed.
    ForeignKey(String),
    /// The named CHECK fails on merged values.
    Check(String),
    /// The store log has deleted the row's circle.
    DeletedCircle,
    /// The same key won in another audience.
    OtherAudience,
    /// The named unique constraint lost its value to an earlier claim.
    Unique(String),
}

/// Removals and reference readings for a closed region. Replace prior removal
/// records for ALL rows in `region`, including rows absent from `removed`, so
/// rows whose reasons cleared come back. Their merged states never change.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct RemovalResult {
    /// Rows recomputed, including deleted/unknown rows at the region's boundary.
    pub region: BTreeSet<RowId>,
    /// Removed rows with every final rule, plus the once-judged losers' rules.
    pub removed: BTreeMap<RowId, BTreeSet<Rule>>,
    /// References as they read in both the app's table and `coven_lost`.
    /// The original cell setters and timestamps are retained in `RowState`.
    pub references: BTreeMap<RowId, BTreeMap<String, ReferenceValue>>,
}

/// Recompute every row using §8's three steps. The view controls iteration
/// order; no choice of monotone rule order changes the result (Appendix B, B8).
pub fn removals(view: &impl RemovalView) -> Result<RemovalResult, MergeError> {
    let rows = view.rows()?;
    let mut seen = BTreeSet::new();
    for row in &rows {
        if !seen.insert(row.clone()) {
            return Err(MergeError::DuplicateRow(row.clone()));
        }
    }
    let region = region(view, view, seen)?;
    let mut order = rows;
    order.extend(
        region
            .iter()
            .filter(|r| !order.contains(r))
            .cloned()
            .collect::<Vec<_>>(),
    );
    evaluate(view, region, &order)
}

/// Recompute only the touched rows' dependency region in the union of the
/// before/after views. Including old edges releases former losers and children
/// when a claim/reference changes. The region is closed under parents and
/// rivals, so Appendix B, B9 (`Audience.lean`, `removal_local`) gives exactly
/// the full recomputation's answer there. Outside it no input or dependency
/// changed. The caller must include store-log changes' rows among `touched`.
/// No global row enumeration is performed by this function.
pub fn recompute(
    before: &impl RemovalView,
    after: &impl RemovalView,
    touched: impl IntoIterator<Item = RowId>,
) -> Result<RemovalResult, MergeError> {
    let region = region(before, after, touched.into_iter().collect())?;
    let order: Vec<_> = region.iter().cloned().collect();
    evaluate(after, region, &order)
}

fn region(
    before: &impl RemovalView,
    after: &impl RemovalView,
    mut rows: BTreeSet<RowId>,
) -> Result<BTreeSet<RowId>, MergeError> {
    let mut pending: Vec<_> = rows.iter().cloned().collect();
    while let Some(row) = pending.pop() {
        for neighbor in before
            .related(&row)?
            .into_iter()
            .chain(after.related(&row)?)
        {
            if rows.insert(neighbor.clone()) {
                pending.push(neighbor);
            }
        }
    }
    Ok(rows)
}

struct Facts {
    row: RemovalRow,
    constraints: Constraints,
    references: BTreeMap<String, ReferenceValue>,
}

fn evaluate(
    view: &impl RemovalView,
    region: BTreeSet<RowId>,
    order: &[RowId],
) -> Result<RemovalResult, MergeError> {
    let mut raw = BTreeMap::new();
    for row in &region {
        let facts = view.row(row)?;
        if facts.present() == facts.generation().is_multiple_of(2) {
            return Err(MergeError::GenerationParity(facts.generation()));
        }
        raw.insert(row.clone(), facts);
    }
    let mut facts = BTreeMap::new();
    for (row, data) in &raw {
        let mut resolved = BTreeMap::new();
        if let RemovalRow::Present { references, .. } = data {
            for (name, reference) in references {
                let parent_gen = raw
                    .get(&reference.parent.row)
                    .ok_or_else(|| MergeError::RegionNotClosed(reference.parent.row.clone()))?
                    .generation();
                let default_gen = match &reference.on_delete {
                    OnDelete::SetDefault {
                        parent: Some(parent),
                        permitted: true,
                    } if parent_gen != reference.parent.generation => Some(
                        raw.get(parent)
                            .ok_or_else(|| MergeError::RegionNotClosed(parent.clone()))?
                            .generation(),
                    ),
                    _ => None,
                };
                resolved.insert(
                    name.clone(),
                    resolve_reference(row, reference, parent_gen, default_gen)?,
                );
            }
        }
        let constraints = if data.present() {
            view.constraints(row, &resolved)?
        } else {
            Constraints::default()
        };
        facts.insert(
            row.clone(),
            Facts {
                row: data.clone(),
                constraints,
                references: resolved,
            },
        );
    }
    let mut out: BTreeSet<_> = facts
        .iter()
        .filter(|(_, f)| !f.row.present())
        .map(|(r, _)| r.clone())
        .collect();
    close(&facts, order, &mut out);
    // Judge every claim against the same pass-one survivors, never against
    // a set being changed by this judgment.
    let mut judged = BTreeMap::<RowId, BTreeSet<Rule>>::new();
    for (row, fact) in &facts {
        if !out.contains(row) {
            for (other, rival) in &facts {
                if row != other && !out.contains(other) {
                    judged
                        .entry(row.clone())
                        .or_default()
                        .extend(rival_rules(row, fact, other, rival));
                }
            }
        }
    }
    for (row, rules) in &judged {
        if !rules.is_empty() {
            out.insert(row.clone());
        }
    }
    close(&facts, order, &mut out);
    let mut result = RemovalResult {
        region,
        ..RemovalResult::default()
    };
    for (row, fact) in &facts {
        if fact.row.present() {
            result
                .references
                .insert(row.clone(), fact.references.clone());
            if out.contains(row) {
                let mut rules = monotone_rules(fact, &out);
                if let Some(unique) = judged.get(row) {
                    rules.extend(unique.iter().cloned());
                }
                result.removed.insert(row.clone(), rules);
            }
        }
    }
    Ok(result)
}

fn close(facts: &BTreeMap<RowId, Facts>, order: &[RowId], out: &mut BTreeSet<RowId>) {
    loop {
        let mut changed = false;
        for row in order {
            if let Some(fact) = facts.get(row) {
                if !out.contains(row) && !monotone_rules(fact, out).is_empty() {
                    changed |= out.insert(row.clone());
                }
            }
        }
        if !changed {
            break;
        }
    }
}

fn monotone_rules(fact: &Facts, out: &BTreeSet<RowId>) -> BTreeSet<Rule> {
    let mut rules = BTreeSet::new();
    for (name, reference) in &fact.references {
        if let Some((parent, stale)) = reference.dependency() {
            if stale || out.contains(parent) {
                rules.insert(Rule::ForeignKey(name.clone()));
            }
        }
    }
    rules.extend(
        fact.constraints
            .failed_checks
            .iter()
            .cloned()
            .map(Rule::Check),
    );
    if matches!(
        fact.row,
        RemovalRow::Present {
            deleted_circle: true,
            ..
        }
    ) {
        rules.insert(Rule::DeletedCircle);
    }
    rules
}

fn rival_rules(row: &RowId, fact: &Facts, other: &RowId, rival: &Facts) -> BTreeSet<Rule> {
    let mut rules = BTreeSet::new();
    if row.table == other.table && row.audience == other.audience {
        for (name, claim) in &fact.constraints.unique {
            if let Some(competing) = rival.constraints.unique.get(name) {
                if claim.value == competing.value
                    && (competing.timestamp < claim.timestamp
                        || (competing.timestamp == claim.timestamp && other.key < row.key))
                {
                    rules.insert(Rule::Unique(name.clone()));
                }
            }
        }
    }
    if row.table == other.table && row.key == other.key && row.audience != other.audience {
        let wins = match (&row.audience, &other.audience, &fact.row, &rival.row) {
            (Audience::Circle(_), Audience::Store, _, _) => true,
            (
                Audience::Circle(_),
                Audience::Circle(_),
                RemovalRow::Present { started, .. },
                RemovalRow::Present {
                    started: earlier, ..
                },
            ) => earlier < started,
            _ => false,
        };
        if wins {
            rules.insert(Rule::OtherAudience);
        }
    }
    rules
}

#[cfg(test)]
#[path = "removal_tests.rs"]
mod tests;
