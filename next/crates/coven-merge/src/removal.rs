use crate::input::validate_audience;
use crate::{Audience, MergeError, Parent, RowId, Timestamp};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

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
/// non-null default; omitting it returns [`MergeError::MissingDefaultGeneration`].
/// Use this for retained lost values as well as winning
/// cells: their references read null/default under the same rule (§8.4).
/// Deleted/removed default parents take the child out;
/// re-adding the default parent lets it return.
pub fn resolve_reference(
    child: &RowId,
    reference: &Reference,
    parent_generation: u64,
    default_generation: Option<u64>,
) -> Result<ReferenceValue, MergeError> {
    reference.parent.validate_written(child)?;
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
                                .ok_or_else(|| MergeError::MissingDefaultGeneration(row.clone()))?,
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
        /// The timestamp recorded for this generation in `_coven_rows`.
        started: Timestamp,
        /// Foreign-key names and their merged references.
        references: BTreeMap<crate::ForeignKey, Reference>,
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
    /// One non-null claim per constraint, identified by terms and predicate.
    pub unique: BTreeMap<crate::UniqueConstraint, UniqueClaim>,
}

/// Rows competing for one unique value or one key across audiences.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Group {
    /// One unique constraint's value after reference substitution.
    Claim {
        /// The synced table's name.
        table: String,
        /// The audience within which the value must be unique.
        audience: Audience,
        /// The unique constraint's name.
        constraint: crate::UniqueConstraint,
        /// The canonical equality encoding used by [`UniqueClaim`].
        value: Vec<u8>,
    },
    /// One primary key competing across its store and circle audiences.
    Key {
        /// The synced table's name.
        table: String,
        /// The encoded primary key.
        key: Vec<u8>,
    },
}

/// Abstract database view for the removal rules. All answers are functions of
/// the merged state and store log, independent of which rows are removed.
/// An adapter can prepare this view in memory; the merge itself performs no I/O.
pub trait RemovalView {
    /// Query failure, including the merge invariant errors checked here.
    type Error: From<MergeError>;
    /// Every known row, including rows absent by their own generation.
    fn rows(&self) -> Result<Vec<RowId>, Self::Error>;
    /// Facts for a row. Unknown keys are `Absent { generation: 0 }`.
    fn row(&self, row: &RowId) -> Result<RemovalRow, Self::Error>;
    /// Evaluate constraints on merged values with these resolved references.
    /// Substitution permission is supplied with the reference; CHECK results
    /// here describe the merged values that remain after that decision.
    fn constraints(
        &self,
        row: &RowId,
        references: &BTreeMap<crate::ForeignKey, ReferenceValue>,
    ) -> Result<Constraints, Self::Error>;
    /// Indexed reference edges in both directions: parents, children, default
    /// parents, and children whose default parent is this row. Include absent
    /// and removed rows, and edges before and after reference substitution.
    /// Competition belongs in [`Self::groups`], not in these edges.
    fn related(&self, row: &RowId) -> Result<BTreeSet<RowId>, Self::Error>;
    /// The row's key group and its current unique claim groups, using values
    /// after reference substitution. Include removed rows' claims. The union
    /// of the before/after views in [`recompute`] covers groups a row left.
    fn groups(&self, row: &RowId) -> Result<BTreeSet<Group>, Self::Error>;
    /// Every row in a competition group, including absent and removed rows,
    /// regardless of rank. Region discovery visits each group once per view.
    fn members(&self, group: &Group) -> Result<BTreeSet<RowId>, Self::Error>;
}

/// A rule recorded for a removed row. Every rule that holds at the end is
/// included; unique/other-audience rules retain the once-judged decision.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rule {
    /// The named foreign key is stale or its parent is absent/removed.
    ForeignKey(crate::ForeignKey),
    /// The named CHECK fails on merged values.
    Check(String),
    /// The store log has deleted the row's circle.
    DeletedCircle,
    /// The same key won in another audience.
    OtherAudience,
    /// The named unique constraint lost its value to an earlier claim.
    Unique(crate::UniqueConstraint),
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
    /// References as they read in both the app's table and `_coven_lost`.
    /// The original cell setters and timestamps are retained in `RowState`.
    pub references: BTreeMap<RowId, BTreeMap<crate::ForeignKey, ReferenceValue>>,
}

/// Recompute every row using §8's three steps. The view controls iteration
/// order; no choice of monotone rule order changes the result (Appendix B, B8).
pub fn removals<V: RemovalView>(view: &V) -> Result<RemovalResult, V::Error> {
    let rows = view.rows()?;
    let mut seen = BTreeSet::new();
    for row in &rows {
        if !seen.insert(row.clone()) {
            return Err(MergeError::DuplicateRow(row.clone()).into());
        }
    }
    let region = region(&[view], seen.clone())?;
    let mut order = rows;
    order.extend(region.iter().filter(|r| !seen.contains(*r)).cloned());
    evaluate(view, region, &order, true)
}

/// Recompute only the touched rows' dependency region in the union of the
/// before/after views. Including old edges releases former losers and children
/// when a claim/reference changes. The region is closed under parents and
/// rivals, so Appendix B, B9 (`Audience.lean`, `removal_local`) gives exactly
/// the full recomputation's answer there. Outside it no input or dependency
/// changed. The caller must include store-log changes' rows among `touched`.
/// No global row enumeration is performed by this function. Each competition
/// group is expanded at most once in each view, so rivals are visited by group
/// membership rather than by every pair of competing rows.
pub fn recompute<E: From<MergeError>>(
    before: &impl RemovalView<Error = E>,
    after: &impl RemovalView<Error = E>,
    touched: impl IntoIterator<Item = RowId>,
) -> Result<RemovalResult, E> {
    let region = region(&[before, after], touched.into_iter().collect())?;
    let order: Vec<_> = region.iter().cloned().collect();
    evaluate(after, region, &order, true)
}

/// Recompute the same closed region without cross-audience key competition.
/// Fingerprints use this result, including its consequent foreign-key removals (§19.1).
pub fn recompute_fingerprint<E: From<MergeError>>(
    before: &impl RemovalView<Error = E>,
    after: &impl RemovalView<Error = E>,
    touched: impl IntoIterator<Item = RowId>,
) -> Result<RemovalResult, E> {
    let region = region(&[before, after], touched.into_iter().collect())?;
    let order: Vec<_> = region.iter().cloned().collect();
    evaluate(after, region, &order, false)
}

fn region<E: From<MergeError>>(
    views: &[&dyn RemovalView<Error = E>],
    mut rows: BTreeSet<RowId>,
) -> Result<BTreeSet<RowId>, E> {
    let mut visited = vec![BTreeSet::new(); views.len()];
    let mut pending: Vec<_> = rows.iter().cloned().collect();
    while let Some(row) = pending.pop() {
        for (view, groups) in views.iter().zip(&mut visited) {
            let mut include = |neighbors: BTreeSet<RowId>| {
                for neighbor in neighbors {
                    if rows.insert(neighbor.clone()) {
                        pending.push(neighbor);
                    }
                }
            };
            include(view.related(&row)?);
            for group in view.groups(&row)? {
                if groups.insert(group.clone()) {
                    include(view.members(&group)?);
                }
            }
        }
    }
    Ok(rows)
}

struct Facts {
    row: RemovalRow,
    constraints: Constraints,
    references: BTreeMap<crate::ForeignKey, ReferenceValue>,
}

fn evaluate<V: RemovalView>(
    view: &V,
    region: BTreeSet<RowId>,
    order: &[RowId],
    other_audiences: bool,
) -> Result<RemovalResult, V::Error> {
    let mut raw = BTreeMap::new();
    for row in &region {
        let facts = view.row(row)?;
        if facts.present() == facts.generation().is_multiple_of(2) {
            return Err(MergeError::GenerationParity(facts.generation()).into());
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
    // Resolved references are the dependencies that propagate removal.
    // Populate each parent's children in the view's order and reuse this
    // index for both monotone passes.
    let mut children = BTreeMap::<&RowId, Vec<&RowId>>::new();
    for row in order {
        for reference in facts[row].references.values() {
            if let Some((parent, _)) = reference.dependency() {
                children.entry(parent).or_default().push(row);
            }
        }
    }
    close(&facts, &children, order, &mut out);
    let judged = judge_groups(&facts, &out, other_audiences);
    out.extend(judged.keys().cloned());
    close(&facts, &children, order, &mut out);
    let mut result = RemovalResult {
        region,
        ..RemovalResult::default()
    };
    for (row, fact) in facts {
        if fact.row.present() {
            if out.contains(&row) {
                let mut rules = monotone_rules(&fact, &out);
                if let Some(unique) = judged.get(&row) {
                    rules.extend(unique.iter().cloned());
                }
                result.removed.insert(row.clone(), rules);
            }
            result.references.insert(row, fact.references);
        }
    }
    Ok(result)
}

fn close(
    facts: &BTreeMap<RowId, Facts>,
    children: &BTreeMap<&RowId, Vec<&RowId>>,
    order: &[RowId],
    out: &mut BTreeSet<RowId>,
) {
    let mut pending = VecDeque::new();
    for row in order {
        if out.contains(row) || !monotone_rules(&facts[row], out).is_empty() {
            out.insert(row.clone());
            pending.push_back(row);
        }
    }
    while let Some(parent) = pending.pop_front() {
        if let Some(children) = children.get(parent) {
            for child in children {
                // Any resolved parent going out suffices; no need to rescan
                // the child's other references. Collect all rules at the end.
                if out.insert((*child).clone()) {
                    pending.push_back(child);
                }
            }
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

fn judge_groups(
    facts: &BTreeMap<RowId, Facts>,
    out: &BTreeSet<RowId>,
    other_audiences: bool,
) -> BTreeMap<RowId, BTreeSet<Rule>> {
    // Judge every group against the same pass-one survivors. A row losing
    // another competition still participates in all of its groups.
    let mut groups = BTreeMap::<Group, Vec<(&RowId, Timestamp)>>::new();
    for (row, fact) in facts {
        if !out.contains(row) {
            for (name, claim) in fact.constraints.unique.iter() {
                groups
                    .entry(Group::Claim {
                        table: row.table.clone(),
                        audience: row.audience.clone(),
                        constraint: name.clone(),
                        value: claim.value.clone(),
                    })
                    .or_default()
                    .push((row, claim.timestamp));
            }
            if other_audiences {
                if let RemovalRow::Present { started, .. } = fact.row {
                    groups
                        .entry(Group::Key {
                            table: row.table.clone(),
                            key: row.key.clone(),
                        })
                        .or_default()
                        .push((row, started));
                }
            }
        }
    }
    let mut judged = BTreeMap::<RowId, BTreeSet<Rule>>::new();
    for (group, members) in groups {
        match group {
            Group::Claim { constraint, .. } => {
                let &(winner, _) = members
                    .iter()
                    .min_by_key(|(row, stamp)| (*stamp, &row.key))
                    .expect("a competition group contains at least one row");
                for (row, _) in &members {
                    if *row != winner {
                        judged
                            .entry((*row).clone())
                            .or_default()
                            .insert(Rule::Unique(constraint.clone()));
                    }
                }
            }
            Group::Key { .. } => {
                let &(winner, earliest) = members
                    .iter()
                    .min_by_key(|(row, stamp)| (row.audience != Audience::Store, *stamp))
                    .expect("a competition group contains at least one row");
                for (row, started) in &members {
                    if matches!(row.audience, Audience::Circle(_))
                        && (winner.audience == Audience::Store || earliest < *started)
                    {
                        judged
                            .entry((*row).clone())
                            .or_default()
                            .insert(Rule::OtherAudience);
                    }
                }
            }
        }
    }
    judged
}

#[cfg(test)]
#[path = "removal_tests.rs"]
mod tests;
