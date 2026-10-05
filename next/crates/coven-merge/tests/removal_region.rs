//! Indexed region discovery and removal through the public API.

use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{
    recompute, removals, Audience, Constraints, Group, MergeError, OnDelete, Parent, Reference,
    ReferenceValue, RemovalResult, RemovalRow, RemovalView, RowId, Rule, Timestamp, UniqueClaim,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};
use uuid::Uuid;

fn row(key: u64) -> RowId {
    RowId {
        table: "rows".into(),
        key: key.to_be_bytes().to_vec(),
        audience: Audience::Store,
    }
}

fn circle(key: u64, circle: u64) -> RowId {
    RowId {
        audience: Audience::Circle(CircleId(Uuid::from_u128(circle.into()))),
        ..row(key)
    }
}

fn stamp(ms: u64) -> Timestamp {
    Timestamp::new(ms, 0, DeviceId(1)).unwrap()
}

fn present(ms: u64, parent: Option<RowId>) -> RemovalRow {
    RemovalRow::Present {
        generation: 1,
        started: stamp(ms),
        references: parent
            .into_iter()
            .map(|row| {
                (
                    "parent".into(),
                    Reference {
                        parent: Parent { row, generation: 1 },
                        on_delete: OnDelete::Cascade,
                    },
                )
            })
            .collect(),
        deleted_circle: false,
    }
}

fn claim(value: &str, ms: u64) -> Constraints {
    Constraints {
        unique: BTreeMap::from([(
            "title".into(),
            UniqueClaim {
                value: value.as_bytes().to_vec(),
                timestamp: stamp(ms),
            },
        )]),
        ..Constraints::default()
    }
}

// Index only input relationships. Removal decisions always run through the
// public merge functions, including their region discovery and closure.
struct IndexedView {
    rows: BTreeMap<RowId, (RemovalRow, Constraints)>,
    edges: BTreeMap<RowId, BTreeSet<RowId>>,
    groups: BTreeMap<RowId, BTreeSet<Group>>,
    members: BTreeMap<Group, BTreeSet<RowId>>,
    visited: RefCell<BTreeSet<Group>>,
}

impl IndexedView {
    fn new(rows: BTreeMap<RowId, (RemovalRow, Constraints)>) -> Self {
        let mut view = Self {
            rows,
            edges: BTreeMap::new(),
            groups: BTreeMap::new(),
            members: BTreeMap::new(),
            visited: RefCell::new(BTreeSet::new()),
        };
        for (row, (data, constraints)) in &view.rows {
            if let RemovalRow::Present { references, .. } = data {
                for reference in references.values() {
                    let mut parents = vec![&reference.parent.row];
                    if let OnDelete::SetDefault {
                        parent: Some(parent),
                        ..
                    } = &reference.on_delete
                    {
                        parents.push(parent);
                    }
                    for parent in parents {
                        view.edges
                            .entry(row.clone())
                            .or_default()
                            .insert(parent.clone());
                        view.edges
                            .entry(parent.clone())
                            .or_default()
                            .insert(row.clone());
                    }
                }
            }
            let mut groups = BTreeSet::from([Group::Key {
                table: row.table.clone(),
                key: row.key.clone(),
            }]);
            for (constraint, claim) in &constraints.unique {
                groups.insert(Group::Claim {
                    table: row.table.clone(),
                    audience: row.audience.clone(),
                    constraint: constraint.clone(),
                    value: claim.value.clone(),
                });
            }
            for group in &groups {
                view.members
                    .entry(group.clone())
                    .or_default()
                    .insert(row.clone());
            }
            view.groups.insert(row.clone(), groups);
        }
        view
    }

    fn reset_visits(&self) {
        self.visited.borrow_mut().clear();
    }
}

impl RemovalView for IndexedView {
    fn rows(&self) -> Result<Vec<RowId>, MergeError> {
        Ok(self.rows.keys().cloned().collect())
    }

    fn row(&self, row: &RowId) -> Result<RemovalRow, MergeError> {
        Ok(match self.rows.get(row) {
            Some((data, _)) => data.clone(),
            None => RemovalRow::Absent { generation: 0 },
        })
    }

    fn constraints(
        &self,
        row: &RowId,
        _: &BTreeMap<String, ReferenceValue>,
    ) -> Result<Constraints, MergeError> {
        Ok(self.rows[row].1.clone())
    }

    fn related(&self, row: &RowId) -> Result<BTreeSet<RowId>, MergeError> {
        Ok(self.edges.get(row).cloned().unwrap_or_default())
    }

    fn groups(&self, row: &RowId) -> Result<BTreeSet<Group>, MergeError> {
        Ok(match self.groups.get(row) {
            Some(groups) => groups.clone(),
            None => BTreeSet::from([Group::Key {
                table: row.table.clone(),
                key: row.key.clone(),
            }]),
        })
    }

    fn members(&self, group: &Group) -> Result<BTreeSet<RowId>, MergeError> {
        assert!(
            self.visited.borrow_mut().insert(group.clone()),
            "visited group twice: {group:?}"
        );
        Ok(self.members.get(group).cloned().unwrap_or_default())
    }
}

#[test]
fn region_expands_each_competition_group_once_per_view() {
    let before = IndexedView::new(BTreeMap::from([
        (row(0), (present(1, None), claim("old", 1))),
        (row(1), (present(2, None), claim("old", 2))),
        (row(2), (present(3, None), claim("new", 3))),
        (circle(0, 1), (present(0, None), Constraints::default())),
    ]));
    let mut changed = before.rows.clone();
    changed.get_mut(&row(0)).unwrap().1 = claim("new", 4);
    let after = IndexedView::new(changed);
    let partial = recompute(&before, &after, [row(0)]).unwrap();
    assert_eq!(partial.region, before.rows.keys().cloned().collect());
    assert_eq!(
        partial.removed,
        BTreeMap::from([
            (row(0), BTreeSet::from([Rule::Unique("title".into())])),
            (circle(0, 1), BTreeSet::from([Rule::OtherAudience])),
        ])
    );
    assert_eq!(before.visited.borrow().len(), before.members.len());
    assert_eq!(after.visited.borrow().len(), after.members.len());
    after.reset_visits();
    assert_eq!(partial, removals(&after).unwrap());
}

fn assert_rule(result: &RemovalResult, row: &RowId, rule: Rule) {
    assert_eq!(result.removed[row], BTreeSet::from([rule]));
}

#[test]
fn competing_claims_keep_every_reason_and_respect_each_scope() {
    let mut first = claim("shared", 1);
    first
        .unique
        .insert("slug".into(), first.unique["title"].clone());
    let mut second = claim("shared", 2);
    second
        .unique
        .insert("slug".into(), second.unique["title"].clone());
    let mut third = claim("different", 0);
    third.unique.insert(
        "slug".into(),
        UniqueClaim {
            value: b"shared".to_vec(),
            timestamp: stamp(0),
        },
    );
    let view = IndexedView::new(BTreeMap::from([
        (row(1), (present(1, None), first)),
        (row(2), (present(2, None), second)),
        (row(3), (present(0, None), third)),
        (circle(4, 1), (present(0, None), claim("shared", 0))),
        (
            RowId {
                table: "other".into(),
                ..row(2)
            },
            (present(0, None), claim("shared", 0)),
        ),
    ]));
    let result = removals(&view).unwrap();
    assert_eq!(
        result.removed,
        BTreeMap::from([
            (row(1), BTreeSet::from([Rule::Unique("slug".into())])),
            (
                row(2),
                BTreeSet::from([Rule::Unique("slug".into()), Rule::Unique("title".into())])
            ),
        ])
    );
}

#[test]
fn closure_records_every_reference_even_when_the_child_went_out_first() {
    let mut child = present(1, Some(row(1)));
    let RemovalRow::Present { references, .. } = &mut child else {
        unreachable!()
    };
    references.insert(
        "second".into(),
        Reference {
            parent: Parent {
                row: row(2),
                generation: 1,
            },
            on_delete: OnDelete::Restrict,
        },
    );
    let mut failed = Constraints::default();
    failed.failed_checks.insert("valid".into());
    let view = IndexedView::new(BTreeMap::from([
        (row(0), (child, Constraints::default())),
        (row(1), (present(1, Some(row(0))), Constraints::default())),
        (row(2), (present(1, None), failed)),
    ]));
    let result = removals(&view).unwrap();
    assert_eq!(
        result.removed[&row(0)],
        BTreeSet::from([
            Rule::ForeignKey("parent".into()),
            Rule::ForeignKey("second".into()),
        ])
    );
    assert_rule(&result, &row(1), Rule::ForeignKey("parent".into()));
    assert_rule(&result, &row(2), Rule::Check("valid".into()));
}

#[test]
fn twenty_thousand_rows_cover_claims_audiences_and_deep_chains() {
    let mut rows = BTreeMap::new();
    for n in 0..8_000 {
        rows.insert(row(n), (present(1, None), claim("shared", 10)));
    }
    for n in 1..=8_000 {
        // Two circles share the earliest start. With no store row both stay.
        let started = if n <= 2 { 0 } else { n };
        rows.insert(
            circle(0, n),
            (present(started, None), Constraints::default()),
        );
    }
    for n in 8_000..12_000 {
        let parent = if n == 9_999 { row(1) } else { row(n + 1) };
        rows.insert(row(n), (present(1, Some(parent)), Constraints::default()));
    }
    rows.get_mut(&row(11_999)).unwrap().0 = present(1, None);
    rows.get_mut(&row(11_999))
        .unwrap()
        .1
        .failed_checks
        .insert("valid".into());
    // This earlier claim must disappear in pass one, before judging claims.
    rows.get_mut(&row(10_000)).unwrap().1 = claim("shared", 0);
    let before = IndexedView::new(rows);
    let mut rows = before.rows.clone();
    rows.get_mut(&row(0)).unwrap().0 = RemovalRow::Absent { generation: 2 };
    let after = IndexedView::new(rows);

    let start = Instant::now();
    let full = removals(&before).unwrap();
    let full_elapsed = start.elapsed();
    assert_eq!(full.region.len(), 20_000);
    assert_eq!(full.removed.len(), 19_999);
    assert!(!full.removed.contains_key(&row(0)));
    for n in 1..8_000 {
        assert_rule(&full, &row(n), Rule::Unique("title".into()));
    }
    for n in 1..=8_000 {
        assert_rule(&full, &circle(0, n), Rule::OtherAudience);
    }
    for n in 8_000..11_999 {
        assert_rule(&full, &row(n), Rule::ForeignKey("parent".into()));
    }
    assert_rule(&full, &row(11_999), Rule::Check("valid".into()));
    assert_eq!(before.visited.borrow().len(), before.members.len());

    before.reset_visits();
    let start = Instant::now();
    let partial = recompute(&before, &after, [row(0)]).unwrap();
    let partial_elapsed = start.elapsed();
    assert_eq!(partial.region.len(), 20_000);
    assert_eq!(partial.removed.len(), 17_996);
    for survivor in [row(1), circle(0, 1), circle(0, 2)] {
        assert!(!partial.removed.contains_key(&survivor));
    }
    for n in 8_000..10_000 {
        assert!(!partial.removed.contains_key(&row(n)));
    }
    assert_eq!(before.visited.borrow().len(), before.members.len());
    assert_eq!(after.visited.borrow().len(), after.members.len());
    assert!(!partial.removed.contains_key(&row(0)));
    for n in 2..8_000 {
        assert_rule(&partial, &row(n), Rule::Unique("title".into()));
    }
    for n in 3..=8_000 {
        assert_rule(&partial, &circle(0, n), Rule::OtherAudience);
    }
    for n in 10_000..11_999 {
        assert_rule(&partial, &row(n), Rule::ForeignKey("parent".into()));
    }
    assert_rule(&partial, &row(11_999), Rule::Check("valid".into()));
    // Visit counts above prove linear group expansion; these bounds only
    // catch a quadratic closure (minutes at this size), with room for a loaded CI.
    assert!(
        full_elapsed < Duration::from_secs(10),
        "removals: {full_elapsed:?}"
    );
    assert!(
        partial_elapsed < Duration::from_secs(10),
        "recompute: {partial_elapsed:?}"
    );
}
