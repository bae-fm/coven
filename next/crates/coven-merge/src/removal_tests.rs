use crate::{tests::*, *};
use std::collections::{BTreeMap, BTreeSet};

fn rules(result: &RemovalResult, r: &RowId) -> BTreeSet<Rule> {
    result.removed.get(r).cloned().unwrap_or_default()
}
fn removal_orders(view: &MemoryView) -> RemovalResult {
    let expected = removals(view).unwrap();
    let mut reverse = view.clone();
    reverse.order = view.data.keys().rev().cloned().collect();
    assert_eq!(removals(&reverse).unwrap(), expected);
    expected
}
fn fk_view(
    states: &BTreeMap<RowId, RowState<String>>,
    history: &History<String>,
    actions: &[(RowId, OnDelete)],
) -> MemoryView {
    let mut view = MemoryView::from_states(states, history);
    for (r, action) in actions {
        if let Some(s) = states.get(r) {
            if s.present() {
                let parent = s.cells()["0"].value.parents["parent"].clone();
                view.reference(r, parent.row, parent.generation, action.clone());
            }
        }
    }
    view
}
fn pointing(g: u64, kind: u8, p: RowId, pg: u64) -> Change<String> {
    let mut c = change(g, kind, &[("0", "reference")]);
    match &mut c.operation {
        Operation::Insert(cols) | Operation::Update(cols) => {
            cols.get_mut("0").unwrap().parents.insert(
                "parent".into(),
                Parent {
                    row: p,
                    generation: pg,
                },
            );
        }
        Operation::Delete => panic!("delete cannot point at a parent"),
    }
    c
}

#[test]
fn default_generation_is_a_required_caller_input_only_when_substituting() {
    let mut reference = Reference {
        parent: Parent {
            row: row(1),
            generation: 1,
        },
        on_delete: OnDelete::SetDefault {
            parent: Some(row(2)),
            permitted: true,
        },
    };
    assert_eq!(
        resolve_reference(&row(3), &reference, 2, None),
        Err(MergeError::MissingDefaultGeneration(row(2)))
    );
    assert_eq!(
        resolve_reference(&row(3), &reference, 1, None).unwrap(),
        ReferenceValue::Original {
            parent: reference.parent.clone(),
            stale: false
        }
    );
    assert_eq!(
        resolve_reference(&row(3), &reference, 2, Some(0)).unwrap(),
        ReferenceValue::Default(Some(Parent {
            row: row(2),
            generation: 0
        }))
    );
    reference.on_delete = OnDelete::SetDefault {
        parent: None,
        permitted: true,
    };
    assert_eq!(
        resolve_reference(&row(3), &reference, 2, None).unwrap(),
        ReferenceValue::Default(None)
    );
    reference.on_delete = OnDelete::SetDefault {
        parent: Some(row(2)),
        permitted: false,
    };
    assert_eq!(
        resolve_reference(&row(3), &reference, 2, None).unwrap(),
        ReferenceValue::Original {
            parent: reference.parent,
            stale: true
        }
    );
}

#[test]
fn a_region_omitting_the_default_parent_is_not_closed() {
    let mut view = MemoryView::default();
    view.data
        .insert(row(1), RemovalRow::Absent { generation: 2 });
    view.present(row(3), 1, stamp(3));
    view.reference(
        &row(3),
        row(1),
        1,
        OnDelete::SetDefault {
            parent: Some(row(2)),
            permitted: true,
        },
    );
    assert_eq!(
        super::evaluate(&view, [row(1), row(3)].into(), &[row(1), row(3)]),
        Err(MergeError::RegionNotClosed(row(2)))
    );
}

#[test]
fn todo_records_every_reason_after_rules_finish_and_reinsert_can_clear_checks() {
    let writes = vec![
        write(
            1,
            1,
            &[],
            vec![
                (row(3), change(0, 0, &[])),
                (row(7), change(0, 0, &[("0", "5"), ("1", "12")])),
            ],
        ),
        write(2, 2, &[1], vec![(row(3), change(1, 2, &[]))]),
        write(3, 3, &[1], vec![(row(7), change(1, 1, &[("0", "13")]))]),
    ];
    let states = agree(&writes, &[&[0, 1, 2], &[0, 2, 1]]);
    let history = History::new(writes).unwrap();
    let mut view = MemoryView::from_states(&states, &history);
    view.reference(&row(7), row(3), 1, OnDelete::Cascade);
    view.checks
        .entry(row(7))
        .or_default()
        .failed_checks
        .insert("start <= end".into());
    let result = removal_orders(&view);
    assert_eq!(
        rules(&result, &row(7)),
        [
            Rule::ForeignKey("parent".into()),
            Rule::Check("start <= end".into())
        ]
        .into()
    );
    let mut tags = vec![write(
        1,
        1,
        &[],
        vec![(row(1), change(0, 0, &[("0", "bad")]))],
    )];
    let mut before =
        MemoryView::from_states(&fold(&tags, &[0]), &History::new(tags.clone()).unwrap());
    before
        .checks
        .entry(row(1))
        .or_default()
        .failed_checks
        .insert("valid label".into());
    assert!(removal_orders(&before).removed.contains_key(&row(1)));
    tags.push(write(
        2,
        2,
        &[1],
        vec![(row(1), change(1, 1, &[("0", "urgent")]))],
    ));
    let states = agree(&tags, &[&[0, 1]]);
    let after = MemoryView::from_states(&states, &History::new(tags).unwrap());
    assert_eq!(states[&row(1)].generation(), 1);
    assert!(removal_orders(&after).removed.is_empty());
}

#[test]
fn child_moves_and_null_keeps_its_setter_before_a_later_move() {
    let writes = vec![
        write(
            1,
            1,
            &[],
            vec![
                (row(43), change(0, 0, &[])),
                (row(44), change(0, 0, &[])),
                (row(8), pointing(0, 0, row(43), 1)),
                (row(5), pointing(0, 0, row(43), 1)),
            ],
        ),
        write(
            7,
            1620,
            &[1],
            vec![
                (row(43), change(1, 2, &[])),
                (row(8), change(1, 2, &[])),
                (row(5), change(1, 1, &[("0", "null")])),
            ],
        ),
        write(
            12,
            1600,
            &[1],
            vec![
                (row(9), pointing(0, 0, row(43), 1)),
                (row(6), pointing(0, 0, row(43), 1)),
            ],
        ),
        write(
            13,
            1610,
            &[1, 12],
            vec![
                (row(9), pointing(1, 1, row(44), 1)),
                (row(6), pointing(1, 1, row(44), 1)),
            ],
        ),
    ];
    let actions = [
        (row(9), OnDelete::Cascade),
        (row(6), OnDelete::SetNull { permitted: true }),
    ];
    let intermediate = fold(&writes, &[0, 1, 2]);
    let hist = History::new([writes[0].clone(), writes[1].clone(), writes[2].clone()]).unwrap();
    let view = fk_view(&intermediate, &hist, &actions);
    let result = removal_orders(&view);
    assert_eq!(
        rules(&result, &row(9)),
        [Rule::ForeignKey("parent".into())].into()
    );
    assert_eq!(result.references[&row(6)]["parent"], ReferenceValue::Null);
    assert_eq!(intermediate[&row(6)].cells()["0"].write, id(12));
    assert!(intermediate[&row(6)].lost().is_empty());
    for action in [
        OnDelete::Restrict,
        OnDelete::NoAction,
        OnDelete::SetNull { permitted: false },
        OnDelete::SetDefault {
            parent: Some(row(44)),
            permitted: false,
        },
    ] {
        let rejected = fk_view(&intermediate, &hist, &[(row(6), action)]);
        assert_eq!(
            rules(&removal_orders(&rejected), &row(6)),
            [Rule::ForeignKey("parent".into())].into()
        );
    }
    let states = agree(&writes, &[&[0, 1, 2, 3], &[0, 2, 3, 1], &[0, 2, 1, 3]]);
    let result = removal_orders(&fk_view(
        &states,
        &History::new(writes.clone()).unwrap(),
        &actions,
    ));
    assert!(result.removed.is_empty());
    assert_eq!(states[&row(6)].cells()["0"].write, id(13));
    assert_eq!(value(&states, &row(5), "0"), "null");
    assert!(!states[&row(8)].present());
    let mut readd = writes[..3].to_vec();
    readd.push(write(14, 1700, &[1, 7], vec![(row(43), change(2, 0, &[]))]));
    let states = agree(&readd, &[&[0, 1, 2, 3], &[0, 1, 3, 2]]);
    let result = removal_orders(&fk_view(&states, &History::new(readd).unwrap(), &actions));
    assert!(result.removed.contains_key(&row(9)));
    assert_eq!(result.references[&row(6)]["parent"], ReferenceValue::Null);
}

#[test]
fn default_inbox_tracks_the_current_generation_and_comes_back() {
    let writes = vec![
        write(
            1,
            1,
            &[],
            vec![(row(1), change(0, 0, &[])), (row(2), change(0, 0, &[]))],
        ),
        write(2, 1600, &[1], vec![(row(1), change(1, 2, &[]))]),
        write(3, 1600, &[1], vec![(row(50), pointing(0, 0, row(1), 1))]),
        write(4, 1610, &[1, 2], vec![(row(2), change(1, 2, &[]))]),
        write(5, 1700, &[1, 2, 4], vec![(row(2), change(2, 0, &[]))]),
    ];
    let action = [(
        row(50),
        OnDelete::SetDefault {
            parent: Some(row(2)),
            permitted: true,
        },
    )];
    let states = agree(&writes[..4], &[&[0, 1, 2, 3], &[0, 2, 1, 3]]);
    let result = removal_orders(&fk_view(
        &states,
        &History::new(writes[..4].to_vec()).unwrap(),
        &action,
    ));
    assert_eq!(
        rules(&result, &row(50)),
        [Rule::ForeignKey("parent".into())].into()
    );
    assert_eq!(
        result.references[&row(50)]["parent"],
        ReferenceValue::Default(Some(Parent {
            row: row(2),
            generation: 2
        }))
    );
    let states = agree(&writes, &[&[0, 1, 2, 3, 4], &[0, 1, 3, 4, 2]]);
    let result = removal_orders(&fk_view(&states, &History::new(writes).unwrap(), &action));
    assert!(result.removed.is_empty());
    assert_eq!(
        result.references[&row(50)]["parent"],
        ReferenceValue::Default(Some(Parent {
            row: row(2),
            generation: 3
        }))
    );
}

#[test]
fn removed_parent_takes_children_out_under_every_action_and_releases_them() {
    for action in [
        OnDelete::Cascade,
        OnDelete::Restrict,
        OnDelete::NoAction,
        OnDelete::SetNull { permitted: true },
        OnDelete::SetDefault {
            parent: Some(row(45)),
            permitted: true,
        },
    ] {
        let mut view = MemoryView::default();
        for n in [45, 46, 7] {
            view.present(row(n), 1, stamp(n));
        }
        view.claim(&row(45), "title", "Groceries", stamp(1600));
        view.claim(&row(46), "title", "Groceries", stamp(1605));
        view.reference(&row(7), row(46), 1, action);
        let result = removal_orders(&view);
        assert_eq!(
            rules(&result, &row(46)),
            [Rule::Unique("title".into())].into()
        );
        assert_eq!(
            rules(&result, &row(7)),
            [Rule::ForeignKey("parent".into())].into()
        );
        assert!(matches!(
            result.references[&row(7)]["parent"],
            ReferenceValue::Original { stale: false, .. }
        ));
        let before = view.clone();
        view.claim(&row(46), "title", "Shopping", stamp(1700));
        assert!(recompute(&before, &view, [row(46)])
            .unwrap()
            .removed
            .is_empty());
    }
}

#[test]
fn primary_key_rename_and_concurrent_rename_are_deletes_plus_inserts() {
    let writes = vec![
        write(
            1,
            1,
            &[],
            vec![
                (row(10), change(0, 0, &[("0", "urgent")])),
                (row(20), pointing(0, 0, row(10), 1)),
            ],
        ),
        write(
            2,
            1600,
            &[1],
            vec![
                (row(10), change(1, 2, &[])),
                (row(20), change(1, 2, &[])),
                (row(11), change(0, 0, &[("0", "important")])),
                (row(21), pointing(0, 0, row(11), 1)),
            ],
        ),
        write(3, 1601, &[1], vec![(row(22), pointing(0, 0, row(10), 1))]),
    ];
    let states = agree(&writes, &[&[0, 1, 2], &[0, 2, 1]]);
    let result = removal_orders(&fk_view(
        &states,
        &History::new(writes.clone()).unwrap(),
        &[(row(21), OnDelete::Cascade), (row(22), OnDelete::Cascade)],
    ));
    assert_eq!(
        rules(&result, &row(22)),
        [Rule::ForeignKey("parent".into())].into()
    );
    assert!(states[&row(21)].present());
    assert!(!result.removed.contains_key(&row(21)));
    let mut renamed = writes[..2].to_vec();
    renamed.push(write(
        4,
        1602,
        &[1],
        vec![
            (row(10), change(1, 2, &[])),
            (row(12), change(0, 0, &[("0", "critical")])),
        ],
    ));
    let states = agree(&renamed, &[&[0, 1, 2], &[0, 2, 1]]);
    assert!(states[&row(11)].present() && states[&row(12)].present());
    assert_eq!(states[&row(10)].generation(), 2);
}

#[test]
fn claim_uses_the_latest_column_and_ties_use_primary_key() {
    let writes = vec![
        write(
            1,
            900,
            &[],
            vec![(row(2), change(0, 0, &[("0", "Home"), ("1", "Draft")]))],
        ),
        write(
            2,
            1000,
            &[],
            vec![(row(1), change(0, 0, &[("0", "Work"), ("1", "Plan")]))],
        ),
        write(
            3,
            1100,
            &[1],
            vec![(row(2), change(1, 1, &[("1", "Plan")]))],
        ),
        write(
            4,
            1200,
            &[1],
            vec![(row(2), change(1, 1, &[("0", "Work")]))],
        ),
    ];
    let states = agree(&writes, &[&[0, 1, 2, 3], &[0, 3, 2, 1]]);
    let history = History::new(writes).unwrap();
    let mut view = MemoryView::from_states(&states, &history);
    for r in [row(1), row(2)] {
        assert_eq!(value(&states, &r, "0"), "Work");
        assert_eq!(value(&states, &r, "1"), "Plan");
        view.claim(
            &r,
            "folder/title",
            "Work/Plan",
            states[&r]
                .claim_timestamp(&["0".into(), "1".into()], &history)
                .unwrap(),
        );
    }
    assert_eq!(
        rules(&removal_orders(&view), &row(2)),
        [Rule::Unique("folder/title".into())].into()
    );
    view.claim(&row(1), "folder/title", "Work/Plan", stamp(10));
    view.claim(&row(2), "folder/title", "Work/Plan", stamp(10));
    assert_eq!(
        rules(&removal_orders(&view), &row(2)),
        [Rule::Unique("folder/title".into())].into()
    );
}

#[test]
fn uniqueness_is_judged_once_between_the_two_other_passes() {
    let mut view = MemoryView::default();
    view.data
        .insert(row(0), RemovalRow::Absent { generation: 2 });
    for n in [1, 2] {
        view.present(row(n), 1, stamp(n));
    }
    view.reference(&row(1), row(0), 1, OnDelete::Cascade);
    view.claim(&row(1), "title", "Plan", stamp(10));
    view.claim(&row(2), "title", "Plan", stamp(11));
    assert!(!removal_orders(&view).removed.contains_key(&row(2)));
    view.present(row(1), 1, stamp(1));
    view.reference(&row(2), row(1), 1, OnDelete::Cascade);
    view.claim(&row(1), "title", "Plan", stamp(11));
    view.claim(&row(2), "title", "Plan", stamp(10));
    let result = removal_orders(&view);
    assert_eq!(
        rules(&result, &row(1)),
        [Rule::Unique("title".into())].into()
    );
    assert_eq!(
        rules(&result, &row(2)),
        [Rule::ForeignKey("parent".into())].into()
    );
    // Re-inserting a removed shared key with a newer Plan clears neither reason.
    view.claim(&row(1), "title", "Plan", stamp(12));
    assert_eq!(removal_orders(&view).removed, result.removed);
    for n in [3, 4] {
        view.present(row(n), 1, stamp(n));
    }
    view.reference(&row(2), row(3), 1, OnDelete::Cascade);
    view.claim(&row(3), "title", "Ideas", stamp(12));
    view.claim(&row(4), "title", "Ideas", stamp(9));
    let result = removal_orders(&view);
    assert_eq!(
        rules(&result, &row(1)),
        [Rule::Unique("title".into())].into()
    );
    assert_eq!(
        rules(&result, &row(2)),
        [Rule::ForeignKey("parent".into())].into()
    );
    assert_eq!(
        rules(&result, &row(3)),
        [Rule::Unique("title".into())].into()
    );
    view.checks
        .entry(row(2))
        .or_default()
        .failed_checks
        .insert("check".into());
    assert!(!removal_orders(&view).removed.contains_key(&row(1)));
    view.data
        .insert(row(2), RemovalRow::Absent { generation: 2 });
    assert!(!removal_orders(&view).removed.contains_key(&row(1)));
}

#[test]
fn merged_check_failure_clears_with_a_later_end() {
    let writes = vec![
        write(
            1,
            1,
            &[],
            vec![(row(50), change(0, 0, &[("0", "5"), ("1", "12")]))],
        ),
        write(2, 2, &[1], vec![(row(50), change(1, 1, &[("0", "10")]))]),
        write(3, 3, &[1], vec![(row(50), change(1, 1, &[("1", "8")]))]),
        write(4, 4, &[1, 3], vec![(row(50), change(1, 1, &[("1", "20")]))]),
    ];
    for order in [
        &[0, 1][..],
        &[0, 2],
        &[0, 1, 2],
        &[0, 2, 1],
        &[0, 1, 2, 3],
        &[0, 2, 3, 1],
    ] {
        let states = fold(&writes, order);
        let history = History::new(order.iter().map(|i| writes[*i].clone())).unwrap();
        let mut view = MemoryView::from_states(&states, &history);
        let fails = value(&states, &row(50), "0").parse::<u8>().unwrap()
            > value(&states, &row(50), "1").parse::<u8>().unwrap();
        if fails {
            view.checks
                .entry(row(50))
                .or_default()
                .failed_checks
                .insert("start <= end".into());
        }
        assert_eq!(
            removal_orders(&view).removed.contains_key(&row(50)),
            order.len() == 3
        );
    }
}

#[test]
fn move_generations_belong_to_each_audience_and_store_wins() {
    let writes = vec![
        write(
            1,
            1,
            &[],
            vec![
                (row(1), change(0, 0, &[("0", "original")])),
                (row(8), pointing(0, 0, row(1), 1)),
            ],
        ),
        write(
            2,
            20,
            &[1],
            vec![
                (row(1), change(1, 2, &[])),
                (row(8), change(1, 2, &[])),
                (circle(1, 1), change(0, 0, &[("0", "moved")])),
                (circle(8, 1), pointing(0, 0, circle(1, 1), 1)),
            ],
        ),
        write(
            4,
            30,
            &[1, 2],
            vec![(row(1), change(2, 0, &[("0", "Ben's values")]))],
        ),
        write(
            5,
            40,
            &[1, 2],
            vec![(circle(1, 1), change(1, 1, &[("0", "Carol's edit")]))],
        ),
    ];
    let states = agree(&writes, &[&[0, 1, 2, 3], &[0, 1, 3, 2]]);
    let history = History::new(writes.clone()).unwrap();
    let view = fk_view(&states, &history, &[(circle(8, 1), OnDelete::Cascade)]);
    let result = removal_orders(&view);
    assert_eq!(states[&row(1)].generation(), 3);
    assert_eq!(states[&circle(1, 1)].generation(), 1);
    assert_eq!(value(&states, &row(1), "0"), "Ben's values");
    assert_eq!(value(&states, &circle(1, 1), "0"), "Carol's edit");
    assert_eq!(rules(&result, &circle(1, 1)), [Rule::OtherAudience].into());
    assert_eq!(
        rules(&result, &circle(8, 1)),
        [Rule::ForeignKey("parent".into())].into()
    );
    let mut outside = writes.clone();
    for w in &mut outside {
        w.changes.retain(|r, _| r.audience == Audience::Store);
    }
    let outside = agree(&outside, &[&[0, 1, 2, 3], &[0, 1, 3, 2]]);
    assert_eq!(outside[&row(1)], states[&row(1)]);
    assert_eq!(outside[&row(8)].generation(), 2);
    let mut back = writes;
    back.push(write(
        6,
        50,
        &[1, 2, 4, 5],
        vec![
            (circle(1, 1), change(1, 2, &[])),
            (circle(8, 1), change(1, 2, &[])),
            (row(8), pointing(2, 0, row(1), 3)),
        ],
    ));
    let states = agree(&back, &[&[0, 1, 2, 3, 4], &[0, 1, 3, 2, 4]]);
    assert_eq!(states[&row(8)].generation(), 3);
}

#[test]
fn two_circles_choose_the_earlier_generation_and_unique_is_audience_scoped() {
    let mut view = MemoryView::default();
    view.present(circle(1, 1), 3, stamp(30));
    view.present(circle(1, 2), 1, stamp(20));
    view.claim(&circle(1, 1), "title", "Plan", stamp(10));
    view.claim(&circle(1, 2), "title", "Plan", stamp(11));
    let result = removal_orders(&view);
    assert_eq!(rules(&result, &circle(1, 1)), [Rule::OtherAudience].into());
    assert!(!result.removed.contains_key(&circle(1, 2)));
    view.data.remove(&circle(1, 2));
    assert!(removal_orders(&view).removed.is_empty());
}

#[test]
fn deleted_circle_removes_concurrent_rows_without_deleting_them() {
    let writes = vec![
        write(
            1,
            1,
            &[],
            vec![
                (circle(7, 1), change(0, 0, &[("0", "Gift seven")])),
                (circle(8, 1), change(0, 0, &[("0", "Gift eight")])),
            ],
        ),
        write(
            31,
            1600,
            &[1],
            vec![
                (circle(7, 1), change(1, 2, &[])),
                (circle(8, 1), change(1, 2, &[])),
            ],
        ),
        write(
            5,
            1601,
            &[1],
            vec![(circle(9, 1), change(0, 0, &[("0", "Ana's gift")]))],
        ),
    ];
    let states = agree(&writes, &[&[0, 1, 2], &[0, 2, 1]]);
    let mut view = MemoryView::from_states(&states, &History::new(writes).unwrap());
    for r in view.data.values_mut() {
        if let RemovalRow::Present { deleted_circle, .. } = r {
            *deleted_circle = true;
        }
    }
    let result = removal_orders(&view);
    assert_eq!(states[&circle(7, 1)].generation(), 2);
    assert_eq!(states[&circle(8, 1)].generation(), 2);
    assert_eq!(states[&circle(9, 1)].generation(), 1);
    assert_eq!(value(&states, &circle(9, 1), "0"), "Ana's gift");
    assert_eq!(rules(&result, &circle(9, 1)), [Rule::DeletedCircle].into());
}

#[test]
fn locality_uses_old_edges_and_never_enumerates_unrelated_rows() {
    struct Indexed(MemoryView);
    impl RemovalView for Indexed {
        fn groups(&self, row: &RowId) -> Result<BTreeSet<Group>, MergeError> {
            view_groups(self, row)
        }
        fn members(&self, group: &Group) -> Result<BTreeSet<RowId>, MergeError> {
            self.0.members(group)
        }
        fn rows(&self) -> Result<Vec<RowId>, MergeError> {
            panic!("local recomputation must not enumerate rows")
        }
        fn row(&self, r: &RowId) -> Result<RemovalRow, MergeError> {
            assert_ne!(r, &row(99));
            self.0.row(r)
        }
        fn constraints(
            &self,
            r: &RowId,
            refs: &BTreeMap<String, ReferenceValue>,
        ) -> Result<Constraints, MergeError> {
            self.0.constraints(r, refs)
        }
        fn related(&self, r: &RowId) -> Result<BTreeSet<RowId>, MergeError> {
            assert_ne!(r, &row(99));
            self.0.related(r)
        }
    }
    let mut before = MemoryView::default();
    for n in [1, 2, 3, 99] {
        before.present(row(n), 1, stamp(n));
    }
    before.claim(&row(1), "label", "Plan", stamp(10));
    before.claim(&row(2), "label", "Plan", stamp(11));
    before.reference(&row(3), row(2), 1, OnDelete::Cascade);
    let mut after = before.clone();
    after.claim(&row(1), "label", "Ideas", stamp(12));
    let patch = recompute(&Indexed(before), &Indexed(after.clone()), [row(1)]).unwrap();
    assert_eq!(patch.region, [row(1), row(2), row(3)].into());
    assert!(patch.removed.is_empty());
    assert_eq!(patch.removed, removals(&after).unwrap().removed);
    assert!(recompute(&Indexed(after.clone()), &Indexed(after), [])
        .unwrap()
        .region
        .is_empty());
}

#[test]
fn null_and_default_substitutions_feed_checks() {
    struct Resolved(MemoryView);
    impl RemovalView for Resolved {
        fn groups(&self, row: &RowId) -> Result<BTreeSet<Group>, MergeError> {
            view_groups(self, row)
        }
        fn members(&self, group: &Group) -> Result<BTreeSet<RowId>, MergeError> {
            view_members(self, self.0.data.keys(), group)
        }
        fn rows(&self) -> Result<Vec<RowId>, MergeError> {
            self.0.rows()
        }
        fn row(&self, r: &RowId) -> Result<RemovalRow, MergeError> {
            self.0.row(r)
        }
        fn related(&self, r: &RowId) -> Result<BTreeSet<RowId>, MergeError> {
            self.0.related(r)
        }
        fn constraints(
            &self,
            _: &RowId,
            refs: &BTreeMap<String, ReferenceValue>,
        ) -> Result<Constraints, MergeError> {
            let mut c = Constraints::default();
            if refs
                .values()
                .any(|r| matches!(r, ReferenceValue::Null | ReferenceValue::Default(_)))
            {
                c.failed_checks.insert("not default".into());
            }
            Ok(c)
        }
    }
    let mut view = MemoryView::default();
    view.data
        .insert(row(1), RemovalRow::Absent { generation: 2 });
    view.present(row(2), 1, stamp(2));
    for action in [
        OnDelete::SetNull { permitted: true },
        OnDelete::SetDefault {
            parent: None,
            permitted: true,
        },
    ] {
        view.reference(&row(2), row(1), 1, action);
        assert_eq!(
            rules(&removals(&Resolved(view.clone())).unwrap(), &row(2)),
            [Rule::Check("not default".into())].into()
        );
    }
}

#[test]
fn cycles_terminate_and_invalid_inputs_are_errors() {
    let mut view = MemoryView::default();
    for n in [1, 2] {
        view.present(row(n), 1, stamp(n));
    }
    view.reference(&row(1), row(2), 1, OnDelete::Cascade);
    view.reference(&row(2), row(1), 1, OnDelete::Cascade);
    assert!(removal_orders(&view).removed.is_empty());
    view.checks
        .entry(row(2))
        .or_default()
        .failed_checks
        .insert("fail".into());
    assert_eq!(removal_orders(&view).removed.len(), 2);
    view.reference(&row(1), row(2), 2, OnDelete::Cascade);
    assert!(matches!(
        removals(&view),
        Err(MergeError::ParentGeneration(2))
    ));
    let reference = Reference {
        parent: Parent {
            row: circle(2, 1),
            generation: 1,
        },
        on_delete: OnDelete::Cascade,
    };
    assert!(matches!(
        resolve_reference(&row(1), &reference, 1, None),
        Err(MergeError::ReferenceAudience(_))
    ));
    view.order = vec![row(1), row(1)];
    assert!(matches!(removals(&view), Err(MergeError::DuplicateRow(_))));
    view.order.clear();
    view.data
        .insert(row(1), RemovalRow::Absent { generation: 1 });
    assert!(matches!(
        removals(&view),
        Err(MergeError::GenerationParity(1))
    ));
}

#[test]
fn default_reference_unique_claim_keeps_the_original_setters_stamp() {
    struct ResolvedClaims(MemoryView);
    impl RemovalView for ResolvedClaims {
        fn groups(&self, row: &RowId) -> Result<BTreeSet<Group>, MergeError> {
            view_groups(self, row)
        }
        fn members(&self, group: &Group) -> Result<BTreeSet<RowId>, MergeError> {
            view_members(self, self.0.data.keys(), group)
        }
        fn rows(&self) -> Result<Vec<RowId>, MergeError> {
            self.0.rows()
        }
        fn row(&self, r: &RowId) -> Result<RemovalRow, MergeError> {
            self.0.row(r)
        }
        fn related(&self, r: &RowId) -> Result<BTreeSet<RowId>, MergeError> {
            self.0.related(r)
        }
        fn constraints(
            &self,
            r: &RowId,
            refs: &BTreeMap<String, ReferenceValue>,
        ) -> Result<Constraints, MergeError> {
            let mut result = Constraints::default();
            let parent = match refs.get("parent") {
                Some(
                    ReferenceValue::Original { parent, .. } | ReferenceValue::Default(Some(parent)),
                ) => Some(parent),
                _ => None,
            };
            if let Some(parent) = parent {
                let RemovalRow::Present { started, .. } = self.0.row(r)? else {
                    panic!("constraints need a present row")
                };
                result.unique.insert(
                    "folder".into(),
                    UniqueClaim {
                        value: parent.row.key.clone(),
                        timestamp: started,
                    },
                );
            }
            Ok(result)
        }
    }
    let mut before = MemoryView::default();
    for (n, ts) in [(1, 1), (2, 2), (3, 10), (4, 15)] {
        before.present(row(n), 1, stamp(ts));
    }
    before.reference(
        &row(3),
        row(1),
        1,
        OnDelete::SetDefault {
            parent: Some(row(2)),
            permitted: true,
        },
    );
    before.reference(&row(4), row(2), 1, OnDelete::Restrict);
    assert!(removals(&ResolvedClaims(before.clone()))
        .unwrap()
        .removed
        .is_empty());
    let mut after = before.clone();
    after
        .data
        .insert(row(1), RemovalRow::Absent { generation: 2 });
    let full = removals(&ResolvedClaims(after.clone())).unwrap();
    assert_eq!(
        rules(&full, &row(4)),
        [Rule::Unique("folder".into())].into()
    );
    assert!(!full.removed.contains_key(&row(3)));
    let partial = recompute(&ResolvedClaims(before), &ResolvedClaims(after), [row(1)]).unwrap();
    assert_eq!(partial, full);
}
