use crate::{tests::*, *};
use coven_foundation::id_source::DeviceId;

#[test]
fn grocery_titles_and_a_lost_value_that_stops_being_lost() {
    let writes = vec![
        write(
            3,
            1200,
            &[],
            vec![(
                row(42),
                change(0, 0, &[("0", "Grocery list"), ("1", "milk, eggs")]),
            )],
        ),
        write(
            4,
            1301,
            &[3],
            vec![(row(42), change(1, 1, &[("0", "Groceries")]))],
        ),
        write(
            9,
            1302,
            &[3, 4],
            vec![(row(42), change(1, 1, &[("0", "Weekly groceries")]))],
        ),
        write(
            2,
            1400,
            &[3],
            vec![(row(42), change(1, 1, &[("0", "Shopping")]))],
        ),
    ];
    let states = agree(&writes, &[&[0, 1, 2, 3], &[0, 3, 1, 2], &[0, 1, 3, 2]]);
    assert_eq!(value(&states, &row(42), "0"), "Shopping");
    assert_eq!(value(&states, &row(42), "1"), "milk, eggs");
    let lost = states[&row(42)].lost();
    assert_eq!(lost.len(), 1);
    assert_eq!(
        lost[&LostKey {
            column: "0".into(),
            write: id(9)
        }]
            .replaced_by,
        id(2)
    );
    let intermediate = fold(&writes, &[0, 3, 1]);
    assert!(intermediate[&row(42)].lost().contains_key(&LostKey {
        column: "0".into(),
        write: id(4)
    }));
    // §8.7: this winning title is the change a local title-index trigger sees.
    assert_eq!(states[&row(42)].cells()["0"].write, id(2));
}

#[test]
fn distinct_columns_and_overlapping_columns_merge_per_cell() {
    let writes = vec![
        write(
            1,
            1,
            &[],
            vec![(
                row(42),
                change(0, 0, &[("0", "Shopping"), ("1", "milk, eggs")]),
            )],
        ),
        write(
            6,
            1500,
            &[1],
            vec![(
                row(42),
                change(
                    1,
                    1,
                    &[("1", "milk, eggs, bread"), ("2", "2026-10-02 15:00")],
                ),
            )],
        ),
        write(
            10,
            1500,
            &[1],
            vec![(row(42), change(1, 1, &[("0", "Weekend shopping")]))],
        ),
    ];
    let states = agree(&writes, &[&[0, 1, 2], &[0, 2, 1]]);
    assert_eq!(value(&states, &row(42), "0"), "Weekend shopping");
    assert_eq!(value(&states, &row(42), "1"), "milk, eggs, bread");
    // The shared edited_at trigger's result is an ordinary column in the write.
    assert_eq!(value(&states, &row(42), "2"), "2026-10-02 15:00");
    assert!(states[&row(42)].lost().is_empty());
    let mut overlap = writes.clone();
    overlap[2].changes.insert(
        row(42),
        change(1, 1, &[("0", "Weekend shopping"), ("1", "Ben's body")]),
    );
    let states = agree(&overlap, &[&[0, 1, 2], &[0, 2, 1]]);
    assert_eq!(value(&states, &row(42), "1"), "Ben's body");
    assert_eq!(states[&row(42)].lost().len(), 1);
}

#[test]
fn delete_edit_readd_and_concurrent_deletes() {
    let writes = vec![
        write(
            5,
            1445,
            &[],
            vec![(row(43), change(0, 0, &[("0", "Hardware store")]))],
        ),
        write(7, 1600, &[5], vec![(row(43), change(1, 2, &[]))]),
        write(
            8,
            1700,
            &[5, 7],
            vec![(row(43), change(2, 0, &[("0", "New note")]))],
        ),
        write(
            11,
            1800,
            &[5],
            vec![(row(43), change(1, 1, &[("0", "Hardware store, Saturday")]))],
        ),
    ];
    let states = agree(&writes, &[&[0, 1, 2, 3], &[0, 3, 1, 2], &[0, 1, 3, 2]]);
    let state = &states[&row(43)];
    assert_eq!(state.generation(), 3);
    assert_eq!(state.generations()[&2], id(7));
    assert_eq!(value(&states, &row(43), "0"), "New note");
    assert_eq!(
        state.lost()[&LostKey {
            column: "0".into(),
            write: id(11)
        }]
            .replaced_by,
        id(7)
    );
    let mut deletes = writes.clone();
    deletes[3].changes.insert(row(43), change(1, 2, &[]));
    let states = agree(&deletes, &[&[0, 3, 1, 2], &[0, 1, 2, 3]]);
    assert_eq!(states[&row(43)].generation(), 3);
    assert!(states[&row(43)].lost().is_empty());
    let mut readds = deletes;
    readds.push(write(
        12,
        1900,
        &[5, 11],
        vec![(
            row(43),
            change(2, 0, &[("0", "Concurrent re-add"), ("1", "body")]),
        )],
    ));
    let states = agree(&readds, &[&[0, 3, 4, 1, 2], &[0, 1, 2, 3, 4]]);
    assert_eq!(states[&row(43)].generation(), 3);
    assert_eq!(value(&states, &row(43), "0"), "Concurrent re-add");
    assert_eq!(value(&states, &row(43), "1"), "body");
}

#[test]
fn second_delete_can_clear_a_lost_title_or_change_its_replacer() {
    let mut writes = vec![
        write(
            1,
            1,
            &[],
            vec![(row(43), change(0, 0, &[("0", "Hardware store")]))],
        ),
        write(
            2,
            2,
            &[1],
            vec![(row(43), change(1, 1, &[("0", "Carol's title")]))],
        ),
        write(3, 4, &[1], vec![(row(43), change(1, 2, &[]))]),
        write(4, 5, &[1, 2], vec![(row(43), change(1, 2, &[]))]),
    ];
    assert_eq!(fold(&writes, &[0, 1, 2])[&row(43)].lost().len(), 1);
    let states = agree(&writes, &[&[0, 1, 2, 3], &[0, 1, 3, 2], &[0, 2, 1, 3]]);
    assert!(states[&row(43)].lost().is_empty());
    writes[3].had_read.remove(&id(2));
    writes[3].timestamp = Timestamp::new(3, 0, DeviceId(4)).unwrap();
    let states = agree(&writes, &[&[0, 1, 2, 3], &[0, 3, 2, 1]]);
    assert_eq!(states[&row(43)].generations()[&2], id(4));
    assert_eq!(
        states[&row(43)].lost()[&LostKey {
            column: "0".into(),
            write: id(2)
        }]
            .replaced_by,
        id(4)
    );
}

#[test]
fn shared_inserts_merge_and_a_never_seen_insert_loses_to_a_delete() {
    let mut writes = vec![
        write(1, 1, &[], vec![(row(42), change(0, 0, &[("0", "urgent")]))]),
        write(
            2,
            2,
            &[],
            vec![(row(42), change(0, 0, &[("0", "urgent"), ("1", "Ben")]))],
        ),
    ];
    let states = agree(&writes, &[&[0, 1], &[1, 0]]);
    assert_eq!(states[&row(42)].generation(), 1);
    assert_eq!(states[&row(42)].cells()["0"].write, id(2));
    writes.push(write(3, 3, &[1], vec![(row(42), change(1, 2, &[]))]));
    let states = agree(&writes, &[&[0, 2, 1], &[1, 0, 2]]);
    assert_eq!(states[&row(42)].generation(), 2);
    assert_eq!(states[&row(42)].lost().len(), 2);
}

#[test]
fn shared_attachment_count_is_not_recomputed_during_merge() {
    let writes = vec![
        write(
            1,
            1,
            &[],
            vec![
                (row(42), change(0, 0, &[("0", "1")])),
                (row(9), change(0, 0, &[])),
            ],
        ),
        write(
            2,
            2,
            &[1],
            vec![
                (row(42), change(1, 1, &[("0", "2")])),
                (row(10), change(0, 0, &[])),
            ],
        ),
        write(
            3,
            3,
            &[1],
            vec![
                (row(42), change(1, 1, &[("0", "2")])),
                (row(11), change(0, 0, &[])),
            ],
        ),
    ];
    let states = agree(&writes, &[&[0, 1, 2], &[0, 2, 1]]);
    assert_eq!(value(&states, &row(42), "0"), "2");
    assert_eq!(
        [9, 10, 11]
            .iter()
            .filter(|n| states[&row(**n)].present())
            .count(),
        3
    );
}

#[test]
fn invalid_apply_preserves_its_input_and_reports_errors() {
    let insert = write(1, 1, &[], vec![(row(1), change(0, 0, &[("0", "x")]))]);
    let state = RowState::new(row(1));
    let empty = History::<String>::new([]).unwrap();
    let state = apply(&state, &insert, &empty).unwrap().state;
    let original = state.clone();
    let oracle = History::new([insert.clone()]).unwrap();
    assert!(matches!(
        apply(&state, &insert, &oracle),
        Err(MergeError::DuplicateWrite(_))
    ));
    let unseen = write(2, 2, &[], vec![(row(1), change(1, 1, &[]))]);
    assert!(matches!(
        apply(&state, &unseen, &oracle),
        Err(MergeError::GenerationNotSeen { .. })
    ));
    let ahead = write(2, 2, &[1], vec![(row(1), change(2, 0, &[]))]);
    assert!(matches!(
        apply(&state, &ahead, &oracle),
        Err(MergeError::GenerationAhead { .. })
    ));
    let missing = write(2, 2, &[1], vec![]);
    assert!(matches!(
        apply(&state, &missing, &oracle),
        Err(MergeError::MissingChange(_))
    ));
    assert_eq!(state, original);
    assert!(matches!(
        state.claim_timestamp(&[], &oracle),
        Err(MergeError::EmptyClaim)
    ));
    assert!(matches!(
        state.claim_timestamp(&["absent".into()], &oracle),
        Err(MergeError::MissingClaimColumn(_))
    ));
}

#[test]
fn compact_state_loading_rejects_damaged_records() {
    let writes = vec![
        write(1, 1, &[], vec![(row(1), change(0, 0, &[("0", "first")]))]),
        write(2, 2, &[1], vec![(row(1), change(1, 1, &[("0", "second")]))]),
        write(3, 3, &[1], vec![(row(1), change(1, 1, &[("0", "third")]))]),
        write(4, 4, &[1, 2, 3], vec![(row(1), change(1, 2, &[]))]),
    ];
    let states = fold(&writes, &[0, 1, 2]);
    let state = &states[&row(1)];
    let oracle = History::new(writes).unwrap();
    let mut missing = state.generations().clone();
    missing.clear();
    missing.insert(2, id(4));
    assert!(matches!(
        RowState::<String>::from_parts(
            row(1),
            missing,
            Default::default(),
            Default::default(),
            &oracle
        ),
        Err(MergeError::GenerationGap(1))
    ));
    let mut deleted = state.generations().clone();
    deleted.insert(2, id(4));
    assert!(matches!(
        RowState::from_parts(
            row(1),
            deleted,
            state.cells().clone(),
            Default::default(),
            &oracle
        ),
        Err(MergeError::DeletedRowHasCells)
    ));
    let mut lost = state.lost().clone();
    lost.values_mut().next().unwrap().replaced_by = id(1);
    assert!(matches!(
        RowState::from_parts(
            row(1),
            state.generations().clone(),
            state.cells().clone(),
            lost,
            &oracle
        ),
        Err(MergeError::InvalidLostValue(_))
    ));
}
