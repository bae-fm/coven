use crate::{tests::*, *};

#[test]
fn invalid_histories_have_typed_errors() {
    let insert = write(1, 1, &[], vec![(row(1), change(0, 0, &[("0", "value")]))]);
    assert!(matches!(
        History::new(vec![insert.clone(), insert.clone()]),
        Err(MergeError::DuplicateWrite(_))
    ));
    let mut duplicate = insert.clone();
    duplicate.id.number = 2;
    assert!(matches!(
        History::new(vec![insert.clone(), duplicate]),
        Err(MergeError::DuplicateTimestamp(_, _))
    ));
    let missing = write(2, 2, &[1], vec![]);
    assert!(matches!(
        History::new(vec![missing]),
        Err(MergeError::MissingWrite(_))
    ));
    let bad_time = write(2, 0, &[1], vec![]);
    assert!(matches!(
        History::new(vec![insert.clone(), bad_time]),
        Err(MergeError::CausalTimestamp(_))
    ));
    for (generation, kind) in [(0, 1), (0, 2), (1, 0)] {
        let bad = write(2, 2, &[1], vec![(row(1), change(generation, kind, &[]))]);
        assert!(matches!(
            History::new(vec![insert.clone(), bad]),
            Err(MergeError::GenerationParity(_))
        ));
    }
    let unseen = write(2, 2, &[], vec![(row(1), change(1, 1, &[]))]);
    assert!(matches!(
        History::new(vec![insert.clone(), unseen]),
        Err(MergeError::GenerationNotSeen { .. })
    ));
    let overflow = write(2, 2, &[1], vec![(row(1), change(u64::MAX, 2, &[]))]);
    assert!(matches!(
        History::new(vec![insert.clone(), overflow]),
        Err(MergeError::GenerationExhausted)
    ));
    let mut wrong_device = insert.clone();
    wrong_device.id.device = 8;
    assert!(matches!(
        History::new(vec![wrong_device]),
        Err(MergeError::TimestampDevice(_))
    ));
    let empty = History::<String>::new([]).unwrap();
    assert!(from_writes(&empty).unwrap().is_empty());
    assert!(matches!(
        empty.had_read(id(1), id(2)),
        Err(MergeError::MissingWrite(_))
    ));
}

#[test]
fn generation_witness_can_be_a_noncanonical_insert() {
    let writes = vec![
        write(1, 1, &[], vec![(row(1), change(0, 0, &[]))]),
        write(2, 2, &[], vec![(row(1), change(0, 0, &[]))]),
        write(
            3,
            3,
            &[2],
            vec![(row(1), change(1, 1, &[("0", "read insert 2")]))],
        ),
    ];
    let states = agree(&writes, &[&[0, 1, 2], &[1, 2, 0]]);
    assert_eq!(states[&row(1)].generations()[&1], id(1));
    assert_eq!(value(&states, &row(1), "0"), "read insert 2");
}
