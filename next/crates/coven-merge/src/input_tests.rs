use super::*;
use uuid::Uuid;

fn row(audience: Audience) -> RowId {
    RowId {
        table: "t".into(),
        key: vec![1],
        audience,
    }
}

#[test]
fn change_validation_checks_parity_and_generation_exhaustion() {
    let row = row(Audience::Store);
    for (operation, generations) in [
        (
            Operation::<()>::Insert(BTreeMap::new()),
            [0, 2, u64::MAX - 1],
        ),
        (Operation::Update(BTreeMap::new()), [1, 3, u64::MAX]),
        (Operation::Delete, [1, 3, u64::MAX - 2]),
    ] {
        for generation in generations {
            let mut change = Change {
                generation,
                operation: operation.clone(),
            };
            assert_eq!(change.validate(&row), Ok(()));
            change.generation ^= 1;
            assert_eq!(
                change.validate(&row),
                Err(MergeError::GenerationParity(change.generation))
            );
        }
    }
    let delete = Change::<()> {
        generation: u64::MAX,
        operation: Operation::Delete,
    };
    assert_eq!(delete.validate(&row), Err(MergeError::GenerationExhausted));
}

#[test]
fn written_references_share_the_change_audience_check() {
    let store = Audience::Store;
    let first = Audience::Circle(CircleId(Uuid::from_u128(1)));
    let second = Audience::Circle(CircleId(Uuid::from_u128(2)));
    for (child, parent, allowed) in [
        (&store, &store, true),
        (&store, &first, false),
        (&store, &second, false),
        (&first, &store, true),
        (&first, &first, true),
        (&first, &second, false),
        (&second, &store, true),
        (&second, &first, false),
        (&second, &second, true),
    ] {
        let child = row(child.clone());
        let parent = Parent {
            row: row(parent.clone()),
            generation: 1,
        };
        let expected = if allowed {
            Ok(())
        } else {
            Err(MergeError::ReferenceAudience(child.clone()))
        };
        assert_eq!(parent.validate_written(&child), expected);
        let columns = BTreeMap::from([(
            "x".into(),
            ColumnValue {
                value: (),
                parents: BTreeMap::from([("fk".into(), parent)]),
            },
        )]);
        for (generation, operation) in [
            (0, Operation::Insert(columns.clone())),
            (1, Operation::Update(columns)),
        ] {
            assert_eq!(
                Change {
                    generation,
                    operation
                }
                .validate(&child),
                expected
            );
        }
    }
}

#[test]
fn change_validation_checks_each_written_parent_generation() {
    let child = row(Audience::Store);
    for (generation, expected) in [
        (0, Err(MergeError::ParentGeneration(0))),
        (1, Ok(())),
        (2, Err(MergeError::ParentGeneration(2))),
        (
            u64::MAX - 1,
            Err(MergeError::ParentGeneration(u64::MAX - 1)),
        ),
        (u64::MAX, Ok(())),
    ] {
        let parent = Parent {
            row: child.clone(),
            generation,
        };
        assert_eq!(parent.validate_written(&child), expected);
        let change = Change {
            generation: 1,
            operation: Operation::Update(BTreeMap::from([
                (
                    "a".into(),
                    ColumnValue {
                        value: (),
                        parents: BTreeMap::new(),
                    },
                ),
                (
                    "b".into(),
                    ColumnValue {
                        value: (),
                        parents: BTreeMap::from([
                            (
                                "first".into(),
                                Parent {
                                    row: child.clone(),
                                    generation: 1,
                                },
                            ),
                            ("second".into(), parent),
                        ]),
                    },
                ),
            ])),
        };
        assert_eq!(change.validate(&child), expected);
    }
}
