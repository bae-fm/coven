use super::*;

#[derive(serde::Serialize)]
struct Reached(u32);
impl OperationState for Reached {}

fn record(last_step: u32) -> OperationRecord {
    OperationRecord {
        id: coven_database::OperationId(7),
        kind: "test".into(),
        last_step,
        data: Vec::new(),
        started_by: "coven".into(),
    }
}

/// A step goes one past the row read, guarded by that row's step, and
/// records the state reached.
#[test]
fn an_update_advances_one_step_from_the_row_it_read() {
    let update = Reached(3).update(&record(4)).unwrap();
    assert_eq!((update.previous, update.last_step), (4, 5));
    assert_eq!(update.data, b"3");
}

/// An operation's steps fitting u32 is an invariant coven keeps, a bug when
/// broken, not the store's sequence running out.
#[test]
fn a_step_past_u32_is_a_broken_invariant() {
    assert!(matches!(
        Reached(0).update(&record(u32::MAX)),
        Err(SyncError::Failed { failure: crate::LocalFailure::Bug, .. })
    ));
}
