use super::DbError;

#[test]
fn db_error_fits_result_without_forcing_callers_to_box_it() {
    assert!(std::mem::size_of::<DbError>() <= 104);
}

#[test]
fn recorded_edit_conflict_survives_operation_context_and_cleanup_errors() {
    use coven_protocol::write::{
        AffectedRow, WriteId, WriteRebaseConflict, WriteRebaseConflictReason,
    };
    let conflict = WriteRebaseConflict {
        write_id: WriteId::from_generated("pending-edit".into()),
        affected_rows: vec![AffectedRow {
            table: "notes".into(),
            primary_key: "invalid-note".into(),
        }],
        reason: WriteRebaseConflictReason::Constraint {
            message: "CHECK constraint failed".into(),
        },
    };
    let error = DbError::context(
        "replace unpublished suffix",
        DbError::ChangeCaptureFailed {
            operation: Box::new(DbError::from(conflict.clone())),
            capture: Box::new(DbError::Message("change capture failed".into())),
        },
    );
    assert_eq!(error.write_rebase_conflict(), Some(&conflict));
    assert!(error.to_string().contains("change capture failed"));
    assert!(DbError::Message("transport failure".into())
        .write_rebase_conflict()
        .is_none());
}
