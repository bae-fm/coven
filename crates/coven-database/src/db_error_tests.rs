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
            primary_key: "deleted-note".into(),
        }],
        reason: WriteRebaseConflictReason::MissingTarget,
    };
    let error = DbError::context(
        "replace unpublished suffix",
        DbError::PayloadCleanupFailed {
            operation: Box::new(DbError::from(conflict.clone())),
            cleanup: Box::new(DbError::Message("payload cleanup failed".into())),
        },
    );
    assert_eq!(error.write_rebase_conflict(), Some(&conflict));
    assert!(error.to_string().contains("payload cleanup failed"));
    assert!(DbError::Message("transport failure".into())
        .write_rebase_conflict()
        .is_none());
}
