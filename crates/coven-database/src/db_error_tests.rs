use super::DbError;

#[test]
fn db_error_fits_result_without_forcing_callers_to_box_it() {
    assert!(std::mem::size_of::<DbError>() <= 104);
}
