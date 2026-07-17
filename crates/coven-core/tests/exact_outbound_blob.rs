use coven_core::database::exercise_exact_outbound_blob_graph;

#[test]
fn store_and_circle_blob_publication_require_body_locator_and_binding() {
    for circle in [false, true] {
        assert!(exercise_exact_outbound_blob_graph(circle, false, true, true).is_err());
        assert!(exercise_exact_outbound_blob_graph(circle, true, false, true).is_err());
        assert!(exercise_exact_outbound_blob_graph(circle, true, true, false).is_err());
        exercise_exact_outbound_blob_graph(circle, true, true, true).unwrap();
    }
}
