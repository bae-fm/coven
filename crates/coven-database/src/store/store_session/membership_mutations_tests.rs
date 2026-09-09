use super::*;

/// The stream a key already selected is kept whenever it is still reusable,
/// so a caller that offers it back does not start a second stream.
#[tokio::test]
async fn a_reusable_selected_stream_is_returned_again() {
    let fixture_store_dir = crate::synthetic_store::test_store_dir();
    let fixture = crate::synthetic_store::open_test_db(fixture_store_dir.clone());
    let database = StoreDatabase::new(&fixture);
    let key = "circle_roster_author_stream/reselect".to_string();

    let selected = database
        .select_causal_author_stream(key.clone(), std::collections::BTreeSet::new())
        .await
        .expect("mint an author stream for a key holding none");

    assert_eq!(
        database
            .select_causal_author_stream(key, std::collections::BTreeSet::from([selected]))
            .await
            .expect("reselect the durable author stream"),
        selected
    );
}
