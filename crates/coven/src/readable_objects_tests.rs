/// Outside a store, `coven_sync_state` shows the state sync computes from
/// the facts there: a new store's sync status says no storage is set up, so
/// the store hasn't synced.
#[test]
fn the_sync_state_reads_outside_a_store() {
    let connection = coven_database::test_utils::Connection::open_in_memory().unwrap();
    super::make_readable_objects(&connection, crate::DeviceId(7)).unwrap();
    let (state, pending): (String, Option<i64>) = connection
        .query_row("SELECT state,pending FROM coven_sync_state", [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert_eq!((state.as_str(), pending), ("not_synced", None));
}
