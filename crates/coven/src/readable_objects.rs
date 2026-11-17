//! Coven's objects that app SQL may read, made outside any store for an
//! app's test of its own schema (api.md E4), with sync's computation of the
//! sync state, which the database crate can't reach.

/// Make on `connection`, a connection an app's test opened itself outside
/// any store, every object of coven's that app SQL may read, and register
/// coven's SQL functions there, with `device` as this device's id, as
/// `coven_database::test_utils::make_readable_objects` says. The sync state
/// function is sync's own, as every store registers it.
pub fn make_readable_objects(
    connection: &coven_database::test_utils::Connection,
    device: coven_foundation::id_source::DeviceId,
) -> coven_database::SqliteResult<()> {
    coven_database::test_utils::make_readable_objects(
        connection,
        device,
        &coven_sync::sync_state_function(),
    )
}

#[cfg(test)]
#[path = "readable_objects_tests.rs"]
mod tests;
