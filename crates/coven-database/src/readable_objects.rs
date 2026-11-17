//! Every object of coven's that app SQL may read, made on a connection
//! outside any store, for an app's test that runs its own schema there
//! (api.md E4).

use coven_foundation::id_source::{DeviceId, FileId};
use sqlive::rusqlite::Connection;

/// Make on `connection`, a connection an app's test opened itself outside
/// any store, every object of coven's that app SQL may read, as every store
/// makes it: the tables the app's local triggers may follow,
/// `coven_sync_status` and `coven_retry_effect` with their one row, and
/// coven's views, with the tables of coven's own that they read. The
/// function registers coven's SQL functions too, with `device` as this
/// device's id and `sync_state` as the sync state's computation, which the
/// coven crate passes from sync as its builder does. Nothing transfers there
/// and nothing watches the network, as on a read-only handle. The test runs this before the app's schema, so the
/// app's triggers and views apply and run as in a store. Nothing of coven's
/// writes the objects there, and coven's triggers that keep the followed
/// tables in a store aren't made: the test writes the followed tables' rows
/// that coven would, and records a user's original with `record_user_file`.
pub fn make_readable_objects(
    connection: &Connection,
    device: DeviceId,
    sync_state: &crate::SyncStateFunction,
) -> sqlive::rusqlite::Result<()> {
    let policy = crate::app_policy::CovenPolicy::new(&[]);
    sqlive::Setup::on_test_connection(connection, policy.clone(), |setup| {
        crate::coven_functions::register(setup, &policy)?;
        // Nothing starts a transfer or hears of the network here, so
        // nothing tells this observer of a change.
        let observer = crate::store_failure::StoreHandle::recording_all(
            coven_foundation::task::HeardBugs::shared(),
        );
        crate::coven_functions::register_views(
            setup,
            device,
            &crate::transfers_in_flight::TransfersInFlight::new(observer.clone()),
            &crate::network_facts::NetworkFacts::new(observer),
            Some(sync_state),
        )
    })?;
    connection.execute_batch(&crate::internal_schema::readable_schema())
}

/// Record on `connection`, where `make_readable_objects` made coven's
/// objects, the original at `path`, last modified at `modified_at` and
/// holding `bytes`, as `file`'s original on this device, as staging it for
/// the cloud records it: its size, modification time, content hash and
/// chunk hashes (§17.3). `coven_user_files` then lists it.
pub fn record_user_file(
    connection: &Connection,
    file: FileId,
    path: &std::path::Path,
    modified_at: std::time::SystemTime,
    bytes: &[u8],
) -> Result<(), crate::DbError> {
    let mut hasher = crate::FileHasher::new();
    hasher.update(bytes);
    let original = crate::UserFile {
        path: path.to_owned(),
        size: u64::try_from(bytes.len()).expect("every slice's length fits in 64 bits"),
        modified_at,
    };
    crate::user_file::record_outside_store(connection, file, &original, &hasher.finish())
}

#[cfg(test)]
#[path = "readable_objects_tests.rs"]
mod tests;
