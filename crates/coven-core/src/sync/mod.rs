pub mod apply;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod blob_content_hash_tests;
// Shared backoff math: the sync loop and blob engine's per-upload wait
// (`crate::blob::upload`) both count attempts in multiples of one base interval,
// so the formula is `pub(crate)`.
pub mod backoff;
pub mod cloud_storage;
pub mod conflict;
pub mod cycle;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod cycle_tests;
pub mod gate;
pub mod hlc;
// Exercises the register clock through `Database::hlc()`, a native-only accessor
// (its sole consumer is the native-only SyncManager), so it builds only on native.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod hlc_register_tests;
pub mod invite;
pub mod loop_policy;
pub mod membership;
pub mod membership_ops;
pub(crate) mod publish_blobs;
pub mod pull;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod pull_tests;
#[cfg(test)]
mod refresh_tests;
pub mod restore_code;
#[cfg(test)]
mod rotation_pending_tests;
pub mod service;
pub mod session;
pub mod snapshot;
pub mod status;
pub mod storage;
pub mod store_ack;
pub mod store_commit;
pub mod store_genesis;
pub mod store_objects;
pub mod store_outbound;
pub mod store_pull;
pub mod store_reclaim;
pub mod store_registration;
pub mod store_snapshot;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_helpers;
#[cfg(test)]
mod tests;
pub mod wrapped_store_key;
