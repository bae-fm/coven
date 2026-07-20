pub mod apply;
pub mod audience_package;
#[cfg(test)]
mod blob_content_hash_tests;
// Shared backoff math: the sync loop and blob engine's per-upload wait
// (`crate::blob::upload`) both count attempts in multiples of one base interval,
// so the formula is `pub(crate)`.
pub mod backoff;
pub(crate) mod blocking;
pub(crate) mod causal_grants;
pub mod circle;
pub(crate) mod circle_activation;
pub mod circle_control;
pub mod circle_ops;
pub mod circle_roster;
pub mod cloud_storage;
pub mod conflict;
pub mod cycle;
pub mod device_join;
pub mod gate;
pub mod hlc;
// Exercises the register clock through `Database::hlc()`.
#[cfg(test)]
mod cycle_tests;
#[cfg(test)]
mod hlc_register_tests;
pub mod invite;
pub mod loop_policy;
pub mod membership;
pub mod membership_ops;
pub mod provider;
pub mod pull;
#[cfg(test)]
mod pull_tests;
#[cfg(test)]
mod refresh_tests;
pub(crate) mod remote_object;
pub mod restore_code;
pub(crate) mod retained_replay;
#[cfg(test)]
mod rotation_pending_tests;
pub(crate) mod routing_contract;
pub mod service;
pub mod session;
pub mod snapshot;
pub mod status;
pub mod storage;
pub mod store_ack;
pub mod store_commit;
pub mod store_device_exclusion;
pub mod store_objects;
pub mod store_outbound;
pub mod store_protocol_root;
pub mod store_pull;
pub mod store_reclaim;
pub(crate) mod store_reclaim_journal;
pub mod store_registration;
pub mod store_snapshot;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_helpers;
#[cfg(test)]
mod tests;
pub mod wrapped_store_key;
