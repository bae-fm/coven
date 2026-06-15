pub mod apply;
mod backoff;
pub mod cloud_storage;
pub mod conflict;
pub mod cycle;
#[cfg(test)]
mod cycle_tests;
#[cfg(test)]
mod delete_propagation_tests;
pub mod envelope;
pub mod gate;
pub mod hlc;
#[cfg(test)]
mod hlc_register_tests;
pub mod invite;
#[cfg(test)]
mod item_key_tests;
// join/restore are the "connect a real cloud backend, then bootstrap a library
// onto local disk" orchestration. Every backend they construct is native-only,
// and only host entry points (CLI/macOS/iOS) call them — nothing in coven's core
// sync does. The browser-runtime work rebuilds these against wasm backends.
#[cfg(not(target_arch = "wasm32"))]
pub mod join;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod join_tests;
pub mod membership;
pub mod membership_ops;
pub mod outbox;
#[cfg(test)]
mod outbox_tests;
pub mod pull;
#[cfg(test)]
mod pull_tests;
pub mod push;
#[cfg(not(target_arch = "wasm32"))]
pub mod restore;
pub mod restore_code;
pub mod service;
pub mod session;
pub mod snapshot;
pub mod status;
pub mod storage;
pub mod sync_loop;
// The host-facing connected-sync controller: builds the cloud home + sync loop
// and drives membership. Every method that does real work constructs a
// native-only backend (via create_cloud_home / create_sync_storage / init_sync),
// and only host entry points build it. The browser-runtime work introduces a
// wasm manager that drives the loop off the JS event loop.
#[cfg(not(target_arch = "wasm32"))]
pub mod sync_manager;
#[cfg(test)]
pub(crate) mod test_helpers;
#[cfg(test)]
mod tests;
