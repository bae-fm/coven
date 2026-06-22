pub mod apply;
mod backoff;
pub mod cloud_storage;
pub mod conflict;
pub mod cycle;
#[cfg(test)]
mod cycle_tests;
// Drives the native-only `join` bootstrap, so it builds only on native.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod delete_propagation_tests;
pub mod envelope;
pub mod gate;
pub mod hlc;
// Exercises the register clock through `Database::hlc()`, a native-only accessor
// (its sole consumer is the native-only SyncManager), so it builds only on native.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod hlc_register_tests;
pub mod invite;
#[cfg(test)]
mod item_key_tests;
// join/restore are the "connect a real cloud backend, then bootstrap a library
// onto local disk" orchestration. Every backend they construct is native-only,
// and only host entry points (CLI/macOS/iOS) call them — nothing in coven's core
// sync does — so they are native-only.
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
#[cfg(all(test, not(target_arch = "wasm32")))]
mod restore_tests;
pub mod service;
pub mod session;
pub mod signed_control;
pub mod snapshot;
pub mod status;
pub mod storage;
// The background sync loop runs on a dedicated OS thread (a current-thread tokio
// runtime that block_on's the loop) holding the `Database` handle. The browser
// is single-threaded — there is no thread to spawn and the wasm `Database` is
// `!Send` — and the loop's only consumer is the native-only `sync_manager`, so it
// is native-only.
#[cfg(not(target_arch = "wasm32"))]
pub mod sync_loop;
// The host-facing connected-sync controller: builds the cloud home + sync loop
// and drives membership. Every method that does real work constructs a
// native-only backend (via create_cloud_home / create_sync_storage / init_sync),
// and only host entry points build it — so it is native-only.
#[cfg(not(target_arch = "wasm32"))]
pub mod sync_manager;
#[cfg(test)]
pub(crate) mod test_helpers;
#[cfg(test)]
mod tests;
// The browser sync runtime drives cycles off the single JS event loop
// (`spawn_local` + gloo-timers), the wasm counterpart of the native thread-based
// `sync_loop`. It holds the `!Send` wasm `Database` directly, so it builds only on
// wasm.
#[cfg(target_arch = "wasm32")]
pub mod wasm_runtime;
