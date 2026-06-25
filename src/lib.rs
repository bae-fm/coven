//! coven — end-to-end encrypted, multi-writer, bring-your-own-storage SQLite
//! sync, with an encrypted blob store and a cryptographic membership model.
//!
//! The host app owns its SQLite schema and domain. coven owns the sync layer:
//! changesets captured via the SQLite session extension, HLC-stamped
//! and signed per author, encrypted and pushed/pulled through a pluggable
//! `CloudHome`, conflict-resolved by row-level last-writer-wins on `_updated_at`.
//! An append-only Ed25519-signed membership chain wraps the per-library
//! symmetric key to each member.
//!
//! Integration contract for the host:
//! - coven OWNS the SQLite connection. The host opens it once with
//!   [`database::Database::open`]`(path, synced_tables, device_id, migrate)`:
//!   coven runs its own bookkeeping migration, then the host's `migrate` closure
//!   for the app's tables, seeds the register clock off the rows on disk,
//!   attaches the capture session, and spawns the connection thread. The host
//!   runs all of its own SQL through [`database::Database::call`]`(|conn| …)`;
//!   coven captures those writes through the attached session.
//! - Every synced table has an `id` text primary key at column 0 and an
//!   `_updated_at TEXT NOT NULL` column, and is declared as a
//!   [`sync::session::SyncedTable`] in the set passed to `Database::open`. A plain
//!   [`sync::session::SyncedTable::new`] table syncs unconditionally;
//!   [`sync::session::SyncedTable::gated_by`] marks a *gated root* whose boolean
//!   gate column decides, per row, whether that row and its declared
//!   FK-descendants are shared. See [`sync::gate`] for the gating semantics.
//! - `_updated_at` is coven's last-writer-wins register, an opaque Hybrid Logical
//!   Clock stamp the host mints with the [`UpdatedAtStamper`] that
//!   `Database::open` returns (non-optional, already seeded) and binds into every
//!   synced-row write. The host must not parse or compare the stamp as a
//!   wall-clock time; coven advances the clock past pulled rows so a later local
//!   write always sorts causally after them, which a wall clock cannot guarantee
//!   under skew. Changeset envelopes and membership entries also carry an HLC
//!   stamp for ordering/debuggability, but it is not authorization-load-bearing —
//!   pull authorizes by signature and current write-capable membership, and
//!   revocation is enforced by key rotation.
//! - When (and only when) a cloud provider is connected, the host builds the
//!   [`sync::sync_manager::SyncManager`] lazily, passing it the same `Database`
//!   handle and synced-table set. `SyncManager::new` is synchronous and
//!   infallible — all seeding happened in `Database::open`. Which rows carry blobs
//!   is declared per table via [`sync::session::SyncedTable::carries_blob`]; the
//!   host also supplies an optional [`blob::BlobTransitionObserver`].
//!
//!   A local-only library that never connects a provider simply stamps and
//!   writes rows through `Database::call` without ever building a `SyncManager`,
//!   an `EncryptionService`, or a cloud config.

// The blob engine: the vocabulary types plus the two lifecycle halves —
// `blob::cache` (bytes on disk, folder-truth retention) and `blob::upload` (drain
// the durable upload queue to the cloud). Documented at the module.
pub mod blob;
pub mod changeset;
pub mod clock;
pub mod config;
pub mod database;
pub mod db;
pub mod encryption;
pub mod id_provider;
pub mod join_code;
pub mod keys;
pub mod library_dir;
// The device-local plaintext file behind each `BlobRef`: read on push, written on
// pull. Native uses the filesystem; wasm uses OPFS. Documented at the module.
pub mod local_blob;
pub mod oauth;
#[cfg(feature = "share-proxy")]
pub mod share;
pub mod storage;
pub mod sync;
// Browser-only storage setup: installing the OPFS-backed SQLite VFS that makes
// the wasm `Database` durable across page loads. Documented at the module.
#[cfg(target_arch = "wasm32")]
pub mod wasm;

// Browser-only key persistence: the device's Ed25519 identity, sealed with a
// non-extractable WebCrypto key in IndexedDB. The async counterpart of the native
// `KeyService` (whose synchronous `keyring_core::Store` can't wrap async browser
// crypto/storage). Documented at the module.
#[cfg(target_arch = "wasm32")]
pub mod wasm_keystore;

// Browser-only: the JS-callable facade (`CovenLibrary`) that assembles coven's
// whole browser stack — OPFS storage, the `Database`, the cipher + blob-path
// choices, the fetch-based S3 cloud home, and the event-loop sync runtime — behind
// one `wasm_bindgen` object a web page drives. Documented at the module.
#[cfg(target_arch = "wasm32")]
pub mod wasm_facade;

// Headless proof that the facade assembles a working library: two `CovenLibrary`
// instances over one shared in-memory cloud converge through the public facade API
// (`exec` / `query` / `start_sync` / `sync_now`), no live S3 needed. Inside the
// crate because it reaches the crate-internal `from_home` seam and the
// `#[cfg(test)]` in-memory cloud; see the module.
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_facade_test;

// Headless proof that the wasm `Database` persists on OPFS through coven's own
// API. Inside the crate (not `tests/`) because it drives the crate-private
// `take_changeset` capture path. Worker-only — see the module.
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_opfs_test;

// Headless proof that the sync engine runs on wasm: a row crosses from one
// `Database` to another through capture → push → pull → apply over a shared
// in-memory cloud. Inside the crate because it reaches crate-internal sync test
// helpers; see the module.
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_sync_test;

// Headless proof that the wasm sync RUNTIME (not a manual cycle call) drives two
// devices to convergence off the browser event loop: each device's
// `WasmSyncRuntime` ticks on `spawn_local` + gloo-timers, and a row written on
// one converges to the other. Inside the crate because it reaches crate-internal
// sync test helpers; see the module.
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_runtime_test;

// Headless proof that device-local blob storage works on OPFS: `local_blob`
// round-trips directly, and a photo blob crosses two devices through the real
// cycle (push reads it from OPFS, pull writes it to OPFS). Inside the crate
// because it reaches crate-internal sync test helpers; see the module.
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_blob_opfs_test;

// Headless proof that the browser keystore persists the device identity: a keypair
// minted on first `open` comes back byte-for-byte on a later `open`, and survives
// the in-memory handle being dropped (so it really round-tripped through IndexedDB
// + WebCrypto, not a cache). See the module.
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_keystore_test;

pub use database::Database;
pub use sync::hlc::UpdatedAtStamper;

/// The exact `rusqlite` coven owns the connection through. The host runs its app
/// SQL via [`Database::call`]`(|conn| …)` against this same crate — use
/// `coven::rusqlite::{params, Row, …}` rather than depending on `rusqlite`
/// directly, so the host can never drift onto a `libsqlite3-sys` version that
/// conflicts with coven's.
pub use rusqlite;

/// Thread-safety floor for coven's async storage traits ([`storage::cloud::CloudHome`],
/// [`sync::storage::SyncStorage`], [`blob::BlobTransitionObserver`]).
///
/// Native sync runs the connection on a thread actor and awaits multi-threaded
/// cloud SDKs, so those traits must be `Send + Sync` and their method futures
/// `Send`. The browser runs one thread: reqwest's wasm `Response` and the wasm
/// `Database`'s `Rc`-held state are `!Send`, and the engine drives every future
/// on that single thread. So this bound is `Send + Sync` on native and empty on
/// wasm, and the traits that carry it (plus their `#[async_trait]` futures) relax
/// to `?Send` there. The blanket impl makes it transparent — every type that
/// meets the native floor satisfies it, and on wasm it constrains nothing.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeThreadSafe: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync + ?Sized> MaybeThreadSafe for T {}

/// See the native [`MaybeThreadSafe`]. On wasm the engine drives every storage
/// future on the browser's single thread, so the floor is empty and every type
/// satisfies it.
#[cfg(target_arch = "wasm32")]
pub trait MaybeThreadSafe {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeThreadSafe for T {}
