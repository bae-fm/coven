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
//!   [`Coven::builder(config)`], declares synced tables and its synced-schema
//!   [`Migration`] ladder, and calls `open`: coven runs its own bookkeeping
//!   migration, then the host's migration ladder over `PRAGMA user_version` for the
//!   app's tables, seeds the register clock off the rows on disk, attaches the
//!   capture session, and spawns the connection thread. The host runs its SQL
//!   through [`CovenHandle::sql`] or [`CovenHandle::write`]; coven captures those
//!   writes through the attached session.
//! - Every synced table has an `id` text primary key at column 0 and an
//!   `_updated_at TEXT NOT NULL` column, and is declared as a
//!   [`SyncedTable`] in the builder's `synced_tables` set. A plain
//!   [`sync::session::SyncedTable::new`] table syncs unconditionally;
//!   [`sync::session::SyncedTable::gated_by`] marks a *gated root* whose boolean
//!   gate column decides, per row, whether that row and its declared
//!   FK-descendants are shared. See [`sync::gate`] for the gating semantics.
//! - `_updated_at` is coven's last-writer-wins register, an opaque Hybrid Logical
//!   Clock stamp the host mints with [`SqlContext::stamp`] and binds into every
//!   synced-row write. The host must not parse or compare the stamp as a wall-clock
//!   time; coven advances the clock past pulled rows so a later local write always
//!   sorts causally after them, which a wall clock cannot guarantee under skew.
//! - When a cloud provider is connected, the host calls
//!   [`CovenHandle::connect_sync`]. Which rows carry blobs is declared per table
//!   via [`sync::session::SyncedTable::carries_blob`]; the host also supplies an
//!   optional [`blob::BlobTransitionObserver`] to the builder. A local-only library
//!   that never connects a provider still reads and writes through the handle.
//!
//! # Blob storage model
//!
//! A blob is opaque bytes a synced row references — a photo, an audio file, a
//! cover image. Each blob declares two orthogonal properties and has one runtime
//! state. [`blob`] holds the full concept tree; the summary:
//!
//! - **Provenance** ([`blob::Provenance`]) — where the bytes live while the blob is
//!   *Local*. *User-provided*: the user's own file at a path coven references (the
//!   Remote→Local restore writes back to a user path, so it needs one).
//!   *Host-provided*: data the host hands coven, which coven keeps in its own local
//!   store at `storage/local/<namespace>/<id>` (no path needed to restore).
//! - **Cache fill** ([`blob::CacheFill`]) — how a device gets the bytes while the
//!   blob is *Remote*. [`CacheEager`](blob::CacheFill::CacheEager) fetches into the
//!   cache on pull (cover art, so a grid renders from local bytes);
//!   [`CacheLazy`](blob::CacheFill::CacheLazy) fetches on first read (audio).
//! - **Locality** — the state: *Local* (bytes on-device, in the user's file or
//!   coven's local store) or *Remote* (bytes in the cloud, each device's copy a
//!   cache copy). `make_remote` uploads the bytes and flips a gated root's gate on;
//!   `make_local` brings them back to a local file and flips it off (see
//!   [`blob::transition`]).
//!
//! The **cache** ([`blob::cache`]) is a Remote-only mechanism: it holds
//! re-fetchable copies of Remote blobs under `storage/cache/<namespace>/…`
//! (evictable, against a per-namespace size budget — [`CovenHandle::set_cache_budget`])
//! and `storage/pinned/<namespace>/…` (kept). A Local blob is never in the cache,
//! and `CacheEager`/`CacheLazy`/pin/budget describe a blob only while it is Remote.
//! An *asset* (a cover, an artist image — [`sync::session::SyncedTable::asset`])
//! rides its subject's gate but never keeps the subject alive.

// The blob engine: the vocabulary types plus the two lifecycle halves —
// `blob::cache` (bytes on disk, folder-truth retention) and `blob::upload` (drain
// the durable upload queue to the cloud). Documented at the module.
pub(crate) mod blob;
pub(crate) mod changeset;
pub(crate) mod clock;
pub(crate) mod config;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod coven;
pub(crate) mod database;
pub(crate) mod db;
pub(crate) mod encryption;
// The native data handle: one object coven opens that owns coven's pieces
// (the `Database`, the `LibraryDir`, the keys, and — once a provider is connected
// — the `SyncManager`) and exposes the whole data interface as methods. The
// native counterpart of the browser-only `wasm_facade::CovenLibrary`. Documented
// at the module.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod handle;
pub(crate) mod id_provider;
pub(crate) mod join_code;
pub(crate) mod keys;
pub(crate) mod library_dir;
// The host's synced-schema ladder: ordered migrations tracked in `PRAGMA
// user_version`, which doubles as the wire `schema_version`. Documented at the
// module.
pub(crate) mod migration;
// The device-local plaintext file behind each `BlobRef`: read on push, written on
// pull. Native uses the filesystem; wasm uses OPFS. Documented at the module.
pub(crate) mod local_blob;
pub(crate) mod oauth;
#[cfg(feature = "share-proxy")]
pub(crate) mod share;
pub(crate) mod storage;
pub(crate) mod sync;
// Browser-only storage setup: installing the OPFS-backed SQLite VFS that makes
// the wasm `Database` durable across page loads. Documented at the module.
#[cfg(all(target_arch = "wasm32", feature = "experimental-wasm"))]
pub mod wasm;

// Browser-only key persistence: the device's Ed25519 identity, sealed with a
// non-extractable WebCrypto key in IndexedDB. The async counterpart of the native
// `KeyService` (whose synchronous `keyring_core::Store` can't wrap async browser
// crypto/storage). Documented at the module.
#[cfg(all(target_arch = "wasm32", feature = "experimental-wasm"))]
pub mod wasm_keystore;

// Browser-only: the JS-callable facade (`CovenLibrary`) that assembles coven's
// whole browser stack — OPFS storage, the `Database`, the cipher + blob-path
// choices, the fetch-based S3 cloud home, and the event-loop sync runtime — behind
// one `wasm_bindgen` object a web page drives. Documented at the module.
#[cfg(all(target_arch = "wasm32", feature = "experimental-wasm"))]
pub mod wasm_facade;

// Headless proof that the facade assembles a working library: two `CovenLibrary`
// instances over one shared in-memory cloud converge through the public facade API
// (`exec` / `query` / `start_sync` / `sync_now`), no live S3 needed. Inside the
// crate because it reaches the crate-internal `from_home` seam and the
// `#[cfg(test)]` in-memory cloud; see the module.
#[cfg(all(test, target_arch = "wasm32", feature = "experimental-wasm"))]
mod wasm_facade_test;

// Headless proof that the wasm `Database` persists on OPFS through coven's own
// API. Inside the crate (not `tests/`) because it drives the crate-private
// `take_changeset` capture path. Worker-only — see the module.
#[cfg(all(test, target_arch = "wasm32", feature = "experimental-wasm"))]
mod wasm_opfs_test;

// Headless proof that the sync engine runs on wasm: a row crosses from one
// `Database` to another through capture → push → pull → apply over a shared
// in-memory cloud. Inside the crate because it reaches crate-internal sync test
// helpers; see the module.
#[cfg(all(test, target_arch = "wasm32", feature = "experimental-wasm"))]
mod wasm_sync_test;

// Headless proof that the wasm sync RUNTIME (not a manual cycle call) drives two
// devices to convergence off the browser event loop: each device's
// `WasmSyncRuntime` ticks on `spawn_local` + gloo-timers, and a row written on
// one converges to the other. Inside the crate because it reaches crate-internal
// sync test helpers; see the module.
#[cfg(all(test, target_arch = "wasm32", feature = "experimental-wasm"))]
mod wasm_runtime_test;

// Headless proof that device-local blob storage works on OPFS: `local_blob`
// round-trips directly, and a photo blob crosses two devices through the real
// cycle (push reads it from OPFS, pull writes it to OPFS). Inside the crate
// because it reaches crate-internal sync test helpers; see the module.
#[cfg(all(test, target_arch = "wasm32", feature = "experimental-wasm"))]
mod wasm_blob_opfs_test;

// Headless proof that the browser keystore persists the device identity: a keypair
// minted on first `open` comes back byte-for-byte on a later `open`, and survives
// the in-memory handle being dropped (so it really round-tripped through IndexedDB
// + WebCrypto, not a cache). See the module.
#[cfg(all(test, target_arch = "wasm32", feature = "experimental-wasm"))]
mod wasm_keystore_test;

// coven's public API is exactly this set of crate-root re-exports. Every
// implementation module is `pub(crate)`; a host reaches coven only through these
// names, never through `coven::blob::…` or `coven::sync::…`.

// Top-level native handle + database error.
#[cfg(not(target_arch = "wasm32"))]
pub use coven::{
    Coven, CovenBuilder, CovenConfig, CovenError, CovenResult, PendingBlob, SqlContext, WriteBatch,
};
pub use database::DbError;
#[cfg(not(target_arch = "wasm32"))]
pub use handle::CovenHandle;

/// The exact `rusqlite` coven owns the connection through. The host runs its app
/// SQL via [`CovenHandle::sql`] / [`CovenHandle::write`] against this same crate
/// — use `coven::rusqlite::{params, Row, …}` rather than depending on `rusqlite`
/// directly, so the host can never drift onto a `libsqlite3-sys` version that
/// conflicts with coven's.
pub use rusqlite;

// Blob descriptors, errors, the host-implemented observer.
pub use blob::cache::BlobCacheError;
// The Remote→Local transition (and its error) is native-only.
#[cfg(not(target_arch = "wasm32"))]
pub use blob::transition::MakeLocalError;
pub use blob::{BlobRef, BlobScope, BlobTransitionObserver, CacheFill, Provenance, ResolvedScope};
// The logical `(namespace, id)` cloud reference — only exists with share-proxy,
// which is the only thing that puts a blob id into a cloud manifest.
#[cfg(feature = "share-proxy")]
pub use blob::BlobId;

// Applied-sync change notification (the host reacts to these).
pub use changeset::{ChangeOp, RowChange};

// Host schema declaration: the synced-table set plus the synced-schema migration
// ladder the host registers with the builder.
pub use migration::{Migration, MigrationStep};
pub use sync::session::{BlobDecl, SyncedTable};

// Config.
pub use config::{CloudHomeConfig, CloudProvider, Config, ConfigError, HomeStorage};

// Keys / oauth / keyring bootstrap. The keyring service name has a setter and the
// two getters that pair with it; the OAuth registration takes/returns its creds
// and tokens.
pub use keys::{
    keyring_service, read_keyring, set_keyring_service, CloudHomeCredentials, KeyError, KeyService,
};
pub use oauth::{set_oauth_client_creds, OAuthClientCreds, OAuthTokens};

// At-rest crypto the host configures (the host sizes cloud stream reads from
// `CHUNK_SIZE`), and the library directory the host points coven at.
pub use encryption::{EncryptionError, EncryptionService, CHUNK_SIZE};
pub use library_dir::LibraryDir;

// Sync vocabulary exposed through the native handle. The browser drives sync
// through `wasm_facade` instead.
pub use sync::hlc::Timestamp;
pub use sync::membership::MemberRole;
#[cfg(not(target_arch = "wasm32"))]
pub use sync::sync_manager::MemberInfo;

// Sync setup / restore / join bootstrap.
pub use join_code::{
    decode_invite_code_info, decode_join_request, generate_join_request, JoinCodeError,
};
pub use storage::cloud::setup::generate_restore_code;
#[cfg(not(target_arch = "wasm32"))]
pub use sync::join::join_from_invite_code;
#[cfg(not(target_arch = "wasm32"))]
pub use sync::restore::{restore_from_cloud, restore_from_code, RestoreSource};
pub use sync::restore_code::decode_restore_code_info;

// Cloud at-rest cipher (host configures / tests inject).
pub use sync::cloud_storage::CloudCipher;

// --- Host-facing surface with no internal coven caller: each is a public-API-only
//     item the host reaches (named by a public signature, or constructed/read by
//     the host), cross-checked against the host's usage and coven's module docs. ---

// Clock + id abstractions the host injects: real impls plus the deterministic
// test fakes coven shares so the host tests against the same ones.
pub use clock::{Clock, ClockRef, SystemClock};
#[cfg(any(test, feature = "test-utils"))]
pub use clock::{FixedClock, SteppingClock};
#[cfg(any(test, feature = "test-utils"))]
pub use id_provider::SequentialIdProvider;
pub use id_provider::{IdProvider, IdRef, UuidProvider};

// Managed local blob store: the host constructs it, coven never does (native-only).
#[cfg(not(target_arch = "wasm32"))]
pub use storage::local::BlobStore;

// The cloud backend the host provides: the `CloudHome` trait (whose `upload` names
// `UploadProgress`), plus the two concrete backends the host constructs/implements
// — `S3CloudHome` and the `CloudKitOps` glue (both native-only). (coven's own
// `SyncStorage` contract over `CloudHome` stays `pub(crate)` — the host implements
// `CloudHome`, never `SyncStorage`.)
#[cfg(not(target_arch = "wasm32"))]
pub use storage::cloud::{cloudkit::CloudKitOps, s3::S3CloudHome};
pub use storage::cloud::{
    BlobBody, BoxPartSink, CloudHome, CloudHomeError, CloudHomeJoinInfo, PartSink, UploadProgress,
};

// Mobile OAuth: hosts whose OS captures the redirect drive the flow through
// these instead of the desktop browser-callback `sign_in_*` above.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub use oauth::{
    authorize_provider, build_authorize_request_for_provider, exchange_code_for_provider,
};

// Pull-result rejection reports the host surfaces to the user.
pub use sync::pull::{InvalidSignature, RejectedUnauthorized};

// Per-provider sign-in entry points.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub use storage::cloud::setup::{sign_in_dropbox, sign_in_google_drive, sign_in_onedrive};

// In-memory cloud home for host integration tests.
#[cfg(any(test, feature = "test-utils"))]
pub use storage::cloud::test_utils::InMemoryCloudHome;

// Durable upload-queue rows the host's integration tests inspect (not production
// surface).
#[cfg(any(test, feature = "test-utils"))]
pub use db::{OutboxEntry, OutboxOperation};

// Item sharing (share-proxy feature). `ShareManifest::allows` is the gate a
// share server calls to authorize a blob request.
#[cfg(feature = "share-proxy")]
pub use share::{open_share, ShareError, ShareManifest, ShareProxy, ShareToken};

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
