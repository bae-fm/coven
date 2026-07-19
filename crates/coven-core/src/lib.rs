//! coven — end-to-end encrypted, multi-writer, bring-your-own-storage SQLite
//! sync, with an encrypted blob store and a cryptographic membership model.
//!
//! The host app owns its SQLite schema and domain. coven owns the sync layer:
//! changesets captured via the SQLite session extension, HLC-stamped
//! and signed per author, encrypted and pushed/pulled through a pluggable
//! `CloudHome`, conflict-resolved per row by `_updated_at` arbitration with a
//! column-level premerge so concurrent edits to different columns of a row survive.
//! An append-only Ed25519-signed membership chain wraps the per-store
//! symmetric key to each member.
//!
//! Integration contract for the host:
//! - coven OWNS the SQLite connection. The host opens it once with
//!   [`Coven::builder(config)`], declares synced tables and its synced-schema
//!   [`Migration`] ladder, and calls `open`: coven installs or verifies its one
//!   current internal schema, then runs the host's migration ladder over
//!   `PRAGMA user_version` for the app's tables and seeds the register clock off
//!   the rows on disk. The host runs its SQL through [`CovenHandle::sql`] or
//!   [`CovenHandle::write`]; coven captures each write into the pending-changeset
//!   journal as it commits, inside its own journaled transaction.
//! - Every synced table has an `id` text primary key at column 0 and an
//!   `_updated_at TEXT NOT NULL` column, and is declared as a
//!   [`SyncedTable`] in the builder's `synced_tables` set with a required
//!   [`RowIdentity`]. `(table, id)` is one logical row across every device.
//!   Independently created rows use canonical UUIDv4 or UUIDv7 ids; shared-key
//!   tables intentionally merge equal application keys. A primary-key change
//!   removes the old identity and inserts the new validated identity. A plain
//!   [`sync::session::SyncedTable::new`] table syncs unconditionally;
//!   [`sync::session::SyncedTable::remote_root`] also syncs the whole table and
//!   makes blobs on the row or its FK-descendants always Remote;
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
//!   optional [`blob::BlobTransitionObserver`] to the builder. A local-only store
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
//!   [`blob::transition`]). A remote root starts Remote and rejects those
//!   transitions.
//!
//! The **cache** ([`blob::cache`]) is a Remote-only mechanism: it holds
//! re-fetchable copies of Remote blobs under `storage/cache/<namespace>/…`
//! (evictable, against a per-namespace size budget — [`CovenHandle::set_cache_budget`])
//! and `storage/pinned/<namespace>/…` (kept). A Local blob is never in the cache,
//! and `CacheEager`/`CacheLazy`/pin/budget describe a blob only while it is Remote.
//! An *asset* (a cover, an artist image — [`sync::session::SyncedTable::asset`])
//! rides its subject's gate but never keeps the subject alive.

// coven-core's documented public API is exactly the crate-root re-exports below.
// The implementation modules are `#[doc(hidden)] pub`: reachable by `coven`,
// which needs the internals to build the public engine, but excluded from the
// documented surface and not part of the host API. A host
// depends on `coven`, whose own modules are `pub(crate)`, so it reaches the
// engine only through the curated re-exports, never through `coven_core::sync::…`
// or `coven::sync::…`.
//
// The blob engine: the vocabulary types plus the two lifecycle halves —
// `blob::cache` (bytes on disk, folder-truth retention) and `blob::upload` (drain
// the durable upload queue to the cloud).
#[doc(hidden)]
pub mod blob;
#[doc(hidden)]
pub mod changeset;
#[doc(hidden)]
pub mod clock;
mod write;
// Shared wire format for pasted codes (invite, restore): prefix + base64url(json).
mod code_envelope;
#[doc(hidden)]
pub mod config;
#[doc(hidden)]
pub mod database;
#[doc(hidden)]
pub mod db;
#[doc(hidden)]
pub mod encryption;
#[doc(hidden)]
pub mod id_provider;
#[doc(hidden)]
pub mod join_code;
#[doc(hidden)]
pub mod keys;
#[doc(hidden)]
pub mod store_dir;
// The host's synced-schema ladder: ordered migrations tracked in `PRAGMA
// user_version`, which doubles as the wire `schema_version`.
#[doc(hidden)]
pub mod migration;
// The device-local plaintext file behind each `BlobRef`: read on push, written on
// pull, backed directly by the local filesystem.
#[doc(hidden)]
pub mod local_blob;
#[doc(hidden)]
pub mod storage;
#[doc(hidden)]
pub mod sync;

// The curated public API — the only names a host is meant to touch.

pub use database::DbError;

/// The exact `rusqlite` coven owns the connection through. The host runs its app
/// SQL via [`CovenHandle::sql`] / [`CovenHandle::write`] against this same crate
/// — use `coven::rusqlite::{params, Row, …}` rather than depending on `rusqlite`
/// directly, so the host can never drift onto a `libsqlite3-sys` version that
/// conflicts with coven's.
pub use rusqlite;

// Blob descriptors, errors, the host-implemented observer.
pub use blob::cache::BlobCacheError;
pub use blob::{
    BlobRef, BlobReplacement, BlobScope, BlobTransitionObserver, CacheFill, Provenance,
    RowBlobAuthority, RowBlobRef,
};

// Applied-sync change notification (the host reacts to these).
pub use changeset::{ChangeOp, RowChange};

// Host schema declaration: the synced-table set plus the synced-schema migration
// ladder the host registers with the builder.
pub use migration::{Migration, MigrationStep};
pub use sync::session::{BlobDecl, RowIdentity, SyncedTable};

// Config.
pub use config::{
    CloudHomeConfig, CloudProvider, Config, ConfigError, CustomS3ExactSlots, CustomS3Serial,
    HomeStorage,
};

// Keys / oauth / keyring bootstrap. The keyring service name has a setter and the
// two getters that pair with it; the OAuth registration takes/returns its creds
// and tokens.
pub use keys::{CloudHomeCredentials, KeyError, MasterKeyCustody, UserKeypair};

// At-rest crypto the host configures (the host sizes cloud stream reads from
// `CHUNK_SIZE`), and the store directory the host points coven at. `MasterKeyring`
// is the master-key custody value type; `EncryptionService` is the cipher coven
// builds from it internally. `SealError` is what the handle's app-data sealing
// returns.
pub use encryption::{
    EncryptionError, EncryptionService, KeyFingerprint, MasterKeyring, SealError, CHUNK_SIZE,
};
pub use store_dir::{StoreDir, StoreLayout};

// Sync vocabulary exposed through the public handle.
pub use sync::circle::{
    Audience, CircleId, CircleInfo, CircleMemberInfo, CircleOperationId, CircleOperationInfo,
    CircleOperationKind, CircleOperationState, CircleRole,
};
pub use sync::device_join::{
    abandon_device_join, accept_device_registration_request, activate_device_join_cleanup,
    authorize_device_provider_access, begin_device_join, bootstrap_pending_device,
    cancel_device_join, close_device_provider_admission, close_joining_device,
    complete_device_join, complete_device_provider_admission, complete_joiner_device_join_cleanup,
    complete_owner_device_join_cleanup, device_join_status, finalize_device_join,
    load_current_device_join_authorization, load_pending_device_join_actions,
    load_pending_device_join_status, load_store_device_join_actions, load_store_device_join_status,
    observe_device_join_abandonment, observe_device_join_activation, prepare_device_join_cleanup,
    prepare_device_provider_access_request, prepare_device_registration_request,
    publish_device_provider_challenge, revoke_device_provider_admission_writes,
    revoke_joining_device_writes, DeviceJoinAbandonment, DeviceJoinAction, DeviceJoinActivation,
    DeviceJoinAuthorization, DeviceJoinCancellation, DeviceJoinCleanupActivation,
    DeviceJoinCleanupProgress, DeviceJoinCleanupReceipt, DeviceJoinError,
    DeviceJoinJournalDatabase, DeviceJoinJournalRecord, DeviceJoinOffer, DeviceJoinProducer,
    DeviceJoinProducerWriteRevocation, DeviceJoinRole, DeviceJoinStatus,
    DeviceProviderAccessAdministrator, DeviceProviderAccessRequest, DeviceProviderAdmission,
    DeviceProviderAdmissionApproval, DeviceProviderAdmissionCompletion, DeviceProviderReadiness,
    DeviceRegistrationRequest, JoinedStore, JoinerJoinClosure, JoinerJoinTerminal,
    ProviderAdminJoinClosure, ProviderAdminJoinTerminal, ProviderReadyDeviceBootstrap,
    ProvisionalDeviceBootstrap,
};
pub use sync::hlc::{Hlc, Timestamp, UpdatedAtStamper};
pub use sync::membership::{MemberInfo, MemberRole};

// Sync setup / restore / join bootstrap.
pub use join_code::{decode_invite_code_info, decode_join_request, JoinCodeError};
pub use sync::restore_code::{
    decode_restore_code_info, ActivatedContinuation, OwnerRecoveryAuthority, RestoreAuthority,
};

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
pub use write::{
    AffectedRow, PendingBranch, PendingBranchId, PendingWrite, PublishedPosition,
    SerializationConflict, WriteBlock, WriteId, WritePolicy, WriteReceipt, WriteResolution,
    WriteStatus,
};

// Managed local blob store: the host constructs it; coven never does.
pub use storage::cloud::{
    BlobBody, BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudHeadCreateError,
    CloudHeadReplaceError, CloudHeadVersion, CloudHome, CloudHomeError, CloudHomeJoinInfo,
    CloudVersionedHead, PartSink, UploadProgress,
};

// Mobile OAuth: hosts whose OS captures the redirect drive the flow through
// these instead of the desktop browser-callback `sign_in_*` above.
pub use sync::store_pull::{HeldStoreCoordinate, HeldStorePosition, HeldStorePositionReason};

// Sync-status surface: the completed-cycle success payload, the per-cycle alert
// bundle it carries, and the per-device activity a host renders "which devices
// synced, and when" from.
pub use sync::loop_policy::{SyncLoopAlerts, SyncLoopSuccess};
pub use sync::status::DeviceActivity;
pub use sync::store_commit::{
    CommitFrontier, ObjectHash, StoreBatchCommitRef, StoreCommitCoord, StoreCommitOrder,
    StoreSerialPredecessor,
};

// In-memory cloud home for host integration tests.
#[cfg(any(test, feature = "test-utils"))]
pub use storage::cloud::test_utils::InMemoryCloudHome;
