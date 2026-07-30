//! The blob engine: coven's single owner of a blob's whole durability lifecycle.
//!
//! coven syncs opaque encrypted blobs referenced by DB rows. By default it owns
//! the cloud layout (the content-addressed `{namespace}/{ab}/{cd}/{id}`) and
//! encryption; the host decides which rows carry blobs, where their plaintext
//! lives locally, and how each is scoped for encryption. A home configured for
//! the unobfuscated blob-path scheme instead stores each blob at the consumer's
//! readable [`BlobRef::cloud_path`] so the bucket is browsable.
//!
//! # The coven concept tree
//!
//! A blob has two **declared** properties — [`Provenance`] (its Local story) and
//! [`CacheFill`] (its Remote story) — and one **state**, locality, flipped by the
//! transitions. The cache is a *mechanism* that serves Remote blobs; it is not a
//! kind of blob.
//!
//! ```text
//! A blob the host declares with:
//!
//! provenance — its LOCAL story: where the bytes live when Local, and the
//!              Remote→Local path requirement
//!    ├─ user-provided   the user's file at a path; coven references it.
//!    │                  Remote→Local writes the bytes back to a user file → NEEDS A PATH.
//!    └─ host-provided   bae hands coven the data; coven keeps it in its local store.
//!                       Remote→Local restores it to the local store → no path.
//!
//! cache fill — its REMOTE story: how a device gets the bytes when the release is
//!              Remote. A cache-mechanism setting; applies to ANY blob, regardless of
//!              provenance, once it is Remote.
//!    ├─ CacheEager   fetched into the cache on pull, with the SQL row   (covers)
//!    └─ CacheLazy    fetched into the cache on first read               (audio — big, fetch what you play)
//!
//! and a current state:
//!
//! locality
//!    ├─ Local    bytes on-device — the user's path (user-provided) or coven's local store (host-provided)
//!    └─ Remote   bytes in the cloud; each device's local copy is a CACHE copy, filled
//!                per `cache fill`, kept-or-evicted per `pin`
//!
//! namespace (bucket)   the blob's category — release_files · covers · artist_images
//!
//! transitions
//!    ├─ Local → Remote   upload the bytes; now cache-distributed to every device per cache fill
//!    └─ Remote → Local   bring the bytes back to a local file — path required iff user-provided
//!
//! cache budget   per-NAMESPACE size limit; each namespace evicts independently, so
//!                evicting release_files (big) never touches covers (small reserved slice)
//! pin            keep one specific Remote blob's cache copy from eviction (e.g. a
//!                release the user pinned for offline)
//! ```
//!
//! ## The cache vs local files
//!
//! The cache holds local copies of **Remote** blobs (filled per `cache fill`,
//! evicted per budget unless pinned). It is **segmented by namespace**: each
//! namespace has its own configurable cache budget and evicts independently, so
//! evicting `release_files` (big) never touches `covers` (a small reserved slice). A
//! `CacheEager` cover that falls out of its namespace budget shows a placeholder
//! until the next read re-fetches it — covers are not pinned. A **Local** blob is not
//! in the cache: a user-provided Local blob is the user's file at its path (an
//! external ref); a host-provided Local blob is in coven's local store (see
//! [`local_files`]). The cache is the mechanism for *remoteness* — so
//! `CacheEager`/`CacheLazy`/pin/budget describe a blob only while it is Remote, never
//! while it is Local.
//!
//! # The engine's halves
//!
//! This module is the engine; its halves move a blob through its lifecycle:
//!
//! - [`cache`] — the device-local cache for **Remote** blobs: bytes on disk keyed
//!   by exact locator hash, with the folder a file lives in as the only retention truth
//!   (`storage/pinned/` protected, `storage/cache/` evictable). Reads — one-shot
//!   whole, which checks the plaintext against the row's hash because it reads
//!   every byte anyway, or an opened stream whose ranges each cost their own
//!   bytes: a positioned read of a local file, or the sealed chunks covering the
//!   range fetched from the cloud object and opened — plus pin/unpin, clear, and
//!   budget eviction.
//! - [`local_files`] — coven's own copy of a **host-provided Local** blob, in the
//!   local store (`storage/local/<namespace>/<id>`). Never evicted; the budget
//!   sweep never walks it. Store, read, drop.
//! - [`upload`] — the cloud-write half: drain the durable upload queue, sealing
//!   each blob under its scope and writing it to the cloud with coalesced progress,
//!   so a local-only blob becomes uploaded. The sync cycle calls the drain
//!   each round before it pushes.
//! - [`delete`] — the cloud-delete half: turn a queued deletion into a signed
//!   cloud tombstone, hold the blob for a convergence grace so a lagging peer
//!   isn't stranded, then GC the blob once the grace has passed. The sync cycle
//!   drains tombstones and runs the GC each round after it pulls.
//!
//! The types below ([`BlobRef`], [`BlobScope`], [`Provenance`],
//! [`CacheFill`], [`BlobTransitionObserver`]) are the vocabulary both halves and
//! the host speak. Which rows carry blobs is not a runtime callback but a per-table
//! declaration ([`crate::sync::session::BlobDecl`]) coven resolves into a
//! [`decl::BlobDecls`] each cycle to derive the blob set itself.
//!
//! coven also owns the two locality transitions ([`transition`]): `make_remote`
//! (Local → Remote: upload the bytes, then flip the gate) and `make_local`
//! (Remote → Local: bring each blob back to a local file, then retract). The
//! make-Remote *completion* — flipping the gate the instant the last user-provided
//! upload lands — lives in the [`upload`] drain, the one place that knows an upload
//! just succeeded.

pub(crate) mod cache;
#[cfg(test)]
mod cache_tests;
pub(crate) mod decl;
pub(crate) mod delete;
#[doc(hidden)]
pub(crate) mod local_cleanup;
pub(crate) mod local_files;
pub(crate) mod locator;
mod retry;
#[cfg(test)]
mod row_ref_tests;
pub(crate) mod transition;
pub(crate) mod upload;

pub use cache::{BlobCacheError, BlobStream};
#[cfg(test)]
pub(crate) use delete::BLOB_TOMBSTONE_GRACE;
pub use transition::{MakeLocalError, MakeRemoteError};

use sha2::{Digest, Sha256};

/// The content hash a blob-bearing row carries: the lowercase-hex SHA-256 of the
/// blob's plaintext bytes, computed at import and stored in the row's blob columns
/// alongside the declared size. The row is carried in a signed changeset (and in a
/// signed snapshot), so this hash is signed by the row's author — that is what
/// makes it authoritative: on download coven hashes the decrypted plaintext and
/// requires equality with the row's hash, so the bytes are pinned by the author,
/// not by the cloud key they happened to arrive under. A host computes this over a
/// blob's plaintext at import and writes it into the row's declared hash column,
/// the same way it writes the plaintext length into the size column.
pub fn content_hash(plaintext: &[u8]) -> String {
    hex::encode(Sha256::digest(plaintext))
}

/// An incremental SHA-256 over a blob's plaintext, so the streaming download path
/// verifies a blob's content hash without holding the whole plaintext in memory:
/// feed each decrypted chunk to [`update`](Self::update), call
/// [`finish`](Self::finish), and compare the returned digest with the row's hash
/// before committing the bytes to the cache. The hex-encoded digest matches
/// [`content_hash`] over the same bytes.
pub struct ContentHasher(Sha256);

impl ContentHasher {
    pub fn new() -> Self {
        ContentHasher(Sha256::new())
    }

    /// Fold the next plaintext chunk into the running digest.
    pub fn update(&mut self, chunk: &[u8]) {
        self.0.update(chunk);
    }

    /// The lowercase-hex digest of everything fed so far.
    pub fn finish(self) -> String {
        hex::encode(self.0.finalize())
    }
}

impl Default for ContentHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// How many blob transfers coven runs at once in each of its two transfer loops:
/// the upload drain ([`upload::drain_uploads`]) and the pin/download loop. An
/// open-time blob-engine tunable the host sets on the builder,
/// carried on [`Database`](crate::database::Database) alongside the other open-time
/// blob config and read back by each loop, which holds `&Database`.
///
/// Each bound is a [`NonZeroUsize`], so a zero — which would leave a loop admitting
/// nothing and never completing — is unrepresentable rather than clamped or rejected
/// at open. `one_at_a_time()` (both `1`) is the default: transfers run one at a
/// time in queue order.
///
/// [`NonZeroUsize`]: std::num::NonZeroUsize
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransferLimits {
    /// Maximum concurrent blob uploads in one upload-drain pass.
    pub uploads: std::num::NonZeroUsize,
    /// Maximum concurrent blob downloads (fetches) in one pin call.
    pub downloads: std::num::NonZeroUsize,
}

impl TransferLimits {
    /// One at a time in each loop.
    pub(crate) fn one_at_a_time() -> Self {
        Self {
            uploads: std::num::NonZeroUsize::MIN,
            downloads: std::num::NonZeroUsize::MIN,
        }
    }
}

// The cache's own tests: real `Database` + `TestStore` over a temp store
// dir, asserting hits/misses, the pinned/cache folder split, and pin/unpin/clear.
// These drive a real temp directory on the filesystem. See [`cache`].
// The upload drain's tests: real `Database` (the `cloud_outbox` queue) driven
// against `InMemoryCloudHome`/`FailingCloudHome`, asserting record-and-continue,
// per-entry backoff, scope-resolved sealing, and the observer callbacks. See
// [`upload`].
#[cfg(test)]
mod upload_tests;
// The coven-owned make-Remote / make-Local transition tests: multi-device
// make_remote + make_local through the real cycle, cancel both directions, the
// drain's completion flip, durable cancellation, crash-idempotency at each commit
// boundary, and a round-trip. Uses a `watch` cancel signal + `run_single_sync_cycle`,
// See [`transition`].
#[cfg(test)]
mod transition_tests;
// The local-files store's tests: store/read round-trip, a host-provided Local blob
// surviving a budget sweep (the sweep never walks `local/`), and drop. These
// drive a real temp directory. See [`local_files`].
#[cfg(test)]
mod local_files_tests;
// The delete half's tests: tombstone signing, the drain that writes tombstones,
// the graced GC that reclaims exact immutable objects, and the delete-outbox row
// shape. Driven against `InMemoryCloudHome` and
// `TestStore`. See [`delete`].
#[cfg(test)]
mod delete_tests;

/// Which key encrypts a blob, as a host names it on a [`BlobRef`].
///
/// The host names *what* a blob is scoped to — the whole store or a derived
/// per-scope key — never the raw key bytes. Storage and encryption consume this
/// same type; there is no key material in it to leak.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BlobScope {
    /// The store master key — every member reads it.
    Master,
    /// A per-scope key derived from the master key (e.g. one key per item).
    Derived(String),
}

/// A blob's **Local story**: where its bytes live while the blob is Local, and
/// whether bringing it back from Remote needs a destination path. Orthogonal to
/// [`CacheFill`] (the Remote story) — a blob declares both.
///
/// The cache never enters into this: a Local blob is not a cache copy. Provenance
/// decides which of the two Local homes holds it, and what `make_local` does to
/// restore it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Provenance {
    /// The user's own file at a path; coven references it but does not own it
    /// (tracked as an external ref — see `local_blob_refs`). `make_local` writes
    /// the bytes back to a user file, so it **needs a destination path**.
    UserProvided,
    /// The host hands coven the data; coven keeps its own copy in the local store
    /// (`storage/local/<namespace>/<id>`, see [`local_files`]). `make_local`
    /// restores it to the local store, so it needs **no path**.
    HostProvided,
}

/// A blob's **Remote story**: how a device gets the bytes once the blob is Remote.
/// A cache-mechanism setting — it describes a blob only while Remote — that applies
/// to ANY blob regardless of [`Provenance`]. Orthogonal to provenance; a blob
/// declares both.
///
/// Both classes are declared per blob and are global (every device reads the same
/// class from the blob's [`BlobRef`]); the difference is what a device does with
/// the blob on pull. The distinction has to be a declared property and not a
/// per-device choice: device B, deciding during its own pull whether to fetch a
/// blob, can only read the blob's declared class — it cannot see what device A
/// chose locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CacheFill {
    /// Fetched into the cache on pull, right away, on every device — part of
    /// "having the store" (e.g. cover art, so the grid renders from local bytes
    /// without a fetch). The cache copy is evictable + re-fetchable, not pinned.
    CacheEager,
    /// Not fetched on pull: a pulling device skips it and fetches it into the cache
    /// on first read — e.g. audio, which is big and streams on demand.
    CacheLazy,
}

/// A blob's **replacement story**: whether the row carrying it may ever be repointed at
/// a different blob. Orthogonal to [`Provenance`] and [`CacheFill`]; a blob declares all
/// three.
///
/// It exists because a cloud object must never be rewritten with different bytes. The
/// pull verifies an object against its row's content hash and a position advances only over
/// a fully-realized changeset, so a key whose content can change leaves a device that
/// pulls an older changeset unable to satisfy it — wedged there for good, not merely
/// missing a blob. Two declarations reach that guarantee by different routes, and coven
/// enforces whichever one the blob declares:
///
/// - [`Replaceable`](Self::Replaceable) — the row may be repointed, so the *key* must
///   move with the blob: a readable `cloud_path` has to name its blob
///   (`cloud_path_names_blob`), and a replacement then writes a new
///   object beside the one it replaces instead of over it.
/// - [`WriteOnce`](Self::WriteOnce) — the row is never repointed, so the object at its
///   key is written once and there is nothing to protect it from. Its path is free to be
///   a stable, fully readable name. coven refuses the repointing.
///
/// `Replaceable` is the default: its guarantee is the airtight one (the key itself
/// carries the blob id, so no path can ever be reused), while `WriteOnce` is a weaker
/// contract a consumer opts into knowingly — see its docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobReplacement {
    /// The row may be repointed at a new blob id — replacing a cover, swapping an
    /// attachment. Requires a readable `cloud_path` that names its blob, so that the
    /// replacement's fresh blob id yields a fresh key.
    Replaceable,
    /// The row is never repointed: the blob it names when it is inserted is the blob it
    /// names for life. Repointing one is refused.
    ///
    /// This buys a stable, fully readable cloud path — `Live at Leeds/01 Sonata.flac`
    /// rather than `01 Sonata-0ef7a1c9.flac` — for content that is written once and never
    /// rewritten: an imported file, whose bytes are what they are.
    ///
    /// **What coven enforces, and what it does not.** coven refuses to repoint the row,
    /// which is the reuse it can see. It cannot see a consumer *deleting* a row and
    /// inserting a different blob at the same `cloud_path` — the deleted row is gone, and
    /// coven keeps no history of the paths it has used. Declaring `WriteOnce` is therefore
    /// also a promise that the path is never reused by a different blob. Derive it from
    /// data that never repeats and it holds by construction: a path carrying a freshly
    /// minted id for the thing being imported can never be handed out twice.
    WriteOnce,
}

/// A blob a row references: its cloud identity, encryption scope, and the two
/// declared properties ([`provenance`](BlobRef::provenance) +
/// [`fill`](BlobRef::fill)). coven derives it from the row's declared columns
/// ([`crate::sync::session::BlobDecl`]) via [`decl::BlobDecls`]. Where its bytes
/// live depends on its locality and provenance: a user-provided Local blob is the
/// user's file at its path; a host-provided Local blob is in coven's local store
/// (`storage/local/<namespace>/<id>`); a Remote blob's device-local copy is a cache
/// copy (`storage/pinned/<namespace>/<locator-hash>` /
/// `storage/cache/<namespace>/<locator-hash>`, built
/// from the validated namespace + exact locator hash — see [`cache`]).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlobRef {
    /// Cloud namespace, e.g. `"images"`. Becomes `{namespace}/{ab}/{cd}/{id}`
    /// under the hashed scheme, or `{namespace}/{cloud_path}` under the plain one.
    pub namespace: String,
    /// Blob id (typically the id of the blob-bearing row).
    pub id: String,
    /// Encryption scope for this blob.
    pub scope: BlobScope,
    /// The consumer's readable cloud-relative path for this blob, e.g.
    /// `"Artist - Album/cover.jpg"`. Used as the object key under `namespace` when
    /// the home's [`crate::storage::BlobPathScheme`] is `Plain`;
    /// ignored when `Hashed`. `None` is only valid for a `Hashed` home — a `Plain`
    /// home with no `cloud_path` is a surfaced error, never a silent fallback.
    pub cloud_path: Option<String>,
    /// The blob's **Local story**: where its bytes live while Local, and whether
    /// `make_local` needs a destination path. See [`Provenance`].
    pub provenance: Provenance,
    /// The blob's **Remote story**: whether a pulling device fetches it into the
    /// cache right away ([`CacheFill::CacheEager`]) or on first read
    /// ([`CacheFill::CacheLazy`]). See [`CacheFill`].
    pub fill: CacheFill,
}

/// One exact blob-bearing row version. A reference becomes stale when the live
/// row stamp or any declared blob value changes.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RowBlobRef {
    table: String,
    row_id: String,
    row_stamp: String,
    column: String,
    blob: BlobRef,
    plaintext_size: u64,
    plaintext_hash: crate::protocol::store_commit::ObjectHash,
    authority: RowBlobAuthority,
    stored: Option<locator::StoredBlobRef>,
}

/// The authority state that determines where one row version's blob lives.
/// A remote-audience blob remains `PendingRemote` while its verified plaintext
/// is local and no cloud object has been created; `Remote` carries the exact
/// package authority needed to open its committed object.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RowBlobAuthority {
    Local,
    PendingRemote(locator::RemoteAudience),
    Remote(crate::protocol::audience_package::PackageAudience),
}

impl<'de> serde::Deserialize<'de> for RowBlobRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            table: String,
            row_id: String,
            row_stamp: String,
            column: String,
            blob: BlobRef,
            plaintext_size: u64,
            plaintext_hash: crate::protocol::store_commit::ObjectHash,
            authority: RowBlobAuthority,
            stored: Option<locator::StoredBlobRef>,
        }

        let fields = Fields::deserialize(deserializer)?;
        Self::new(
            fields.table,
            fields.row_id,
            fields.row_stamp,
            fields.column,
            fields.blob,
            fields.plaintext_size,
            fields.plaintext_hash,
            fields.authority,
            fields.stored,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl RowBlobAuthority {
    pub fn audience(&self) -> crate::protocol::circle::Audience {
        match self {
            Self::Local => crate::protocol::circle::Audience::Local,
            Self::PendingRemote(locator::RemoteAudience::Store) => {
                crate::protocol::circle::Audience::Store
            }
            Self::PendingRemote(locator::RemoteAudience::Circle(circle_id)) => {
                crate::protocol::circle::Audience::Circle(*circle_id)
            }
            Self::Remote(crate::protocol::audience_package::PackageAudience::Store) => {
                crate::protocol::circle::Audience::Store
            }
            Self::Remote(crate::protocol::audience_package::PackageAudience::Circle {
                circle_id,
                ..
            }) => crate::protocol::circle::Audience::Circle(*circle_id),
        }
    }
}

/// Whether `locator` describes exactly the row version it was minted for.
///
/// A stored blob's locator carries the namespace, id, plaintext size and hash,
/// and encryption scope of the row it was sealed from. Anything that hands a
/// `StoredBlobRef` and a row to each other checks this before trusting the pair.
///
/// [`RowBlobRef::new`] enforces the same facts and more, one field at a time so
/// it can name which one diverged; this is the yes-or-no form for callers that
/// answer a mismatch their own way.
pub(crate) fn locator_describes_row(
    locator: &locator::BlobLocator,
    blob: &BlobRef,
    plaintext_size: u64,
    plaintext_hash: crate::protocol::store_commit::ObjectHash,
) -> bool {
    locator.namespace() == blob.namespace
        && locator.blob_id() == blob.id
        && locator.plaintext_size() == plaintext_size
        && locator.plaintext_hash() == plaintext_hash
        && locator.scope().is_none_or(|scope| scope == &blob.scope)
}

impl RowBlobRef {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        table: String,
        row_id: String,
        row_stamp: String,
        column: String,
        blob: BlobRef,
        plaintext_size: u64,
        plaintext_hash: crate::protocol::store_commit::ObjectHash,
        authority: RowBlobAuthority,
        stored: Option<locator::StoredBlobRef>,
    ) -> Result<Self, String> {
        let remote = match &authority {
            RowBlobAuthority::Local => None,
            RowBlobAuthority::PendingRemote(audience) => Some(audience.clone()),
            RowBlobAuthority::Remote(package) => Some(package.remote_audience()),
        };
        match (&authority, remote.as_ref(), stored.as_ref()) {
            (RowBlobAuthority::Local, None, None)
            | (RowBlobAuthority::PendingRemote(_), Some(_), None) => {}
            (RowBlobAuthority::Remote(_), Some(expected), Some(stored))
                if &stored.locator().audience() == expected => {}
            (RowBlobAuthority::Local, None, Some(_)) => {
                return Err("Local row blob carries a remote locator".to_string());
            }
            (RowBlobAuthority::PendingRemote(_), Some(_), Some(_)) => {
                return Err("pending remote row blob carries a cloud locator".to_string());
            }
            (RowBlobAuthority::Remote(_), Some(_), None) => {
                return Err("remote row blob has no exact locator".to_string());
            }
            (RowBlobAuthority::Remote(_), Some(expected), Some(stored)) => {
                return Err(format!(
                    "row audience {expected:?} differs from locator audience {:?}",
                    stored.locator().audience()
                ));
            }
            _ => unreachable!("authority determines whether a remote audience exists"),
        }
        if let Some(stored) = &stored {
            let locator = stored.locator();
            if locator.namespace() != blob.namespace {
                return Err(format!(
                    "row blob namespace {:?} differs from locator namespace {:?}",
                    blob.namespace,
                    locator.namespace()
                ));
            }
            if locator.blob_id() != blob.id {
                return Err(format!(
                    "row blob id {:?} differs from locator id {:?}",
                    blob.id,
                    locator.blob_id()
                ));
            }
            if locator.plaintext_size() != plaintext_size
                || locator.plaintext_hash() != plaintext_hash
            {
                return Err(
                    "row blob plaintext size or hash differs from its exact locator".to_string(),
                );
            }
            match locator {
                locator::BlobLocator::Opaque {
                    scope,
                    key_fingerprint,
                    ..
                } => {
                    if scope != &blob.scope {
                        return Err(
                            "row blob encryption scope differs from its exact locator".to_string()
                        );
                    }
                    if let RowBlobAuthority::Remote(
                        crate::protocol::audience_package::PackageAudience::Circle {
                            key_fingerprint: expected,
                            ..
                        },
                    ) = &authority
                    {
                        if key_fingerprint != expected {
                            return Err(
                                "row blob Circle key differs from its exact locator".to_string()
                            );
                        }
                    }
                }
                locator::BlobLocator::Browsable { cloud_path, .. } => {
                    if blob.cloud_path.as_deref() != Some(cloud_path) {
                        return Err(
                            "row blob cloud path differs from its exact locator".to_string()
                        );
                    }
                }
            }
        }
        Ok(Self {
            table,
            row_id,
            row_stamp,
            column,
            blob,
            plaintext_size,
            plaintext_hash,
            authority,
            stored,
        })
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn row_id(&self) -> &str {
        &self.row_id
    }

    pub fn row_stamp(&self) -> &str {
        &self.row_stamp
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn blob(&self) -> &BlobRef {
        &self.blob
    }

    pub fn plaintext_size(&self) -> u64 {
        self.plaintext_size
    }

    pub fn plaintext_hash(&self) -> crate::protocol::store_commit::ObjectHash {
        self.plaintext_hash
    }

    pub fn authority(&self) -> &RowBlobAuthority {
        &self.authority
    }

    pub fn audience(&self) -> crate::protocol::circle::Audience {
        self.authority.audience()
    }

    pub fn stored(&self) -> Option<&locator::StoredBlobRef> {
        self.stored.as_ref()
    }
}

/// Notified about coven's blob transitions, for host-specific bookkeeping and UI:
/// per-blob upload progress while a make_remote uploads, per-blob materialize
/// progress while a make_local copies files back, and a completion hook per
/// direction the host turns into its own UI event.
///
/// The host no longer drives the transition — coven owns flipping the gate and
/// deciding when a cycle publishes — so this observer only *reports*. The upload
/// callbacks fire as the drain works: `on_blob_upload_started` before each
/// attempt, `on_blob_upload_progress` zero or more times as encrypted bytes reach
/// the cloud (backends that can't report sub-file progress call it once at the end
/// with `bytes_done == bytes_total`), `on_blob_uploaded` on success (notification
/// only — coven, not the host, flips the gate and breaks the drain to publish),
/// and `on_blob_upload_failed` when an attempt fails and its entry stays queued.
///
/// `on_root_made_remote` / `on_root_made_local` fire whenever coven *completes* a
/// transition — including one resumed after a restart — so the host's own
/// row-updated event survives a restart rather than being lost with an in-memory
/// flag. `on_blob_materialize_progress` moves a make_local's per-file progress bar.
///
/// `should_skip_uploads` lets the host pause the upload pipeline without touching
/// the queue: the sync cycle consults it before draining so a paused queue still
/// accepts new entries but doesn't drain ([`upload::drain_uploads`] checks once at
/// the top of each entry; in-flight uploads complete normally).
///
#[async_trait::async_trait]
pub trait BlobTransitionObserver: Send + Sync {
    /// An upload attempt for this blob is starting now.
    async fn on_blob_upload_started(&self, blob_id: &str);

    /// `bytes_done` of `bytes_total` encrypted bytes have reached the cloud for
    /// this in-flight blob. `bytes_done` is cumulative and monotonic within one
    /// upload attempt. The default is a no-op so observers that don't surface
    /// sub-file progress don't need a stub.
    async fn on_blob_upload_progress(&self, blob_id: &str, bytes_done: u64, bytes_total: u64) {
        let _ = (blob_id, bytes_done, bytes_total);
    }

    /// The blob was uploaded to the cloud successfully — notification only. coven
    /// owns flipping the gate and breaking the drain to publish a completed
    /// make_remote.
    async fn on_blob_uploaded(&self, blob_id: &str);

    /// An upload attempt failed; the entry remains queued for retry.
    async fn on_blob_upload_failed(&self, blob_id: &str, error: &str);

    /// If true, the sync cycle skips the upload drain this round and
    /// [`upload::drain_uploads`] short-circuits before pulling the next queued
    /// entry. The default is `false` so existing implementations don't need a stub.
    fn should_skip_uploads(&self) -> bool {
        false
    }

    /// coven completed a make_remote of `(root_table, root_id)`: every blob is
    /// uploaded and the gate is flipped true (the subtree publishes this cycle).
    /// Fires for a restart-resumed completion too, so the host's row-updated event
    /// is not lost to a crash. The default is a no-op.
    async fn on_root_made_remote(&self, root_table: &str, root_id: &str) {
        let _ = (root_table, root_id);
    }

    /// coven completed a make_local of `(root_table, root_id)`: every blob is back
    /// to a local file (a user file for user-provided, the local store for
    /// host-provided), the gate is flipped false (the subtree retracts from peers),
    /// and the cloud blobs are queued for tombstoning. The default is a no-op.
    async fn on_root_made_local(&self, root_table: &str, root_id: &str) {
        let _ = (root_table, root_id);
    }

    /// `done` of `total` of a make_local's blobs have been materialized back to a
    /// local file, so the host can move a per-file progress bar. The default is a
    /// no-op.
    async fn on_blob_materialize_progress(
        &self,
        root_table: &str,
        root_id: &str,
        blob_id: &str,
        done: u64,
        total: u64,
    ) {
        let _ = (root_table, root_id, blob_id, done, total);
    }
}
