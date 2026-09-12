# Storage

coven syncs over a [`CloudHome`](rustdoc:trait:coven::CloudHome):
a trait that moves object bytes between a device and storage the user
already controls. coven owns encryption, the key layout, ordering, and retry.
A provider handles bytes without interpreting application rows or assigning
protocol sequence numbers. Coven signs the publication order; the provider
enforces conditional replacement of its current record.

The provider boundary combines two traits: `CloudHome` supplies flat-key byte
operations, while
[`ExactSlotStorage`](rustdoc:trait:coven::ExactSlotStorage)
allocates a provider-specific location before publication and creates it once.
[`ExactCloudHome`](rustdoc:trait:coven::ExactCloudHome) requires both traits on
the same provider. A repeated create of the same exact
object reports `AlreadyPresent`; different bytes report `SlotCollision` and
never replace the first object.

`ExactSlotStorage` also reads a mutable record with its provider revision and
replaces it only if that revision still matches. Store publication uses this
operation on one root-bound current record. Immutable candidates carry the
content; the successful conditional replacement accepts a publication. A
changed revision reports `VersionChanged`, so the publisher verifies the
competing accepted history before retrying.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

<svg class="flow" viewBox="0 0 660 210" role="img" aria-label="Sync concepts pass through the sealing layer to the raw byte trait and then to a provider">
<rect class="lane" x="80" y="16" width="500" height="40" rx="9"/>
<text class="lbl s11" x="330" y="34" text-anchor="middle">sync concepts</text>
<text class="sub" x="330" y="48" text-anchor="middle">a device's changeset seq · a blob id · a membership entry</text>
<line class="arr" x1="330" y1="60" x2="330" y2="74" marker-end="url(#fa)"/>
<rect class="chipo" x="80" y="78" width="500" height="40" rx="9"/>
<text class="lbl s11" x="330" y="96" text-anchor="middle">CloudSyncConnection</text>
<text class="sub" x="330" y="110" text-anchor="middle">seals and opens · maps concepts to flat keys</text>
<line class="arr" x1="330" y1="122" x2="330" y2="136" marker-end="url(#fa)"/>
<rect class="chipo" x="80" y="140" width="500" height="40" rx="9"/>
<text class="lbl s11" x="330" y="158" text-anchor="middle">ExactCloudHome</text>
<text class="sub" x="330" y="172" text-anchor="middle">bytes · immutable slots · conditional record replacement · access</text>
<line class="arr" x1="330" y1="184" x2="330" y2="196" marker-end="url(#fa)"/>
<text class="sub" x="330" y="208" text-anchor="middle">S3 · Dropbox · OneDrive · CloudKit</text>
</svg>

Examples use the todos app. Its changesets, snapshots, attachment
blobs, and membership records all land in one cloud home under keys like
`store-v1/candidates/<family>/commits/<device>/42/<hash>.json`.

## What the host configures

The host selects one provider at a time and fills its settings in
[`CloudHomeConfig`](rustdoc:struct:coven::CloudHomeConfig), held on
[`Config`](rustdoc:struct:coven::Config). coven reads the current config
fresh on each operation rather than caching its own copy, so a provider swap or
disconnect takes effect on the next sync cycle without rebuilding the sync layer.

With no provider selected there is no sync layer at all; the store is
local-only and complete. At connect time coven builds the cloud home: it reads
the selected provider's settings from config and its credentials from the OS
keyring; a missing setting or credential fails with a `Storage` error naming
the field ("S3 bucket not configured", "Google Drive OAuth token not in
keyring").

## The trait

Encryption, protocol ordering, verification, and retry live above the provider
implementations. A backend supplies bytes by key plus the provider-shaped
operations no wrapper can manufacture: multipart uploads, create-once exact
slots, conditional replacement, and account access. These excerpts show the
publication boundary; the linked trait definitions include the complete API.

```rust
pub trait CloudHome: Send + Sync {
    async fn probe(&self) -> Result<(), CloudHomeError> { /* default: no-op list */ }

    // Uploads: one bounded request, or a streaming multipart session.
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError>;
    async fn open_multipart<'a>(&'a self, key: &str, total_len: u64)
        -> Result<BoxPartSink<'a>, CloudHomeError>;
    fn multipart_threshold(&self) -> u64;

    // Provided: picks put_object vs multipart and pumps the parts.
    async fn write(&self, key: &str, body: BlobBody, progress: &UploadProgress)
        -> Result<(), CloudHomeError> { /* central driver */ }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError>;
    async fn read_range(&self, key: &str, start: u64, end: u64)
        -> Result<Vec<u8>, CloudHomeError>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError>;
    async fn delete(&self, key: &str) -> Result<(), CloudHomeError>;
    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError>;

    async fn set_access(&self, desired: CloudAccessState)
        -> Result<CloudAccessOutcome, CloudHomeError>;
}

pub trait ExactSlotStorage: Send + Sync {
    async fn provider_binding(&self) -> Result<ResolvedProviderBinding, CloudHomeError>;
    async fn cross_principal_evidence(&self)
        -> Result<CrossPrincipalProviderEvidence, CloudHomeError>;
    async fn allocate_slot(&self, logical_key: &str)
        -> Result<ObjectSlot, CloudHomeError>;
    async fn list_slots(&self, prefix: &str) -> Result<Vec<ObjectSlot>, CloudHomeError>;
    async fn create_at(&self, upload: &ExactUpload<'_>, control: &UploadControl)
        -> Result<ExactCreateOutcome, CloudHomeError>;
    async fn create_versioned_at(&self, upload: &ExactUpload<'_>, control: &UploadControl)
        -> Result<ExactCreateOutcome, CloudHomeError>;
    async fn read_versioned_at(&self, slot: &ObjectSlot)
        -> Result<CloudVersionedObject, CloudHomeError>;
    async fn replace_at_if_version(
        &self, slot: &ObjectSlot, expected: &CloudObjectVersion, bytes: Vec<u8>,
    ) -> Result<ConditionalWriteOutcome, CloudHomeError>;
    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError>;
    async fn read_range_at(&self, slot: &ObjectSlot, start: u64, end: u64)
        -> Result<Vec<u8>, CloudHomeError>;
    async fn open_stream_at(&self, slot: &ObjectSlot)
        -> Result<CloudObjectStream, CloudHomeError>;
    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError>;
}

pub trait ExactCloudHome: CloudHome + ExactSlotStorage {}
```

- `ExactCloudHome` supplies raw operations and exact storage through one
  provider value. There is no optional second provider object to obtain after
  opening the home.
- `create_at` preserves immutable object identity. `read_versioned_at` returns
  bytes and the revision that `replace_at_if_version` must present for that
  exact slot. A revision mismatch is a competing writer; an unavailable or
  ambiguous result remains an error. The current record is not deleted during
  Store publication.
- `open_stream_at` serves one exact object's stored bytes as the provider
  delivers them, and nothing is written to disk on the way. The stream ends
  only when the whole object has arrived; a provider that stops part-way ends
  it with that error instead, which is what lets the reader above tell a
  complete body from a truncated one while it authenticates each chunk.
- `probe` checks that the backend is reachable with the configured credentials.
  Setup flows call it before persisting credentials, so a typo or a missing
  bucket fails at setup instead of via a delayed reconnect banner. The default
  implementation lists a sentinel prefix; backends override it with a cheaper
  check. S3 uses `HeadBucket`, then creates a probe object twice to prove
  create-only behavior and runs the configured integrity check. Upload-checksum
  mode also sends a deliberately wrong SHA-256 and requires the endpoint to
  reject it.
- A provider implements two raw upload pieces: `put_object`, one bounded
  single-request upload, and `open_multipart`, a streaming session that accepts
  ordered parts. `multipart_threshold` is the cut between them. The provided
  `write` method is the one coven calls: a central driver that picks the path
  by size and pumps a sized `BlobBody` through it, reporting cumulative bytes
  to the `progress` callback for the per-file bar. Small control files (auth
  keys, head pointers, the snapshot) pass
  `no_progress`, which
  discards the reports.
- `read` returns the whole value. `read_range` returns a half-open byte range
  (`start` inclusive, `end` exclusive), which is how coven fetches only the
  encrypted chunks covering a blob byte range.
- `list` returns every key under a prefix. `delete` is not an error when the key
  is absent. `exists` is a presence check.
- `set_access` sets whether one member principal can reach the cloud home. The
  command carries the absolute desired state, verifies provider readback, and
  is safe to retry after an unknown outcome. It is provider-shaped and
  described below.

## Granting and revoking access

Membership is cryptographic, but a new member still has to *reach* the bytes:
the storage itself must admit them. That is inherently provider-shaped (a
folder share, a credential, a share URL), so it lives on the trait.

`set_access(CloudAccessState::Present { ... })` carries the member's public key
plus the provider account email for backends that share by account, and returns
a
[`CloudHomeJoinInfo`](rustdoc:enum:coven::CloudHomeJoinInfo), one
variant per provider, carrying exactly what another device needs to reach the
same cloud home:

- The consumer clouds (Drive, Dropbox, OneDrive) share the store folder with
  the member's provider account and return its folder or drive id.
  setting access to `Absent` unshares it and reports `RevokeOutcome::Revoked`.
- S3 returns the bucket, region, endpoint, access key, secret key, and optional
  key prefix: access rides pre-shared credentials. One member's copy of a
  shared key cannot be withdrawn alone, so setting access to `Absent` reports
  `RevokeOutcome::Unsupported` and removal proceeds anyway: the
  [key rotation](/docs/sharing#revocation-is-key-rotation) that removal
  performs, not credential withdrawal, is what protects post-removal content.
  Cutting the removed member's residual *write* access means rotating the
  bucket credentials, which is the user's call.
- CloudKit returns a share URL.

`set_access(CloudAccessState::Absent { ... })` withdraws access where the
provider supports per-member revocation and reports a `RevokeOutcome`.
Because access updates work with folder shares and share URLs,
not encrypted payloads, they live below the encryption layer and are called
directly on the `CloudHome`, not through the wrapper described under
[Where encryption sits](#where-encryption-sits).

## Errors

`AccessDenied` means nothing to the person looking at a sync banner, and the
host can't translate it either; it doesn't know S3 from Dropbox. So every
failure crosses this boundary as a sentence a UI can show verbatim.

```rust
// Selected error variants; the enum also preserves typed backend,
// local-filesystem, blob-source, and protocol failures.
pub enum CloudHomeError {
    NotFound(String),
    AlreadyExists(String),
    SlotCollision(String),
    Configuration(String),
    Transport(String),
    CleanupFailed {
        operation: Box<CloudHomeError>,
        cleanup: Box<CloudHomeError>,
    },
    UnresolvedOutcome {
        operation: Box<CloudHomeError>,
        settlement: Box<CloudHomeError>,
    },
    Io(#[from] std::io::Error),
}
```

- `NotFound(key)`: the key is not there. coven uses it for the expected misses
  (no snapshot yet, a blob not uploaded yet), so a host that maps it to a UI
  state matches the variant directly.
- `AlreadyExists(key)`: a provider's create-only request reported an occupied
  destination. Exact-slot adapters settle that response internally and return
  `ExactCreateOutcome::AlreadyPresent` only when the stored size and hash match.
- `SlotCollision(key)`: an exact slot contains bytes other than the object the
  caller named.
- `Configuration(msg)`: missing or invalid settings, credentials, OAuth
  authorization, or provider capability. Retrying the same request cannot
  succeed until configuration changes.
- `Transport(msg)`: a backend, network, response, or service failure that may
  succeed when the initiating operation retries.
- `CleanupFailed`: the primary operation and its required cleanup both failed;
  neither cause is discarded.
- `UnresolvedOutcome`: a create lost its response and the configured metadata,
  checksum, or readback check needed to determine the result also failed.
- `Io`: a local filesystem or I/O failure surfaced from `std::io::Error`.

Each driver classifies the failures a user can act on. For example, S3
`AccessDenied` becomes "Your S3 credentials don't have permission to write to
this bucket. Check the access policy in sync settings."; `NoSuchBucket` becomes
"The S3 bucket no longer exists. Check the bucket name in sync settings."; and
Backblaze or MinIO `OverQuota`/`QuotaExceeded` becomes a quota message (AWS
rarely returns these). The consumer clouds do the same for their full-storage
codes (`storageQuotaExceeded`, `path/insufficient_space`, `quotaLimitReached`).
Every other service error keeps its raw code and message so it stays debuggable
in logs.

Above the raw `CloudHome` boundary, blob reads retain three distinct causes. A
provider or network failure is transport and sets the sync loop to `Offline`.
Plaintext that fails its signed content hash is `InvalidContent`; failure to
create, write, sync, or rename the local destination is `LocalFilesystem`.
Invalid content and local filesystem failures hold or fail the affected work
without reporting that storage is offline. Inline host-blob uploads, snapshot
uploads, row-gate `make_remote` uploads, and candidate blob downloads all keep
provider transport typed through this boundary.

## Providers

The storage crate contains five cloud adapters plus an in-memory home for tests.
Sync requires both exact immutable objects and provider-enforced conditional
replacement. The Google Drive adapter rejects the versioned-record operations
with a configuration error, so its byte-storage support does not make it a
usable Store publication home.

The host selects one
local `exact_upload_verification` policy: `upload_checksum`, `metadata_hash`,
`readback`, or `unchecked`. Invitations and restore codes do not carry that
choice. Upload-checksum enforcement is available on S3; Dropbox, Google Drive,
OneDrive, and CloudKit reject that policy during setup. Metadata mode uses the
provider's content identity: S3's `HeadObject` SHA-256, Drive's `md5Checksum`,
Dropbox's `content_hash`, OneDrive's `sha1Hash`, or the hash in CloudKit's
atomically committed manifest. Readback downloads and verifies the full body.
Unchecked mode trusts only an observed successful create response. It cannot
confirm that an occupied slot holds the same bytes or settle a lost response,
so those cases fail the initiating operation instead of being accepted from
presence alone.

- **S3** ([`S3CloudHome`](rustdoc:struct:coven::S3CloudHome))
  works against any S3-compatible endpoint (AWS, Backblaze B2, Wasabi, MinIO).
  Files at or below 8 MiB go up as a single `PutObject`; larger files use a
  multipart upload with 8 MiB parts, reporting progress per completed part and
  aborting the in-progress upload on failure so the bucket holds no orphaned
  parts. An optional key prefix is prepended to every key (trailing slashes
  normalized), so `changes/dev1/42.enc` can become
  `libs/abc/changes/dev1/42.enc`. Exact uploads send SHA-256 checksums for both
  bounded and multipart objects when checksum or metadata verification is
  selected.

- **Google Drive**
  (`GoogleDriveCloudHome`)
  implements immutable slots and raw storage, but refuses the conditional
  record operations required by Store publication. Its storage methods keep
  files flat in one folder. Drive filenames cannot carry the key's
  slashes, so each key is hex-encoded into a slash-free filename and decoded on
  list; the encoding is exact and reversible, never a lossy substitution. Large
  files use a resumable upload session in 8 MiB chunks (Drive requires 256
  KiB alignment). Exact-upload metadata verification compares Drive's
  `md5Checksum` and size; an ambiguous create response is settled through that
  metadata without downloading the body.

- **OneDrive**
  (`OneDriveCloudHome`)
  uses the same hex filename encoding and a Microsoft Graph resumable
  upload session in 7.5 MiB chunks (Graph requires 320 KiB alignment).
  Metadata verification compares the Graph `sha1Hash` and size.

- **Dropbox**
  (`DropboxCloudHome`)
  uses native Dropbox paths under the store folder (for example
  `/Apps/your-app/my-store/changes/dev1/42.enc`), so no filename encoding is
  needed. Metadata verification computes Dropbox's block-based `content_hash`
  locally and compares it with the provider's value and size. Sharing goes
  through `share_folder` to get a `shared_folder_id`.

- **CloudKit**
  (`CloudKitCloudHome`)
  stores files in the user's iCloud private database. A `CKAsset` caps at 50 MB,
  so a file larger than 10 MiB is split into 10 MiB part records and read back
  by reassembling those parts. Exact parts and their hash-bearing manifest are
  committed as one atomic record batch. The raw record operations are defined by the
  [`CloudKitOps`](rustdoc:trait:coven::CloudKitOps)
  trait and implemented in Swift through a UniFFI callback interface; coven
  cannot build this one from Rust alone and returns a `Storage` error directing
  you to construct it through your Swift layer.

- **In-memory**
  ([`InMemoryCloudHome`](rustdoc:struct:coven::InMemoryCloudHome),
  under the `test-utils` feature) is a `HashMap`-backed home that two simulated
  devices share through an `Arc` to round-trip changesets and blobs in unit
  tests. It exposes `keys()`, `get()`, `len()`, and `deletes_seen()` for
  after-the-fact assertions.

## OAuth sessions: refresh and retry

Drive, Dropbox, and OneDrive share their token lifecycle through an OAuth
session. Each backend owns one and routes its requests through it. Before a
request, the session checks the access token's expiry: if it expires within 60
seconds it refreshes first, persisting the new tokens to the keyring. After a
request, a `401` triggers one refresh and one retry.

The session also absorbs transient pressure: a `429` or any `5xx` retries up to
four times with exponential delay (500 ms doubling, capped at 32 seconds,
honoring a server-supplied `Retry-After`), so routine quota throttling degrades
a cycle to slow rather than failed, while a hard outage exhausts the attempts
in seconds and fails loud to the cycle's own minutes-long backoff.

When the refresh itself fails because the grant is gone (refresh token revoked,
expired, or the account password changed), the underlying
[`OAuthError::Reauthorize`](rustdoc:variant:coven::OAuthError::Reauthorize)
surfaces as a `Storage` message: "Your {provider} access was revoked or expired.
Reconnect to keep syncing." That is a user-facing message, not a transient
network error, so a host should wire it to a reconnect affordance rather than
retrying. A session missing its refresh token entirely produces the same kind of
reconnect message.

## Where encryption sits

Encryption stays out of the providers so that all five share one at-rest
implementation instead of five slightly different ones. `CloudHome` deals
only in raw bytes. The at-rest protection and the key layout
live one level up, in
`CloudSyncConnection`,
which wraps an `Arc<dyn ExactCloudHome>`: it seals on the way down, opens on the way up,
and owns the mapping from Store protocol objects, blob ids, and wrapped member
keys to the flat keys the trait stores. Both how it seals (the
[`CloudCipher`](rustdoc:enum:coven::CloudCipher)) and how it
keys blobs (the
`BlobPathScheme`) come
from the home's [storage mode](/docs/encryption#opaque-and-browsable-homes), one
choice set when the home is created:

- An **opaque** home (the default) encrypts objects whose protocol context
  selects the Store cipher, including Store packages. Blob paths use
  `{namespace}/opaque/{locator_hash}`.
- A **browsable** home keeps Store-cipher objects in plaintext. Each blob uses
  the consumer's readable path followed by an immutable version:
  [`{namespace}/readable/{cloud_path}/.coven-versions/{locator_hash}`](/docs/blobs#browsable-home-blob-paths).
  Anyone with bucket access can read that Store content by name.

Exact protocol slots and blob paths do not acquire a cipher-dependent suffix.

Object context determines protection independently of the home's blob naming.
Signed plaintext controls, including Store commits and publication entries,
remain readable so a device can verify authority before obtaining Store keys.
Recipient-sealed objects arrive already encrypted for their recipient. Circle
objects use their Circle key, rather than the Store cipher.

## Ranged reads

A host that streams a large opaque blob (audio playback, scrubbing) opens a
`BlobRangeReader`. The stored blob header declares independently authenticated
chunks. Opening the reader fetches that header; each plaintext range then
fetches and opens only the sealed chunks that cover it. The chunk tag binds the
header, blob identity, and chunk index, so a provider cannot substitute a chunk
from another blob or position. Browsable blobs have no authenticated chunk
format and therefore require the whole-object materialization path.

## Lifecycle

`handle.connect_sync(...)` builds the cloud home from the current config and
spawns the sync loop when a provider is configured. `handle.stop_sync()` drops
the loop, and `handle.start_sync()` starts it again. Because the config is read
fresh each operation, swapping providers is a config change followed by a
stop/start, with no app restart.
