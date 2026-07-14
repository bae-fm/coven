# Storage

coven syncs over a [`CloudHome`](rustdoc:trait:coven::storage::cloud::CloudHome):
a small trait that moves opaque encrypted bytes between a device and storage the
user already controls. coven owns encryption, the key layout, and retry; the
trait is the raw byte boundary below all of that. A `CloudHome` never sees
plaintext, never assigns sequence numbers, and never coordinates concurrent
writers. It reads, writes, lists, and deletes objects addressed by a flat string
key.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

<svg class="flow" viewBox="0 0 660 210" role="img" aria-label="Sync concepts pass through the sealing layer to the raw byte trait and then to a provider">
<rect class="lane" x="80" y="16" width="500" height="40" rx="9"/>
<text class="lbl s11" x="330" y="34" text-anchor="middle">sync concepts</text>
<text class="sub" x="330" y="48" text-anchor="middle">a device's changeset seq · a blob id · a member's wrapped key</text>
<line class="arr" x1="330" y1="60" x2="330" y2="74" marker-end="url(#fa)"/>
<rect class="chipo" x="80" y="78" width="500" height="40" rx="9"/>
<text class="lbl s11" x="330" y="96" text-anchor="middle">CloudSyncStorage</text>
<text class="sub" x="330" y="110" text-anchor="middle">seals and opens · maps concepts to flat keys</text>
<line class="arr" x1="330" y1="122" x2="330" y2="136" marker-end="url(#fa)"/>
<rect class="chipo" x="80" y="140" width="500" height="40" rx="9"/>
<text class="lbl s11" x="330" y="158" text-anchor="middle">CloudHome</text>
<text class="sub" x="330" y="172" text-anchor="middle">bytes by key: put_object · open_multipart · read · list · delete</text>
<line class="arr" x1="330" y1="184" x2="330" y2="196" marker-end="url(#fa)"/>
<text class="sub" x="330" y="208" text-anchor="middle">Google Drive · Dropbox · OneDrive · iCloud · S3</text>
</svg>

Examples use the todos app. Its encrypted changesets, snapshots, attachment
blobs, and membership records all land in one cloud home under keys like
`changes/dev1/42.enc`.

## What the host configures

The host selects one provider at a time and fills its settings in
[`CloudHomeConfig`](rustdoc:struct:coven::config::CloudHomeConfig), held on
[`Config`](rustdoc:struct:coven::config::Config). coven reads the current config
fresh on each operation rather than caching its own copy, so a provider swap or
disconnect takes effect on the next sync cycle without rebuilding the sync layer.

With no provider selected there is no sync layer at all; the store is
local-only and complete. At connect time
[`create_cloud_home`](rustdoc:fn:coven::storage::cloud::create_cloud_home)
reads the selected provider's settings from config and its credentials from the
OS keyring; a missing setting or credential fails with a `Storage` error naming
the field ("S3 bucket not configured", "Google Drive OAuth token not in
keyring").

## The trait

A rich storage interface would mean five implementations of encryption,
ordering, and retry, each subtly different, with the bugs living in the
least-tested backend. So the trait is deliberately dumb. Everything that has
to be *correct* lives above it, written once; a backend supplies bytes by
key, plus the two provider-shaped concerns no wrapper can hide (uploads and
sharing).

```rust
pub trait CloudHome: Send + Sync {
    async fn probe(&self) -> Result<(), CloudHomeError> { /* default: no-op list */ }

    // Uploads: one bounded request, or a streaming multipart session.
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError>;
    async fn open_multipart<'a>(&'a self, key: &str, total_len: u64)
        -> Result<BoxPartSink<'a>, CloudHomeError>;
    fn multipart_threshold(&self) -> u64;

    // Provided: picks put_object vs multipart and pumps the parts.
    async fn write(&self, key: &str, body: BlobBody, progress: &UploadProgress<'_>)
        -> Result<(), CloudHomeError> { /* central driver */ }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError>;
    async fn read_range(&self, key: &str, start: u64, end: u64)
        -> Result<Vec<u8>, CloudHomeError>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError>;
    async fn delete(&self, key: &str) -> Result<(), CloudHomeError>;
    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError>;

    async fn grant_access(&self, grant: CloudAccessGrant)
        -> Result<CloudHomeJoinInfo, CloudHomeError>;
    async fn revoke_access(&self, revoke: CloudAccessRevoke)
        -> Result<RevokeOutcome, CloudHomeError>;
}
```

- `probe` checks that the backend is reachable with the configured credentials.
  Setup flows call it before persisting credentials, so a typo or a missing
  bucket fails at setup instead of via a delayed reconnect banner. The default
  implementation lists a sentinel prefix; backends override it with a cheaper
  check (S3 uses `HeadBucket`).
- A provider implements two raw upload pieces: `put_object`, one bounded
  single-request upload, and `open_multipart`, a streaming session that accepts
  ordered parts. `multipart_threshold` is the cut between them. The provided
  `write` method is the one coven calls: a central driver that picks the path
  by size and pumps a sized [`BlobBody`] through it, reporting cumulative bytes
  to the `progress` callback for the per-file bar. Small control files (auth
  keys, head pointers, the snapshot) pass
  [`no_progress`](rustdoc:fn:coven::storage::cloud::no_progress), which
  discards the reports.
- `read` returns the whole value. `read_range` returns a half-open byte range
  (`start` inclusive, `end` exclusive), which is how coven fetches only the
  encrypted chunks covering a blob byte range.
- `list` returns every key under a prefix. `delete` is not an error when the key
  is absent. `exists` is a presence check.
- `grant_access` and `revoke_access` change who can reach the cloud home. They
  are provider-shaped and described below.

## Granting and revoking access

Membership is cryptographic, but a new member still has to *reach* the bytes:
the storage itself must admit them. That is inherently provider-shaped (a
folder share, a credential, a share URL), so it lives on the trait.

`grant_access` takes a
[`CloudAccessGrant`](rustdoc:struct:coven::storage::cloud::CloudAccessGrant)
(the member's public key, plus the provider account email for backends that
share by account) and returns a
[`CloudHomeJoinInfo`](rustdoc:enum:coven::storage::cloud::CloudHomeJoinInfo), one
variant per provider, carrying exactly what another device needs to reach the
same cloud home:

- The consumer clouds (Drive, Dropbox, OneDrive) share the store folder with
  the member's provider account and return its folder or drive id.
  `revoke_access` unshares it and reports `RevokeOutcome::Revoked`.
- S3 returns the bucket, region, endpoint, access key, secret key, and optional
  key prefix: access rides pre-shared credentials. One member's copy of a
  shared key cannot be withdrawn alone, so `revoke_access` reports
  `RevokeOutcome::Unsupported` and removal proceeds anyway: the
  [key rotation](/docs/sharing#revocation-is-key-rotation) that removal
  performs, not credential withdrawal, is what protects post-removal content.
  Cutting the removed member's residual *write* access means rotating the
  bucket credentials, which is the user's call.
- CloudKit returns a share URL.

Because `grant_access`/`revoke_access` work with folder shares and share URLs,
not encrypted payloads, they live below the encryption layer and are called
directly on the `CloudHome`, not through the wrapper described under
[Where encryption sits](#where-encryption-sits).

## Errors

`AccessDenied` means nothing to the person looking at a sync banner, and the
host can't translate it either; it doesn't know S3 from Dropbox. So every
failure crosses this boundary as a sentence a UI can show verbatim.

```rust
pub enum CloudHomeError {
    NotFound(String),
    Storage(String),
    Io(#[from] std::io::Error),
}
```

- `NotFound(key)`: the key is not there. coven uses it for the expected misses
  (no snapshot yet, a blob not uploaded yet), so a host that maps it to a UI
  state matches the variant directly.
- `Storage(msg)`: every other failure. The `msg` is the string a host can show
  in an error banner without rewording. coven and its drivers translate
  provider-specific signals into a sentence that names the cause and the
  recovery.
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

## Providers

Five cloud backends ship, plus an in-memory home for tests. Each maps the same
flat keys onto its own naming and upload protocol; the differences below are the
only places a provider deviates from "write opaque bytes by key".

- **S3** ([`S3CloudHome`](rustdoc:struct:coven::storage::cloud::s3::S3CloudHome))
  works against any S3-compatible endpoint (AWS, Backblaze B2, Wasabi, MinIO).
  Files at or below 8 MiB go up as a single `PutObject`; larger files use a
  multipart upload with 8 MiB parts, reporting progress per completed part and
  aborting the in-progress upload on failure so the bucket holds no orphaned
  parts. An optional key prefix is prepended to every key (trailing slashes
  normalized), so `changes/dev1/42.enc` can become
  `libs/abc/changes/dev1/42.enc`.

- **Google Drive**
  ([`GoogleDriveCloudHome`](rustdoc:struct:coven::storage::cloud::google_drive::GoogleDriveCloudHome))
  stores files flat in one folder. Drive filenames cannot carry the key's
  slashes, so each key is hex-encoded into a slash-free filename and decoded on
  list; the encoding is exact and reversible, never a lossy substitution. Large
  files use a resumable upload session in 8 MiB chunks (Drive requires 256
  KiB alignment).

- **OneDrive**
  ([`OneDriveCloudHome`](rustdoc:struct:coven::storage::cloud::onedrive::OneDriveCloudHome))
  uses the same hex filename encoding and a Microsoft Graph resumable
  upload session in 7.5 MiB chunks (Graph requires 320 KiB alignment).

- **Dropbox**
  ([`DropboxCloudHome`](rustdoc:struct:coven::storage::cloud::dropbox::DropboxCloudHome))
  uses native Dropbox paths under the store folder (for example
  `/Apps/your-app/my-store/changes/dev1/42.enc`), so no filename encoding is
  needed. Sharing goes through `share_folder` to get a `shared_folder_id`.

- **CloudKit**
  ([`CloudKitCloudHome`](rustdoc:struct:coven::storage::cloud::cloudkit::CloudKitCloudHome))
  stores files in the user's iCloud private database. A `CKAsset` caps at 50 MB,
  so a file larger than 10 MiB is split into 10 MiB part records and read back
  by reassembling those parts; a failed multipart upload deletes the part
  records it wrote. The raw record operations are defined by the
  [`CloudKitOps`](rustdoc:trait:coven::storage::cloud::cloudkit::CloudKitOps)
  trait and implemented in Swift through a UniFFI callback interface;
  [`create_cloud_home`](rustdoc:fn:coven::storage::cloud::create_cloud_home)
  cannot build this one from Rust alone and returns a `Storage` error directing
  you to construct it through your Swift layer.

- **In-memory**
  ([`InMemoryCloudHome`](rustdoc:struct:coven::storage::cloud::test_utils::InMemoryCloudHome),
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
[`OAuthError::Reauthorize`](rustdoc:variant:coven::oauth::OAuthError::Reauthorize)
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
[`CloudSyncStorage`](rustdoc:struct:coven::sync::cloud_storage::CloudSyncStorage),
which wraps any `dyn CloudHome`: it seals on the way down, opens on the way up,
and owns the mapping from sync concepts (a device's changeset seq, a blob id, a
member's wrapped key) to the flat keys the trait stores. Both how it seals (the
[`CloudCipher`](rustdoc:enum:coven::sync::cloud_storage::CloudCipher)) and how it
keys blobs (the
[`BlobPathScheme`](rustdoc:enum:coven::sync::cloud_storage::BlobPathScheme)) come
from the home's [storage mode](/docs/encryption#opaque-and-browsable-homes), one
choice set when the home is created:

- An **opaque** home (the default) encrypts every object under the store key
  and stores it with the `.enc` suffix (`changes/{device}/{seq}.enc`,
  `heads/{device}.json.enc`, `snapshot/{author}/{seq}.db.enc`,
  `snapshot/current.json.enc`, ...), and keys each blob by its generated path
  `{namespace}/{uploader}/{ab}/{cd}/{id}/generations/{generation}`. A provider sees `changes/dev1/42.enc` and a
  blob of ciphertext under an opaque key; it never sees a todo title or an
  attachment's bytes.
- A **browsable** home stores every object verbatim and drops the suffix, so the
  same objects are at bare names (`changes/{device}/{seq}`,
  `snapshot/{author}/{seq}.db`, ...), and stores each blob at the consumer's
  readable generated path
  [`{namespace}/.coven-generations/{uploader}/{generation}/{id}/{cloud_path}`](/docs/blobs#browsable-home-blob-paths).
  Anyone with bucket access reads the actual bytes by name.

## Ranged reads

A host that streams a large blob (audio playback, scrubbing) needs a byte
window from the middle of the file without downloading and decrypting
everything before it. The cache's
[`open_blob_stream`](/docs/cache#reading-a-blob) serves a window from the local
file on a hit, and on a miss reads it from the cloud through
[`SyncStorage::read_blob_range`](rustdoc:trait:coven::sync::storage::SyncStorage),
which is backed by a
[`BlobRangeReader`](rustdoc:struct:coven::sync::cloud_storage::BlobRangeReader).

On an opaque home a blob is stored as `[nonce: 24 bytes][encrypted chunks...]`.
The reader fetches the 24-byte base nonce once on the first read and reuses it,
then for each window range-reads only the encrypted chunks covering it and
decrypts them (see [chunked encryption](/docs/encryption#chunked-encryption)). So
streaming a blob in N windows issues one nonce read, not N, and never pulls the
whole object. On a browsable home the blob is stored verbatim, so a window is
read straight through with no nonce or decryption. The blob's
[scope](/docs/blobs#encryption-scope) is resolved to its key once when the reader
is built, so master-, derived-, and item-key blobs all stream the same way.

## Lifecycle

`handle.connect_sync(...)` builds the cloud home from the current config and
spawns the sync loop when a provider is configured. `handle.stop_sync()` drops
the loop, and `handle.start_sync()` starts it again. Because the config is read
fresh each operation, swapping providers is a config change followed by a
stop/start, with no app restart.
