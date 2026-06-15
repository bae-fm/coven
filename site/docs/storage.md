# Storage

coven syncs over a [`CloudHome`](rustdoc:trait:coven::storage::cloud::CloudHome):
a small trait that moves opaque encrypted bytes between a device and storage the
user already controls. coven owns encryption, the key layout, and retry; the
trait is the raw byte boundary below all of that. A `CloudHome` never sees
plaintext, never assigns sequence numbers, and never coordinates concurrent
writers. It reads, writes, lists, and deletes blobs addressed by a flat string
key.

The examples use a todos app. Its synced tables are `workspaces`, `lists`,
`todos`, `todo_attachments`, and a `todo_labels` join. The encrypted changesets,
snapshots, attachment blobs, and membership records for that library all land in
one cloud home under keys like `changes/dev1/42.enc`.

## What the host configures

The host selects one provider at a time and fills its settings in
[`CloudHomeConfig`](rustdoc:struct:coven::config::CloudHomeConfig), held on
[`Config`](rustdoc:struct:coven::config::Config). coven reads the current config
through a [`ConfigProvider`](rustdoc:type:coven::sync::sync_manager::ConfigProvider)
(a closure called fresh on each operation) rather than caching its own copy, so a
provider swap or disconnect takes effect on the next sync cycle without
rebuilding the sync layer.

[`Config::sync_enabled`](rustdoc:method:coven::config::Config::sync_enabled)
returns true only when a provider is selected and both its config fields and its
credentials are present (an S3 bucket plus stored access keys, a Drive folder id
plus a stored OAuth token, and so on).

## The trait

```rust
#[async_trait]
pub trait CloudHome: Send + Sync {
    async fn probe(&self) -> Result<(), CloudHomeError> { /* default: no-op list */ }
    async fn write(&self, key: &str, data: Vec<u8>, progress: &UploadProgress<'_>)
        -> Result<(), CloudHomeError>;
    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError>;
    async fn read_range(&self, key: &str, start: u64, end: u64)
        -> Result<Vec<u8>, CloudHomeError>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError>;
    async fn delete(&self, key: &str) -> Result<(), CloudHomeError>;
    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError>;
    async fn grant_access(&self, member_id: &str)
        -> Result<CloudHomeJoinInfo, CloudHomeError>;
    async fn revoke_access(&self, member_id: &str) -> Result<(), CloudHomeError>;
}
```

- `probe` checks that the backend is reachable with the configured credentials.
  Setup flows call it before persisting credentials, so a typo or a missing
  bucket fails at setup instead of via a delayed reconnect banner. The default
  implementation lists a sentinel prefix; backends override it with a cheaper
  check (S3 uses `HeadBucket`).
- `write` creates or overwrites the key. `read` returns the whole value.
  `read_range` returns a half-open byte range (`start` inclusive, `end`
  exclusive), which is how coven fetches only the encrypted chunks covering a
  blob byte range.
- `list` returns every key under a prefix. `delete` is not an error when the key
  is absent. `exists` is a presence check.
- `grant_access` and `revoke_access` change who can reach the cloud home. They
  are provider-shaped and described below.

The `progress` argument to `write` is a
[`UploadProgress`](rustdoc:type:coven::storage::cloud::UploadProgress) callback,
`dyn Fn(u64)`. coven passes one that drives the blob upload progress bar; for
small control files (auth keys, head pointers, the snapshot) it passes
[`no_progress`](rustdoc:fn:coven::storage::cloud::no_progress), which discards
the reports.

## Granting and revoking access

`grant_access` returns a
[`CloudHomeJoinInfo`](rustdoc:enum:coven::storage::cloud::CloudHomeJoinInfo), one
variant per provider, carrying exactly what another device needs to reach the
same cloud home. The variant the host embeds in an invite code depends on the
provider:

- S3 returns the bucket, region, endpoint, access key, secret key, and optional
  key prefix. Access is managed outside coven through pre-shared credentials, so
  `grant_access` ignores `member_id` and `revoke_access` is a no-op.
- The consumer clouds (Drive, Dropbox, OneDrive) share the library folder with
  the member's account and return its folder or drive id. `revoke_access`
  unshares it.
- CloudKit returns a share URL.

Because `grant_access`/`revoke_access` work with folder ids and share URLs, not
encrypted payloads, they live below the encryption layer and are called directly
on the `CloudHome`, not through the wrapper described under
[Where encryption sits](#where-encryption-sits).

## Errors

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

Six backends ship, plus an in-memory home for tests. Each maps the same flat
keys onto its own naming and upload protocol; the differences below are the only
places a provider deviates from "write opaque bytes by key".

- **S3** ([`S3CloudHome`](rustdoc:struct:coven::storage::cloud::s3::S3CloudHome))
  works against any S3-compatible endpoint (AWS, Backblaze B2, Wasabi, MinIO).
  Files at or below 8 MiB go up as a single `PutObject`; larger files use a
  multipart upload with 8 MiB parts, reporting progress per completed part and
  aborting the in-progress upload on failure so the bucket holds no orphaned
  parts. An optional key prefix is prepended to every key (trailing slashes
  normalized), so `changes/dev1/42.enc` can become `libs/abc/changes/dev1/42.enc`.

- **Google Drive**
  ([`GoogleDriveCloudHome`](rustdoc:struct:coven::storage::cloud::google_drive::GoogleDriveCloudHome))
  stores files flat in one folder and encodes path separators as `__`, so the
  key `changes/dev1/42.enc` is the Drive filename `changes__dev1__42.enc`. Large
  files use a resumable upload session in 256 KiB-aligned chunks.

- **OneDrive**
  ([`OneDriveCloudHome`](rustdoc:struct:coven::storage::cloud::onedrive::OneDriveCloudHome))
  uses the same `__` filename encoding as Drive and a Microsoft Graph resumable
  upload session in 320 KiB-aligned chunks for large files.

- **Dropbox**
  ([`DropboxCloudHome`](rustdoc:struct:coven::storage::cloud::dropbox::DropboxCloudHome))
  uses native Dropbox paths under the library folder (for example
  `/Apps/your-app/my-library/changes/dev1/42.enc`), so no filename encoding is
  needed. Sharing goes through `share_folder` to get a `shared_folder_id`.

- **CloudKit**
  ([`CloudKitCloudHome`](rustdoc:struct:coven::storage::cloud::cloudkit::CloudKitCloudHome))
  stores files in the user's iCloud private database. A `CKAsset` caps at 50 MB,
  so a file larger than 10 MiB is split into 10 MiB records named `key.part0`,
  `key.part1`, and so on, and read back by reassembling those parts. The raw
  record operations are defined by the
  [`CloudKitOps`](rustdoc:trait:coven::storage::cloud::cloudkit::CloudKitOps) trait and
  implemented in Swift through a UniFFI callback interface;
  [`create_cloud_home`](rustdoc:fn:coven::storage::cloud::create_cloud_home)
  cannot build this one from Rust alone and returns a `Storage` error directing
  you to construct it through your Swift layer.

- **In-memory**
  ([`InMemoryCloudHome`](rustdoc:struct:coven::storage::cloud::test_utils::InMemoryCloudHome),
  under the `test-utils` feature) is a `HashMap`-backed home that two simulated
  devices share through an `Arc` to round-trip changesets and blobs in unit
  tests. It exposes `keys()`, `get()`, `len()`, and `deletes_seen()` for
  after-the-fact assertions.

[`create_cloud_home`](rustdoc:fn:coven::storage::cloud::create_cloud_home) reads
the selected provider from `Config` and the matching credentials from the OS
keyring the host installs at startup, and returns a `Box<dyn CloudHome>`. A
missing setting fails with a `Storage` error naming the field
("S3 bucket not configured", "Google Drive folder ID not configured"); missing
credentials fail the same way ("S3 credentials not in keyring", "Google Drive
OAuth token not in keyring").

## OAuth token refresh

Drive, Dropbox, and OneDrive share their token lifecycle through
[`OAuthSession`](rustdoc:struct:coven::storage::cloud::oauth_session::OAuthSession).
Each backend owns one session and routes its requests through it. Before a
request, the session checks the access token's expiry: if it expires within 60
seconds it refreshes first, persisting the new tokens to the keyring. After a
request, a `401` triggers one refresh and one retry.

When the refresh itself fails because the grant is gone (refresh token revoked,
expired, or the account password changed), the underlying
[`OAuthError::Reauthorize`](rustdoc:variant:coven::oauth::OAuthError::Reauthorize)
surfaces as a `Storage` message: "Your {provider} access was revoked or expired.
Reconnect to keep syncing." That is a user-facing message, not a transient
network error, so a host should wire it to a reconnect affordance rather than
retrying. A session missing its refresh token entirely produces the same kind of
reconnect message.

## Where encryption sits

`CloudHome` deals only in raw bytes. The at-rest protection and the key layout
live one level up, in `CloudSyncStorage`, which wraps any `dyn CloudHome`: it
seals on the way down, opens on the way up, and owns the mapping from sync
concepts (a device's changeset seq, a blob id, a member's wrapped key) to the
flat keys the trait stores. How it seals — and the key suffix — comes from the
home's `CloudCipher`:

- An **encrypted** home (the default) encrypts every object under the library key
  and stores it with the `.enc` suffix (`snapshot.db.enc`,
  `heads/{device}.json.enc`, `changes/{device}/{seq}.enc`, …). A provider sees
  `changes/dev1/42.enc` and a blob of ciphertext; it never sees a todo title or an
  attachment's bytes.
- A **plaintext** home stores every object verbatim and drops the suffix, so the
  same objects are at bare names (`snapshot.db`, `heads/{device}.json`,
  `changes/{device}/{seq}`, …). The bucket is browsable; the provider sees the
  actual bytes.

Blob objects are keyed `{namespace}/{ab}/{cd}/{id}` by default — content-addressed
and sharded by the id — or `{namespace}/{cloud_path}` for an
[unobfuscated home](blobs.md#unobfuscated-blob-paths), which stores each blob at
the consumer's readable path so the bucket is browsable by name. The blob-path
scheme is independent of the at-rest cipher above.

## Lifecycle

[`SyncManager::start_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::start_sync)
builds the cloud home from the current config and spawns the sync loop when sync
is enabled.
[`SyncManager::stop_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::stop_sync)
drops the loop and the cloud home. Because the config is read through the
provider closure, swapping providers is a config change followed by a
stop/start, with no app restart.
