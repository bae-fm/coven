# Storage

coven syncs over a [`CloudHome`](rustdoc:trait:coven::storage::cloud::CloudHome) —
a small trait that moves opaque encrypted bytes between a device and storage
the user already controls. coven owns encryption, paths, and retry; the trait
is the raw byte boundary.

## Providers

The repository includes implementations for:

- S3 (any S3-compatible bucket: AWS, Backblaze B2, Wasabi, MinIO, …).
- Google Drive.
- Dropbox.
- OneDrive.
- iCloud (CloudKit private database).
- Local filesystem.

The host wires provider configuration into
[`config::Config`](rustdoc:struct:coven::config::Config). coven reads the
current config through a
[`ConfigProvider`](rustdoc:type:coven::sync::sync_manager::ConfigProvider)
instead of holding its own mutable copy, so a host swap or disconnect
propagates without restarting the sync layer.

## Storage role

Storage persists encrypted sync envelopes, membership entries, encrypted
blobs, and snapshot metadata. It sees opaque bytes and flat keys. It does
not coordinate writes, assign sequence numbers, or hold plaintext.

## The trait

```rust
#[async_trait]
pub trait CloudHome: Send + Sync {
    async fn write(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError>;
    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError>;
    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError>;
    async fn delete(&self, key: &str) -> Result<(), CloudHomeError>;
    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError>;
    async fn grant_access(&self, member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError>;
    async fn revoke_access(&self, member_id: &str) -> Result<(), CloudHomeError>;
}
```

All eight methods are required. `grant_access` is provider-shaped — S3 returns
scoped credentials, iCloud returns a share URL, OAuth providers return folder
identifiers — and is what the joiner uses to reach the cloud home.

## Errors

```rust
pub enum CloudHomeError {
    NotFound(String),
    Storage(String),
    Io(#[from] std::io::Error),
}
```

- **`NotFound(key)`** — the key isn't there. Used for cache misses (no
  snapshot yet, blob not uploaded yet, etc.); hosts that map it to a UI state
  can match the variant directly.
- **`Storage(msg)`** — every other failure. The `msg` is **user-facing**:
  coven and its drivers write it as the message a host can show in an error
  banner without rewording.

## Per-provider classification

Each driver translates provider-specific signals into a `Storage` whose `msg`
names the cause and the recovery in one sentence:

- Google Drive `storageQuotaExceeded` → "Your Google Drive storage is full.
  Free up space at drive.google.com to keep syncing."
- Dropbox `path/insufficient_space` → "Your Dropbox storage is full. Free up
  space at dropbox.com to keep syncing."
- OneDrive `quotaLimitReached` → "Your OneDrive storage is full. Free up
  space at onedrive.live.com to keep syncing."
- S3 `AccessDenied` → "Your S3 credentials don't have permission to write to
  this bucket. Check the access policy in sync settings."
- S3 `NoSuchBucket` → "The S3 bucket no longer exists. Check the bucket name
  in sync settings."
- S3 `OverQuota` / `QuotaExceeded` → "Your S3 storage quota is exceeded.
  Free up space or expand the quota." (Backblaze, MinIO; AWS rarely returns
  these.)

OAuth providers also propagate
[`OAuthError::Reauthorize`](rustdoc:variant:coven::oauth::OAuthError::Reauthorize)
(refresh-token revoked, expired, password changed) through
[`OAuthSession`](rustdoc:struct:coven::storage::cloud::oauth_session::OAuthSession)
as `Storage("Your {provider} access was revoked or expired. Reconnect to
keep syncing.")` — the host's reconnect affordance is the right next step.

Other failure shapes keep their raw HTTP status, S3 error code, or driver
message so transient or unclassified failures stay debuggable in logs.

## Testing

The `test-utils` feature exposes
[`InMemoryCloudHome`](rustdoc:struct:coven::storage::cloud::test_utils::InMemoryCloudHome) —
a HashMap-backed `CloudHome` two simulated devices can share to round-trip
changesets and blobs in unit tests, with `keys()` / `get()` / `len()` helpers
for after-the-fact assertions.

## Lifecycle

[`SyncManager::start_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::start_sync)
constructs the cloud home from the current config and spawns the sync loop
when sync is enabled.
[`SyncManager::stop_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::stop_sync)
drops the loop and the cloud home.
