# Storage

coven syncs over a [`CloudHome`](rustdoc:trait:coven::storage::cloud::CloudHome).

## Providers

The repository includes cloud storage implementations for:

- S3.
- Google Drive.
- Dropbox.
- OneDrive.
- iCloud.
- Local storage.

The host stores provider configuration in
[`config::Config`](rustdoc:struct:coven::config::Config). coven reads the
current config through a
[`ConfigProvider`](rustdoc:type:coven::sync::sync_manager::ConfigProvider)
instead of keeping its own mutable copy.

## Storage role

Storage persists encrypted sync envelopes, membership data, and encrypted blobs.
It is not trusted with plaintext and does not coordinate writes.

## Lifecycle

[`SyncManager::start_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::start_sync)
creates the cloud home from current config and starts the sync loop when sync is
enabled.
[`SyncManager::stop_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::stop_sync)
drops the active sync loop and cloud home.
