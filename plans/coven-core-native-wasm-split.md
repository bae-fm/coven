# Split coven into core, native, and browser crates

## Goal

Create a Cargo workspace with three packages:

- `coven-core`: platform-neutral sync/database/blob/storage engine.
- `coven`: native package preserving the current crate name and public API.
- `coven-wasm`: browser package owning wasm-bindgen, OPFS, WebCrypto, fetch S3, browser runtime, browser tests, and the web example.

The native `coven` package remains the user-facing Rust crate. Browser code no longer lives in the native package or behind `experimental-wasm`.

## Current Shape

The root crate currently contains all platform code. `src/lib.rs` exposes the native API behind `#[cfg(not(target_arch = "wasm32"))]` and exposes browser modules behind `experimental-wasm`. Shared engine modules (`blob`, `changeset`, `clock`, `config`, `database`, `db`, `encryption`, `id_provider`, `join_code`, `keys`, `library_dir`, `migration`, `storage`, `sync`) sit beside browser modules (`wasm`, `wasm_facade`, `wasm_keystore`, wasm tests) and native modules (`coven`, `handle`, native cloud backends, keyring-backed services).

The highest-risk coupling is:

- `database.rs` contains the shared `DatabaseCore` plus the native actor shell and the wasm inline shell.
- `local_blob.rs` contains the shared blob-file API plus native `tokio::fs` and browser OPFS implementations.
- `storage/cloud/mod.rs` contains shared `CloudHome`/`BlobBody` plus native backends and the browser S3 backend.
- `sync/mod.rs` contains shared sync engine modules plus native join/restore/loop/manager and the browser runtime.

## Workspace Layout

Use `crates/` for every package:

- `crates/coven-core`
- `crates/coven`
- `crates/coven-wasm`

Root `Cargo.toml` becomes a workspace manifest with members for those three crates and shared lint settings. Move the current package manifest contents into the relevant package manifests instead of keeping a root package.

`crates/coven/Cargo.toml` keeps:

- `package.name = "coven"`
- the current version, edition, and license
- native dependencies and native features
- public API feature names except `experimental-wasm`

`crates/coven-core/Cargo.toml` owns shared dependencies: serde, serde_json, serde_yaml, thiserror, async-trait, chrono, hex, base64, sha2, hmac, hkdf, blake2, chacha20poly1305, ed25519-dalek, crypto_box, rand, uuid, urlencoding, fallible-streaming-iterator, bytes, futures-util, tracing, rusqlite/libsqlite3-sys target wiring needed by core database/session code, and dev dependencies used by pure engine tests.

`crates/coven-wasm/Cargo.toml` owns browser dependencies: getrandom wasm features, wasm-bindgen, wasm-bindgen-futures, wasm-bindgen-test, serde-wasm-bindgen, console_error_panic_hook, js-sys, web-sys, sqlite-wasm-rs, sqlite-wasm-vfs, gloo-timers, reqsign-core, reqsign-aws-v4, jiff with `js`, http, percent-encoding, quick-xml, and wasm-compatible tokio/rusqlite settings.

Do not add new third-party crates unless the implementation proves one is required. If one is required, check the registry first.

## Module Ownership

Move shared modules into `coven-core`:

- `blob` except native user-file transition pieces and browser OPFS implementation details
- `changeset`
- `clock`
- `config`
- shared database/session core
- `db`
- `encryption`
- `id_provider`
- `join_code`
- shared `keys` data and errors, excluding OS keyring persistence
- `library_dir`
- `migration`
- shared `storage::cloud` traits, `BlobBody`, `s3_common`, setup data helpers that do not construct native backends
- shared `sync` modules: apply, backoff, cloud_storage, conflict, cycle, envelope, gate, hlc, invite, membership, membership_ops, pull, push, restore_code, service, session, signed_control, snapshot, status, storage

Keep in native `coven`:

- `coven.rs`
- `handle.rs`
- native `Database` actor shell over `coven_core::DatabaseCore`
- native local blob filesystem backend over the core local blob trait
- native changeset staging and snapshot file backend
- OS keyring-backed `KeyService`
- native cloud backends: S3 SDK, OAuth providers, CloudKit, OAuth callback server
- native join/restore/bootstrap entry points
- native sync loop and `SyncManager`

Move to `coven-wasm`:

- `wasm.rs`
- `wasm_facade.rs`
- `wasm_keystore.rs`
- `sync/wasm_runtime.rs`
- `storage/cloud/s3_wasm.rs`
- browser OPFS local blob backend
- browser database shell over `coven_core::DatabaseCore`
- browser tests: `wasm_*_test.rs`
- `examples/web`
- `pkg/README.md` if it still documents generated browser package output

## Core Platform Interfaces

Expose real platform seams from `coven-core` instead of target-specific implementations hidden inside shared modules:

- `DatabaseCore`: owns `rusqlite::Connection`, capture session, schema version, synced tables, and `Hlc`.
- `DatabaseState`: exposes synced tables, schema version, `Hlc`, and `UpdatedAtStamper` construction.
- `DatabaseShell` or concrete native/browser wrappers: platform crates decide actor thread versus inline browser owner.
- `ConnectionOpenPolicy`: platform-specific mapping from a library DB path to the SQLite connection target and journal policy. Native opens the path and enables WAL; browser maps to the OPFS VFS filename and skips WAL with a log.
- `ChangesetStager`: writes captured non-empty changeset bytes before reset. Native writes to the filesystem. Browser either implements OPFS staging or returns an unsupported error that names staging.
- `LocalBlobFiles`: async operations currently provided by `local_blob`: open reader, length, copy/write/read/read_range, exists, rename, remove_file, remove_dir_all for tests, create_dir_all, walk_files.
- `SnapshotFiles`: snapshot temp/read/write behavior used by restore/join/cycle. Native implements filesystem snapshots. Browser returns explicit unsupported errors until OPFS snapshot support exists.
- Clock and ID generation use existing `Clock` and `IdProvider` primitives where the current code already has them; do not invent parallel abstractions.

Core code calls these interfaces through concrete values assembled by the platform crate. Existing data types stay single-sourced in core and are re-exported by platform crates.

## Public API Preservation

`crates/coven/src/lib.rs` re-exports the same native names currently exported by root `src/lib.rs`:

- `Coven`, `CovenBuilder`, `CovenConfig`, `CovenError`, `CovenResult`, `PendingBlob`, `SqlContext`, `WriteBatch`
- `CovenHandle`
- `DbError`
- `rusqlite`
- blob descriptors and errors
- `Migration`, `MigrationStep`, `SyncedTable`, `BlobDecl`
- `Config`, `CloudHomeConfig`, `CloudProvider`, `ConfigError`, `HomeStorage`
- keyring and OAuth functions/types
- `EncryptionService`, `EncryptionError`, `CHUNK_SIZE`
- `LibraryDir`
- sync/member/join/restore types and functions
- `CloudCipher`
- clock and ID traits/test fakes
- native `BlobStore`, native `CloudHome` backends, `CloudKitOps`, `S3CloudHome`
- OAuth provider sign-in helpers under `oauth-providers`
- test-utils re-exports
- `share-proxy` re-exports
- `MaybeThreadSafe` with the native bound

The native crate must not publicly expose browser modules or browser feature flags. Remove `experimental-wasm` from `coven`.

`coven-wasm` exposes browser entry points:

- `install_browser_storage`
- `BrowserKeystore`
- `CovenLibrary`
- browser S3 home if tests/examples need direct construction
- a JS-callable `stamp()` backed by `UpdatedAtStamper`

`CovenLibrary` must accept migrations and synced table declarations from the caller or a Rust-side browser assembly builder. Notes schema stays in tests/examples, not hard-coded into production facade construction.

## Error Shape

Incomplete browser capabilities return typed unsupported errors. Add an error variant in `coven-wasm` or reuse a core error type if it already represents capability absence. The error text names the unsupported operation:

- snapshot read/write
- join/restore
- blob operation not backed by OPFS
- changeset staging if OPFS staging is not implemented

Do not silently skip these operations or substitute an empty result.

## Tests

Keep native tests with `coven` unless they exercise only engine behavior. Move pure engine tests to `coven-core` when their imports become core-only.

Add focused tests where the trait boundary could change behavior:

- native changeset capture stages non-empty bytes before resetting the session and leaves the session intact on staging failure
- native snapshot temp/read/write paths preserve current behavior
- native local blob backend preserves atomic write/read/rename/existence behavior
- browser unsupported operations fail with the explicit unsupported error
- browser facade `stamp()` returns a stamp minted by `UpdatedAtStamper`
- browser facade assembly accepts caller-supplied migrations and synced tables; notes schema appears only in example/test code

## Verification

Run:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p coven-wasm --target wasm32-unknown-unknown
```

Run wasm tests when the toolchain is present:

```sh
wasm-pack test --headless --firefox -p coven-wasm
```

Run separation checks:

```sh
rg -n "wasm-bindgen|web-sys|sqlite-wasm-vfs|wasm_facade|BrowserKeystore|S3Wasm|experimental-wasm" crates/coven Cargo.toml README.md
rg -n "OPFS|WebCrypto|fetch|wasm_bindgen" crates/coven-core crates/coven
```

Any match in `crates/coven` or `coven-core` must be either a test fixture that does not compile into the native crate or documentation explaining that browser support lives in `coven-wasm`.

## Commit And PR

Use one branch for this separation. Commit the implementation, push, open one PR, run review and CI, then fast-forward merge into `main` after checks pass.
