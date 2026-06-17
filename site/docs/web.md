# coven on the web

coven compiles to `wasm32-unknown-unknown` and runs in a browser. It is the same
engine — capture a change with SQLite's session extension, sign and encrypt it,
push it to storage you own, pull and apply remote changes — with each platform
seam swapped for a browser API. A note written in one tab converges to another
tab, or to a native device, through the same cloud bucket.

Everything runs on a **dedicated Web Worker**: the browser's synchronous file
handles (which back the SQLite database) exist only off the main thread, and the
browser build is single-threaded, so the database, the sync loop, the keystore,
and the cloud client all live on one Worker. The page talks to it across the
Worker boundary.

## The seams

The engine is unchanged; only the edges differ. Each row is one trait or module
with a native implementation and a browser one.

| Seam | Native | Browser |
| --- | --- | --- |
| SQLite file storage | OS filesystem | OPFS (Origin Private File System) |
| The database handle | a thread actor (`Send`) | an `Rc<RefCell<…>>` on the Worker (`!Send`) |
| The sync loop | an OS thread running a tokio runtime | a task on the browser event loop |
| Device identity (keys) | [`KeyService`](rustdoc:struct:coven::keys::KeyService) over the OS keychain | WebCrypto + IndexedDB (`BrowserKeystore`) |
| Local blob files | `tokio::fs` | OPFS sync access handles (`local_blob`) |
| Cloud storage | AWS SDK / provider SDKs | S3 over `fetch` with hand-signed SigV4 |

Because the browser runs one thread and reqwest's wasm response types are `!Send`,
coven's storage traits ([`CloudHome`](rustdoc:trait:coven::storage::cloud::CloudHome),
[`SyncStorage`](rustdoc:trait:coven::sync::storage::SyncStorage),
[`BlobUploadObserver`](rustdoc:trait:coven::blob::BlobUploadObserver)) relax from
`Send + Sync` to `?Send` on wasm through the
[`MaybeThreadSafe`](rustdoc:trait:coven::MaybeThreadSafe) marker — empty on wasm,
`Send + Sync` on native. The same source builds both ways.

## The facade

A web page does not assemble the stack piece by piece. `CovenLibrary`, the
`wasm-bindgen` facade, takes one config object and wires the whole thing together:
it installs the OPFS storage, opens the database, loads the device identity from
the keystore, builds the at-rest cipher and blob-path scheme, constructs the
fetch-based S3 home, and starts (but does not run) the sync runtime.

```js
import init, { CovenLibrary } from '../../pkg/coven.js'

await init()
const lib = await CovenLibrary.open({
  bucket: 'my-bucket',
  region: 'us-east-1',
  endpoint: null,            // set for an S3-compatible service (MinIO, R2, B2, GCS)
  access_key: '…',
  secret_key: '…',
  key_prefix: null,
  library_id: 'my-library',  // names the OPFS database; distinct per library on an origin
  storage: 'browsable',      // 'opaque' requires encryption_key_hex (64 hex chars)
  encryption_key_hex: null,
  device_id: 'tab-a',        // each tab/device must use a distinct id
})

lib.start_sync()
await lib.exec("INSERT INTO notes (id, body, _updated_at, created_at) VALUES (…)")
const rows = await lib.query("SELECT id, body FROM notes")
lib.sync_now()
```

The methods are `exec` (writes), `query` (reads, returned as a JSON array of
rows keyed by column), `start_sync` / `stop_sync` / `sync_now`, and `is_syncing`.

coven owns the sync layer, not the domain. The facade hard-codes a small `notes`
schema purely to demonstrate row sync; a real browser app replaces the demo
schema and synced-table set with its own — the rest of the assembly is unchanged.

## Storage in the browser

The wasm database opens against the **opfs-sahpool VFS**: each SQLite file is
backed by an OPFS file reached through a synchronous access handle, so the
database survives a page reload. The facade installs it once on the Worker before
opening the database; the VFS keys files by name, so the `library_id` is the
database filename and a page that opens two libraries on one origin must give
them distinct ids.

The pool pre-reserves a fixed number of OPFS files (a database is one file plus,
transiently, its rollback journal). coven reserves enough for several concurrent
libraries; opening more than that fails with `SQLITE_CANTOPEN` rather than
growing without bound.

The browser build runs the database with WAL disabled — WAL needs shared-memory
the single-threaded OPFS VFS doesn't provide — and serves a single connection on
the Worker.

## Keys in the browser

On native, coven's keys live behind
[`KeyService`](rustdoc:struct:coven::keys::KeyService), a `keyring_core::Store`
over the OS keychain. That trait is synchronous and `Send + Sync`; the browser's
secure primitives — WebCrypto (`crypto.subtle`) and IndexedDB — are asynchronous
and single-thread-bound, and a browser has no synchronous secure storage at all.
So the trait cannot wrap them.

The browser keeps keys a different way: an async keystore the facade awaits at
`open` to materialize the device's Ed25519 identity before the sync runtime
starts. The 64-byte signing key is sealed with AES-GCM under a **non-extractable**
`CryptoKey` kept in IndexedDB — the wrapping key's raw bytes never enter JS or
wasm memory, so a dump of IndexedDB yields only ciphertext plus a key handle that
can decrypt in-page but never be exported. A reopened tab loads the same identity,
so other members keep trusting it rather than seeing a new device each time.

The signing identity is the only secret the keystore persists. The at-rest
encryption key (for an [opaque home](encryption.md#opaque-and-browsable-homes))
and the cloud credentials are supplied by the page on each `open`.

## Blobs in the browser

[Blobs](blobs.md) — the large files coven moves alongside row changes — read and
write their device-local plaintext through one `local_blob` seam: `tokio::fs` on
native, OPFS sync access handles on the Worker. A coven path like
`/library/images/ab/cd/<id>` maps to nested OPFS directories ending in the file,
so the layout the host chooses is preserved under the OPFS root. The blob's
ciphertext still travels to the cloud through the same `CloudHome` as on native.

## Cloud storage in the browser

The one cloud backend that works from a browser is **S3** (and S3-compatible
services: MinIO, Cloudflare R2, Backblaze B2, Google Cloud Storage's S3 API). It
builds and signs each request with SigV4 in Rust ([reqsign](https://crates.io/crates/reqsign))
and sends it over the browser's `fetch`, so it needs no AWS SDK. Authentication is
a static access key and secret — there is no OAuth.

Google Drive and Dropbox **stay native**: their APIs don't return CORS headers a
browser page can read, so a browser can't call them directly. OneDrive — and the
OAuth redirect flow it needs in a browser — is not part of the browser build.

### Bucket CORS

A browser only reads a response if the bucket returns
`Access-Control-Allow-Origin` for the page's origin. This is a one-time,
bucket-side configuration the owner applies; the client can't set it per request.
In AWS S3's JSON form (adapt the syntax for MinIO/R2/etc.):

```json
[
  {
    "AllowedOrigins": ["https://your.app"],
    "AllowedMethods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
    "AllowedHeaders": ["*"],
    "ExposeHeaders": ["ETag"]
  }
]
```

- `AllowedMethods` must include all of GET/PUT/POST/DELETE/HEAD — coven reads,
  writes, lists, and deletes objects.
- `AllowedHeaders: ["*"]` lets the signed `Authorization` and `x-amz-*` headers
  through the preflight.
- `ExposeHeaders: ["ETag"]` lets the client read an upload's ETag.

Use your real origin in `AllowedOrigins`; `*` is only acceptable for a throwaway
local demo.

## Building

The browser build needs the wasm32 target, a C compiler that targets wasm (the
SQLite amalgamation is compiled from C), and `wasm-pack`. On macOS, point the C
build at Homebrew LLVM:

```sh
rustup target add wasm32-unknown-unknown
brew install llvm wasm-pack

LLVM=/opt/homebrew/opt/llvm/bin
RUSTC_WRAPPER= \
  CC_wasm32_unknown_unknown="$LLVM/clang" \
  AR_wasm32_unknown_unknown="$LLVM/llvm-ar" \
  wasm-pack build --target web
```

This writes the JS glue and the `.wasm` binary to `pkg/` at the crate root. The
crate pins a Rust toolchain in `rust-toolchain.toml`; `rustup` installs it
automatically.

## Try it

`examples/web/` is a runnable two-tab demo: a notes list that syncs between two
browser tabs through an S3 bucket, driving `CovenLibrary` end to end. Build the
wasm module as above, serve the crate root over HTTP (OPFS needs a real origin,
not a `file://` URL), configure the bucket's CORS, and open the page in two tabs.
Its `README.md` has the step-by-step, including the simplest first config (a
browsable bucket for inspecting what coven writes).

## Status

coven on the web is pre-1.0, like the rest of coven. The browser build runs the
full sync engine — capture, push, pull, apply, snapshots, membership — over
OPFS-backed SQLite and an S3 bucket, with the device identity persisted in the
keystore. S3 is the only browser cloud backend; Google Drive, Dropbox, OneDrive,
and the OAuth redirect flow are native-only.
