# coven two-tab notes (browser harness)

A web page that opens a coven store in the browser and syncs a shared notes
list between two tabs through an S3 bucket. It uses the `CovenStore` wasm
facade: OPFS-backed database, the fetch-based S3 cloud home, and the event-loop
sync runtime.

The page collects an S3 config, opens the store, and shows a notes list with an
"add note" box and a sync indicator. Open it in two tabs against the same bucket
and a note added in one tab appears in the other within a sync interval.

## 1. Build the wasm module

From `crates/coven-wasm`:

```sh
LLVM=/opt/homebrew/opt/llvm/bin
RUSTC_WRAPPER= \
  CC_wasm32_unknown_unknown="$LLVM/clang" \
  AR_wasm32_unknown_unknown="$LLVM/llvm-ar" \
  wasm-pack build --target web --out-dir pkg
```

(The `CC`/`AR` overrides point the C build at Homebrew LLVM, which wasm32 needs;
they are the same overrides the wasm build and tests use.)

This writes the JavaScript glue and the `.wasm` binary to `pkg/` in
`crates/coven-wasm`. The harness imports them from `../../pkg/coven_wasm.js`, so
the `pkg/` directory must sit next to this `examples/` directory. `pkg/` is not
checked in; rebuild it whenever the Rust changes.

## 2. Serve the files

OPFS needs a real origin (not a `file://` URL), so serve over HTTP. Any static
server works, served from `crates/coven-wasm` so both `examples/` and `pkg/` are
reachable:

```sh
python3 -m http.server 8000
# then open http://localhost:8000/examples/web/
```

No special cross-origin-isolation headers are required: this build is
single-threaded (no SharedArrayBuffer), and the OPFS sahpool VFS uses
`FileSystemSyncAccessHandle` on a dedicated Worker, which a same-origin page can
use directly.

## 3. Configure the S3 bucket's CORS policy

The browser will only read S3's responses if the bucket returns CORS headers for
the page's origin. The bucket owner applies this one-time policy (AWS S3 JSON CORS
form; adapt the syntax for MinIO/R2/etc.):

```json
[
  {
    "AllowedOrigins": ["*"],
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
- `ExposeHeaders: ["ETag"]` lets the client read the upload's ETag.
- Scope `AllowedOrigins` to your real origin in any non-throwaway setup; `*` is
  shown only for a local demo.

## 4. The simplest first config

In the page's form, leave the store **browsable** — that is what the form does
by default (`storage: "browsable"`). A browsable home is plaintext with readable
blob paths: every object is stored in the clear under a readable key, so anyone
with bucket access (you, here) can inspect what coven writes (`heads/`, `changes/`,
your `notes` rows in the changesets) directly in the S3 console while debugging.

For an end-to-end-encrypted home instead, set `storage: "opaque"` and supply the
serialized `encryption_keyring_json` shared by every tab/device — coven seals
every object under that keyring and uses obfuscated, content-addressed blob paths.
(The form does not expose this; edit `app.js`'s `config` object to try it.)

**Never commit real credentials.** The form takes the access key and secret key at
runtime; nothing here stores or ships them.

## What it shows / what it does not

This harness exercises the full live path: real S3 over `fetch`, the OPFS
database, and the sync runtime driving cycles on the event loop. It uses one
unconditionally-synced `notes` table — coven's demo schema. It does **not** cover
blobs, the encrypted-home key exchange between members, or snapshot bootstrap.
