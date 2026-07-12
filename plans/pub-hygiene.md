# Pub hygiene / dead-code hardening

`dead_code` never fires on a `pub` item, regardless of whether anything calls
it — visibility, not reachability, is what the lint checks. Every module in
this workspace's engine and facade crates is `pub` (deliberately — see
below), so an internal item that stops being called anywhere simply goes
quiet: no warning, no error, just a function nobody reaches, forever.
`unreachable_pub` closes the gap: it flags any `pub` item that no *external*
crate can actually name, i.e. `pub` items whose only role was internal. Once
those are narrowed to `pub(crate)`/`pub(super)`, `dead_code` starts seeing
them like any other private item, and does its job.

This is the same detection bundle applied to bae: deny `unreachable_pub`,
deny `dead_code`, add a lib-only CI check that neither of those two lints
alone can replace, and close the door on `#[allow(dead_code)]` as an escape
hatch.

## Where the deny lives

All three crates' `[lints]` tables are bare `workspace = true` with no local
rules (`coven-core/Cargo.toml:53`, `coven/Cargo.toml:60`,
`coven-wasm/Cargo.toml:82`) — nothing to migrate off. The deny goes straight
into `[workspace.lints.rust]` in the root `Cargo.toml`, next to the existing
`[workspace.lints.clippy]` table.

## Blast radius, measured

`RUSTFLAGS="-W unreachable_pub" cargo check -p <crate> --all-targets
--all-features`, counting only warnings whose path is inside the crate's own
`src/`:

| crate | target | warnings |
|---|---|---|
| `coven-core` | host, `--all-features` | **0** |
| `coven` | host, default features, `--all-targets` | 68 |
| `coven` | host, `--all-features`, `--all-targets` | **117** |
| `coven-wasm` | host (native cfg) | 0 |
| `coven-wasm` | `wasm32-unknown-unknown`, `--all-features` | **16** |

The default-vs-all-features gap on `coven` (68 → 117) is the `oauth-providers`
feature: with it off, `cargo check` never compiles the OAuth backend modules
(`oauth_session.rs`, `oauth_rest.rs`, `http.rs`, `key_encoding.rs`,
`resumable.rs`, `sharing.rs`, the four provider files, `account_email.rs`),
so their internal-only `pub` items are invisible rather than counted as
reachable. **117 is the real count** — `--all-features` is what CI's clippy
step already uses, so that's the number the deny has to hold against.

**`coven-core`: zero, on purpose.** Its `lib.rs` doc comment states the
design directly: implementation modules (`blob`, `sync`, `storage`, …) are
`#[doc(hidden)] pub mod`, not `pub(crate)`, because the *sibling* crates
`coven` (native) and `coven-wasm` (browser) need to reach coven-core's
internals directly to build the native/browser engines — only the curated
re-exports at the crate root are the documented host-facing API. Since every
module in the chain from crate root to any given item is `pub`, every pub
item is reachable by *some* path and `unreachable_pub` has nothing to flag.
This already holds today; the deny just keeps it holding. (Two genuine
exceptions inside coven-core already narrow correctly without any change
here: `sync::gate`'s `create_table`/`ffi`/`model`/`outbound` submodules are
private, and `gate/mod.rs` re-exports only the specific names other code
needs — `Gates`, `gate_outbound`, etc. — as `pub`/`pub(crate)` by hand.)

**`coven`: 117, real findings.** Unlike coven-core, `coven`'s internal
modules are `pub(crate) mod X { pub use coven_core::X::*; ... }` — the outer
module is already correctly scoped, but the items and sub-modules inside
were left `pub` (redundant — matches how they were written in coven-core,
where `pub` is required for cross-crate reach) instead of `pub(crate)`. The
117 warnings are almost entirely this shape: `pub use` → `pub(crate) use`,
`pub mod` → `pub(crate) mod`, `pub fn`/`pub struct` on things reached only
from within `coven` itself (`sync_manager.rs`, `sync_loop.rs`, the storage/cloud
provider backends, `oauth.rs`, `keys.rs`, `storage/local`). None of these
changed any type identity or call path — every demoted item's callers are
already inside the crate.

**`coven-wasm`: 16, all in `local_blob.rs`.** These sit inside a private
`mod imp { ... }` block that holds the OPFS (browser file system)
read/write primitives; the parent `local_blob` module uses them directly
without re-exporting, so the compiler's own suggestion (`pub(super)`, not
`pub(crate)`) is exactly right — nothing outside `local_blob.rs` needs
them. **The `#[wasm_bindgen]` surface itself — `wasm.rs`, `wasm_facade.rs`,
`wasm_keystore.rs` — has zero findings**, because the crate root already
does the right thing: `pub use wasm::install_browser_storage; pub use
wasm_facade::CovenStore; pub use wasm_keystore::BrowserKeystore;` at
`coven-wasm/src/lib.rs`. No wasm_bindgen item needed demoting or
re-exporting differently; the existing convention already matches what this
whole exercise wants.

## The public/internal boundary: no genuine public API was demoted

`unreachable_pub` looks hostile to a facade crate — coven exists to export
types a *downstream* crate (bae) consumes, and coven does not self-consume
most of that surface. The load-bearing fact that makes the lint safe here:
**`unreachable_pub` never fires on an item reachable from the crate root.**
coven's public API *is* its crate-root `pub use` exports (stated verbatim in
`lib.rs`'s doc comment), which is exactly the root-reachable set. So every
one of the 117 flagged items was internal by construction, and every genuine
public export (`CloudHome`, `Config`, `CovenHandle`, `CovenError`,
`CloudKitOps`/`Scope`/`Share`, the provider and OAuth types, `Clock`/
`IdProvider`, the error enums, `keyring_service`, …) was never flagged and
never touched.

Verified against the actual downstream, not assumed: patch-build bae against
this branch (`[patch]` its coven git dep to these local crates,
`cargo check -p bae-core -p bae-bridge --features bae-bridge/desktop`). Every
`use coven::{…}` in bae's real consumer code resolves against this branch,
with one exception that is *not* ours (below). The kept/demoted split also
lands correctly where it's subtle: `create_cloud_home` stays public (the host
entry point), `create_cloud_home_with_cloudkit` demoted (internal compose
helper); `CloudKitOps`/`Scope`/`Share` stay public, `CloudKitCloudHome`
(constructed only via `create_cloud_home`) demoted.

### `coven::read_keyring` is pre-existing drift, NOT a pub-hygiene regression

The bae patch-build fails on one symbol — `unresolved import
coven::read_keyring` — and this branch is **not** the cause. `read_keyring`
does not exist anywhere on this branch and was never demoted by it.
`b88557d` ("Type the keyring slot"), two commits before this branch's base,
replaced the old `pub fn read_keyring(account: &str)` with a private
`fn read(slot: &KeyringSlot)` when it introduced the typed `KeyringSlot`
enum. bae is pinned to an older coven rev (`85d1eaf`, before `b88557d`), so
the failure is bae's stale pin catching up to coven main's already-merged
keyring refactor. Evidence: `git grep read_keyring HEAD` is empty;
`git log -S 'pub fn read_keyring'` points at `b88557d`;
`git merge-base --is-ancestor b88557d HEAD` is true.

The `read_keyring` vs typed-`KeyringSlot` seam — whether coven should re-expose
a generic host-composition keyring read at all — is owned by separate in-flight
work reworking bae's encryption-service / keyring composition, and is
deliberately **out of scope here**. pub-hygiene does not touch, restore, or
pre-empt that decision. When bae bumps its coven pin as part of that work, it
resolves `read_keyring` there.

## `#[allow(dead_code)]`: none exist

`grep -rn dead_code crates --include='*.rs'` — zero hits, anywhere, before
this change. Nothing to classify or resolve; the grep-gate is purely
preventive.

## The lib-only CI gate: what it catches that `--all-features --all-targets` can't

CI's existing `cargo clippy --all-targets --all-features -- -D warnings`
already denies `dead_code` in principle, but two of its own flags conspire
to blind it to exactly the category this whole exercise exists to catch —
confirmed empirically, not assumed:

- `--all-features` unconditionally turns on `test-utils` (coven-core's
  and coven's dev-only feature: deterministic clock/id fakes, in-memory
  cloud home, outbox-row test accessors). No production host ever enables
  it — bae's real dependency is `coven = { workspace = true }`, bare, with
  `test-utils` appearing only in `bae-core`'s `[dev-dependencies]`.
- `--all-targets` always compiles in `--tests`, so a `#[cfg(test)] mod
  tests` counts as a caller.

A `pub(crate)` item whose *only* caller lives behind either of those —
`#[cfg(test)]` or `#[cfg(feature = "test-utils")]` — looks used under
`--all-targets --all-features` and stays invisible to `dead_code` forever,
even after the `unreachable_pub` demotion. A reproduction confirms it:
`cargo clippy --all-targets --all-features` on a crate with a
`pub(crate)` fn called only from a `test-utils`-gated helper reports
nothing; `cargo check --lib` on the same crate, default features, flags it
immediately as unused.

`scripts/check-lib-only.sh` runs each native crate's `--lib` target alone,
with the feature sets a host actually ships:

- `coven-core --lib` (default features — nothing forwards features to it
  independently of `coven`)
- `coven --lib` (default features — bae's real production shape)
- `coven --lib --features oauth-providers` (off by default, but a real
  production configuration — bae-bridge's "full" build turns it on; the
  `oauth-providers`-gated OAuth backend code needs its own pass since it's
  entirely absent from the default-features build)

`coven-wasm` doesn't need an entry: `wasm.yml` already invokes
`scripts/check-wasm.sh` bare (no `--tests`, no `--all-features`) as its
`Check (wasm32-unknown-unknown)` step — that bare invocation already is a
lib-only, default-features check for the wasm32 target, coincidentally
matching exactly what this gate is for. Nothing to add there.

`coven-core`'s `test-utils` feature isn't checked alone (only through
`coven --lib --features test-utils` would ever matter, and that's
deliberately excluded — it's the one feature set a production host never
enables, so a lib-only gate that turned it on would defeat the point).

## Commits

Single-concern, each green on its own:

1. This plan document.
2. `coven`: demote the 117 `unreachable_pub` sites to `pub(crate)`/`pub(super)`
   (mechanical `cargo fix`, with one hand-fixed regression — see below).
3. `coven-wasm`: demote the 16 `local_blob.rs` sites to `pub(super)`.
4. Deny `unreachable_pub` and `dead_code` in `[workspace.lints.rust]`.
5. CI: add `scripts/check-lib-only.sh` and wire it into `ci.yml`.
6. CI: grep-gate against new `#[allow(dead_code)]`.

## The one real bug this surfaced

`cargo fix` applies a lint's suggested visibility to the whole `use` item,
not per-name. `coven/src/keys.rs` had:

```rust
pub use coven_core::keys::{
    CloudHomeCredentials, KeyError, KeyPersistence, UserKeypair, SIGN_PUBLICKEYBYTES,
    SIGN_SECRETKEYBYTES,
};
```

`unreachable_pub` correctly flagged only `SIGN_PUBLICKEYBYTES` and
`SIGN_SECRETKEYBYTES` (the other four are re-exported at the crate root via
`coven/src/lib.rs`'s `pub use keys::{ ..., CloudHomeCredentials, ...
KeyError, KeyPersistence, ... UserKeypair };`). `cargo fix` doesn't know
that — it saw two warnings against one `use` statement and demoted the
*whole* statement to `pub(crate)`, which broke the crate root's re-export
(`E0365: is only public within the crate, and cannot be re-exported
outside`). Caught by the build, not by inspection — split into two `use`
statements, one `pub` (the four re-exported names) and one `pub(crate)` (the
two `SIGN_*BYTES` constants, used internally by `keys.rs` and
`sync/restore.rs` but never re-exported). Every other `cargo fix` site in
both crates was single-name and needed no correction; confirmed by a full
`cargo check --all-targets --all-features` (clean) after the fix and again
after every subsequent change.
