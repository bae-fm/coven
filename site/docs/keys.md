# Keys

coven runs entirely on the host's own device and the host's own storage;
there is no coven server. Every key coven needs — the master key that
encrypts a store's data, this device's signing identity, a host's own
secrets — lives on the device, and the only trustworthy place on a device to
keep a secret is the platform's own key store. Getting that right across four
platforms is real work: four different APIs, four different access-policy
models, a decision about what never leaves the device, and a decision about
what happens when a host wants something coven doesn't provide out of the
box. Most cross-platform apps get some part of this wrong, silently — a key
that works but isn't actually protected, an access policy more (or less)
permissive than intended, a secret that migrates to a new device through a
backup channel nobody meant to grant.

This page is coven's position: the right default should require no decision
from the host, and a host with a real reason to deviate should still be able
to.

## Bundled platform key stores

coven bundles a keyring store for each platform it ships to: macOS and iOS
(Apple's data-protection keychain, via `apple-native-keyring-store`'s
protected store), Android (the Android Keystore, via
`android-native-keyring-store`), and Windows (Credential Manager, via
`windows-native-keyring-store`). A host names its own keyring service once at
startup with [`set_keyring_service`](rustdoc:fn:coven::set_keyring_service),
and that one call also installs whichever of the three is bundled for the
target it's compiled for; every key coven or the host stores afterward goes
through it.

```rust
coven::set_keyring_service("todos")?;
```

That is the whole integration. A host does not depend on a keyring-store crate
itself, does not call `keyring_core::set_default_store`, and does not construct
a store — coven brings the store for each target it supports, installs it, and
fails loud with a typed error if it cannot. A host that installs its own store
alongside coven's is duplicating work coven already did, and the usual way that
goes wrong is a construction failure that gets logged and swallowed, leaving no
store installed and the real cause discarded. On Android there is one further
step, but it belongs to the *app*, not to key storage:
[the JNI application context](#android) must be initialized before the first
coven call.

The one exception is a host with a requirement no bundled store meets:
`keyring_core::set_default_store` installs any store implementing
`keyring_core`'s trait, and if one is already installed when
`set_keyring_service` runs, coven keeps it instead of installing its own. That
is the deliberate escape hatch — see [Linux](#linux) below, the one target with
no bundled store at all — not the normal path.

## Correct access policy, by default

Every keyring item coven writes on Apple — the master key, cloud-home
credentials, the device signing key, and any [host secret](#host-secrets) —
is created `WhenUnlockedThisDeviceOnly`: unreadable while the device is
locked, and, the part that matters, excluded from restoring onto a different
device through an encrypted local (Finder/iTunes) backup, because the item is
bound to this device's Secure Enclave. This is the default, not an opt-in — a
host does not choose it and cannot opt out of it.

The access policy an item is created under is fixed for its lifetime; an item
a build wrote before this policy existed keeps its old accessibility class
until something deletes and recreates it.
[`StoreKeys::reprotect_apple_keys`](rustdoc:method:coven::StoreKeys::reprotect_apple_keys)
and
[`DeviceKeys::reprotect_apple_keys`](rustdoc:method:coven::DeviceKeys::reprotect_apple_keys)
are the explicit upgrade path — a host calls them once, after upgrading to a
coven build that writes device-only, to bring items an older build wrote
forward. coven never does this on its own, on a read or a write.

```rust
// Per store: the master key, cloud-home credentials, and any host secret
// names the host passes (coven doesn't track which names a host used).
key_service.reprotect_apple_keys(&["discogs_api_key"])?;
// Device-global: the signing identity. No arguments.
coven::DeviceKeys::reprotect_apple_keys()?;
```

Both are cfg'd to Apple targets only.

## Custody is a choice, with working presets

Two of the three secrets coven manages — the store's master key, and this
device's signing identity — are each protected by a **custody**: a value you
choose that says where the secret is unlocked from, where it's written when
established, and how it's removed. coven never touches a cipher or a raw key
directly; it drives every master-key and identity operation through whatever
custody resolves to.

**The master key** is selected per store, on the builder, with
[`CovenBuilder::key_custody`](rustdoc:method:coven::CovenBuilder::key_custody):

```rust
Coven::builder(config)
    .key_custody(coven::KeyCustody::Keyring)   // the default
    .synced_tables(tables)
    .migrations(migrations)
    .open()?;
```

- [`KeyCustody::Keyring`](rustdoc:enum:coven::KeyCustody) — the OS keyring,
  the default, byte-for-byte today's behavior described above.
- [`KeyCustody::Passphrase`](rustdoc:enum:coven::KeyCustody) — Argon2id over
  a memorized [`Passphrase`](rustdoc:struct:coven::Passphrase) wraps the
  master keyring; the wrapped blob is a file in the store directory
  (`master.keyring`), not a keyring entry.
- [`KeyCustody::InMemory`](rustdoc:enum:coven::KeyCustody) — a
  [`MasterKeyring`](rustdoc:struct:coven::MasterKeyring) supplied for this
  session, never persisted by coven — the native counterpart of what coven's
  wasm build already does (the page supplies the key on every open).
- [`KeyCustody::Custom`](rustdoc:enum:coven::KeyCustody) — a host's own
  [`MasterKeyCustody`](rustdoc:trait:coven::MasterKeyCustody) implementation
  (`unlock` / `persist` / `forget`).

**The device identity** is process-global, not per-store — the signing
keypair is shared by every store on the device, so it is registered once at
startup with
[`set_identity_custody`](rustdoc:fn:coven::set_identity_custody), before any
key operation:

```rust
coven::set_identity_custody(coven::IdentityCustody::Keyring)?;   // the default
```

Its presets mirror the master key's, minus `InMemory` (an identity that
vanishes at process exit is not workable as a default for something every
store's changesets are signed with):
[`IdentityCustody::Keyring`](rustdoc:enum:coven::IdentityCustody) (the
default),
[`IdentityCustody::Passphrase`](rustdoc:enum:coven::IdentityCustody) (the
same envelope format, at a host-chosen path outside any store
directory — identity is device-global, so it does not belong inside one
store's directory):

```rust
coven::set_identity_custody(coven::IdentityCustody::Passphrase {
    path: data_dir.join("identity.envelope"),
    passphrase: coven::Passphrase::new(passphrase_string),
})?;
```

and [`IdentityCustody::Custom`](rustdoc:enum:coven::IdentityCustody) (a
host's own
[`DeviceIdentityCustody`](rustdoc:trait:coven::DeviceIdentityCustody)
implementation). Registering twice — or registering after a key operation
already resolved the default implicitly — is refused; the choice is a
one-time startup decision, like `set_keyring_service`.

The two presets are not interchangeable defaults — they protect against
different things. The keyring protects against a stolen, locked device: the
OS won't hand the key to any process without the device being unlocked (and,
on Apple, without the device besides — see above). A passphrase additionally
protects against a process running as the signed-in user: nothing usable is
at rest until the passphrase is supplied, so malware running as you, or a
second process sharing your OS session, gets nothing without also knowing the
passphrase. That is also its cost: the passphrase is state a host has to
prompt the user for and the user has to remember, and losing it loses the key
exactly as thoroughly as losing a keyring entry would.

## No silent identity minting

coven never mints a device signing identity implicitly. Connect and join
require an already-established identity; its absence surfaces as
[`KeyError::NoDeviceIdentity`](rustdoc:enum:coven::KeyError), never a
mint-on-demand.
[`coven::ensure_device_identity()`](rustdoc:fn:coven::ensure_device_identity)
is the explicit call that establishes one — a host runs it at its own setup
moment (store create, first run) — and it is idempotent and safe under
concurrent callers: two callers racing it converge on one identity, never
two. Requesting a join code
([`coven::generate_join_request`](rustdoc:fn:coven::generate_join_request))
and completing a restore are the other two acts that establish an identity,
deliberately, as a side effect of what they were asked to do — a fresh device
still gets a usable join request or a completed restore without a separate
setup step.

## Sealing your own data

A host with its own secret to keep in a row — a password entry's payload, an
API token synced as app data — has a problem coven's own encryption doesn't
solve: **the local SQLite database is not encrypted.** coven's encryption is
at rest *in the cloud* (see [Encryption](/docs/encryption)); the row data
sitting in the on-device `.sqlite` file is plaintext, the same as any local
SQLite database.

[`CovenHandle::seal_app_data`](rustdoc:method:coven::CovenHandle::seal_app_data)
and
[`CovenHandle::open_app_data`](rustdoc:method:coven::CovenHandle::open_app_data)
seal under the store's own master keyring instead of a second, hand-rolled
cipher — the same custody this page describes, no second key to manage:

```rust
let sealed = handle.seal_app_data(plaintext, row_id.as_bytes())?;
// ... store `sealed` in a BLOB column ...
let plaintext = handle.open_app_data(&sealed, row_id.as_bytes())?;
```

`aad` (the second argument) binds the ciphertext to its context — the row's
own primary key, say — so a payload moved to a different row does not
silently open there. The sealed payload records the key generation it was
sealed under, so it stays openable across any number of later
[rotations](/docs/sharing#revocation-is-key-rotation).
[`SealError::Locked`](rustdoc:enum:coven::SealError) if the store has no
established master key — the same gate `connect_sync` applies before it
seals cloud traffic.

## Host secrets

A host with its own store-scoped secret that isn't row data — an API key for
a third-party service the app integrates with, say — stores it in the same
platform keyring, under the same access policy, as coven's own key material,
without importing `keyring_core` or hand-building an `Entry` against coven's
account scheme (which would not get Apple's device-only policy — see above):

```rust
handle.set_host_secret("discogs_api_key", &api_key)?;
let api_key = handle.host_secret("discogs_api_key")?;   // None if never set
handle.delete_host_secret("discogs_api_key")?;
```

`name` is scoped to the store the handle is open on — two stores never share
a host secret even under the same name. coven validates it: empty,
containing `:` (the account scheme's own separator — allowing it would let a
host secret's name forge another store's account), or matching one of
coven's own reserved slot names, is refused with
[`KeyError::InvalidSecretName`](rustdoc:enum:coven::KeyError). On Apple, a
host secret is not automatically reprotected when coven ships a build that
changes the default policy — coven doesn't track which names a host used —
so `StoreKeys::reprotect_apple_keys` takes the host's own secret names as an
argument (see [above](#correct-access-policy-by-default)).

## Per-platform host requirements

### Android

The bundled Android store needs Android's JNI application context — nothing
a `cargo build` receives on its own. A host supplies it via a small Kotlin
shim: a class exposing `external fun initializeNdkContext(Context)`, backed
by whichever shared library the host's Rust build produces (the crate's own
if built standalone, or the host's own combined native library if coven is
linked into one — bae calls its `bae_bridge`). The host calls
`initializeNdkContext(applicationContext)` once at startup, **before any
coven call** — before `set_keyring_service`, before opening a store — and
packages the backing `.so` in `jniLibs/<abi>/` like any other native
library. (bae does exactly this: `BaeApp.onCreate` calls
`Keyring.initializeNdkContext(this)` before its own `initKeyring()` wrapper
around `set_keyring_service`.)

### Windows

Windows Credential Manager caps a stored secret at
`CRED_MAX_CREDENTIAL_BLOB_SIZE`: 2560 bytes. coven's own keys fit
comfortably inside it. A host secret or a passphrase export that might not —
a long API token, a bundle of credentials serialized as JSON — should check
its own size before writing; coven surfaces the overage as an error, not a
silent truncation.

### Apple

No setup call is required. What is required is a signed binary: a plain
`cargo test` harness (or any unsigned process) cannot touch the
data-protection keychain at all — the OS refuses with
`errSecMissingEntitlement`, because the protected store needs entitlements
only a signed, provisioned app carries. coven's own tests mock the keyring
for this reason (`keyring_core::mock::Store`); a real key operation —
reading, writing, or reprotecting an Apple keyring item — needs a signed
app, in CI as much as on a device.

### Linux

There is no bundled store: `set_keyring_service` fails with
[`KeyError::UnsupportedKeyringPlatform`](rustdoc:enum:coven::KeyError) on a
target with none. A host installs one itself before calling
`set_keyring_service` — `keyring_core::set_default_store(store)` with any
store implementing `keyring_core`'s trait (a Secret Service-backed one, an
in-memory one for a CI harness) — and coven uses whatever is installed
rather than requiring one of its own three.
