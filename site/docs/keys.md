# Keys

coven runs entirely on the host's own device and the host's own storage;
there is no coven server. Every key coven needs — the master key that
encrypts a store's data, this device's signing identity for that store, a
host's own secrets — lives on the device, and the platform key store protects
secrets that must persist. Each operating system exposes different APIs and
access policies, so coven defines what may leave the device and how a host can
supply a different custody implementation.

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
credentials, a store's signing identity, and any [host secret](#host-secrets)
— is created `WhenUnlockedThisDeviceOnly`: unreadable while the device is
locked, and, the part that matters, excluded from restoring onto a different
device through an encrypted local (Finder/iTunes) backup, because the item is
bound to this device's Secure Enclave. This is the default, not an opt-in — a
host does not choose it and cannot opt out of it.

The access policy is fixed when the item is created, and every Coven-created
Apple keyring item receives this policy on its first write.

## Custody is a choice, with working presets

Two of the three secrets coven manages — the store's master key, and that
store's signing identity — are each protected by a **custody**: a value you
choose that says where the secret is unlocked from, where it's written when
established, and how it's removed. coven never touches a cipher or a raw key
directly; it drives every master-key and identity operation through whatever
custody resolves to. Both are selected per store, on the builder — a device
holds a distinct signing identity in each store it belongs to, generated
fresh when that store is created, joined, or restored, never derived from a
shared root secret and never reused across stores.

**The master key** is selected per store, on the builder, with
[`CovenBuilder::key_custody`](rustdoc:method:coven::CovenBuilder::key_custody):

```rust
Coven::builder(store_dir, config)
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
  session and never persisted by coven.
- [`KeyCustody::Custom`](rustdoc:enum:coven::KeyCustody) — a host's own
  [`MasterKeyCustody`](rustdoc:trait:coven::MasterKeyCustody) implementation
  (`unlock` / `persist` / `forget`).

**The store's signing identity** is selected next to the master key, on the
builder, with
[`CovenBuilder::identity_custody`](rustdoc:method:coven::CovenBuilder::identity_custody):

```rust
Coven::builder(store_dir, config)
    .identity_custody(coven::IdentityCustody::Keyring)   // the default
    .synced_tables(tables)
    .migrations(migrations)
    .open()?;
```

Its presets mirror the master key's:
[`IdentityCustody::Keyring`](rustdoc:enum:coven::IdentityCustody) (the
default),
[`IdentityCustody::Passphrase`](rustdoc:enum:coven::IdentityCustody) (the
same envelope format, wrapped in a file inside the store directory —
`identity.envelope` — alongside `master.keyring`),
[`IdentityCustody::InMemory`](rustdoc:enum:coven::IdentityCustody) (a
[`UserKeypair`](rustdoc:struct:coven::UserKeypair) supplied for this session,
never persisted by coven), and
[`IdentityCustody::Custom`](rustdoc:enum:coven::IdentityCustody) (a host's
own [`DeviceIdentityCustody`](rustdoc:trait:coven::DeviceIdentityCustody)
implementation).

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

coven never mints a store's signing identity implicitly. Connect requires an
already-established identity; its absence surfaces as
[`KeyError::NoDeviceIdentity`](rustdoc:enum:coven::KeyError), never a
mint-on-demand. A store's identity is established exactly once, by whichever
of these three acts brought the store into existence on this device:

- **Creating a store fresh** —
  [`CovenHandle::initialize_identity`](rustdoc:method:coven::CovenHandle::initialize_identity)
  generates and establishes it, refusing with
  [`IdentityError::AlreadyEstablished`](rustdoc:enum:coven::IdentityError) on
  a second call, the same discipline
  [`initialize_master_key`](rustdoc:method:coven::CovenHandle::initialize_master_key)
  keeps for the master key.
- **Joining a shared store** —
  [`coven::generate_join_request`](rustdoc:fn:coven::generate_join_request)
  mints the keypair before the joiner even knows which store the invite
  names (a *pending* identity, held under a slot keyed by the request), and
  [`coven::DeviceJoinClient`](rustdoc:struct:coven::DeviceJoinClient) drives
  the signed admission exchange and promotes it into that store's own identity
  custody only after the activation is installed. A join never finished can discard its pending identity with
  [`coven::abandon_join_request`](rustdoc:fn:coven::abandon_join_request).
- **Restoring a store** —
  [`coven::restore_from_code`](rustdoc:fn:coven::restore_from_code) imports
  the identity the restore code carries, scoped to the one store it names.

Each of these is per store: a device holds a distinct identity in every store
it belongs to, so a restore code or a join for one store carries no authority
in another, and the same device's pubkey never appears in more than one
store's membership chain.

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
[`KeyError::InvalidSecretName`](rustdoc:enum:coven::KeyError).

## Per-platform host requirements

### Android

The bundled Android store needs Android's JNI application context — nothing
a `cargo build` receives on its own. A host supplies it via a Kotlin bridge: a
class exposing `external fun initializeNdkContext(Context)`, backed
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

No setup call is required. What is required is a team-prefixed
`keychain-access-groups` entitlement backed by an embedded provisioning
profile — signing alone is not enough. In Xcode, the practical way to get
this is to set `DEVELOPMENT_TEAM` so automatic signing fetches and embeds a
profile:

```xml
<!-- YourApp.entitlements -->
<key>keychain-access-groups</key>
<array>
    <string>$(AppIdentifierPrefix)com.yourcompany.yourapp</string>
</array>
```

Two distinct failure modes come from getting this wrong, and they look
nothing alike:

- **The entitlement is missing entirely** — an ad-hoc-signed binary, a plain
  `cargo test` harness, or an app built with no team. The OS refuses every
  data-protection-keychain call with `errSecMissingEntitlement` (OSStatus
  -34018), which coven surfaces as
  [`KeyError::MissingKeychainEntitlement`](rustdoc:enum:coven::KeyError) —
  typed, naming the fix above, not a generic string.
- **The entitlement is present but has no provisioning profile behind
  it** — e.g. an ad-hoc-signed build that declares `keychain-access-groups`
  without a team. This is not a keychain error at all: the kernel kills the
  process at launch (`SIGKILL`, exit 137) before any coven code runs. If a
  build has no team, the entitlement must be **omitted**, not included —
  omitting it degrades to the first failure mode (a clear, typed error on
  first key use) instead of a launch-time kill with no diagnostic.

A plain `cargo test` binary can never carry a provisioning profile (Apple
doesn't code-sign `cargo test` harnesses), so it can never reach the real
data-protection keychain — this is a platform limitation, not a coven gap.
coven's own tests mock the keyring for this reason
(`keyring_core::mock::Store`); a real key operation needs the entitlement and
profile above, in CI as much as on a device.

### Linux

There is no bundled store: `set_keyring_service` fails with
[`KeyError::UnsupportedKeyringPlatform`](rustdoc:enum:coven::KeyError) on a
target with none. A host installs one itself before calling
`set_keyring_service` — `keyring_core::set_default_store(store)` with any
store implementing `keyring_core`'s trait (a Secret Service-backed one, an
in-memory one for a CI harness) — and coven uses whatever is installed
rather than requiring one of its own three.
