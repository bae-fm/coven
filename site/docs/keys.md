# Keys

coven runs entirely on the app's devices and the household's own storage; there
is no coven server to hold a key. Every key coven needs lives on the device:
the member's two key pairs, the store and circle keys this device has opened,
this device's id, provider credentials, and secrets the app asks coven to keep.
This page covers where each lives, the custody choices, and what each platform
needs. What the keys do is the [Encryption](/docs/encryption) page.

## Naming the keychain service

The app names its keychain service once at startup, before any store opens:

```rust
coven::set_keyring_service("com.example.notes")?;
```

Every entry coven keeps goes under that service. Calling it again with the same
name is harmless; another name fails with `KeyError::ServiceAlreadyRegistered`,
and an empty name with `KeyError::InvalidServiceName`. Using the keychain before
naming the service fails with `KeyError::ServiceNotRegistered`.

coven brings the native credential store for each platform it supports: Apple's
data-protection keychain on macOS and iOS, the Android Keystore, Windows
Credential Manager, and the Secret Service over D-Bus on Linux. The app doesn't
install a store itself. On any other platform, keychain custody fails with
`KeyError::UnsupportedKeyringPlatform`; the other custody choices below still
work.

## Custody

*Custody* is where secret keys live. Coven keeps two things in custody, chosen
separately on the builder:

- **key custody** (`CovenBuilder::key_custody`): every store and circle key this
  device has opened, including older ones, so it reads writes made under them;
- **identity custody** (`CovenBuilder::identity_custody`): the member's id and
  private key pairs, including a replacement pair while one is being published.

```rust
Coven::builder(layout.clone())
    .key_custody(KeyCustody::Keyring)           // the default
    .identity_custody(IdentityCustody::Keyring) // the default
    .synced_tables(tables())
    .migrations(migrations())
    .open(store)
    .await?;
```

Each takes one of four choices:

| Choice | Keeps keys in |
| --- | --- |
| `Keyring` | the OS keychain, under the registered service; the default |
| `Passphrase(Passphrase)` | a file in the store's directory, sealed with a key derived from the passphrase by Argon2id |
| `InMemory` | memory, for this handle's session only; it starts empty |
| `Custom(..)` | the app's own [`StoreKeyCustody`](rustdoc:trait:coven::StoreKeyCustody) or [`MemberKeyCustody`](rustdoc:trait:coven::MemberKeyCustody) |

The keychain and the passphrase protect against different things. The keychain
protects against a stolen, locked device: the OS won't hand the key to any
process until the device is unlocked. A passphrase also protects against a
process running as the signed-in user: nothing usable is on disk until the
passphrase is supplied. That is also its cost: the app has to ask for it, the
person has to remember it, and losing it loses the keys as thoroughly as losing
a keychain entry would.

Custody is unlocked once, when the store is created or opened, and kept for that
handle's session, so sync passes never derive a passphrase key again. A custom
custody follows the same lifetime: `unlock` is called once, `persist` reuses
what `unlock` unlocked, `forget` removes the keys, and `close` erases what it
holds in memory.

`create_store` puts the new member's keys in identity custody before publishing
anything. Restoring takes them from the restore code; joining makes them. Opening
an existing store never makes new keys.

`handle.store_key_state()` says whether this session holds the store key
(`Available` or `Locked`). `handle.unlock_store_key()` opens the store keys
from their copies sealed to this member in storage, keeps them in custody, and
connects without starting sync. `handle.forget_store_keys()` removes them from
custody and the session, and drops the provider connection.

## What else is in the keychain

Besides the two custodies, coven keeps device-only keychain entries for this
device's id, which backups don't copy ([Sync](/docs/sync-model#another-copy-of-the-same-device)),
and for provider credentials and sign-in tokens.

On Apple platforms coven also keeps the [restore code](/docs/sharing#a-person-s-new-device-the-restore-code)
in a synced iCloud Keychain entry, and an S3 key in a separate synced entry
beside it. Replacing the S3 key updates that entry without changing the code.
`restore_from_keychain` reads them on a new device.

## Host secrets

An app can keep its own secrets, such as an API token for a service it
integrates with, in the same keychain and under the same access policy as
coven's keys:

```rust
handle.set_host_secret("discogs_api_key", &api_key)?;
let api_key = handle.host_secret("discogs_api_key")?;   // None if never set
handle.delete_host_secret("discogs_api_key")?;
```

Names are arbitrary strings, scoped to the store and encoded so they can't
collide with coven's own entries. Coven keeps a list of the names in another
keychain entry, outside the database: saving adds the name before writing the
secret, and deleting removes the secret before the name. So deleting the store
removes every secret even when its database is damaged, and the app needn't
remember the names. A crash between the two steps can leave an extra name,
never an unlisted secret.

## Deleting a store from this device

`handle.delete_store(IfUnsent::Refuse)` refuses while writes or files wait to
upload; with storage set up, it publishes this device's removal; then it closes
the store and deletes it. `IfUnsent::Discard` drops the unsent work instead.
For a store that no longer opens, `Coven::discard_store(&store_dir)` deletes it
without opening it. Deleting removes every host secret, then the list, then
coven's other keychain entries, then the directory. A deletion that fails partway
is finished by running it again. Storage is never touched.

## Per-platform requirements

### Apple

The data-protection keychain needs a team-prefixed `keychain-access-groups`
entitlement backed by an embedded provisioning profile; signing alone isn't
enough. In Xcode, setting `DEVELOPMENT_TEAM` lets automatic signing fetch and
embed one:

```xml
<!-- YourApp.entitlements -->
<key>keychain-access-groups</key>
<array>
    <string>$(AppIdentifierPrefix)com.yourcompany.yourapp</string>
</array>
```

Without the entitlement, as in an ad-hoc-signed binary or a plain `cargo test`
harness, the OS refuses every keychain call with `errSecMissingEntitlement`
(-34018). With the entitlement but no provisioning profile behind it, the OS
kills the process at launch, before any coven code runs; a build with no team
should omit the entitlement. While the keychain is locked or the session can't
show UI, the OS answers `errSecInteractionNotAllowed` (-25308): the entry is
there, and the same read succeeds once the device unlocks. Coven returns these
as `KeyError::Keychain`, keeping the native cause; an app that reads a key at
startup should treat the last one as "device locked, ask again", not as a
missing key.

iCloud storage also needs the iCloud entitlement for the app's CloudKit
container ([Storage](/docs/storage#icloud-cloudkit)).

### Android

The Android Keystore store needs the app's JNI context before the first keychain
use. The app declares the bridge class the store expects and calls it once at
startup, before `set_keyring_service` or opening a store:

```kotlin
package io.crates.keyring

import android.content.Context

class Keyring {
    companion object {
        external fun initializeNdkContext(context: Context)
    }
}

// In Application.onCreate, after loading the app's native library:
Keyring.initializeNdkContext(applicationContext)
```

### Windows

Credential Manager caps a stored secret at 2,560 bytes. Coven's own keys fit. A
host secret that might not, such as a long token or a bundle of credentials,
should check its size first; a value that doesn't fit fails with
`KeyError::Keychain`, never a silent truncation.

### Linux

The session must provide a Secret Service, such as GNOME Keyring or KWallet.
Without one, keychain custody fails with `KeyError::Keychain`; passphrase or
custom custody still works.
