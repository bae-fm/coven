# Encryption

Everything coven writes to cloud storage is encrypted before it leaves the
device and decrypted only after it comes back. The storage provider holds
ciphertext and a flat set of key paths; it never holds a plaintext row, a
plaintext file, or a real user identity. This page describes the three
cryptographic layers, exactly what the provider can observe, where the store
key lives, and the chunk format that lets a byte range be fetched and decrypted
without the whole file.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

Examples use the todos app; a todo's attachment is the blob being sealed.

## Three key layers

coven uses three separate keys, each for a distinct job.

**A symmetric store keyring** encrypts the data itself. Each **generation**
in the keyring is one 32-byte key, shared by every member, used with
XChaCha20-Poly1305 (an authenticated cipher: decryption fails if a byte was
changed). Every changeset, every snapshot (a full database image), every blob
(a file referenced by a row, such as a `todo_attachments` image), and every
membership record is sealed under the current generation.
[Removing a member](/docs/sharing#revocation-is-key-rotation) appends a fresh
generation that the removed member never receives; older generations stay in
the keyring so data sealed before the rotation stays readable.
[`MasterKeyring`](rustdoc:struct:coven::MasterKeyring) is the value type that
carries these generations — what [custody](/docs/keys) unlocks, persists, and
forgets. coven builds the cipher that does the actual encrypt/decrypt round
trip internally from whatever the store's custody supplies; a production
host never constructs a cipher or touches a raw key directly.

**A per-device Ed25519 identity** signs, it does not encrypt. Each device has one
[`UserKeypair`](rustdoc:struct:coven::UserKeypair), and its 32-byte public
key is the member's identity across every store that device joins. The device
signs each changeset and each membership chain entry with `UserKeypair::sign`;
peers check the 64-byte signature with
`verify_signature` against the
author's public key. This answers "who wrote this", which the store key alone
cannot: anyone holding the store key could otherwise forge a changeset as
anyone else.

**X25519 sealed-box wrapping** hands the store keyring to a new member. The
keyring is symmetric material, so it cannot travel in the clear; it is
encrypted to the joiner. Each member's X25519 key is derived deterministically from their Ed25519
public key
([`to_x25519_public_key`](rustdoc:method:coven::UserKeypair::to_x25519_public_key),
or [`ed25519_to_x25519_public_key`](rustdoc:fn:coven_keys::keys::ed25519_to_x25519_public_key)
when only the remote pubkey is known), so an inviter who knows a member's
identity can wrap to them without any extra handshake. The inviter calls
[`seal_box_encrypt`](rustdoc:fn:coven_keys::keys::seal_box_encrypt) and stores the
result under its own prefix, at
`keys/{owner_ed25519_pubkey}/{member_ed25519_pubkey}.enc`; the joiner
downloads it and calls
[`seal_box_decrypt`](rustdoc:fn:coven_keys::keys::seal_box_decrypt) with their X25519
secret key. A sealed box is anonymous: it carries an ephemeral sender key, so the
stored file reveals nothing about who created the invitation.

The three are needed together. The store keyring keeps data secret; the
Ed25519 identity proves who authored each change; the sealed box moves the
keyring to a member without ever exposing it. Losing the Ed25519 secret key also loses
access to every sealed box wrapped to its derived X25519 key, since that X25519
key cannot be reconstructed without it.

<svg class="flow" viewBox="0 0 500 190" role="img" aria-label="The store keyring holds key generations; scoped keys derive from the current one">
<text class="hdr" x="130" y="22" text-anchor="middle">STORE KEYRING</text>
<text class="hdr" x="390" y="22" text-anchor="middle">DERIVED</text>
<rect class="lane" x="10" y="32" width="240" height="118" rx="10"/>
<rect class="lane" x="290" y="32" width="200" height="118" rx="10"/>
<rect class="chip" x="30" y="48" width="90" height="26" rx="6"/>
<text class="lbl s11" x="75" y="65" text-anchor="middle">gen 1</text>
<rect class="chipa" x="130" y="48" width="100" height="26" rx="6"/>
<text class="lbl s11" x="180" y="65" text-anchor="middle">gen 2 · current</text>
<text class="sub" x="130" y="96" text-anchor="middle">rotation appends; old data stays readable</text>
<text class="sub" x="130" y="134" text-anchor="middle">wrapped to each member (sealed box)</text>
<line class="arr" x1="234" y1="61" x2="286" y2="61" marker-end="url(#fa)"/>
<rect class="chipo" x="310" y="48" width="160" height="26" rx="6"/>
<text class="lbl s11" x="390" y="65" text-anchor="middle">HKDF per scope label</text>
<text class="sub" x="390" y="96" text-anchor="middle">deterministic · one-way</text>
<text class="sub" x="250" y="176" text-anchor="middle">a blob's scope (Master · Derived) picks which of these seals it</text>
</svg>

## What the storage provider sees

The provider (S3, Google Drive, Dropbox, OneDrive, iCloud) is an
opaque byte store. It sees:

- Ciphertext. Every object is encrypted under the store key (or, for a wrapped
  store key, under a member's sealed box). Without the store key the bytes
  are unreadable.

- Flat key paths, which describe structure, not content:

  ```text
  store-v1/candidates/{family}/packages/{device}/{seq}/{hash}.pkg
  store-v1/candidates/{family}/commits/{device}/{seq}/{hash}.json
  circles/{circle}/candidates/{family}/access-leaves/{owner}/{epoch}/{recipient}/{leaf}
  circles/{circle}/candidates/{family}/access-envelopes/{owner}/{recipient}/{control_hash}.json
  store-v1/heads/{device}/{seq}.json
  store-v1/snapshot-images/{author}/{hash}.db
  store-v1/snapshots/{author}/{hash}.json
  store-v1/membership/entries/{author}/{grant}/{stream_id}/{seq}/{hash}.json
  store-v1/membership/heads/{author}/{grant}/{stream_id}/{seq}.json
  images/{ab}/{cd}/{id}                       encrypted application blobs
  keys/{owner_pubkey}/{recipient_pubkey}.enc  store keyring wrapped to a member
  ```

  Storage encryption adds its suffix beneath this logical-key API. A browsable
  home writes the same signed Store objects without the opaque ciphertext suffix
  and uses declared readable paths for application blobs.

- The existence and count of key files under each owner's `keys/{owner_pubkey}/`
  prefix, and the hex Ed25519 public keys in those paths. A pubkey is a
  random-looking 32-byte value, not a name or an email.

- On each master- or derived-scoped object, a 12-byte cleartext prefix naming
  the key **generation** it was sealed under, so a reader picks the right key
  without trial decryption. The generation number is a counter, not content.

The provider never sees a plaintext row (`todos.title`, `lists.shared`), a
plaintext file (`todo_attachments` contents), or a real identity. Device IDs and
public keys in the paths are opaque tokens, not user records.

## Where the store key lives

Every copy of a key is a place it can leak from: a database file rides along
in backups, an environment variable leaks into logs and child processes. So
coven never writes the master key into the database it controls, and there
is no environment-variable or file fallback for it. Where it *does* live is
a choice a host makes once, on the builder, called
[custody](/docs/keys) — the default is the OS keyring, behind the system's
own access control, and a host with a different threat model can choose a
passphrase-wrapped file or supply its own store instead. [Keys](/docs/keys)
covers every preset and the tradeoffs between them; this page assumes the
default.

The host names itself in the keyring once at startup with
[`set_keyring_service`](rustdoc:fn:coven::set_keyring_service), passing its
own app identity. That name becomes the first component of every keyring account,
so two coven-based apps on one machine never read or overwrite each other's keys.
It is required, not defaulted: a key operation attempted before the host calls
it fails with a typed error rather than falling back to a shared name.

[`CovenHandle::initialize_master_key`](rustdoc:method:coven::CovenHandle::initialize_master_key)
is the only place coven ever generates a master key; it refuses to run again
once one is established, so a corrupt entry is never silently overwritten.
Lose both the established key and every member's wrapped copy of it and the
encrypted data is unrecoverable.

**The local SQLite database is not encrypted.** coven's encryption is at
rest *in the cloud* — every object a device pushes is sealed before it
leaves, as this page describes — but the row data living in the on-device
`.sqlite` file is plaintext, the same as any local SQLite database. A host
that keeps its own secret in a row (a password entry's payload, an API
token) seals it itself with
[`CovenHandle::seal_app_data`](rustdoc:method:coven::CovenHandle::seal_app_data)
before writing the row, and reads it back with
[`CovenHandle::open_app_data`](rustdoc:method:coven::CovenHandle::open_app_data).
Both run under the store's own master key — the same custody this page
describes protects them, so there is no second key to manage — and the
sealed payload records the key generation it was sealed under, so it stays
readable across any number of later rotations. See [Keys](/docs/keys#sealing-your-own-data)
for the full API.

## Opaque and browsable homes

Everything above describes an **opaque home**, the default. A store can
instead be created as a **browsable home**, which stores every object in the
clear. This is one per-store choice, `cloud_home.storage`, set when the home is
created and fixed thereafter (it determines how every object is written).

This choice is about whether what's stored is *legible*, not about who can reach
it. It is **not** access control: the storage provider's own access control (the
bucket's credentials, the account's sign-in) applies either way. "Browsable" does
not mean open to the world, it means that anyone who already has access to the
bucket sees the actual files instead of ciphertext.

- An **opaque home** (`storage: opaque`, the default) seals every object under
  the store key and stores it with the `.enc` suffix, and keys each blob by its
  content-addressed shard `{namespace}/{ab}/{cd}/{id}`. Anyone with bucket access
  sees only ciphertext under opaque keys.
- A **browsable home** (`storage: browsable`) stores every object verbatim with
  no `.enc` suffix (bare Store package, commit, head, and snapshot names
  under `store-v1/`) and stores each blob at the
  consumer's own readable path `{namespace}/{cloud_path}`. Anyone with bucket
  access can open the snapshot or a blob directly without any key, which is the
  point: it is for a store whose contents are not secret and whose owner wants
  to read the bucket by name (e.g. inspect it in the storage console).

The one choice drives two mechanisms together, the at-rest cipher and the
blob-path scheme, both held by a `CloudSyncStorage`:

- `CloudCipher::Encrypted(key)` (opaque) seals every object under the store key
  (the behavior described everywhere above); `CloudCipher::Plaintext` (browsable)
  stores every object verbatim and drops the `.enc` suffix.
- `BlobPathScheme::Hashed` (opaque) keys a blob by its content-addressed shard;
  `BlobPathScheme::Plain` (browsable) keys it at the readable `cloud_path` the
  consumer supplies (see [blobs](blobs.md#browsable-home-blob-paths)).

The storage mode changes only what happens at rest. Changesets, snapshots, the
snapshot metadata, the min-schema marker, membership entries, and blobs are
stored sealed (opaque) or verbatim (browsable); everything else, the sync
protocol, the HLC register, the row-level gate, is unchanged.

Two capabilities exist only on an opaque home:

- **Restore codes** carry the store key (`ek`) so a second device can read the
  bucket. A browsable home has no key, so its restore code omits `ek` entirely.
  The presence of `ek` *is* the home's storage mode: present ⇒ opaque (rebuilt as
  `CloudCipher::Encrypted` + `BlobPathScheme::Hashed`), absent ⇒ browsable
  (`CloudCipher::Plaintext` + `BlobPathScheme::Plain`).
- **Sharing** (inviting and removing members) wraps and rotates the store key.
  With no key there is nothing to wrap or rotate, so a member operation on a
  browsable home is a clear error ("sharing requires an opaque cloud home")
  rather than a silent no-op. An invite is therefore always for an opaque home,
  and the invite code carries no storage flag.

The two kinds of home at a glance:

| | Opaque (default) | Browsable |
| --- | --- | --- |
| Config `cloud_home.storage` | `opaque` | `browsable` |
| Restore-code `ek` | `Some(hex)` | absent |
| Runtime cipher | `CloudCipher::Encrypted(key)` | `CloudCipher::Plaintext` |
| Blob-path scheme | `BlobPathScheme::Hashed` | `BlobPathScheme::Plain` |
| Object bytes at rest | sealed (XChaCha20-Poly1305) | verbatim |
| Object-key suffix | `.enc` | none |
| Blob key | `{namespace}/{ab}/{cd}/{id}` | `{namespace}/{cloud_path}` |
| Sharing (invite / remove member) | available | error |

## Chunked encryption

A single authenticated ciphertext has no random access: the whole object
must be fetched and decrypted to read any part of it. Chunking restores
random access without giving up authentication. A blob can be large (a
`todo_attachments` image, a snapshot), and a reader often wants only part of
it: the first frames of a video, one page of a document. coven's cipher
splits the plaintext into 64KB chunks
([`CHUNK_SIZE`](rustdoc:const:coven::CHUNK_SIZE)) and encrypts each chunk
independently, so a byte range can be fetched and decrypted without touching
the rest of the file. The output is a 24-byte base nonce followed by the
encrypted chunks back to back:

```text
[base nonce: 24 bytes][chunk 0][chunk 1]...[chunk n]
```

Each chunk's nonce is the base nonce with the chunk index mixed in (a XOR with
the little-endian index), so every chunk gets a distinct nonce derived from one
random base nonce, and chunk boundaries and nonces are computable from the
index alone — a reader decrypts chunk `k` without touching the chunks before
it. This is what makes a [ranged read](/docs/storage#ranged-reads) possible:
only the encrypted chunks covering the requested plaintext range are fetched
and decrypted, never the whole object. The base nonce is random per seal, so
sealing the same plaintext twice produces different ciphertext.

A scope can get its own key derived from the store key, deterministically
(the same store key and scope always yield the same derived key) and
one-way (the derived key does not reveal the store key) — coven derives it
internally with HKDF-SHA256 over the store key and the scope label whenever
it seals a `Derived`-scoped blob (see [Encryption scope](/docs/blobs#encryption-scope)),
so a blob encrypted under a scoped key cannot be read with the store key,
only with the same derivation. None of this cipher machinery — chunks,
nonces, derivation — is public API; a host never touches it directly.

## The key fingerprint

[`MasterKeyring::fingerprint`](rustdoc:method:coven::MasterKeyring::fingerprint)
returns the first 8 bytes of SHA-256 over the current generation's key as 16
hex characters — the same value
[`CovenHandle::master_key_fingerprint`](rustdoc:method:coven::CovenHandle::master_key_fingerprint)
returns for an open store. It is a
display hint, short enough to show in a UI so a user can spot that two devices
hold different keys, long enough that two real keys are very unlikely to collide.
It is not a cryptographic commitment: it does not bind a ciphertext to a key and
must not be used to authenticate one. Tamper detection is the cipher's job, not
the fingerprint's: a changed byte makes XChaCha20-Poly1305 decryption fail
outright.
