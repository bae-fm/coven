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
[`EncryptionService`](rustdoc:struct:coven::encryption::EncryptionService)
holds the keyring and does the work;
[`EncryptionService::encrypt`](rustdoc:method:coven::encryption::EncryptionService::encrypt)
and [`EncryptionService::decrypt`](rustdoc:method:coven::encryption::EncryptionService::decrypt)
are the round trip. Construct one from a raw key with
[`from_key`](rustdoc:method:coven::encryption::EncryptionService::from_key) (a
one-generation keyring) or from a stored string with
[`new`](rustdoc:method:coven::encryption::EncryptionService::new); a fresh key
comes from
[`generate_random_key`](rustdoc:fn:coven::encryption::generate_random_key).

**A per-device Ed25519 identity** signs, it does not encrypt. Each device has one
[`UserKeypair`](rustdoc:struct:coven::keys::UserKeypair), and its 32-byte public
key is the member's identity across every store that device joins. The device
signs each changeset and each membership chain entry with `UserKeypair::sign`;
peers check the 64-byte signature with
[`verify_signature`](rustdoc:fn:coven::keys::verify_signature) against the
author's public key. This answers "who wrote this", which the store key alone
cannot: anyone holding the store key could otherwise forge a changeset as
anyone else.

**X25519 sealed-box wrapping** hands the store keyring to a new member. The
keyring is symmetric material, so it cannot travel in the clear; it is
encrypted to the joiner. Each member's X25519 key is derived deterministically from their Ed25519
public key
([`to_x25519_public_key`](rustdoc:method:coven::keys::UserKeypair::to_x25519_public_key),
or [`ed25519_to_x25519_public_key`](rustdoc:fn:coven::keys::ed25519_to_x25519_public_key)
when only the remote pubkey is known), so an inviter who knows a member's
identity can wrap to them without any extra handshake. The inviter calls
[`seal_box_encrypt`](rustdoc:fn:coven::keys::seal_box_encrypt) and stores the
result at `keys/{member_ed25519_pubkey}.enc`; the joiner downloads it and calls
[`seal_box_decrypt`](rustdoc:fn:coven::keys::seal_box_decrypt) with their X25519
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
  changes/{device_id}/{seq}.enc          encrypted changeset envelopes
  heads/{device_id}.json.enc             encrypted head pointers
  images/{ab}/{cd}/{id}                  encrypted blobs
  snapshot/{author}/{seq}.db.enc         a generation's encrypted database image
  snapshot/{author}/{seq}_meta.json.enc  that generation's signed per-device cursors
  snapshot/current.json.enc              signed pointer to the live generation
  membership/{author_pubkey}/{seq}.enc   encrypted membership entries
  membership/{author_pubkey}/head.enc    that author's signed membership head
  keys/{member_pubkey}.enc               store keyring wrapped to each member
  ```

  (These are the paths of an opaque home. A browsable home, see below, drops
  the `.enc` suffix, so the same objects are at `changes/{device_id}/{seq}`,
  `snapshot/{author}/{seq}.db`, and so on.)

- The existence and count of per-member key files under `keys/`, and the hex
  Ed25519 public keys in those paths. A pubkey is a random-looking 32-byte value,
  not a name or an email.
- On each master- or derived-scoped object, a 12-byte cleartext prefix naming
  the key **generation** it was sealed under, so a reader picks the right key
  without trial decryption. The generation number is a counter, not content.

The provider never sees a plaintext row (`todos.title`, `lists.shared`), a
plaintext file (`todo_attachments` contents), or a real identity. Device IDs and
public keys in the paths are opaque tokens, not user records.

## Where the store key lives

Every copy of a key is a place it can leak from: a database file rides along
in backups, an environment variable leaks into logs and child processes. So
coven does not persist the store key in any database it controls. The key
lives in the operating system keyring, behind the system's own access
control. There is
no environment-variable or file fallback: the keyring is the only place coven
reads it from. [`KeyService`](rustdoc:struct:coven::keys::KeyService) reads and
writes it, scoped per store so two stores never share a key.
[`KeyService::new`](rustdoc:method:coven::keys::KeyService::new) does no I/O: keyring
reads happen lazily inside the getters, because reading the protected keyring can
trigger a system password prompt. `get_or_create_encryption_key` returns the
existing key or mints and stores a new one; the `get_or_create` name is
deliberate, because the keyring is the only copy. Lose both the keyring entry and
every member's wrapped copy and the encrypted data is unrecoverable.

The host names itself in the keyring once at startup with
[`set_keyring_service`](rustdoc:fn:coven::keys::set_keyring_service), passing its
own app identity. That name becomes the first component of every keyring account,
so two coven-based apps on one machine never read or overwrite each other's keys.
It is required, not defaulted: a getter called before the host sets it panics
rather than fall back to a shared name.

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
  no `.enc` suffix (bare names like `snapshot/{author}/{seq}.db`,
  `heads/{device}.json`, `changes/{device}/{seq}`) and stores each blob at the
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
it: the first frames of a video, one page of a document. To
fetch and decrypt a byte range without downloading and decrypting the whole file,
[`encrypt`](rustdoc:method:coven::encryption::EncryptionService::encrypt) splits
the plaintext into 64KB chunks (`CHUNK_SIZE`) and encrypts each chunk
independently. The output is a 24-byte base nonce followed by the encrypted
chunks back to back:

```text
[base nonce: 24 bytes][chunk 0][chunk 1]...[chunk n]
```

Each chunk's nonce is the base nonce with the chunk index mixed in (a XOR with
the little-endian index), so every chunk gets a distinct nonce derived from one
random base nonce. Because chunk boundaries and nonces are computable from the
index alone, a reader can decrypt chunk `k` on its own:
[`decrypt_chunk`](rustdoc:method:coven::encryption::EncryptionService::decrypt_chunk)
takes the whole object (it reads the base nonce from the first 24 bytes) and the
chunk index, and returns that one chunk's plaintext.

To read a byte range, fetch the 24-byte nonce separately, then use
[`encrypted_chunk_range`](rustdoc:fn:coven::encryption::encrypted_chunk_range)
to get the encrypted byte bounds of the chunks covering the plaintext range.
Those bounds start at the first needed encrypted chunk, not byte 0, so the caller
passes the nonce, the fetched encrypted chunks, and the first chunk index to
[`decrypt_range_with_offset`](rustdoc:method:coven::encryption::EncryptionService::decrypt_range_with_offset).
It returns exactly the requested plaintext bytes. The base nonce is random per
`encrypt` call, so encrypting the same plaintext twice produces different
ciphertext; within one call the index-derived nonces keep each chunk distinct.

A scope can get its own key derived from the store key with
[`derive_scoped`](rustdoc:method:coven::encryption::EncryptionService::derive_scoped),
which runs HKDF-SHA256 over the store key and a scope label to produce a
distinct 32-byte key. It is deterministic (same store key and scope always
yield the same derived key) and one-way (the derived key does not reveal the
store key), so a blob encrypted under a scoped key cannot be read with the
store key, only with the same derivation.

## The key fingerprint

[`fingerprint`](rustdoc:method:coven::encryption::EncryptionService::fingerprint)
returns the first 8 bytes of SHA-256 over the key as 16 hex characters. It is a
display hint, short enough to show in a UI so a user can spot that two devices
hold different keys, long enough that two real keys are very unlikely to collide.
It is not a cryptographic commitment: it does not bind a ciphertext to a key and
must not be used to authenticate one. Tamper detection is the cipher's job, not
the fingerprint's: a changed byte makes XChaCha20-Poly1305 decryption fail
outright.
