# Encryption

Everything coven writes to cloud storage is encrypted before it leaves the
device and decrypted only after it comes back. The storage provider holds
ciphertext and a flat set of key paths; it never holds a plaintext row, a
plaintext file, or a real user identity. This page describes the three
cryptographic layers, exactly what the provider can observe, where the library
key lives, and the chunk format that lets a byte range be fetched and decrypted
without the whole file.

The examples use a todos app: `workspaces` hold `lists`, a list holds `todos`, a
todo has `todo_attachments`, and todos carry labels through a `todo_labels` join.

## Three key layers

coven uses three separate keys, each for a distinct job.

**A symmetric library key** encrypts the data itself. It is one 32-byte key,
shared by every member of the library, used with XChaCha20-Poly1305 (an
authenticated cipher: decryption fails if a byte was changed). Every changeset,
every snapshot (a full database image), every blob (a file referenced by a row,
such as a `todo_attachments` image), and every membership record is encrypted
under this key. [`EncryptionService`](rustdoc:struct:coven::encryption::EncryptionService)
holds the key and does the work;
[`EncryptionService::encrypt`](rustdoc:method:coven::encryption::EncryptionService::encrypt)
and [`EncryptionService::decrypt`](rustdoc:method:coven::encryption::EncryptionService::decrypt)
are the round trip. Construct one from a raw key with
[`from_key`](rustdoc:method:coven::encryption::EncryptionService::from_key) or from a
hex string with [`new`](rustdoc:method:coven::encryption::EncryptionService::new); a
fresh key comes from
[`generate_random_key`](rustdoc:fn:coven::encryption::generate_random_key).

**A per-device Ed25519 identity** signs, it does not encrypt. Each device has one
[`UserKeypair`](rustdoc:struct:coven::keys::UserKeypair), and its 32-byte public
key is the member's identity across every library that device joins. The device
signs each changeset and each membership chain entry with `UserKeypair::sign`;
peers check the 64-byte signature with
[`verify_signature`](rustdoc:fn:coven::keys::verify_signature) against the
author's public key. This answers "who wrote this", which the library key alone
cannot: anyone holding the library key could otherwise forge a changeset as
anyone else.

**X25519 sealed-box wrapping** hands the library key to a new member. The library
key is symmetric, so it cannot travel in the clear; it is encrypted to the
joiner. Each member's X25519 key is derived deterministically from their Ed25519
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

The three are needed together. The library key keeps data secret; the Ed25519
identity proves who authored each change; the sealed box moves the library key to
a member without ever exposing it. Losing the Ed25519 secret key also loses
access to every sealed box wrapped to its derived X25519 key, since that X25519
key cannot be reconstructed without it.

## What the storage provider sees

The provider (S3, Google Drive, Dropbox, OneDrive, iCloud, an HTTP proxy) is an
opaque byte store. It sees:

- Ciphertext. Every object is encrypted under the library key (or, for a wrapped
  library key, under a member's sealed box). Without the library key the bytes
  are unreadable.
- Flat key paths, which describe structure, not content:

  ```text
  changes/{device_id}/{seq}.enc          encrypted changeset envelopes
  heads/{device_id}.json.enc             encrypted head pointers
  images/{ab}/{cd}/{id}                  encrypted blobs
  snapshot.db.enc                        full encrypted database image
  membership/{author_pubkey}/{seq}.enc   encrypted membership entries
  keys/{member_pubkey}.enc               library key wrapped to each member
  ```

- The existence and count of per-member key files under `keys/`, and the hex
  Ed25519 public keys in those paths. A pubkey is a random-looking 32-byte value,
  not a name or an email.

The provider never sees a plaintext row (`todos.title`, `lists.shared`), a
plaintext file (`todo_attachments` contents), or a real identity. Device IDs and
public keys in the paths are opaque tokens, not user records.

## Where the library key lives

coven does not persist the library key in any database it controls. The key lives
in the operating system keyring, behind the system's own access control. There is
no environment-variable or file fallback: the keyring is the only place coven
reads it from. [`KeyService`](rustdoc:struct:coven::keys::KeyService) reads and
writes it, scoped per library so two libraries never share a key.
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

## Chunked encryption

A blob can be large (a `todo_attachments` image, a snapshot), and a reader often
wants only part of it: the first frames of a video, one page of a document. To
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

There are two ways to read a byte range, depending on what bytes you fetched.
When the object you hold starts at byte 0 (the nonce, then chunks from index 0,
possibly truncated after the last chunk the range needs),
[`decrypt_range`](rustdoc:method:coven::encryption::EncryptionService::decrypt_range)
takes that object and the plaintext start and end.
[`encrypted_chunk_range`](rustdoc:fn:coven::encryption::encrypted_chunk_range)
goes with the other path: it returns the encrypted byte bounds of just the chunks
covering the range (no nonce, starting at the first needed chunk, not at 0), so
you store the 24-byte nonce separately, range-request only those bytes, and pass
both plus the first chunk index to
[`decrypt_range_with_offset`](rustdoc:method:coven::encryption::EncryptionService::decrypt_range_with_offset).
Either returns exactly the requested plaintext bytes. The base nonce is random
per `encrypt` call, so encrypting the same plaintext twice produces different
ciphertext; within one call the index-derived nonces keep each chunk distinct.

A scope can get its own key derived from the library key with
[`derive_scoped`](rustdoc:method:coven::encryption::EncryptionService::derive_scoped),
which runs HKDF-SHA256 over the library key and a scope label to produce a
distinct 32-byte key. It is deterministic (same library key and scope always
yield the same derived key) and one-way (the derived key does not reveal the
library key), so a blob encrypted under a scoped key cannot be read with the
library key, only with the same derivation.

## The key fingerprint

[`fingerprint`](rustdoc:method:coven::encryption::EncryptionService::fingerprint)
returns the first 8 bytes of SHA-256 over the key as 16 hex characters. It is a
display hint, short enough to show in a UI so a user can spot that two devices
hold different keys, long enough that two real keys are very unlikely to collide.
It is not a cryptographic commitment: it does not bind a ciphertext to a key and
must not be used to authenticate one. Tamper detection is the cipher's job, not
the fingerprint's: a changed byte makes XChaCha20-Poly1305 decryption fail
outright.
