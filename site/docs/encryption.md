# Encryption

An opaque cloud home encrypts application data before uploading it. Its Store
packages and snapshot images use the Store key; Circle data uses the relevant
Circle key. Signed Store control records remain readable so a device can verify
membership and publication history before opening encrypted data.

Encryption hides the protected contents. It does not hide the existence of the
store, its activity, or all of its metadata. A browsable home deliberately stores
application data in plaintext.

## Three key layers

**Symmetric keys protect data.** The Store keyring holds 32-byte keys used with
XChaCha20-Poly1305, which authenticates ciphertext as it decrypts it. The keyring
retains previous keys so existing data remains readable after rotation. Each
key has a generation number, but its identity is its full SHA-256 fingerprint:
concurrent rotations can produce different keys with the same generation.
New seals use the highest generation, with the greatest fingerprint breaking a
tie. Readers resolve the exact fingerprint recorded in the sealed payload.
[`MasterKeyring`](rustdoc:struct:coven::MasterKeyring) is the material managed by
[custody](/docs/keys); coven constructs the cipher internally.

[Circles](/docs/circles) have their own encryption keys. Store membership alone
does not supply a Circle's key. A blob's audience selects its Store or Circle
key, and `BlobScope::Derived` derives a further key from that audience key and
the scope label using HKDF-SHA256. A derived scope does not itself create a
membership boundary: a holder of the audience key can perform the derivation.

**Ed25519 keys prove authorship.** A Store identity and a registered device are
distinct. [`UserKeypair`](rustdoc:struct:coven::UserKeypair) supplies the identity
key. A device registration binds its author identity, device ID, and device
signing public key to one Store root and a creation, join, or recovery origin.
The device signing key is derived from that identity key and registration
context. Registrations are signed by the identity key; device-authored records
are checked against the registered device signing key. This separates
permission to decrypt from proof of who authored a record.

**Recipient encryption delivers keys.** To share the Store keyring, an owner
seals it to the recipient's X25519 public key, derived from their Ed25519 public
key. The recipient derives the matching secret key to open it. The sealed box
alone does not authenticate a sender, so coven also signs the wrapping record:
the signature binds the Store, recipient, owner, generation, and sealed bytes.
The reader verifies the authorized owner and exact reference before adopting
the keyring.

A wrapped key is stored at:

```text
keys/{owner_pubkey}/{recipient_pubkey}/{generation}/{wrap_hash}.json
```

Its key material is encrypted to the recipient; its owner and generation remain
readable. The path also exposes the recipient. The invitation therefore does
not hide who created or received the wrapping record.

## What the storage provider sees

Protection is selected by the object kind, not by its filename extension:

| Object | Protection in an opaque home |
| --- | --- |
| Store row packages, Store snapshot images, reclamation evidence | Store-key encryption |
| Store root, publications, commit records, acknowledgements, device registrations, snapshot metadata, membership entries and rollups | Signed, readable records |
| Wrapped Store keyrings and Circle access leaves | Recipient-encrypted contents, without another Store-key encryption layer |
| Circle packages, roster and metadata records, acknowledgements, snapshot images and metadata | Circle-key encryption |
| Circle control records and access envelopes | Store-key encryption |
| Application blobs | Encryption under their audience key and declared scope |

A commit record can be readable while the row changes it references are in an
encrypted package. Likewise, readable snapshot metadata refers to an encrypted
database image. Those are separate objects with separate protections.

The provider can observe object paths, stored lengths, writes, reads, and
deletions. It can read the signed control records, including their public keys,
device registrations, membership relationships, sequence and publication
positions, and exact object references. Public keys are identifiers, not an
anonymity guarantee; encryption does not conceal the provider account used to
access storage.

Representative logical paths are:

```text
store-v1/candidates/{family}/packages/{device}/{seq}/{hash}.pkg
store-v1/candidates/{family}/commits/{device}/{seq}/{hash}.json
store-v1/acks/{device}/{seq}.json
store-v1/publications/current.json
store-v1/publications/entries/{position}/{hash}.json
store-v1/snapshot-images/{author}/{hash}.db
store-v1/snapshots/{author}/{hash}.json
store-v1/membership/entries/{author}/{grant}/{stream_id}/{seq}/{hash}.json
store-v1/membership-rollups/{author}/{hash}.json
{namespace}/opaque/{locator_hash}
```

Exact protocol objects use their domain's extension, and blobs use their
locator's logical key. There is no blanket `.enc` suffix on these paths.
Providers can assign a separate physical object identifier; the retained exact
reference records that identifier along with the logical key, stored length,
and stored-byte hash.

Payloads encrypted under Store or Circle keys expose a 36-byte key tag and
their chunk header. The tag contains `CKF`, a format-version byte, and the 32-byte key fingerprint. The
header records the chunk size, plaintext length, and nonce policy. These let a
reader choose the key and locate chunks; they do not expose the key itself.
See [the threat model](/docs/threat-model) for the limits of these protections.

## Where the store key lives

The host selects [custody](/docs/keys) for its Store keyring: the OS keyring,
a passphrase-wrapped file, an in-memory value, or a host-supplied implementation.
Coven uses that owner to unlock and persist the material. On key rotation, it
persists the merged keyring before using the new key for cloud writes.

Losing every usable copy of an encryption key makes data encrypted under that
key unreadable. Keeping older keyring entries is what lets existing objects
remain readable after later rotations.

Cloud encryption does not encrypt the local SQLite database. To protect a
secret stored in an application row, the host can use
[`CovenHandle::seal_app_data`](rustdoc:method:coven::CovenHandle::seal_app_data)
and
[`CovenHandle::open_app_data`](rustdoc:method:coven::CovenHandle::open_app_data).
They use the Store keyring and record the sealing key's fingerprint. Both take
the same authentication context, such as a row identifier; opening with a
different context fails. [Keys](/docs/keys#sealing-your-own-data) describes the
API and custody requirements.

## Opaque and browsable homes

The home's storage mode selects how Store data and application blobs are
stored. An **opaque** home encrypts those contents; a **browsable** home stores
them in plaintext. Signed Store control records remain readable in either mode.
The provider's account and container access controls still apply to both.

Blob paths bind a particular stored version:

| Home | Blob logical key | Stored contents |
| --- | --- | --- |
| Opaque | `{namespace}/opaque/{locator_hash}` | Encrypted |
| Browsable | `{namespace}/readable/{cloud_path}/.coven-versions/{locator_hash}` | Plaintext |

The locator hash covers more than file contents: it includes the blob identity,
uploader registration, content hash and length, and protection-specific fields.
For an opaque blob those fields include audience, scope, and key fingerprint;
for a browsable blob they include the declared readable path. Two uploads with
equal file contents therefore need not identify the same stored object.

A browsable blob has no per-chunk authentication tags. Coven verifies a complete
download against its exact reference and plaintext hash; it refuses the
encrypted-blob range-reader API for that blob rather than returning an
unverified partial read.

## Chunked encryption

Coven encrypts chunks independently with XChaCha20-Poly1305. The default
plaintext chunk size is 64 KiB, and each stored header records the size used by
its writer. Each chunk adds a 16-byte authentication tag, including the single
tag-only chunk used for an empty payload.

The stored form for a payload encrypted with an audience key is:

```text
[key tag: 36 bytes]
[format version: 1 byte][nonce policy: 1 byte]
[chunk size: 4 bytes, little-endian][plaintext length: 8 bytes, little-endian]
[base nonce: 24 bytes, only for the random-stored policy]
[encrypted chunk 0][encrypted chunk 1]...[encrypted chunk n]
```

Whole-object encryption, including encrypted protocol objects and sealed
application-row data, uses a fresh random base nonce stored in the header.
Streaming blob encryption derives its base nonce from the sealing key and the
blob context using HKDF-SHA256. That context includes the Store ID and locator
key, which binds the plaintext hash. Re-sealing the same blob with the same key,
context, and chunk size reproduces the same ciphertext; different plaintext
must have a different context.

Each chunk's nonce is the base nonce with its index mixed in. Its authentication
tag binds the complete header, the object context, its index, and its contents.
Changing chunk positions, the declared length, or the context makes
authentication fail.

For an encrypted blob, the [range reader](/docs/storage#ranged-reads) reads the
key tag and header once, then fetches and authenticates only the chunks covering
the requested plaintext range. It does not need preceding chunks or a complete
file download to verify that range.
