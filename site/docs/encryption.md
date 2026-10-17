# Encryption

The provider holds every byte of the store and must read none of it. Members
must read everything they're entitled to, including on a new device or after
joining late. And once Ben is removed, new data must stop reaching him. This
page covers which keys exist, what the provider can see, how a key reaches a
member, and which key new data uses. Where keys are kept on the device is the
[Keys](/docs/keys) page.

## The keys

| Key | Encrypts or does |
| --- | --- |
| The store key | the store's rows and its metadata, such as admin entries and slot headers |
| Each circle's key | that circle's rows ([Circles](/docs/circles)) |
| Each file's key | that file; the key travels inside its encrypted row |
| A member's signing pair (Ed25519) | signs every slot the member's devices write |
| A member's sealing pair (X25519) | opens keys sealed to the member |
| Provider credentials | reach storage; they encrypt nothing |

The store key and circle keys are *audience keys*. An audience can have several:
rotation adds new ones, and older ones stay readable, because old data was
written with them. Contents are encrypted with XChaCha20-Poly1305, slots are
signed with Ed25519, and keys are sealed to members with X25519 sealed boxes.

Every slot is signed by its author's member key, and the signature covers the
slot's path, so the provider can't move one object into another's place.

## What the provider sees

| Who can read it | What |
| --- | --- |
| The provider, in the clear | each slot's kind (writes, admin entry, key copy, announcement); a key copy's routing: its audience, key id, recipient member and sealing key, so circle ids are visible; object sizes and framing; the device and audience named in file and snapshot paths; which account writes each object |
| Every member, under the store key | each slot's device and claim number, and which audience each part of a write belongs to |
| A circle's members, under its key | the contents of that circle's parts |
| Members of an audience only | its names, values and operation contents |

The provider also sees counts and timing. It doesn't see which device claimed a
slot: that is sealed inside the slot.

A write's signature covers, for each of its parts, a hash over that part's row
changes. The hash is keyed with a secret derived from the part's audience key,
so a member outside that audience can't test a guessed row against it.

## Getting a key to someone

A key reaches a member as a *key copy*: the key sealed to that member's current
sealing key, published in its own slot. The copy's routing is readable, which
is how Dan finds his copy before he holds any key.

A new audience key is *introduced* by an admin entry naming the key's random id
and a SHA-256 hash of its 32 bytes. An operation claims every copy a new key
needs before it claims the introduction, so by the time a key is in use,
everyone who should have it has a copy. Store creation is the one exception:
slot 1 creates the store and the creator's copy follows at slot 2.

Opening a copy:

1. Open it with the private key its routing names.
2. If the key's introduction has already applied, check the key against its
   hash. A mismatch stops the device at that slot.
3. If the introduction hasn't arrived yet, keep the key as *tentative*. It
   can't encrypt app data until its introduction confirms it.

A device needn't open a copy meant for someone else; its routing still records
whom the key reached. A member who has no copy of an older key is sent one, so
they can read history written under it. Approving Dan's join sends him copies
of every store key, not just the newest, so he can read a late write made under
an older one.

## Which key new data uses

A copy *exposes* its key to its recipient, whether or not they ever open it. A
key exposed to someone who has since left the audience, or to a sealing key
since replaced, isn't used for new data.

Before a write is first sent, its device picks, for each audience, the newest
key that is introduced, held in custody, and not exposed that way. If none is,
the device waits for a new key from another member, or rotates one itself
through an [operation](/docs/sharing#how-admin-changes-are-made). Only a member
of the audience can rotate its key.

Carol, an admin outside Gifts, removes Ben from the store. Ben had copies of the
store key and the Gifts key. Ana is still in Gifts, so her next pass rotates the
Gifts key before any new Gifts data uses it; Carol can't make that key. If Ana
is offline, new Gifts data waits until a member of Gifts is online. Any
remaining member's device replaces the store key the same way before it sends
more store data. Older keys stay readable.

A few rules keep this from getting stuck or changing bytes:

- **Admin entries can use any accepted store key,** even an exposed one.
  Otherwise exposure could block the very removal or rotation that fixes it.
- **Retries keep their keys.** A write keeps the keys chosen on its first
  attempt, so its bytes don't change.
- **A copy can arrive late.** A copy to Ben made before its author saw his
  removal can land after Ana's read. On her next read it exposes that key, and
  Ana uses another key from then on.

Removal can't take back what Ben already downloaded. The provider's cut-off
stops him downloading more ([Sharing](/docs/sharing#provider-access)), not
opening what he holds.

## Nonces

Slots and the writes inside them are encrypted with nonces derived from their
content: an HMAC, under the encryption key, of the object's path or the write's
identity, its readable prefix, the chunk's position, and a hash of the chunk's
plaintext. So:

- a retried write or slot encrypts to the same bytes, which the byte comparison
  that settles a claim relies on ([Sync](/docs/sync-model#when-the-reply-is-lost));
- changed plaintext gets a different nonce, even if a restored backup has
  forgotten an earlier attempt at the same path.

The writes inside a slot are bound to their store, device and write number
rather than to the slot's path. When Ben loses slot 41 and resends at 42, only
the outer envelope changes; the bytes he wrote stay the same.

## Files

An uploaded file at `<store>/files/<device>/<file>` has an 11-byte readable
header, then its chunks:

```text
[kind: 1 byte (38)][format version: 2 bytes][plaintext size: 8 bytes]
[chunk 0][chunk 1]…[chunk n]
```

Each chunk is 64 KiB of the file, the last possibly shorter, encrypted with
XChaCha20-Poly1305 under the file's own random key and followed by its 16-byte
tag. A chunk's nonce is its index. Its authentication binds the path, the
header and the index, so a chunk can't be altered, swapped or reordered
unnoticed. Chunk `i` starts at `11 + i × (65,536 + 16)`, so any range is read
and checked without the rest. An empty file has a header and no chunks.

Upload retries check each plaintext chunk against its recorded hash before
encrypting it, so a retry can never encrypt different bytes under the same key
and index ([Files](/docs/files#uploads)).

The file's key is written into its row's where column, inside the row's
encrypted writes. It never reaches storage any other way, so only the row's
readers can read the file.

## Algorithms

| Use | Algorithm |
| --- | --- |
| Encrypting contents | XChaCha20-Poly1305 |
| Signing slots | Ed25519, over the path and a SHA-256 of every preceding byte |
| Sealing a key to a member | X25519 anonymous sealed box with XChaCha20-Poly1305 |
| Keys for each purpose | HKDF-SHA256 from the store or circle key, with separate labels for encryption and authorship |
| A join request's key | HKDF-SHA256 from the invite secret |
| Key introductions | SHA-256 of the key's 32 bytes, with a random key id |

## On the device

Coven doesn't encrypt the database on the device. Members' private keys and
audience keys stay in [custody](/docs/keys), never in the database. Losing every
copy of a key makes the data encrypted under it unreadable; keeping older keys
is what keeps old data readable after a rotation.
