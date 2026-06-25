# How coven compares

coven is an embeddable engine for **end-to-end-encrypted, multi-writer SQLite
sync over storage you already own**, an S3 bucket, a Google Drive folder, a
Dropbox folder, an iCloud container. There is no coven server and no central
database: the cloud home is the only backend, and all trust is cryptographic.

Lots of projects share one or two of those traits; the table below places coven
against them on the axes that distinguish it. No single row matches all of them, 
coven is a synthesis of pieces that usually ship separately.

> The columns are coven's distinguishing axes, not a scorecard, "fewer ✓" does
> not mean "worse," only "different design." Entries are best-effort and compress
> fast-moving projects; corrections by PR are welcome.

## Feature matrix

| System | Your own storage is the only backend | Consumer cloud + S3 | End-to-end encrypted | Multi-writer merge | Embeddable engine | Cryptographic multi-user |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| **coven** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Jazz | ~ | ✗ | ✓ | ✓ | ✓ | ✓ |
| Evolu | ~ | ✗ | ✓ | ✓ | ✓ | ~ |
| DXOS / ECHO | ~ | ✗ | ✓ | ✓ | ✓ | ✓ |
| Joplin | ✓ | ✓ | ✓ | ~ | ✗ | ✗ |
| Cryptomator | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ |
| cr-sqlite (Vlcn) | n/a | n/a | ✗ | ✓ | ✓ | ✗ |
| Litestream / LiteFS | ✓ | ~ | ✗ | ✗ | ✗ | ✗ |
| Replicache / Zero | ✗ | ✗ | ✗ | ✓ | ✓ | ✗ |
| ElectricSQL / PowerSync | ✗ | ✗ | ✗ | ✓ | ✓ | ✗ |
| remoteStorage / Solid | ✓ | ~ | ✗ | ✗ | ~ | ~ |

✓ yes · ~ partial or with a caveat (explained below) · ✗ no · n/a not in scope for that project.

## Closest peers

**Local-first encrypted engines: Jazz, Evolu, DXOS.** These are the nearest in
spirit: an embeddable engine, end-to-end encrypted, edits merge across devices.
The difference is the backend. Jazz and Evolu sync through their own
infrastructure, a sync node (Jazz) or a relay (Evolu), both self-hostable, but
neither is your raw Dropbox/S3 bucket. DXOS replicates peer-to-peer through
signaling/relay infrastructure rather than an object store. coven has no such
component at all: it reads and writes the user's bucket directly. Jazz and DXOS
have group permissions like coven's membership chain; Evolu's multi-user sharing,
built on asymmetric cryptography, is newer and owner-to-owner rather than a group
model.

**Bring-your-own consumer cloud + E2EE: Joplin, Cryptomator.** These match
coven's most unusual trait, your own Dropbox/OneDrive/S3/WebDAV is the backend,
encrypted, with no vendor server, but they are not sync engines. Joplin is a
notes *app*: it syncs multi-device with end-to-end encryption, but resolves a
concurrent edit by keeping the remote version as the live note and saving your
local edit as a conflict copy for manual resolution, rather than merging, and is
not something you embed. Cryptomator is a *file vault*: it transparently
encrypts files into your cloud, with no app-state sync, no merge, and no
multi-user model. coven takes the same "encrypt, then write to your bucket"
transport and puts a multi-writer SQLite sync engine and a cryptographic
membership chain on top.

**The SQLite multi-writer primitive: cr-sqlite (Vlcn).** cr-sqlite is the merge
layer alone: a CRDT extension that lets concurrent SQLite writers converge. It
brings no transport, encryption, or access control, you would build a coven-like
system on top of it. coven instead resolves conflicts with row-level
last-writer-wins on a hybrid logical clock (see [Sync](/docs/sync-model)) and
ships the transport, encryption, and membership with it.

**Server-backed sync engines: Replicache/Zero, ElectricSQL, PowerSync.** Strong,
mature sync engines, but they assume a server and a central database (Postgres,
typically): the client is a local-first cache that syncs to that authority.
(Replicache is now in maintenance mode; Zero is its successor.) Access control is
enforced server-side by the app, and the engines do not encrypt end-to-end (the
server reads the data), though PowerSync supports an opt-in app-layer pattern
where rows are encrypted before sync so the service stores ciphertext. coven
removes the server and the central database entirely, which is what forces the
E2EE and the cryptographic membership: with no trusted server, trust has to live
in keys.

**SQLite-to-object-store replication: Litestream / LiteFS.** These stream a
SQLite database to S3-family storage you own, which overlaps coven's
"SQLite + your bucket." But they are single-writer disaster-recovery /
read-replica tools, not multi-device merge: one primary writes, replicas follow.
No concurrent-writer convergence, no membership, and no end-to-end encryption:
Litestream removed its client-side (age) encryption in v0.5, leaving only
server-side encryption where the cloud or KMS holds the keys.

**User-owns-the-storage protocols: remoteStorage, Solid.** These share coven's
philosophy that your data lives in storage *you* control, with apps granted
scoped access. They differ in nearly everything else: per-app key-value or RDF
rather than a synced SQLite database, last-write semantics rather than a merge,
and no end-to-end encryption by default.

## What coven combines

Read down coven's row: the backend is storage the user already owns (no server,
no central database); the providers are ordinary consumer clouds and S3; every
object is encrypted before it leaves the device; concurrent writers across
devices merge; it is a library a host app embeds, not a product; and multiple
users and devices are admitted through a signed, append-only membership chain
that wraps the library key to each member.

Individually, each trait has strong implementations above. The combination, 
serverless **and** bring-your-own-consumer-cloud **and** end-to-end encrypted
**and** multi-writer SQLite **and** embeddable **and** cryptographic-membership, 
is the gap coven fills. The closest single peer is Jazz (engine + E2EE + groups),
and the distance even there is the backend: coven needs nothing running but the
user's own bucket.
