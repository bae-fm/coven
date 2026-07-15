# Constraints

coven has no server. Removing it is what makes the rest possible — no backend to
run, storage the user already owns, end-to-end encryption that no operator can
undo — but it also removes the one place a traditional system puts its
enforcement. A server is a trusted third party that sits in the middle of every
request: it can refuse a token the instant you revoke it, hand each user only the
rows they may see, keep a log nobody can forge, throttle an abuser, and delete a
record on command. coven has none of that, because there is nobody in the middle.
Trust lives in keys, and a key, once given, cannot be taken back.

This page is the honest ledger of what that costs. Every limit below traces to
the same root: **with no server, there is no central point of enforcement.** The
[comparison](/docs/comparison) page places coven against other systems on what it
*does*; this one is what it gives up to do it.

## Revocation withholds new secrets; it cannot un-send old ones

When you remove a member, coven rotates the store to a
[new key generation](/docs/encryption#three-key-layers) the removed member never
receives, re-wraps the keyring to everyone who remains, and deletes the removed
member's wrapped copy (see
[Revocation is key rotation](/docs/sharing#revocation-is-key-rotation)). From
that point on, everything sealed under the new generation is unreadable to them.

What removal cannot do is reach into their device. Anything the removed member
already pulled and decrypted before the rotation, they keep — the plaintext is on
their disk, and the old key generations that were valid when they held them stay
valid for data sealed under them. There is no server to send a "forget this"
command, and no way to claw a byte back once it has left. Removal is a guarantee
about the *future* — no new content — not about the past. If a member has seen
something secret, treat it as seen; rotate the actual secret (a password, an API
key stored in a row), not just the membership.

## No server-side access control

A server enforces policy at the moment of each request. coven has no such moment,
so several controls a server-backed system offers do not exist:

- **No instant, universal cutoff.** Removing a member cuts their cloud access
  where the provider allows it (an unshare on the consumer clouds), but on S3 a
  shared credential cannot be withdrawn from one holder alone, so
  [setting access to absent](/docs/storage#granting-and-revoking-access) reports it as
  unrevocable and removal proceeds on the [key rotation](#revocation-withholds-new-secrets-it-cannot-un-send-old-ones)
  alone. Even where cloud access *is* cut, it does not un-see already-synced data.
- **No per-row or per-field filtering between members.** Membership grants the
  whole store: a new member receives the [store keyring](/docs/sharing#the-store-keyring)
  and can decrypt every object in the store's access scope. There is no server to
  hand member A some rows and member B others, or to hide a column from one
  member. Confidentiality is between the store's members and the outside world
  (non-members and the storage provider), not among the members. The
  [gate](/docs/local-data) that keeps some rows on one device is the owner's
  device holding its own rows back, not the store partitioning data across
  members.
- **No tamper-proof access log.** coven signs append-only
  [membership state](/docs/sharing#membership-records), so who may *write* is
  recorded and forgery-evident. But a *read* is just a download of ciphertext from
  the provider; coven keeps no record of who fetched what, and there is no server
  to keep one.
- **No rate limiting.** Nothing in coven throttles how fast a member reads or
  writes. The only limits are the storage provider's own; coven absorbs a
  provider's throttling ([the OAuth session retries a `429`](/docs/storage#oauth-sessions-refresh-and-retry))
  but imposes none of its own.
- **No forced deletion.** You can delete rows and let the delete propagate (see
  [deletes are remove-wins](/docs/merge#the-merge)), but you cannot compel another
  member's device to erase what it already holds, and you cannot reach past your
  own access to purge the provider.

## Every member device holds the whole store

coven is fully replicated: every device carries the entire database within its
access scope, not a server-filtered slice of it. A new device
[bootstraps](/docs/bootstrap) from a snapshot, which is a full image of the
database, and thereafter every row that syncs reaches every member device.

This is the direct consequence of granting the keyring rather than mediating
access: there is no "need to know" sub-store a member is kept out of, because
there is no query-time authority to enforce one. It is a strength for
availability and offline use — every device is complete and self-sufficient — and
a limit on confidentiality *within* a store: sharing a store is sharing all of
it. When two sets of people should not see each other's data, that is two stores,
not one store with internal walls.

## Consistency follows the write policy

Devices are not consistent at every instant. `MergeConcurrent` allows offline
branches and [merges](/docs/merge) them locally. `Serial` gives published
commits one global order through a conditional storage head, but a local replica
may lag that head and stale offline work becomes an explicit branch conflict.
Neither policy offers a transaction spanning devices. The consequences:

- **`MergeConcurrent` uses column-wise last-writer-wins.** Concurrent edits to different columns of one
  row both survive; concurrent edits to the *same* column resolve to the later
  writer, ordered by a [hybrid logical clock](/docs/merge#the-clock) rather than
  wall-clock time. The loser's value is overwritten, not queued for review.
- **`MergeConcurrent` uses delete-wins, and only after sync.** A delete beats a concurrent edit
  ([`arbitrate_row_conflict`](rustdoc:fn:coven::sync::conflict::arbitrate_row_conflict)),
  but a delete only takes effect on a device once that device pulls it. Until
  then, the row is still live there.
- **No cross-device transactions.** A transaction is atomic on the device that
  writes it, but coven does not offer a transaction that spans devices. Under
  `MergeConcurrent`, two offline commits meet and merge. Under `Serial`, only
  one branch can advance a given global head; another remains local until the
  host discards it or reruns its intent against the current state.
- **No globally-enforced uniqueness or foreign keys.** SQLite enforces a
  `UNIQUE` or `CHECK` constraint within one device, but coven cannot enforce one
  across the store: two offline devices can each insert a row that is locally
  valid but collides on merge. When that happens the conflicting rows are omitted
  and the affected tables surfaced, rather than reconciled
  ([constraints and foreign keys](/docs/merge#constraints-and-foreign-keys)). A
  foreign-key reference whose parent has not yet arrived is retried and, if still
  unsatisfied, dropped. Design the schema so concurrent writers do not depend on a
  globally unique value being unique everywhere at once.

## The key lives on the device and in the restore code

There is no account to sign into and no server that can gate access to the data.
The store key lives, by default, in the [operating system
keyring](/docs/encryption#where-the-store-key-lives) — a host can choose a
different [custody](/docs/keys) instead, but never an
environment-variable or file fallback — and a
[restore code](/docs/sharing#restore-codes) carries the store keyring and signing
key so a new device of an existing member can reconnect. The restore code
therefore *is* the key: anyone who holds it has full access to the store, so it is
the most sensitive string coven produces.

The flip side is that there is no reset. If another member remains, they can
re-invite you and you rejoin under a new identity. But if you are the store's only
member and you lose every device *and* the restore code, the encrypted data is
unrecoverable — there is no operator to verify your identity and let you back in,
because there is no operator at all. The security that keeps the provider from
reading your data is the same property that means nobody can recover it for you.

## The storage provider is the shared dependency

Removing the server does not remove every dependency; it concentrates one. There
is no coven service to go down, but availability now rides entirely on the
[storage provider](/docs/storage). If the provider is unreachable, the account is
suspended, or its credentials are revoked, no device can sync until it is restored
— and because the provider is a dumb byte store by design, it cannot mediate,
arbitrate, or enforce anything on coven's behalf when it is up. It holds
ciphertext and serves it back; that is the whole contract.

## What the absence of a server buys

Stated plainly, and kept in balance with the limits above, removing the server is
also what gives coven its properties:

- **Nothing to run or operate.** No backend to deploy, scale, patch, or pay for;
  a store needs only storage the user already has.
- **Provider-agnostic and portable.** The same store works over S3, Google Drive,
  Dropbox, OneDrive, or iCloud, and the data is the user's own — on a
  [browsable home](/docs/encryption#opaque-and-browsable-homes) they can even read
  it in the storage console by name.
- **Works offline.** Every device holds the full database and commits writes
  locally; with no provider connected a store is
  [local-only and complete](/docs/storage#what-the-host-configures), and syncs
  when a provider is attached.
- **The provider never sees plaintext.** On the default opaque home every object
  is [encrypted before it leaves the device](/docs/encryption); the provider holds
  ciphertext and opaque key paths, never a row, a file, or a real identity.
- **Full ownership.** The data lives in the user's storage under their
  credentials, with no operator holding a copy or a key.

These are not separate features bolted onto a serverless design; they are the
same decision as every constraint above, read from the other side.
