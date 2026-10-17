# coven

Coven lets an app keep its data in SQLite on each device and sync it between a
household's devices through cloud storage the household already pays for. There is
no coven server. This document explains how it works and why, for someone deciding
whether the design is right.

Two companion references hold what this document doesn't: [format.md](format.md),
the exact bytes coven writes, and [api.md](api.md), the Rust API an app calls.

Proof links point into [proofs/](proofs/), Lean models checked by machine. Most
proofs are listed in Appendix G, "What's proved", which names each claim, its
theorems, their file and the section they support. The body links a proof only
where it shows why a rule exists, usually a checked counterexample. The proofs
cover the model, not the Rust: a link proves what its theorem states about the
model, under the model's assumptions. Tests compare the Rust against several of
these models (§22).

The examples follow one household. Ana, Ben and Carol share a store called
Household on Dropbox. Ana and Ben also share a circle called Gifts, which Carol
can't read; Carol may have a circle of her own, Recipes. Each has a phone and a
laptop. Dan joins later.

## Contents

- [Part I. What coven is](#part-i-what-coven-is)
  - [1. The problem](#1-the-problem)
  - [2. What you can rely on](#2-what-you-can-rely-on)
  - [3. What coven assumes](#3-what-coven-assumes)
- [Part II. How data moves](#part-ii-how-data-moves)
  - [4. Writing locally](#4-writing-locally)
  - [5. The sequence](#5-the-sequence)
  - [6. A sync pass](#6-a-sync-pass)
  - [7. Order](#7-order)
  - [8. Merging](#8-merging)
  - [9. The rules SQL brings with it](#9-the-rules-sql-brings-with-it)
- [Part III. People](#part-iii-people)
  - [10. Members, roles and devices](#10-members-roles-and-devices)
  - [11. Admin operations](#11-admin-operations)
  - [12. Keys and encryption](#12-keys-and-encryption)
  - [13. Joining and restoring](#13-joining-and-restoring)
  - [14. Removing people and devices](#14-removing-people-and-devices)
  - [15. Circles](#15-circles)
- [Part IV. Keeping storage bounded](#part-iv-keeping-storage-bounded)
  - [16. Snapshots, the floor, and deleting
    history](#16-snapshots-the-floor-and-deleting-history)
  - [17. Files](#17-files)
- [Part V. Change and failure](#part-v-change-and-failure)
  - [18. Schema changes](#18-schema-changes)
  - [19. When things go wrong](#19-when-things-go-wrong)
- [Part VI. Storage](#part-vi-storage)
  - [20. What coven needs from a provider](#20-what-coven-needs-from-a-provider)
  - [21. Costs](#21-costs)
  - [22. What's proved and what isn't](#22-whats-proved-and-what-isnt)
- [Appendix A. SQLite features in synced
  tables](#appendix-a-sqlite-features-in-synced-tables)
- [Appendix B. Coven's merge records](#appendix-b-covens-merge-records)
- [Appendix C. Coven's own schema and storage
  format](#appendix-c-covens-own-schema-and-storage-format)
- [Appendix D. Cryptography and keys
  reference](#appendix-d-cryptography-and-keys-reference)
- [Appendix E. Provider reference](#appendix-e-provider-reference)
- [Appendix F. Crates and conventions](#appendix-f-crates-and-conventions)
- [Appendix G. What's proved](#appendix-g-whats-proved)
- [Appendix H. Errors and statuses](#appendix-h-errors-and-statuses)

---

## Part I. What coven is

Part I says what coven is for, what it promises, and what it takes for granted.
The rest of the document explains how each promise is kept.

### 1. The problem

An app like a music library or a notes app wants three things at once:

- **It works offline.** Ana edits on a plane. Her edit commits immediately, and
  the app never waits for the network.
- **Everyone ends up with the same data.** When Ana lands, her edits reach Ben,
  and Ben's reach her. If they both changed the same thing, every device makes
  the same choice about what wins, and nothing is lost silently.
- **No one runs a server.** The household already has Dropbox, OneDrive, iCloud
  or an S3 bucket. Coven syncs through that, and the provider sees only
  encrypted bytes.

Coven is built for one person's devices or a household of up to about twenty
people; four or five is typical. It is not built for high write rates or large
groups.

The app writes ordinary SQL to its own tables and keeps its own schema. It tells
coven which tables sync. Coven captures each committed change, encrypts it,
publishes it to storage, reads everyone else's, and merges them into the local
database. Rows can carry files, such as songs, photos or documents, which sync
with them.

### 2. What you can rely on

These are the promises. The rest of the document explains how each is kept.

- **Local writes never wait.** A commit needs no network and never waits for
  sync. It can still be refused for local reasons: a SQL constraint, a write into
  a circle the member isn't in, or an audience in the middle of a reload
  (`AudienceReloading`, §16.5).
- **Every device converges.** Two devices that have applied the same history hold
  the same rows, whatever order things arrived in.
- **Conflicts are decided, not hidden.** When two edits can't both stand, one
  wins by a rule every device applies the same way, and the other is kept as a
  *lost value* the app can show and offer back (§8).
- **Only members read the data.** Everything coven stores is encrypted. The
  provider sees object sizes, counts, timing and a few routing facts, never
  names or values. A circle's rows are readable only by its members (§15).
- **Only authorized members change membership,** and the store always keeps at
  least one admin (§10).
- **Removal is final from a point in history.** Once a person or device is
  removed, nothing they send afterwards counts (§14).
- **Committed work survives crashes.** A commit, and every admin operation's
  record of what it has done, outlives a crash or a lost reply.
- **Storage stays bounded.** Old history is replaced by snapshots and deleted,
  without waiting for devices that are away. A device that comes back after a
  year catches up from a snapshot and keeps its unsent edits (§16).
- **Failures are visible.** Anything that can't proceed tells the app what it is
  waiting for (§19.1).

The subsections below give each promise's exact edges: what it covers, what it
doesn't, and what it assumes. A reader who only wants the story can skip to §3.

#### 2.1 Local writes

Ana's commit on the plane needs no network. Neither does a read of data already
on her phone. Two things do need storage: reading a file whose bytes aren't
cached yet (§17.5), and an admin operation, which must first catch up with the
sequence online (§11.1).

Every check a commit makes uses state already applied on the device. Membership,
audience and row-key checks (§9.2, §15) can refuse a write, but none of them
asks the network, waits for a reload, or looks at the state a reload will
install. So a write queued during a reload keeps its original bytes, but not
necessarily its verdict: once the reload installs, the same write can be judged
differently (§16.5).

A commit can be refused for these local reasons:

| Refusal | Why | Where |
|---|---|---|
| A SQL constraint, a trigger, or coven's own SQL access rules | the write breaks the schema or touches what the app may not | §4, §9 |
| Not a member of the row's circle, a bad audience, a bad row key | judged against locally applied state | §9.2, §15 |
| `AudienceReloading` | that audience is being replaced by a reset, breaking-raise or newly-readable-circle reload | §16.5 |
| `MemberKeysChanging` | the member is replacing their own keys | §12.3 |
| `DbError::ClockOutOfRange` | no valid timestamp can be made | §7.2 |
| Representation checks, such as `DbError::TooLarge` | the write can't be stored as a write record; one write's plaintext must fit SQLite's 1 GB value limit | §6.3 |
| File checks | the attached file can't be used | §17.2, [api.md E3](api.md#e3-writing) |
| `RollbackFailed`, database or disk failures | the database itself failed; each keeps its cause | §4 |
| Closed or unusable installation | nothing can be written | [api.md E1](api.md#e1-opening) |

Schema and migration checks can also refuse opening the database or running a
migration (§18).

**Edge cases.**

- **Reloads.** Only three kinds of reload refuse writes: a reset, a breaking
  schema raise, and a circle the device can newly read. While one runs, that
  audience refuses writes with `AudienceReloading` until the reload commits.
  Other audiences stay writable. An ordinary reload, including one because the
  floor has passed the device's next slot, allows app writes throughout (§16.5).
- **A raise not yet published.** An app that has migrated can commit writes
  before its raise has claimed a slot. Those writes wait in its queue (§18.5).
- **File cleanup after commit.** If cleaning up a file fails after the commit,
  the commit's result is still returned (Appendix C.3).
- **Not proved for the code.** No proof shows that the Rust implementation
  avoids every network wait. The promise is a design rule the code must follow.

#### 2.2 Convergence

One applied prefix of the sequence (§5) fixes the membership and the data of
every audience the device can read, at a given schema version. The model proves
this given deterministic admin, merge and conversion rules. In practice:

- timestamps choose which value a cell keeps (§7.2, §8.1);
- generations make an edit concurrent with a delete lose (§8.3);
- the first slot decides who keeps a unique value and who originally created an
  owned row (§9.2, §9.6).

The losers are kept: a replaced original value, and a change dropped because its
data didn't count (§9.5), both stay available as lost values. Two outcomes create
no lost values on purpose: writes refused by a removal (§14.1), and parts ignored
by a reset (§19.4).

A slot's effects and the device's "applied through this slot" commit together
(§6.2). So its verdicts and lost values never show up with only part of the slot
applied. This relies on SQLite's transactions being atomic.

#### 2.3 Confidentiality

Neither the provider nor an outsider can read encrypted contents. Only members'
devices hold the store key, and only a circle's members hold its key. The
provider sees each slot's kind, a key copy's routing, object framing, and the
sizes, counts, timing and accounts §3 lists; §12 says exactly who can read what.
Names, values, key commitments and operation contents stay encrypted.

#### 2.4 Authorization

Signatures identify who wrote each slot. An admin entry is judged against the
state just before its slot. A write is judged against the history its author had
seen (its `read_through`, §7.1) and against the removal cut-off (§14.1). So only
authorized members change membership, and at least one admin always remains
(§10). The Lean models assume the cryptography is secure; they don't prove it.

#### 2.5 Revocation

- **Catch up before sending.** A device applies the sequence before it sends, and
  a first attempt uses the newest key it may use (§12.2, §14.1). The model
  proves that, once caught up, a device never picks for a first attempt a key
  that was copied to a removed member's sealing keys.
- **A block can't be skipped.** A device stuck at a slot stops there (§5.4), so
  it can't read past the block to a later removal and keep sending as if the
  removal hadn't happened.
- **No grace period.** Writes claimed in a slot after their device's or member's
  removal, or after the signing key they were signed with was replaced, have no
  effect. Every device records
  the refusal. A late claim gets no extra time (§14.1).
- **Earlier attempts keep their bytes.** A retry of a write first attempted before
  the removal keeps its original inner bytes and keys (§6.3).
- **Provider access is separate.** Cutting off storage access is provider work
  (§14.2). A key copy already in flight leaves the disclosure window §12.2
  describes.
- **Leaving a circle.** Edits Ben made in a circle before he was removed from it
  still count (§15.8).

#### 2.6 Durability

Commits and operation journals (§5.1, §11) survive crashes, as long as the
database and the key custody (§12.4) stay usable. If an installation becomes
unusable, the person can reset it, and coven reports which unsent edits it
discarded (§19.3).

Queued writes eventually claim a slot, given that their prerequisites succeed,
storage works, and the device isn't always beaten to the next slot. Nothing bounds
how often a device can lose that race.

Admin work either claims its slot or undoes everything its journal recorded. An
undo that fails stays in the journal and is reported (§11.3).

#### 2.7 Storage bounds

Snapshot coverage plus a grace period let history be deleted without waiting for
absent devices. A returning device reloads and keeps the unsent writes that still
count (§16.5, §16.6).

The model gives a deadline. From the moment a prefix of history is ready to
delete, it is deleted within U + G + P + D, assuming:

- **U:** a snapshot is prepared, uploaded and announced successfully within U;
- **G:** the grace period;
- **P:** a successful conditional floor update within P after the grace period;
- **D:** deletion within D after that update.

Every audience involved must supply snapshots that cover the prefix and stay
usable through the floor update. Missing coverage holds deletion back. An
audience with no reader online to supply its snapshot has no deletion deadline
at all.

The bound covers history only. It doesn't limit live rows, lost values, the
evidence needed to check old signatures, or a write rate with no limit.

#### 2.8 Visible failures

A blocked slot, operation, file or cleanup names its first unmet condition to the
local app (§19.1). Work that is still allowed continues meanwhile.

#### 2.9 What the proofs cover

The [sequence proof](proofs/sequence.md) states each result's assumptions and
where its model stops. §22 says what the proofs cover and what they don't, and
Appendix G lists them.

### 3. What coven assumes

- **The provider is honest but curious.** It may read what it stores. It does
  not alter or withhold it. It sees object sizes, counts and timing, and which
  account writes each object. For files and snapshots it also sees which device
  uploaded them, because their paths name that device:
  `<store>/files/<device>/<file>` and
  `<store>/snapshots/<audience>/<device>/<n>`, which also names the snapshot's
  audience ([format.md D10](format.md#d10-paths)). It doesn't see which device
  claimed a slot: that is sealed inside the slot (§12). A thief holding storage
  credentials without the store key may behave worse, but can't read anything.
- **Deletion and withholding can't be prevented.** Whoever controls storage can
  delete it or hold it back, and no design stops that. Coven doesn't recover lost
  or damaged storage; the household starts a new store by hand. The reset in
  §19.4 replaces an audience from a chosen device; it can't rebuild storage.
- **Current members trust each other.** Each has access to the storage and is not
  trying to damage it. An ex-member may keep a copy of the store key they had.
- **A person keeps their restore code private,** on paper or shown on a screen
  (§13.1). Anyone who sees it can open the store, given storage access, while its
  keys are current. Whoever holds that current key pair *is* that member to
  coven, which can't tell them apart. Replacing the keys can't take back old
  keys and ciphertext the pair could already reach; writes made afterwards with
  the old keys fall under the cut-off in §14.1. Coven offers no recovery from a
  stolen code; the household removes that person and starts over by hand if
  needed.
- **The building blocks work:** the ciphers, signatures, hashes, key derivation,
  random number generation and key custody (§12, Appendix D); SQLite's
  transactions being atomic and durable; file and custody writes being durable.
  No Lean model proves the cryptography secure: the models take authentication
  and sealing as given.
- **The provider behaves as required.** Complete reads, create-once that
  publishes atomically, conditional replace, and access cut-off that is confirmed
  (§14.2, §20.1). The models assume these; they don't establish them for real
  providers. Live tests check them (§20.7, §22).

---

## Part II. How data moves

Part II follows one edit from the app's commit to every other device: how it is
recorded (§4), published in the sequence (§5), sent and received in a sync pass
(§6), ordered (§7), and merged, including the rules the app's SQL schema brings
(§8, §9).

### 4. Writing locally

The app declares which of its tables sync; the rest stay on the device. A
*write* is one SQLite transaction that changes synced tables, and a *row change*
is one row's insert, update or delete inside it. SQLite's pre-update hook tells
coven each row change as it happens.

When a write commits, coven records, in the same transaction:

- the rows as they now stand;
- a *write record* for upload, in coven's `_coven_uploads` table: each row change
  with which columns it touched and their old and new values, the write's device
  and number, its timestamp, the schema version it was made with, and how far
  this device had read (`read_through`, §7.1);
- the facts later owner checks need (§9.6): who originally created each owned row,
  which rows the write changed directly, and which foreign-key action caused each
  other change, including actions SQLite folded into a single row change.

So a committed write is never lost, even if the app crashes a moment later.

Writing needs no key and no storage. The write record waits unencrypted and
unsigned; it is encrypted and signed only when it is sent (§6.3). Ana can use the
app before she has set storage up at all ([api.md E1](api.md#e1-opening)).

Here is the write record for a write that fixes a note's title and deletes a tag:

```
ana-phone, write 3, 2026-10-02 12:00:00.000 #0
  read_through: slot 20
  notes  row 42  update  title: "Grocry list" → "Grocery list"
  tags   "errands"  delete
```

A write keeps its identity, device and number (`ana-phone`, write 3), from the
moment it commits. The numbers order one device's writes; they are not storage
paths.

Coven keeps its own bookkeeping in tables inside the same database, which the app
can't read or write. The app reads its own tables normally, on several read-only
connections at once. It can subscribe to a query, which reruns only when rows it
read change. Another process, such as a home-screen widget, can open the store
read-only while the app has it open.

**Edge cases.**

- **Rowids.** In a table whose rowid isn't its primary key, the app can read the
  rowid but can't change it once the row exists. The rowid is SQLite's private
  address for the row: coven doesn't sync or watch it, and VACUUM can renumber it.
- **A callback panics.** Database callbacks give their connections back before
  the panic continues. The unfinished transaction rolls back, and later calls can
  use the writer and the readers again.
- **A rollback fails.** Then the connection may still be inside the transaction.
  The writer would treat its uncommitted changes as committed in its own later
  reads and writes, and a reader would keep reading its old snapshot of the
  database. So every later call and live query on that handle, read-only or not,
  fails with `RollbackFailed` until the store is reopened.
  - A write or check that failed and then failed to roll back reports both
    failures.
  - A read ends by rolling back. If only that rollback fails, the read reports
    it; a read that had already failed keeps its own cause.
  - If a callback panicked, the panic still continues, and the failed rollback is
    logged.

### 5. The sequence

All of a store's history is one numbered sequence of objects in storage:
`<store>/seq/1`, `seq/2`, `seq/3`, and so on. Each object is a *slot*. A slot
holds one of four things:

- **writes:** one device's queued writes, sent together in one pass;
- **an admin entry:** a membership or settings change, such as adding Carol;
- **a key copy:** a key sealed to one member (§12.1);
- **a snapshot announcement:** "a snapshot of this audience, covering slots up
  to n, is at this path" (§16.2).

Every device applies slots in order, 1, 2, 3, and never skips one. Because
everyone applies the same slots in the same order, everyone agrees on what
happened and when. That single agreed order is the backbone of the design: it
decides who was a member when a write was made, which of two inserts of the same
unique value came first, and when history can be deleted.

A device claims slot n+1 only after it has applied 1 through n. It applies each
slot in one database transaction together with "applied through this slot". If
that transaction fails, both stay at the slot before. This rests on SQLite's
transactions being atomic (§3).

#### 5.1 Claiming a slot

To publish, a device *claims* the next slot: it creates `seq/n+1` with a request
the provider accepts only if nothing is there yet. Every supported provider has
such a create-once request (§20.1). If two devices try the same slot, exactly one
wins.

Ana and Ben both have writes to send, and both have applied through slot 40.
Both try to create slot 41. The provider stores Ana's and refuses Ben's. Ben
reads slot 41, applies Ana's writes, and tries slot 42 with the same writes he
had queued. Nobody's work is lost; Ben's simply lands one slot later.

A claim can also go wrong in a less obvious way: the request succeeds, but the
reply never arrives. Ana retries, and the provider says the slot is taken
(`AlreadyExists`). Taken by whom? Ana reads slot 41 and compares it with the
exact bytes she sent. If they match, the slot is hers. Without that comparison
she would claim slot 42 too, and her writes would land twice
([`no_compare_lands_twice`](proofs/sequence/CovenSequence/Claim.lean)).

So before every create, a device records in its local *journal*, its durable
record of unfinished work, what it is claiming. After a crash or a lost reply, it
*settles* that claim first: read the slot, compare, and only then move on.

The rules:

- **Claim numbers.** Each device numbers its claims 1, 2, 3, with one counter for
  every kind of slot: writes, admin entries, key copies and announcements. The
  claim number is sealed inside the slot under the store key. It is separate from
  write numbers.
- **What the journal records.** Before each create: the claim number, the slot
  path, and the exact bytes. An admin operation keeps this record in its own
  journal (§11); the writes queue keeps it with its batch.
- **One claim at a time.** A device settles its current claim before starting
  another, across all its queues and operations. A retry after losing a race
  keeps the same claim number; a new claim takes the next number. A settled
  number is never reused for different work.
- **Settle first.** On reopening or retrying, the device settles before it
  validates again, undoes anything, or tries another slot. An operation that
  forgot its claim could land twice
  ([`forgetful_lands_twice`](proofs/sequence/CovenSequence/Operation.lean)).

Settling means reading the recorded slot and comparing all its bytes:

| What the device finds at its recorded slot | What it means | What the device does |
|---|---|---|
| Its own bytes, even after `AlreadyExists` | the claim landed | checks the floor (§5.2), then treats the slot like any slot it received |
| Different bytes signed by another device | another claim won | applies the new slots, validates the waiting work again, and claims the next slot |
| Different bytes signed with this device's id | another copy of this device exists | reports `SyncError::DeviceIdInUse` and stops sending (§19.3) |
| Nothing, and the floor hasn't passed the slot | not landed yet | retries the same path with the same bytes |
| The read fails | unknown | leaves the claim unsettled and tries later |
| The floor has passed the slot | the slot was deleted | settles from the floor's snapshot (§5.3) |

**Edge cases.**

- **Absence isn't cancellation.** An empty slot doesn't cancel a request still in
  flight, which is why the retry sends the same bytes to the same path.
- **Different bytes alone prove nothing about copies.** Losing a race is normal.
  Only different bytes signed with this device's own id show another copy.
- **What changes after losing.** Claiming a new slot needs a new outer envelope,
  because the signature covers the slot's path (§12). The writes inside keep their
  original bytes.

#### 5.2 Why devices check the floor

Old slots are eventually deleted (§16.6). That creates a trap: once slot 41 is
deleted, its path is empty again, and a create-once request for it can succeed.
A request from long ago, delayed in the network, could land there and look like a
genuine slot to a device that reads it.

The defence is a single record, the *floor*, at `<store>/floor`. It says which
slot is the first one still kept. A device that reads slots checks the floor
after reading them and before applying them. Anything below the floor is not
trusted; the device loads a snapshot instead.

A device that has just claimed a slot checks the floor the same way before
believing its claim. If its slot is still at or above the floor, the bytes there
are genuine. If the floor has passed it, bytes it finds there prove nothing: a
stale create can win a deleted slot
([`stale_create_wins_deleted_slot`](proofs/sequence/CovenSequence/Claim.lean)),
so the device settles from the snapshot instead (§5.3).

Bytes recreated below the floor never change what the sequence means. Deleting
them is recorded cleanup work; if the delete fails, the device keeps that
obligation and tries again.

#### 5.3 Settling a claim after its slot is gone

A device can be away so long that the slot it was claiming has been deleted. It
then settles from the floor's snapshot. For every device, the snapshot records
the greatest claim number it includes from that device, and a hash of that
claim's slot bytes (§16.1). This works for every kind of slot, whether it applied
validly or was refused.

Ana's phone was claiming its claim number 9 when it went offline. The floor's
snapshot records her phone's last claim as number 9 with a hash matching what she
sent, so her claim landed. Had it recorded number 8, the claim never landed: she
reads forward, validates her work again, and claims at the current end with the
same claim number 9. A record above 9, or number 9 with a different hash, means
another copy of her phone is using its identity, for example a restored backup;
coven stops that copy with `SyncError::DeviceIdInUse` (§19.3).

The comparison is between claim numbers, never slot numbers: the device compares
the claim number c it journaled with the snapshot's last claim number L for this
device.

| Snapshot's last claim L for this device, against the journaled claim number c | Meaning |
|---|---|
| L below c | the claim didn't land |
| L equal to c, hash matches the recorded bytes | it landed |
| L above c, or L equal to c with another hash | another copy uses this device id |

The hash matters. Two copies of the same phone can both claim with number 9. The
one that lost would otherwise see "9" and believe the winner's slot was its own
([`copies_fool_number_only`](proofs/sequence/CovenSequence/Settle.lean)).

The claim number matters too. Looking at the data instead can't settle a claim:
later writes or admin entries can erase every visible effect of a claim that
landed ([`state_alone_cannot_settle`](proofs/sequence/CovenSequence/Settle.lean)).

With this journal order and the floor's coverage, settling tells apart a claim
that landed, one absent from the sequence, and one whose slot is still empty. A
settled claim, kept or claimed again after it didn't land, ends in exactly one
slot. Nothing bounds how long that takes or how many races it loses.

#### 5.4 A stuck device stops

A device that can't apply a slot stops there. It may be missing a key, need an
app update to read a newer format, or have read damaged bytes. Until it can apply
that slot it:

- applies nothing after it;
- reloads no audience from the floor;
- sends nothing: no writes, no file parts, no operation retries, no provider
  changes.

It reports why: `KeyUnavailable`, `UpdateRequired`, the read failure, or a
`Refused` reason. The app can still read its data and commit the local writes it
is allowed to.

Stopping is deliberate. Skipping slot 80 could mean missing Ben's removal at 81
while still sending writes Ben's removal should have cut off.

Ben's laptop reads slot 41, a copy of a Gifts key it can't open. A key that
arrives in a later slot can't let it get past 41.

**Edge cases.**

- **Parts a device can't read.** A slot's parts for circles this member isn't in
  need not be opened, but a part the device should read whose key is missing
  stops the whole slot (§15.6).
- **Refused writes don't stop anything.** Writes cut off by a removal or a key
  replacement are consumed without effect and recorded as `WritesRefused` (§14.1).
  The device moves on to the next slot.
- **What the model covers** is set out in §22.

### 6. A sync pass

A device syncs in *passes*. The order inside a pass is what keeps the rules of §5:
a device must have applied everything before it claims, and it must check the
floor before it trusts what it read. A pass does four things, in order:

1. **Read forward.** Read `seq/<last applied + 1>`, then the next, until one is
   missing. On a miss, read the floor.
2. **Apply.** Check each slot's signature, then apply it in one database
   transaction together with "applied through this slot".
3. **Claim.** Settle any claim still in the journal, then send: at most one
   writes slot holding this device's queued writes, plus any admin operations
   the app asked for (§11).
4. **Effects.** Do the work the new state calls for: share or unshare provider
   access, delete S3 keys no longer in use, replace keys a removed member had
   seen, take snapshots, delete history the floor has passed.

A pass over a quiet store, with nothing new and nothing to send, costs two
requests whatever the store's size; §21.1 counts the requests for every other
kind of work.

#### 6.1 Read forward

Ben's laptop has applied through slot 40. It reads `seq/41`, `seq/42`, and so on,
until one is missing.

- **A failed request is an error, never a miss.** Only the provider's "not found"
  counts as absent.
- **Far behind.** A device far behind finds the end quickly by checking n+1,
  n+2, n+4, … and then narrowing the gap. These checks only test existence; they
  apply nothing.
- **On a miss, read the floor.** If the missing slot is below the first kept slot,
  the device *reloads*: it loads the floor's snapshots in place of the history it
  missed, then applies the kept slots after them (§16.5).
- **A failed floor read proves nothing,** neither that the slot is absent nor that
  the device may skip it.
- **No floor yet.** Then the sequence starts at 1, and a miss right after the
  known end means there is no new slot yet.
- **A gap is damage.** A missing slot before a slot known to exist is damage,
  unless the floor has passed it. The device reports the path and stops.
- **A block ends reading.** Once a slot is found that can't apply (§5.4), the
  device reads no further. Requests already started may finish, but their results
  can't move the device past the block.
- **A new device** starts from the floor, or from slot 1 before a floor exists.
  Opening its first key follows the joining rules (§12.1, §13).

#### 6.2 Apply in order

- **Floor before commit.** The device reads the floor after this pass's slot reads
  and before committing them. The floor read at the final miss serves as that
  check. If the floor has passed a slot just read and not yet applied, the device
  discards what it read and reloads (§16.5) instead: bytes recreated at a deleted
  path can't become new history (§5.2).
- **Signature before use.** The device checks each slot's framing and complete
  signature before applying it. It finds the slot's author in the state just
  before the slot. The writes inside are judged against the history their author
  had seen when making them (§7.1).
- **One transaction per slot,** together with "applied through this slot". A
  failed transaction changes neither the data nor the progress.

What applying means depends on the slot's kind:

| Slot | Applying it |
|---|---|
| Admin entry | validate and apply the change (§10.2). Any reload it requires must finish before this slot commits. |
| Writes | merge the batch in write-number order (§8). Writes cut off by a removal or an old key are consumed without effect and recorded as `WritesRefused` (§14.1). |
| Key copy | record its signed routing and the exposure it causes, and open it if this device's member needs it (§12.1, §12.2). |
| Snapshot announcement | check its signed coverage and purpose (§16.2). Applying it reads no snapshot, so a snapshot that an abandoned operation's undo deleted can't stop the device. |

A slot that can't apply stops the device (§5.4): a missing key reports
`KeyUnavailable`, a newer schema or format reports `UpdateRequired`, and a damaged
complete object reports its `Refused` cause. The app still reads its data and
commits the writes it is allowed to. The pending records name the cause (§19.1),
and §6.7 says when the work retries.

#### 6.3 Claim

- **Settle first.** Any unsettled claim is settled before anything else, by the
  byte comparison of §5.1, or from the floor's snapshot if the floor has passed
  its slot (§5.3).
- **Caught up.** A claim at n+1 requires having applied 1 through n. No clock
  deadline expires that progress.
- **Admin operations** validate, journal what they will prepare, then do the work
  needed before their entry (§11 covers finishing and undoing).
- **Journal, then create.** The journal records each claim before its create
  (§5.1). A large writes slot uploads in parts and becomes visible only when
  complete.
- **After contention,** read forward and validate again. If the action is still
  allowed, claim the next slot; otherwise undo the journaled work and fail with
  the reason (§11.1).
- **After a create lands,** the floor check of §5.2 confirms it. A confirmed claim
  is then applied like any slot received.

Writes go out in a writes slot:

- **One per pass.** A pass claims at most one writes slot. It holds this device's
  queued writes that can be sent, in write-number order. Writes the app commits
  after the pass has gathered them wait for the next pass.
- **Writes that can't be sent yet.** A schema raise can leave later writes
  waiting (§18.5). A pass with nothing to send claims no writes slot.
- **No size cap.** A writes slot can be any size; a large one uploads in parts.
  Each single write's plaintext must fit SQLite's 1 GB value limit; otherwise the
  commit itself fails with `DbError::TooLarge`.
- **Fixed before the first send.** Before a write is first sent, the device
  records its format and the keys that seal it. §12.2 chooses the keys;
  [format.md D9](format.md#d9-sealed-objects) keeps the write's signed inner bytes
  the same across retries.
- **Leaving the queue.** Queued writes are removed only once their claim is
  settled and the state that consumed them is committed. The rest of the local
  queue is then applied over that state, and no write is ever applied twice.

Snapshots go out as announcements. The device first uploads the snapshot's
journaled bytes, then claims the announcement. A snapshot captured too early is
replaced as §16.3 says; an upload never announced stays owed cleanup (§16.2).

#### 6.4 Effects

After applying and claiming, the device does the work the new state calls for:

- **Provider sharing** follows the active members and open invites (§14.2,
  §20.9). A grant or revocation that fails stays pending with its cause.
- **S3 keys** no longer current become deletion work. A key's record stays until
  its deletion is confirmed (§14.3).
- **Exposed keys** are replaced through the admin operation path. Only a member of
  that audience can supply its new key (§12.2).
- **Reloads.** A reset, a breaking schema raise or a newly readable circle
  requires a reload. The device installs the reloaded data together with the
  slot that triggered it, or leaves that slot pending (§16.5).
- **Snapshots** are taken when enough has been written since the last one
  (§16.2).
- **Deleting history.** The device writes the new floor conditionally, then
  deletes the slots below it (§16.6).
- **Deleting files** needs §17.8's reference checks. Once a removed device's
  provider access is confirmed cut off, its whole file folder may be scanned for
  deletion.

At the end, the device's status says how the pass went
([api.md E5](api.md#e5-storage-and-sync)). `Blocked` names the slot that stopped
the pass. `Synced` means the pass reached the end of the sequence; files or
provider work may still be pending.

#### 6.5 Reading an object

- **One stream, checked before use.** A whole object is read as one bounded
  stream and authenticated before anything uses it. No slot or snapshot commits
  before its complete signature verifies.
- **Snapshot prefixes.** Reading only the start of a snapshot takes at most two
  bounded range requests; reading the rest takes at most one more stream.
- **Writes come inside their slot,** with no request of their own. If a provider
  splits a stream into several requests of its own, each counts (§21.1).
- **A broken connection is a failed read.** It can never be taken as proof that
  bytes which haven't all arrived are bad, and so never leads to a permanent
  refusal (§19.1).

#### 6.6 Files travel outside the pass

Ana adds a 300 MB recording to a note. Her write commits at once, and her next
pass sends it. The recording uploads on its own schedule, through a separate
upload worker, so a pass never waits for a file. §17.7 gives the rules for
uploads, including how they resume and when they stop.

#### 6.7 Waiting and retrying

A device that polled storage every second would cost requests for nothing, and
one that never woke would miss Ana's edits. So a device sleeps until something
can have changed.

- **What wakes it:** work from the app, a changed prerequisite, or a retry that
  is due. There is no unconditional one-second scan.
- **Idle passes** run every 30 seconds, subject to the provider's cooldown and the
  connection backoff. App work that arrives during a pass leaves a wake for the
  next one.
- **Bookkeeping doesn't wake it.** Queue and operation bookkeeping never asks for
  another pass. The model checks that this loses no write, even when passes race.
- **A wait for the app or an update** makes no automatic request: a wait for an
  app action, or for an app update, sits until that happens.
- **Join requests** are polled as §13.2 describes.

Ana's invite has no join request yet, so polling for one backs off. Once Dan's
request arrives, waiting for Ana to decide sends no repeated request.

**Retry timing.** Automatic waits, including those of operations, invites,
joining, missing keys and files, share one schedule of delays: 1, 2, 4, …, 256
seconds, then 300.
Unrelated commits and finished passes don't reset the delay.

- **Deadlines survive restarts.** Before waiting, coven saves with the work the
  wall-clock deadline T and the full wait W. It measures the delay with a clock
  that wall-clock changes don't move.
- **Reopening** waits `max(0, min(T - now, W))`, keeping T, W and the backoff step
  until the next attempt. Ana restarts with four seconds left of eight and waits
  four. A clock jumped backward allows at most eight; one jumped forward can cut
  it to zero. This bounds waits across restarts, not real-time cooldowns under
  arbitrary clock jumps.
- **Retry-After.** A provider's or account's cooldown (`Retry-After`) can lengthen
  the wait, and can exceed five minutes. It is saved separately, at the scope it
  applies to, before any affected worker retries. It applies to every request it
  affects, including retries inside the provider's SDK and explicit retries from
  the app. Restarting, `sync_now` and the app's retry never discard it.
- **A failed save.** If saving the delay fails, the work that started it fails and
  won't retry on its own.

### 7. Order

Two things order writes, and they answer different questions:

- **causality** decides when a device may apply a write;
- **timestamps** decide which write a cell keeps.

#### 7.1 Causality

Causality answers "what had the author seen?" Each write records `read_through`,
the last slot its device had applied when it was made. Its *past* is every write
in slots up to `read_through`, plus the same device's earlier writes. Slot zero
means it had read no published history.

That record never changes, even if the write is sent much later. Catching up
doesn't change it, and claiming a later slot doesn't enlarge it.

Ana creates note 43 in slot 20. Ben reads through 20, then queues an attachment
to it. Whatever slot Ben's write lands in comes after 20 on every device, so
every device has note 43 before the attachment.

Ben and Carol both edit while at slot 20: their edits are *concurrent*, even if
Ben's lands in slot 21 and Carol's in slot 30, because neither saw the other.
Their values for the same cell still compete by timestamp (§7.2).

Applying slots in order is enough to respect causality. The writes in one slot
are in their author's write-number order, so everything a write saw is in an
earlier slot, or is an earlier write of the same batch.

Each write is checked against its author's view:

- **Signature.** A write is checked with its device owner's signing key as it
  stood at the write's `read_through`. That state must authorize the device, the
  member, and each circle part. If evidence is still missing, the device waits; a
  bad signature or missing authority refuses the slot.
- **The first device's offline writes.** Writes the first device made before
  setting up storage have `read_through` 0 and are checked against slot 1's
  record of the store's creation. No other device can write at zero.
- **Cut-offs are separate.** Writes cut off by an earlier removal or member-key
  replacement are consumed by §14.1's rule, without their data showing up as
  lost values.

**Edge cases.**

- **A consumed cause may have no effect.** A cause still counts as seen even if
  its data didn't stand: changes the schema excluded, parts a reset ignored, and
  writes refused by the cut-off (`WritesRefused`, §14.1) keep their verdicts.
  Whether a later change is dropped depends on the data it actually built on
  (§9.5).
- **The local view.** The app sees its own commits before they claim slots. The
  device shows the published slots plus its local queue on top; it never pretends
  an offline write saw slots that arrived after it was made. Once the same writes
  are published, every device uses the same slot order and the same authored
  pasts.

#### 7.2 Timestamps

Timestamps answer "of two concurrent edits to the same cell, which wins?" Every
write carries a timestamp: the wall-clock time in milliseconds, a counter, and the
device's id, compared in that order. Ana's write 3 is stamped
`2026-10-02 12:00:00.000 #0`: 12:00:00.000 is the time, #0 the counter.

| Part | Size |
|---|---|
| milliseconds since 1970 | 48 bits |
| counter | 16 bits |
| device id | 64 bits |

No two devices ever produce equal timestamps, because device ids differ. Every
new installation and every device reset gets a new device id (§10.4, §19.3).
Restoring a backup on the same installation can keep its id, when key custody is
intact and the journal check of §19.3 finds no sign of another copy.

A device never stamps a write earlier than anything it has already seen. If its
clock is behind the latest timestamp it knows, it reuses that time and raises the
counter. So Ben's edit made after reading Ana's always beats Ana's, even if Ben's
clock is slow.

The stamping rule:

- Each device keeps, on disk, the latest timestamp it has seen, from its own
  writes and from every write it applies. A write waiting for a key or an app
  update isn't seen until it applies.
- To stamp a new write: if the wall clock is past that latest time, use the wall
  clock with counter 0. Otherwise use the latest time with the counter raised by
  one. A counter past its maximum moves to the next millisecond.
- So a new write is always stamped later than everything its device had seen,
  whatever the wall clocks say.

A write applies as soon as its causes and keys are available, whatever its
timestamp says about the receiving device's clock. Loading a snapshot adopts its
writes' timestamps under the same rule. Neither loading snapshots nor deleting
history waits on the wall clock.

A device whose clock jumps a year ahead drags everyone's timestamps forward
until real time catches up, but nothing freezes. Ana's phone jumps a year ahead
and edits note 42. Ben's laptop applies that edit as soon as its causes arrive.
Ben's next edit is stamped after Ana's, even with his clock set right. Carol's
concurrent edit, made without reading Ana's, has the earlier stamp and loses;
its value is kept as a lost value (§8.2). Until real time catches up, devices
that have seen the future stamp keep using that time with rising counters.

**Edge cases.**

- **A clock before 1970** is never past the latest timestamp, so the write takes
  the latest time with its counter raised.
- **The end of the range.** A wall clock past the last time 48 bits hold, in the
  year 10889, fails the write with `DbError::ClockOutOfRange`. So does a latest
  timestamp already at that time with its counter at the maximum.

#### 7.3 An example

- At 13:00, Ana's phone, whose clock is fast, stamps write 4 at
  13:01:00.000 #0. It changes "Grocery list" to "Groceries", with `read_through`
  20, and claims slot 21.
- Ben reads slot 21 at 13:00:20. At 13:00:30 his phone stamps write 9 at
  13:01:00.000 #1, setting "Weekly groceries", with `read_through` 21. He claims
  slot 22.
- Carol reads 21, then 22. Ben's value wins over Ana's because of his counter,
  even though his wall clock was behind.

An offline edit concurrent with these would still compete by its timestamp, not by
the slot it eventually lands in.

### 8. Merging

Applying a write means merging it into the local database. Every device that has
applied the same writes ends with the same database, whatever order they arrived
in. That includes coven's own merge records, not only the app's tables.

A device merges every write, its own included. It applies a write only once it has
applied every write that write had read (§7.1), so a write never lands before
something it built on.

Merging has two layers:

- **The merged state** comes from the writes alone: each row's generation (§8.3)
  and each cell's winning value (§8.1).
- **The removal rules** then decide which rows the app sees, from the merged state
  and the admin entries the device has applied (§8.4, §9).

This section covers the merged state and how a row is taken out and comes back.
§9 covers the rules the app's SQL schema adds. Appendix B lists the internal
tables a device keeps for all of this.

#### 8.1 Cells

A *cell* is one column of one row: note 42's title. Of two concurrent values for
a cell, the one with the larger timestamp stays. A cell only one write touched
keeps that write's value. So if, at 15:00, Ben retitles note 42 while Ana,
offline, edits its body, both edits survive on every device:

|                | title              | body                |
|----------------|--------------------|---------------------|
| Ana's write 6  | ·                  | "milk, eggs, bread" |
| Ben's write 10 | "Weekend shopping" | ·                   |
| result         | "Weekend shopping" | "milk, eggs, bread" |

All the cells one write sets share its timestamp. So between any two writes, the
one with the larger timestamp wins every cell they both set.

**An example in full,** continuing §7.3. Note 42 is titled "Grocery list" (Ana's
write 3). Carol's tablet goes offline at 12:30, having applied through slot 20,
including Ana's write 3 and Ben's write 8.

- At 13:00 Ana's phone, whose clock runs fast, changes the title to "Groceries"
  (write 4, stamped 13:01:00.000 #0) and claims slot 21.
- Ben reads slot 21. At 13:00:30 his phone changes the title to "Weekly
  groceries" (write 9, stamped 13:01:00.000 #1, `read_through` 21) and claims
  slot 22. His counter beats Ana's stamp even though his clock is behind (§7.2).
- At 14:00 Carol, still offline, retitles it "Shopping":

  ```
  carol-tablet, write 2, 2026-10-02 14:00:00.000 #0
    read_through: 20
    notes  row 42  update  title: "Grocery list" → "Shopping"
  signed with Carol's key
  ```

- At 14:30 her tablet comes back, reads slots 21 and 22, and uploads write 2 to
  `<store>/seq/23`. Its `read_through` stays 20.

Carol's write never read Ben's, and Ben's never read Carol's. They are
concurrent, so only their timestamps can order them. When a device applies a
write that sets the title, it compares the write's timestamp with that of the
write that set the title's current value, and takes the new value if its
timestamp is larger.

Ben's phone, which was online:

| Time  | Applies         | New stamp   | Current stamp | Wins | Title              |
|-------|-----------------|-------------|---------------|------|--------------------|
| 13:00 | Ana's write 4   | 13:01:00 #0 | 12:00:00 #0   | yes  | "Groceries"        |
| 13:00 | its write 9     | 13:01:00 #1 | 13:01:00 #0   | yes  | "Weekly groceries" |
| 14:30 | Carol's write 2 | 14:00:00 #0 | 13:01:00 #1   | yes  | "Shopping"         |

Carol's tablet, offline from 12:30 until 14:30:

| Time  | Applies       | New stamp   | Current stamp | Wins | Title      |
|-------|---------------|-------------|---------------|------|------------|
| 14:00 | its write 2   | 14:00:00 #0 | 12:00:00 #0   | yes  | "Shopping" |
| 14:30 | Ana's write 4 | 13:01:00 #0 | 14:00:00 #0   | no   | "Shopping" |
| 14:30 | Ben's write 9 | 13:01:00 #1 | 14:00:00 #0   | no   | "Shopping" |

The two devices applied the same three writes in opposite orders, and both end
with "Shopping": each cell holds the value of the largest-stamped write applied.
Appendix B.2 shows the same steps in coven's internal tables.

#### 8.2 Lost values

"Weekly groceries" didn't survive, and no one who replaced it had seen it. Coven
records it as a *lost value*: which cell, the value, which write set it, and which
write replaced it. The app reads lost values through `lost_values`
([api.md E4](api.md#e4-reading)), can show one ("Ben's title was replaced") and
can offer to restore it.

The rule: **a value is lost when some write replaced it, and no write that
replaced it had read it.** A value replaced by someone who had seen it is not
lost: that was an edit, not a conflict. Note 42's title went through four values:

| Value              | Set by          | Replaced by            | Lost?                 |
|--------------------|-----------------|------------------------|-----------------------|
| "Grocery list"     | Ana's write 3   | Ana 4, Ben 9, Carol 2  | no: all had read it   |
| "Groceries"        | Ana's write 4   | Ben 9, Carol 2         | no: Ben 9 had read it |
| "Weekly groceries" | Ben's write 9   | Carol 2                | yes                   |
| "Shopping"         | Carol's write 2 | nothing                | the current value     |

On Carol's tablet, Ana's write 4 arrives after Carol's write 2, so "Groceries" is
lost at first. Ben's write 9, which had read it, then arrives and clears that
record. Every device ends with one lost value:

```
lost values
  cell            lost value          set by          replaced by
  note 42 title   "Weekly groceries"  Ben's write 9   Carol's write 2
```

Every device ends with the same lost values, because they follow from the writes
alone.

**Edge cases.**

- A value stops being lost only when a later write that had read it replaces it
  in place: sets the same cell in the same generation of its row (§8.3), or
  deletes that generation. Changing another cell of the row, or writing to the
  row's next generation after a delete, leaves it lost. Without this, a value
  could be cleared by a write that never touched it.
- Lost values show what was written. Deleting a reference's parent doesn't change
  them, including the values of a row that was taken out (§8.4, §9.1).

#### 8.3 Deletes

Each row has a *generation*: 1 when created, 2 when deleted, 3 if added again,
and so on. It is odd while the row exists and even while it is deleted. Each row
change records the generation the row had on the device that made it:

```
ben-phone, write 11, 2026-10-02 16:00:00.000 #0
  read_through: 20
  notes  row 43  generation 1  update  title: "Hardware store" → "Hardware store, Saturday"
signed with Ben's key
```

Editing a row's columns never changes its generation. Deleting a row removes it
from the app's table and moves its generation to the next even number. Adding it
again moves it to the next odd number. Its cells go with it, but coven keeps a
record of each generation and the write that moved the row there, so later writes
to the row still have a generation to lose to.

**An edit made at a generation that has since been deleted loses, whenever it
arrives.** Ana deletes note 43 at 16:00; Ben, offline, edits its title; Ana
re-adds note 43 at 17:00. On Carol's tablet:

| Time  | Generation | Moved there by     | Note 43 in `notes` |
|-------|------------|--------------------|--------------------|
| 14:45 | 1          | Ana's write 5      | present            |
| 16:00 | 2          | Ana's write 7      | gone               |
| 17:00 | 3          | Ana's write 8      | present again      |

Ben's edit was made at generation 1, so it doesn't come back to life in
generation 3, even if his clock stamped it later than 17:00. It becomes a lost
value, replaced by Ana's delete. Generation 2's record names Ana's write 7, so
even a device that gets Ben's edit after the re-add records it as replaced by
write 7. Every device records:

```
lost values
  cell           lost value                  set by          replaced by
  note 43 title  "Hardware store, Saturday"  Ben's write 11  Ana's write 7
```

Either arrival order gives that result:

- If the edit arrives after the delete, coven sees it was made at an older
  generation.
- If it arrives before, the delete records as lost the cells set by writes it
  hadn't read, before the row goes.

So a concurrent edit never brings a deleted row back, and never reaches it once
it is re-added.

**Edge cases.**

- **Two concurrent deletes** of the same generation both move it from 1 to 2, so
  they count as one. A value is lost only if neither delete had read it (§8.2).
  The first delete to arrive records what it hadn't read; if the second had read
  one of those values, it clears that record, which needs nothing from the deleted
  row. The generation's record names the delete with the smaller timestamp,
  whichever arrived first. E.g. Carol retitles note 43, and Ana and Ben, both
  offline, delete it; Ben had read Carol's edit and Ana hadn't. A device that gets
  Ana's delete first records Carol's title as lost, then clears it when Ben's
  delete arrives. Every device ends with note 43 deleted and Carol's title not
  lost.
- **A delete concurrent with the re-add's generation.** If Ben had deleted note 43
  instead of editing it, Ana's 17:00 re-add would still bring it back: Ben deleted
  the same generation she did.
- **Concurrent re-adds** both move the generation from 2 to 3, and their cells
  merge as in §8.1.

#### 8.4 Rows taken out, and rows that come back

Some conflicts can't be settled cell by cell. Coven settles them by *taking a row
out*: removing it from the app's table while a reason holds, and recording it as
lost with all its values. It is never stored as a delete, so it comes back if the
reason goes away. The reasons are:

- **Its parent was deleted** after it pointed at it, under a cascade, restrict or
  no-action foreign key; or its parent was itself taken out, under any action
  (§9.1).
- **Its merged values fail a CHECK** (§9.3).
- **Its circle was deleted** by an admin entry the device has applied (§15.9).
- **It lost a unique value** to an earlier claim (§9.2).
- **The same key is present in two audiences** (§15.4): the store's row wins
  over a circle's.

While a row is out, its lost-value record keeps its values, and later edits to
the row update them. Coven puts the row back from there when the reason goes away,
e.g. when the reference that made it a deleted parent's child is pointed
elsewhere. When a write changes which rows are out, coven makes the change in the
app's table with ordinary SQL, which the app's triggers see like any other change.

A row can be out for several reasons at once, and comes back only when none
holds. Its record names every reason that holds once the rules have run. Todos
need `start <= end`, and todo 7 is in list 3. Ana deletes list 3 while Ben moves
todo 7's start past its end. Todo 7 is out for both reasons on every device,
whichever rule a device ran first.

**Dismissing.** The app can dismiss a taken-out row for good
([api.md E4](api.md#e4-reading)). The dismissal is a write that records a real
delete of the row, so it never comes back, and its children follow their foreign
keys' delete actions (§9.1). The dismissed lost value is dropped.

**Inserting the same key again.** The app can't see a row that is out, so it can
insert the same shared key again (§9.2). The write records that insert as an
update of the row that is out, setting every column. The row comes back if the new
values clear its reasons; otherwise it stays out, like any write to a row that is
out. E.g. the tag "urgent" is out because it fails a CHECK; Ana adds "urgent"
again with values that pass, and every device puts the row back with her values.

This is how an app puts a taken-out row somewhere else, in its circle or in the
store. A copy under a new key would show twice if the original's reason later went
away; the same key shows once (in two audiences, the store's row wins).

**Edge cases.**

- **A local insert can still lose.** SQLite can't check against rows that are
  out. E.g. tags have shared keys and unique labels, and tag b, a child of tag a,
  claimed the label "Plan" first: a loses it, and b goes with a. Adding a again
  with "Plan" loses the same way, and both stay out.
- **Unique-value and two-audience losers** count their rule as holding from the
  step that judged it (below).
- **The order the rules run in.** Every rule except unique values and keys in
  two audiences keeps firing when more rows are removed, so applying those in
  any order ends with the same rows out. The two exceptions are judged once,
  between two passes of the others:
  1. apply the other rules until none fires;
  2. judge unique values, and keys present in two audiences, among the rows still
     present;
  3. apply the other rules again, to the losers' children.

  So every device that applied the same writes sees the same rows, whatever order
  the writes arrived in and whatever order the rules ran in. The machine-checked
  merge model must establish this for the merged state and every removal rule,
  including rows hidden by a circle-deletion slot. The
  [merge write-up](proofs/merge.md) explains the model.

### 9. The rules SQL brings with it

The app's schema has foreign keys, unique constraints, CHECKs and triggers. Each
device's SQLite enforces them on its own writes. Merging writes made on different
devices can break them, so coven decides each case the same way everywhere.

Some schemas can't be decided the same way everywhere. Coven refuses those when
the database opens and again after every migration, with an error naming the
table and constraint. Appendix A lists each SQLite feature and what coven does
with it. Appendix B.5 says how coven names foreign keys, unique constraints and
CHECKs, since SQLite gives most of them no lasting name.

#### 9.1 Foreign keys

A foreign key makes one row point at another, its *parent*. Attachment 9's
`note_id` points at note 43, so note 43 is attachment 9's parent.

Ana deletes note 43 while Ben, offline, adds attachment 9 to it. On Ana's device
SQLite already ran the foreign key's action for the attachments it knew about.
Attachment 9 it never saw.

**Parents apply before children.** For changes in different writes, every device
consumes the parent's insert before it processes a change that points at it,
through causality alone (§7.1). When both changes count, it applies them in that
order:

```
Ana's phone    write 5: insert note 43
                  │
                  │  Ben's phone applies write 5, then Ana's write 6
                  ▼
Ben's phone    write 12: insert attachment 9, pointing at note 43
               read_through: 20
                  │
                  │  another device downloads Ben's write 12 first
                  ▼
that device    holds Ben's write 12
                  → applies Ana's write 5, inserting note 43
                  → applies Ana's write 6, and Ben's earlier writes
                  → applies Ben's write 12, inserting attachment 9
```

A row change that points at a parent also records the parent's generation (§8.3).

**On the device that deletes the parent,** SQLite runs each child's foreign-key
action, and the write records the results as *derived* row changes, with their
causes (§9.6). They merge by the ordinary generation and cell rules. Attachments
point at notes with cascade, links with set null. Carol added attachment 8 to
note 43 at 15:30, and every device has it. At 16:00 Ana deletes note 43:

```
ana-phone, write 7, 2026-10-02 16:00:00.000 #0
  read_through: 20
  notes        row 43  generation 1  delete
  attachments  row 8   generation 1  delete
  links        row 5   generation 1  update  note_id: 43 → null
signed with Ana's key
```

A `RESTRICT` or `NO ACTION` key makes SQLite refuse that delete instead, if the
deleting device has a child.

**A child the deleting device didn't have,** because it was added or pointed at
the parent concurrently, points at a deleted generation on every device. When
attachment 9 arrives where note 43 is gone, coven follows the foreign key's
declared action:

- **cascade, restrict or no action:** attachment 9 is taken out (§8.4), as SQLite
  would have done or refused, and recorded as lost;
- **set null:** it stays, and its reference reads as null, as SQLite would have
  made it. Nothing is recorded as lost.

**Children come back when their reason goes.** A taken-out child's own generation
never moves, so it comes back if its reference is later pointed at a parent that
is present. At 16:00, while Ana deletes note 43, Ben, offline, adds attachment 9
to it, then moves it to note 44:

```
Ben's phone
  16:00  offline. Ben's write 12: insert attachment 9 → note 43
  16:05  Ben's write 13: attachment 9 → note 44
  16:30  online. Applies Ana's write 7: delete note 43
           attachment 9 points at note 44, so no rule applies

Carol's tablet
  16:00  applies Ana's write 7: delete note 43
  16:30  Ben's write 12 arrives: attachment 9 → note 43
           note 43 is gone, so attachment 9 is taken out, and recorded as lost
         Ben's write 13 arrives: attachment 9 → note 44
           the reason is gone, so attachment 9 is put back
```

Both devices end with attachment 9 on note 44, and nothing lost. Had Ben not moved
it, both would end with attachment 9 taken out and the same lost-value record, so
the app can offer to put it on another note by inserting it again with its key
(§8.4).

**Re-adding a parent** doesn't bring back the rows taken out with it under
cascade, restrict or no action, since they point at its old generation. Re-adding
is a new insert; any children the app wants back, it inserts in the same write.

**Edge cases.**

- **Set null in detail.** The app's table reads the reference as null, but the
  cell still names the write whose reference won. Later writes to the cell compete
  with that write's timestamp, as with any cell. Coven's merge records keep the
  reference as written, with its parent's generation: the cell reads as null while
  that generation is deleted, and as written again if a reset brings the
  generation back (§19.4). A later concurrent write that displaces it records the
  written value as lost, whichever write arrived first. Lost values and snapshots
  always carry the reference as written, without the null. E.g. links point at
  notes with set null:

  ```
  16:00  Ben adds link 6 → note 43
  16:10  Dan, having read Ben's write, points link 6 at note 44
  16:20  Ana, having read neither, deletes note 43
  ```

  A device that gets Ana's delete and Ben's insert first reads link 6 as null,
  with the cell still naming Ben's write. Dan's write then wins over Ben's, so
  every device ends with link 6 on note 44, whatever order the writes arrived in.
- **Set null that would fail a CHECK.** The child is taken out, as under restrict.
- **A parent taken out, not deleted.** A child whose parent is taken out by a rule
  is taken out with it under every action, set null included, and comes back with
  it. E.g. note 46 loses its title to note 45 and is taken out; link 7, which
  points at note 46 with set null, is taken out with it, not set to null.
- **References of several columns** merge as one value. Changing any component
  records every component, including unchanged ones, with the same parent and
  generation on each. The write with the larger timestamp wins the whole tuple,
  including a tuple containing NULL. For `FOREIGN KEY(a,b)`, concurrent changes
  from `(A,3)` to `(B,3)` and to `(A,5)` give `(B,3)` or `(A,5)`, never `(B,5)`.
  References whose columns overlap grow together until every touched reference is
  complete, and a generated component brings its input columns into the group, so
  SQL can't compute a different tuple from inputs merged one by one.
- **How coven takes rows out.** It runs with SQLite's foreign keys enforced and
  takes children out first, so SQLite's own actions reach only local children:
  rows in tables that don't sync.
- **Removals caused by admin entries,** such as a circle's deletion, use the same
  local foreign-key actions. A final circle deletion can't later restore those
  local children; the app's own writes still follow the ordinary local schema
  rules.
- **Anything else in the local schema that refuses a delete,** such as a trigger
  that raises an error, fails the write or the apply that needs it on that device,
  until the app changes its schema.

**What the schema may declare.** Coven checks these when the database opens and
after migrating, and refuses with an error naming the table and foreign key:

| Refused | Why |
|---|---|
| `ON DELETE SET DEFAULT` or `ON UPDATE SET DEFAULT` on a foreign key of a synced table, or from a local table into a synced one | the default parent merges like any row and can itself be gone (Appendix A.8) |
| set null on a `NOT NULL` column, in any table | SQLite could never apply it: no device could delete the parent, and coven couldn't take the child out |
| set null on a primary-key column | nulling a key column changes the row's key: a delete plus an insert no write recorded |
| a foreign key of a synced table that doesn't reference the parent's whole primary key: a `UNIQUE` non-key column, part of a composite key, or a repeated target column (`SchemaError::ForeignKeyNotPrimaryKey`) | a row's identity and generation can't track a value that changes (Appendix A.9). An explicit column list may reorder the key's columns; an omitted list means primary-key order |
| a foreign key of a synced table that is one generated column (`SchemaError::GeneratedForeignKey`) | below |
| a foreign key from a local table into a synced table, or into a local table one of those reaches, whose delete action isn't cascade, or set null on a column that allows NULL | below |
| a foreign key from a local table into a synced table's non-key column whose update action isn't cascade or set null (`SchemaError::LocalChildUpdate`, naming the table and column) | below |

*Generated references.* Coven merges a generated column's inputs cell by cell. Ana
setting `a` while Ben sets `b` would make `ref AS (a || b)` name a parent neither
write named, and the foreign key would fail on every device
([`unwritten_reference`](proofs/merge/CovenMerge/GeneratedReference.lean)). A
reference of several columns brings its generated components' inputs with it, so
every reference a device shows is a tuple one write set.

*Local children.* A synced row that coven deletes or takes out can have children
only this device has, in local tables, so a local foreign key must never stop it.
`pins(note_id REFERENCES notes ON DELETE RESTRICT)` is refused: Ben's phone,
holding a pin, couldn't take out a note that Ana's phone could, and the two would
differ.

Coven applies another device's change to a synced non-key column as an `UPDATE`,
so a local foreign key must follow that update too. A local column such a key
rewrites passes the update on, so a local key into that column must follow it the
same way. `pins(code REFERENCES parents(code) ON DELETE CASCADE)` is refused,
because its update action defaults to no action. Ana's phone, holding a pin on
code `'a'`, could never apply Ben's renaming it to `'b'`: every attempt fails
SQLite's foreign-key check at commit and rolls back to the same state
([`stuck_forever`](proofs/merge/CovenMerge/LocalUpdateAction.lean)). Cascade and
set null keep every local reference valid along the chain. A key column changes
by a delete and an insert (§9.2), which the delete rule covers.

#### 9.2 Keys and unique values

Each synced table declares one of two kinds of primary key:

- **independent:** each new row gets a random UUID, so rows made on different
  devices never collide (notes);
- **shared:** the app derives the key from what makes the row unique, so the same
  thing made on two devices is one row (the tag "urgent").

Ana and Ben, both offline, each add the tag "urgent". Both derive the same key, so
their inserts are one row, and their cells merge (§8.1).

Either kind can span several columns. `note_tags(note_id, tag_id)` is a shared
key: Ana and Ben, both offline, each tag note 42 "urgent", and make one row. An
independent key over several columns needs a UUID in one of them. Apps bind
independent keys with `UuidText` ([api.md E3](api.md#e3-writing)); an insert of
text that isn't a canonical lowercase UUIDv4 or UUIDv7 is refused. Opening the database
never scans rows to check this.

Coven refuses, when the database opens and after migrating:

- a key SQLite picks itself, such as an integer rowid alias, because two devices
  would pick the same number for different rows. An integer key derived from the
  row's content is not one of these, and is allowed;
- a synced table with no primary key;
- an independent key with no text column to hold its UUID;
- a primary-key column that allows NULL. SQLite lets a non-integer key column hold
  NULL unless it is declared `NOT NULL`, and a key with a NULL in it names no row;
- a column that declares a collation other than `BINARY` when it is:
  - a primary-key column of a synced table;
  - a column of a synced table's unique constraint or unique index (a unique
    index term may not declare one either);
  - a column of a foreign key of a synced table, or of a foreign key from a
    local table into a synced table;
  - a parent column such a foreign key references.

  Coven compares key and unique values by their bytes
  ([format.md D3](format.md#d3-ordered-keys)), as the merge model does. Under
  `COLLATE NOCASE`, SQLite would hold "Urgent" and "urgent" as one row that coven
  treats as two. Other columns may use any collation.

**Shared keys and deletes.** With shared keys, a device can insert a row another
device is deleting. Ana deletes the tag "urgent" while Ben, offline and never
having received it, adds "urgent". Ben's insert, made at generation 0, would start
generation 1, which Ana's delete has ended, so it loses (§8.3), and his cells
become lost values.

**Key changes.** Changing a row's primary key is a delete of the old row plus an
insert of the new one, on every device. On the device changing the key, SQLite
runs each child's `ON UPDATE` action, and the write records the results as
ordinary row changes. A child that device didn't have sees its parent deleted, and
follows its `ON DELETE` action (§9.1).

`note_tags(note_id, tag_id)` points at tags with `ON UPDATE CASCADE` and
`ON DELETE CASCADE`. At 16:00 Ana renames the tag "urgent" to "important", while
Ben, offline, tags note 44 "urgent":

```
Ana's phone
  16:00  renames "urgent" to "important"
           SQLite changes note_tags (42, "urgent") to (42, "important")
           her write: delete tag "urgent", insert tag "important",
                      delete (42, "urgent"), insert (42, "important")

Ben's phone
  16:00  offline. Ben's write: insert note_tags (44, "urgent")
  16:30  online. Applies Ana's write
           "urgent" is deleted, so (44, "urgent") is taken out

Carol's tablet
  16:00  applies Ana's write
  16:30  Ben's write arrives: insert note_tags (44, "urgent")
           "urgent" is deleted, so (44, "urgent") is taken out
```

Every device ends with note 42 tagged "important", and note 44's "urgent" tag
taken out and recorded as lost, so the app can offer to tag it again. Two devices
changing one key concurrently are two concurrent deletes of its generation
(§8.3), and both new rows exist after the merge.

**Unique values.** On one device, SQLite refuses a write that repeats a unique
value, so two rows can claim one value only through concurrent writes. Ana and Ben
both create a note titled "Groceries". On each device that was fine. When both
arrive, **the first slot wins**: Ana's note 45 claimed slot 40 and Ben's note 46
slot 41, so Ana keeps the title and Ben's note is taken out and recorded as lost.
That holds even if Ben wrote his note earlier, while offline. Ben's phone reads
slot 40 before sending his note, and his local note loses "Groceries" right then.
If Ana later renames hers, the title goes to the earliest remaining claimant:
Ben's note comes back.

Slots decide this, not timestamps, so a device with a wrong clock can't steal a
value by backdating. Timestamps still choose the cell values first; uniqueness is
judged on the merged values. A write still waiting in a device's queue has no slot
yet and ranks after everything published, in local write order, so a row created
locally can lose its unique value before it claims a slot.

The rules in full:

- Uniqueness is judged among rows still present after the first pass of the other
  removal rules (§8.4).
- A row's *claim* on its value is the greatest (slot, write number) among the
  writes that set the constraint's current inputs, at the point the complete value
  was established. The lowest claim keeps the value. Within one slot, the lower
  write number wins; within one write, the lower primary key wins.
- The other row is taken out, whether its write inserted it or edited it to claim
  the value. A change to its own value that ends the conflict clears that reason.
- When the winner gives the value up, it goes to the earliest live claimant still
  standing, including an older loser. The first claim made after the release wins
  only when no earlier claimant remains.

**Edge cases.**

- **Which columns count.** A constraint reads its columns, the columns its
  expressions use, and, for a partial index, the columns its `WHERE` clause uses.
  A generated column counts as set by each write whose changes to its inputs
  changed its value.
- **A claim made of several writes.** Notes are unique by `(folder, title)`, and
  note 2 is "Draft" in Home:

  ```
  10:00  Ana adds note 1: "Plan" in Work
  11:00  Ben renames note 2 to "Plan"
  12:00  Carol, not having seen Ben's write, moves note 2 to Work
  ```

  After the merge, note 2 is "Plan" in Work, like note 1. If these writes claim
  slots 50, 51 and 52, note 2's claim is 52 and note 1's is 50. Note 1 keeps the
  value.
- **A winner taken out later doesn't bring its loser back.** Notes are unique by
  title, and a sub-note points at its parent note with cascade:

  ```
  09:00  note 1 is "Ideas"
  10:00  Ana adds note 2, "Plan", as a sub-note of note 1
  11:00  Ben, not having seen note 2, renames note 1 to "Plan"
  ```

  If these writes claim slots in the order shown, note 2 keeps "Plan" and note 1
  is taken out. Note 2's parent is out, so note 2 goes too, by cascade. Note 1
  doesn't come back: if it did, note 2 would come back with it and take it out
  again. Both are recorded as lost, so the app can offer them back.

#### 9.3 CHECK constraints

A CHECK that reads one stored column can't fail after a merge, since each value
passed it on the device that wrote it. A CHECK that reads several can, when
concurrent writes each set some of them. A generated column counts as the stored
columns it is computed from, so a CHECK on one generated column can fail too.

Todos need `start <= end`. Ana moves the start later while Ben, offline, moves the
end earlier:

|             | start | end |       |
|-------------|-------|-----|-------|
| before      | 5     | 12  |       |
| Ana's write | 10    | ·   |       |
| Ben's write | ·     | 8   |       |
| merged      | 10    | 8   | fails |

Each device's own write passed. Until the second write arrives, each device's row
passes too, since it has seen only one of them. Once both have arrived, the row is
taken out and recorded as lost on every device, with the same record. It comes
back when a later write makes it pass, e.g. Ben setting `end` to 20.

Coven evaluates every CHECK on the merged row itself and records each one that
fails. SQLite's own failure report can't be used for this: it may name only the
first token of the expression (Appendix B.5).

#### 9.4 Triggers

The app declares each trigger on a synced table as **local** or **shared**
([api.md E2](api.md#e2-declaring-synced-tables)). A trigger not declared shared is
local.

A **local** trigger runs on every device, for its own writes and applied ones
alike, and writes only local tables. A search index is a local trigger:

```sql
CREATE TRIGGER notes_title_index AFTER UPDATE OF title ON notes
BEGIN
  UPDATE title_index SET title = new.title WHERE note_id = new.id;
END;
```

When Ben's phone applies Carol's write 2 (§8.1), the trigger updates Ben's index
to "Shopping". Every change coven makes is ordinary SQL, including a late write
taking a row back out (§8.4), so a count a local trigger keeps stays right.

A **shared** trigger runs only on the device making the write, writes only synced
tables, and its changes become part of that write and sync with it. Setting
`edited_at` when a body changes is a shared trigger:

```sql
CREATE TRIGGER notes_edited_at AFTER UPDATE OF body ON notes
WHEN NOT coven_applying()
BEGIN
  UPDATE notes SET edited_at = datetime('now') WHERE id = new.id;
END;
```

Ana's write 6 records her edit and the trigger's:

```
ana-phone, write 6, 2026-10-02 15:00:00.000 #0
  read_through: 20
  notes  row 42  generation 1  update  body: "milk, eggs" → "milk, eggs, bread"
  notes  row 42  generation 1  update  edited_at: → 2026-10-02 15:00    shared trigger
signed with Ana's key
```

Ben's phone applies both changes without running the trigger: the write already
holds what it did. SQLite can't turn off one trigger, so coven provides the SQL
function `coven_applying()`, true while it applies another device's write. A
shared trigger declares `WHEN NOT coven_applying()`, and coven refuses one that
doesn't, when the database opens and after migrating.

**Edge cases.**

- **A trigger writing the wrong kind of table fails.** SQLite's authorizer
  callback reports, when a statement is prepared, each table it would write and
  the trigger doing the write. Coven refuses the statement when a local trigger
  writes a synced table, or a shared trigger a local one.
- **A value a shared trigger derives can be wrong after a merge,** because its
  writes merge like any other. A shared trigger counts a note's attachments, and
  Ana and Ben, both offline, each add one to note 42:

  |             | attachments on note 42 | `attachment_count` |
  |-------------|------------------------|--------------------|
  | Ana's write | adds attachment 10     | 1 → 2              |
  | Ben's write | adds attachment 11     | 1 → 2              |
  | merged      | 3 attachments          | 2                  |

  A local trigger writing a local table counts 3 on every device.
- **Owner tables.** For owner permissions (§9.6), a shared trigger's targets are
  direct changes, even when a foreign-key action fired it, unless a step equals a
  foreign-key action's exact effect.

#### 9.5 Dependent changes

Some changes only make sense on top of data that turned out not to count. A write
can be refused or partly dropped for several reasons: a change to someone else's
owned row (§9.6), a write from an old schema version whose migration declared
`Drop` (§18.6), a part ignored by an audience reset (§19.4). A later change can
have been built on that data.

Ben, offline, creates note 50 in write 20, then retitles it in write 21. Suppose
write 20 is dropped when it arrives, for one of those reasons, while write 21 on
its own would apply. Write 21's retitle has nothing to stand on: the row it edits
was created by a write that doesn't count. Coven drops write 21's change too, and
records it as a lost value, with write 20 as its cause.

**Dependence is decided by data, never by recorded reads.** For each row change,
coven keeps the writes that supplied the data it changed, as the authoring device
used them:

- the write that supplied the row's generation (a row here is its table, key and
  audience; generation zero has no source);
- the write that set each cell it replaced;
- for a foreign-key action, also the direct changes and earlier actions in the
  same write that caused it (§9.6), recomputed from the original schema and the
  authoring inputs. When several actions combine on one row, every contributing
  cause is kept.

Each source names its write and its row, since two changes in one write can have
different verdicts. A change that would otherwise apply is dropped if any of its
sources was dropped, and this follows through: a change dropped by this rule
can't support another, even if the rest of its write survived. One row change,
with all its columns, is the unit of loss.

Reading a dropped write's slot position, or having it as an implicit own earlier
write, creates no dependence by itself. Other changes in the same write apply by
the ordinary rules. An ordinary merge loss (§8.2) doesn't drop its setter's
change.

**The lost-value record.** The dropped change is recorded on every device that
reads that audience: its write, row, generation, operation, old column names and
written values, with the failed sources as its cause. The cause lists the
generation's source first, then the replaced cells' setters in the order written,
then any other direct sources, then foreign-key causes in reconstructed execution
order. A write that played several roles appears once per role. A delete with no
values is recorded too. Such a lost value is *frozen*: it keeps the change exactly
as written, and a later replacement of the cell doesn't clear it.

**Edge cases.**

- **Sources outlive dismissal.** Coven keeps the source writes and each change's
  verdict separately from the lost values the app can dismiss, through schema
  conversion and snapshots. Dismissing a lost value can't change a source's
  verdict. A snapshot's coverage records which slots were consumed; it can't turn
  a dropped source into data that counts. The original cause survives later
  schema raises.
- **Checking sources.** Sources are checked against the original authored
  history. Missing evidence waits; invalid evidence is refused. None of these
  establishes a source: a read position, an old value that happens to be equal,
  or the write that currently holds the generation.
- **Other drop rules.** Removal of a device or member and member-key replacement
  (§14.1), exclusion by a schema change (§18.6) and a reset (§19.4) each have
  their own rule for whether a write counts. A write refused directly by the
  cut-off, or a part ignored by a reset, creates no lost value itself; a change
  that depends on it and would otherwise apply does.

#### 9.6 Owner tables

A table can be declared `owner_only()`
([api.md E2](api.md#e2-declaring-synced-tables)). A row's owner is the member who
created it. Only the owner may update or delete it directly. Tables without the
declaration have no owner restriction.

This is for authenticity, not secrecy: "Ana rated this" really was Ana. A change
to someone else's owned row is refused on every device and recorded as lost.

Foreign-key actions caused by someone else's authorized change still apply. Ben
deletes a track while Ana's review points at it. With cascade, Ben's write records
the review's derived delete. With restrict, SQLite refuses Ben's delete while the
review points at the track. Ben can never edit Ana's review directly.

**Who the owner is.** Ownership comes from coven's own record of the authenticated
member who created the row, never from an app column the app could change.

- Inserting a new row makes its author the owner. Updating, deleting, re-adding
  or moving it between audiences keeps that owner.
- When several members create the same shared key from generation zero, the
  lowest claimed slot wins, then the lower write number within that slot. This is
  decided before permission checks, and later claims never change an established
  owner.
- A creation still in the local queue is provisional. When a published creation
  already owns that key, the queued changes are checked against that owner before
  their effects count.
- Coven keeps the creating write's member through deletion, dismissal of a lost
  value, schema conversion and snapshots. Renaming a table or key doesn't make the
  migration's author the row's creator.

**Checking a write.** An insert over an existing or removed shared key counts as
an update for this permission. Direct changes are the write's own statements and
its shared triggers. Every direct change is checked, even when its effect combines
with a foreign-key action on the same row; a trigger can't borrow permission from
the row that fired it.

- **At commit,** the author's device checks its own write the way receivers will.
  A change they would refuse refuses the whole local transaction with
  `DbError::NotOwner` or `DbError::InvalidForeignKeyEffect`, leaving its rows and
  upload queue unchanged.
- **On receipt,** an unauthorized row change becomes a frozen lost value (§9.5)
  naming the owner; the write's other changes still apply. Changes that used its
  data are dropped by §9.5's rule. This is separate from the whole-write checks of
  signature and membership (§7.1, §14.1).

**Foreign-key effects on owned rows.** A foreign-key action derived from an
authorized direct change in the same write may update or delete another member's
row. That includes chains of cascade and set null actions, on deletes and on
updates of the referenced key. Receivers don't trust the write's labels for this.
They recompute the whole effect from the original schema, the write's direct
changes and the verified authoring inputs; a claimed label of direct or derived, a
target, a generation, a set of values or a list of causes counts for nothing.

**Edge cases.**

- **What counts as a foreign-key action.** A step of a write is one exactly when
  it equals what the schema says an earlier step of the same write causes, through
  a reference its row holds at that moment, with the parent each written reference
  names. "At that moment" means the author's view replayed through the write's
  earlier steps. Every other step is direct. A trigger's step that equals such a
  consequence, made before SQLite's action reaches the row, is that action's exact
  effect and counts as the action.
- **References that share columns.** An action also records the columns another
  reference of the row shares with its own, and that reference's parent: the
  parent it held, when the action leaves those values unchanged, or else the row
  its new values name, as the write's earlier steps leave it. When the write never
  touched that row, or a recorded reference's parent may have ended before the
  write, the step depends on rows of the author's view that no step carries, and
  every device refuses it.
- **Exact match.** The recomputed action must match the exact row, generation,
  operation and every changed column and value, including all components of a
  composite key. An action's delete shows the values its replaced setters wrote
  ([format.md D5](format.md#d5-writes)). Several actions on one row must match their full
  combined effect in the original execution order; matching one part isn't enough.
- **Every derivation must reach an authorized direct cause in this write.** A
  cycle with no direct cause proves nothing. A proposed change to another member's
  row without this exact derivation is an unauthorized lost value. Missing
  evidence waits; it never grants permission.
- **What is kept.** Coven keeps the verified derivation, the full original row
  change and all contributing causes, separately from lost values the app can
  dismiss. A direct change and its sources are kept even when its own row ends
  with no net change, because its foreign-key effects can remain. An accepted
  cascade is recorded as a derived delete even when it knowingly replaced the old
  values and the cell rules call for no lost value.
- **When a cause drops,** its derived row changes drop by §9.5's rule. The
  surviving changes are replayed: the row's earlier generation and values return,
  unless an independent surviving change or a removal rule keeps them out. A
  combined row change loses if any contributing cause loses.
- **Ordinary rules still apply.** Owner tables follow every schema and removal
  rule. Unique and CHECK conflicts can take owned rows out, and shared triggers
  can cause such conflicts through their authorized targets. Every row taken out
  has its ordinary lost-value record naming the causes, and comes back when the
  last cause goes. The `SET DEFAULT` refusal (§9.1) applies. Ownership adds no
  other schema restriction.
- **Every device decides the same.** Owner permissions and verified foreign-key
  effects are checked on every device from the same authenticated inputs,
  including after a reload.

---

## Part III. People

Part III is about who may do what. It covers members and devices (§10), the one
path every admin change takes (§11), how keys reach people (§12), joining (§13),
removing people (§14), and circles, which let some members share rows the others
can't read (§15).

### 10. Members, roles and devices

A store is shared by several people, each with several devices. Someone must be
allowed to add Dan, and nobody may remove the last admin. There is no server to
ask, so every device has to reach the same answer on its own. This section says
who the actors are and what each may do; §11 says how a change is made, and §12
how keys reach people.

A *member* is a person. Each member has a random, permanent `MemberId` and two
current key pairs: one to sign with and one that others seal keys to (Appendix D
gives their exact form). Devices and circles refer to the member by that id.

A *device* is one installation of the app, belonging to one member, with its own
id. The id is also kept in custody that backups don't copy, which is how coven
notices a restored backup posing as the original (§19.3).

#### 10.1 Who may do what

| Action | Who may do it |
|---|---|
| Create the store | the first device, which becomes the first admin (§10.3) |
| Add or remove a member, change a role | an admin |
| Add a device | the member it belongs to, from that new device itself |
| Remove a device | its member, or an admin |
| Replace a member's keys | that member, or an admin (§12.3) |
| Set a member's provider access | that member; an admin may set another member's S3 access |
| Create a circle | any member, who becomes its first member |
| Rename a circle, add or remove its members, rotate or delete it | its members only; an admin outside it can't |

A store may have several admins. The store always keeps at least one: removing
the last admin, or demoting them, is refused. The member whose provider account
holds the store (the Dropbox folder's owner, say) can't be removed either,
because nobody else could take provider access back; coven refuses with
`SyncError::StoreOwner` before claiming anything (§14.2).

Provider-admin access is not store authority. Ben holding the S3 account's admin
key doesn't make him a store admin, and Carol being a store admin doesn't give
her the S3 account.

#### 10.2 Admin entries

Every change to membership or settings is an *admin entry* in a slot. Its id,
`EntryId`, is its slot number. format.md
[D6](format.md#d6-admin-entries) lists the actions and their fields.

A device judges each entry against the state after every slot before it, so
every device judges it the same way. Equal preceding slots give equal decisions
([`valid_prefix`](proofs/sequence/CovenSequence/Converge.lean)). A device that
loaded a snapshot and applied the slots after it holds that same state, so it
judges the same way too.

An entry counts only if its author may make it:

- it is signed with the member's current signing key as of the state before its
  slot;
- its authoring device is active, or is the fresh device the entry itself
  registers;
- its target exists;
- removing a member or changing a role still leaves an admin.

An applied entry is final. Nothing later undoes it, although a later entry can
change things again from its own slot on.

Applying an entry commits the new state and the work it calls for, such as an
unshare, in one transaction. A failed transaction changes nothing, and the app's
subscriptions only ever see committed results.

Ana and Ben, both admins, each try to remove the other. Ana's removal claims slot
41 first. Ben reads it, no longer has authority, and his call undoes its work and
fails (§11). No removal of Ana ever enters history.

Devices register themselves. A new installation claims an entry adding its own
fresh id. A member who has lost every device can still register a new one,
because the entry is authorised by the member's current signing key, not by an
existing device.

#### 10.3 Creating a store

Slot 1 creates the store and registers its first device and its first admin.
Since nobody is a member yet, slot 1 carries its author's public key, and that key
verifies its signature. The store id in slot 1 must match its path.

Ana sets up Household on Dropbox:

1. Her phone sets up her member keys in custody (OS keychain by default, §12.4).
2. It claims slot 1, which creates the store with the store id and name she
   gave and registers her phone and Ana as admin.
3. It holds the first store key and claims its sealed copy to Ana next, at slot 2
   (§12.1).
4. Setup succeeds once both slots are confirmed.

If another creation already holds slot 1, setup fails with
`StorageFailure::StoreExists`.

Creation supplies the store's id and name. Settings supplied later when setting
up from the store must match them; a mismatch fails with
`DbError::StoreSettingsMismatch`.

#### 10.4 Edge cases

- **Already done.** A request that is already satisfied completes without another
  entry. Adding someone who is already an active member counts as satisfied only
  if it repeats their exact keys, role and access.
- **Ids are never reused.** Neither device ids nor member ids are reused. Added
  devices always get fresh ids. A person removed and invited again gets a new
  member id and new keys.
- **Key replacement races.** Replacing a member's keys names the old pair it
  expects. If that pair is no longer current, because another replacement landed
  first, the call fails and the person must make a new one. Both new keys must
  never have been used (§12.3).
- **Old identities stay known.** Snapshots keep the history of which keys each
  member had, removed members and devices, and every public key ever used. Old
  writes are checked against them, and they stop an id or key being reused.
- **What the proofs cover.** The model proves membership and role changes
  authorised, and an admin always present. Other admin actions are checked against
  the authority rules this section states, which the model takes as given.

### 11. Admin operations

An admin action needs preparation before its entry can land. Rotating the Gifts
key needs a new key and copies of it; inviting Dan needs a provider share. Any
step can fail, the device can crash between steps, and another device can claim
the slot first. Every admin action, from renaming a circle to replacing keys,
inviting, rotating or resetting, therefore follows one path, so there is one way
to get it right. That path is an *operation*.

#### 11.1 The steps

1. **Catch up.** Read and apply the sequence to its end, and check the action is
   allowed. A new call on a device without storage, or whose sequence it can't
   apply, fails here, before creating anything.
2. **Journal, then prepare.** Record each piece of preparatory work in the journal
   before doing it: a new key, a provider share, an S3 key, a key delivery, a
   snapshot, an upload session. Record enough to finish it or undo it. Then do the
   work. Key copies and snapshot announcements claim their own, earlier slots.
3. **Claim** the next slot with the entry, settling the claim as §5.1 describes.
4. **If someone else claimed it first,** apply their slot and check again. If the
   action is still allowed, finish any preparation that changed and claim the
   next slot. If not, undo everything the journal still owns and fail with
   `SyncError::Rejected(OperationRejection)`.
5. **Finish.** Once the claim settles, apply the entry and do the local work it
   needs, such as installing a new key in custody, before reporting success.

Ben rotates the Gifts key. His journal holds the new key, K2, and the copies he
claimed at slots 41 and 42. He sends his entry for slot 43 and crashes before the
reply. On restart he settles 43 first. If his rotation landed, he finishes it. If
another entry took 43, he applies it and checks again. If 43 removed him from
Gifts, his call undoes what's left and fails. Copies 41 and 42 stay in history.

Without settling first, a restarted operation would check again, claim 44, and
land twice
([`forgetful_lands_twice`](proofs/sequence/CovenSequence/Operation.lean)).

#### 11.2 Losing a race

Losing a race never asks the app to decide anything. The operation checks again
by itself:

- If the action is now already satisfied, it completes without a redundant entry.
- The target and any approved keys stay fixed. Checking again can't turn the
  operation into a different one: it can't overwrite a competing key replacement,
  or approve a different join request than the one Ana approved.
- There is no fairness or time bound. A device can lose any number of times;
  progress needs only that its requests succeed and it gets to run.

#### 11.3 Undo

An undo removes everything the journal recorded, and nothing may be left behind.

- **Wait first.** Before deleting anything, wait for, or cancel, every request
  already sent. Then delete by the recorded name and confirm it's absent. An undo
  that deletes while a create is still in flight can end with that create landing
  afterwards, leaving what it made behind
  ([`eager_undo_leaks`](proofs/sequence/CovenSequence/Operation.lean)).
- **Some calls can't be cancelled.** A provider call that can't be cancelled keeps
  its cleanup duty in the journal until it ends. An S3 key also carries the name
  of the device that made it, so §14.3 can find it if everything else is lost.
- **Deleting what's already gone succeeds,** so retrying an undo is safe.
- **A failed undo stays.** It keeps both causes, the rejection and the undo
  failure, reports them, and retries. Cancelling the call can't report success
  while cleanup is still owed.
- **Disclosed keys stay disclosed.** A key copy already claimed is history. It has
  told its recipient the key, and undo can't take that back, so it still counts as
  exposure (§12.2).

#### 11.4 Crashes and waiting

- **Settle first.** After a restart the device settles its claim (§5.1), then
  either finishes or undoes.
- **A claimed entry is final.** After a crash, the device finishes what the entry
  needs locally, such as activating a key and putting it in custody. It never
  substitutes another action.
- **Progress commits with local changes.** Local changes and journal progress
  commit in one transaction. The record of unfinished work is deleted only after
  completion or confirmed undo.
- **Dropped calls keep going.** If the app drops the call, the journal stays.
  Temporary failures and failed undos retry with the shared backoff (§6.7).
- **Waits are visible.** Each wait records its first unmet condition (§19.1). The
  app may ask for a retry, but it can't discard a reload or provider work that is
  required.
- **Occupied paths.** Preparation that creates objects outside the sequence (a
  snapshot, a join request, a key delivery) uses fixed paths. An occupied one
  counts as this attempt's only under §19.3's complete byte comparison with what
  the journal recorded.

#### 11.5 How each operation prepares

| Operation | Preparation, in order |
|---|---|
| Key rotation, circle creation | keep the new key; claim its copies; claim its introduction (§12.1) |
| Adding a circle member | supply the circle's earlier keys to them first (§15.7) |
| Invite | record the fixed target member and the new provider access, then show the code (§13.2) |
| Replacing one's own keys | pause app writes; confirm the writes queued before the replacement; keep the new pair; claim copies, then the replacement; install custody and refresh the restore code before lifting the pause (§12.3) |
| Schema raise | confirm captured writes and the raise marker; upload and announce the snapshot; claim the raise (§18.5) |
| Snapshots | §16 covers taking, announcing and keeping them |
| Reload | stage the data, then commit it at once (§16.5) |

Writes and files keep their journals with their own queues (§4, §17.7). An
upload's progress survives reopening only as far as the provider has confirmed
it; bytes merely sent don't advance it.

#### 11.6 What the proofs cover

The model proves that an operation keeps its recorded work and in-flight
requests accounted for at every step, occupies at most one slot, lands only
after passing its check against every slot before it, and, once undone, leaves
neither slot bytes nor anything it made. It keeps all slots and proves safety,
not that an operation eventually finishes; §5.3 covers settling after slots are
deleted.

### 12. Keys and encryption

The provider holds every byte of the store and must read none of it. Members must
read everything they're entitled to, including on a new device or after joining
late. And once Ben is removed, new data must stop reaching him. This section
covers which keys exist, how a key reaches a member, how a device chooses which
key to write with, and where keys are kept. Appendix D lists the algorithms and
exact bindings.

Coven uses these keys:

| Key | Encrypts or does |
|---|---|
| The store key | the store's data and its metadata, such as admin entries and slot headers |
| Each circle's key | that circle's rows (§15) |
| Each file's key | that file, carried inside its encrypted row (§17.2) |
| A member's signing pair | signs every slot the member's devices write |
| A member's sealing pair | opens keys sealed to the member |
| Provider credentials | reach storage; they encrypt nothing |

An *audience key* is the store key or a circle's key. Each audience may have
several: rotation adds new ones, and older ones stay readable.

Contents are encrypted with XChaCha20-Poly1305, slots are signed with Ed25519, and
keys are sealed to members with X25519 sealed boxes.

**Every slot is signed** by its author's member key. The signature covers the
slot's path, so the provider can't move one object to another's place.

**Who can read what.** Each slot's device and claim number, and which audiences
a write's parts belong to, are in a header encrypted under the store key. Every
member can read that header before deciding which parts to open; the provider
can't. So whoever learns a store key reads those headers, the store's data and
the admin entries' contents under it, but still not a circle's parts or a key
copy's sealed box: each of those needs its own key.

| Who can read it | What |
|---|---|
| The provider (in the clear) | each slot's kind (writes, admin entry, key copy, announcement); a key copy's routing (its audience, key id and recipient member, §12.1), so circle ids are already visible; object framing and sizes ([format.md D9](format.md#d9-sealed-objects)); the probes S3 detection writes (§20.7); the device and audience that file and snapshot paths name (§3) |
| Every member (sealed under the store key) | a slot's device, its claim number (§5.1), and which audience each part belongs to |
| A circle's members (sealed under its key) | the contents of that circle's parts |
| Members of the audience only | names, values, key commitments, operation contents |

**Authorship roots are keyed.** A write's signatures cover, for each of its
parts, an *authorship root*: a hash over that part's row changes
([format.md D9](format.md#d9-sealed-objects)). The hash's leaves are keyed with a
secret derived from the part's audience key, so a member outside that audience
can't test a guessed row against the published root
([format.md D11](format.md#d11-keys-and-contexts)).

#### 12.1 Getting a key to someone

A key reaches a member as a *key copy*: the key sealed to that member's current
sealing key, published in its own slot. Its routing is readable by the provider:
the audience, the key's id, the recipient member and the exact sealing key it is
sealed to. That is how Dan finds his copy before he holds any key.

A new audience key is *introduced* by an admin entry naming the key's random id
and a SHA-256 hash of its exact 32 bytes. Store creation and rotations introduce
keys; removals carry none. The introduction's fields are encrypted like the rest
of the entry.

**Copies come first, then the introduction.** An operation claims every copy the
key needs before it claims the introduction, so by the time a key is in use,
everyone who should have it has a copy. Each copy's sealed box sits in the
clear part of its slot, sealed to its recipient alone; no second layer of
encryption wraps it. The rest of the copy's slot, only an acknowledgement, is
encrypted with a store key already accepted. The new key may encrypt its own
introduction. Store creation is the one exception: slot 1 sets up the first
member and keys, and the creator's copy follows at slot 2 (§10.3).

Opening a copy:

1. Open it with the private key its routing names.
2. If the key's introduction has already been applied, check the key against its
   hash. A mismatch stops the device at that slot (§5.4).
3. If the introduction hasn't arrived yet, keep the key as *tentative* and apply
   the copy. A tentative key may only be used to check its own introduction. It
   can't encrypt app data until the introduction confirms it.

A device needn't open a copy meant for someone else. Its signed routing still
records whom the key reached.

Two copies of one key to one member are harmless. Ana and Ben may both send key K
to Carol in different slots; both stay readable and record the same recipient.

**Old keys reach new people too.** A current audience member whose sealing key
has no copy of an older key is sent one, so they can read history made under it.
An operation that retries doesn't need to send a copy that is already in the
sequence.

**A new device** first opens the copies for its sealing key, from the floor or the
sequence, and checks their introductions and its membership before loading any
data (§13, §16.4).

#### 12.2 Exposure, and which key new data uses

A copy *exposes* its key to its recipient, whether or not the recipient ever opens
it. A key exposed to someone who has since left the audience, or to a sealing key
since replaced, can't be used for new data.

Removing one device doesn't retire anything here. A member's key pairs are shared
by all their devices, so removing Ben's lost phone, without removing Ben or
replacing his keys, leaves his sealing key current (§14).

A tentative copy's recipient is checked once the later entry that admits them, or
the key's introduction, applies. A recipient who never ends up admitted still
received the key; undo can't erase that.

**Choosing a key.** Before a write is first sent, its device picks, for each
audience, the newest usable key by introduction slot. Usable means, judged on
the slots this device has applied:

- it has been introduced, and its introduction is authorised;
- this device holds it in custody;
- no applied copy of it reached someone who has since left the audience, or a
  sealing key since replaced.

If no key is usable, the device waits for a new key from another member, or
rotates one itself through an operation (§11). Only a member of the audience can
rotate its key: an admin outside a circle can't.

Carol, an admin outside Gifts, removes Ben from the store. Ben had copies of the
store key and the Gifts key. Ana is still in Gifts, so her next pass rotates the
Gifts key before any new Gifts data uses it. Carol can't make that key. If Ana is
offline, Gifts waits until a member of it is online. Older Gifts keys stay
readable, because old data was written with them. The store key, which Ben also
had, is replaced the same way by any remaining member's device before it sends
more store data.

Several rotations can follow each other. Every introduced older key stays
readable; new data uses the newest usable one.

**Admin entries use any accepted store key.** An admin entry may be encrypted with
the newest accepted store key the device holds, even an exposed one. Otherwise
exposure could block the very removal or rotation that fixes it.

**Retries keep their keys.** A write keeps the keys chosen on its first attempt
when it is retried, so its bytes don't change (§6.3). Circle parts written before
the author left the circle are covered in §15.8.

**A copy can arrive late.** Once Ana applies Ben's removal, her next first
attempts avoid every key known to have reached Ben. But a copy to Ben made before
its author saw the removal can still land after Ana's read. On her next read it
exposes that key, and Ana needs another usable key from then on. Writes she has
already attempted keep their bytes.

Removal can't take back what Ben already downloaded: the provider's access
cut-off stops him downloading more, not opening what he holds (§14.2).

#### 12.3 Replacing a member's keys

A member replaces their keys when they suspect a leak, or an admin replaces them
for a member who lost everything (§13.3).

- **Both new keys must be fresh.** Both the signing and sealing key must never
  have been used.
- **The member stays the same.** Replacement keeps the member's id, role,
  circles, devices and provider access records. It implies no provider cut-off.
- **The old pair must still be current** when it lands (§10.4).
- **The old sealing key is retired.** Copies sealed to it stay exposure, although
  the member is still active, so keys copied to it need replacing.
- **Old-key writes stop counting** from the replacement's slot (§14.1). A device
  still holding the replaced keys can't send, and reports `MemberKeysReplaced`.

Ana replaces her own keys:

1. Her device pauses the app's writes; a write in the meantime is refused with
   `MemberKeysChanging`.
2. It sends and confirms the writes already queued, while her old keys still
   count.
3. It keeps the new private pair in custody before claiming anything.
4. It claims copies of the audience keys she holds, sealed to her new sealing
   key, then the replacement entry.
5. On success it installs the new pair in custody and returns a new restore code
   (§13.1).

The pause lifts only after step 5, once custody and the restore code are in
place.

Step 2 is why the pause exists. A write Ana made before the replacement is signed
with her old signing key. Once the replacement applies, writes under that key are
cut off (§14.1), and the new key can't vouch for it either, because the write's
recorded history predates the new key. A queued write that missed step 2 would
never count
([`self_replacement_counterexample`, `later_replacement_counterexample`](proofs/sequence/CovenSequence/QueueReloadExamples.lean)).

If it fails, both pairs and the recorded progress are kept for a retry. The pause
lifts only once the undo is done and the device has confirmed that the keys it
holds still match Ana's current keys.

#### 12.4 Custody

*Custody* is where secret keys live: the OS keychain by default, or another
store the app supplies. Members' private keys and audience keys stay in custody,
never in the database.

- Store creation sets up the member in custody before publishing the store.
- A device keeps every earlier private sealing key it still needs to open old
  copies, or has those keys copied to its current sealing key instead.
- The app can keep its own secrets, such as an API token, in separate keychain
  entries under the same access policy. Coven tracks their names so that deleting
  the local store removes them all, even when the database is damaged. Appendix
  D.6 gives the mechanics.

### 13. Joining and restoring

A device can only read the store once it holds the store key, and the only way to
get it is a key copy sealed to its member. A person's new device has to prove it
is that person. A new person has to be let in by an admin, and the provider must
let them reach the storage. This section covers both, and what happens when
someone loses everything.

#### 13.1 A person's new device: the restore code

A *restore code* holds:

- the member's id and current private signing and sealing keys, which get the
  device the store key (§12);
- the store's id and name;
- the storage location, but never storage credentials or sign-in tokens.

Coven returns the code at setup and whenever the member's keys are replaced, and
`restore_code()` shows it again. Keeping it safe, or asking the person to confirm
they wrote it down, is up to the app; coven tracks no pending task for it.

A new device gets the code in one of three ways:

- **Scanning** a QR code on one of the person's devices that has the store open.
  The QR code is blurred until the person taps to show it. Because the person
  controls both devices, this transfer may also carry their S3 key.
- **iCloud Keychain,** on Apple platforms. It holds the code and may hold the S3
  key separately beside it. Coven refreshes the Keychain code after setup, joining
  and member-key replacement. A changed S3 key updates its own entry without
  changing the code (api.md [E9](api.md#e9-members-and-devices)).
- **Typing** it in.

Storage access is separate. On S3 the person keeps their access key apart, in a
password manager say, and enters it, unless the QR transfer or Keychain supplied
it. On Dropbox and OneDrive, the new device asks coven to sign in to the person's
own account first, through the sign-in presenter the app supplied; coven keeps the
tokens. A new device on the same Apple account still needs that sign-in.

Ana's new laptop scans the code from her phone and signs in to her Dropbox. Then:

1. It reads the floor and the key copies it carries for Ana, loads the snapshot,
   and applies the slots after it.
2. It checks that the code's id and keys match a current, active member. An old
   code, whose keys have since been replaced, fails with `MemberKeysReplaced`; it
   can't put its keys back.
3. It registers itself as a new device through an operation (§11). A claim
   interrupted along the way is settled before it is retried or cancelled.
4. It finishes loading and hands the app the open store.

Restoring always makes a fresh device. It doesn't rescue the old installation's
unsent writes.

#### 13.2 Adding a person: invites

Ana adds Dan to Household on Dropbox:

1. Ana enters Dan's Dropbox email and picks his role. Her phone shares the store's
   folder with that account and shows an *invite*: a QR code holding the store's
   location, a member id chosen for Dan, and a one-time *invite secret*.
2. Dan's phone scans it, signs in to his own Dropbox, mounts the shared folder,
   makes Dan's member keys, and writes a *join request* to storage, encrypted with
   a key derived from the invite secret and signed with Dan's new key.
3. Ana's phone, the only device holding the secret, shows the request. Ana
   approves.
4. Ana's phone claims copies of every store key it holds, sealed to Dan's new
   sealing key, then the entry adding him. Every key, not just the newest, so Dan
   can read a late write made under an older one.
5. Dan's phone opens its keys, loads the store, registers itself with its own
   "add device" entry, and Dan writes down his own restore code.

The invite only lets Dan ask; Ana's approval lets him in.

On Dropbox, OneDrive and iCloud, only the account that holds the store can share
it, so the inviting admin must be the member whose account holds it (§20.9).

**What the invite holds:**

| Field | Used for |
|---|---|
| store id, name and location | finding the store |
| invite id | the join request's path |
| the chosen member id | fixed by Ana before she shows the invite |
| the invite secret | encrypting the request; only Ana's phone has it |
| the store's first key id, from creation | identifying the store's first key |
| Ana's device id, and the latest slot it had applied when making the invite | which device's approval Dan's phone trusts, and where it starts reading |
| on S3, an access key made for Dan | reaching storage; made with admin access or guided console steps (§20.10) |

**Fixing the member id.** Ana chooses the member id before showing the invite. For
a new person coven makes a fresh id and Ana picks the role. To recover an existing
member (§13.3), Ana selects that active member instead, and their role and circles
are kept. Dan can't pick a different target.

**The join request** lives at `<store>/join-requests/<invite id>`. It holds the
invite id, the chosen member id, both of Dan's new public keys, and his device's
name. The provider sees only an encrypted object under the invite's id.

**Approving.** Ana's phone opens the request and checks it:

- the signature verifies with the signing key the request supplies;
- the member id matches the invite;
- both keys are fresh.

Approval records exactly the pair it checked: as *add member* for a fresh id, or
*replace member keys* for an existing one. A request that changes afterwards
needs a new approval. Before the entry, approval claims copies for Dan's exact
sealing key: every store key in history, and, when recovering an existing
member, the keys Ana holds for that member's circles. If Ana's phone lacks a key
it must send, approval is blocked until it has it.

The entry adding a new member records how they reach storage: their provider
account, or the id of the S3 key made for them and who holds it. Any admin can
then take it back later (§14.2, §14.3).

**Dan's side, while he waits.** Starting after the slot the invite names, Dan's
phone reads ahead, looking only at each slot's kind and any key copy's routing.
This doesn't apply anything and gives no right to send. It keeps what it read and
how far it got, stops at the first missing slot, and retries with the shared
backoff (§6.7).

Once his copies arrive, Dan's phone opens them, checks the entry from Ana's
approving device that admits him, loads the floor's snapshot and applies the
slots after it in order. If history he had scanned has since been deleted, the
copies the floor carries supply his keys (§16.4). He registers only with both his
own sealed keys and that applied entry; copies alone aren't enough.

If the request disappears, Dan's phone reads forward again and checks the floor
before concluding it was declined or expired, because Ana's approval may have
landed just after the read that missed it. If approval was rejected, Dan's phone
reports it. Another device's unrelated entry can't stand in for it.

**Before Dan's phone sends the request,** it keeps his new keys and the request's
exact bytes in a local directory not yet published to the app, encrypted with the
invite secret and tied to the store, the invite and the chosen member id. These
are not his keys' final custody. Restarting with the same invite and device name
reuses them, and the same device id.

- It records the create before sending it. If the reply is lost, and on retry
  neither the request nor his sealed keys exist, it treats the invite as finished.
  Sending the request again could revive an invite Ana already declined; Ana can
  make a new invite instead.
- A request the provider refused was never stored, so the retry sends it again.
- Provider permission errors reach the app, even when they come from access being
  taken back after a decline or expiry. A request it can't read is not a request
  that's absent.

**Ending an invite.** Once Ana approves or declines, or the invite expires, Ana's
phone deletes the request object.

If Ana's phone is reset (§19.3) before the invite ends, the reset carries the
invite into the new database, and it is ended the same way as on expiry: its
grant taken back and its request deleted, while access that active members still
use stays. Without this, a reset would strand the access the invite granted.

Declining, or letting the invite expire, takes back the provider access it
granted. On S3, Dan's key becomes due for deletion (§14.3), through coven with
admin access or through the provider's console. An invite expires one day after
it was made, by the inviting device's clock, when that device is next online.

Recovering an existing member changes none of their access records or device
ids. The new installation registers its own fresh device after the key
replacement is claimed. If provider access changes too, that is separate work with
its own access entry (§14.3). Cleaning up a recovery invite takes back only what
that invite newly granted, never the member's existing access, and the usual
checks on shared accounts and other grants still apply (§14.2).

#### 13.3 Losing everything

Ben loses every device and his restore code. An admin invites him again, to his
*existing* member id. Approving replaces his keys:

- his role, circles and existing device ids stay attached to his id;
- his old keys count as exposed (§12.2);
- writes signed with his old keys that land after the replacement are refused
  (§14.1).

A removed member can't be brought back by replacing keys. Inviting that person
again gives them a fresh id, under the ordinary rules for adding a member.

If every member loses every device and restore code, the store is lost:
everything is encrypted.

#### 13.4 Edge cases: installing on the device

- **One builder.** Restore and join take the app's configured builder, scoped to
  its storage layout, and return the open store
  (api.md [E10](api.md#e10-joining-and-restore)). The app supplies its opening
  choices once. Key custody that lasts only for a session stays with the returned
  handle.
- **No sends before loading.** Store creation and joining by invite have their
  own special rules (§10.3, §13.2). Neither lets a device send ordinary writes
  before it has applied the sequence.
- **Unfinished installations are hidden.** While loading, the installation is
  missing from listings and ordinary opens refuse it. It stays at its permanent
  path throughout. Only after loading does coven commit the final keys and
  credentials and make it visible, without closing or moving the database. So the
  app's first commits are made against the installed membership and rows (§2.1).
- **Cancelling.** Cancelling closes the installation and removes its local work.
  Dropping a join that is still waiting for approval keeps its encrypted request,
  so the app can retry it explicitly.
- **Dropping mid-setup.** Dropping a setup in progress marks the installation as
  closing. Its locks stay held until its blocking work in flight finishes. Another
  setup or open of the same store in this process waits for them, as any
  reopening does (Appendix F.4, api.md [E1](api.md#e1-opening)).

### 14. Removing people and devices

A removed person still has the keys and data they had. Two things stop them doing
more: the sequence stops counting what they send, and the provider stops letting
them reach storage. This section covers both.

#### 14.1 The cut-off

A removal is an admin operation (§11), and its slot is a line in history. Writes
from the removed device or member that land in later slots don't count. The same
line falls at a member-key replacement: later writes signed with the old key don't
count, while writes signed with the new key do.

Ana's old phone sends write 8 while her replacement device claims slot 70,
registering itself in the old phone's place (§19.3). If write 8 lands at slot 69,
it counts. At slot 71, it doesn't.

What happens to such a writes slot:

- Every device checks who wrote it, consumes the slot, and reports `WritesRefused`
  to the app, naming the entry that decided it.
- The writes have no effect on data and record no lost values. The model proves
  rows unchanged, and no new unique-value or owner claim, while the device's claim
  number is still recorded.
- A later change by someone else that relied on the refused data is dropped by
  §9.5's rule.
- Writes that landed before the removal keep counting.

Sending follows slot order, not time. No device needs permission based on how
long ago something happened. A removed device, or one holding member keys that
have been replaced, can't send.

When a device applies its own removal, or its member's, it stops syncing and tells
the app. A device whose id was replaced by a reset (§19.3) reports
`DeviceIdInUse`; one whose member's keys were replaced reports
`MemberKeysReplaced`.

Removing a member also removes their devices and their circle memberships. A
circle left empty is deleted at that slot. The removal entry lists no keys or
circles: the state before its slot supplies them.

Removing someone from a circle only, without removing them from the store, is
different: §15.8 covers it.

#### 14.2 Provider access

Removal in the sequence stops Ben's writes from counting. Taking away his storage
access stops him reading anything new. That second step is provider work.

Applying a removal or an access change records the provider work it needs in the
same transaction. That is the only way it gets scheduled: the call that asked for
the removal doesn't revoke anything itself. `remove_member` returns once its entry
applies, and the app's pending list shows the provider work's progress, including
waits for an owner.

- **Dropbox, OneDrive, iCloud:** sharing follows the active members and open
  invites. Only the account that holds the store can change sharing, so that
  member's device does it when it applies the change. If Ana owns the folder and
  is offline, Ben's removal applies at once everywhere, but the unsharing waits
  for Ana's next pass.
- **S3:** each member has their own access key. Removing Ben deletes his
  (§14.3), with provider admin access the person supplies for that one operation,
  or through guided console steps (§20.10).

Ben removes Dan while Ana, the folder owner, is offline. Ben's call returns as
soon as its entry applies, and his pending list names the wait for Ana. Ana's next
pass does the unsharing.

The owner's device makes these changes one request at a time, following what
the applied sequence intends. It revokes only a grant to that one account, keeps
an account shared while another member or an open invite still uses it, and
reports grants it leaves in place as `AccessRemains`. §20.9 gives these rules.

**One lost device.** These providers can't cut off one device of an account
alone. To lock out a lost phone:

| Provider | What the person does |
|---|---|
| Dropbox, OneDrive | remove the app's access from their account, then sign in again on the devices they keep |
| iCloud | remove the lost device from their Apple account |
| S3 | an admin replaces that member's access key (§14.3) |

Removal changes no ciphertext or private key anyone already obtained. Confirmed
cut-off prevents further reads and publishing. Until then, every device still
applies the sequence's rules on who counts.

#### 14.3 S3 access keys

On S3, access keys are separate from the sequence, so coven tracks every key it
makes until it is confirmed deleted.

**Replacing a member's key** (to lock out a lost device, say) is an operation:

1. An admin, holding provider admin access for this operation only, makes a fresh
   key for the member and checks it works. Everything the operation makes is
   recorded in the journal before it is made (§11).
2. The admin's device delivers the new key sealed to that member, at
   `<store>/access-keys/<member>/<attempt>` (format.md
   [D9](format.md#d9-sealed-objects)).
3. It claims a *set access* entry naming the new key as current.
4. It deletes the superseded key.

The member's devices install the delivered key only once the same author's
applied set-access entry names it as current. A device that missed the delivery
before the old key was deleted must get the new key from the person. The restore
code doesn't change.

If the claim loses a race, the operation checks again. If the target or the
authority is gone, it undoes the new key and delivery and leaves the old key
current. A failed undo keeps the journal and both causes visible. A claimed
set-access entry is final; later access changes queue its superseded key for
deletion.

**Keys due for deletion.** Every distinct S3 key that is no longer current becomes
`DeleteAccessKey` work:

- removing a member: every access key recorded for them;
- ending an invite: the keys it newly granted;
- a failed operation that never claimed: the keys its journal owns.

Undoing a recovery invite never deletes the active member's still-current key.

A deletion record clears only after deleting that exact key id and reading it
back as absent, or after the app calls `confirm_access_key_deleted`. A success
reply, elapsed time, a failed read, or another key's absence can't clear it.
Confirmations are kept by key id across reopening, so a retry can't bring the
notice back. Keys made in the console must be deleted in the console.

**Keys nobody recorded.** A key can outlive every record of it, if the database
was lost or the reply to making it never arrived. Each key's provider name
carries the store and the device that made it (§20.10). Once that device's
removal has been applied, an admin holding provider access lists those keys
and queues for deletion each one that is neither recorded in the sequence or the
store snapshot nor current. Later entries by the removed device can't make such a
key current. A live device's unrecorded key is protected by its own operation.

A failed mint can also leave behind a principal with no key: an IAM user or a GCS
service account it made, when the rollback failed or the journal was lost. The
listing includes those too, matched by the same store-and-device naming. Once the
device's removal has been applied, the reclaim removes them without a deletion
record, because they grant nothing. A failed removal fails the reclaim call; on
retry, a principal already gone counts as removed.

Separately, the orphan scan of the file folders still requires confirmed provider
cut-off, including uploads in flight (§17.8). A device reset carries these
obligations into its new database (§19.3).

**Key replacement is not cut-off.** Replacing a member's keys (§12.3) changes no
access record or device id. Old-key writes follow the slot cut-off; provider
credentials and exposed audience keys have their own recorded work. It doesn't
certify that a file folder is closed.

### 15. Circles

Some rows are for only some members: Ana and Ben's Gifts list shouldn't be
readable by Carol, though all three share Household. A *circle* is a group of
members inside a store who share rows the others can't read. Every synced row
belongs to an *audience*: the store, or one circle. A row in the store reaches
every member's devices; a row in a circle reaches only its members' devices.

The hard part is keeping SQL's rules working when some devices can't see some
rows. Carol's device must still check foreign keys and unique values, and decide
the same things, without reading Gifts. This section covers how rows get their
audience (§15.1–§15.4), how circles are run (§15.5–§15.7), and how members leave
and circles end (§15.8–§15.9).

#### 15.1 Declaring audiences

The app says, for each synced table, how its rows get their audience:

- a **root** table names an audience column, never NULL, holding `store` or a
  circle's id;
- a **descendant** table names one foreign key, and each row takes the audience of
  the row it points at, so a todo is in its list's circle;
- any other table is in the store, every row of it.

```
lists         root         audience column: store or a circle
todos         descendant   through list_id
todo_labels   descendant   through todo_id
labels        store
```

When the app binds an `Audience` or `CircleId` value, coven writes canonical text:
`store`, or the circle's UUID in lowercase with hyphens. Any other spelling,
uppercase included, fails with `InvalidAudience`, because two spellings of one
circle would otherwise look like two audiences.

A row's audience is defined once, from these declarations, and everything that
decides where a row goes uses that one definition: writes, snapshots and files.

A descendant's foreign key must be:

- **one column, into a synced table;**
- **not set null:** the row would lose its audience;
- **part of a chain that ends:** following declared foreign keys from any
  descendant reaches a root or a store table. A loop is refused.

Coven checks these when the database opens and after each migration.

Writes and snapshots downloaded from other devices go through the same checks: an
independent key must hold a canonical UUID (§9.2), a root's audience cell must
match its row, and a descendant's reference must name a parent in the same
audience. Inserts, and rows present in a snapshot, supply their key, reference and
audience cells; updates may leave unchanged cells out.

#### 15.2 Unique values don't cross audiences

A unique constraint, or a shared key (§9.2), must not span audiences. A device
outside a circle can't see the circle's claim to a value, so it would decide
differently.

- **On a root table** it must include the audience column, as in
  `UNIQUE(audience, title)`: the same title in the store and in a circle are two
  separate claims.
- **On a descendant table** it must include the foreign key the table takes its
  audience from, since that parent fixes the audience.

`note_tags(note_id, tag_id)` is allowed: only someone who can see note 42 can tag
it, so two rows `(42, "urgent")` are always in the same audience.

`UNIQUE(file_name)` on attachments is refused. Ana's `plan.pdf` on a Gifts note
and Ben's on a store note would clash on Ana's device but not on Carol's, outside
Gifts. `UNIQUE(note_id, file_name)` is allowed.

Coven checks this when the database opens and after each migration, and refuses to
open with an error naming the table and the constraint.

#### 15.3 References stay readable

A row may point only at rows that everyone who can read it can also read:

- a circle's row may point at rows in the same circle, or the store's;
- a store row may point only at store rows.

So every device that reads a row can check its foreign keys, and §9.1 applies
unchanged. When note 42 is deleted, the devices in Gifts apply its foreign key's
action to pin 3.

Carol also has a circle of her own, Recipes:

```
store     notes  row 42   "Groceries"
Gifts     pins   row 3    note → 42        allowed
store     links  row 10   pin → 3          refused: Carol can't read pin 3
Recipes   pins   row 2    pin → 3          refused: Carol, in Recipes, can't read pin 3
```

Coven refuses a write that breaks this, on the device making it. That includes a
move: moving note 42 into Gifts while store link 5 still points at it fails with
`DbError::ReferenceAudience`, since link 5 would point at a row Carol can't read.

Which circle a row is in never depends on who is in the circle, so a change of
members never breaks a reference.

#### 15.4 Moving rows

A row moves when its root's audience column changes, or when a descendant is
pointed at a parent in another audience. A move is an ordinary write, such as
`UPDATE notes SET audience = …`, and commits at once on the moving device.

A move is a delete of the root and its descendants in the old audience, and a full
insert of them in the new one:

- devices in the new audience receive the whole subtree, not changes to rows they
  never had;
- devices that read only the old audience see a delete;
- which rows move is worked out against the database as it was before the write;
- moving them back later is a re-add (§8.3).

Ana moves note 42 and its attachments from the store into Gifts. Carol's devices
delete them; Ana's and Ben's insert them in Gifts. The moved rows' file references
stay as they were: each file's key travels in its row (§17.2).

**Generations count per audience.** `_coven_rows` has one row per table, key,
audience and generation. A move ends the row in one audience and starts it in the
other. Ana moves note 1 into Gifts; Carol, outside it, sees it deleted and adds
note 1 to the store again. The store's note 1 and Gifts' note 1 are now two rows,
each with its own generations, and Ben's edit in Gifts changes only Gifts' one.

**One key in two audiences.** When one key is present in two audiences on a
device, the app's table shows one of them, and the other is taken out like a
unique value's loser (§8.4, §9.2):

- the store's row wins over a circle's: Ana's devices show the store's note 1 and
  take out Gifts' note 1;
- of two circles' rows, the one whose current generation started with the smaller
  timestamp wins, the write `_coven_rows` records for that generation.

A device in both circles can show a different row for that key than a device in
only one, since each reads different rows.

#### 15.5 Running a circle

Circles are made, and members added and removed, by admin entries, under §10's
rules. Any member of the store can make a circle and is its first member. Its own
members rename it, add and remove its members, rotate its key and delete it; an
admin outside the circle can't.

Renames take effect in slot order, and the latest valid rename supplies the name.
A rename after the circle's deletion fails its check, because the target is
deleted.

Each circle has its own key, introduced and copied like the store key (§12.1).
Each copy is sealed to a member's current sealing key, never a retired one, and
authenticates both the member id and that sealing key. Copies are slots whose
routing names the circle, the key id and the recipient. The key is replaced
whenever someone leaves the circle (§15.8).

Everyone in the store can see that a circle exists, who is in it, who writes to
it, when, and how much. Only its members read what it says.

#### 15.6 Writes into circles

A circle's changes are parts of the same writes slot as store changes, each
encrypted with its own audience's key. One slot can carry Ana's store note and her
Gifts pin, with one signature over every encrypted part.

Applying such a slot:

- every device opens its header with the store key and checks the whole
  signature, including parts it can't read;
- the state the device has applied decides which parts it may read; what the
  author had applied when writing decides whether the author was allowed to write
  them;
- all readable parts apply in one transaction with the slot;
- parts for audiences this member isn't in are consumed without being opened.
  Not being a recipient is not a missing key;
- a missing key for a part the device should read stops the device at that slot
  (§5.4).

A write into a circle the member isn't in, or into a circle absent from the
membership this device has applied, fails on the writing device with
`DbError::NotInCircle`.

#### 15.7 Joining a circle

Dan has joined Household, and Ana adds him to Gifts. That needs two things:
Gifts' older keys, sent to him before the entry adding him (§11.5), and a reload
of the Gifts parts his devices consumed without opening while he was outside. His
overall applied slot is no evidence that he read them.

So Dan's laptop loads Gifts' announced snapshot and the slots after it, up to the
common target §16.5 describes, then commits Gifts' data and position together.
Until that commits, writes into Gifts fail with `AudienceReloading`. Other
audiences stay usable meanwhile. A missing key or snapshot is reported.

The same holds when Ben is added back after leaving (§15.8): his laptop consumed
Gifts' parts unopened while he was out, and loads that history before writing to
Gifts. Older keys stay readable to him. Adding him back may require sending him
keys introduced while he was outside; it doesn't make exposed keys usable for new
writes (§12.2).

A newly made circle has no earlier parts, so its first members load nothing.

#### 15.8 Leaving a circle

Ana removes Ben from Gifts with an admin operation. That slot changes Gifts'
membership. Its remaining members replace any key Ben was sent before writing new
Gifts data (§12.2). Ben keeps the rows and keys he already had.

**Edits made before he read the removal still count.** Removal from a circle
doesn't remove Ben's device or member from the store. He may still send Gifts
parts he queued before reading his removal, sealed with the old Gifts key he
already held, which exposes no new data to him. This relies on current members
being trusted about what they had read (§3). Once Ben reads his removal, new
writes into Gifts fail with `NotInCircle`, and he no longer snapshots Gifts. A
separate removal of his device or member, or a replacement of his keys, applies
the full cut-off (§14.1).

**A circle left empty is deleted** at that slot, whether its last member left it
or was removed from the store. The state before the slot decides this, and no
later operation can add a member to a deleted circle.

#### 15.9 Deleting a circle

Any member of a circle can delete it. The operation claims one admin entry; it
writes no rows and advances no generation.

From that slot on, Gifts' rows are taken out (§8.4), including late edits made
before the deletion that arrive afterwards. Their lost values name
`DeletedCircle` and the entry. A new local write into Gifts fails with
`DeletedCircle`.

Ana queued note 9 in Gifts before Ben's deletion claimed slot 50. If her member
and device are still active, her later batch is consumed and note 9 is taken out
with Gifts' other rows. The app can offer its values in another audience,
keeping its key (§8.4). A deletion is never reversed; making another circle
gives it a fresh id.

**Edge cases.**

- **Records stay.** Merge records, lost values and the evidence of where each
  value came from are kept, for old writes still arriving and for loading
  snapshots. Devices outside Gifts see the deletion entry, not values.
  Dismissing a lost value follows the ordinary write and owner rules.
- **Files.** Applying the deletion releases local records of where values came
  from that no visible row can name again. Queued files keep their own records and
  finish uploading (§17.7). Lost values, snapshots, writes and queue entries that
  still point at stored files keep protecting them; a deleted circle is not
  permission to delete its files.
- **Storage.** Deleting old slots and snapshots follows §16.6's coverage and grace
  period, and deleting files requires §17.8's complete reference checks.

---

## Part IV. Keeping storage bounded

Part IV covers what lives in storage besides the sequence, and how it stays
bounded: snapshots that let old history be deleted (§16), and the files rows
carry (§17).

### 16. Snapshots, the floor, and deleting history

The sequence would grow forever. A household that has used the app for five years
would have hundreds of thousands of slots, and a new phone would have to read
every one. Instead, devices take *snapshots*: the complete state of one audience
as of a slot (its *coverage*). Once every audience has a snapshot, the slots
before it can be deleted, and a device that has been away loads the snapshot
instead of the deleted slots.

This section follows that story in order: what a snapshot holds (§16.1), how one
is taken (§16.2), the extra rules schema raises and resets add (§16.3), the floor
that records what was deleted (§16.4), how a device reloads from it (§16.5), and
deleting history (§16.6).

#### 16.1 What a snapshot holds

A snapshot records one audience after applying through a slot. Its coverage is
that slot number, and it counts every slot up to it, including writes that were
consumed without effect, such as refused writes. A snapshot holds:

- the audience's rows, merge records and lost values;
- the evidence old writes still need: each deleted row's generations (§8.3), and
  each row change's *source verdict*, the record of whether the data it built on
  was kept or dropped (§9.5). Without them, an old edit arriving late could bring
  a deleted generation back, or refused data could become an accepted source.

A store snapshot also holds the signed admin entries, key copies and
announcements needed to know who was a member when, which keys exist, and which
schema raises and resets happened
([format.md D7](format.md#d7-snapshots)).

Every snapshot also records, for each device, the greatest claim number it
includes from that device and the SHA-256 hash of that claim's slot bytes (§5.3).
Two included slots can carry the same greatest number only when two
installations share one device id; the snapshot then keeps the hash of the later
slot.
Every slot through coverage counts: writes, admin entries, key copies and
announcements, valid or refused, including writes with parts this device can't
open and parts a reset ignored. A device with no claim included has number zero
and no hash. These numbers let a device settle a claim of any kind after its
slot is deleted. They tell it whether the claim landed, not whether the claim's
effects survived.

A snapshot leaves out everything that belongs to one device: local uploads,
journals, pending records (§19.1), file sources and caches. Reloading keeps all
of these. A reset clears pending records only as §19.1 describes.

A device snapshots only audiences it can read, and only slots it has applied.
Its own local writes that have no slot yet stay outside the snapshot.

#### 16.2 Taking a snapshot

A snapshot becomes usable in three steps, each one recorded before it happens:

1. the device journals the snapshot;
2. it uploads the snapshot's fixed bytes;
3. it claims a slot announcing it, naming its path, its coverage and its purpose:
   ordinary, a schema raise (§18.3) or a reset (§19.4).

So an announcement that counts always names a body already in storage. An upload
never announced supplies no coverage. If the operation that made it can't
finish, the journal deletes the upload, and keeps any failure to delete it
(§11.3).

**When.** A device asks for a new snapshot of an audience once the encoded write
parts since the last one exceed that snapshot's size, or 1 MiB if there is none
yet. Deleting history (§16.6) also asks for one when it needs coverage that
threshold hasn't reached.

**Coverage only moves forward.** An announcement's coverage must not be lower
than the coverage already announced for that audience. Of two announcements with
equal coverage, the later one is the one devices use. There is no deadline for
an upload; a slow upload simply announces later.

**Announcing and loading are separate.** Applying an announcement reads no
snapshot: the announcement is signed and carries the coverage, the purpose and a
hash of the snapshot. A snapshot is read only when a device loads it. The device
checks the body against the announced hash, then its signature and its prefix
([format.md D9](format.md#d9-sealed-objects)). So a snapshot whose body an
undone operation deleted never blocks a device that doesn't load it.

**Which snapshot a device loads.** It loads the newest snapshot that applies to
the audience's current schema version and reset. If that snapshot needs a key the
device lacks, needs an app update, or has a damaged body, the device waits and
reports why (§19.1). It never loads an older snapshot to get around the problem.

#### 16.3 Snapshots for schema raises and resets

A schema raise (§18.3) and a reset (§19.4) each replace an audience's state, and
each names a snapshot taken for it. These *prepared* snapshots, and the ordinary
ones around them, need rules so that a device joining later can always load a
snapshot and then find every slot it still needs.

Suppose Ana's phone captures an ordinary store snapshot at slot 300, Ben's
laptop raises the schema at slot 310, and Ana announces her capture at slot 320.
A device joining later would load a version-1 snapshot from after the raise,
then need slot 310's raise to replay, which may already be deleted. Coven
refuses Ana's announcement; the checked counterexample is [`slot4_refused`,
`stale_floor_strands_joiner`](proofs/sequence/CovenSequence/ReaderExamples.lean).

The rules:

- **An ordinary snapshot must cover the latest raise and reset** before its
  announcement, and its schema version and reset must match the audience's at
  its coverage.
- **A prepared snapshot counts only once its admin entry applies.** A raise or
  reset snapshot is announced first and then named by the raise or reset entry.
  Until that entry applies, its announcement can't support deleting history or an
  ordinary reload. An entry can name only a snapshot already announced.
- **A prepared snapshot must cover at least as much as every earlier
  announcement** for its audience, and must cover the slots of the raise and
  reset before it.
- **After losing a race, recapture.** If someone else claims the slot first and
  these conditions no longer hold, the device takes the snapshot again before
  trying the next slot.
- **An abandoned preparation leaves only history.** Its operation's journal
  deletes the body (§11.3). The announcement stays in the sequence but supplies no
  usable snapshot, and an announcement alone changes no rows, schema version or
  reset.

With these rules, a device that has an audience's key can always move forward
and reach the end of the sequence, without anyone holding deletion back for it.

#### 16.4 The floor

The floor (§5.2) is one signed record at `<store>/floor`. It names the first kept
slot (`first`) and, for each audience, the snapshot that audience relies on.
Before any floor exists, the sequence starts at slot 1.

- **Every named snapshot covers at least `first - 1`,** so nothing deleted is
  lost.
- **It names every audience whose deleted history still matters:** the store and
  every circle, even one the device writing the floor can't open.
- **Snapshots may cover different slots.** The store's might cover 560 and
  Gifts' 520, with `first` at 501.
- **It carries the original key-copy boxes** needed to open the store key. Their
  routing is visible, as in the sequence (§12.1); the snapshot references are
  encrypted ([format.md D8](format.md#d8-small-frames),
  [D9](format.md#d9-sealed-objects)). So a new device can find its copy before it
  holds any key.
- **It takes effect when written,** even while old slots still sit in storage.

**Reading with a floor.** A pass reads the floor when its next slot is missing,
and again before committing the slots it read; §6.1 and §6.2 give those rules.
One more case belongs here. Applying a schema raise sometimes means replaying
slots. If the replay needs a slot below `first`, the device instead reloads that
audience from the floor's snapshot. That snapshot was announced after the raise,
so it covers the raise. A device behind a floor snapshot's coverage can always
reload.

**A new device starts from the floor.** It opens its key copy, checks the
historical entry that bound that key to its member and the floor's signature,
loads the snapshots, and applies the kept slots after them (§13.1, §13.2).
Without a floor, it starts at slot 1.

#### 16.5 Coming back after a long absence

Ben's laptop was off for a year. The floor now starts at slot 501; Ben had
applied through 40. Reading slot 41, he finds it missing, reads the floor, and
reloads: he loads the floor's snapshots and applies the slots from 501 on. His
unsent edits keep their original timestamps and `read_through`, and are judged
exactly as if he had sent them a year ago. Being late doesn't refuse them.

**One target for every audience.** The floor's snapshots can cover different
slots, so the device picks one *target*: the highest coverage among the snapshots
it selected, or the slot it must at least reach, whichever is later. For each
audience it reads, it loads the snapshot, then reads the kept slots after it up
to the target. With the store's snapshot at 560 and Gifts' at 520, Ben's laptop
reads slots 521 to 560 for Gifts. It commits all the audiences together, only
after every one has reached the target, and then reads on from 561.

The target is fixed when loading begins. New writes or announcements don't
restart it. If deletion removes slots or a snapshot the reload still needs, the
device uses the newer floor's snapshots instead, but only a snapshot that
replaces the one it selected, never an older one. The floor's rules alone don't
guarantee that. Storage can delete the snapshot a reload selected once a later one
is announced, while the floor still names an older snapshot; switching to the
floor's snapshot would then move the reload's coverage backward
([`unchecked_reselection_goes_backwards`](proofs/sequence/CovenSequence/ReloadExamples.lean)).

**A reload commits all at once:** rows, merge records, lost values, source
verdicts and the applied slot, in one transaction. A failed reload leaves the old
state visible.

**Writing during a reload.** During an ordinary reload the app can keep writing.
When the loaded state commits, coven applies the device's queued writes over it.
Their numbers, timestamps, schema versions and `read_through` stay as they were.
It uses the queue as it stands at commit, including writes made while loading.

A queued write is *eligible* for an audience when it hasn't been consumed yet, it
hasn't been refused, it has a part in that audience, and its `read_through`
reaches that audience's latest reset. Each eligible write is applied over the
loaded state exactly once. A write already consumed in the loaded state is not
applied again. A write that isn't eligible stays in the queue anyway, to be
published as it was. If the device repeats this step, it rebuilds from the loaded
state again rather than stacking twice.

**When writes into an audience are refused.** Three kinds of reload replace an
audience with state the device's own writes didn't see: a reset (§19.4), a
breaking schema raise (§18.3), and a circle the device can newly read (§15.7).
While one of these runs, writes into that audience are refused with
`AudienceReloading` until it finishes, because they'd be made against state
about to be replaced. A write touching other audiences only commits normally.
During an ordinary reload, including one because the floor has passed this
device's next slot, no audience refuses writes.

Keeping a write's bytes isn't keeping its outcome. The queued writes are kept
exactly and the eligible ones applied exactly once, yet across one of these three
reloads a write can still come out differently from the same write made after the
reload installs:

- **a reset** the write never read ignores it (§19.4);
- **a breaking migration** can bring in a row whose key the write now conflicts
  with;
- **a newly readable circle** brings membership and rows that change whether the
  write is allowed and which keys it clashes with.

That is why these reloads refuse writes instead of queueing them
([`reset_counterexample`, `breaking_raise_counterexample`, `newly_readable_counterexample`](proofs/sequence/CovenSequence/QueueReloadExamples.lean)).

**Newly reading a circle.** When the set of audiences a device reads changes, for
example when Ben is added to Gifts, each audience, the new circle included,
loads its own snapshot and slots up to the common target. Then each is rebuilt
from its eligible queued writes. With good storage and every audience's key, all
of them can be loaded to any target between the selected floor snapshots'
coverage and the end of the sequence.

#### 16.6 Deleting history

Once every audience has a snapshot covering slots up to some point, and the grace
period has passed, any device with permission to delete in storage:

1. journals the new floor and the deletions it plans;
2. writes the new floor, naming the new `first` and each audience's snapshot
   together, with a request that succeeds only if the floor hasn't changed since
   it read it;
3. then deletes the slots below `first`.

**The grace period** is usually seven days from the latest announcement of the
snapshots chosen. It spares short absences a full reload. It is not needed for
correctness. Deletion never waits for an absent device, and no device has to
acknowledge anything. No deletion waits for the device that wrote the slots.

The rules:

- **Choose announced snapshots that apply** (§16.2, §16.3) and cover the slots to
  delete. Every audience with a part in a deleted write needs coverage. Missing or
  unchecked coverage waits, including for a deleted circle whose lost values or
  source verdicts are still needed.
- **A lost race changes nothing.** If the floor changed, the conditional write
  fails. The device reads the current floor and decides again. It never moves
  `first`, or any audience's coverage, backward.
- **A lost reply is settled by reading the floor** before deleting anything. The
  device deletes only slots the confirmed floor puts below `first`.
- **Failed deletions stay journaled and reported** (§19.1). A later floor update
  doesn't wait for them to finish.
- **Keep the snapshots still in use:** those the floor names, those a pending
  floor write names, and those a raise or reset still needs. A snapshot replaced
  by a newer one is deleted only after its replacement has passed the grace
  period and keeps the evidence still needed.
- **An audience nobody can snapshot has no deadline.** If no member able to read
  a circle comes online to supply the coverage, that circle's uncovered history
  stays. History it already covers is deleted without waiting for that member.

### 17. Files

Rows can carry files: songs, photos, documents. A file is far too large to travel
in a slot, yet it has to sync with its row, be readable only by the row's
readers, be readable a piece at a time while a song plays, and never put the
user's own original at risk. This section covers what the app declares, the two
kinds of file (§17.1), attaching one (§17.2), how it's stored (§17.3), its status
(§17.4), reading it (§17.5), the cache (§17.6), uploads (§17.7), deleting stored
files (§17.8), and what a device keeps about files locally (§17.9).

For each synced table that carries files, the app declares:

| Declaration | What it says |
|---|---|
| file column | which column refers to the file |
| size column | which column holds its size in bytes; a user-provided file is checked against it |
| retargeting | whether a row, once it has a file, may be pointed at a different one |
| content hash column | which column holds the file's *content hash*, the SHA-256 of its bytes; coven fills it in when a write attaches the file |
| where column | which column holds the file's fixed storage reference; coven fills it in (§17.2) |
| kind | user-provided or app-provided (§17.1) |
| download | whether devices download an uploaded file as soon as its row arrives (*eager*), or on first read |

The file declarations also belong to a *namespace*, which the cache budgets by
(§17.6, [api.md E2](api.md#e2-declaring-synced-tables)).

#### 17.1 Two kinds of file

A **user-provided** file is the user's own file at a path on their device, such
as a song in their music folder.

- Coven records its path, size and modification time. It never copies, moves,
  changes or deletes it.
- If it moves or changes before upload, coven reports it as missing or changed,
  before reading any of it.
- After a successful upload, reads on the attaching device use the cache or
  storage. Coven never reads the original again, and it stays where it is.
- Deleting a row deletes only coven's copies of its file, never the original.

An **app-provided** file is bytes the app hands to coven, which keeps and owns
them. The app can hand them over as a stream, so a large file never has to fit in
memory.

For both kinds, the first read records the whole-file content hash and a SHA-256
hash of each 64 KiB chunk of plaintext (the last chunk may be shorter). That read
happens when coven prepares a user-provided original or stages an app-provided
file. The chunk hashes stay on the device and are not part of the storage format;
every upload checks against them (§17.7).

#### 17.2 Attaching a file

Attaching a file is part of the write that changes its row. That write picks the
file's random id and key, records its uploader, and fixes its storage path,
`<store>/files/<device>/<file>`.

- The where column carries that reference from the start
  ([format.md D12](format.md#d12-files)). It doesn't say whether the upload has
  finished.
- The same transaction queues the upload, even with no storage connected.
- No later write marks the file uploaded, and nothing changes the row when the
  transfer ends. Pinning and caching change no file reference either. A file
  reference (`FileRef`) stays valid across upload completion.

So a slow upload can't overwrite a newer file. Ana attaches photo A to row 7, then
Ben replaces it with photo B. Ana's upload of A finishes later. It changes no row
and can't restore A over B; each file keeps the reference chosen when it was
attached.

**The four file columns move together.** A write that changes any of a row's
file, size, content hash or where columns writes all four, unchanged ones
included, so a row's file always comes whole from one write. Ana's phone and
Ben's laptop each attach a different file to row 7: the later write wins all four
columns, never one file's size with the other's hash.

- A row with no file has NULL in its hash and where columns, so both must allow
  NULL. The app never writes them; coven fills them in the write that attaches a
  file.
- Coven refuses a foreign key with set null on any of the four, checked when the
  database opens and after migrating: that action would change one column alone
  (§9.1).

**The key travels with the row.** An uploaded file is encrypted with a key of its
own, which travels in its row's where column, inside the row's encrypted writes.
So only the row's readers can read the file. Moving a row between the store and a
circle changes nothing about its file: the new audience's devices get the key
with the row.

#### 17.3 How a file is stored

The content hash is a column of the row and syncs inside the encrypted writes
like any other. Every device checks a downloaded file against it.

Uploading stores the file, encrypted with the attaching write's key, at its
already chosen path.

- The provider sees only a random name, never a hash of the content.
- Each upload is a copy of its own. Identical files attached to two rows are
  stored twice, and deleting one never touches the other.
- The key never reaches storage outside the row's encrypted writes, so a member
  who can't read the row can't read the file.

A file is encrypted in 64 KiB chunks; the last may be shorter. Its header records
the file size. Each chunk is encrypted and authenticated on its own, with its
index bound in, so a chunk can't be altered, swapped or reordered unnoticed, and
any chunk can be read and checked without the rest of the file.

#### 17.4 A file's status

Rows can arrive before their files, so the app needs to know whether a file can be
read yet. Status reads the fixed path's metadata in storage, then checks the
uploader's current state and any source failure known on this device
([api.md E8](api.md#e8-files-and-the-cache)):

| Status | Storage says | And |
|---|---|---|
| **available** | an object at the path, of the file's complete encrypted size | |
| **damaged** (integrity error) | an object at the path, of any other size | never available, never absent |
| **uploading on Ana's phone** | nothing at the path | Ana's phone is an active device and no source failure is known here; it may be offline or paused, and bytes need not be moving |
| **missing** | nothing at the path | the uploader was removed or replaced, or this device is the uploader and its source failed |

- A network or permission failure is an error, never proof of absence. With no
  storage set up, the call returns `NoStorage`.
- Other devices can't see Ana's source failure. Ana's phone reports it to its own
  app.
- What storage holds wins over a local source failure. No source failure is
  evidence that stored bytes are absent.

**Reads agree with status.** A read can still use a checked local source or cached
bytes. If it needs remote bytes that are absent, its typed error says uploading or
missing, names the uploader, and matches what status says. The app can ask for a
missing file to be attached again.

A ranged read's reply still reports the whole object's size (§20.5), so a read
checks that size as status does: any size but the file's complete encrypted size
is damage, even when the requested range is there. Every range a read requests
lies within that size, because the header is checked against the row first. So a
range the provider refuses as reaching past the object's end is the same damage,
never absence.

#### 17.5 Reading a file

The app reads a file through a file reference, taken from its row, naming the
file the row had then. A read checks the reference against the row first, so a
row changed since can't redirect it to another file. A write can check a
reference the same way before changing the row.

The app reads any byte range, at any offset, as a stream. Playing a song and
seeking within it are reads of different ranges.

- **A file on this device, or in the cache,** is read with positioned reads of the
  file on disk.
- **An uploaded file not in the cache** is read by fetching only the chunks that
  cover the range, with ranged requests to the provider.
  - Its checked header is reused across opens while it stays cached. If it's
    missing and the range starts at zero, it comes with the first chunks; seeking
    elsewhere may need a separate header request.
  - Neighbouring chunks are fetched together, up to 1 MiB per request.
  - Each chunk is checked as it arrives; a failed check fails the read.
  - Fetched chunks go into the cache, so reading a range again costs nothing.
- **While a file is read in order,** coven fetches chunks ahead of the reader, so
  playback doesn't wait on the network. Seeking costs only the chunks under the
  new position.
- **Offline,** reading a range whose chunks aren't cached returns the storage
  network failure and its native cause, for the app to show or log. Pinning a file
  ahead of time (§17.6) is how it becomes available offline.

**No key yet.** A file key travels inside its row. If the audience key needed to
read that row hasn't arrived, the write or snapshot holding the row waits with
`KeyUnavailable` before it yields any `FileRef`. Coven never starts a download for
a reference it doesn't know. Once the key arrives, coven reuses the encrypted
input it kept.

**When a read, pin or download fails.** Reads, pinning and eager downloads share
the verified cache ranges and one pending record per file (§19.1). A call that
fails returns its typed cause at once; the pending list records the first unmet
condition:

| Condition | Error | What happens next |
|---|---|---|
| no storage connected | `NoStorage` | |
| network or provider failure | the provider's `Storage` category | retried by its failure class (§17.7) |
| storage confirms absence, the uploader is active, no local source failure | `Waits(Object(path))` | waits for the upload |
| the uploader was removed or replaced | `FileUnavailable(FileMissingReason)` | needs the app to act |
| this device is the uploader and its source failed | `FileUnavailable(FileMissingReason)` | the upload retries (§17.7) |
| a complete chunk fails authentication | `Refused(Decryption)` | the same bytes are never retried on their own; an applicable coven update can permit a recheck (§19.1) |
| authenticated bytes disagree with the row's content hash | `Refused(ContentHash)` | as above |
| writing the local cache fails | `Failed(Disk)` | |
| the database fails | `Failed(Database)` | |

No failed range is marked cached or pinned.

Ben's laptop is downloading an eager file and has two verified ranges when Wi-Fi
drops. The list reports the network failure, and progress stays at those two
committed ranges. The retry fetches the rest. A bad final content hash would
instead leave a permanent refusal and publish no completed file or pin.

[IO and observable work](proofs/io.md) specifies where downloads are checked,
including disk-full failures injected through the real Rust reader. Its tests
must show that bytes are authenticated before they're delivered, and that a
complete in-order read checks the content hash before releasing its final
buffer.

#### 17.6 Cache and pinning

Each device caches uploaded files and chunks of them within a size budget the app
declares for each namespace. Each namespace evicts on its own
([api.md E8](api.md#e8-files-and-the-cache)). Over budget, the least recently
used files and chunks go first. Freeing space never fails a read.

**Pinning** keeps a file whole on the device regardless of the budget, until the
app unpins it.

- Pinning assembles a complete encrypted cache file from cached and fetched
  chunks, checks its content hash, and syncs it to disk before publishing the pin.
  Publishing replaces the separate cached chunks in one step. A cancellation or a
  crash leaves no partly downloaded file exempt from eviction.
- A file still queued for upload on this device is pinned at once. The
  transaction that retires its upload carries the pin to the stored file, which
  coven then keeps whole from storage like any pin.

The app can remove an uploaded file from the cache; that never touches storage.

**Eager downloads** keep their requested file references until each one settles,
failures included. Evicting a completed download doesn't request it again; a
changed reference can.

**Progress** for pins and eager downloads counts authenticated bytes committed to
the cache. The one-time reads and the subscriptions report the same data
([api.md E4](api.md#e4-reading)). Cancellation commits before it's reported;
failures stay in `pending()`.

Ana's app restarts after the provider confirmed two upload parts and her cache
committed three download ranges. Her first reads and subscriptions show those same
parts and ranges, even before storage reconnects. If opening replaced her old
device (§19.3), both ways of reading the reset notice return that stored notice.

#### 17.7 Uploads

The attaching write queues each file in the same transaction as its row. The file
waits in that queue until storage is connected and the upload succeeds.

**A separate worker uploads.** A file-upload worker serves that queue outside sync
passes. It wakes for queued work, a changed prerequisite or a due retry, and it
shares the connection's identity check, transfer limits and backoff. It uses the
file key fixed at attachment; it never chooses an audience key. A pass never
waits for file uploads (§6.6): finishing an upload changes no app row and doesn't
delay the write that attached the file. A finished upload supplies evidence for
deleting stored files (§17.8) without scheduling another full pass.

- **Same stops as writes.** File sends follow the stuck-device rule (§5.4) and the
  removal cut-off (§14.1). Parts the provider confirmed stay recorded for
  resuming; no time-based permission to send is needed.
- **Stopping sync** waits for transfers in progress. Requests made outside the
  pass count as well as those inside it (§21.1).

**What the queue keeps.** Each queued upload keeps the attaching write's fixed id,
key and reference, and the source location with its size, content hash, original
modification time where there is one, and the chunk hashes. It keeps no encrypted
copy.

- Retargeting the row can't replace the source facts of an upload already queued.
- Deleting or retargeting the row while its file uploads keeps the upload queued.
  It settles by its own reference, and only a confirmed upload retires it.
  Completion writes no row, so a deleted row stays deleted.
- `_coven_user_files` keeps an original's path while a current row or a local
  write not yet published can still name it. An applied delete releases it in the
  same transaction once no row can return to it. A queued upload owns its own
  copy of the source facts and keeps them until it settles.

**Every attempt checks the source.** Each attempt, including retries and resumed
provider sessions, checks the source's size and whole-file content hash, and a
user-provided original's recorded modification time, before sending any bytes.
As it streams, it checks each plaintext chunk against its recorded hash before
encrypting or sending that chunk. A missing or changed source fails the upload
with the file error and records `FileUnavailable` for this device and file
(§19.1). The local app sees the cause; a chunk that differs is never encrypted or
sent.

The reason is the encryption: a retry must encrypt the same plaintext under the
same key and chunk nonces (Appendix D.4). The per-chunk check guarantees that
even if the source changes after the whole-file check, without keeping a copy.
Size and modification time alone can't. A provider part may begin or end inside
an encrypted chunk, so coven reads and checks the whole plaintext chunk before
encrypting it and taking the bytes the part needs.

**Retries.** A missing, changed or mismatching source and a disk failure retry
under backoff (§6.7), checking the source before any provider request. Uploads
and eager downloads retry storage failures by their class
([api.md E5](api.md#e5-storage-and-sync)):

| Class | Retry |
|---|---|
| `Automatic` | under backoff |
| `AppAction` | when credentials or the connection change, with no timer |
| `Never` | not retried; the pending reason stays |

`retry_uploads_now` lets the app try recoverable uploads early. Progress never
depends on it.

**One request or many.** A file up to the provider's single-request limit, or
32 MiB, whichever is less, uploads in one create. A larger one goes up in parts,
through the provider's resumable or multipart upload. Providers require this
above a size: Dropbox, for example, above 150 MiB per request. Any object past the
provider's single-request limit goes up this way, a large writes slot or snapshot
included.

- The upload session records each part the provider confirms, and its byte count,
  before reporting progress. Resuming, and the app's `uploads()` and
  `subscribe_uploads()`, read that same state. Bytes written to a socket are not
  progress; there are no separate progress writes or in-memory channel.
- Pausing an upload is persisted too. An upload in one request stays `Waiting`
  until confirmed completion removes it from the queue.
- After a crash, a session the provider still has continues from its confirmed
  parts; reopening the app forgets none of them.
- A session the provider has expired starts over, reading and checking the
  source again, with the same id, key and chunk hashes.

**Completion** removes the queue row and its pending record in one local
transaction. It creates no app write, changes no row or file version, and doesn't
hold up the attaching write's own upload.

- A crash before that transaction retries the same upload. An occupied path
  counts as this upload only after its bytes compare equal (§19.3).
- Rows may arrive before their files. No write waits for file bytes.

#### 17.8 Deleting stored files

A stored file is deleted only when nothing can still point at its fixed path: no
retained row, lost value, snapshot, or queued or published write. Local rows,
waiting writes, upload queues and work a journal owns protect it too. Deleting
too early would leave a row naming a file that's gone for good.

- **Proof of absence must be complete.** The device reads and authenticates all
  the data needed to show the file is unreferenced. Incomplete or unreadable data
  prevents deletion.
- **If it can't read everything, it leaves the file.** When retained data belongs
  to an audience this device can't read, lacks a key copy, or fails validation,
  the device can't prove the file is unreferenced and leaves uploaded files in
  storage. It tries again only when that prerequisite or its own version changes,
  or the object is deleted. An unchanged pass doesn't download that data again.
- **Permission.** A device deletes uploaded files only with provider permission;
  missing permission stays visible (§19.1).
- **No idle listing.** No quiet pass lists files. File work follows changed
  references, uploads, deleted history or a provider cut-off. Announcements and
  the floor name the snapshots to inspect; file deletion never lists snapshot
  folders.
- **Ownership stays known.** A file's storage path and fixed reference carry the
  uploader's device id, so whose file it is stays known after the last reference
  disappears.

**Files nobody refers to any more.** A removed device can leave files in storage
whose attaching rows didn't survive. The device responsible for that removed
device's cleanup lists `<store>/files/<device>/` once, after the device's storage
access has been taken away (§14.2).

- The cut-off must also stop the device's in-flight upload sessions from
  publishing later. Then one complete listing finds every orphan file.
- An incomplete listing fails visibly and retries. A completed one isn't repeated
  after reopening.
- If access is restored, the listing no longer counts; a later cut-off needs
  another complete listing, because the device could upload again in between.
- Removal alone doesn't make a listing the last one while storage access is
  still unresolved.
- A device removed while its member keeps provider access has no cut-off and
  gets no listing: that account can still publish into its folder. Its orphans
  are found at the member's own later cut-off, whose listing covers the folder of
  every device the member held.
- Replacing a member's keys doesn't close a file folder either. The slot cut-off
  (§14.1) decides which writes count, and retained lost values keep their file
  references. Deleting files still needs the snapshot and reference evidence
  above and, for an orphan listing, a confirmed provider cut-off.

Ana removes Ben while his last photo upload is in flight. After his access is cut
off, she lists his file folder: the photo either landed before the cut-off and is
in the listing, or it can't land. Retained references and coverage still decide
whether she can delete it.

#### 17.9 What a device keeps about files

A device keeps local records of file sources, uploads, cache work and cleanup.
They don't sync.

A queued upload owns its fixed reference and source facts (§17.7). Ana attaches A,
then changes the row to B before A finishes uploading. Finishing A changes no row
and can't delete B's source or hashes; retargeting the row can't make A's upload
forget itself.

Native errors stay available while the process runs. After reopening, pending
work shows its persisted failure category (§19.1).

Coven writes file bytes on the device so that a crash never leaves a row naming
bytes that are gone, and never leaves bytes nobody will delete. Appendix C.3
lists those rules. Original user files are never deleted by any of this work.

---

## Part V. Change and failure

Part V covers what changes under a running store, the app's schema and coven's
own (§18), and what happens when things go wrong (§19).

### 18. Schema changes

The app owns its schema and changes it with migrations. But Ana's phone may run
the new version for a week before Ben's laptop updates, and Ben keeps editing
offline in the meantime. Every device has to agree on what the data looks like
after the change, and Ben's edits made in the old shape have to land somewhere.

Every write records the schema version it was made with. Coven sorts each
migration into one of two kinds by comparing the schema before and after:

- **An addition** only adds tables or columns. Devices on the old version keep
  working, and their writes apply everywhere. A device holds a newer-version
  write it can't read until its app updates.
- **A breaking change** is anything else: renaming, retyping or dropping, a new
  constraint, or a migration that changes rows. It raises the store's schema
  version.

This section covers how coven tells them apart (§18.1), additions (§18.2), raising
the version (§18.3), the write a migration makes (§18.4), the migrating device's
own queue (§18.5), late writes in the old shape (§18.6), lost values across a
migration (§18.7), two devices raising at once (§18.8), and coven's own schema and
format (§18.9).

#### 18.1 Telling an addition from a breaking change

Coven compares the schema before and after the migration, as SQLite lists it,
together with the app's declarations.

| The migration | Kind |
|---|---|
| adds only new tables or new columns | addition |
| adds an index, constraint or foreign key, or anything else | breaking |
| changes an existing table's `owner_only` declaration | breaking (so declarations are compared too, not only SQLite's schema) |
| drops or renames a table | breaking, since it may have synced |
| inserts, updates or deletes rows of synced tables | breaking, even if it only adds tables or columns |
| changes only local tables that keep their names (their columns, indexes and triggers), or views | neither: they don't sync |

A migration that changes rows is breaking because each device would otherwise
compute those changes from its own rows, and the devices would disagree.

The migration's statements decide its kind, whether or not a row matches on this
device. Its kind, and the converters kept for it, never depend on a device's
data. Deciding by the rows a device happens to hold makes devices disagree
([`row_classification_diverges`](proofs/queue/CovenQueue/ConversionChain.lean)).

The app also declares each migration's kind, and coven refuses a migration whose
declared kind disagrees with the kind found here (§18.3).

#### 18.2 Additions

Writes from devices on older versions still apply everywhere; they just don't set
the new columns. A device holds writes from a newer version until its app
updates, then applies them.

Ana's app adds `color` to notes. Ben, on the old version, can still edit titles
locally. Once his laptop reaches a slot from the newer version, it stops there
(§5.4) and waits for his app update before his queued titles can reach Ana.

#### 18.3 Breaking changes and the raise

**What the app declares.** Each migration changes the database, for example
`ALTER TABLE notes RENAME COLUMN title TO name`, and declares its kind,
`addition` or `breaking` ([api.md E13](api.md#e13-migrations)). A breaking
migration must also say what happens to late old-version writes, because there is
no safe default:

- `OldWrites::Convert(f)`: `f` is a *converter*. It turns an eligible row change
  of a late old-version write received from another device into the new shape,
  for example "note 43, title: X" into "note 43, name: X".
- `OldWrites::Drop`: such changes are kept as frozen lost values in their old
  shape (§18.6).

Neither choice ever changes an upload record.

**Running migrations.** An update runs every pending migration in one
transaction. Coven then checks the final schema against the schema rules (§9,
Appendix A) and the tables the app declares, which describe only the newest
version, and checks each migration's declared kind against the kind §18.1 finds.
A migration that changes only local tables may be declared `addition`. If a rule
fails, or a declared kind doesn't match, nothing commits; a kind mismatch fails
with `DbError::MigrationKindMismatch { version }`. Migration 1 makes `notes(id)`
and migration 2 renames `id` to `note_id`: only the final schema has to match
`key_columns(["note_id"])`. Reopening the database checks the kinds declared for
earlier migrations against the kinds they committed with.

Declaring the kind, and the choice for old writes, means late old writes are
never lost without the app having chosen it: a breaking change the app took for an
addition would otherwise drop them silently. The model refuses an
undeclared breaking change and a breaking change declared as an addition, and
commits no part of the run when either happens
([`undeclared_breaking_refused`, `declared_breaking_addition_refused`, `accepted_prefix_does_not_commit_on_mismatch`](proofs/queue/CovenQueue/ConversionChain.lean)).

**The raise.** A breaking change raises the store's schema version, which every
device shares. Whichever device's app updates first makes the raise, whoever's
device it is.

1. That device runs the migration on its database.
2. It takes a snapshot at the new version of every audience it can read (§16.3).
3. It announces those snapshots.
4. It claims the raise: one admin entry per audience it raises to the new
   version, the store's own entry raising the store.

For a circle it can't read, the first device of one of that circle's members to
update writes the circle's snapshot at the new version, with an entry raising the
circle. The circle's other devices reload from that one.

A raise must increase the current version. An operation that finds the version it
wanted already established adopts the announced state instead of claiming another
raise. A higher raise is checked against that state.

An update that runs several breaking migrations makes one migration write (§18.4)
and raises the store once, to the newest version.

**Devices on an older app.** An older app can't apply an audience's newer schema.
It records `UpdateRequired` and stops at that slot under §5's rule for stuck
devices, which also means it stops uploading. After updating, it reloads from the
raise's snapshot, and it sends its waiting old-schema records unchanged, including
records it never tried to send before migrating.

#### 18.4 The migration write

Everything a breaking migration changes in the synced tables is one write by the
device that runs it, at the new version: its *migration write*. It holds what
differs between the synced tables before and after the migration: each row
inserted or deleted, and each cell whose value changed.

- **A cell the migration left as it was keeps the write that set it,** so a late
  write competes with that write, not with the migration.
- **A cell whose references change counts as changed,** such as one a new foreign
  key now covers. The migration write sets it, naming the parent's generation
  after the migration (§9.1).
- **Only the tables before and after count,** not the statements between. A table
  or column after the migration is the one renamed to it with
  `ALTER TABLE … RENAME` from one that existed before, or else the one with its
  name. After `DROP TABLE b; ALTER TABLE a RENAME TO b`, the new `b` is the old
  `a`, and the old `b` is gone.
- So a renamed column keeps its cells' writes under its new name, and so does a
  table rebuilt by copying it into a new one and renaming that back. A table or
  column with no match after the migration loses them.

Ana's migration renames `title` to `name` and fills a new column `slug` from it.
Each note's `name` keeps the write that last set its title, and each `slug` is set
by the migration write.

**The migration write carries no row changes in its slot.** Its record in a writes
slot names the migration and holds nothing else; its changes reach other devices
only in the raise's snapshot. So a device that runs the same migration later and
then reloads from that snapshot changes nothing anywhere with its own migration
write, and neither does one that adopts the raise already claimed.

#### 18.5 The migrating device's own queue

When Ana's phone migrates, its queue may already hold writes made at the old
version, and she keeps writing at the new one before the raise is claimed. Each
has to go out in the right shape and order.

**The marker.** Migrating appends the migration write's empty record to the
queue, after the writes already there: the *migration marker*. Writes made after
it are new-version writes and wait behind it.

- **Old-schema writes go out unchanged, in write-number order,** even when the
  local app is ahead of the store, under the usual identity and key checks
  (§12.2, §14.1).
- **A pass's writes slot can carry those writes and the marker.** Later ordinary
  writes wait for a raise to their version, this device's or another's, or until
  a later local breaking migration captures them (below).
- **Before uploading the raise snapshot,** the device confirms the captured
  writes and the marker have landed. The snapshot leaves out app writes without a
  slot and includes the migration's effects over the slots it covers. Then the
  device announces the snapshot and claims the raise.
- **Each lost race** means reading and applying the slots in between. If the
  capture's coverage is now behind an earlier announcement, the device takes it
  again (§16.3). The queue doesn't have to be empty first.

Capturing freezes the queue up to the marker: later writes, and whether they're
eligible, don't change what goes out before the raise. The captured writes are
published once, the writes after the marker wait until the raise applies, and
every original field and the order are kept.

**A second breaking migration before the raise.** It captures the writes between
its marker and the previous one too. Those writes and both markers go out before
that raise; writes after the latest marker wait. Ana queues write 1, migrates
(marker 2), writes 3, migrates again (marker 4), then writes 5. Writes 1 to 4 can
claim one slot, the raise follows, and 5 waits for it. Reopening keeps the same
queued writes, captured version and wait.

**An addition before the raise.** An update that only adds, run while a raise
this device captured still waits to publish, moves that raise to the new
version. Each waiting capture is taken again at the new schema, through the
device's newest write, with no new marker. A capture is always at the version
its raise publishes, so the snapshot's header names it. Ana migrates to version
2 offline, then her app updates to 3, adding `color`. Her raise publishes
version 3, and Ben's app has to update to 3 to load it.

**Nothing in the queue is rewritten.** Migrating keeps every queued write, tried
or untried: its device, number, timestamp, `read_through`, schema version,
disposition and plaintext stay unchanged. A write already tried also keeps its
format and the keys it was sealed with.

**Judging late writes before the raise.** Until it claims its raise, a migrated
device interprets other devices' old-schema changes that its capture doesn't
cover using its own local capture's conversion, keeping the original records. A
recapture includes every write already judged. If another device claims the
version first, Ana's phone loads that announced state and judges the uncovered
writes the way every receiver does (§18.6). Its local migration's results never
become shared lost values.

#### 18.6 Late writes in the old shape

Writes made on the old version still arrive afterwards: Ben edited offline before
updating. If the migration declared `Convert`, Ben's late edit converts and
applies. If it declared `Drop`, the edit is kept as a lost value in its old shape,
so nothing disappears silently.

Conversion is for late old-schema writes received from other devices, including
writes their author had already tried to send that arrive after the raise. Once
Ben adopts the raise, his own device rebuilds its local state from his unchanged
records by the same rule every receiver uses.

**What the raise's snapshot supplies.** It supplies the effects of every write it
covers, with the consumed slots and the source verdicts (§16.1) kept separately.
Each old-schema write it doesn't cover is converted by the converters of the
breaking migrations after the write's version, up to the raise's, applied in
version order. Writes are taken in version order and causal order. Whether the
write's author had already tried uploading it makes no difference.

If that range holds no breaking migration, no converter is needed: the write's
eligible changes apply unchanged. So a write made at the newest breaking version
at or below the raise still applies when every later version only adds tables or
columns. In the example of §18.5, where version 2 is breaking and version 3 only
adds `color`, a write made at version 2 applies as it is under the version 3
raise, while a write made at version 1 still needs version 2's conversion
([`newest_breaking_write_admitted`, `retained_step_required`](proofs/queue/CovenQueue/ConversionChain.lean)).

**Checks before converting.** Coven never calls the converter on a change that
couldn't count anyway. For each row change, in order:

1. decide whether the change can be admitted at its schema version, without
   changing any values;
2. check owner permission (§9.6), using the current table declaration and the
   original creators coven kept;
3. check its foreign-key effects against the write's original schema (§9.1);
4. resolve the change's original data sources (§9.5).

Only a change that passes all four reaches the converter. A change whose sources
were dropped stays frozen in its original shape, with the dropped sources as its
cause. Unrelated changes in the same write, even in the same part, can convert
and apply. A write cut off by removal or key replacement (§14.1) stays refused;
conversion can't rescue it, and changes that build on it follow §9.5.

**When a migration declares `Drop`.** If any breaking migration in the write's
range declares `Drop`, coven keeps each original change as a *frozen* lost value:
kept in its old shape, outside the merge, naming the breaking version as its
cause. That includes a delete, which has no values. The write is consumed. Lost
values already frozen keep their original values and cause through later raises.
A converter supplied later can't revive them, and no converter rewrites any
upload, tried or untried.

**What doesn't exclude a write.** The writes a write recorded as read, including
its own device's earlier writes, decide the order it's received in, not whether
it's excluded. So there is no list of excluded writes to keep. Coven does keep,
for its schema and reset checks, the selected raise or reset, the snapshot's
coverage and each write's `read_through`. It keeps each change's source verdict
apart from its lost values, which the app may dismiss. A later snapshot covering
an excluded write's slot doesn't turn its dropped data into a source.

**Resets come first.** A part a reset ignored (§19.4) follows the reset's rule
before schema exclusion. Ignored parts, and migration markers a later capture
replaced, create no schema lost values. A change that would otherwise be
eligible but builds on an ignored part follows §9.5.

**Committing.** Converted changes, frozen originals, source verdicts, merged rows
and consumed slots commit together. A failed conversion or reload keeps the state
committed before it and reports the failure.

**An example.** Ana's announced snapshot covers a slot holding Ben's write 8. His
old-schema write 9, already tried, arrives after the raise and creates a note.

- With a converter, every device converts 9 on receipt, Ben's included once he
  adopts the raise. The upload's original bytes stay unchanged.
- If `Drop` has already excluded 9, a converter supplied later can't revive it.
  Ben's old-schema write 10 edits that note and renames an unrelated playlist
  whose generation and replaced cell survive. With a converter for 10, its note
  edit becomes a frozen lost value (its source, 9, was dropped), and its
  playlist rename converts and applies on every device.
- The same holds if the note's generation survived but the cell value 10
  replaced came from excluded 9. Later changes built on 10's playlist rename
  apply; changes built on its frozen note edit lose.
- With `Drop` for both writes, both keep their original changes as lost values.
  Ben's write 11, made after his reload, uses the announced state; having read 9
  and 10 doesn't exclude it.

Loading the established snapshot replaces values another device's local
migration computed; those never become shared lost values. Ordinary excluded
writes keep their original values as lost values, judged the same way by every
receiver.

#### 18.7 Lost values across a migration

**Rows already taken out.** A breaking migration deletes rows that removal rules
(§8.4) are already hiding, through the ordinary generation path.

- Its migration write moves each such row to the next even generation, a delete.
  Coven keeps the row's generation records, so late edits at the old generation
  lose to that delete.
- It captures one frozen lost value for the whole row, with the written values
  and the writes that set them, caused by this breaking version. A plain delete
  would keep only values no one had read, but the migration read them all.
- It uses the same shape as a lost value from an excluded write, and the
  generation delete removes the row's live taken-out record.
- Frozen values keep their old column names and plain values. Their references
  have no live parent links, even if the migration drops their foreign key.

**Dropping a table or column doesn't dismiss its lost values.** Ana's migration
drops `notes.color` while Ben's lost value "blue" is still pending. Every device
keeps that lost value under `notes.color`; opening the new schema doesn't silently
discard it.

- Each affected lost value is frozen as old-shape data before its schema records
  are removed. One holding a whole row keeps the whole captured row.
- It keeps its table and column names, key, audience, plain values, the writes
  that set them and its original cause. Live parent links are stripped; the old
  schema need not exist.
- Frozen lost values take no part in the merge or the current removal rules. The
  app can still read and dismiss them, and snapshots include them.

#### 18.8 Two devices raising at once

Ana and Ben update on the same afternoon, and both prepare the same raise. The
first valid claim establishes it. The other device reads that slot, adopts its
snapshot, and undoes its own unused work. Its announced snapshot, never activated
by an admin entry, is still its operation's to clean up: once the device checks
again and finds the raise taken, it deletes the body. The announcement stays in
history and supplies no coverage (§16.3). The losing device publishes no entry.

Raises and resets are always checked against the actual state just before their
slot, with §16.3's coverage rules.

#### 18.9 Coven's own schema and format

Coven changes too. Its own tables in the app's database migrate in place each time
a device opens for writing, before the app's migrations run. Everything coven
writes to storage records the *format* version it was written in, so an older
coven can tell a newer object from a damaged one and ask for an update instead of
reporting damage. A format change needs no admin entry, snapshot or reload.
Appendix C lists the rules for both.

### 19. When things go wrong

Sync can stop for many reasons: a missing key, an old app, a moved file, an
owner who hasn't come online. Coven never hides these and never guesses its way
past them. This section covers what the app sees (§19.1), reloading one device
(§19.2), a device that has been copied or damaged (§19.3), and resetting an
audience to one device's data (§19.4).

#### 19.1 What the app sees

Ana's laptop needs an app update to read slot 80. Her app shows that cause, and
her local data. The laptop can't read Ben's removal at slot 81, or send its queued
writes, until slot 80 applies.

Coven keeps one local record per thing that can't proceed, in `_coven_pending`.
The app reads the list with `pending()` and subscribes with `subscribe_pending()`,
offline too ([api.md E5](api.md#e5-storage-and-sync)).

- **Each record names its first unmet condition,** such as "needs an app update",
  "key not available yet", "waiting for Ana's device to unshare" or "source file
  moved". It stays until that condition changes or the work settles. Time passing
  never clears it.
- **A stuck slot has one record** naming its cause (§5.4). Coven doesn't add a
  wait for every later slot it can't reach.
- **The other subjects** are writes, key copies, snapshots, files, operations,
  history deletion, audiences, joins and the connection. A key copy's record
  names the exact recipient key.
- **Refused writes stay reported.** Writes refused by a cut-off keep their
  `WritesRefused` record (§14.1). No later slot reverses that verdict.
- **These stay visible:** missing keys, required updates, failed reloads,
  unavailable files and failed cleanup. A failed snapshot read never justifies
  loading an older state (§16.2).
- **A reset clears records.** Applying a new reset clears pending records along
  with its reload. A failed reload clears nothing.
  Journals and source evidence remain, and work still unresolved still reports its
  cause.

Sync status is one of: no storage set up, stopped, offline, syncing, synced,
blocked at a slot with a reason, or failed.

**Refusals and failures.** A *refusal* is permanent: coven downloaded an
immutable object completely and it failed a check. A failure to download it is
only a failed read, never a refusal.

- Calls and pending records use the same `Refusal` variant. A call keeps the
  native error; a durable record keeps its category.
- An applicable coven update permits one recheck of a refused object. Reopening
  the same version doesn't repeat it.
- A refused slot stops the device under §5.4's rule. No later floor, reset or
  admin entry can get past the refusal.
- A failed database transaction keeps the old state and applied slot. A
  corrupted database reports `Failed(LocalFailure::Database)` with the native
  cause on the call.
- If coven can't save pending state, it stops sync and database calls with
  `DbError::PendingPersistence`. The app must never see an empty list after a
  failed save.

**Retry timing.** Work that can retry on its own waits on the shared schedule of
§6.7, which survives restarts.

#### 19.2 Reloading one device

The app can ask a working device to reload from snapshots and the sequence
(§16.5), for example when its data looks wrong. It keeps its device id and its
unsent writes. A stuck device still stays stuck (§5.4).

- Claims in flight settle first, as usual (§5.1).
- Old-schema writes received from others are converted or kept as lost values
  under §18.6. The device's own upload records stay unchanged.
- Resets still decide which parts apply (§19.4).

#### 19.3 Another copy of the same device

Restoring a phone from a backup can leave two installations with one device id.
Both would sign slots as the same device, number their claims alike, and each
could mistake the other's slot for its own.

**The first defence is custody.** A device's id also lives in custody, such as the
OS keychain, that backups don't copy. Opening with missing or different custody
is refused, and the database is left unchanged.

**History is the second.** A copy that does open stops sending with
`SyncError::DeviceIdInUse` as soon as history shows another installation using
its id:

- it reads a new slot signed by its own id at a path its journal never attempted;
- it settles a claim and finds different bytes there signed by its own id
  (different bytes signed by another device are just a lost race);
- it settles from the floor's snapshot and finds a later claim number, or the
  same number with a different hash (§5.3).

Outside the sequence, an occupied path that never changes (a file, a snapshot, a
key delivery) counts as this device's only if its complete bytes equal the
recorded attempt. A mismatch at a path of its own stops that attempt. A failed
read for the comparison stays pending, and metadata alone never proves the bytes
equal.

A backup may keep going with intact custody unless history shows another copy, a
removal or a replacement. Coven detects only that evidence, not every clone. Even
where a backup forgot an attempt, content-bound nonces keep changed plaintext
from being sealed with a reused nonce (Appendix D.4).

**What the app is told.** Besides the evidence above, applying a slot that
replaces this device's id, removes the device or its member, or replaces its
member's keys stops it and tells the app (§14.1). Restoring never rescues the old
installation's queue (§13.1).

**Resetting a device.** The app can reset the device with
`CovenBuilder::reset_device` ([api.md E1](api.md#e1-opening)). Coven never
resets a device on its own. A reset discards the local database and unsent work,
takes a fresh id, loads storage, registers the new id as replacing the old one,
and tells the app what was discarded.

- Only the same member may replace its device id. Separate new devices may
  replace the same old id; each keeps its own fresh identity.
- **A damaged database** fails to open with `DbError::DamagedDatabase` and is
  reset the same way, from saved settings and custody.
- Before moving anything, coven checks storage and unlocks the member and store
  keys. A missing setting, an unavailable key or a failed connection leaves the
  damaged files in place and returns its typed error.
- Coven stops all local database use and holds the store's writer and reader
  locks. It moves the old directory aside whole, including SQLite's journals and
  local file sources, and keeps it for inspection, not as live state.
- Before moving it, coven records the new id, the replacement entry and the old
  directory's identity in the installation's setup state (§13.4). After a failure,
  the reset resumes with that recorded new id, and ordinary opening refuses the
  installation until it finishes. There is no separate recovery journal or
  marker.
- Only owed provider cleanup carries over: readable `DeleteAccessKey` work, owed
  revocations of account sharing, and the old installation's open invites, which
  become owed cleanup. They are copied into the new database, with its reset
  notice, before it opens. A database too damaged to read supplies none; its
  orphaned S3 keys are found as §14.3 describes.
- Coven loads storage under the fresh id, then returns the new handle. It never
  imports unsent rows or files from the damaged directory.
- Before returning the handle, coven commits a `_coven_device_reset` record, the
  `DeviceReset` notice, with the old and new ids
  ([api.md E5](api.md#e5-storage-and-sync)). Both ways of reading it report that
  unsent edits may have been lost, including after reopening. If the notice can't
  be stored, the reset fails: coven never hands back a working handle that hides
  the discarded installation.
- A reset with no storage connected creates an empty database with a fresh id.
  Competing first setups still contend for slot 1 (§10.3).

Ana's laptop has uploaded through write 12, but its damaged database may hold
write 13. Resetting loads the stored history through 12 under a new device id.
Coven doesn't guess which fragments of 13 are usable; the app gets the notice.

#### 19.4 Resetting an audience

If devices disagree about the data, the person can choose one device's version
and reset an audience to it: an admin for the store, a member of the circle for a
circle, one reset per audience. Ana asks to reset the store from her phone.

A reset is an operation (§11):

1. the device writes a snapshot of what it has (§16.2);
2. it claims the snapshot's announcement;
3. it checks again that the reset is allowed, then claims a reset entry naming
   that announcement.

Every device reloads from that snapshot when it applies the entry, the resetting
device included, so writes Ana's phone applied after writing the snapshot are
judged like everyone else's. Writes into the audience are refused with
`AudienceReloading` until that reload finishes (§16.5).

**What counts after a reset.** On every device, the author's included, only two
things count for the reset audience:

- the snapshot's rows and merge records;
- later writes whose `read_through` includes the reset slot.

A write outside the snapshot that hadn't read the reset entry is ignored for that
audience, even if it read every write in the snapshot. It is consumed so the slot
can advance, but its row changes aren't applied or recorded as lost. That is what
a reset is for. Its parts in other audiences follow their own reset and schema
rules. A write made after the reset that read an ignored pre-reset write still
counts: the reset deliberately discards that old history.

A device still uploads its unsent pre-reset writes, keeping their identities,
sources and the inner bytes it already tried. Like every other device, it ignores
their effects in the reset audience.

Ana resets from her phone's snapshot, which covers Ben's writes through 8. Ben's
laptop made writes 9 and 10 without reading the reset; 10 read 9.

- Ben uploads both. Neither changes the store or creates a lost value.
- After reading the reset, Ben makes write 11. It applies on every device.
- Ana's own writes, made after capturing the snapshot but before reading her reset
  entry, are ignored by the same rule.

**Later resets and raises** take effect in slot order, and each reloads its
audience. The entry before stays part of history; it is never dropped.

**Two resets at once.** If Ana and Ben both reset, the first claim applies. The
other operation reads it, checks again, and captures the newly read state before
claiming a later reset. No snapshot can lower the announced coverage (§16.2).

---

## Part VI. Storage

Part VI covers the cloud storage underneath: what coven needs from a provider and
how each provider supplies it (§20), what a store costs in requests (§21), and
what the proofs establish (§22).

### 20. What coven needs from a provider

Coven has no server of its own. Everything that keeps a store consistent, such
as exactly one device winning slot 41 or the floor never moving backward, rests
on a handful of things the provider does for it. This section names those
things, says why each is needed, and then describes how each provider supplies
them: signing in, where a store's objects live, how large objects are published,
how S3 services are told apart and checked, how access is shared, and how S3
member keys are made.

The order is the order a household meets them. Ana signs in and creates Household
(§20.2); coven checks the provider before trusting it (§20.8); devices then read,
write and list objects (§20.3 to §20.6); Ana invites Dan, which shares access
(§20.9), or on S3 makes him a key (§20.10).

Appendix E holds the reference detail: each provider's native calls, every S3
profile's behaviours and certification history, the detection probes, and how
each S3 service mints member keys.

#### 20.1 The requirements

A store lives on exactly one provider: an S3-compatible service, Dropbox,
OneDrive, or iCloud through CloudKit. Every member reaches it with their own
provider account, or on S3 their own access key. Coven needs seven things from
it.

**Create once.** Create an object only if its path is empty, and refuse an
occupied path without replacing its bytes. Ana and Ben both try to create slot
41; the provider must store exactly one and refuse the other. Without this, two
devices would each believe they hold slot 41 and history would split.

- It must hold under simultaneous claims as well as retries.
- At most one distinct byte string may ever occupy a slot, including a slot
  published in parts.
- It covers objects created in one request and, past the provider's
  single-request limit, objects sent through its resumable upload (§20.6).
- A slot path has competing writers. Each claim attempt fixes its bytes, and
  different bytes there are ordinary contention (§5.1).
- A path that carries one device's id, such as its files and snapshots, has one
  intended writer. An occupied one is accepted only if its bytes equal that
  device's recorded attempt; a mismatch stops the attempt (§19.3).
- `AlreadyExists` can reach the winner itself, after its success reply was
  lost. Comparing the occupied slot's complete bytes with what was sent decides
  it (§5.1).

A provider lacking create-once cannot hold a store.

**Conditional replace.** Replace the floor only if its revision is still the one
the device read, as one atomic step. A failed condition changes nothing; the
caller rereads the floor and validates the new one. Without this, two devices
moving the floor at once could each overwrite the other's floor. Setup tests both
a matching replacement and a refusal with a stale revision. A provider without
conditional replacement is refused.

**Complete reads, whole and by byte range.** A complete-object read streams
through bounded buffers, so the reader checks each chunk as it arrives without a
request per chunk (§6.5).

**Exact status.** Asked about one exact path, the provider answers either
"absent" or the object's complete encrypted size, its first-publication or
replacement time, its object id and its revision.

- A revision changes whenever a replacement changes the encrypted bytes, even if
  size and time stay the same. Otherwise a reader would keep stale bytes.
- A write of identical bytes may keep the old revision; Dropbox does. That
  changes nothing a reader holds.
- **An error is never absence.** A timeout read as "absent" would make a present
  slot look like the end of the sequence, or a stored file look missing.

**Listing.** List one prefix, with the time storage stored each object. Each
query is limited to that prefix, follows every page, and needs no change feed.
The sequence never depends on a listing (§20.4); listings serve files, setup
checks, and S3's check that a bucket exists (§20.5).

**Delete** an object by its exact path.

**Sharing,** where the provider has it: grant and revoke one member's access.
Dropbox, OneDrive and iCloud share with an account. S3 has no sharing; each
member has their own access key instead (§20.10).

#### 20.2 The providers and signing in

The four providers reach these requirements in different ways. In brief:

| Provider | Members reach the store by | Large objects published by |
|---|---|---|
| Dropbox | a shared folder, from their own account | its resumable upload |
| OneDrive | a shared folder, from their own account | upload to a temporary, then a copy that fails on an occupied name |
| iCloud (CloudKit) | a CloudKit share of the store's zone, from their own account | an atomic save once every part exists |
| S3 services | their own access key | per service: conditional completion, or a conditional copy |

**Least privilege.** Access starts at the least privilege that supports the
store. Full-account access exposes the person's unrelated files and needs the
provider's review, so an app offering only one-person stores need not register
it.

**Dropbox access levels.** Dropbox offers two kinds of app key. An *App-folder*
key reaches only the app's own folder, and Dropbox refuses sharing inside it. A
*Full Dropbox* key reaches the whole account and can share. The access level is
fixed when the store is created:

- With both keys registered, creating a store asks the person to choose shared or
  not shared. There is no default.
- With one key, that key decides.
- A Full Dropbox store can be shared later even if it starts with one person.

Ana chooses not-shared for her Notes store, so coven uses the App-folder key.
When she later tries to invite Carol, coven reports that Notes would have to be
relocated, before changing anything at the provider. Choosing shared when she
created Household used the Full Dropbox key and allows invitations at any time.

Switching keys isn't an in-place upgrade: moving files on Dropbox changes their
server times and revisions. So relocating a store is outside the access-level
choice. Full Dropbox apps also
need Dropbox's production review before serving people beyond development.

**Signing in to Dropbox and OneDrive.** Coven does the sign-in itself, so no
token passes through the app:

- The app gives coven one *presenter*: something that opens an authorization URL
  and returns the provider's redirect, or a cancellation. On desktops coven
  supplies the browser and a local listener; iOS and Android apps supply their
  platform's sign-in sheet.
- The app starts sign-in with `authenticate`, on the builder or an open handle.
  Coven builds the request, checks the redirect, exchanges the code, and holds
  the tokens.
- Until setup publishes the store, the sign-in lives in coven's session custody.
  Publishing commits it to the device's custody.
- Coven uses a token until the provider rejects it with HTTP 401. It then
  refreshes once, commits the new token, and retries that one request once. If
  the refresh fails or the retry is rejected again, the caller gets the failure.
  Every part of coven that uses storage follows this same rule.

Token expiry never consults the device clock. Ben sets his laptop's clock back a
year; his token still works until Dropbox rejects it, and one refresh lets the
waiting request continue.

**iCloud (CloudKit)** is built into coven on Apple platforms. The app supplies its
container identifier and the iCloud entitlement for it. The store is a zone in the
owner's private database, shared with members through a CloudKit share. Other
platforms refuse CloudKit at setup with `StorageFailure::ProviderUnavailable`;
they never crash and never ask the app to implement provider calls. Appendix E.1
gives CloudKit's native mapping.

**S3** services each implement the requirements a little differently. §20.7
explains how coven tells them apart and checks them.

#### 20.3 Where a store lives

Every object path starts with the store's id: slots, the floor, snapshots, files,
the setup check's test objects, and S3 detection's probes. The one exception is
the own-bucket marker `coven-bucket`, at the root of a bucket that a
bucket-scoped S3 service gives one store (§20.7).
[format.md D10](format.md#d10-paths) gives each object exactly one spelling.

So several stores can share one Dropbox or OneDrive folder, one CloudKit zone, or
one S3 bucket prefix where member keys can be limited to a prefix. Ana creates
Household and Ben creates Garden in the same folder; their store ids differ, so
their first entries land at different paths. On an S3 service whose keys reach a
whole bucket, each store gets its own bucket instead (§20.7).

Setup writes and reads only its own store's prefix. Where stores can share a
prefix-scoped location, other stores and unrelated objects outside this prefix
don't compete with this store. A few accesses are wider, and none reads another
store's data:

- creating a store that needs its own bucket lists the bucket's root, to check
  it holds nothing else, then creates the bucket marker; later setups read the
  marker (§20.7);
- S3's bucket-versioning query has bucket scope, because the owner's setting
  applies to every object (§20.7).

Reconnecting uses the recorded location. Setup never moves an existing store
elsewhere, because its other devices still use the location in their restore
codes.

Key copies are ordinary slots, found by reading the sequence, never by listing
(§12.1).

#### 20.4 Listings

Finding the sequence never lists anything. Slots, snapshots and members are found
by exact path and by signed slots. Ana's `files/` folder can hold 20,000 photos;
reading `seq/41` never enumerates them, or another store's objects.

Listings are used for files and setup checks, and they follow three rules:

- **List the exact place.** On Dropbox and OneDrive every directory in a coven
  path is a real provider folder, and coven lists that folder directly, never the
  configured root filtered on the device. S3 lists the exact key prefix. CloudKit
  queries the exact ancestor directory in the store's zone (Appendix E.1). Folder
  ids stay with the connection.
- **Skip what coven didn't write.** A store's folder can also hold a person's
  files and folders, native documents, shortcuts, packages, or another store's
  folder. A listing skips every entry that isn't a coven path. What the provider
  itself gets wrong still fails the listing: an entry outside the requested
  scope, missing metadata, or a failed or incomplete page.
- **A failed listing proves nothing.** It can't establish that every object was
  found.

Listings may lag behind publication. The *listing margin*, M, is how long after
an object is stored a listing may still omit it. The measured lag is a fact about the
provider, recorded at setup, not a rule the sequence depends on. Exact reads and
status must reflect completed publication. Deleting orphaned files needs a
complete listing taken after the provider cut-off, independent of ordinary lag
(§17.8). The setup check (§20.8) and the own-bucket check (§20.7) allow for the
margin.

#### 20.5 Status, times, revisions and checksums

Every status answer and every read carries the object's complete encrypted size,
native object id, revision, time and the time's resolution, plus any checksum
the provider returned. A ranged read still describes the complete object. File
status and upload confirmation use this exact-path call, never a folder listing.

**Times come from the provider,** never the device clock. An immutable object
keeps the time of its first complete publication; replacing the floor records
the replacement time. These times describe what the provider did. They don't
order slots and aren't deadlines for accepting writes or admin entries.

**Resolution.** Providers report times at different precisions: whole seconds on
S3 and Dropbox, milliseconds on CloudKit, whatever digits OneDrive returns. Coven
keeps each time with its resolution δ, compares at that precision, and never rounds
a time or invents an order within an equal time. It stores times as integer
seconds and nanoseconds, because a platform type such as Windows's 100 ns
`SystemTime` would drop digits.

On S3, listings and HEAD must give one object the same time. GCS listings include
milliseconds while HEAD and GET omit them, so coven truncates listing times to
whole seconds; otherwise one immutable object would have two times depending on
how it was read.

**Revisions.** Each provider has its own revision: Dropbox's `rev`, OneDrive's
`eTag`, CloudKit's change tag, and on S3 a generation, a version id, or a value
coven stores itself. A revision proves freshness, not equal bytes: an occupied
immutable path still needs §5's complete byte comparison.

**Checksums** name their algorithm, encoding and scope (whole object, or a
multipart composite). An ETag or revision is never taken to be a checksum, and a
checksum's scope is never guessed. Metadata and downloaded bytes must describe the
same revision.

Appendix E.11 lists each provider's time field, resolution, revision and
checksum.

**Edge cases.**

- **OneDrive reads twice.** S3 and Dropbox downloads carry their metadata, so they
  need no separate status request. OneDrive fetches exact metadata first, and for
  replaceable objects checks it again after downloading, failing if a concurrent
  replacement changed it.
- **S3's bodyless 404.** An S3 HEAD miss doesn't say whether the object or the
  whole bucket is missing. Coven checks the bucket with a listing that should
  come back empty: its prefix is the exact object key, under the configured
  prefix and store id. HEAD Bucket isn't used, because it needs bucket-list
  permission without a prefix condition, which a store-scoped member key lacks.
  Absence still comes from the HEAD, never from the listing, so a miss is two
  requests. Known profiles list with ListObjectsV2; a custom profile using V1
  lists with ListObjects.
- Any check that the container exists is counted separately (§21.1).

#### 20.6 Publishing a large object

A small object is one create request, and create-once applies to it directly. A
large one, past the provider's single-request limit, goes up in parts. That opens
three gaps, and coven closes each:

- **Unfinished uploads can be visible.** If a half-sent slot could be read, a
  device could apply a fragment.
- **The final step may not be create-once.** Some services complete a multipart
  upload over an occupied name.
- **The time may be wrong.** Some services date a multipart object from when the
  upload *started*. An immutable object's time must be its complete publication.

Where a provider's own upload closes all three, coven uses it: Dropbox's
resumable upload, CloudKit's atomic save of a record once all its parts exist,
and on most S3 services a multipart completion carrying the create-once
condition (*conditional completion*).

Where it doesn't, coven uploads to a private *temporary* and publishes it with one
server-side copy that refuses an occupied name. OneDrive's upload sessions expose
unfinished items, so OneDrive always publishes this way. AWS dates multipart
objects from their start, and GCS doesn't enforce create-once on completion, so
both copy too. The copy's completion is the object's publication time.

How a temporary is kept safe:

- It lives at `<store>/.coven-uploads/<session>`, a reserved place no coven path
  uses, and listings of published objects skip it.
- Its name is recorded before anything is sent, and it stays recorded until it is
  deleted. A retry or resume derives the same name, so a crash never strands a
  temporary that no record names.
- After a successful or definitively refused copy, the temporary is deleted
  before the result is reported. A network failure or throttling keeps it for
  the same session. A failed deletion fails the call, keeps both errors, and
  leaves the record for a retry.
- A lost copy reply is settled by reading the destination. On S3, this session's
  marker there means this session's bytes are stored, because a session's bytes
  are fixed (Appendix E.10); OneDrive compares the complete bytes (Appendix E.2).

Copy publication is only as good as the provider's copy. A copy that checks a
free name and then writes it as two steps lets two racers both succeed
([`check_then_write_two_winners`](proofs/storage/CovenStorage/CopyPublication.lean)).
That is what certification's races test (§20.7). A related trap: even when
copies are exclusive against each other, a conditional PUT that checks a free
name, then writes after a copy landed, is accepted beside the copy
([`put_racing_copy_two_winners`](proofs/storage/CovenStorage/CopyPublication.lean)).
SeaweedFS behaves exactly this way, so on SeaweedFS coven publishes *every*
object by copy, small ones included.

One S3 `CopyObject` copies at most 5 GiB. On a profile that publishes by S3 copy
(AWS, SeaweedFS), a larger upload is refused before any request with
`StorageFailure::CopyTooLarge`, because it can't be published atomically there. A
batch, snapshot or file past that limit waits with that named failure. The
sequence format adds no cap of its own on batch size.

Appendix E.10 gives the full mechanics: session records, the every-object copy
path, temporaries shared by concurrent attempts, aborts, and what concurrent-
mutation errors do to a session. Appendix E.2 covers OneDrive's sessions, and
Appendix E.1 CloudKit's.

#### 20.7 S3 services

S3 is a protocol many services implement, and they differ on exactly the points
coven depends on: which header makes a PUT create-once, whether multipart
completion honours it, how multipart objects are dated, and how checksums and
revisions come back. Garage, for example, accepts a write to an occupied name, so
it can't hold a store.

##### Settings and profiles

Coven describes a service's behaviour as data: `S3Settings` (api.md E5), one
choice per behaviour. Signing, the create-once header, how objects are published
(copy or conditional completion), how multipart objects are dated, the checksum
policy, the listing protocol, the revision kind, the response headers, and
whether reads ask for checksums. Each behaviour is implemented once in coven's S3
adapter; apps don't supply capability flags or service-specific code.

A location's *profile* is one of three:

- **none:** the endpoint's host picks the service;
- **a known service, by name;**
- **a custom profile,** carrying its own complete `S3Settings`.

The location records the profile as given, so restore codes and every other
device behave identically.

The known services are AWS S3, Google Cloud Storage (GCS) through its XML API,
Cloudflare R2, and four self-hosted services: RustFS, SeaweedFS, Ceph RGW and
Versity Gateway. Hosted services are recognised by their host; self-hosted ones
have arbitrary addresses and must be named. A name or custom profile never
overrides a known host. Appendix E.3 gives the selection and validation rules,
and Appendix E.4 each profile's behaviours.

An endpoint that matches no known host and names no profile has no AWS fallback.
Setup detects its behaviours (below) and records them as the location's custom
profile; the location setup returns and records carries those settings.

##### A member key's reach, and a bucket of its own

Each member has their own S3 access key, limited as narrowly as the service
allows:

- **To the store's prefix:** AWS, by IAM `s3:prefix` conditions; GCS, by a
  managed folder per store; RustFS, SeaweedFS, Ceph RGW and Versity Gateway, by
  policy `s3:prefix` conditions. Stores on these services can share a bucket and
  base prefix.
- **Only to a whole bucket:** R2, whose API tokens are limited to buckets, never
  to a folder inside one. A custom profile says nothing about keys, so it is
  treated the same way.

On a bucket-scoped service a member of one store could delete another store's
encrypted objects in the same bucket. So there, **every store has its own bucket
from creation,** whoever makes its keys, by hand in the provider's dashboard or
through coven. Ana creates Household in the R2 bucket `household`. Creating Garden
in that bucket is refused; Garden gets the bucket `garden`.

The rule applies from creation, even to one-member stores, because moving a store
into its own bucket at its first invite would mean copying all its data. It is
enforced like this:

- The location's base prefix must be empty. Any other prefix fails before any
  request with `StorageFailure::OwnBucketRequired { reason: Prefix }`.
- A store's first setup, before any location is recorded, lists the bucket's root
  with delimiter `/`, before detection or any other write. Any object or folder
  other than the store's own `<store>/` and the bucket marker `coven-bucket`
  refuses setup with `OwnBucketRequired { reason: Occupied }`. A retried
  creation that already wrote under `<store>/` passes.
- Then, after detection and the provider check pass (§20.8), and before
  publishing any store state or recording the location, first setup claims the
  bucket: it creates `coven-bucket` at the bucket's root once, with the same
  create-once request slots use. Its complete bytes are the store id as a
  lowercase hyphenated UUID, 36 ASCII bytes with no newline
  ([format.md D10](format.md#the-bucket-marker)).
- If that create succeeds, setup proceeds. If the name is taken, setup reads the
  marker exactly: the same bytes mean this is a retry of this store's setup,
  which proceeds; any other bytes refuse with
  `OwnBucketRequired { reason: Occupied }`.
- Reconnecting, restoring and joining use the recorded location. Besides their
  usual checks they read `coven-bucket` exactly and require it to name this
  store; a missing, malformed or other store's marker refuses with
  `OwnBucketRequired { reason: Occupied }`. They neither list the bucket's root
  nor create a missing marker. A failed read keeps its provider cause.
- The marker is never replaced or deleted. A failed setup leaves it for the same
  store's retry. Deleting a store from a device (`delete_store` or
  `discard_store`, [api.md E1](api.md#e1-opening)) leaves the marker in storage,
  like the store's other objects.

Why both checks? The root listing finds objects stored before setup, but only
those older than the listing margin: anything stored within the margin may be
missing from it. Ana and Ben could each start a store in the bucket `household`
at the same moment, and both listings would look empty. The marker closes that
gap. Create-once lets only one of them claim `coven-bucket`, so at most one
store's setup passes per bucket, however far listings lag
([`at_most_one_store`](proofs/storage/CovenStorage/BucketClaim.lean)).

##### Detection

Detection works out an unknown service's behaviour by trying each one against the
person's own bucket with their key. Setup runs it on every S3 location; the app
may also run it beforehand to prefill advanced settings (`detect_s3_settings`,
api.md E5).

- It writes only fresh objects under `<prefix>/<store>/.coven-check/<run>/`, where
  `<run>` is a fresh random UUID. That reserved place is distinct from every
  coven path, and listings exclude it, so a probe left by a killed process never
  shows up as a published object.
- Afterwards it deletes its objects and aborts its multipart uploads, also after
  a failed probe. A failed cleanup fails the call, keeping both causes.
- It returns the detected `S3Settings` and one typed result per probe: whether
  the behaviour held, with a native outcome that names no key, endpoint or
  object.

Appendix E.7 lists the probes, their waits, and how the settings are chosen from
the results.

Outside setup, connecting a location with an unknown host and no profile fails
with `InvalidConfiguration`. Reconnecting this device with the same location and
no profile reuses the recorded location's settings, which every other device
already uses, rather than detecting again.

##### Versioned buckets

A bucket with versioning on can break create-once in a way sequential tests
don't see. A versioned Ceph RGW 20.2.4 bucket accepted concurrent conditional
creates of one name as separate versions: both writers got success while readers
saw only one writer's bytes. Sequential setup probes passed that same bucket.

So a versioned bucket needs its own certification. Setup reads the bucket's
versioning state. `Enabled` or `Suspended` requires the selected profile to hold
a passing certification with versioning on; otherwise setup refuses with
`StorageSetupError::S3VersioningUnsupported`. Detected and custom settings never
inherit this certification, even when they equal a known profile's settings.

Setup never changes the owner's versioning setting. The owner must keep the
bucket in a supported state while it holds the store; every setup reads it
again. Coven always addresses current objects; it never picks an older version to
repair a wrong publication. Appendix E.9 gives the full rules, including an
unreadable versioning state.

##### Certification

A service is *certified* when coven's live suite has raced many writers against a
real bucket and every race kept exactly one winner. Sequential checks aren't
enough: SeaweedFS 4.48 refused occupied names one at a time, but two racing
conditional multipart completions of one name both succeeded. Its earlier
certificate was withdrawn, and its profile now publishes every object by copy,
which passed.

Today:

| Service | Certified |
|---|---|
| GCS | yes, versioning off only |
| R2 | yes (R2 has no versioning) |
| RustFS 1.0.1 | yes, versioning off and on |
| SeaweedFS 4.48 | yes, versioning off and on, publishing every object by copy |
| Ceph RGW 20.2.4 | versioning off only; fails the versioned races |
| Versity Gateway 1.9.0 | yes, versioning off and on |
| AWS S3 | no: there is no live account to run against |

Certification never exempts a connection from setup's own checks (§20.8). Appendix
E.4 gives each profile's behaviours, E.5 the certification history, and E.6 how
to run the suite.

**Edge cases.**

- **Clock skew.** `RequestTimeTooSkewed` means the device clock is off, not that
  access was denied. Coven retries once, signed with the server offset the
  response's `Date` states. If that retry still fails for skew, it reports
  `ClockSkew` with the provider's cause (api.md E5). Ben's laptop signs with
  yesterday's date; the first reply supplies the offset, and the one retry uses
  it without asking Ben to change his keys.
- **Concurrent-mutation errors** from AWS, GCS and Ceph RGW mean another write is
  in progress, never throttling. Appendix E.10 names them and says what they do to
  an upload session.

#### 20.8 Checking a provider before use

Every storage setup, including reconnecting an existing device, checks the
provider with the supplied credentials before it prepares an admin claim or
commits credentials, keys, location or restore code. A check that runs against
the real server catches what a service's claims and its profile can't.

On S3 the **safety check** runs first. Detection runs, and whatever chose the
settings, detected, named or custom, they must agree with what detection observed.
After the versioning check (§20.7), setup refuses with
`StorageSetupError::ProviderCheck`, naming the first failed check in this order:

| Check | Fails when |
|---|---|
| `CreateOnce` | the single-PUT condition wasn't enforced, or changed the occupied object's bytes or time |
| `MultipartCreateOnce` | the multipart publication method didn't refuse an occupied name with bytes and time unchanged, or, for conditional completion, an unfinished upload was visible |
| `PublicationTime` | multipart objects aren't dated by complete publication (conditional completion with a start timestamp, or a copy not dated later than its source), or the recorded multipart timestamp contradicts the observed one |
| `Behavior` | another setting contradicts a probe result (Appendix E.8 lists the cases) |

A manual override that contradicts the
results is refused this way, never silently accepted. Where no setting makes a
check hold, no override can either: a server that accepts writes to an occupied
name, such as Garage, is refused with `CreateOnce`.

Then the **provider check** runs on every provider:

1. Create a fresh sealed test object, and require a second create to be refused.
2. Read it back whole and by range.
3. Run its scoped listing query, which must succeed. If the listing returns the
   test object, its size must agree. Its absence within the listing margin is
   allowed, and proves nothing about completeness.
4. Delete it, and confirm absence by an exact-path read.

It also tests conditional replacement of the floor (§20.1). A failure names the
failed check and leaves the previous connection in place (api.md E5).

Every S3 location, whether known, named, custom or detected, runs both checks,
including `CreateOnce`. Accepting a second create is a protocol failure even if
the service claims compatibility.

#### 20.9 Sharing with accounts

On Dropbox, OneDrive and iCloud, only the member whose account holds the store
can share it or take sharing back. So:

- that member's devices make invites (§13.2);
- any admin can remove a member, and the owner's device takes that member's
  access back when it applies the removal (§14.2).

Coven promises the access the applied sequence intends. Provider requests take
time and can fail, so actual access need not match that intent at every instant.
The work starts as soon as the owner's device applies the entry.

The owner's device keeps actual access close to intent this way:

- It sends grant and revoke requests one at a time.
- Before each request it reads the current intended access. If a later entry
  changes what's intended while a request runs, it lets that request finish. If
  a completed request has since become obsolete, it records and sends the
  opposite request before reporting that account's work complete.
- Pending and failed requests stay in the operation journal (§11.4) and the
  pending list (§19.1).

Sharing is per account, not per invite: taking access back takes the account's
access, whatever shared it. So the owner's device leaves an account shared while a
member in its store reaches storage through it, or while another of its own open
invites names it.

**Edge cases.**

- An invite made at the same moment on another of the owner's devices can lose
  its access this way. The invited device's join then fails with the provider's
  permission error, and an admin invites them again, as after any removal (§14.2).
- Taking access back removes only what grants that one account. Grants through
  a parent folder, public links, groups, unidentified grantees and the owner's
  own grant, and any share that also grants others, are left in place and
  reported to the owner as `AccessRemains`; the owner changes them at the
  provider.

#### 20.10 Making S3 member keys

S3 has no sharing, so each member gets their own access key, and removing a
member deletes it (§14.3). Creating and deleting keys is specific to each service
and needs the storage account's admin access.

**Admin access is borrowed, never kept.** Coven holds it in memory for one
operation. It never enters the store's slots, invites, logs or device custody.
Member credentials stay separate and follow their ordinary custody rules. Having
provider admin access doesn't make anyone a store admin.

The person chooses where admin access comes from: a signed-in CLI and its profile
or account, a browser sign-in, a pasted admin key, or guided console steps. Coven
reads no CLI configuration and runs no CLI credential helper until the person
chooses that CLI. With guided steps the person creates or deletes the key in the
provider's console, shown the bucket, the store's prefix and the member policy
filled in; a custom profile always uses guided steps. With any other source coven
makes the provider calls itself. Both paths record member access and pending
deletions the same way (§14.3), and a retry that needs admin access asks for it
again.

**Making a key is one journaled operation (§11):**

1. Name the new key from a fresh random id. A mint deletes nothing, so a second
   live copy of the database never takes a key this one made.
2. Make the key, limited to the store (the *member policy*, Appendix E.12).
3. Check it from the member's side: it can create, read, list and delete inside
   the store, and is refused just outside it. A key that reaches further fails
   with `StorageFailure::KeyBeyondStore`.
4. Only then does its secret join the journal.

A failed mint or check rolls back what that call created and reports the
failure, with the rollback's own failure if that fails too. Admin access is
released once the key is checked, or when the call fails; for a key replacement,
when the replacement ends (§14.3). After a restart, a step that needs admin
access waits with `PendingReason::AdminAccessRequired` until a retry supplies it
again.

Each key's name carries the store and the device that made it, so §14's reclaim
can list a store's keys and find ones nobody recorded. A failed or incomplete key
listing is an error, never a shorter list.

Appendix E.12 gives the admin-access sources, each service's minter, the member
policy, the naming, the listing calls, the member-side check and its retry
windows, and the live evidence.

### 21. Costs

Coven is meant to cost almost nothing when nothing is happening, and to cost in
proportion to what changed when something is. This section counts the requests,
and says what else is bounded.

#### 21.1 Counting the requests of a pass

A *quiet pass*, where nothing arrived and nothing is queued, makes **two
requests**: read the next slot, which is absent, and read the floor. It lists
nothing and writes nothing. Ana's quiet store costs those same two reads with
three devices or twenty, and with 20,000 files or none.

Each kind of work adds its own requests:

| Work | Adds |
|---|---|
| a slot arrives | its read; the floor read after the final miss also checks it before commit |
| a claim | its create, and a floor read after it lands |
| losing a race | the occupied slot's read, and each further attempt; there is no bound on how often a device can lose |
| admin preparation | its actual requests: key copies, access changes, deliveries, snapshot uploads |
| a snapshot | its upload and its announcement claim |
| a reload | the floor's named snapshots, and the slots after them it needs |
| deleting history | one conditional floor write, and the deletes |
| files | their own reads, upload parts and deletes, outside the pass |
| reading an object | one bounded stream; a snapshot's start takes at most two range requests and the rest one more stream (§6.5) |

If Ana added one note, Ben's next pass makes three reads: the slot, the miss
after it, and the floor.

A device far behind finds the end of the sequence in a logarithmic number of
existence checks (§6.1). Those checks only find the end; applying the missing
history still reads each needed slot, or reloads from snapshots.

**Extra requests are counted too,** and reported separately from the pass's own:
provider metadata requests, redirects, listing pages, native asset parts, upload
parts and retries, including failed requests. OneDrive's metadata fetches and
rechecks (§20.5) and any container-existence check count here.

#### 21.2 What else stays bounded

- **More history or files never make a quiet pass work harder.** A quiet pass
  decodes no history and commits no database transaction.
- **Custody is unlocked once per open handle.** Sync passes don't repeat
  passphrase derivation.
- **The database writer is never held** across a network request, a custody
  call, decoding, a timer or file IO, so the app's own writes never wait on the
  network.
- **Retries** follow the persisted delays of §6.7: 1, 2, 4, … seconds up to five
  minutes. Finishing a pass doesn't reset a delay.

#### 21.3 How the costs are tested

Tests count requests, bytes, custody derivations, decoding and durable commits.
They cover reopening, contention, interrupted reads, blocked slots and floor
changes.

### 22. What's proved and what isn't

Some of this design's rules are checked by machine. The Lean models under
[proofs/](proofs/) state the rules as small programs and prove properties of
them: that a claim lands exactly once, that the floor check is needed and enough,
that every device converges, that a removed member's later writes never count.
Appendix G lists every such claim with its theorems, their file, and the section
of this document it supports.

What the models cover:

- **The sequence:** claiming and settling, the floor, reading, deletion,
  operations, causality, authority, unique values, owners, the cut-off,
  snapshots, reloads and schema raises.
- **The merge,** and conversion of old writes.
- **Pieces of storage behaviour,** such as copy publication, and S3 key
  minting and its admin access.

What they don't cover:

- **The Rust code.** A proof says what its theorem states about the model, under
  the model's assumptions. Tests run the Rust against the sequence, merge,
  conversion and storage models on generated cases and compare the answers; that
  is evidence, not proof.
- **Their own assumptions.** The models take as given correct cryptography,
  honest snapshot contents, and providers that behave as §20 requires.
- **The providers.** Whether a real service really is create-once is checked
  live: certification races writers against each service (§20.7), and every
  setup re-checks the server (§20.8). A live sample measures listing lag; it
  doesn't prove the listing margin.
- **The composition.** The merge and conversion proofs cover their stated inputs;
  they don't establish the whole storage protocol built from them.
- **Prepared snapshots loaded from the floor.** The models load ordinary
  snapshots from the floor and prepared snapshots (§16.3) at their raise or reset
  entries. A device loading a prepared snapshot directly from the floor is not
  modeled.
- **What a stopped device sends.** The model proves a stopped device takes no
  apply or reload step (§5.4). That it sends nothing, and how it recovers once the
  cause clears, are outside the model.

CI builds every proof from scratch, with no unfinished proof and no axiom beyond
Lean's own (Appendix F.6).

---

## Appendix A. SQLite features in synced tables

Each SQLite feature whose meaning changes when devices write offline and merge
later. "Refused" means coven refuses the schema when the database opens and after
every migration.

| Feature | Status |
|---|---|
| Keys SQLite chooses itself (integer rowid aliases) | refused; integer keys derived from the content are allowed (A.1) |
| Tables without a primary key | refused (A.2) |
| Primary-key columns that allow NULL | refused (A.3) |
| Independent keys with no text column for the UUID | refused (A.3) |
| Unique constraints besides the key | allowed; first slot wins (A.4, §9.2); must not cross audiences (§15.2) |
| `RESTRICT` / `NO ACTION` foreign keys | allowed; a merged child of a deleted parent is taken out (A.5, §9.1) |
| `SET NULL` foreign keys | allowed; a merged child of a deleted parent reads null (§9.1) |
| `SET NULL` on a `NOT NULL` or key column | refused (§9.1) |
| `SET DEFAULT` actions | refused (A.8) |
| References to non-key columns | refused; foreign keys reference the parent's whole primary key (A.9) |
| Foreign keys on one generated column | refused (§9.1) |
| Local foreign keys into synced tables | delete action must be cascade or set null; update action into a non-key column must be cascade or set null (§9.1) |
| CHECK constraints | allowed; a row failing after a merge is taken out (A.6, §9.3) |
| Triggers that write synced tables | must be declared shared (A.7, §9.4) |
| Collations on key, unique and foreign-key columns (and the parent columns those keys reference) | must be `BINARY`, comparing text by bytes (A.10) |
| Schema changes | additions or breaking changes (A.11, §18) |

### A.1 Integer primary keys

- **Problem:** keys SQLite chooses itself, integer rowid aliases, can collide
  offline: two devices pick the same id for different rows, and coven would treat
  them as one row.
- **Status:** refused. An integer key derived from the row's content is allowed,
  like any key derived from the content (§9.2).

### A.2 Tables with no primary key

- **Problem:** coven can't tell which row a change belongs to, and SQLite's hidden
  rowid collides across devices like an integer key.
- **Status:** refused (§9.2).

### A.3 Key columns that can be empty, and independent keys without a UUID column

- **Problem:** SQLite lets a non-integer key column hold NULL unless it is
  declared `NOT NULL`, and a key with a NULL in it names no row. An independent
  key needs a text column to hold its UUID.
- **Status:** both refused (§9.2).

### A.4 Unique constraints besides the primary key

- **Problem:** two offline devices can each insert a row with the same value.
- **Status:** the row with the first slot claim keeps the value; the other row is
  taken out while it conflicts, and recorded as lost (§9.2). A unique constraint
  must include the audience column, or the foreign key that fixes the audience, so
  it doesn't cross audiences (§15.2).

### A.5 Restrict and no-action foreign keys

- **Problem:** a device can delete a parent while another adds a child it hasn't
  seen.
- **Status:** the child is taken out while its parent is gone, and recorded as
  lost (§9.1).

### A.6 CHECK constraints

- **Problem:** two concurrent edits that each pass can merge into a row that
  fails, such as one device setting `start` and another `end`.
- **Status:** a row that fails after a merge is taken out until it passes, and
  recorded as lost (§9.3).

### A.7 Triggers that write synced tables

- **Problem:** a trigger that runs again while coven applies another device's
  write would repeat what the original device already sent.
- **Status:** allowed as shared triggers, which run only on the device making the
  write (§9.4).

### A.8 SET DEFAULT foreign keys

- **Problem:** the default parent is a second parent that merges like any row and
  can itself be gone. Supporting it would need restrict-like removal while that
  parent is gone, tracking its current generation, and handling rows that can't be
  deleted because their defaults fail a CHECK.
- **Status:** refused on foreign keys of synced tables and on foreign keys from
  local tables into synced tables, for both delete and update actions (§9.1).
  `SET DEFAULT` appears in so few SQL files that refusing it affects almost
  nobody. Counts of SQL files on GitHub using each action, from GitHub's REST
  code search (`GET /search/code`, query `"ON DELETE <action>" language:SQL`,
  its `total_count`), retrieved 2026-10-07; approximate, across all SQL
  dialects:

  | Action | SQL files |
  | --- | --- |
  | `ON DELETE CASCADE` | ~1,714,000 |
  | `ON DELETE SET NULL` | ~646,000 |
  | `ON DELETE RESTRICT` | ~496,000 |
  | `ON DELETE NO ACTION` | ~115,000 |
  | `ON DELETE SET DEFAULT` | ~2,200 |

### A.9 References to non-key columns

- **Problem:** a parent can change a `UNIQUE` non-key value while another device
  inserts a child referring to the old value. The parent's row identity and
  generation can't track that changing value. Depending on arrival order, the
  child would either make `COMMIT` fail or let SQLite rewrite a synced child with
  `ON UPDATE CASCADE`.
- **Status:** refused for foreign keys of synced tables, with
  `SchemaError::ForeignKeyNotPrimaryKey`; the target must be the parent's complete
  primary key (§9.1). Local children keep SQLite's behaviour, subject to the
  local foreign-key rules: a local key into a synced non-key column follows its
  updates with cascade or set null.

### A.10 Collations on keys

- **Problem:** a key or unique column with `COLLATE NOCASE` or `RTRIM` makes
  SQLite treat different spellings as one value, while each device's write records
  the spelling it used. Two devices adding "Urgent" and "urgent" would make two
  rows to coven where SQLite can hold one, and two claims of one unique value that
  coven sees as different.
- **Status:** refused on open and after every migration. These columns must
  compare by bytes (`BINARY`): a synced table's primary-key columns and the
  columns of its unique constraints and unique indexes (a unique index term may
  not declare another collation either); the columns of every foreign key of a
  synced table, and of every foreign key from a local table into a synced table;
  and the parent columns those foreign keys reference (§9.2). Other columns may
  use any collation.

### A.11 Schema changes

- **Problem:** devices running different app versions hold different schemas.
- **Status:** additions sync with no change of version; any other change raises
  the store's version, and every device updates and reloads (§18).

---

## Appendix B. Coven's merge records

The merge state coven keeps in its own tables, inside the app's database, where
the app can't read or write them. §8 and §9 explain what each record is for.

### B.1 The tables

- **`_coven_writes`:** one row per write the device has applied, naming the
  write's timestamp (which includes its device), the write's number, the slot that
  holds it, and its original `read_through`. The device's own earlier writes are
  implicit reads.
- **`_coven_columns`:** one row per synced column, naming its table and column.
- **`_coven_rows`:** one row per generation of each synced row, naming its table,
  primary key and audience; the generation, how many times the row had been
  created, deleted or re-added; and the write that moved it there, or of several
  concurrent ones, the one with the smallest timestamp. This write is not the
  first-slot creator kept for owner permission (§9.6).
- **`_coven_cells`:** one row per synced cell, naming its `_coven_columns` row,
  its `_coven_rows` row and the write that set it.
- **`_coven_references`:** one row per reference a synced cell holds, naming the
  cell's `_coven_rows` and `_coven_columns` rows, its `_coven_foreign_keys` row,
  and the parent's table, key, audience and the generation it points at (§9.1).
- **`_coven_foreign_keys`:** one row per foreign key of a synced table, naming it
  as B.5 says.
- **`_coven_claims`:** one row per unique value a taken-out row claims, naming the
  row's `_coven_rows` row, its `_coven_constraints` row (which names the unique
  constraint as B.5 says), and the row's audience and the value it claims (§9.2).
- **`_coven_written_values`:** a present row's displayed values are in the app's
  table. When a cell reads differently from what was written (a reference read as
  null, §9.1, or a generated column computed from merged inputs), this table keeps
  the written value by cell until the two agree again.
- **`_coven_lost`:** every lost value (B.3).
- **`_coven_circle_deletions`:** the entry deleting each circle (§15.9).

The applied admin state holds, for each audience, the slot of its latest schema
raise or reset and the snapshot it names. Snapshot metadata keeps the historical
facts that old writes still need.

### B.2 Worked example: note 42

Note 42 on Ben's phone, after Ana's write 4 and its own write 9 (§8.1):

```
notes                            the app's own table
  id   title               body
  42   "Weekly groceries"  "milk, eggs"

_coven_columns
  id   table   column
  1    notes   title
  2    notes   body

_coven_rows
  id   table   key   audience   generation   write
  3    notes   42    store      1            1

_coven_cells
  column   row   write
  1        3     7
  2        3     1

_coven_writes
  id   timestamp                                number
  1    2026-10-01 09:12:40.511 #0  ana-phone    1
  7    2026-10-02 13:01:00.000 #1  ben-phone    9
```

To find which write set note 42's title:

1. in `_coven_columns`, notes' title is column 1;
2. in `_coven_rows`, notes row 42 is row 3;
3. in `_coven_cells`, column 1 of row 3 points to write row 7;
4. in `_coven_writes`, write row 7 is Ben's phone's write 9, stamped
   13:01:00.000 #1.

When a device applies a write that sets the title, it adds the write's row to
`_coven_writes`, compares the write's timestamp with that of the title's current
write, and, if the new one is larger, sets the title in `notes` and points the
title's `_coven_cells` row at the new write.

Each device has its own `_coven_writes`, so one write gets a different row number
on each device. Ana's write 4, for example, is row 6 on Ben's phone and row 14 on
Carol's tablet. The §8.1 example with those row numbers:

Ben's phone:

| Time  | Applies → `_coven_writes` row | New stamp   | Current stamp | Wins | Title              | Title's `_coven_cells` → `_coven_writes` row |
|-------|-------------------------------|-------------|---------------|------|--------------------|----------------------------------------------|
| 13:00 | Ana's write 4 → 6             | 13:01:00 #0 | 12:00:00 #0   | yes  | "Groceries"        | 6                                            |
| 13:00 | its write 9 → 7               | 13:01:00 #1 | 13:01:00 #0   | yes  | "Weekly groceries" | 7                                            |
| 14:30 | Carol's write 2 → 8           | 14:00:00 #0 | 13:01:00 #1   | yes  | "Shopping"         | 8                                            |

Carol's tablet (offline from 12:30; back at 14:30, when it uploads its write 2):

| Time  | Applies → `_coven_writes` row | New stamp   | Current stamp | Wins | Title      | Title's `_coven_cells` → `_coven_writes` row |
|-------|-------------------------------|-------------|---------------|------|------------|----------------------------------------------|
| 14:00 | its write 2 → 13              | 14:00:00 #0 | 12:00:00 #0   | yes  | "Shopping" | 13                                           |
| 14:30 | Ana's write 4 → 14            | 13:01:00 #0 | 14:00:00 #0   | no   | "Shopping" | 13, unchanged                                |
| 14:30 | Ben's write 9 → 15            | 13:01:00 #1 | 14:00:00 #0   | no   | "Shopping" | 13, unchanged                                |

Note 43 on Carol's tablet (§8.3):

```
_coven_rows
  time    id   table   key   audience   generation   write
  14:45   4    notes   43    store      1            16      Ana's write 5 creates it
  16:00   5    notes   43    store      2            19      Ana's write 7 deletes it
  17:00   6    notes   43    store      3            20      Ana's write 8 re-adds it

notes
  14:45   row 43 present
  16:00   row 43 gone
  17:00   row 43 present again
```

Between 14:45 and 16:00, Carol's tablet applies two writes that leave note 43's
generation as it is: Ana's write 6, her 15:00 edit to note 42's body (§8.1), as
row 17; and Carol's own write 3, at 15:30, which adds attachment 8 to note 43
(§9.1), as row 18.

### B.3 The lost-value record

`_coven_lost` holds one record for every kind of loss, and snapshots carry them
in one section. Each record names:

- the table, key and audience;
- the column, for a cell; no column, for a whole row;
- the generation the value lived in, or, for an excluded row change, the
  generation it was made at;
- the value that lost, or every value of a row that was taken out;
- the write that set each value;
- what replaced it: a write that hadn't read it (§8.2), the rules that took the
  row out (§8.4), a breaking schema change that excluded it (§18.6), dropped data
  sources (§9.5), or a refused owner permission or foreign-key derivation (§9.6);
- whether its values are frozen: unauthorized changes, dependent changes, writes
  excluded by a schema change, and rows deleted by a breaking migration keep their
  values in the old shape;
- for an excluded or dependent change, its write and operation, even for a delete
  with no old values.

A taken-out row's record is not frozen: later edits to the row update its values.
The sources and verdicts that dependent and owner checks need (§9.5, §9.6) are
kept separately, so dismissing a record can't change a source's verdict.

### B.4 How lost-value records follow the writes

- A record is added when a write replaces a value it hadn't read (§8.2), when a
  delete arrives before a concurrent edit (§8.3), and when a row is taken out
  (§8.4).
- A record is removed when a later write that had read the value replaces it in
  place (§8.2), when the app dismisses it, and when a taken-out row comes back.
- A frozen record is never removed by a later replacement (§9.5).

### B.5 How constraints are named

SQLite gives foreign keys and unique constraints no lasting name, so coven names
them by what they are:

- **A foreign key** by its columns, in order, and the table and columns it points
  at. Two keys on one column into different tables stay two.
- **A unique constraint** by its terms, in order, each a column's name or an
  expression's text as written, and by its `WHERE` clause when it is partial.
  `UNIQUE(title)` and `UNIQUE(lower(title))` stay two.
- **A CHECK** by its name, or, when unnamed, by its full expression text from the
  schema. Only outer ASCII whitespace is removed; quoting, comments and internal
  spacing are kept. So `CHECK("n">0)` and `CHECK("n"<10)` have different names,
  `"n">0` and `"n"<10`. SQLite may report only the dequoted first token (`n`) for
  either failure, so coven evaluates every CHECK on the merged row and records the
  full names of all that fail.

---

## Appendix C. Coven's own schema and storage format

Reference for §17.9 and §18.9: how coven keeps its own tables in the app's
database, how its storage format is versioned, and how it keeps file bytes on the
device consistent with the database.

### C.1 Coven's tables in the app's database

- Coven's own tables, indexes, triggers and views, temporary ones included, use
  the `_coven_` prefix. Indexes SQLite generates for constraints keep SQLite's
  `sqlite_autoindex_` prefix before the table name.
- App SQL objects and synced-table declarations may not start with `_coven_` or
  `coven_`, compared without regard to ASCII case. Reserving both keeps the app's
  access limited to its own objects; coven creates its own only under `_coven_`.
  SQL functions such as `coven_applying()` keep their names.
- Coven's tables are local only; they never sync.
- Opening for writing always migrates coven's tables in place before running the
  app's migrations. Each internal migration is atomic.
- A read-only open refuses tables that need migrating. Any open refuses an
  internal schema newer than this coven supports.

Coven's other tables, besides the merge records Appendix B.1 lists:

| Table | Holds |
|---|---|
| `_coven_uploads` | each write record waiting to be sent (§4) |
| `_coven_user_files` | a user-provided original's path while a row or unpublished write can name it (§17.7) |
| `_coven_pending` | one record per thing that can't proceed (§19.1) |
| `_coven_device_reset` | the reset notice with old and new device ids (§19.3) |

### C.2 Storage format versions

- What coven writes to storage has a *format*: sequence slots, inner writes,
  snapshots and paths. format.md gives every byte.
- Every object records its format version outside its encryption, so an older
  coven tells a newer object from a damaged one and asks for an update instead of
  reporting damage.
- Format 1 is the first, unreleased format. Its layouts include stable member
  ids, replacement entries, and key copies and restore codes specific to each
  member key pair
  ([format.md D1](format.md#d1-rules-for-every-object)). There are no older
  formats and no compatibility readers.
- After release, a layout change gets a new format number. A newer coven reads
  every released older format and writes the newest, because a device can come
  back with waiting writes from any released format.
- A slot or inner write already tried is retried in the format of its first
  attempt, so sealing it again reproduces the same bytes under the same
  content-bound nonces (§6.3, Appendix D.4).
- An older coven that meets an object in a newer format asks for an update and
  waits, as with a schema addition. Once updated, it reads that object and
  carries on.
- Format versions belong to objects. A format change needs no admin entry,
  snapshot or reload.

### C.3 Keeping file bytes on the device

These rules keep a device's own file bytes (staged app files, cache files,
pins) consistent with its database across crashes.

- Write owned bytes to disk and sync them before committing a row that names
  them. Remove the row before deleting its bytes. A crash then can't leave a row
  naming missing bytes.
- Record every new file's name before creating it. Attaching the file clears its
  cleanup record in the same transaction; a failure leaves the cleanup recorded.
- An unfinished cache write keeps its own name until the transaction exposing its
  verified range commits. Opening never deletes bytes another live writer still
  owns.
- Cancelling staging or a cache write releases its active names. Durable records
  keep any abandoned bytes findable for deletion.
- A transaction that releases owned bytes records their deletion before
  committing. Coven deletes the bytes after releasing the database writer, then
  removes the record.
- Owed local deletions run after writes and on opening. A failure keeps the
  record. A committed write still returns its result; opening reports the cleanup
  failure.
- Original user files are never deleted by this work. Deleting files in storage
  follows §17.8.

---

## Appendix D. Cryptography and keys reference

This appendix lists the algorithms, what each signature and seal binds, how nonces
are made, how keys are derived and routed, and how secrets are stored. §12 tells
the story. format.md [D9](format.md#d9-sealed-objects) and
[D11](format.md#d11-keys-and-contexts) give the exact bytes.

### D.1 Member keys

A member's keys are two key pairs, kept together:

| Part | Form |
|---|---|
| `MemberId` | an independent random UUID; never changes |
| `SigningKey` | a 32-byte Ed25519 public key; invalid and weak points are refused |
| `SealingKey` | a 32-byte X25519 public key, which opens keys sealed to the member |
| `MemberKeys` | the public pair of both (format.md D2) |

§12.3 covers replacing them.

### D.2 Algorithms

| Use | Algorithm |
|---|---|
| Encrypting contents | XChaCha20-Poly1305 |
| Signing slots | Ed25519 |
| Sealing a key to a member | anonymous sealed box: X25519 with XChaCha20-Poly1305 |
| Keys for each purpose | HKDF-SHA256 from the store key or a circle's key, with separate labels for encryption and authorship |
| A join request's key | HKDF-SHA256 from the invite secret, with its own label |
| Key introductions | SHA-256 of the key's exact 32 bytes, with a random key id |
| Slot signatures | over the path and a SHA-256 hash of every preceding byte |
| A file | a random key of its own; its storage name is a random id (§17.3) |

### D.3 What authentication binds

- **Paths.** Each object's storage path is bound into its authentication, so the
  provider can't swap one object for another.
- **File chunks** bind their index.
- **Write and snapshot chunks** bind their section and index. A write's header is
  section 0 and its part i is section i + 1. A snapshot is one section.
- **Slot signatures** cover the slot's path and a SHA-256 hash of every byte
  before the signature.
- **Writes inside a slot** bind their stable store, device and write identity
  instead of the slot's path (format.md D9). So when Ben loses slot 41 and resends
  at 42, the slot changes but the bytes he authored don't.

### D.4 Nonces

| Encrypting | Nonce |
|---|---|
| A file's chunks | the chunk's index. It never repeats under the file's own key, so retrying an upload sends the same bytes |
| Writes and sequence slots | the first 24 bytes of HMAC-SHA256 under the encryption key, over a context binding the path or the write's identity, the cleartext prefix, the section, the chunk index and the SHA-256 of that chunk's plaintext (format.md D11) |
| Everything else | random |

The content-bound nonce has three consequences:

- A retried write or slot re-encrypts to the same bytes, which §5's byte
  comparison relies on.
- Changed plaintext gets a different nonce, even when a restored backup has
  forgotten an earlier attempt at the same path (§19.3).
- Binding the cleartext prefix also separates changed authentication data.

Rollback and copied identities need no assumption that paths are unique for this
separation. It still relies on SHA-256 and the truncated HMAC resisting
collisions.

### D.5 Key introductions and routing

| Object | In the clear | Encrypted |
|---|---|---|
| Key introduction (inside an admin entry) | nothing beyond the slot's kind | the key's random id and the SHA-256 of its 32 bytes |
| Key copy (its own slot) | audience, key id, recipient member, the exact sealing key, and the sealed box itself | the key, inside a box sealed to the recipient alone; the box also authenticates the clear routing. No store key wraps the box. The slot's body, only an acknowledgement, is under an accepted store key |
| Any slot | its kind | its device and claim number, under the store key |
| Writes slot | the kind | a header naming which audiences its parts belong to, under the store key; each part under its audience's key |
| S3 key delivery, `<store>/access-keys/<member>/<attempt>` | the path | the access key, sealed to the member |
| Join request, `<store>/join-requests/<invite id>` | the path | everything, under a key derived from the invite secret; signed with the new member key |

The floor also carries key copies with their routing in the clear (§16.4).

### D.6 Where secrets live

§12.4 says what custody holds. This is how the app's own secrets, such as an API
token, are kept beside coven's keys (api.md [E11](api.md#e11-keys-and-secrets)):

- **Separate entries.** Each secret is its own keychain entry, under the same
  access policy as coven's keys and the platform's per-secret size limit.
- **Names.** Names are arbitrary strings, encoded into native account names that
  can't collide with coven's own entries.
- **The list.** Coven records each store's saved names in another keychain entry,
  outside the database.
- **Order of steps.** Saving adds the name before writing the secret; deleting
  removes the secret before the name. So the list always covers every secret that
  exists. A failure or crash between steps can leave only a harmless extra name.
- **Deleting the local store** removes every listed secret, then the list, then
  coven's other entries (api.md [E1](api.md#e1-opening)). This works even when the
  database is damaged, and the app needn't supply names.
- **Retries are safe.** Removing an entry that's already absent succeeds.

---

## Appendix E. Provider reference

Reference detail for §20. Each part is self-contained; read the one you need.

### E.1 iCloud (CloudKit)

**Setup.** CloudKit is built into `coven-storage` on Apple platforms, using
`objc2-cloud-kit` and `block2` completion handlers awaited through a oneshot
channel. Setup takes the app's container identifier, gets the current user's
record id, and creates the store's custom zone with
[CKModifyRecordZonesOperation](https://developer.apple.com/documentation/cloudkit/ckmodifyrecordzonesoperation).
The recorded location keeps container, owner and zone for restore and joins.
Coven uses that zone in the owner's private database, or a participant's shared
database. Other platforms refuse CloudKit at setup (§20.2).

**Records.** Each object is a `CovenObject` record whose name is the object's
full coven path (format.md D10), store id included. These ASCII names fit
[CKRecord.ID's 255-character
limit](https://developer.apple.com/documentation/cloudkit/ckrecord/id).

| Requirement | CloudKit call |
|---|---|
| create once | save a **new** record with that id under [ifServerRecordUnchanged](https://developer.apple.com/documentation/cloudkit/ckmodifyrecordsoperation/recordsavepolicy/ifserverrecordunchanged); an occupied name returns `CKError.serverRecordChanged`, mapped to `AlreadyExists`. Coven never merges or overwrites that immutable record |
| conditional replace | keep the floor's fetched change tag and save with `ifServerRecordUnchanged` |
| time | the server-assigned [creationDate](https://developer.apple.com/documentation/cloudkit/ckrecord/creationdate) for an immutable object's complete publication; [modificationDate](https://developer.apple.com/documentation/cloudkit/ckrecord/modificationdate) for replacements; resolution 1 ms |
| revision | [recordChangeTag](https://developer.apple.com/documentation/cloudkit/ckrecord/recordchangetag), which changes on every save, including a replacement with equal bytes |
| status | fetch by exact record id; only `CKError.unknownItem` means absent, while `zoneNotFound` means `ContainerNotFound`. Missing size, date or revision is a protocol error |

**Listing.** A
[CKQuery](https://developer.apple.com/documentation/cloudkit/ckquery) on
`CovenObject`, in the store's zone, using equality on the exact directory the
prefix names. Each publication atomically stores indexed string fields
`directory1` through `directory4`, one per existing ancestor directory, without
trailing slashes: store, store/kind, store/kind/scope, and the snapshot writer's
directory. A prefix selects the field at its depth, so a scoped listing is one
equality query. Queries request only `path` and `size`, exclude assets, and
follow every [CKQueryOperation
cursor](https://developer.apple.com/documentation/cloudkit/ckqueryoperation).
Finding sequence slots uses exact record ids, never queries.

**Bytes and large objects.** A
[CKAsset](https://developer.apple.com/documentation/cloudkit/ckasset) carries
the encrypted bytes. Saving a record saves its assets too; coven never exposes
an incomplete asset as a complete object. Bounded asset parts let ranged reads
skip unrelated parts.

- CloudKit has no app-controlled resumable byte upload, so coven keeps its own
  durable session: its id, destination, total size and confirmed part boundary.
- Each asset part and its confirmed boundary are saved together in the custom
  zone, using [atomic
  modification](https://developer.apple.com/documentation/cloudkit/ckmodifyrecordsoperation/isatomic).
- Resuming fetches the session by id, including after a lost reply, and keeps
  every confirmed part (§17.7).
- The destination record is created only once every part exists, atomically
  marking the session published. Its `creationDate` therefore dates complete
  publication, never the session's start or an early part.
- Pending sessions and parts use their own record types and reserved names
  outside coven's paths, so listings can't mistake them for published objects.
- The session id identifies a lost successful publication reply.
- Abort keeps published objects; deletion removes the published record and the
  parts it owns. CloudKit's [deleteSelf
  references](https://developer.apple.com/documentation/cloudkit/ckrecord/reference)
  express that ownership.
- Native atomic saves need no conditional server-side copy to hide partial
  uploads.

CloudKit's atomic publication and asset-session guarantees still need live
verification through a signed, entitled host.

**Sharing.** A private, read/write [CKShare for the
zone](https://developer.apple.com/documentation/cloudkit/ckshare). Only the
owner changes participants.

- Inviting fetches the participant's native identity, saves the share, and puts
  its server-issued URL in the encrypted invite.
- Joining fetches [share
  metadata](https://developer.apple.com/documentation/cloudkit/ckfetchsharemetadataoperation),
  verifies container, owner and zone, then calls
  [CKAcceptSharesOperation](https://developer.apple.com/documentation/cloudkit/ckacceptsharesoperation).
- Removing a member removes only that participant and saves the share. Owner,
  public or otherwise shared access stays, and is reported.

**Errors.** `notAuthenticated`, `permissionFailure` and `quotaExceeded` keep their
native causes as authentication, permission and quota failures. `partialFailure`
is read through `CKPartialErrorsByItemIDKey`, never taken as success.
`CKErrorRetryAfterKey` supplies the provider's retry delay.

### E.2 Dropbox and OneDrive

**Dropbox access levels.** The app registers an App-folder key, a Full Dropbox
key, or both (§20.2). An App-folder app addresses paths relative to its own
folder, without a shared-namespace path-root header. Dropbox refuses sharing
inside that folder, so `create_invite` returns a typed error before changing the
provider. OneDrive, CloudKit and S3 have no such choice; their access rules are
those of §20.

**OneDrive upload sessions.** OneDrive's sessions expose unfinished items, so
coven uploads to `<store>/.coven-uploads/<session>` inside the configured folder.
That reserved place is distinct from every coven path.

- The session's random UUID is recorded with its native upload capability before
  any part is sent. Retries and resumption derive the same temporary name.
- After all parts are stored, coven publishes with one server-side Graph `copy`
  to the final parent and name, with `@microsoft.graph.conflictBehavior=fail`. It
  follows the returned monitor URL until the asynchronous copy completes. An
  occupied name keeps its original bytes and time.
- After a lost publication reply, confirming an existing object needs complete
  byte comparison.
- The copy keeps the temporary's `createdDateTime`, which records the session's
  start. Publication time is therefore the top-level `lastModifiedDateTime`,
  which the copy assigns. Coven never modifies a published immutable object, so
  reads and listings keep that first publication time.
- Deleting the temporary follows §20.6. On OneDrive, transport failures and
  retryable responses are what keep it for the recorded session.
- Abort cancels an open native session and deletes its temporary, including after
  native upload completion. The same cleanup of abandoned recorded sessions
  removes leftovers after a crash.
- Listings of published objects exclude the temporary namespace, unfinished
  uploads included.

### E.3 S3 settings and profiles

**The settings menu.** `S3Settings` (api.md E5) holds one value per behaviour:

| Behaviour | Choices |
|---|---|
| signing | AWS SigV4, or Google HMAC |
| single-PUT create-once condition | `If-None-Match: *`, or `x-goog-if-generation-match: 0` |
| copy publication (`S3CopyPublication`) | **never**: single PUTs and multipart completions carry the create-once condition; **multipart uploads only**; or **every object**, single-request creates included. Copies carry a destination condition: `If-None-Match: *`, `x-goog-if-generation-match: 0` or `cf-copy-destination-if-none-match: *` |
| native multipart timestamp | completion, or start |
| checksum policy | scope stated; unstated scope ignored; or none returned |
| listing | V2 or V1 |
| revision | ETag, or generation |
| response headers | S3, or Google |
| reads request checksums | yes or no |

**Known profiles** are const entries in the adapter's
[`s3_profile.rs`](../crates/coven-storage/src/providers/s3_profile.rs). Each
holds: its name; its endpoint host or host suffix (none for a self-hosted
service); the settings it fills; the finest scope a member key can have (§20.7);
the date and suite version of its last certification; separate certification
evidence for versioned buckets; and its key minter (E.12). Versioning support
belongs to the profile, never to `S3Settings`.

**Choosing by host.**

- No endpoint selects **AWS S3**. Host suffix `.amazonaws.com` selects AWS.
- Exact host `storage.googleapis.com` selects **GCS** through its XML API.
- Suffix `.r2.cloudflarestorage.com` selects **Cloudflare R2**, including
  jurisdiction-specific account hosts.
- Suffixes include the separating dot and need a host component before them.
- **RustFS**, **SeaweedFS**, **Ceph RGW** and **Versity Gateway** are
  self-hosted; no hostname selects them, and a named profile is used as is.

**Validation,** before any request; a failure is `InvalidConfiguration`:

- A known host admits only its own profile's name. A contradicting name or a
  custom profile is refused.
- Without an endpoint, requests reach AWS, so only AWS may be named, and no
  custom profile is accepted.
- Known hosts require `https`. Other endpoints may use plain `http`, as a
  self-hosted server on the same machine or a private network can.

### E.4 Certified profiles

Each profile's selected behaviours:

| Profile | Signing | Single PUT | Copy publication | Native multipart time | Checksums | Listing | Revision |
| --- | --- | --- | --- | --- | --- | --- | --- |
| AWS (uncertified) | AWS SigV4 | `If-None-Match: *` | Multipart, `If-None-Match: *` | Start | Scope stated | V2 | ETag |
| GCS (certified) | Google HMAC | `x-goog-if-generation-match: 0` | Multipart, the same generation condition | Completion | Scope stated | V2 | Generation |
| R2 (certified) | AWS SigV4 | `If-None-Match: *` | Never (conditional completion) | Completion | Unstated scope ignored | V2 | ETag |
| RustFS (certified, versioned too) | AWS SigV4 | `If-None-Match: *` | Never (conditional completion) | Completion | None returned | V2 | ETag |
| SeaweedFS (certified, versioned too) | AWS SigV4 | `If-None-Match: *` | Every object, `If-None-Match: *` | Completion | None returned | V2 | ETag |
| Ceph RGW (certified, unversioned) | AWS SigV4 | `If-None-Match: *` | Never (conditional completion) | Completion | None returned | V2 | ETag |
| Versity Gateway (certified, versioned too) | AWS SigV4 | `If-None-Match: *` | Never (conditional completion) | Completion | Scope stated | V2 | ETag |

AWS, R2 and Versity Gateway request checksums. RustFS, SeaweedFS and Ceph RGW
aren't asked for checksums they never return. GCS supplies its hashes without a
checksum-mode request. Every profile but GCS reads S3 response headers; the
adapter translates GCS's Google headers.

**Versioned buckets.** RustFS 1.0.1, SeaweedFS 4.48 publishing every object by
copy, and Versity Gateway 1.9.0 (POSIX backend with `--versioning-dir`) passed
every suite 5 race with versioning on. GCS ran only with versioning off, so it
has no versioned-bucket evidence. AWS remains uncertified. R2 has no versioning
feature. Ceph RGW 20.2.4 fails the versioned races.

**Why each profile is what it is.**

- **AWS** creates once with `If-None-Match: *` on `PutObject`; an occupied
  destination returns HTTP 412 `PreconditionFailed`, mapped to `AlreadyExists`.
  Conditional replacements use `If-Match` with the observed ETag. Multipart
  parts stay unpublished until completion. AWS enforces `If-None-Match: *` on
  `CompleteMultipartUpload` too, but its multipart `Last-Modified` records the
  upload's start ([system
  metadata](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html)),
  which fails the complete-publication requirement. So the AWS profile publishes
  multipart uploads by copy, with `If-None-Match: *` on `CopyObject`, which [AWS
  documents](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
  since October 2025. AWS single-request PUTs are dated at completion. Listing
  uses `ListObjectsV2`, an exact prefix and every continuation token. The
  profile is uncertified; the setup safety check verifies it against the
  account's bucket.
- **GCS** ignores `If-None-Match: *` on PUT, so create-once sends
  `x-goog-if-generation-match: 0`. An occupied destination returns HTTP 412
  `PreconditionFailed`, mapped to `AlreadyExists`, without changing bytes or
  publication time. Conditional replacements put the observed generation in
  `x-goog-if-generation-match`; each replacement's revision is its new
  `x-goog-generation`, including equal-byte replacements. Requests use Google V4
  HMAC authentication and `x-goog-*` headers, because GCS refuses requests mixing
  Google and AWS extension headers. GCS XML multipart initiation rejects
  generation conditions, and completion doesn't enforce create-once, even though
  its measured native timestamp is completion; so coven publishes by copy, with
  `x-goog-if-generation-match: 0` on the copy. GCS conditional-copy timestamps
  passed the completion checks. Listing uses `ListObjectsV2` with the requested
  prefix, continuation tokens and delimiter, as on AWS; published objects keep
  the server's `LastModified`.
- **R2**'s live certification chose conditional completion without a copy:
  single PUT and multipart completion both refused occupied names with HTTP 412
  and kept bytes and times. Unfinished uploads were absent from HEAD, GET and
  listing, and multipart `Last-Modified` fell between the completion markers. R2
  returned a checksum without its scope, so the adapter discards it before
  parsing metadata and before the SDK validates the body. A stated scope stays
  usable; none is inferred from the algorithm or ETag. Cloudflare's [S3
  compatibility table](https://developers.cloudflare.com/r2/api/s3/api/)
  documents its operation and checksum differences, and its [copy
  extensions](https://developers.cloudflare.com/r2/api/s3/extensions/#conditional-operations-in-copyobject-for-the-destination-object)
  document `cf-copy-destination-if-none-match`. The R2 profile doesn't use that
  extension, because native conditional completion passed. Profile values come
  from the live suite, not a compatibility claim.
- **RustFS and unversioned Ceph RGW** refused occupied names with HTTP 412 on PUT
  and on multipart completion, keeping bytes and times, and their multipart
  `Last-Modified` fell between the completion markers. So they use conditional
  completion without a copy.
- **SeaweedFS 4.48** refuses occupied names one request at a time too, but not
  under concurrency: two racing conditional completions of one name both
  succeed, and in an unversioned bucket a conditional PUT racing a conditional
  copy of one name both succeed too, while copies racing copies keep one winner.
  So the SeaweedFS profile publishes every object by copy with `If-None-Match: *`.
- None of RustFS, SeaweedFS and Ceph RGW returned a checksum, even with checksum
  mode, so their profiles don't request one.
- **Versity Gateway 1.9.0**, versioned or not, refused occupied names the same
  way, sequentially and in every suite 5 race, so it uses conditional completion
  too. Unlike the others it returns checksums with their scope stated, so its
  profile requests them.

R2's single-PUT and multipart timestamps, and RustFS, SeaweedFS, unversioned Ceph
RGW and Versity Gateway single-PUT and multipart timestamps, passed the
completion checks. A service dating multipart objects from their start needs a
conditional copy that dates complete publication; the adapter never invents a
later time, and the setup safety check refuses any S3 location whose multipart
objects aren't dated that way.

### E.5 Certification history

- **2026-10-13:** suite version 1 passed GCS and R2; suite version 2 passed RustFS
  and SeaweedFS. Suite version 3, which detects with the production probes,
  reproduced each of those four entries the same day and passed their setup
  safety check.
- **2026-10-14:** suite version 4 passed Ceph RGW 20.2.4 with versioning off, and
  RustFS 1.0.1 with versioning off and on. It failed SeaweedFS 4.48 in both
  states: concurrent conditional multipart completions both succeeded, so
  SeaweedFS's suite 2 certificate was withdrawn.
- **2026-10-14:** suite version 5, which races the named entry's own
  publication, passed SeaweedFS's multipart copy publication with versioning off
  and on. But with versioning off, a conditional PUT racing a conditional copy of
  one name both succeeded in 14 of 200 trials. Its entry now publishes every
  object by copy.
- **2026-10-14:** suite 5 passed R2, RustFS 1.0.1 with versioning off and on, and
  Ceph RGW 20.2.4 and GCS with versioning off; those entries record suite 5
  certificates. It also passed Versity Gateway 1.9.0 with versioning off and on
  from detected settings.
- **2026-10-15:** suite 5 passed SeaweedFS's every-object entry with versioning
  off and on. A rerun later that day failed its multipart and mixed races three
  times, but that run had put the profile name in the env file, which the suite
  then ignored, so it raced the detected settings' conditional completion, not
  the entry's copies. Run again as the named entry, suite 5 passed it three times
  with versioning off and three times with it on; its certificates cover both
  states.
- **2026-10-15:** suite 5 passed Versity Gateway again as the named `VersityGw`
  entry; its certificates cover both states.
- **AWS** is uncertified: there is no live account.

The live runs and their bucket states are in the [suite 4
evidence](s3-certification.md#concurrent-certification-suite-4), the [full
battery](s3-certification.md#full-battery-2026-10-14), the [GCS and Versity
Gateway runs](s3-certification.md#gcs-and-versity-gateway-2026-10-14), the
[SeaweedFS every-object
run](s3-certification.md#seaweedfs-every-object-copy-publication-2026-10-15),
the [SeaweedFS rerun](s3-certification.md#seaweedfs-rerun-2026-10-15) and the
[named-entry
reruns](s3-certification.md#seaweedfs-reruns-as-the-named-entry-2026-10-15).

### E.6 Running the certification suite

**Setup.**

- Put the bucket, region, endpoint and HMAC keys in an env file:
  `COVEN_LIVE_S3_{BUCKET,REGION,ENDPOINT,ACCESS_KEY_ID,SECRET_ACCESS_KEY}`. The
  file holds only these five; the suite refuses a file holding anything else, so
  a profile name written there can't be silently ignored.
- Run `COVEN_LIVE_S3_ENV=<file> cargo test -p coven-storage s3_certification --
  --ignored --nocapture`.
- Select a self-hosted profile with `COVEN_LIVE_S3_PROFILE=CephRgw` (or
  `RustFs` / `SeaweedFs` / `VersityGw`) in the process environment. Otherwise the
  suite detects its settings.
- Never publish the env file or credentials.

**What it does.**

- Runs production detection from the location's settings, or from plain S3
  settings where detection must supply them, and prints every result.
- A detected entry that fails the setup safety check prints as uncertified, with
  its first failed check. A profile that setup refuses prints that verdict
  separately.
- From suite version 5, a named profile's own entry is the candidate, checked
  against those results; otherwise the detected settings are. Sequential probes
  can't show that a race needs copy publication, so detection alone would race
  SeaweedFS's conditional completion instead of its copy, and detection never
  chooses copy publication for every object. A profile publishing every object
  by copy races its single-request creates as copies too, so its PUT and mixed
  races are copies against copies.
- The candidate runs the adapter's full live contracts: setup, reads, ranges,
  resumption, replacement revisions and listing lag.

**The races** (from suite 4). The suite records the bucket's versioning state and
runs six concurrent checks:

- 2 and 8 conditional PUTs;
- 2 and 8 multipart publication requests;
- a PUT racing a multipart publication;
- 2 replacements conditioned on the current ETag or generation.

Copy profiles assemble their temporaries before racing the publishing copy. Each
trial uses one fresh destination and distinct 64-KiB payloads, with every writer
released by one barrier. Each request's start, end and outcome are recorded, and
all intervals must overlap; a trial that doesn't overlap fails certification too.
Exactly one request must succeed, every other must refuse the condition
(`AlreadyExists` or `Conflict`), and the stored bytes must be the winner's.
Two-writer checks run 200 trials each; eight-writer checks run 50 each.
Replacement races are required, not exempt from failure.

**Results.**

- A service that returns no checksum on any probe, even with checksum mode, is
  recorded as returning none. §20 allows a missing checksum, so suite version 2
  accepts that result where version 1 failed detection.
- Each requirement of §20 prints PASS or FAIL.
- Cleanup removes the run's objects, temporaries and uploads even after a failed
  assertion, including object versions and delete markers in a versioned run.
  Certification needs permission to list and delete those versions; ordinary
  member deletion still addresses only the current object.
- The suite prints a Rust profile entry with the passing date and suite version.
  Commit it only after every check passes, alongside the
  [raw certification evidence](s3-certification.md).

**Versioning certification.** Only a passing suite with the recorded state
`Enabled` supplies it; a passing run on an unversioned bucket never does.
Certification exercises a versioned candidate even when setup would refuse it,
reporting both verdicts separately. An unknown bucket state can't be certified.

### E.7 Detection probes

**The probes,** in this order:

1. Both signers, each with a read-only zero-entry listing.
2. Both single-PUT conditions: the first PUT must be accepted and a second
   refused as `AlreadyExists`, leaving the original bytes and time.
3. An unfinished multipart upload, which must be absent from HEAD, GET and
   listing.
4. Conditional completion: accepted on a free name, refused on an occupied one
   with bytes, ETag and time unchanged.
5. The native multipart timestamp, bracketed by marker objects written before and
   after the upload starts, and before and after it completes.
6. Each copy destination condition: accepted on a free name, refused on an
   occupied one with bytes and time unchanged; and whether the copy's time is
   later than its source's.
7. V2 listing against paginated V1 listing of the probe objects.
8. Checksum mode, returned checksums and their scope, Google generation and hash
   headers, and a zero-entry listing at a missing object's key.

**Waiting.** Detection pauses only where elapsed time tells outcomes apart; no
other probe waits. Pauses use the store's clock.

- Times are whole-second `Last-Modified` values. So after the marker written
  after the upload starts, detection waits 1 second, then rewrites a marker,
  pausing 250 ms between attempts, until the server dates it a later second than
  that marker. Completion follows. A completion dated inside the start bracket is
  a start timestamp.
- The copy probes run after that wait, so a copy dated later than its source is
  dated by the copy itself.
- Listing probes poll immediately and after each 250 ms pause until the listing
  shows every probe object.
- Both stop after 40 pauses, 10 seconds of waiting, beyond the worst listing lag
  measured (5.5 seconds). A marker that never advances fails detection; a listing
  that never completes is a failed listing result.

**Choosing settings from the results:**

| Setting | Chosen |
|---|---|
| single-PUT condition | the first enforced, in probe order |
| publication | conditional completion when it is enforced, unfinished uploads are hidden, and multipart times date completion; otherwise the first enforced copy condition in the order `x-goog-if-generation-match`, `If-None-Match`, `cf-copy-destination-if-none-match` |
| multipart timestamp | as observed |
| checksum policy | none when no probe returned one; unstated scope ignored when one arrived without its scope; scope stated otherwise |
| checksum requests | only when checksum mode is accepted and checksums don't arrive without it |
| revision and headers | generation revision and Google headers when GCS's headers arrive |
| listing | V2 when it agrees with V1 |

Where no behaviour holds, the setting keeps the plain S3 value and the setup
safety check names the failure. Detection never chooses copy publication for
every object (E.6).

### E.8 The setup safety check

§20.8 lists the four checks, `CreateOnce`, `MultipartCreateOnce`,
`PublicationTime` and `Behavior`, in the order setup reports the first failure.
`Behavior` fails when another setting contradicts a result:

- a signer the service refused;
- a listing protocol that failed or disagreed;
- no checksum policy where a checksum was returned;
- stated scope where scope was missing;
- checksum requests the service refused;
- generation revision or Google headers without GCS's headers;
- S3 headers where GCS's arrived;
- a refused zero-entry listing at a missing object's key.

### E.9 Versioned buckets

- Setup reads `GetBucketVersioning`; GCS's [`GET ?versioning` XML
  API](https://docs.cloud.google.com/storage/docs/xml-api/get-bucket-metadata#versioning)
  uses the same `Status` values.
- `Enabled` and `Suspended` need the selected profile's separate passing date and
  suite version for versioned buckets (§20.7).
- **An unreadable state.** A refused or unimplemented versioning read leaves the
  state unknown. Profiles without versioning certification refuse it with
  `S3VersioningUnknown`, keeping the native cause. Certified profiles may proceed
  and log the unknown state. Other failures, including malformed responses and
  transport failures, fail detection.
- **R2.** Hosted R2 has no object versioning and
  [no versioning API](https://developers.cloudflare.com/r2/api/s3/api/). Its
  documented absence, together with its refused read, is `Unavailable`, not an
  unknown owner setting. This exception requires R2's known hostname, not an R2
  name at an arbitrary endpoint.
- **Permissions.** A member's policy needs bucket-level
  `s3:GetBucketVersioning`, separately from its prefix-limited object and list
  permissions. GCS needs
  [`storage.buckets.get`](https://docs.cloud.google.com/storage/docs/using-object-versioning#required_roles).

### E.10 S3 publication mechanics

**Where uploads go.** A copy-publication profile uploads unconditionally to
`<prefix>/<store>/.coven-uploads/<session>`, where `<session>` is the upload's
random UUID. Other profiles upload at the destination: the shared slot path or an
immutable object's path. That reserved place is distinct from every coven path.

**Session records.** Every S3 session is recorded with its name before its native
multipart upload exists. The first part starts the native upload, and the record
then gains the native id.

- A record made before a start whose reply was lost keeps its unresolved session.
- Recovery may abort only uploads proven to belong to that session. A shared
  slot path doesn't establish ownership; a private staging name can identify its
  own uploads.
- If the native id can't be recovered, coven reports `SessionIdentityUnknown` and
  keeps the journal until provider evidence settles cleanup.
- Aborting never deletes a destination or another claimant's session.
- Retrying or resuming a record derives the same name, including after multipart
  completion or a lost copy reply.

**Copy publication.** The assembled temporary is sent with one server-side
`CopyObject` to the destination, carrying the profile's destination condition
(`x-goog-if-generation-match: 0` for GCS).

- The destination's time is the copy's completion, never the temporary's
  creation or the multipart start.
- An occupied destination stays untouched. The session marker tells this
  session's lost successful reply apart from another object occupying the path.
- Deleting the temporary follows §20.6.

**Every object by copy (SeaweedFS).** A single-request create is published the
same way:

- One unconditional PUT stages its bytes at
  `<prefix>/<store>/.coven-uploads/<create>`, carrying a fresh attempt id as its
  marker; then one conditional copy publishes it.
- `<create>` is the create's own name: the UUID formed from the first 16 bytes of
  SHA-256 over `coven/copy-temporary/v1`, the destination path's length as a
  big-endian u64, the path, and the bytes.
- Slots below the single-request limit are such creates, each still dated by one
  request's completion: the copy's.
- A refused copy, or one whose reply was lost, settles as stored when the
  destination carries its marker; otherwise the copy's answer stands.
- The temporary is deleted whatever the copy's outcome. A failed deletion fails
  the call, with both errors kept.

**Attempts that share a temporary.** Concurrent attempts of one create, such as
two creators of one path with identical bytes, share its temporary. One can delete
it between another's staging and copy, so that copy finds no source. Its answer
then says nothing about the destination, which settles it instead:

- its marker at the destination: stored;
- another object: an occupied path whose bytes §5 compares (the create's own,
  when only its attempts publish there);
- an empty destination: a retryable `Conflict`, which the caller retries under
  its kept record.

No attempt ends in a hard error or loses the create. The marker names the attempt
that staged the bytes the copy carried, not the copy itself: an attempt that
stages over another's before that one copies sees its own marker at the
destination, so both answer stored, which equal bytes settle to anyway.

**Temporaries are always recorded.** Every temporary is staged under a name its
publisher has already recorded, and stays recorded until deleted. Every attempt
of one create stages and deletes the same name, so the caller's record of the
create, which it keeps with its fixed bytes until the create settles (§5.1),
records the temporary too. A retry after a crash between staging and deletion,
or after a failed deletion, removes it, and finds its own bytes at the
destination as an occupied path, which §5's byte comparison accepts as the same
create. Under a fresh name per attempt, a crash would strand a temporary no
record names.

**Why it is create-once.** Given a copy that checks and publishes its destination
atomically:

- the copy changes published objects exactly as a create does;
- a temporary is never visible as a published object;
- of sessions racing on one name, the first copy to arrive publishes its own
  bytes, and every other copy refuses the condition and deletes its temporary;
- a lost copy reply settles by reading this session's marker at the destination.

When every publication of a name is a conditional copy, at most one is accepted,
in any arrival order, assuming only that the provider applies one name's copies
one at a time; none is when the name was already taken. That assumption says
nothing about a conditional PUT racing a copy (§20.6), so a profile whose service
keeps copies, but not PUTs, exclusive against copies publishes every object by
copy.

**The 5 GiB limit.** See §20.6: `StorageFailure::CopyTooLarge` on AWS and
SeaweedFS.

**Aborting.** Aborting a copy-publication session aborts its native multipart
upload and deletes its temporary, even when the native upload has already
completed. So a crashed device's recorded session stays usable by the same
cleanup of abandoned sessions. Temporaries stay discoverable in the reserved
place but are excluded from published-object listings.

**Concurrent-mutation errors.** AWS `ConditionalRequestConflict`, GCS
`OperationAborted` and Ceph RGW `ConcurrentModification` mean a concurrent
mutation, never throttling.

- A single-request PUT can retry its fixed bytes.
- At conditional multipart completion or copy publication, the error invalidates
  the recorded session instead: discard its part list, abort the native upload,
  and delete any copy temporary. If abort or deletion also fails, both causes are
  kept.
- The invalidated session is recorded even on failure. It keeps only the ids
  needed for cleanup and can't complete or copy again.
- A replacement session starts only after cleanup succeeds, at offset zero, with
  the same path, size and fixed bytes.
- An occupied destination still needs complete byte comparison (§5.1).

### E.11 Times, revisions and checksums per provider

| Provider | Time field | Resolution δ | Revision | Checksum |
|---|---|---|---|---|
| S3 (HEAD) | `Last-Modified` ([HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)); listings truncated to whole seconds | `δ = 1 second` | GCS: generation. AWS and R2 replaceable paths: the version id when versioning is on, otherwise a fresh `coven-revision` value stored atomically in the replacement's user metadata. Immutable objects: ETag | the explicitly returned checksum and its scope; GCS's CRC32C is whole-object; R2's unscoped checksum is dropped; RustFS, SeaweedFS and Ceph RGW return none |
| Dropbox | `server_modified`, never `client_modified` ([format](https://github.com/dropbox/dropbox-api-spec/blob/main/common.stone)) | `δ = 1 second` | `rev` | its block-based content hash |
| OneDrive | top-level `lastModifiedDateTime` for publication and replacement, never the client's `fileSystemInfo` times ([driveItem](https://learn.microsoft.com/en-us/graph/api/resources/driveitem?view=graph-rest-1.0)) | the returned `DateTimeOffset` precision: `δ = 10^(-p) seconds` for `p` returned fraction digits; none means 1 second, three means 1 ms | `eTag` | SHA-1 or QuickXorHash |
| CloudKit | the server's creation or modification date ([types](https://developer.apple.com/library/archive/documentation/DataManagement/Conceptual/CloudKitWebServicesReference/Types.html)) | `δ = 1 millisecond`, the unit of CloudKit's date representation | `recordChangeTag` | none |

Notes:

- S3 listings fetch exact status for replaceable paths, because `ListObjectsV2`
  carries neither versions nor user metadata.
- A checksum returned under the RustFS, SeaweedFS or Ceph RGW profile, scoped or
  not, contradicts the certified result and fails as a protocol error rather than
  being used.
- OneDrive rechecks replaceable metadata after download and fails if it changed,
  so metadata and bytes describe the same revision.
- The resolution is kept independently of listing lag.

### E.12 S3 member keys

**Admin-access sources** (api.md E9). On desktops:

- the AWS CLI's `aws configure export-credentials --format process` for the
  chosen profile: short-lived keys with their session token;
- `gcloud auth print-access-token` for the chosen account;
- Google sign-in through the app's Google client, with the installed-app
  loopback, PKCE and the `cloud-platform` scope, requesting no refresh token;
- AWS IAM Identity Center's device authorization: register a public client, show
  the verification page and code, poll at the given interval, then fetch the
  chosen account and role's credentials.

On every platform the person may paste an admin access key or API token; a pasted
key for Versity Gateway names its IAM endpoint too (below). A CLI runs only after
the person chooses it, with no input. Its output is parsed in memory and erased,
and a failure reports only its exit status, since CLI text can echo
configuration.

**Minters.** Each known profile's entry names its key minter: functions that
make, roll back, delete and read a member key with admin access, and list a
store's keys. A custom profile uses guided console steps.

| Service | Member key | Admin access |
|---|---|---|
| AWS | an IAM user, at IAM's global endpoint signed for `us-east-1`, named with the key name, under path `/coven/`, tagged `coven-store` and `coven-member` with the store id and the member or invite id (never an email), with the member policy as inline policy `coven-member`, and one access key | IAM admin access |
| SeaweedFS | the same, through its embedded IAM on the location's S3 endpoint and region (the operator sets `-iam.readOnly=false`); the policy goes through `CreatePolicy` and `AttachUserPolicy`, because its `PutUserPolicy` drops conditions and refuses `s3:ListMultipartUploadParts` | IAM admin access |
| Ceph RGW | the inline form, at the location's RGW endpoint and region, on RGW accounts (Squid or newer) ([RGW IAM](https://docs.ceph.com/en/squid/radosgw/iam/)) | an account-root key, or a user's delegated IAM access |
| Versity Gateway | the inline form, from its standalone IAM service (`versitygw iam`, which the gateway reads through `--iam-standalone-endpoint`), signed for the location's region | a pasted key naming the IAM service's own address (`AdminCredential::AccessKey`'s `iam_endpoint`) |
| GCS | an HMAC key of its own service account (below) | an OAuth access token with the `cloud-platform` scope (the signed-in `gcloud` account's, in the live test) |
| R2 | a Cloudflare account API token (below) | an API token allowed to write account tokens |
| RustFS | a service account (below) | admin access signed with Signature Version 4 for `s3` |

Versity Gateway's IAM service listens at its own address, apart from the S3
endpoint. Admin access naming an IAM endpoint for any other service, or none for
Versity Gateway, fails with `InvalidConfiguration` before any request. The
gateway's own admin API (`versitygw admin create-user`) has no per-user policy and
isn't used.

**GCS.** The minter reads the bucket's project through the JSON API, and refuses
a bucket without uniform bucket-level access, which managed folders need. It
creates the service account `<name>@<project>.iam.gserviceaccount.com` under the
short name, with the description `<key name> member <member or invite id>`. It
creates the store's managed folder `<prefix>/<store>/` if absent, and binds the
account to it as `roles/storage.objectUser` and to the bucket as
`roles/storage.bucketViewer`, whose only bucket permission is
`storage.buckets.get`, retrying once when a policy changes concurrently. Then
`hmacKeys?serviceAccountEmail=` creates the key, whose access id is the member
key id. Google can take 60 seconds to see a new account or key; the retry window
is 60 seconds. Revoking deletes the key and, while IAM still has the account,
its other HMAC keys, then both bindings and the account; the store's managed
folder stays. Absence is read from the key: `DELETED` or gone.

**R2.** The token is named `<key name> member <member or invite id>` and limited
to the store's own bucket
(`com.cloudflare.edge.r2.bucket.<account>_<jurisdiction>_<bucket>`) with the
`Workers R2 Storage Bucket Item Write` permission group: object reads and writes.
The location's endpoint host names the account and its jurisdiction. The token id
is the S3 key id, and the hex SHA-256 of the token value is its secret. The admin
token can grant anything its owner holds. Revocation deletes the token; absence
is read from the token itself. Because the key reaches its whole bucket, its
member-side check requires the account's bucket listing to be refused.

**RustFS.** Each key is a service account made through
`/rustfs/admin/v3/add-service-account` with the member policy inline, as plain
JSON; MinIO's `/minio/admin` alias expects madmin's encrypted bodies. Every
member key's parent is one dedicated user, `coven`, created once with a random
secret nobody keeps and granted `readwrite`. A service account's access is its
parent's intersected with its own policy. Deleting that parent would delete every
member's key, so coven never deletes it. Each account takes the short name, with
the description `<key name> member <member or invite id>`. Revocation calls
`delete-service-account`; absence is read with `info-service-account`. RustFS
answers a delete of an absent account with HTTP 400 `InvalidRequest`, a status
other refusals share, so a refused delete counts as success only when
`info-service-account` then reads the account absent.

**The member policy.**

- Allow `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`,
  `s3:AbortMultipartUpload` and `s3:ListMultipartUploadParts` on
  `arn:aws:s3:::<bucket>/<prefix>/<store>/*`.
- Allow `s3:ListBucket` on the bucket under the condition `StringLike`
  `s3:prefix` `<prefix>/<store>/*`.
- An unconditioned statement allows `s3:GetBucketVersioning` and
  `s3:ListBucketMultipartUploads`, which AWS can't limit to a prefix. Neither
  reads an object.
- Never add a `Deny` on `StringNotLike` `s3:prefix`: a request carrying no
  prefix, such as a bucket check, matches the negated condition and would be
  denied.

**Names.**

- The *short name* is `coven-` and 24 hex digits of the call's random id, used
  for GCS service accounts (6 to 30 characters) and RustFS service accounts.
- The *key name* carries the store and the device making the key, for §14's
  reclaim: `coven-<store>-<device>-<call>`, with the first 24 hex digits of the
  store id, the device id's 16 and the call's first 16. That is 64 characters, an
  IAM user name's limit.

**Listing a store's keys.** A listing returns every key held under a key name for
that store, with the device the name carries:

- IAM services: `ListUsers` (under `/coven/` where users carry a path; SeaweedFS
  keeps none) and each such user's `ListAccessKeys`, with the managed policy's ARN
  from `ListPolicies` on SeaweedFS;
- GCS: the bucket project's service accounts, every page, and each matching
  account's HMAC keys;
- R2: the account's tokens;
- RustFS: the parent's service accounts.

On IAM services and GCS the listing also returns users and service accounts under
a store's key names that hold no key, so the reclaim of §14.3 can remove them.

**The member-side check,** before delivery. Using the new key:

- an object in `<store>/.coven-check/<run>/` is created, refused a second time,
  read back and deleted, and that prefix is listed, with its multipart uploads;
- bucket versioning is read as setup reads it;
- where keys reach a prefix, a listing of the location's base prefix, outside
  every store, must be refused; where they reach a whole bucket, the account's
  bucket listing must be.

Otherwise the check fails with `StorageFailure::KeyBeyondStore`.

**Retry windows.** A new key may not be visible everywhere at once. Two kinds of
failure are retried each second within the service's window:

- authentication and permission failures (after which a retry finds its run's
  object already created);
- admin calls answered NotFound for a principal the service doesn't see yet (a
  mint repeats under its names).

The window is 60 seconds for AWS IAM, Ceph RGW and Versity Gateway, and 30 seconds
for SeaweedFS, whose gateways read identity changes asynchronously. The 60-second
window is a retry limit, not a verified provider propagation guarantee; a key
still refused at the limit fails the operation.

**Live evidence.** On 2026-10-14 the RustFS, SeaweedFS and Ceph RGW 20.2.4
minters, and on 2026-10-15 the Versity Gateway 1.9.0 and GCS minters, each made a
key live with admin access, passed the member-side check, were refused writes,
reads and listings outside the store's prefix, revoked the key twice, read it
absent and saw the service refuse it
([evidence](s3-certification.md#member-keys)). On 2026-10-15 the Versity Gateway
1.9.0 minter's live run also listed the minted key under its store with its
device, and listed nothing after revocation; the other listings are checked
against in-process fakes of their APIs. To run it:

```
COVEN_LIVE_S3_ENV=<file holding the admin key> COVEN_LIVE_S3_PROFILE=<name> \
  cargo test -p coven-storage s3_member_key_lifecycle -- --ignored --nocapture
```

adding `COVEN_LIVE_S3_IAM_ENDPOINT=<url>` for Versity Gateway. GCS's admin access
is the signed-in `gcloud` account's.

---

## Appendix F. Crates and conventions

How the Rust is organised, and the rules a checker enforces on it.

### F.1 Crates

Coven is a Cargo workspace of eight crates, each owning one part of this
document:

| Crate | Owns | Sections |
| --- | --- | --- |
| `coven-foundation` | the clock, the id source, atomic file writes, the store's directory and its lock | §7, §10, §19.3, api.md E1 |
| `coven-crypto` | ciphers, sealed boxes, derived keys, file keys, member keys and their custody | §12 |
| `coven-merge` | timestamps, the merged state, the removal rules and lost values, as functions with no I/O | §7, §8, §15 |
| `coven-format` | the bytes in storage: slots, inner writes, snapshots, file headers and chunks, encoded, decoded and checked, using merge's and crypto's types | format.md |
| `coven-database` | the SQLite connection, coven's internal tables, applying the merge's results, triggers, live queries, migrations | §4, §9, §18 |
| `coven-storage` | each provider, and the operations coven needs from it, including upload sessions | §20, Appendix E |
| `coven-sync` | the sequence, members, circles, snapshots, files and the cache, operations, recovery | §5, §6, §10, §11, §13 to §19 |
| `coven` | the API, and nothing else | api.md |

- Each crate depends only on crates above it in the table, except that
  `coven-database` and `coven-storage` never depend on each other. So the
  database never reaches storage, and storage never reads the database;
  `coven-sync` is where the two meet.
- `coven-merge` and `coven-format` read no clock, file, database or network. So
  the merge is tested, and checked against the Lean model
  ([proofs/merge.md](proofs/merge.md)), without SQLite or storage.
- Building the whole history is test support only. Production merges arriving
  writes against the applied metadata without keeping write bodies.
- Ids shared by several crates (store, member, device and circle ids) live in
  `coven-foundation`; each concept has one type.
- Each external dependency's version is set once, in the workspace, and crates
  name only the features they need.

### F.2 Capabilities

A *capability* is something outside the program's own memory: the network,
cryptography, SQLite, the OS keychain, the current time, new ids, and files on
disk. Each is used directly in one place only:

```
network          coven-storage's providers
cryptography     coven-crypto
SQLite           coven-database
OS keychain      coven-crypto's custody
current time     coven-foundation's clock
new ids          coven-foundation's id source
files on disk    coven-foundation's files
```

Everything else reaches a capability through the object that owns it, given to
it when it is built. The snapshot writer never calls the system clock; it asks
the clock it was given, and a test gives it a fixed one.

- The builder configures the clock, custody and OAuth. Ids use OS randomness;
  only `test-utils` allows replacing their source (api.md E1).
- A method never takes a raw capability as a parameter, such as a store directory
  or a database connection; it calls the capability's owner instead.

### F.3 Owners and tasks

An *owner* is an object that holds a capability, or holds another owner, and
lives while the store is open. The database owner holds the SQLite connection;
the sync owner holds the database owner and the storage owner. A *task* is a
value that lives for one piece of work and is then dropped, such as one sync pass
or one step of an operation (§11). A task borrows the owners it needs for that
work, and holds nothing past it.

**Owners receive their collaborators when they are built.** The sync owner takes
the database owner and the storage owner as arguments and opens neither itself.

**Composition roots.** One authoritative list in the checker's policy names every
place allowed to construct owners or start long-lived work. Each entry names an
exact source file, type and method (or free function), not a whole file or a
type-name suffix. The roots include opening and bootstrap, provider connection
construction, owner constructors and task starts.

- The checker infers owners from retained capabilities and other owners.
- Any owner construction outside a listed root fails the check, including
  construction of an owner's private representation.
- Runtime acquisition and thread or task spawning use the same list.
- A legitimate new place must be added deliberately; returning an owner grants no
  permission to construct one.
- Test-only sources and items may assemble their own graphs.

**Long-lived tasks.** Each has one owner responsible for starting and stopping
it, including when that owner is dropped. Its start sites are on the same list;
there is no separate list of lifetime authorities. Only the sync owner starts the
sync loop, so closing the store stops it, and nothing else can leave one running.

**Owners don't hand out what they hold,** by returning it or through a public
field; callers ask the owner to do the work. Nothing outside `coven-database`
gets the SQLite connection; it asks the database owner to run a write. A struct
built only to be taken apart again, with every field public and no methods, isn't
used to pass collaborators; they are passed by name.

### F.4 Opening, closing and sync

- **Closing.** Closing a handle, or dropping its last clone, marks its store as
  closing before releasing its owners. Blocking work keeps the database
  connections and writer lock until it finishes; subscriptions keep only their
  active database reads.
- **Reopening in the same process** waits for release, using a process-wide
  registry keyed by the store's canonical directory and id, and a notification
  when the lock handles close. There are no timeouts or polling sleeps. A live
  instance in this process, or a lock held by another process, still returns
  `AlreadyOpen` at once (api.md E1).
- **Opening** leaves configured storage `Stopped`, or `Disconnected` when none is
  set up. `start_sync` starts the loop, whose first pass builds the storage
  client from custody credentials (api.md E5). Starting again keeps the running
  loop.
- **`stop_sync`** returns at once; release follows the active pass and transfers.
  It keeps session keys, location and credentials. Status reports completion, or
  `Failed` with its cause, without a pending connection record.
  `disconnect_storage` also removes credentials; remote contents stay untouched
  (api.md E5).
- **Custody.** A closed custody owner is represented by the absence of its held
  custody, with no separate closed flag. Storage calls serialize with closing;
  calls already started finish even if the app drops their future. Sign-in stays
  cancellable while presenting or exchanging credentials (api.md E5).

### F.5 Code conventions

- **Visibility.** Nothing is `pub` that can't be reached from outside its crate.
  `pub(in path)` and `super::super::` aren't used: an item needed elsewhere moves
  to where both callers can see it. `coven` re-exports the API at its root and
  keeps every module private.
- **Errors** are typed enums per crate. An error is never turned into text to be
  passed on, and nothing returns `Result<_, String>`.
- **Failures keep their causes.** Failed app calls and whole-pass sync failures
  keep typed causes. `Offline` means the last attempt couldn't reach storage;
  `Failed` means storage answered with an error that prevents the whole pass
  (api.md E5). Every object or operation that can't advance has a typed pending
  record, including maintenance and prerequisites (§19.1, api.md E5). A
  persisted reason never replaces its category with text.
- **Files.** A source file holds at most 1,000 lines, and its tests live beside it
  in `<name>_tests.rs`.
- **Fakes.** Crates offer shared fakes through `test-utils` where needed, such as
  an in-memory provider and a fixed clock. Test-only implementations stay behind
  injected dependencies; tests build the same object graph production does.

### F.6 Checks

One script runs every check. CI runs the code checks on every platform, and the
Lean proofs once, on Linux, since a proof checks the same on any platform.

1. Formatting, and clippy with warnings denied.
2. The dependency rules of F.1.
3. The rules of F.2 and F.3, by a checker that reads the syntax tree of every
   crate, including inside macro calls, against the policy file.
4. The checker's own guard tests: every file, owner and composition root the
   policy names must exist, so the policy can't go stale.
5. The visibility and file-size rules of F.5.
6. `cargo doc` with broken links denied.
7. Every crate built without test code, so an item only tests use shows up as
   dead.
8. The tests, with all features and with none. Among them:
   - §2's promises are exercised through app calls where visible: committed
     writes survive dropping the handle and process exits at each point after
     commit, stay queued, and upload after reopening; app reads and writes finish
     while a sync pass is held;
   - after a removal, the next write and snapshots open with the replaced keys
     and fail with the removed member's keys; identical file uploads store
     different encrypted bytes; snapshots delete the history they cover and the
     device's own superseded snapshots;
   - failures injected at each remote-write step roll back app rows, merge
     records, lost values, the applied slot and query notifications; these tests
     use fixed inputs, an injected clock and explicit barriers.
9. The Lean proofs, built from scratch, with no `sorry` and no axiom beyond
   Lean's own, and their differential tests against the Rust.
10. The spec's own checks (`scripts/check-spec.py`): in coven.md and format.md,
    every section reference and link resolves, every proof link in the body is a
    *Body.* row of Appendix G, prose lines stay within 100 characters, and no
    paragraph is split by two blank lines. spec/WRITING.md is the standard they
    help keep.

The checker is the first thing built, before any crate, so every rule holds from
the first line of code.

Tests control clocks, transfer limits and request completion. Upload boundary
tests use byte-sized payloads; races wait for observed events; interrupted
operations are reopened until they report completion.

The pre-commit hook runs the fast checks: formatting, clippy, the rules of F.1 to
F.5, and the spec's own checks.

---

## Appendix G. What's proved

Every machine-checked claim this document relies on, in the order of the sections
it supports. Each row names the claim in plain words, its theorems, and the file
under [proofs/](proofs/) that proves them. A theorem proves what it states about
its model, under that model's assumptions; it does not prove the Rust code
(§22). Rows marked *body* are also linked where the rule they explain appears.

Write-ups explain each model and state its assumptions:
[sequence](proofs/sequence.md), [merge](proofs/merge.md),
[queue](proofs/queue.md) (conversion of old writes and the reload gate) and
[IO and observable work](proofs/io.md).

### G.1 Promises (§2)

| Claim | Theorems | File | Section |
|---|---|---|---|
| One applied prefix of the sequence determines membership and each readable audience's data at a schema version, given deterministic admin, merge and conversion rules | `prefix_determines_state` | `proofs/sequence/CovenSequence/Converge.lean` | §2.2, §8 |
| Once caught up, a device never picks for a first attempt a key copied to a removed member's sealing keys | `revocation` | `proofs/sequence/CovenSequence/Keys.lean` | §2.5, §12.2 |
| From the moment a prefix is ready to delete, it is deleted within U + G + P + D, without waiting for absent devices | `bounded_retention` | `proofs/sequence/CovenSequence/Deadline.lean` | §2.7, §16.6 |
| An audience with no reader to supply coverage has no deletion deadline | `uncovered_has_no_deadline` | `proofs/sequence/CovenSequence/Deadline.lean` | §2.7, §16.6 |
| Each result's assumptions and where its model stops | (write-up) | `proofs/sequence.md` | §2.9 |

### G.2 The sequence and the pass (§5–§7)

| Claim | Theorems | File | Section |
|---|---|---|---|
| Each followed audience stays at the state of a sequence prefix in storage that satisfies the coverage and snapshot rules | `read_ok` | `proofs/sequence/CovenSequence/Reader.lean` | §5, §16.4 |
| (same) | `dev_ok` | `proofs/sequence/CovenSequence/Device.lean` | §5 |
| A claim with byte comparison wins exactly once | `conclude_correct`, `exactly_one_claimant` | `proofs/sequence/CovenSequence/Claim.lean` | §5.1 |
| *Body.* Without the byte comparison, a batch lands twice | `no_compare_lands_twice` | `proofs/sequence/CovenSequence/Claim.lean` | §5.1 |
| *Body.* An operation that forgets its claim on restart lands twice | `forgetful_lands_twice` | `proofs/sequence/CovenSequence/Operation.lean` | §5.1, §11.1 |
| A read followed by a floor check admits only genuine first bytes | `floor_check_sound` | `proofs/sequence/CovenSequence/Claim.lean` | §5.2, §6.2 |
| *Body.* Without the floor check, a stale create can win a deleted slot and be taken as genuine, so recreated bytes can't confirm a claim | `stale_create_wins_deleted_slot` | `proofs/sequence/CovenSequence/Claim.lean` | §5.2, §6.2 |
| A slot at or above the floor read after the create holds the genuine bytes | `claim_floor_check` | `proofs/sequence/CovenSequence/Retention.lean` | §5.2 |
| Settling from the floor snapshot's last claim number and hash is correct | `settle_correct'`, `settle_from_floor` | `proofs/sequence/CovenSequence/Settle.lean` | §5.3 |
| *Body.* Two copies of one device id fool a check of the claim number alone | `copies_fool_number_only` | `proofs/sequence/CovenSequence/Settle.lean` | §5.3 |
| *Body.* Settling from the state alone fails: later writes or admin entries can erase a landed claim's effects | `admin_state_cannot_settle`, `state_alone_cannot_settle` | `proofs/sequence/CovenSequence/Settle.lean` | §5.3 |
| With the journal order and the floor's coverage, settling tells apart a claim that landed, one absent from the sequence, and one whose slot is still empty | `settle_correct` | `proofs/sequence/CovenSequence/Settle.lean` | §5.3 |
| A settled claim, kept or claimed again after it didn't land (the model's `notLanded`), ends in exactly one slot (no bound on contention or time) | `lands_once_never_lost` | `proofs/sequence/CovenSequence/Settle.lean` | §5.3 |
| A snapshot's per-device claim numbers settle claims of every kind after deletion; they don't describe surviving effects | `claims_ge` | `proofs/sequence/CovenSequence/Settle.lean` | §16.1 |
| A device already stopped takes no apply or reload step | `stopped_stays` | `proofs/sequence/CovenSequence/Device.lean` | §5.4, §22 |
| Bookkeeping requests no pass, and no write is lost even when passes race | `write_never_lost`, `racing_passes`, `bookkeeping_retains_none` | `proofs/io/CovenIO/Wakes.lean` | §6.7 |
| The authored `read_through` and every queued original are preserved; a device's own earlier writes count as read | `authored_readThrough`, `catchup_preserves_originals`, `own_predecessor_is_read` | `proofs/sequence/CovenSequence/Causality.lean` | §7.1 |
| Given publication after the authored read, a device's own writes in number order, and authored reads that never decrease, slot order consumes every cause first; authored pasts are transitive | `ordered_application_causal`, `past_transitive` | `proofs/sequence/CovenSequence/Causality.lean` | §7.1 |
| Receivers that agree on a cause's slot agree whether it is in a write's past; later publication adds no intervening remote write | `receivers_agree`, `intervening_write_not_read` | `proofs/sequence/CovenSequence/Causality.lean` | §7.1 |

### G.3 Merging and SQL rules (§8, §9)

| Claim | Theorems | File | Section |
|---|---|---|---|
| Two orders of the same writes give the same merged state | `merge_converges` | `proofs/merge/CovenMerge/Converge.lean` | §8 |
| The merge model and its write-up (its Appendix B) | (write-up) | `proofs/merge.md` | §8, §8.4 |
| A lost value stays lost unless a write that read it replaces it in place | `lostStep` | `proofs/merge/CovenMerge/Model.lean` | §8.2 |
| The removal rules give the same rows in any order | `device_converges`, `rule_order_converges` | `proofs/merge/CovenMerge/Removal.lean` | §8.4 |
| Stated requirement: the merge model must establish convergence for the merged state and every removal rule, including rows hidden by a circle-deletion slot | (requirement) | `proofs/merge.md` | §8.4 |
| *Body.* A foreign key on one generated column can name a parent no write named | `unwritten_reference` | `proofs/merge/CovenMerge/GeneratedReference.lean` | §9.1 |
| Every reference a device shows is a tuple one write set | `accepted_reference_written`; write-up B19 | `proofs/merge/CovenMerge/GeneratedReference.lean`; [`proofs/merge.md`](proofs/merge.md#b19-foreign-keys-on-generated-columns) | §9.1 |
| *Body.* A local key with no update action can never apply a remote rename | `stuck_forever` | `proofs/merge/CovenMerge/LocalUpdateAction.lean` | §9.1 |
| Cascade and set null keep every local reference valid along the chain | `chain_holds`; write-up B20 | `proofs/merge/CovenMerge/LocalUpdateAction.lean`; [`proofs/merge.md`](proofs/merge.md#b20-local-keys-into-values-coven-updates) | §9.1 |
| The merge model compares keys by bytes | write-up B2 | [`proofs/merge.md`](proofs/merge.md#b2-terms) | §9.2 |
| The first slot wins a unique value and keeps it | `first_slot_wins`, `owner_stays` | `proofs/sequence/CovenSequence/Unique.lean` | §9.2 |
| The model selects the lowest live claim, counting its setter, with the within-slot order; sound readers of one audience at one position select the same winner | `first_live_claim`, `winner_counted_setter`, `within_slot`, `readers_agree` | `proofs/sequence/CovenSequence/LiveClaims.lean` | §9.2 |
| A released value goes to the earliest live claimant, including an older loser; the first claim after the release wins only when none earlier remains | `deleted_releases`, `first_after_release`, `older_loser_returns` | `proofs/sequence/CovenSequence/LiveClaims.lean` | §9.2 |
| A change is kept exactly when none of its data sources was dropped | `retained_iff` | `proofs/merge/CovenMerge/DataDependency.lean` | §9.5 |
| The data rule and value accounting: receive order, progress, and dropping by data | write-up B17 | [`proofs/merge.md`](proofs/merge.md#b17-receive-order-progress-and-dropping-by-data) | §9.5 |
| The owner is the creation with the lowest slot, then the lower write number | `selected_creation`, `lower_write_number_wins` | `proofs/sequence/CovenSequence/Owners.lean` | §9.6 |
| Later claims, updates, deletion, re-adding and audience moves keep the creator | `later_claim_preserves_creator`, `move_preserves_owner`, `retained_creator` | `proofs/sequence/CovenSequence/Owners.lean` | §9.6 |
| A kept foreign-key effect on an owned row traces back to an authorized direct change in the same write | `derived_root_authorized` | `proofs/merge/CovenMerge/OwnerAdmission.lean` | §9.6 |
| An accepted effect is one the schema actually implies | `no_forged_effect` | `proofs/merge/CovenMerge/ForeignKeyActions.lean` | §9.6 |
| Owner authorization and loss recording, at the stated SQL extraction boundary, and the definition of a foreign-key action step | write-up B18 | [`proofs/merge.md`](proofs/merge.md#b18-owner-tables-and-row-authenticity) | §9.6 |
| A step that depends on rows of the author's view that no step carries is refused | B18 `Overlapping` | `proofs/merge/CovenMerge/ForeignKeyOverlap.lean`; [`proofs/merge.md`](proofs/merge.md#b18-owner-tables-and-row-authenticity) | §9.6 |

### G.4 People (§10–§15)

| Claim | Theorems | File | Section |
|---|---|---|---|
| *Body.* Whether a slot is valid depends only on the slots before it, so equal prefixes give equal decisions | `valid_prefix` | `proofs/sequence/CovenSequence/Converge.lean` | §10.2 |
| Membership and role changes apply only from an active admin in the preceding state; leaving no admin is refused; every prefix after creation keeps an admin | `applied_membership_authorized`, `last_admin_refused`, `every_prefix_has_admin` | `proofs/sequence/CovenSequence/Authority.lean` | §10.2, §10.4 |
| A member with no active device can still register a new one | `member_without_active_device_can_register` | `proofs/sequence/SequenceApply.lean` | §10.2 |
| With validation depending only on preceding slots, every operation step keeps its recorded work and in-flight requests accounted for | `steps_inv`, `step_req` | `proofs/sequence/CovenSequence/Operation.lean` | §11.6 |
| An operation occupies at most one slot; a landed one passed validation against the slots before it; a confirmed undo leaves neither slot bytes nor anything it made | `operation_outcome` | `proofs/sequence/CovenSequence/Operation.lean` | §11.6 |
| *Body.* An undo that doesn't wait for in-flight creates leaves something behind | `eager_undo_leaks` | `proofs/sequence/CovenSequence/Operation.lean` | §11.3 |
| *Body.* A write queued before a member-key replacement needs the old signing key, which is cut off afterward, and the new key can't authenticate its history | `self_replacement_counterexample`, `later_replacement_counterexample` | `proofs/sequence/CovenSequence/QueueReloadExamples.lean` | §12.3 |
| Writes after a removal or key replacement are refused with rows unchanged and no new unique-value or owner claim, while the claim number is recorded | `cutoff` | `proofs/sequence/CovenSequence/Cutoff.lean` | §14.1 |

### G.5 Snapshots, the floor and reloads (§16)

| Claim | Theorems | File | Section |
|---|---|---|---|
| A counted preparation announcement has its body already in storage | `announcement_uploaded` | `proofs/sequence/CovenSequence/Preparations.lean` | §16.2 |
| A counted ordinary announcement covers the latest raise and reset, and matches the schema and reset state | `ordinary_covers_boundary`, `ordinary_matches_schema_reset` | `proofs/sequence/CovenSequence/Preparations.lean` | §16.3 |
| *Body.* A stale capture announced after a raise is refused; accepting it would strand a joiner | `slot4_refused`, `stale_floor_strands_joiner` | `proofs/sequence/CovenSequence/ReaderExamples.lean` | §16.3 |
| Activating a prepared snapshot needs an earlier matching announcement, which makes its coverage usable | `activation_requires_preparation`, `activated_is_usable` | `proofs/sequence/CovenSequence/Preparations.lean` | §16.3 |
| With the coverage rules, a sound reader with its key advances and reaches the end without anyone holding deletion back (honest snapshot bodies and kept raise and reset bodies assumed); a reader behind a floor snapshot's coverage may always reload | `read_progress`, `reaches_head` | `proofs/sequence/CovenSequence/Reader.lean` | §16.3, §16.4, §16.6 |
| An abandoned preparation has no effect, supplies no coverage and isn't usable | `preparation_no_effect`, `abandoned_supplies_no_coverage`, `abandoned_not_usable` | `proofs/sequence/CovenSequence/Preparations.lean` | §16.3 |
| Loading a floor snapshot and the slots after it reaches the full state; sound readers at one position agree | `joiner_reaches_head`, `readers_agree` | `proofs/sequence/CovenSequence/Reader.lean` | §16.4, §16.5 |
| A running device with fixed audiences and all their keys brings every reader to the end of the sequence | `device_reaches_head` | `proofs/sequence/CovenSequence/Device.lean` | §16.5 |
| A reload installs the queue as it stands at commit, including writes made while loading, and keeps originals that aren't eligible | `queued_originals_preserved`, `write_during_reload` | `proofs/sequence/CovenSequence/QueueReload.lean` | §16.5 |
| Each eligible original is applied over the loaded state exactly once; consumed ones are left out; repeating the step rebuilds from the loaded state | `queued_exactly_once`, `covered_not_reapplied`, `install_twice` | `proofs/sequence/CovenSequence/QueueReload.lean` | §16.5 |
| Reset, breaking-raise and newly-readable-circle reloads refuse a commit containing the affected audience's part; an ordinary reload admits commits to followed audiences | `affected_commit_refused`, `ordinary_commit_allowed` | `proofs/sequence/CovenSequence/QueueReload.lean` | §2.1, §16.5 |
| The queue model's reload gate refuses a commit into a reloading audience | `reloading_blocks_commit` | `proofs/queue/CovenQueue/Verdicts.lean` | §16.5 |
| *Body.* Keeping queued writes and applying the eligible ones once doesn't make their outcome equal to writes made after the reload installs: an unread reset ignores them, a breaking migration can bring a conflicting key, a newly readable circle changes authorization and key checks | `reset_counterexample`, `breaking_raise_counterexample`, `newly_readable_counterexample` | `proofs/sequence/CovenSequence/QueueReloadExamples.lean` | §2.1, §16.5 |
| *Body.* Without checking that a snapshot replaces the reload's selection, switching to the floor's snapshot after deletion can move coverage backward | `unchecked_reselection_goes_backwards` | `proofs/sequence/CovenSequence/ReloadExamples.lean` | §16.5 |
| When the set of audiences changes, each loads its own prefix at the common target and is rebuilt from its eligible originals; a new circle keeps the queue | `reload_sound`, `new_circle_keeps_queue` | `proofs/sequence/CovenSequence/QueueReload.lean` | §16.5 |
| On good storage with every key, every audience can be loaded to a target between the floor snapshots' coverage and the end | `audiences_can_load` | `proofs/sequence/CovenSequence/QueueReload.lean` | §16.5 |
| Deleting history keeps kept slots genuine, floor snapshots honest, raise and reset bodies kept, and every sound reader sound | `store_good`, `store_reader` | `proofs/sequence/CovenSequence/Retention.lean` | §16.6 |
| Under any interleaving of storage and device steps, a device can still reach the full state at the end of the sequence | `deletion_keeps_reachable` | `proofs/sequence/CovenSequence/Device.lean` | §16.6 |
| Model limit: prepared snapshots are loaded at their raise or reset entries; loading one directly from the floor is not modeled | (none) | — | §22 |

### G.6 Files (§17)

| Claim | Theorems | File | Section |
|---|---|---|---|
| Status is available exactly when an object of the complete size is there; any other size is damage | `available_iff`, `wrong_size_is_damage` | `proofs/storage/CovenStorage/Files.lean` | §17.4 |
| A read's absence error matches status | `read_absence_matches_status` | `proofs/storage/CovenStorage/Files.lean` | §17.4 |
| Reads and status agree on every storage read; a wrong size and a range past the end are damage; a present object is never absent | `read_agrees_with_status`, `wrong_size_read_is_damage`, `past_end_is_damage`, `present_never_absent`, `complete_object_reads` | `proofs/storage/CovenStorage/Files.lean` | §17.4 |
| Where downloads are checked, including disk-full failures injected through the real Rust reader | (specification) | `proofs/io.md` | §17.5 |
| Deleting or retargeting a row keeps its upload in flight, which settles by its own reference | `steps_keep_upload`, `inflight_upload_settles` | `proofs/storage/CovenStorage/Files.lean` | §17.7 |
| A complete listing after the cut-off names every orphan file; restored access makes a completed listing stale | `scan_names_every_file`, `retention_forgetting_counterexample` | `proofs/io/CovenIO/Retired.lean` | §17.8 |

### G.7 Schema changes and resets (§18, §19)

| Claim | Theorems | File | Section |
|---|---|---|---|
| *Body.* A migration's kind follows its statements; deciding by the rows a device holds makes devices disagree | `statement_plans_agree`, `row_classification_diverges` | `proofs/queue/CovenQueue/ConversionChain.lean` | §18.1 |
| *Body.* An undeclared breaking change, or a breaking change declared as an addition, is refused, and no part of the run commits | `undeclared_breaking_refused`, `declared_breaking_addition_refused`, `accepted_prefix_does_not_commit_on_mismatch` | `proofs/queue/CovenQueue/ConversionChain.lean` | §18.3 |
| Migrating keeps every queued original, tried or untried | `conversions_preserve_uploads`, `conversions_preserve_attempt`, `conversions_preserve_untried` | `proofs/queue/CovenQueue/Conversion.lean` | §18.5 |
| Capturing freezes the queue through the marker; later writes don't delay it | `captured_prefix`, `later_writes_do_not_delay` | `proofs/sequence/CovenSequence/RaiseQueue.lean` | §18.5 |
| The captured writes are published once; writes after the marker wait for the raise; originals and their order are kept | `confirm_captured`, `confirmation_not_repeated`, `tail_waits`, `raise_without_waiting`, `originals_preserved` | `proofs/sequence/CovenSequence/RaiseQueue.lean` | §18.5 |
| Uploading the raise snapshot waits for confirmation, and the raise waits for the announcement; no storage or contention deadline is imposed | `preparation_waits_for_confirmation`, `activation_waits_for_announcement` | `proofs/sequence/CovenSequence/RaiseQueue.lean` | §18.5 |
| A later breaking migration's capture includes both markers and the writes between them | `recapture_includes_waiting` | `proofs/sequence/CovenSequence/RaiseQueue.lean` | §18.5 |
| Interpreting a late original from the queue gives the same result as receiving it from the sequence | `late_original_converted`, `reload_matches_receiver` | `proofs/sequence/CovenSequence/RaiseQueue.lean` | §18.5 |
| The author rebuilding after adopting a raise matches every receiver; its uploads stay unchanged | `reload_matches_receiver`, `conversions_preserve_uploads` | `proofs/queue/CovenQueue/Conversion.lean` | §18.6, §19.2 |
| Uncovered old-schema changes convert in version and causal order, whether or not their upload was tried | `receive_matches_evaluation`, `receive_agreement` | `proofs/queue/CovenQueue/Conversion.lean` | §18.6 |
| The converters used are those after the write's version, up to the raise's | `one_step_is_conversion` | `proofs/queue/CovenQueue/ConversionChain.lean` | §18.6 |
| Dependent changes stay frozen; independent ones convert and match receivers | `dependent_change_not_converted`, `independent_change_converted`, `converted_change_matches_receiver` | `proofs/queue/CovenQueue/Conversion.lean` | §18.6 |
| With `Drop`, values are kept; frozen stays frozen; a lost value is never converted again | `explicit_drop_preserves_values`, `conversions_preserve_frozen`, `lost_never_reconverted` | `proofs/queue/CovenQueue/Conversion.lean` | §18.6 |
| *Body.* With no breaking migration in a write's range, no converter is needed; older writes still need the conversions they cross | `newest_breaking_write_admitted`, `retained_step_required` | `proofs/queue/CovenQueue/ConversionChain.lean` | §18.6 |
| Recorded reads don't exclude a write | `recorded_reads_do_not_exclude` | `proofs/queue/CovenQueue/Model.lean` | §18.6 |
| A later snapshot doesn't revive dropped data; a snapshot's lost values keep their cause | `snapshot_drop_stays_dropped`, `snapshot_loss_preserves_cause` | `proofs/queue/CovenQueue/Verdicts.lean` | §18.6 |
| Ben's mixed write 10: its note edit stays frozen and its playlist rename converts; a device's own reads don't exclude | `mixed_write`, `own_reads_do_not_exclude` | `proofs/queue/CovenQueue/DataExamples.lean` | §18.6 |
| The setter actually replaced decides; partial chains of writes | `actual_setter_decides`, `partial_write_chain` | `proofs/queue/CovenQueue/DataExamples.lean` | §18.6 |
| Conversion of old writes (queue model overview) | (write-up) | `proofs/queue.md` | §18.6 |
| Store-reset authority is checked in the state before the slot (circle-reset authority is left to the model's supplied audience policy) | `store_reset_authorized` | `proofs/sequence/CovenSequence/Authority.lean` | §19.4 |
| An applied reset loads its chosen rows without replaying the slots before it | `reset_replaces` | `proofs/sequence/CovenSequence/Preparations.lean` | §19.4 |
| From empty storage, with every create recorded and the journal kept, a slot signed by this device's id that it never attempted means another copy | `foreign_slot_means_copy` | `proofs/sequence/CovenSequence/Settle.lean` | §19.3 |

### G.8 Storage (§20, Appendix E)

| Claim | Theorems | File | Section |
|---|---|---|---|
| *Body.* A copy that checks a free name and then writes it lets two racers both win | `check_then_write_two_winners` | `proofs/storage/CovenStorage/CopyPublication.lean` | §20.6 |
| *Body.* A conditional PUT racing a copy can be accepted beside it | `put_racing_copy_two_winners` | `proofs/storage/CovenStorage/CopyPublication.lean` | §20.6 |
| *Body.* At most one store's setup passes per bucket, however far listings lag | `at_most_one_store` | `proofs/storage/CovenStorage/BucketClaim.lean` | §20.7 |
| With an atomic copy, copy publication is create-once: same effect as a create, temporaries hidden, one winner per race, lost replies settled by the marker, a marker means the session's bytes | `copy_is_create`, `stage_hidden`, `race_one_winner`, `lost_reply_settles`, `marker_settles_bytes` | `proofs/storage/CovenStorage/CopyPublication.lean` | E.10 |
| When every publication of a name is a conditional copy, at most one is accepted in any order, and none if the name was taken | `every_copy_at_most_one`, `copied_at_most` | `proofs/storage/CovenStorage/CopyPublication.lean` | E.10 |
| Concurrent attempts sharing one temporary never hard-fail or lose the create; a missing temporary settles from the destination; an empty destination retries and publishes | `shared_never_fails`, `shared_occupied_identical`, `attemptCopy_retry_empty`, `retry_publishes`, `shared_temporary_missing` | `proofs/storage/CovenStorage/CopyPublication.lean` | E.10 |
| An attempt staging over another's sees its own marker, so both answer stored, which equal bytes settle to anyway | `shared_temporary_both_stored` | `proofs/storage/CovenStorage/CopyPublication.lean` | E.10 |
| Temporaries are always recorded; a retry clears them, even after a failed deletion; a fresh name per attempt would strand one | `staged_recorded`, `retry_clears`, `failed_deletion_retry_accepted`, `attempt_naming_strands` | `proofs/storage/CovenStorage/CopyPublication.lean` | §20.6, E.10 |
| A mint makes and checks its key before the secret joins the journal, and deletes nothing, so another live copy of the database never takes a key this one made | `run_keeps_provider` | `proofs/storage/CovenStorage/KeyDelivery.lean` | §20.10 |
| A mint by a cloned database spares the key this one recorded | `clone_mint_spares_recorded_key` | `proofs/storage/CovenStorage/KeyListing.lean` | §20.10 |
| Admin access is held only by the operation that needs it, and released once its key is checked or its call fails | `released_makes_nothing`, `check_keeps_admin` | `proofs/storage/CovenStorage/AdminAccess.lean` | §20.10 |

---

## Appendix H. Errors and statuses

Each name coven reports, what it means, and where the rule that raises it is.
The exact types are in [api.md](api.md); provider-native errors and how coven
maps them are with each provider in Appendix E.

### H.1 Refusing a local commit or open

| Name | Raised when | Section |
|---|---|---|
| `AudienceReloading` | a write targets an audience being replaced by a reset, breaking-raise or newly-readable-circle reload | §2.1, §15.7, §16.5, §19.4 |
| `MemberKeysChanging` | a write arrives while the member replaces their own keys | §12.3 |
| `DbError::TooLarge` | one write's plaintext exceeds SQLite's 1 GB value limit | §6.3 |
| `DbError::ClockOutOfRange` | no timestamp fits in 48 bits of milliseconds | §7.2 |
| `RollbackFailed` | a rollback failed; every later call on that handle fails until the store is reopened | §4 |
| `DbError::NotOwner`, `DbError::InvalidForeignKeyEffect` | the author's own check finds a change receivers would refuse under the owner rules | §9.6 |
| `DbError::ReferenceAudience` | a write or move would leave a row pointing at a row some of its readers can't read | §15.3 |
| `DbError::NotInCircle` (`NotInCircle`) | a write into a circle the member isn't in, or one absent from the membership this device has applied | §15.6, §15.8 |
| `DeletedCircle` | a new local write into a deleted circle; also the cause named by its rows' lost values | §15.9 |
| `InvalidAudience` | an audience bound in any spelling but the canonical one | §15.1 |
| `DbError::StoreSettingsMismatch` | settings supplied when setting up from the store don't match its creation | §10.3 |
| `DbError::PendingPersistence` | coven can't save pending state; sync and database calls stop | §19.1 |
| `DbError::DamagedDatabase` | a damaged database fails to open | §19.3 |
| `DbError::MigrationKindMismatch { version }` | a migration's declared kind, `addition` or `breaking`, disagrees with the kind coven finds; nothing commits | §18.3 |
| `AlreadyOpen` | the store is still open in this process, or locked by another process | F.4 |
| `SchemaError::ForeignKeyNotPrimaryKey` | a synced table's foreign key doesn't reference the parent's whole primary key | §9.1, A.9 |
| `SchemaError::GeneratedForeignKey` | a synced table's foreign key is one generated column | §9.1 |
| `SchemaError::LocalChildUpdate` | a local foreign key into a synced non-key column has an update action other than cascade or set null | §9.1 |

Other schema refusals have no variant of their own here; each names the table and
the constraint or foreign key (§9, §15.1, §15.2, Appendix A).

### H.2 What stops a device or refuses its work

| Name | Raised when | Section |
|---|---|---|
| `KeyUnavailable` | a slot, or a row's file, needs a key the device doesn't hold yet | §5.4, §6.2, §17.5 |
| `UpdateRequired` | a slot needs a newer schema or format | §5.4, §18.3, C.2 |
| `Refused` (the `Refusal` variant) | a completely downloaded immutable object failed a check; calls and pending records use the same variant | §5.4, §6.2, §19.1 |
| `WritesRefused` | writes cut off by a removal or key replacement were consumed; names the deciding entry | §14.1, §19.1 |
| `SyncError::DeviceIdInUse` | another installation uses this device's id, or a reset replaced it | §5.1, §5.3, §14.1, §19.3 |
| `MemberKeysReplaced` | this device holds member keys that were replaced, or an old restore code is used | §12.3, §13.1, §14.1 |
| `SyncError::StoreOwner` | a removal targets the member whose account holds the store | §10.1, §14.2 |
| `SyncError::Rejected(OperationRejection)` | an operation is no longer allowed after losing a race | §11.1 |
| `AccessRemains` | a grant coven leaves in place: through a parent folder, a public link or a group, to an unidentified grantee, or the owner's own | §14.2 |
| `DeviceReset` | the notice, stored in `_coven_device_reset`, that a reset replaced this device | §17.6, §19.3 |

### H.3 Status and pending records

| Name | Meaning | Section |
|---|---|---|
| sync status | no storage set up, stopped, offline, syncing, synced, blocked at a slot with a reason, or failed | §19.1 |
| `Stopped`, `Disconnected` | opening leaves configured storage stopped, or disconnected when none is set up | F.4 |
| `Blocked`, `Synced` | a pass stopped at a slot; a pass reached the end of the sequence | §6.4 |
| `Offline`, `Failed` | the last attempt couldn't reach storage; storage answered with an error that prevents the whole pass | F.5 |
| `NoStorage` | no storage is connected | §17.4, §17.5 |
| `Storage` | a network or provider failure, kept with the provider's category | §17.5 |
| `Waits(Object(path))` | storage confirms a file absent while its uploader is active | §17.5 |
| `FileUnavailable(FileMissingReason)` | the uploader was removed or replaced, or this device's source failed | §17.5, §17.7 |
| `Refused(Decryption)`, `Refused(ContentHash)` | a complete chunk failed authentication; authenticated bytes disagree with the content hash | §17.5 |
| `Failed(Disk)`, `Failed(Database)` | writing the local cache failed; the database failed | §17.5 |
| `Failed(LocalFailure::Database)` | a corrupted database, with the native cause on the call | §19.1 |
| `Waiting` | an upload sent in one request, until confirmed completion removes it | §17.7 |
| `Automatic`, `AppAction`, `Never` | how a storage failure retries: under backoff, when credentials or the connection change, or not at all | §17.7 |
| `DeleteAccessKey` | an S3 key no longer current, owed deletion | §14.3, §19.3 |
| `PendingReason::AdminAccessRequired` | a step needs provider admin access again after a restart | §20.10 |
| `SessionIdentityUnknown` | an S3 upload session's native id can't be recovered | E.10 |

### H.4 Storage and setup

| Name | Raised when | Section |
|---|---|---|
| `AlreadyExists` | a create-once request found the path occupied | §5.1, §20.1 |
| `Conflict` | a racer's condition was refused; for a shared temporary, a retryable answer | E.6, E.10 |
| `StorageFailure::StoreExists` | another creation already holds slot 1 | §10.3 |
| `StorageFailure::ProviderUnavailable` | CloudKit on a platform other than Apple's | §20.2, E.1 |
| `ContainerNotFound` | CloudKit's zone is missing | E.1 |
| `StorageFailure::OwnBucketRequired { reason: Prefix }` | a bucket-scoped service's location has a base prefix | §20.7 |
| `OwnBucketRequired { reason: Occupied }` | at first setup, a bucket-scoped store's bucket holds anything besides its own folder and the bucket marker, or the marker `coven-bucket` names another store; on reconnecting, restoring or joining, the marker is missing, malformed or names another store | §20.7 |
| `InvalidConfiguration` | an S3 location contradicts its host, names an IAM endpoint wrongly, or has an unknown host outside setup | §20.7, E.3, E.12 |
| `StorageSetupError::S3VersioningUnsupported` | a versioned bucket without a versioned certification | §20.7, E.9 |
| `S3VersioningUnknown` | an uncertified profile can't read the bucket's versioning state | E.9 |
| `Unavailable` | R2's documented lack of versioning, at R2's known host | E.9 |
| `StorageSetupError::ProviderCheck` | the S3 safety check failed, naming the first failed of `CreateOnce`, `MultipartCreateOnce`, `PublicationTime` and `Behavior` | §20.8, E.8 |
| `StorageFailure::CopyTooLarge` | an object past 5 GiB on a profile that publishes by S3 copy | §20.6 |
| `StorageFailure::KeyBeyondStore` | a new S3 member key reaches beyond its store | §20.10, E.12 |
| `ClockSkew` | a request still fails for clock skew after one retry with the server's offset | §20.7 |
