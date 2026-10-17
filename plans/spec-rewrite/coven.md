# coven

Coven lets an app keep its data in SQLite on each device and sync it between a
household's devices through cloud storage the household already pays for. There is
no coven server. This document explains how it works and why, for someone deciding
whether the design is right.

Two companion references hold what this document doesn't: [format.md](format.md),
the exact bytes coven writes, and [api.md](api.md), the Rust API an app calls.
Proof links point into [proofs/](proofs/), Lean models checked by machine. A link
proves what its theorem states about the model, under the model's assumptions; it
does not prove the Rust code. Tests compare the Rust against several of these
models.

*(Draft note: this replaces coven.md and sync-pass.md. Links are written as if it
lived in `spec/`. The provider detail of the old §4 would move to
`spec/providers.md`.)*

The examples follow one household. Ana, Ben and Carol share a store called
Household on Dropbox. Ana and Ben also share a circle called Gifts, which Carol
can't read. Each has a phone and a laptop; Dan joins later.

---

## Part I. What coven is

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

The app writes ordinary SQL to its own tables. It tells coven which tables sync.
Coven captures each committed change, encrypts it, publishes it to storage, reads
everyone else's, and merges them into the local database. Rows can carry files,
such as songs or photos, which sync with them.

### 2. What you can rely on

These are the promises. The rest of the document explains how each is kept.

- **Local writes never wait.** A commit needs no network and never waits for
  sync. It can still be refused for local reasons: a SQL constraint, a write into
  a circle the member isn't in, or an audience in the middle of a reload
  (`AudienceReloading`, §16).
- **Every device converges.** Two devices that have applied the same history hold
  the same rows, whatever order things arrived in.
- **Conflicts are decided, not hidden.** When two edits can't both stand, one
  wins by a rule every device applies the same way, and the other is kept as a
  *lost value* the app can show and offer back (§9).
- **Only members read the data.** Everything coven stores is encrypted. The
  provider sees object sizes, counts, timing and a few routing facts, never
  names or values. A circle's rows are readable only by its members (§15).
- **Only authorized members change membership,** and the store always keeps at
  least one admin (§10).
- **Removal is final from a point in history.** Once a person or device is
  removed, nothing they send afterwards counts (§14).
- **Storage stays bounded.** Old history is replaced by snapshots and deleted,
  without waiting for devices that are away. A device that comes back after a
  year catches up from a snapshot and keeps its unsent edits (§16).
- **Failures are visible.** Anything that can't proceed tells the app what it is
  waiting for (§19).

### 3. What coven assumes

- **The provider is honest but curious.** It may read what it stores. It does
  not alter or withhold it. It sees sizes, counts, timing and which account
  writes. A thief holding storage credentials without the store key may behave
  worse, but can't read anything.
- **Deletion can't be prevented.** Whoever controls storage can delete it, and
  no design stops that. Coven doesn't rebuild lost storage; the household starts
  a new store.
- **Current members trust each other.** Each has access to the storage and is not
  trying to damage it. An ex-member may keep keys they once had.
- **A person keeps their restore code private.** Anyone who has it can act as that
  person while those keys are current. Coven offers no recovery from a stolen
  code; the household removes that person and starts over if needed.
- **The building blocks work:** the ciphers, signatures and hashes, the OS
  keychain, SQLite's transactions, and the provider behaviours §21 lists. The
  proofs take these as given.

---

## Part II. How data moves

### 4. Writing locally

The app declares which of its tables sync; the rest stay on the device. A
*write* is one SQLite transaction that changes synced tables, and a *row change*
is one row's insert, update or delete inside it.

When a write commits, coven records, in the same transaction:

- the rows as they now stand;
- a *write record* for upload: each row change with its old and new values, the
  write's device and number, its timestamp, the schema version it was made with,
  and how far this device had read (`read_through`, §8);
- the facts later checks need, such as who originally created each owned row
  (§9.6).

So a committed write is never lost, even if the app crashes a moment later, and
writing needs no key and no storage: Ana can use the app before she has set
storage up at all.

```
ana-phone, write 3, 2026-10-02 12:00:00.000 #0
  read_through: slot 20
  notes  row 42  update  title: "Grocry list" → "Grocery list"
  tags   "errands"  delete
```

Coven keeps its own bookkeeping in tables inside the same database, which the app
can't read or write. The app reads its own tables normally, on several read-only
connections at once, and can subscribe to a query that reruns when rows it read
change.

### 5. The sequence

All of a store's history is one numbered sequence of objects in storage:
`<store>/seq/1`, `seq/2`, `seq/3`, and so on. Each object is a *slot*. A slot
holds one of four things:

- **writes:** one device's queued writes, sent together in one pass;
- **an admin entry:** a membership or settings change, such as adding Carol;
- **a key copy:** a key sealed to one member (§12);
- **a snapshot announcement:** "a snapshot of this audience, covering slots up
  to n, is at this path" (§16).

Every device applies slots in order, 1, 2, 3, and never skips one. Because
everyone applies the same slots in the same order, everyone agrees on what
happened and when. That single agreed order is the backbone of the design: it
decides who was a member when a write was made, which of two inserts of the same
unique value came first, and when history can be deleted.

#### Claiming a slot

To publish, a device *claims* the next slot: it creates `seq/n+1` with a request
the provider accepts only if nothing is there yet. Every supported provider has
such a create-once request (§21). If two devices try the same slot, exactly one
wins.

Ana and Ben both have writes to send, and both have applied through slot 40.
Both try to create slot 41. The provider stores Ana's and refuses Ben's. Ben
reads slot 41, applies Ana's writes, and tries slot 42 with the same writes he
had queued. Nobody's work is lost; Ben's simply lands one slot later.

A claim can also go wrong in a less obvious way: the request succeeds, but the
reply never arrives. Ana retries, and the provider says the slot is taken. Taken
by whom? Ana reads slot 41 and compares it with the exact bytes she sent. If they
match, the slot is hers. Without that comparison she would claim slot 42 too, and
her writes would land twice.

So before every create, a device records in its local *journal* the slot it is
claiming and the exact bytes. After a crash or a lost reply, it settles that
claim first: read the slot, compare, and only then move on.

#### Why devices check the floor

Old slots are eventually deleted (§16). That creates a trap: once slot 41 is
deleted, its path is empty again, and a create-once request for it can succeed.
A request from long ago, delayed in the network, could land there and look like a
genuine slot to a device that reads it.

The defence is a single record, the *floor*, at `<store>/floor`. It says which slot
is the first one still kept. A device that reads slots checks the floor after
reading them and before applying them. Anything below the floor is not trusted;
the device loads a snapshot instead. A device that has just claimed a slot checks
the floor the same way before believing its claim.

#### Settling a claim after its slot is gone

A device can be away so long that the slot it was claiming has been deleted. It
then settles from the floor's snapshot, which records, for every device, the
number of the last slot that device claimed and a hash of that slot's bytes.
Each device numbers its claims 1, 2, 3, across all kinds of slot.

Ana's phone was claiming its claim number 9 when it went offline. The floor's
snapshot records her phone's last claim as number 9 with a hash matching what she
sent, so her claim landed. Had it recorded number 8, the claim never landed and
she sends again. A record above 9, or number 9 with a different hash, means
another copy of her phone is using its identity, for example a restored backup;
coven stops that copy (§19.3).

The hash matters. Two copies of the same phone can both claim with number 9. The
one that lost would otherwise see "9" and believe the winner's slot was its own.

#### A stuck device stops

A device that can't apply a slot stops there. It may be missing a key, need an
app update to read a newer format, or have read damaged bytes. It applies
nothing after that slot and sends nothing, until the cause clears. The app can
still read its data and commit local writes, and it is told why sync stopped.

Stopping is deliberate. Skipping slot 80 could mean missing Ben's removal at 81
while still sending writes Ben's removal should have cut off.

Proofs: a claim with byte comparison wins exactly once
([`conclude_correct`, `exactly_one_claimant`, `no_compare_lands_twice`](proofs/sequence/CovenSequence/Claim.lean));
the floor check is needed and enough
([`floor_check_sound`, `stale_create_wins_deleted_slot`](proofs/sequence/CovenSequence/Claim.lean),
[`claim_floor_check`](proofs/sequence/CovenSequence/Retention.lean));
settling after deletion
([`settle_correct'`, `settle_from_floor`, `copies_fool_number_only`, `lands_once_never_lost`](proofs/sequence/CovenSequence/Settle.lean));
a stopped device stays stopped
([`stopped_stays`](proofs/sequence/CovenSequence/Device.lean)).

### 6. A sync pass

A device syncs in *passes*. A pass does four things, in order:

1. **Read forward.** Read `seq/<last applied + 1>`, then the next, until one is
   missing. A device far behind finds the end quickly by checking n+1, n+2, n+4,
   … and narrowing. On a miss, read the floor.
2. **Apply.** Check each slot's signature, then apply it in one database
   transaction together with "applied through this slot". A failed transaction
   changes nothing. Before committing the slots it just read, the device checks
   the floor (§5).
3. **Claim.** Settle any claim still in the journal, then send: at most one
   writes slot holding this device's queued writes, plus any admin operations
   the app asked for (§11).
4. **Effects.** Do the work the new state calls for: share or unshare provider
   access, delete S3 keys no longer in use, replace keys a removed member had
   seen, take snapshots, delete history the floor has passed.

A quiet store costs two requests per pass whatever its size: read the next slot,
which is absent, and read the floor. Ana's store costs the same two requests with
three devices or twenty, and with 20,000 files or none.

Files upload and download outside the pass, so a large upload never holds up
sync (§17). Retries wait 1, 2, 4, … seconds up to five minutes, and a provider's
"retry after" is honoured by every request to that account.

### 7. Order

Two things order writes, and they answer different questions.

**Causality** answers "what had the author seen?" Each write records
`read_through`, the last slot its device had applied when it was made. Its past
is everything in slots up to `read_through`, plus the same device's earlier
writes. That record never changes, even if the write is sent much later. Ben and
Carol both edit while at slot 20: their edits are *concurrent*, even if Ben's lands
in slot 21 and Carol's in slot 30, because neither saw the other.

Applying slots in order is enough to respect causality. Everything a write saw is
in an earlier slot, or is an earlier write of the same batch.

**Timestamps** answer "of two concurrent edits to the same cell, which wins?"
Every write carries a timestamp: the wall-clock time in milliseconds, a counter,
and the device's id, compared in that order. No two devices ever produce equal
timestamps.

A device never stamps a write earlier than anything it has already seen. If its
clock is behind the latest timestamp it knows, it reuses that time and raises the
counter. So Ben's edit made after reading Ana's always beats Ana's, even if Ben's
clock is slow. A device whose clock jumps a year ahead drags everyone's timestamps
forward until real time catches up, but nothing freezes: writes still apply as
soon as their causes have.

Proofs: slot order respects causality
([`ordered_application_causal`, `receivers_agree`](proofs/sequence/CovenSequence/Causality.lean)).

### 8. Merging

Applying a write means merging it into the local database. Every device that has
applied the same writes ends with the same database, whatever order they arrived
in.

#### Cells

A *cell* is one column of one row: note 42's title. Of two concurrent values for
a cell, the one with the larger timestamp stays. A cell only one write touched
keeps that write's value. So if Ben retitles note 42 while Ana, offline, edits its
body, both edits survive.

At 13:00 Ben retitles note 42 "Weekly groceries". At 14:00 Carol, offline since
12:30, retitles it "Shopping". When she comes back, every device applies both
edits, in whatever order, and every device ends with "Shopping".

#### Lost values

"Weekly groceries" didn't survive, and no one who replaced it had seen it. Coven
records it as a *lost value*: which cell, the value, which write set it, and which
write replaced it. The app can show it ("Ben's title was replaced") and offer to
restore it. A value replaced by someone who had seen it, such as Ben replacing
Ana's earlier title after reading it, is not lost: that was an edit, not a
conflict.

Every device ends with the same lost values, because they follow from the writes
alone.

#### Deletes

Each row has a *generation*: 1 when created, 2 when deleted, 3 if added again,
and so on. Each row change records the generation it was made at.

An edit made at a generation that has since been deleted loses, whenever it
arrives. Ana deletes note 43 at 16:00; Ben, offline, edits its title; Ana re-adds
note 43 at 17:00. Ben's edit was made at generation 1, so it doesn't come back to
life in generation 3, even if his clock stamped it later than 17:00. It becomes a
lost value, replaced by Ana's delete.

Two concurrent deletes of the same generation count as one.

#### Rows taken out, and rows that come back

Some conflicts can't be settled cell by cell. Coven settles them by *taking a row
out*: removing it from the app's table while a reason holds, and recording it as
lost with all its values. It is never stored as a delete, so it comes back if the
reason goes away. The reasons are:

- **Its parent was deleted** (foreign keys, §9.1).
- **Its merged values fail a CHECK** (§9.3).
- **Its circle was deleted** (§15).
- **It lost a unique value** to an earlier claim (§9.2).
- **The same key is present in two audiences** (§15): the store's row wins over a
  circle's.

A row can be out for several reasons at once and comes back only when none holds.
The app can also dismiss it for good, which records a real delete.

Since a removed row is invisible to the app, the app can insert the same key
again. Coven treats that as an update of the removed row, which comes back if the
new values clear its reasons.

Proofs: two orders of the same writes give the same merged state
([`merge_converges`](proofs/merge/CovenMerge/Converge.lean)), and the removal rules
give the same rows in any order
([`device_converges`, `rule_order_converges`](proofs/merge/CovenMerge/Removal.lean));
one sequence of slots determines one state
([`prefix_determines_state`](proofs/sequence/CovenSequence/Converge.lean)).
The [merge write-up](proofs/merge.md) explains the model.

### 9. The rules SQL brings with it

The app's schema has foreign keys, unique constraints, CHECKs and triggers. Each
device's SQLite enforces them on its own writes. Merging writes made on different
devices can break them, so coven decides each case the same way everywhere.

#### 9.1 Foreign keys

Attachment 9 points at note 43. Ana deletes note 43 while Ben, offline, adds
attachment 9 to it. On Ana's device SQLite already ran the foreign key's action
for the attachments it knew about. Attachment 9 it never saw.

When attachment 9 arrives at a device where note 43 is gone, coven follows the
foreign key's declared action:

- **cascade, restrict or no action:** attachment 9 is taken out, as SQLite would
  have done or refused, and recorded as lost;
- **set null:** it stays, and its reference reads as null.

If Ben had instead moved attachment 9 to note 44 in the same offline session, it
would survive on every device: it no longer points at a deleted parent.

Coven refuses, when the database opens, schemas where it couldn't follow these
rules: `SET DEFAULT` actions, set null on a `NOT NULL` or key column, and a few
others listed in Appendix A.

#### 9.2 Keys and unique values

Each synced table has one of two kinds of primary key:

- **independent:** each new row gets a random UUID, so rows made on different
  devices never collide (notes);
- **shared:** the app derives the key from what makes the row unique, so the same
  thing made on two devices is one row (the tag "urgent").

Coven refuses keys SQLite picks itself, such as integer rowids, because two
devices would pick the same number for different rows.

A unique constraint that isn't the key can still be claimed twice offline. Ana
and Ben both create a note titled "Groceries". On each device that was fine. When
both arrive, **the first slot wins**: Ana's note claimed slot 40 and Ben's slot
41, so Ana keeps the title and Ben's note is taken out and recorded as lost. If
Ana later renames hers, the title goes to the earliest remaining claimant: Ben's
note comes back.

Slots decide this, not timestamps, so a device with a wrong clock can't steal a
value by backdating. A write still waiting in a device's queue has no slot yet
and ranks after everything published.

#### 9.3 CHECK constraints

A CHECK that reads several columns can fail after a merge. Todos need
`start <= end`; Ana moves the start later while Ben moves the end earlier. Each
device's own write passed; together they fail. The row is taken out and recorded
as lost on every device, and comes back when a later write makes it pass.

#### 9.4 Triggers

The app declares each trigger on a synced table as **local** or **shared**.

- A local trigger runs on every device, for its own writes and applied ones,
  and may write only local tables. A search index is a local trigger.
- A shared trigger runs only on the device making the write. Its changes are
  part of that write and sync with it. Setting `edited_at` when a body changes
  is a shared trigger. It declares `WHEN NOT coven_applying()` so it doesn't run
  again when other devices apply the write.

#### 9.5 Dependent changes

Some changes only make sense on top of data that turned out not to count. If
Ben's write was refused, for example because he had already been removed, then
Carol's later edit to the row Ben's write created has nothing to stand on.
Coven tracks, for each row change, which writes supplied the data it changed:
the row's generation and the cells it replaced. If any of those was dropped, the
change is dropped too and recorded as a lost value with that cause. Recorded
reads don't count for this, only actual data.

#### 9.6 Owner tables

A table can be declared `owner_only`. A row's owner is the member who created it,
decided by first slot. Only the owner may update or delete it directly. Foreign-
key actions caused by someone else's authorized change still apply: if Ben
deletes a track, the cascade removes Ana's review of it.

This is for authenticity, not secrecy: "Ana rated this" really was Ana. A change
to someone else's owned row is refused on every device and recorded as lost.

Proofs: first slot wins and stays
([`first_slot_wins`, `owner_stays`](proofs/sequence/CovenSequence/Unique.lean),
[`first_after_release`, `older_loser_returns`](proofs/sequence/CovenSequence/LiveClaims.lean),
[`lower_write_number_wins`, `retained_creator`](proofs/sequence/CovenSequence/Owners.lean));
a change is kept exactly when none of its data sources was dropped
([`retained_iff`](proofs/merge/CovenMerge/DataDependency.lean));
a kept foreign-key effect on an owned row traces back to an authorized direct
change in the same write
([`derived_root_authorized`](proofs/merge/CovenMerge/OwnerAdmission.lean)),
and an accepted effect is one the schema actually implies
([`no_forged_effect`](proofs/merge/CovenMerge/ForeignKeyActions.lean)).

---
## Part III. People

### 10. Members, roles and devices

A *member* is a person, with a random, permanent `MemberId` and two current key
pairs: one to sign with and one that others seal keys to. A member has devices;
each *device* is one installation of the app, with its own id.

What each person may do:

| Action | Who may do it |
|---|---|
| Add or remove a member, change a role | an admin |
| Add a device | the member it belongs to, from that new device itself |
| Remove a device | its member, or an admin |
| Replace a member's keys | that member, or an admin |
| Set a member's provider access | that member; an admin for S3 keys |
| Create a circle | any member, who becomes its first member |
| Manage a circle | its members |

The store always keeps an admin: removing the last one, or demoting them, is
refused. The member whose provider account holds the store (the Dropbox folder's
owner, say) can't be removed, because nobody else could take the provider access
back.

Every change to membership or settings is an admin entry in a slot. A device
judges each entry against the state just before its slot, so every device judges
it the same way. An applied entry is final: nothing later undoes it, although a
later entry can change things again.

Ana and Ben, both admins, each try to remove the other. Ana's removal claims slot
41 first. Ben reads it, no longer has authority, and his attempt fails. No removal
of Ana ever enters history.

A device registers itself: a new installation claims an entry adding its own
fresh id. Device ids are never reused, and neither are member ids. A person who
is removed and invited again gets a new member id.

Proofs: authority is checked against the preceding state and an admin always
remains
([`applied_membership_authorized`, `last_admin_refused`, `every_prefix_has_admin`](proofs/sequence/CovenSequence/Authority.lean));
a member who has lost every device can still register a new one
([`member_without_active_device_can_register`](proofs/sequence/SequenceApply.lean)).

### 11. Admin operations

Every admin action, from renaming a circle to replacing keys, follows the same
path, so there is one way to get it right:

1. **Catch up** to the end of the sequence and check the action is allowed.
2. **Journal, then prepare.** Record each piece of preparatory work before doing
   it: a new key, a provider share, an S3 key, a snapshot upload. Then do it.
3. **Claim** the next slot with the entry.
4. **If someone else claimed it first,** apply their slot and check again. If the
   action is still allowed, claim the next slot. If not, undo everything the
   journal recorded and fail with the reason.
5. **Finish** local work, such as installing a new key, before reporting success.

The journal makes this safe across crashes. After a restart the device settles
its claim first (§5), then either finishes or undoes. An undo waits for every
request it already sent to end before deleting what it made, so a request that
lands late can't leave something behind. A failed undo stays in the journal and
retries. Some things can't be undone: a key copy already published has disclosed
that key to its recipient, so it stays in history and counts as exposure (§12).

Carol rotates the Gifts key. Her journal holds the new key and the copies she
published at slots 41 and 42. She claims 43 and crashes before the reply. On
restart she settles 43. If her rotation landed, she finishes. If slot 43 removed
her from Gifts, she undoes what's left and the call fails. Copies 41 and 42 remain
in history.

Proofs: an operation occupies at most one slot, and confirmed undo leaves nothing
behind
([`operation_outcome`, `steps_inv`, `forgetful_lands_twice`, `eager_undo_leaks`](proofs/sequence/CovenSequence/Operation.lean)).

### 12. Keys and encryption

- **The store key** encrypts everything the store holds. **Each circle's key**
  encrypts that circle's rows. **Each file** has its own key, carried inside its
  encrypted row (§17).
- **Every slot is signed** by its author's member key. The signature covers the
  slot's path, so the provider can't move one object to another's place.
- **Contents are encrypted with XChaCha20-Poly1305;** signatures are Ed25519;
  keys are sealed to members with X25519 sealed boxes. Format.md has the details.
- **What the provider can see** is each slot's kind (writes, admin, key copy,
  announcement) and a key copy's routing (which member and key it's for), so a
  new member can find their copy before they hold any key. Everything else,
  including which device wrote a slot and which circles its parts belong to, is
  encrypted under the store key.

#### Getting a key to someone

A key reaches a member as a *key copy*: the key sealed to that member's current
public key, published in its own slot. A new key is introduced by an admin entry
naming its id and a hash of its bytes. Copies are published first, then the
introduction, so by the time a key is in use, everyone who should have it has a
copy.

A copy is exposure, whether or not its recipient ever opens it. So when Ben is
removed, any key a copy ever reached Ben with can't be used for new data. A
remaining member's device creates a new key and introduces it before sending
anything else into that audience. If Ana is outside Gifts, she can't make a new
Gifts key; Gifts waits until one of its members is online. Older keys stay
readable, because old data was written with them.

Removal can't take back what Ben already downloaded. The provider's access
cut-off (§14) stops him downloading more.

#### Custody

Private keys stay in the OS keychain, or another custody the app supplies, never
in the database. The app can keep its own secrets, such as an API token, in
separate keychain entries under the same access policy.

### 13. Joining and restoring

#### A person's new device: the restore code

A *restore code* holds a member's id and current private keys, and the store's id,
name and storage location, but never storage credentials. Coven returns it at
setup and whenever the member's keys are replaced, and can show it again as a QR
code. On Apple platforms it is also kept in iCloud Keychain.

Ana's new laptop scans the code from her phone, or reads it from iCloud
Keychain, or she types it. It signs in to her Dropbox, reads the floor and her
key copies, loads the snapshot and the slots after it, and registers itself as a
new device. An old code whose keys have since been replaced is refused.

#### Adding a person: invites

Ana adds Dan to Household on Dropbox:

1. Ana enters Dan's Dropbox email and picks his role. Her phone shares the store's
   folder with that account and shows an *invite*: a code holding the store's
   location, a member id chosen for Dan, and a one-time secret.
2. Dan's phone scans it, signs in to his Dropbox, creates Dan's member keys, and
   writes a *join request* to storage, encrypted with the invite's secret and
   signed with Dan's new key.
3. Ana's phone, the only one holding the secret, shows the request. Ana approves.
4. Ana's phone publishes copies of every store key to Dan, then the entry adding
   him.
5. Dan's phone opens its keys, loads the store, and registers itself.

The invite only lets Dan ask; Ana's approval lets him in. Declining, or letting
the invite expire after a day, takes back the provider access it granted. On S3,
the invite carries an access key made for Dan, which declining deletes.

On Dropbox, OneDrive and iCloud, only the account that holds the store can share
it, so only that member's devices make invites.

#### Losing everything

If Ben loses every device and his restore code, an admin invites him again to
his *existing* member id. Approving binds new keys to it, keeping his role and
circles. Writes signed with his old keys after that point don't count. If every
member loses every device and code, the store is lost: everything is encrypted.

### 14. Removing people and devices

A removal is an admin operation. Its slot is a line in history: writes from the
removed device or member that land in later slots are refused on every device,
consumed without effect and reported to the app (`WritesRefused`). Writes that
landed before the removal still count. The same line falls at a member-key
replacement: later writes signed with the old key don't count.

Ana's old phone sends write 8 while her replacement device claims slot 70. If
write 8 lands at slot 69, it counts. At slot 71, it doesn't.

Removing a member also removes their devices and their circle memberships. A
circle left empty is deleted.

#### Provider access

Removal in the sequence stops Ben's writes from counting. Taking away his
storage access stops him reading anything new. That second step is provider
work, done by whoever can do it:

- **Dropbox, OneDrive, iCloud:** the account that holds the store stops sharing
  with Ben's account. That member's device does it when it applies the removal,
  so if Ana owns the folder and is offline, Ben's removal applies at once
  everywhere, but the unsharing waits for Ana's next pass. The app's pending list
  shows that wait.
- **S3:** each member has their own access key. Removing Ben deletes his. Keys
  are made and deleted with the account's admin access, which the person
  supplies for that one operation and coven never stores. Guided console steps
  are the alternative.

These providers can't cut off one device of an account alone. To lock out a lost
phone, the person removes the app's access from their account and signs in again
on their other devices; on S3, an admin replaces that member's key.

Coven only removes a share that grants that one account. A share that also grants
others is left in place and reported.

Proofs: the cut-off
([`cutoff`](proofs/sequence/CovenSequence/Cutoff.lean)).

### 15. Circles

A *circle* is a group of members inside a store who share rows the others can't
read. Every synced row belongs to an *audience*: the store, or one circle.

The app says how each table's rows get their audience:

- a **root** table has an audience column (`store` or a circle's id);
- a **descendant** table follows one foreign key and takes its parent's audience,
  so a todo is in its list's circle;
- any other table is in the store.

Rules that keep circles sound:

- **References stay readable.** A row may point only at rows everyone who can read
  it can also read. A circle row may point into its circle or the store; a store
  row only into the store. Otherwise a device in the store couldn't check a
  foreign key into a circle it can't read.
- **Unique values don't cross audiences.** A unique constraint must include the
  audience column, or the foreign key that fixes the audience. Otherwise Ana's
  device, in the circle, would see a clash Dan's device can't.
- **Moving a row** between audiences is an ordinary update of its audience column.
  It is a delete in the old audience and a full insert, with its descendants, in
  the new one. So devices that gain the row get all of it.
- **Members only.** Writes into a circle the member isn't in are refused.
  Everyone can see that a circle exists, who is in it, and when and how much it
  writes; only members read what it says.

**Joining and leaving.** Adding Ben to Gifts gives him its older keys too, and
his device loads Gifts' history from its snapshot; writes into Gifts wait until
that load finishes. Removing Ben from Gifts means its remaining members replace
the key before writing more. Edits Ben made before he read his removal still
count, because he made them while a member, using keys he already had.

**Deleting a circle** hides its rows from that slot on, including late edits
that arrive afterwards. Their values are kept as lost values, so the app can
offer to move them elsewhere.

One slot can carry both Ana's store note and her private Gifts pin, each part
encrypted with its own audience's key. Every device checks the whole slot's
signature; it opens only the parts it may read.

---

## Part IV. Keeping storage bounded

### 16. Snapshots, the floor, and deleting history

The sequence would grow forever. Instead, devices take *snapshots*: the complete
state of one audience as of a slot (its *coverage*), including rows, merge
records, lost values, and the evidence old writes still need. A store snapshot
also carries the admin history needed to know who was a member when, and each
device's last claim number and hash (§5).

#### Taking a snapshot

A device uploads a snapshot, then claims a slot announcing it. An upload never
announced counts for nothing, and its journal deletes it. A device takes a new
snapshot of an audience once the writes since the last one exceed that snapshot's
size, or 1 MiB if there's none yet.

Two rules keep snapshots consistent with schema changes and resets (§18, §19.4):

- An ordinary snapshot must cover the latest schema raise and reset before it.
  A snapshot captured before a raise and announced after it is refused, because a
  device joining later would load it and then need slots that may be gone.
- A raise or reset snapshot must cover at least as much as every earlier
  announcement for its audience.

Applying an announcement doesn't read the snapshot: the announcement is signed and
carries the coverage and a hash of the snapshot. A snapshot is read only when
someone loads it, and is checked against that hash. So a snapshot that an undone
operation deleted never blocks anyone.

#### The floor and deletion

Once every audience has a snapshot covering slots up to some point, and about a
week has passed, any device with permission:

1. writes a new floor, naming the first kept slot and the snapshot each audience
   relies on, with a request that succeeds only if the floor hasn't changed since
   it read it;
2. then deletes the slots below it.

Deletion never waits for an absent device, and no device has to acknowledge
anything. The week is a cost setting, sparing short absences a full reload; it is
not needed for correctness. A circle whose members never come online to snapshot
it holds back deletion only of its own coverage.

#### Coming back after a long absence

Ben's laptop was off for a year. The floor now starts at slot 501; Ben had
applied through 40. Reading slot 41, he finds it missing, reads the floor, and
reloads: he loads the floor's snapshots and applies the slots from 501 on. His
unsent edits keep their original timestamps and `read_through`, and are judged
exactly as if he had sent them a year ago. Being late doesn't refuse them.

A reload commits all at once: rows, merge records, lost values and the applied
slot together. A failed reload leaves the old state visible. During an ordinary
reload the app can keep writing. During a reload caused by a reset, a schema
raise or newly joining a circle, writes into that audience are refused with
`AudienceReloading` until it finishes, because they'd be made against state about
to be replaced.

Proofs: deletion keeps every reader able to reach the head
([`read_progress`, `reaches_head`, `joiner_reaches_head`](proofs/sequence/CovenSequence/Reader.lean),
[`deletion_keeps_reachable`](proofs/sequence/CovenSequence/Device.lean));
a deadline exists without waiting for absent devices
([`bounded_retention`, `uncovered_has_no_deadline`](proofs/sequence/CovenSequence/Deadline.lean));
reload keeps queued writes exactly once
([`queued_exactly_once`, `reload_sound`](proofs/sequence/CovenSequence/QueueReload.lean));
why the snapshot rules are needed
([`slot4_refused`, `stale_floor_strands_joiner`](proofs/sequence/CovenSequence/ReaderExamples.lean)).

### 17. Files

Rows can carry files. The app declares, per table, which column holds the file,
its size, its content hash and coven's storage reference, and whether devices
download it as soon as the row arrives or only on first read.

There are two kinds:

- **User-provided:** the user's own file at a path, such as a song in their music
  folder. Coven records where it is and reads it to upload, but never copies,
  moves, changes or deletes it. If it moves or changes before upload, coven says
  so.
- **App-provided:** bytes the app hands over, which coven keeps.

Attaching a file is part of the write that changes its row. That write picks the
file's random id and key and its fixed storage path,
`<store>/files/<device>/<file>`, and queues the upload. The row never changes
again when the upload finishes, so a slow upload can't overwrite a newer file.

The file is encrypted with its own key in 64 KiB chunks, each checked on its own.
So the app can read any byte range, fetching only the chunks it needs: playing a
song and seeking within it are reads of different ranges. Fetched chunks go into a
size-limited cache. The app can pin a file to keep it whole for offline use.

A file's status is one of:

- **available:** the complete file is in storage;
- **uploading on Ana's phone:** not there yet, and the uploader is still active;
- **missing:** not there, and the uploader is gone;
- **damaged:** something is there, but not the right size.

A network error is never taken to mean "absent".

Uploads retry on their own when their cause can clear: a missing source is
re-checked after a pause, a sign-in failure retries when the connection changes.
The app never has to nudge them. A stored file is deleted only once nothing that
could still point at it remains: no row, lost value, snapshot or queued write.

---

## Part V. Change and failure

### 18. Schema changes

The app owns its schema and changes it with migrations. Coven sorts each
migration into one of two kinds by comparing the schema before and after:

- **An addition** only adds tables or columns. Devices on the old version keep
  working, and their writes apply everywhere. A device holds a newer-version
  write it can't read until its app updates.
- **A breaking change** is anything else: renaming, retyping or dropping, a new
  constraint, or a migration that changes rows. It raises the store's schema
  version.

The first device whose app migrates claims the raise. It snapshots each audience
it can read at the new version, announces the snapshots, then claims the raise
naming them. Other devices load those snapshots when they update. A device whose
app is older stops uploading until it updates, and says so.

Writes made on the old version still arrive afterwards: Ben edited offline before
updating. For each breaking migration, the app can supply a converter that turns
an old-version row change into the new shape. With one, Ben's late edit converts
and applies. Without one, it is kept as a lost value in its old shape, so nothing
disappears silently.

Proofs: a raise keeps every queued original
([`raise_without_waiting`, `originals_preserved`](proofs/sequence/CovenSequence/RaiseQueue.lean));
conversion
([queue model](proofs/queue.md)).

### 19. When things go wrong

#### 19.1 What the app sees

Coven keeps one local record per thing that can't proceed: a stuck slot, an
upload, a key copy, an operation, a reload, a provider change. Each names its
first unmet condition, such as "needs an app update", "key not available yet",
"waiting for Ana's device to unshare" or "source file moved". The app reads the
list and subscribes to it. A record clears only when its condition changes,
never because time passed.

Sync status is one of: no storage set up, stopped, offline, syncing, synced,
blocked at a slot with a reason, or failed.

#### 19.2 Reloading one device

The app can ask a device to reload from snapshots and the sequence, keeping its
id and unsent writes.

#### 19.3 Another copy of the same device

Restoring a phone from a backup can leave two installations with one device id.
Coven notices when it reads a slot signed by its own id that its journal never
attempted, or settles a claim and finds another copy's. That copy stops with
`DeviceIdInUse`. The app can then reset the device: discard its local database
and unsent work, take a fresh id, load storage, and tell the app what was
discarded. A damaged database is reset the same way. Coven never resets a device
on its own.

#### 19.4 Resetting an audience

If devices disagree about the data, the person can choose one device's version
and reset an audience to it: an admin for the store, a circle member for a
circle. The device snapshots what it has, announces it, then claims a reset entry
naming it. Every device, the resetting one included, reloads from that snapshot.
Writes that hadn't seen the reset are ignored for that audience: they are
consumed, not applied, and not recorded as lost. That is what a reset is for.
Writes made after seeing it apply normally. If two people reset at once, the
first claim wins and the second recaptures and goes after it.

---

## Part VI. Storage

### 20. What coven needs from a provider

A store lives on one provider: an S3-compatible service, Dropbox, OneDrive, or
iCloud through CloudKit. Every member reaches it with their own account or key.
Coven needs:

- **create once:** create an object only if its path is empty, refusing an
  occupied path without touching it, even when many devices race, and including
  large uploads published in parts;
- **conditional replace:** replace the floor only if it hasn't changed since read;
- **complete reads,** whole and by byte range;
- **exact status:** whether an object exists, its size, time and revision. An
  error is never "absent";
- **listing** of a prefix, used only for files and setup checks;
- **delete;**
- **sharing** with another account, where the provider has it.

A provider lacking create-once can't hold a store. Setup checks these properties
against the real server with the person's credentials before using it, and
refuses with the first failed check.

#### The providers

- **Dropbox and OneDrive** share a folder with each member's account. Coven signs
  in through a browser or the platform's sign-in sheet and holds the tokens
  itself. Dropbox offers an app-folder key, which can't be shared, and a
  full-Dropbox key, which can; the choice is made when a store is created.
- **iCloud (CloudKit)** keeps the store in a zone in the owner's private
  database and shares it with a CloudKit share. It works on Apple platforms only.
- **S3** services differ in how they implement create-once, multipart uploads,
  timestamps and checksums. Coven knows each certified service's behaviour by
  name and can detect an unknown one's. Every setup re-checks safety against the
  server. Garage, which overwrites occupied names, is refused. A service is
  certified only after racing many writers against it; versioned buckets need
  their own certification. Each member has their own access key, limited to the
  store's prefix where the service allows it, otherwise to a bucket of the
  store's own.

The full provider detail, including each S3 profile's behaviours and certification
dates, the detection probes, and how member keys are made on each service, is in
[providers.md](providers.md), moved from the former §4.

### 21. Costs

- A quiet pass makes two requests, however large the store.
- Receiving a slot adds its read. Sending adds a create and a floor read.
  Losing a race adds a read and another attempt; there's no bound on how often a
  device can lose.
- A device far behind finds the end in a logarithmic number of checks, then
  reads what it needs or reloads.
- Reading a slot or snapshot streams it once, checking as it goes.
- Coven never holds the database writer across a network request, a keychain
  call, decoding or a timer. A quiet pass commits nothing.

### 22. What's proved and what isn't

The Lean models check the design's rules, not the Rust: the sequence (claiming,
settling, the floor, reading, deletion, operations, causality, authority, unique
values, owners, cut-off, snapshots, reloads, raises), the merge, conversion of
old writes, and pieces of storage behaviour. Tests run the Rust against the
sequence, merge, conversion and storage models on generated cases and compare
the answers.

The models assume correct cryptography, honest snapshot contents, and providers
that behave as §20 requires. Live tests check the providers: create-once races and
floor races against each certified service.

---

## Appendix A. SQLite features in synced tables

| Feature | Status |
|---|---|
| Integer rowid keys (`INTEGER PRIMARY KEY`) | refused: two devices would pick the same number |
| Tables without a primary key | refused |
| Unique constraints besides the key | allowed; first slot wins (§9.2); must not cross audiences (§15) |
| `RESTRICT` / `NO ACTION` foreign keys | allowed; a merged child of a deleted parent is taken out (§9.1) |
| `SET DEFAULT` actions | refused |
| `SET NULL` on a `NOT NULL` or key column | refused |
| CHECK constraints | allowed; a row failing after a merge is taken out (§9.3) |
| Triggers that write synced tables | must be declared shared (§9.4) |
| Collations on key columns | must compare text by bytes |
| References to non-key columns | refused; foreign keys reference the parent's whole primary key |
| Schema changes | additions or breaking changes (§18) |
