# Sync

A device's writes commit locally. This page follows them from there to every
other device: how they're published, in what order everyone applies them, what
a sync pass does, and what the app sees while it runs. How a write merges into
another device's rows is the [Merge](/docs/merge) page.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

## The sequence

All of a store's history is one numbered sequence of objects in storage:
`<store>/seq/1`, `seq/2`, `seq/3`, and so on. Each object is a *slot*, and a
slot holds one of four things:

- **writes:** one device's queued writes, sent together in one pass;
- **an admin entry:** a membership or settings change, such as adding Carol;
- **a key copy:** a key sealed to one member ([Encryption](/docs/encryption#getting-a-key-to-someone));
- **a snapshot announcement:** "a snapshot of this audience, covering slots up
  to n, is at this path" ([Snapshots](/docs/bootstrap)).

Every device applies slots in order, 1, 2, 3, and never skips one. Because
everyone applies the same slots in the same order, everyone agrees on what
happened and when. That one order decides who was a member when a write was
made, which of two inserts of the same unique value came first, and when
history can be deleted.

A device applies each slot in one database transaction together with "applied
through this slot". If that transaction fails, both the data and the progress
stay where they were.

## Claiming a slot

To publish, a device *claims* the next slot: it creates `seq/n+1` with a
request the provider accepts only if nothing is there yet. Every supported
provider has such a create-once request ([Storage](/docs/storage#what-coven-needs-from-a-provider)).
If two devices try the same slot, exactly one wins.

<svg class="flow" viewBox="0 0 660 222" role="img" aria-label="Ana and Ben have both applied through slot 40 and both try to create slot 41. Storage keeps Ana's and refuses Ben's. Ben applies slot 41 and claims 42 with the same writes.">
<text class="hdr" x="100" y="22" text-anchor="middle">ANA'S PHONE</text>
<text class="hdr" x="330" y="22" text-anchor="middle">STORAGE</text>
<text class="hdr" x="560" y="22" text-anchor="middle">BEN'S LAPTOP</text>
<rect class="lane" x="10" y="32" width="180" height="160" rx="10"/>
<rect class="lanec" x="240" y="32" width="180" height="160" rx="10"/>
<rect class="lane" x="470" y="32" width="180" height="160" rx="10"/>
<rect class="chip" x="255" y="44" width="150" height="26" rx="7"/>
<text class="lbl s11" x="330" y="61" text-anchor="middle">seq/39</text>
<rect class="chip" x="255" y="78" width="150" height="26" rx="7"/>
<text class="lbl s11" x="330" y="95" text-anchor="middle">seq/40</text>
<rect class="chipa" x="255" y="112" width="150" height="26" rx="7"/>
<text class="lbl s11" x="330" y="129" text-anchor="middle">seq/41 · Ana's writes</text>
<rect class="chipo" x="255" y="146" width="150" height="26" rx="7"/>
<text class="lbl s11" x="330" y="163" text-anchor="middle">seq/42 · Ben's writes</text>
<rect class="chip" x="25" y="112" width="150" height="26" rx="7"/>
<text class="lbl s11" x="100" y="129" text-anchor="middle">queued writes</text>
<line class="arr" x1="179" y1="125" x2="251" y2="125" marker-end="url(#fa)"/>
<text class="sub" x="100" y="160" text-anchor="middle">create seq/41 ✓</text>
<rect class="chip" x="485" y="112" width="150" height="60" rx="7"/>
<text class="lbl s11" x="560" y="138" text-anchor="middle">queued writes</text>
<text class="sub" x="560" y="156" text-anchor="middle">same bytes inside</text>
<line class="arrd" x1="481" y1="122" x2="409" y2="122" marker-end="url(#fam)"/>
<text class="sub" x="445" y="108" text-anchor="middle">taken</text>
<line class="arr" x1="481" y1="160" x2="409" y2="160" marker-end="url(#fa)"/>
<text class="sub" x="445" y="186" text-anchor="middle">apply 41, create 42 ✓</text>
<text class="sub" x="330" y="212" text-anchor="middle">Both had applied through 40. One create of seq/41 succeeds; the other device applies it and moves on.</text>
</svg>

Ana and Ben both have writes to send, and both have applied through slot 40.
Both try to create slot 41. The provider stores Ana's and refuses Ben's. Ben
reads slot 41, applies Ana's writes, and tries slot 42 with the same writes he
had queued. Nobody's work is lost; Ben's lands one slot later.

A device claims slot n+1 only after it has applied slots 1 through n. No clock
or deadline is involved: a device that comes back after a month catches up,
then claims.

### When the reply is lost

A claim can go wrong in a less obvious way: the request succeeds, but the reply
never arrives. Ana retries, and the provider says the slot is taken
(`AlreadyExists`). Taken by whom? Ana reads slot 41 and compares it with the
exact bytes she sent. If they match, the slot is hers. Without that comparison
she would claim slot 42 too, and her writes would land twice.

So before every create, a device records in its local *journal* what it is
claiming: a claim number, the slot path, and the exact bytes. Each device
numbers its claims 1, 2, 3, with one counter across every kind of slot. It
settles its current claim before starting another, and after a crash or a lost
reply it settles first: read the recorded slot, compare, and only then move on.

| What the device finds at its recorded slot | What it does |
| --- | --- |
| its own bytes | treats the claim as landed, after checking the floor (below) |
| different bytes from another device | applies them, checks the work is still allowed, and claims the next slot |
| different bytes signed with its own device id | stops sending: another copy of this device exists ([below](#another-copy-of-the-same-device)) |
| nothing | retries the same path with the same bytes |
| a failed read | leaves the claim unsettled and tries later |

The same bytes go out on every retry. Coven encrypts slots with nonces derived
from their content ([Encryption](/docs/encryption#nonces)), so sealing the same
writes again reproduces the same object, byte for byte.

## The floor

Old slots are eventually deleted, once snapshots cover them
([Snapshots](/docs/bootstrap#deleting-history)). That creates a trap: once
slot 41 is deleted, its path is empty again, and a create-once request for it
can succeed. A request from long ago, delayed in the network, could land there
and look like a real slot.

The defence is one record, the *floor*, at `<store>/floor`. It names the first
slot still kept and, for each audience, the snapshot it relies on. A device
reads the floor after reading slots and before applying them. Anything below
the floor is not trusted; the device loads a snapshot instead. A device that
has just claimed a slot checks the floor the same way before believing its
claim. If the floor has passed the slot it was claiming, it settles the claim
from the floor's snapshot, which records each device's last claim number and a
hash of that slot's bytes.

## A sync pass

A device syncs in *passes*. A pass does four things, in order:

1. **Read forward.** Read `seq/<last applied + 1>`, then the next, until one is
   missing. On a miss, read the floor. A device far behind finds the end
   quickly by checking n+1, n+2, n+4, … and narrowing the gap.
2. **Apply.** Check each slot's framing and complete signature, then apply it
   in one transaction with "applied through this slot". The floor read at the
   final miss also checks that nothing just read has been deleted.
3. **Claim.** Settle any unsettled claim, then send: at most one writes slot
   holding this device's queued writes, plus any admin operations the app
   asked for.
4. **Effects.** Do what the new state calls for: share or unshare provider
   access, replace keys a removed member had seen, take snapshots, and delete history the floor has passed.

A pass over a quiet store, with nothing new and nothing to send, costs **two
requests**: the next slot, which is missing, and the floor. It lists nothing
and writes nothing, whether the store has three devices or twenty, and 20,000
files or none. If Ana added one note, Ben's next pass makes three reads: her
slot, the miss after it, and the floor.

A writes slot carries every write this device has queued and can send, in the
order they were made. It has no size cap; a large one uploads in parts and
becomes visible only when complete. Each single write must fit SQLite's 1 GB
value limit, or the commit itself fails with `DbError::TooLarge`. Writes the
app commits while a pass is sending wait for the next one. A queued write
leaves the queue only once its slot is confirmed and applied.

Files don't travel in slots. A separate upload worker sends them on its own
schedule, so a pass never waits for a file ([Files](/docs/files#uploads)).

## Order

Each write records `read_through`, the last slot its device had applied when
the write was made. That record never changes, even if the write is sent much
later. A write's *past* is every write in slots up to its `read_through`, plus
its own device's earlier writes.

Ana creates note 43 in slot 20. Ben reads through 20, then attaches a file to
it. Whatever slot Ben's write lands in comes after 20 on every device, so every
device has note 43 before the attachment. Applying slots in order is all it
takes.

Ben and Carol both edit while at slot 20: their edits are *concurrent*, even if
Ben's lands in slot 21 and Carol's in slot 30, because neither saw the other.
Which value a cell keeps is decided by timestamps, not slot numbers
([Merge](/docs/merge#timestamps)).

Every write is checked against its author's view: its signature with the
author's key as it stood at its `read_through`, and its author's membership and
circles there. The app sees its own commits at once, before they claim a slot;
the device shows the published slots with its local queue on top.

## A stuck device stops

A device that can't apply a slot stops there. It may be missing a key, need an
app update to read a newer format or schema, or have read damaged bytes. Until
it can apply that slot it applies nothing after it, reloads nothing, and sends
nothing: no writes, no files, no operations.

Stopping is deliberate. Skipping slot 80 could mean missing Ben's removal at 81
while still sending writes that removal should have cut off. Ben's laptop reads
slot 41, a copy of a Gifts key it can't open; a key that arrives in a later
slot can't get it past 41.

The app can still read its data and commit the local writes it is allowed to.
The sync status is `Blocked` with the slot and its reason: `KeyUnavailable`,
`UpdateRequired`, or a `Refused` cause for damaged bytes. A slot whose bytes
haven't all arrived is a failed read, retried; only a complete object that
fails a check is refused.

Two things look like blocks but aren't:

- **Circle parts this member can't read.** A writes slot can carry parts for
  several audiences. Parts for circles this member isn't in are passed over
  without being opened ([Circles](/docs/circles#writes-into-circles)).
- **Writes refused by a removal.** Writes from a device or member that land
  after their removal, or after their member's keys were replaced, have no
  effect. Every device consumes the slot, records `WritesRefused` naming the
  entry that decided it, and moves on ([Sharing](/docs/sharing#the-cut-off)).

## Waiting and retrying

A device sleeps until something can have changed: a write from the app, a
changed prerequisite, or a retry that is due. While idle, it runs a pass every
30 seconds; a local write or `handle.sync_now()` asks for one at once. Requests
that arrive during a pass, or during connection backoff, coalesce into the next
one.

Automatic retries share one schedule of delays: 1, 2, 4, …, 256 seconds, then
300. The deadline is saved with the work, so reopening the app neither restarts
the wait nor skips it, and a clock changed in between can shorten a wait but
not stretch it past its full length. A provider's `Retry-After` can lengthen a
wait, and it applies to explicit retries from the app too. A wait for an app
action or an app update makes no requests at all.

## Status and pending work

`handle.start_sync()` starts the loop; its first pass builds the provider client
from the credentials in custody. Setting up storage starts it too.
`handle.stop_sync()` returns at once and stops after the active pass and file
transfers, keeping keys, credentials and the storage location.
`handle.disconnect_storage()` also removes this device's storage credentials.
None of them touch what's in storage.

`handle.subscribe_sync_status()` returns a `watch::Receiver` whose first value
is the current status:

```rust
pub enum SyncStatus {
    Disconnected,                                  // no storage set up
    Stopped,                                       // set up, not syncing
    Offline { error: Arc<StorageError> },          // the last attempt couldn't reach storage
    Syncing,
    Synced { finished_at: SystemTime },            // the pass reached the end of the sequence
    Blocked { slot: Slot, reason: PendingReason }, // a slot that can't apply
    Failed { error: SyncFailure },                 // storage refused the whole pass, or coven failed
}
```

`Synced` means the pass reached the end of the sequence; files or provider work
may still be pending. `Offline` describes the last attempt, not the
connection's history: Ana syncs, then loses Wi-Fi, and her next attempt is
`Offline`. A provider error that denies a required read is `Failed`. One bad
photo while the rest syncs leaves `Synced`, with a pending record for the
photo.

Anything that can't proceed has one record in `handle.pending()` (live: a
query over the `coven_pending` view, with each record's error from
`subscribe_pending_causes()`), offline too. Each
[`PendingRecord`](rustdoc:struct:coven::PendingRecord) names its subject, such
as a write, a slot, a file, an operation or the connection, and its first unmet
condition as a typed [`PendingReason`](rustdoc:enum:coven::PendingReason):
"needs an app update", "key not available yet", "waiting for Ana's device to
unshare", "source file moved". The record stays until that condition changes
or the work settles; time passing never clears it. `record.retry()` says how it
will retry: `Automatic` on the schedule above, `AfterUpdate`, `AppAction` (for
example new credentials), or `Never`. While the process that met the failure
runs, `record.cause` holds the error itself; a record read through the
`coven_pending` view finds it with `subscribe_pending_causes()`
([Coven's state in SQL](/docs/local-data#coven-s-state-in-sql)).

If coven can't save a pending record, it stops syncing and fails database calls
with `DbError::PendingPersistence` until the store is reopened, so the app never
sees an empty list that hides a lost update.

## Another copy of the same device

Restoring a phone from a backup can leave two installations with one device id.
Both would sign slots as the same device and number their claims alike, and
each could mistake the other's slot for its own.

The first defence is custody: a device's id also lives in a device-only
keychain entry, which a backup doesn't carry to another device (on Apple, it
comes back only when a backup is restored onto the same device), and opening
with a missing or different id there is refused. A copy that does open stops sending with `SyncError::DeviceIdInUse` as
soon as history shows another installation using its id: a slot signed by its
id at a path its journal never attempted, or different bytes signed by its id
where its own claim should be.

Coven never resets a device on its own. The app can call
`CovenBuilder::reset_device(store, device_name)`, which loads the store from
storage under a fresh device id, registers it as replacing the old one, and
moves the old directory aside for inspection. A damaged database is reset the
same way: any call whose statement meets the damage fails with
`DbError::DamagedDatabase`, opening included. Unsent writes from the old
installation are not imported; `handle.device_reset()` returns the stored notice
with the old and new ids, so the app can tell the person that unsent edits may
have been lost.
