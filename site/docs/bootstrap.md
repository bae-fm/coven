# Snapshots and joining

The sequence would grow forever. A household that has used the app for five
years would have hundreds of thousands of slots, and a new phone would have to
read every one. Instead, devices take *snapshots*: the complete state of one
audience as of a slot, its *coverage*. Once every audience has a snapshot, the
slots before it can be deleted, and a device that is new, or has been away,
loads the snapshot instead of the deleted slots.

## What a snapshot holds

A snapshot records one audience, the store or one circle, after applying
through a slot. It holds the audience's rows, coven's merge records and lost
values, and the evidence old writes still need: each deleted row's generations,
and whether each change's data counted. Without that evidence, an old edit
arriving late could bring a deleted row back.

A store snapshot also holds the signed admin entries, key copies and
announcements needed to know who was a member when, which keys exist, and which
schema raises and resets happened. Every snapshot records, for each device, the
number of the last claim it includes and a hash of that slot's bytes, so a
device can settle a claim after its slot is deleted
([Sync](/docs/sync-model#the-floor)).

A snapshot leaves out everything that belongs to one device: unsent writes,
journals, pending records, file sources and the cache. A device snapshots only
audiences it can read, and only slots it has applied.

## Taking a snapshot

A snapshot becomes usable in three steps, each recorded before it happens:

1. the device journals the snapshot;
2. it uploads the snapshot's bytes to `<store>/snapshots/<audience>/<device>/<n>`;
3. it claims a slot *announcing* it, naming its path, its coverage, its purpose
   and a hash of its bytes.

So an announcement always names a snapshot already in storage. A device asks
for a new snapshot of an audience once the writes to it since the last one add
up to more than that snapshot's size, or 1 MiB if there is none yet. An
announcement's coverage never goes backward for its audience.

Applying an announcement reads no snapshot. A snapshot is read only when a
device loads it, and then it is checked against the announced hash and its
signature. A device always loads the newest snapshot that applies; if that one
needs a key the device lacks, an app update, or has damaged bytes, the device
waits and says why. It never loads an older snapshot to get around the problem.

## The floor

The floor, at `<store>/floor`, names the first kept slot and, for each audience,
the snapshot it relies on. Every snapshot it names covers at least the slot
before the first kept one, and it names every audience whose deleted history
still matters: the store and every circle, even one the device writing the floor
can't open. Snapshots may cover different slots: the store's might cover 560 and
Gifts' 520, with the first kept slot 501.

The floor also carries the original key copies needed to open the store key,
with their routing readable, so a new device can find its copy before it holds
any key. Before any floor exists, the sequence starts at slot 1.

## A new device

A device that restores or joins ([Sharing](/docs/sharing)) starts from the
floor. It opens its key copy, checks the entry that bound that key to its member
and the floor's signature, loads the floor's snapshots, applies the kept slots
after them, and only then registers itself and hands the app the open store.
Without a floor, it reads from slot 1.

Restoring, joining and resetting report their progress through the status
callback the app passes in, as
[`BootstrapStatus`](rustdoc:enum:coven::BootstrapStatus): `Opening(progress)`
while the new installation's database runs its migrations, then `Connecting`,
`WaitingForApproval`, or `Pending(records)` naming what it waits for, then
`Loading(progress)` with each step of loading the store: reading the slots for
a copy of the member's keys when the floor holds none, counted in slots;
streaming each of the floor's snapshots, counted in bytes; installing their
rows, counted in tables; the slots installed with them; then reading and applying the slots after them,
counted in slots. The device finds the end of those slots before reading them,
so reading has a total, which grows if slots land meanwhile. A join that
stopped part way through loading reports the slots it has left when it
resumes. A `restore_device` that finds the code's keys already installed just opens
the store, and reports the open's steps as `Opening(progress)` too. Until it
finishes, the installation is hidden from
`layout.stores()` and ordinary opens refuse it. Cancelling removes it. A file
whose row arrives downloads when the app reads or pins it
([Cache](/docs/cache)).

## Coming back after a long absence

Ben's laptop was off for a year. The floor now starts at slot 501; Ben had
applied through 40. Reading slot 41, he finds it missing, reads the floor, and
*reloads*: he loads the floor's snapshots and applies the slots from 501 on.
His unsent edits keep their original timestamps and `read_through`, and are
judged exactly as if he had sent them a year ago. Being late doesn't refuse
them.

The floor's snapshots can cover different slots, so the device picks one target:
the highest coverage among them. With the store's snapshot at 560 and Gifts' at
520, Ben's laptop reads slots 521 to 560 for Gifts, then commits every audience
together and reads on from 561. A reload commits all at once: rows, merge
records, lost values and progress in one transaction. A failed reload leaves
the old state visible.

**Writing during a reload.** During an ordinary reload, like Ben's, the app
keeps writing. When the loaded state commits, coven applies the device's queued
writes over it, with their original numbers, timestamps and `read_through`,
including writes made while loading.

Three kinds of reload replace an audience with state the device's own writes
didn't see: a [reset](#resetting-an-audience), a breaking
[schema raise](/docs/schema-evolution#breaking-changes-and-the-raise), and a
circle the device can newly read ([Circles](/docs/circles#joining-a-circle)).
While one of these runs, writes into that audience fail with
`DbError::AudienceReloading`, because they'd be made against state about to be
replaced. Other audiences stay writable.

The app can ask a working device to reload, for example when its data looks
wrong, with `handle.reload_from_snapshot()`. It keeps its device id and its
unsent writes.

## Deleting history

Once every audience has a snapshot covering slots up to some point, and a grace
period has passed, any device with permission to delete in storage:

1. journals the new floor and the deletions it plans;
2. writes the new floor with a request that succeeds only if the floor hasn't
   changed since it read it;
3. deletes the slots below the new first kept slot.

The grace period is usually seven days from the latest announcement of the
snapshots chosen. It spares short absences a full reload; it isn't needed for
correctness. Deletion never waits for an absent device, and no device has to
acknowledge anything.

If two devices move the floor at once, one conditional write fails; that device
reads the current floor and decides again. The floor never moves backward. A
lost reply is settled by reading the floor before deleting anything, and failed
deletions stay journaled and show in `pending()`.

An audience nobody can snapshot holds its history back. If no member of a
circle comes online to snapshot it, that circle's uncovered history stays;
history it already covers is deleted without waiting.

## Resetting an audience

If devices disagree about the data, the person can choose one device's version
and reset an audience to it: an admin for the store with `handle.reset_store()`,
a circle's member for that circle with `handle.circles().reset(circle)`.

The device writes a snapshot of what it has, announces it, and claims a reset
entry naming it. Every device reloads from that snapshot when it applies the
entry, the resetting device included. From then on, only two things count for
that audience: the snapshot, and later writes whose `read_through` includes the
reset slot.

Ana resets the store from her phone's snapshot, which covers Ben's writes
through 8. Ben's laptop made writes 9 and 10 without reading the reset. Ben
still uploads both, but neither changes the store or creates a lost value: that
is what a reset is for. After reading the reset, Ben makes write 11, and it
applies on every device.

A reset replaces an audience from a device's copy. It doesn't recover lost or
damaged storage; if storage itself is gone, the household starts a new store.
