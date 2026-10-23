# Constraints

coven has no server. Removing it is what makes the rest possible: no backend to
run, storage the user already owns, end-to-end encryption no operator can undo.
But it also removes the one place a traditional system puts its enforcement. A
server sits in the middle of every request: it can refuse a token the instant
you revoke it, hand each user only the rows they may see, keep a log nobody can
forge, throttle an abuser, and delete a record on command. coven has none of
that, because there is nobody in the middle. Trust lives in keys, and a key,
once given, can't be taken back.

This page is the ledger of what that costs. The [comparison](/docs/comparison)
page places coven against other systems on what it *does*; this one is what it
gives up to do it.

## Small groups, modest write rates

coven is built for one person's devices or a household of up to about twenty
people, four or five typically. Every device applies every slot of the store's
one sequence, and every publish is a create-once request that only one device
can win at a time. A busy group of hundreds, or a stream of writes many times a
second, is not what it is for.

## Removal withholds new secrets; it can't un-send old ones

When Ana removes Ben, his writes stop counting from the removal's slot, and
every key he was sent is replaced before new data uses it
([Encryption](/docs/encryption#which-key-new-data-uses)). Everything written
after that is unreadable to him.

What removal can't do is reach into his devices. Rows, files and keys he already
has stay there. There is no server to send a "forget this" command. Removal is a
promise about the future, not the past. If a member has seen something secret,
treat it as seen: change the secret itself, such as a password kept in a row,
not just the membership.

## No server-side access control

A server enforces policy at the moment of each request. coven has no such
moment, so several controls a server-backed system offers don't exist:

- **No instant cut-off.** A removal applies on every device as soon as each
  reads it, but taking back storage access is provider work. On Dropbox,
  OneDrive and iCloud only the account that holds the store can unshare, so if
  its owner is offline, the unsharing waits for them. On S3, the person
  deletes the member's access key in the provider's console
  ([Sharing](/docs/sharing#provider-access)).
- **Rows, not columns.** [Circles](/docs/circles) keep rows from members
  outside them, but every member still sees that a circle exists, its name, who
  is in it, and how much they write. Nothing hides one column of a row from a
  member who can read the row. When two groups of people must learn nothing
  about each other, that is two stores.
- **No log of reads.** Every slot is signed, so who wrote what is recorded. A
  read is a download of ciphertext from the provider; coven keeps no record of
  who fetched what.
- **No rate limiting.** Nothing in coven throttles how fast a member reads or
  writes. The only limits are the provider's own, which coven honours.
- **No forced deletion.** A delete propagates to every device, but nothing can
  make another member's device forget what it already had.

## Every device holds what it can read

coven is fully replicated: every device carries every row of every audience its
member can read, not a slice a server picked. That makes every device complete
and usable offline, and it means sharing a store, or a circle, is sharing all of
it.

## Consistency is convergent, not global

Devices aren't consistent at every instant. Each device commits offline and
merges other devices' writes when it reads them. Every device that has applied
the same slots holds the same rows, but there is no transaction spanning
devices. The consequences:

- **The larger timestamp wins a cell.** Concurrent edits to different cells of
  a row both survive; concurrent edits to the same cell resolve to the larger
  timestamp. The losing value isn't thrown away: it is kept as a lost value the
  app can show and offer back ([Merge](/docs/merge#lost-values)).
- **Deletes win, after sync.** An edit made to a row that has since been deleted
  loses, whenever it arrives. But a delete takes effect on a device only once
  that device reads it.
- **Constraints are decided after the fact.** SQLite enforces a `UNIQUE` or
  `CHECK` constraint, or a foreign key, on one device. Two offline devices can
  each make a row that is valid alone and breaks the constraint together. coven
  decides those the same way everywhere: the first slot keeps a unique value,
  and a row that fails a CHECK or lost its parent is taken out and kept as a
  lost value ([Merge](/docs/merge#the-rules-sql-brings-with-it)). So a row Ana
  created offline can be taken out later, after Ben's earlier claim arrives.
  Design the app to show that, not to assume it can't happen.
- **Admin changes need the network.** Writes never wait, but adding a member,
  changing a role or making a circle first catches up with the sequence online.

## Member identity lives on the device and in the restore code

A member's private keys live in [custody](/docs/keys) and in their
[restore code](/docs/sharing#a-person-s-new-device-the-restore-code). Anyone
holding the code is that member to coven. Coven offers no recovery from a stolen
code.

The flip side is that there is no reset. If another admin remains, they can
invite you again to your existing member id and replace your keys. But if every
member loses every device and restore code, the encrypted data is unrecoverable:
there is no operator to check who you are and let you back in. The security that
keeps the provider from reading the data is the same property that means nobody
can recover it for you.

## The storage provider is the shared dependency

Removing the server doesn't remove every dependency; it concentrates one. There
is no coven service to go down, but availability now rides on the
[storage provider](/docs/storage). If the provider is unreachable, the account
is suspended, or its credentials are revoked, no device can sync until that is
fixed. Whoever controls the storage can delete it, and coven can't rebuild a
store from nothing.

## What the absence of a server buys

Kept in balance with the limits above, removing the server is also what gives
coven its properties:

- **Nothing to run or operate.** No backend to deploy, scale, patch or pay for;
  a store needs only storage the household already has.
- **The provider never sees contents.** Every object is
  [encrypted before it leaves the device](/docs/encryption); the provider holds
  ciphertext, sizes and timing.
- **Works offline.** Every device holds its whole database and commits writes
  locally. A store works before any storage is set up
  ([Local data](/docs/local-data)), and syncs once it is.
- **Full ownership.** The data lives in the household's storage under their own
  accounts, with no operator holding a copy or a key.

These aren't separate features added to a serverless design; they are the same
decision as every constraint above, read from the other side.
