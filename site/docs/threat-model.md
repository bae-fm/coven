# Threat model

coven has no server. A store's data lives in the household's own cloud storage,
and every member's devices hold the store key. This page says, for each party,
what coven defends against and what it doesn't, so the line between coven's
job and the household's is written down.

Two mechanisms carry most of the defence:

- **Encryption.** Everything coven stores is encrypted with an audience key
  that only that audience's members hold: the store key, or a circle's key
  ([Encryption](/docs/encryption)). Authentication fails on any changed byte.
- **Signed slots in one sequence.** Every slot is signed by its author's member
  key, over its path, and every device judges each slot against the membership
  as it stood just before that slot ([Sync](/docs/sync-model)). A device that
  meets a slot it can't check stops there rather than skipping it.

## The storage provider

coven assumes the provider is *honest but curious*: it may read what it stores,
but it doesn't alter or withhold it.

**What it sees.** Object sizes, counts and timing; which account writes each
object; each slot's kind; a key copy's routing, which names a circle id and a
recipient member; and the device and audience named in file and snapshot paths.
It doesn't see names, values, file contents, or which device claimed a slot
([what the provider sees](/docs/encryption#what-the-provider-sees)).

**What it can't do.** Read contents, or forge a slot, a snapshot or an admin
entry, without a member's keys.

**What no design prevents.** Whoever controls storage can delete it or hold it
back. Coven doesn't recover lost or damaged storage; the household starts a new
store by hand. A [reset](/docs/bootstrap#resetting-an-audience) replaces an
audience from one device's copy; it can't rebuild storage.

coven also relies on the provider behaving as [Storage](/docs/storage#what-coven-needs-from-a-provider)
requires: complete reads, create-once that publishes atomically, conditional
replacement of the floor, and access cut-off that really cuts off. Setup checks
these against the real service every time, and certification races many
writers against each S3 service, but neither can prove them for all time.

## Someone with storage credentials but no key

A leaked storage credential, a misconfigured bucket. Unlike the provider, this
party may be hostile.

**Defended: confidentiality and integrity.** They hold no audience key, so they
read nothing but sizes, counts and timing. They can't produce a slot that
passes a signature check, and moving an authentic object to another path fails
its check there, because the path is signed.

**Not defended: availability.** They can delete or replace objects. A device
that downloads a complete object that fails a check refuses it and stops at
that slot, reporting `Refused`; it never skips ahead. Keeping storage
credentials to the members who need them is the household's job.

## A current member

Current members trust each other: each has access to the storage and isn't
trying to damage it. A member's devices hold the store key and the member's
signing key, so a member can do whatever their role allows
([Sharing](/docs/sharing#members-roles-and-devices)). Authority is checked on
every device: an admin entry counts only if its author may make it in the state
before its slot, and the store always keeps an admin.

Within that trust, two things are still checked:

- **Who wrote what.** Every slot is signed, and a table declared
  [`owner_only`](/docs/merge#owner-tables) keeps each row's changes to its
  creator, so "Ana rated this" really was Ana.
- **Circles.** A member outside a circle doesn't hold its key and can't read
  its rows or files. They do see that it exists, its name, who is in it, who
  writes to it, when and how much ([Circles](/docs/circles#what-other-members-can-see)).

## A removed member

Removing Ben stops what comes next; it can't take back what he has.

- **His writes stop counting.** Writes from his devices that land after his
  removal, or signed with keys that have since been replaced, have no effect on
  any device. There is no grace period ([the cut-off](/docs/sharing#the-cut-off)).
- **New data uses keys he never had.** Every key copied to him counts as
  exposed and is replaced before new data uses it
  ([Encryption](/docs/encryption#which-key-new-data-uses)).
- **His storage access is taken back** by provider work: the owner's device
  unshares on Dropbox, OneDrive and iCloud, and on S3 his access key is deleted
  ([provider access](/docs/sharing#provider-access)). Until that happens he can
  still download ciphertext, which he can't open if it's under new keys, and he
  can still delete objects.
- **He keeps what he had.** Rows, files and keys already on his devices stay
  there. If he has seen something secret, treat it as seen.

A key copy already on its way when Ben was removed can still reach him: a copy
made before its author saw the removal can land after it. Other devices then
treat that key as exposed too.

## A stolen restore code

A restore code holds a member's private keys. Anyone who sees it can open the
store, given storage access, for as long as those keys are current; coven can't
tell them apart from the member. Replacing the member's keys stops writes made
with the old ones from counting, but can't take back keys and data the code
already reached. Coven offers no recovery from a stolen code; the household
removes that member and starts over by hand if needed. Apps should blur the
code until the person asks to show it.

## A copied device

Restoring a phone from a backup can leave two installations with one device id.
The device id also lives in the keychain, which backups don't copy, so a copy
usually can't open. A copy that does open stops sending as soon as history shows
another installation using its id ([Sync](/docs/sync-model#another-copy-of-the-same-device)).
Coven detects that evidence, not every clone. Even where a copy forgot an
earlier attempt, content-derived nonces keep it from sealing changed bytes with
a reused nonce.

## An attacker on the device

Someone who can write the store's directory, its database or its passphrase
file is outside coven's model. Coven trusts its own local database; the
device's protections, such as full-disk encryption, OS process isolation and
the keychain that holds the keys ([Keys](/docs/keys)), are the boundary. The
passphrase file is the one thing coven reads defensively: it refuses key
derivation settings beyond fixed bounds rather than spending whatever the file
asks for.

## What coven assumes

- The ciphers, signatures, hashes, key derivation, random numbers and key
  custody work as designed.
- SQLite transactions are atomic and durable, and file and custody writes are
  durable.
- The provider behaves as described above.

coven's design is checked by machine-checked models of the sequence, the merge
and parts of storage. They prove properties of the models under these
assumptions; they don't prove the cryptography or the Rust code.
