# One sync pass

A pass reads slots, applies them, claims waiting work, then performs effects.
[§6](coven.md#6-syncing-writes) defines slots, claims and journals.
[§15](coven.md#15-snapshots) defines snapshots and the floor.

<a id="requests-in-order"></a>

## 1. Read forward

- Read `<store>/seq/<applied+1>`, then consecutive slots until a miss.
  A failed request is an error, never a miss.
- When far behind, check n+1, n+2, n+4, and so on, then narrow the interval.
  These existence checks find the end; they apply nothing.
- On a miss, read `<store>/floor`. If the missing slot is below its first
  kept slot, reload from the floor's snapshots (§15).
- Without a floor, the sequence starts at 1. A miss immediately after the
  known end means there is no new slot yet.
- A missing slot before a known later slot is damage unless the floor has
  passed it. Report its path and stop.
- Discovering a blocked slot stops further reads. Already-started requests may
  finish, but their results cannot advance the device past the block.
- A new device starts from the floor, or slot 1 before a floor exists.
  Opening its first key uses [§12](coven.md#12-joining-and-restore)'s bootstrap rules.

<a id="4-observe-keys-and-peers"></a>

## 2. Apply in order

- Read the floor after the pass's slot reads and before committing them.
  The read at the ending miss supplies this check.
- If the floor has passed an unapplied slot just read, reload under §15.
  Bytes recreated at a deleted path cannot become new history.
- Check each slot's framing and complete signature before applying it.
  Resolve its author from the preceding state; inner writes use their original authored history.
- Apply each slot in one transaction with its applied slot number.
  A failed transaction leaves both state and progress unchanged.
- **Admin entry:** validate and apply the change under [§9](coven.md#9-members-and-roles).
  Required reloads must finish before this slot commits.
- **Writes:** process the batch in write-number order under [§8](coven.md#8-merge).
  Removal and old-key cut-offs consume writes without effects and record `WritesRefused` (§10).
- **Key copy:** record its signed routing and exposure, and open a required
  copy for this device's member (§11). A copy for someone else need not be opened.
- **Snapshot announcement:** check its named snapshot's signed prefix, purpose and
  coverage (§15). Applying the announcement does not load its rows.
- An unavailable required key reports `KeyUnavailable`; a newer schema or format
  reports `UpdateRequired`. A damaged complete object reports its `Refused` cause.
- Any slot that cannot apply stops the device there. It sends nothing,
  including file parts, operation retries and provider changes.
- The local app still reads its data and commits eligible writes.
  Pending records name the cause; [§19.1](coven.md#191-noticing) specifies retries.

Ben's laptop reads slot 41, a copy of a key it cannot open.
It applies nothing after 41 and sends nothing until it can open that copy.
A key in a later slot cannot bypass 41.

## 3. Claim

- Settle any unsettled claim first, using [§6](coven.md#settling-a-claim).
  On reopening or retrying, read its recorded path and compare its exact bytes.
- If the floor has passed that slot, use the floor's snapshots: last-write
  numbers settle writes; the resulting state settles admin changes (§6).
- A new claim at n+1 requires the device to have applied 1 through n.
  No clock deadline expires that progress.
- For an admin operation, validate, journal its prerequisites, then do the work
  needed before its entry. [§18](coven.md#18-operations) governs completion and undo.
- Before each create, journal the slot path and exact bytes.
  A large writes slot uploads in parts; it becomes visible only when complete.
- On `AlreadyExists`, compare the occupied slot with the attempted bytes.
  Equal bytes can be a lost success reply; different bytes mean contention.
- After contention, read forward and validate again. Claim the next slot if
  still allowed; otherwise undo the journaled work and fail with the validation reason.
- After a create lands, read the floor before confirmation.
  A create below the floor is void and settles under §6's deleted-slot rules.
- Apply a confirmed claim as any received slot.
  Different bytes at an attempted slot do not prove a copied device id (§10).
- Claim at most one writes slot for this pass's queued, sendable writes.
  Writes committed after capture wait for the next pass.
- To announce a snapshot, first upload its journaled bytes, then claim the
  announcement. A stale capture is replaced under §15; abandoned uploads remain owed cleanup.

Ana and Ben try 41. Ana reads her bytes there and checks the floor.
Ben reads different bytes, applies 41, and tries 42 if his action is still allowed.

<a id="9-post-changed-positions-and-finish"></a>

## 4. Effects after applying

- Provider sharing follows active members and open invites (§4, §13).
  Failed grants or revocations stay pending with their cause.
- S3 keys no longer current become deletion work (§13).
  A key's record remains until its deletion is confirmed.
- Exposed audience keys rotate through the operation path (§11).
  Only a member of that audience can supply its replacement key.
- A changed reset, breaking schema raise or newly readable circle requires a
  reload (§15). Install its data with the triggering slot, or leave that slot pending.
- Retention conditionally writes the new floor, then deletes slots below it (§15).
  Conflicts reread; failures retain the journal. The floor never moves backward.
- File deletion requires [§16.5](coven.md#165-uploads-and-deletion)'s reference checks.
  Confirmed provider cut-off permits the removed device's complete file-folder scan.
- `Blocked` names a slot that stopped the pass. `Synced` means the pass reached
  its read end; files or provider work may still be pending ([E5](api.md#e5-storage-and-sync)).

<a id="counting-a-pass"></a>

## 5. Counting requests

- An idle pass makes **2 requests**: the next-slot read, which misses, and the
  floor read. It makes no listing or write request.
- Each arriving slot adds its read. The floor read after the final miss also
  checks those slots before commit.
- A claim adds its create and a floor read after it lands.
  Contention adds the occupied-slot read and each further attempt.
- Admin prerequisites add their actual requests: key copies, access changes,
  deliveries or snapshot uploads. There is no fixed bound on contention.
- A snapshot adds its upload and announcement claim.
  A reload reads the floor's named snapshots and their required following slots.
- Retention adds a conditional floor write and deletes.
  Files add their own reads, upload parts or deletes outside the pass.
- Provider metadata, redirects, native asset parts, pages and retries are extra
  requests. Count all of them under [§3.1](coven.md#31-io-bounds), including failed requests.

Ben's idle pass makes two reads. If Ana added one note, its slot adds one read.

<a id="one-stream-checked-as-it-arrives"></a>

### Object reads

- A full object is read as one bounded stream, with authentication before use.
  No slot or snapshot commits before its complete signature verifies.
- Reading only a snapshot prefix takes at most two bounded range requests.
  Reading the remaining body takes at most one further stream.
- An inner write arrives inside its slot, with no separate object request.
  Every native asset request counts if the provider splits a stream.
- A failed transport is a failed read. It cannot establish a permanent refusal
  of bytes that have not all arrived.

## 6. Files and waiting

<a id="file-uploads-outside-the-pass"></a>

### File uploads outside the pass

- A pass never waits for file uploads. Their completion changes no app row
  and does not delay the write that attached them (§16.5).
- Files use their fixed keys and references. Source checks in §16.5 prevent
  a retry from encrypting changed bytes under the same file key and nonce.
- A blocked sequence or removed identity stops file sends too.
  Confirmed parts remain recorded for resumption; no elapsed-time send permission is needed.
- Up to the provider's single-request limit or 32 MiB, whichever is less,
  use one create. Larger files use a resumable upload.
- Resuming preserves provider-confirmed parts. An expired session starts again;
  progress counts confirmed bytes, never bytes merely sent.
- Stopping sync waits for active transfers. Count every request outside the
  pass as well as inside it.

<a id="waiting-without-repeated-work"></a>

### Waiting without repeated work

- Wake for app work, a changed prerequisite or a due retry.
  There is no unconditional one-second scan.
- Idle passes run every 30 seconds, subject to provider cooldown and connection
  backoff. App work arriving during a pass keeps a wake for the next pass.
- Queue and operation bookkeeping do not request another pass
  ([`write_never_lost`, `racing_passes`, `bookkeeping_retains_none`](proofs/io/CovenIO/Wakes.lean)).
- Automatic waits use 1, 2, 4, …, 256, then 300 seconds.
  Unrelated commits and pass completion do not reset the delay.
- Provider `Retry-After` can lengthen the wait and applies to every affected
  request, including SDK retries and explicit app retries.
- Persist deadlines as [§16.5](coven.md#165-uploads-and-deletion) specifies.
  An app-action or after-update wait issues no automatic request.
- Join polling follows [§12.2](coven.md#122-adding-a-person).
  After request deletion, read forward and check the floor before deciding approval was absent.

Ana's invite has no request yet, so polling backs off.
Once Carol's request arrives, waiting for Ana's decision sends no repeated request.
