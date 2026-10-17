# One sync pass

This is the request order for [§6](coven.md#6-syncing-writes) and
[E5](api.md#e5-storage-and-sync). [§3.1](coven.md#31-io-bounds) gives its bounds.
There is one numbered sequence. Discovery never lists devices or log history.
Each request below needs its stated condition.

## What survives a pass

Reads and exact status supply paths, sizes, revisions, publication times and
available provider checksums. Keep their checked observations in one catalog
shared by sync, operations, reloads, agreement and retention. Distinguish a
whole-object digest from a multipart composite. A failed read is not absence.

Reuse immutable facts by `(path, size, stored_at)`. A changed immutable object
reports `StorageFailure::Protocol`; retain the earlier evidence. For floor
and status objects, compare provider revisions too: equal size and time do
not prove equal bytes. Read a new revision once and share its result.

These records survive reopening:

- **Slots:** received bytes, complete-signature verdicts, decoded records,
  publication identities and the first unmet prerequisite. Applied progress
  is one slot number, advanced atomically with all that slot's effects.
  Downloaded progress does not imply applied progress.
- **Write headers:** original identity, `read_through`, timestamp, schema,
  audience lengths, source evidence and containing slot. Conversion keeps
  the authored bytes separate from the received interpretation
  ([`conversions_preserve_uploads`](proofs/queue/CovenQueue/Conversion.lean)).
- **Snapshots:** announcements, checked prefixes, coverage, purpose, size,
  body verdicts and the floor's current bundle. An authenticated prefix
  permits a coverage check, never loading unchecked data. Growth uses indexed
  lengths, not body scans or snapshot listings.
- **Peer status:** the last revision, applied slot, fingerprints and reports,
  or its invalid-status verdict. An unchanged invalid revision is not
  downloaded again. Unknown keys or authors wait on retained bytes.
- **Own status:** the complete canonical plaintext last confirmed published
  and any uncertain replacement's exact bytes. Random sealing bytes never
  justify a new post of unchanged contents.
- **File references:** fixed paths named by rows, losses, queued writes,
  snapshots and retained source evidence, plus whether each input's relevant
  contents were checked. Remove remote protection only after confirmed
  deletion and when no retained input still needs it.
- **Claims and journals:** exact attempted destination and bytes, reserved
  publication identity, confirmed artifacts and every outstanding undo or
  deletion. A lost reply cannot lose the obligation to settle that path.
- **Keys:** original signed copy slots, routing, introductions and exposure
  evidence. Presence and successful opening are distinct facts; secrets stay
  in custody. Checkpoint evidence replaces deleted copy slots.
- **Removed-device file scans:** after confirmed provider cut-off, the complete
  folder observation and candidates. A failed or partial scan records no
  completion; it retries. A successful scan is not repeated on idle passes.

Keep unfinished ranges and bodies under the retained-input disk budget.
Eviction is explicit work and any later read is charged to it. Durable
refusals and unknown file-reference sets survive byte eviction. Never evict
bytes needed for an uncommitted apply while advancing its cursor.

One serialized owner holds the decoded administrative state matching the
committed database revision. It extends that state when slots commit. Prepare
outside the writer; validate the revision and install the slot, its effects
and journal progress in one transaction. Failure leaves both decoded and
durable state at the preceding revision. Workers have no separate writable
copy of this state.

The owner also retains the monotonic start of the last completed catch-up
for §10's send gate. It expires after five minutes, counting sleep, and is
invalidated by reopen, reconnect, reset or any blocked slot. It is session
state, not a retention clock or an acknowledgement from another device.

**Ben's missing key.** His laptop retains slot 41 and the earlier copy meant
for him, but custody cannot open that copy. Slot 41 blocks later slots and all
sending. Repairing custody lets him open the retained bytes and resume. A
copy in a later slot cannot be read around the block.

## Requests, in order

### 1. Read forward

Serialize the pass with other claimers. Capture the local writes and commands
it will service. Borrow keys unlocked at handle opening; opening or saving a
key does not re-derive a passphrase key. Persist newly acquired keys before
durable work depends on them. Create no journal merely to record an idle check.

Read `<store>/seq/<applied+1>`. Continue consecutive exact reads until a miss.
A far-behind device may bracket the end with existence checks at n+1, n+2,
n+4, … and narrow the interval; these checks establish no applied progress.
Fetch-ahead stays within transfer limits. Once a block is discovered, schedule
no later read and retain already returned bytes.

On a miss, read or check the revision of `<store>/floor`. A checkpoint covering
the required slot starts §15's reload; it works even when deletion is underway
and `first` has not advanced. A miss below a known later slot without such
coverage is damage. A miss immediately after the known end otherwise ends
this observation. A provider error supplies neither kind of miss.

A new device begins at the floor when present, otherwise slot 1. Before it
holds the store key, §11's bootstrap may inspect clear copy routing and open
its own boxes. This scan confers no applied cursor, membership or send gate.
Check its binding history and snapshot signatures before committing bootstrap.
No ordinary pass scans beyond a blocked slot for a later key or admin entry.

<a id="4-observe-keys-and-peers"></a>

### 2. Apply in order

Before committing the batch, use a floor check made after its reads; reuse
the check at the ending miss when it satisfies that order.
If its advertised checkpoint covers these slots, load that replacement: a
late create at a deleted path cannot replace the original claimed history.

Authenticate each slot's complete framing and signature. Resolve its author
from the preceding administrative history; inner writes retain their own
original authored views. Apply the slot and cursor in one transaction:

- **Admin:** validate against the immediately preceding prefix and apply the
  final entry. A changed schema, reset or newly readable circle stages its
  required reload before the slot can commit.
- **Writes:** validate each original write and apply the batch in write-number
  order. Lower-slot device/member removal or credential replacement consumes
  the affected writes without effects or losses, recording `WritesRefused`.
  Other admission, data-source and conversion checks remain (§8, §10, §17).
- **Key copy:** retain authenticated routing and exposure; open a required
  recipient copy using its named credential. Nonrecipient copies are consumed
  without opening them. An introduced key's hash must match.
- **Announcement:** check the named snapshot, purpose and monotone coverage
  under §15. Retain its metadata; ordinary application need not load its body.

A readable part with no usable key, an unsupported version, damaged object,
failed transaction or unfinished reload leaves the cursor before that slot.
Record the named cause locally and stop all sending, including status, file
parts and operation retries. Local reads and eligible app writes continue.
There is no independent admin lane past the block.

A reload retains its selected inputs and fixed target. Stage each readable
audience from its selected snapshot through a common required slot, then
commit all rows, merge records, losses, source verdicts and cursor together.
Reapply the eligible local queue with its original pasts. If deletion overtakes
a required suffix read, obtain the newer floor bundle and restage; failure
changes no visible state. A newer announcement alone does not restart it.

At a confirmed current end, refresh the send gate using this catch-up's start.
Verify custody, device identity and publication reservations against slots or
checkpoint receipts. A shared-slot collision is ordinary contention; only
conflicting authenticated use of this device's publication identity is clone
evidence (§10). Removed or replaced identities send nothing.

Read status revisions by exact path for devices active in this applied state.
Fetch only changed reports. Agreement compares equal applied slots, schema,
audience and key; queued local writes omit a fingerprint. Status never drives
sequence discovery or retention. A failed peer report affects that report,
not already authenticated sequence data.

### 3. Send by claiming

All publishers share this owner and gate, including callers running outside
the periodic pass. First settle any uncertain claim at its recorded path.
Use the exact retained bytes; after retention, authenticate its checkpoint
receipt before confirming it. Never publish elsewhere while its outcome is
unknown.

For every admin action, validate its target against the latest applied state,
then do and journal its prerequisites (§18). Generated keys, created S3 keys,
deliveries and uploaded snapshots are owned by that operation. Prerequisite
key copies and snapshot announcements claim their own earlier slots.

To claim after n, seal for `<store>/seq/<n+1>`, save the exact bytes and issue
create-once. Upload a large writes slot in parts; only complete publication
makes the slot visible. On `AlreadyExists`, read the complete object:

- equal attempted bytes confirm success, including a lost success reply;
- different bytes mean another claim won. Read and apply forward to the new
  end, validate again, then try its first empty slot;
- a read or comparison failure keeps this attempt pending.

After the response, check the floor before confirmation. Covered slots settle
only from its authenticated receipts; a different recorded occupant means
this attempt never claimed that logical slot. Journal prefix cleanup of any
recreated bytes at that covered path (§6), then revalidate.
A successful claim applies by step 2. A new candidate path gets a new outer
envelope; its inner write bytes stay fixed. Never reuse a publication identity
for different work. Never republish a confirmed publication.

If intervening slots make an admin action invalid, undo every artifact its
journal still owns and fail the call with the validation reason. Failed undo
retains the journal and retries. Previously claimed prerequisite slots remain
final history; they cannot be erased to undo a disclosure. An already-satisfied
target completes without claiming a redundant entry.

Claim at most one writes slot per pass, containing the captured sendable
prefix of this device's queue. A schema publication gate can end that prefix;
later app commits wait for the next pass. Remove queue records only when
consuming their confirmed slot. Local writes are not applied twice.

Snapshot publication uploads the captured object and then claims its
announcement. Revalidate coverage after contention; recapture a stale
candidate and delete its unannounced object through the journal. Raise/reset
snapshots become usable for that state only with their matching admin entry.
No upload-time deadline or concurrent-raise winner calculation applies.

<a id="9-post-changed-positions-and-finish"></a>

### 4. Perform effects and finish

Derive effects from the now-applied state: sharing follows active members and
open invites; noncurrent S3 keys await deletion; exposed audience keys rotate
through the same operation path. Required reloads commit with their triggering
slot. A blocked sequence can update local journals and report causes, but
issues no outbound effect request until it can continue.

For due retention, select a verified announced bundle whose audiences cover
the deletion cut and whose grace has elapsed. Conditionally advertise it in
the floor before deleting any slots. Delete the covered prefix, then
conditionally advance `first`. Journal every unfinished deletion and floor
replacement. A conflict rereads the floor and never lowers its coverage or
first slot. No reader acknowledgement or uploader's return is required (§15).

File retention runs only when references, upload completion, history deletion
or provider cut-off can change eligibility. Use exact paths from the reference
index and queue; never list files on an idle pass. After confirmed storage
cut-off, one complete `<store>/files/<device>/` scan discovers removed-device
orphans. Settle in-flight sessions first. An incomplete scan or unreadable
reference-bearing input protects the files it might name. Unknown references
are not empty. Retain the failure instead of fetching the same body each pass.

Replace `<store>/status/<this device>` at most once, only when canonical
contents change or its absence was observed. Include the applied slot and
this device's pending reports; fingerprints require exactly that published
state. A lost reply retains its attempted bytes or confirms the stored value
before settling. An unchanged refusal, heartbeat or time observation causes
no replacement. A blocked sequence cannot publish this status.

Publish completion in memory. `Synced` describes a finished pass, not the
absence of pending work. A blocked slot gives `Blocked`, with its cause also in pending
records; it is not `Synced`. Retain one wake for app work arriving after this pass's capture;
queue removal, cache publication and operation bookkeeping create no immediate
second pass. Otherwise wait for the idle schedule, a genuine external change,
a command or the earliest due retry
([`write_never_lost`, `racing_passes`, `bookkeeping_retains_none`](proofs/io/CovenIO/Wakes.lean)).

## File uploads outside the pass

The file worker wakes for a queued file, changed prerequisite or due retry;
a pass never waits for it. It shares the owner’s observations, transfer limits,
provider cooldown and current identity/send gate. Before every request,
including parts, completion and retries, check that gate and any sequence
block. Expiry or a block preserves the fixed reference and confirmed progress.

Use one create up to the provider's single-request limit or 32 MiB, whichever
is less. Encryption chunks alone do not start a session. Above that, begin
or resume the recorded session, check confirmed progress, send the remaining
parts and finish. Check the source as §16.5 requires without holding the
SQLite writer across disk reads or crypto. Completion changes no app row.
Stopping sync waits for active transfers. Charge every request and byte to
§3.1 even though the worker runs outside pass completion.

## One stream, checked as it arrives

A complete-object read is one streaming body request. Bound framing before
allocation, open readable chunks and incrementally hash all signed bytes,
including unreadable ciphertext. Check complete framing and the final
signature before applying any slot, write or snapshot. Share retained bytes
and checked records; later consumers do not unlock custody or fetch them again.

On an early authentication failure, retain the cause and drain that same
stream if complete download is required for a permanent refusal (§19.1).
A failed transport remains a failed read. An update uses retained bytes first.

A consumer needing only a signed snapshot prefix starts with a bounded range.
If incomplete, one second range extends it to the exact end, or the maximum
header bound when the first range cannot reveal that end, capped by object
size. Parse counts before use. Keep body bytes returned with the prefix.
A later body read streams from that retained prefix's end to EOF and hashes
both as one object. A direct full read needs no preliminary request.

An inner write header is obtained from its containing slot's checked stream;
it is not a second object to fetch. CloudKit streams bounded asset parts;
each native asset request is charged separately. A logical stream does not
promise one native request for an object held in several assets.

**Carol's import.** Slot 42 carries Ana's queued import. Carol streams the
slot once, checking the inner writes as they arrive. Its whole signature
must verify before the batch commits. Snapshot growth and agreement reuse
those facts instead of downloading the import again.

## Counting a pass

Let N be the active devices in the applied membership, including this one.
With warm observations and usable credentials, an unchanged idle pass needs:

```
D = 1 next-slot miss + 1 floor revision check + N status revision checks
R_idle = 2 + N
```

These are logical provider requests. Charge path lookups, redirects and native
asset parts when needed. S3 uses exact HEAD for revisions; no LIST occurs.
There are no idle body bytes, publications, snapshot queries or file queries.
Changed status or floor revisions add their body reads. Nonempty batches
check the floor after reading; claim responses also require that check.
Initial discovery and eviction have their own counts.

**Ben receives one note.** Household has three active devices. An unchanged
idle pass needs five requests. One readable slot adds its body read and one
changed-status replacement: seven requests; the floor check at the ending
miss also supplies the required post-read check when peer reports
and the floor have not also changed. Any already-issued fetch-ahead probe counts.

Contention adds failed creates and winner reads. New writes, snapshots, files,
permission calls, retries and due retention each add their actual requests and
bytes under §3.1. A snapshot reload reads only the named bundle and required
suffix; grace creates no clock-object publication.

## Waiting without repeated work

Workers wake for commands, changes to named prerequisites or a due retry.
There is no unconditional one-second database scan. A wake reevaluates only
affected work; unrelated commits never reset backoff
([`evidence_keeps_backoff`, `evidence_due_once`, `unrelated_commit_ignored`,
`unchanged_rate`](proofs/io/CovenIO/WaitingEvidence.lean)). A wait names the
committed rows its attempt reads; a change during that attempt makes the
wait due, but the attempt's own commits do not
([`unblocking_commit_named`, `named_row_matters`](proofs/io/CovenIO/PrerequisiteRows.lean),
[`racing_commit_wakes`](proofs/io/CovenIO/WaitingEvidence.lean)). Reopening
retains the persisted wait and watches subsequent changes
([`reopened_due`, `reopen_rate`](proofs/io/CovenIO/WaitingReopen.lean)).

Key waits share retained copies; agreement shares status observations. A
blocked slot waits on its own named cause, never a later slot. Reloads stage
their required inputs without requiring the blocked cursor to advance first.
App-action and after-update conditions issue no automatic requests.

Backoff applies to operations, misses, invites, join polling, files and provider
job checks. SDK retries share the same allowance. Provider `Retry-After`
restricts every worker in that account scope, including explicit retries.
Interpret HTTP dates against the provider response clock; a missing time basis
reports a failure. Delay-seconds use a monotonic timer. Restart preserves
`max(0, min(T - now, W))`, with the full provider wait W (§16.5).

A joiner retains its encrypted request, routing cursor, copies and checkpoint
inputs across polls. It reads forward from the invite's `read_through`, or
uses the floor if those slots have been deleted. A due poll checks the next
slot and request status; new arrivals add their reads. A key alone does not
prove admission: require the approving device's claimed binding for the fixed
member id and requested pair (§12.2). After request deletion, refresh sequence
routing and the floor before deciding decline; approval may have raced the
previous miss. This bootstrap scan sends no ordinary writes or status.

**Ana's open invite.** With no request, Ana's device backs off its shared
request check. Once Carol's request arrives, the checked request stays local
while Ana decides; waiting does not replay history each second.
