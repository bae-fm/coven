# One sync pass

This is the request order for [§6](coven.md#6-syncing-writes) and
[E5](api.md#e5-storage-and-sync). Its bounds are requirements in
[§3.1](coven.md#31-io-bounds). The next-number discovery conflict remains in
[§4.1](coven.md#41-open-decisions-for-io-bounds); all other bounds use the
settled contract. A request below is made only when its stated condition holds.

## What survives a pass

A listing supplies paths, encrypted sizes and storage publication times.
The device keeps a local catalog of these observations. Within a pass,
every consumer shares each complete listing; no consumer lists it again
under its own name. A failed or incomplete listing establishes neither
absence nor a complete view of storage. Keep the preceding committed catalog
and report the failure. Explicitly observed objects can still be downloaded.

For an immutable object, reuse checked facts by `(path, size, stored_at)`.
A previously observed immutable path whose size or stored time changes
reports `StorageFailure::Protocol`; keep the old evidence rather than apply
another version. For posted positions, compare path, size, stored time
and provider revision. Read only a new revision or missing retained bytes;
equal size and time alone never establish that a replacement is unchanged.

These local records survive reopening and have no pass-end expiry:

- **Write headers:** each part's audience and encoded plaintext length,
  plus the positions and schema facts consumers need. Record them when a
  write is applied, or with the local write that authored them. A conversion
  of an untried write changes these facts in that same transaction. Once
  attempted, both are fixed. Header-only retention keeps its authenticated
  header separately from a complete-signature verdict; it grants no author
  authority. Keep the facts while the object or a reversible effect needs
  them. Snapshot growth is an indexed sum of these lengths; coverage is a
  join with checked snapshot positions.
- **Snapshot prefixes:** the signed prefix, verified author, listed size
  and derived plaintext length. Keep them while the snapshot or a boundary
  names them. Selection, identity and coverage share them; a verified prefix
  is never a claim that the body passed its checks.
- **Peer positions:** the last checked object identity and decoded positions,
  fingerprints and relevant reports, including invalid-position verdicts.
  Keep them until a completed observation replaces or removes that object.
  An unchanged invalid version is not downloaded again; discovery continues
  so its replacement can be checked. Unknown authors or keys are waits on
  locally retained bytes, retried when those prerequisites arrive.
- **Own post:** the exact canonical D8 plaintext last confirmed published,
  with the stored metadata when known. Compare all its fields, including
  schema, fingerprints and blocked reports, before sealing another post.
  Random sealing bytes are not a reason to replace an unchanged post.
- **File references:** an index from each retained write or snapshot to
  the fixed file paths it mentions, with whether all relevant contents
  were checked. Include old values, losses and inputs retained for non-final
  effects, not only displayed rows. Local row, waiting-write, queue and
  reversible-input references change in their existing transactions.
  Record checked remote references with the apply, or after complete
  authentication for retention alone. Delete an object's protection only
  after its deletion is confirmed and no retained input needs it.
- **Unfinished reads:** downloaded entries, write sections and snapshot
  input still waiting for keys, causes, schema or an atomic reload. Keep
  the bytes, checked facts and first unmet prerequisite across passes;
  learning a key is a reason to open cached bytes, not fetch them again.
  Keep fetched ranges under a configured retained-input disk budget.
  Losing bytes is explicit eviction work, never ordinary
  pass cleanup. Durable refusals and file-protection uncertainty are not
  evicted merely to permit another attempt.
- **Sealed-key presence:** observed copy paths remain known, since sealed
  copies are not deleted. A presence observation and a successfully opened
  member copy are different facts. Secrets stay in custody. A failed read
  or absent copy is remembered within the pass and then probed only when
  discovery or its backoff permits it; absence is not permanent.

The sync owner also holds the **decoded store log between passes**, matching
the committed database revision. It decodes the saved log once on startup
through a read connection, then extends or replaces that value when a ready
batch commits. All entry publishers use this same serialized owner. Prepare
replay outside the writer; at commit, validate the revision and atomically
install the entries, author checks, replay, affected data and operation
progress. A failed transaction retains the previous decoded and durable
state. No independently writable decoded copy is maintained by a worker.

**Ben's missing key.** His laptop downloads Ana's write 12, but lacks its
Gifts key. The next pass uses the cached bytes and reports the same wait.
When the copy arrives, Ben opens those bytes and applies them; he does not
download write 12 again.

## Requests, in order

### 1. Prepare local work and storage time

Serialize this pass with the store's other sync work. Capture the newest
committed local write and commands it will service. Use the owner's decoded
log and indexed pending work; create no snapshot or retention operation
merely to find out that nothing needs doing.

Borrow the member keys and keyring unlocked when the handle opened. Every
pass, file task and operation uses that same session. Persist acquired or
generated keys only when they change, before durable work depends on them.
The custody owner retains its unlocked persistence capability; neither a
read nor a save re-derives the passphrase key.

**Ana reopens Household.** Her passphrase unlocks each configured custody
once. Syncing a thousand objects uses those held keys. Stopping and starting
sync does not unlock again; closing the handle erases the session.
Choose an already observed storage time `T` before the store-log scan, if
one is available. With no time sample yet, discovery and ordinary sync
still proceed; finality and age-based deletion wait for a qualifying scan.
From saved entry times and covered-log times, compute the next instant at
which §9 finality or §15's 30-day age test could change. Include the last late
entry's window and the strict boundary for finality; equality at 30 days
does not establish it. No pending time-dependent work means no time probe.

If a monotonic timer says a threshold may be reached and the known `T`
cannot decide it, replace `<store>/clock/<this device>` and read its status.
These are two logical requests; provider overhead is counted separately.
A device clock only schedules a check; storage time decides it.
A sample still before the threshold schedules another check with backoff.
A probe that writes anything must first satisfy §10 with the catalog and
identity steps below, then start a new complete store-log scan after its
time observation. Count that extra scan; a probe cannot bypass identity.
No unchanged positions post supplies this observation.

### 2. Discover and read the store log

List **`<store>/store-log/` completely**, across all devices and pages,
including unknown devices, dropped entries and the history retained for
finality. A listing used to establish finality begins after observing `T`.
Compare with the catalog; read an entry only for a newly listed path or
missing retained local bytes or checked record. Do not reread an already
decoded entry.

Read each needed entry as one stream. Its clear prefix names the exact
store key. If custody lacks that key, read **that key's copy sealed to this
member**, unless it is already cached or this pass already observed it
absent, and only when that wait's retry is due. A copy can be needed before
its introducing entry can be decrypted;
opening it does not by itself authorize that entry or key for sealing.
No search through every device's entry 1 is repeated on later passes.

Judge landing times against the entire completed listing, then check
author views and apply causally ready entries in batches. One batch needs
one replay of the received set and one atomic commit, not one full replay and
transaction per received entry. Author-view checks still use exactly each
entry's recorded past. Entries blocked on a key or cause stay cached.
Independent ready entries can proceed, but a gap or unreadable entry
through `T` prevents finality and any cleanup depending on it.
Do not read at or past a log's permanent refusal (§19.1). An incomplete
catch-up also prevents new administrative work, key sharing and first
attempts requiring a current membership view; it is not permission to seal
against an older replay. Fixed attempted uploads retain their retry rules.

Establish finality only from all entries through `T` and the exact §9
window. A storage time learned from this listing or any later request can
be used by a later scan, never retroactively as the time before this one.
Reading this device's removal or replacement stops sends as §10 requires.

### 3. Collect the other shared listings and check identity

In this order, list once each:

1. **`<store>/devices/`**: complete write paths for every device, including
   unknown and removed devices. Discovery, gaps, uploads, snapshot growth
   and retention all consume this same result.
2. **`<store>/snapshots/`**: all audiences and authors, not one scan per
   readable audience. Local filtering chooses readable snapshots and the
   prefix evidence required for this device's identity and boundaries.
   Bodies in unreadable audiences are not decrypted.
3. **`<store>/positions/`**: all posted device objects, including this
   device. Agreement, retention and received reports share this observation.

Read previously unchecked snapshot prefixes needed for selection, coverage
or identity, and this device's positions only if their observed identity
is new or the local checked record is missing. Reuse authored or previously
verified evidence. A matching confirmed own post needs no GET. Header and
prefix inspection uses retained ranges; load a body only when
a consumer needs it, as specified below.

Check §10 from these complete listings, the already received store log,
checked own positions/snapshot evidence and the device-only custody id.
This is the check for every send in this pass. A failed check sends nothing,
including keys, files and positions. An out-of-loop sender serializes with
this work and uses a current completed check plus the occupied-path rule;
it cannot reuse evidence from before reconnect, reset or intervening work
that invalidates it. Its required refreshes count as that call's requests.

### 4. Observe keys and peers

List **`<store>/keys/` completely on every pass**, including every
writer's store and circle copies. Do this even when all entries are final
and every current member already has a copy. A delayed publication can
expose a key without another entry arriving. A failed or incomplete scan
prevents new key selection and remains recorded; no stale catalog substitutes
for this pass's observation. A provider cooldown delays the pass itself.

Use the listing for sharing and exposure knowledge; do not GET every
recipient's copy. Read only named copies this member needs and lacks,
sharing the bytes already acquired in step 2. This device's successful
creates also establish presence. Reads returning NotFound stay waits; a
later listing showing presence permits a due attempt, not a tight loop.
Decide recipients and usable keys from the current received log before
first attempts. Own key publication in this pass updates the catalog and
presence facts without relisting all keys after each entry.

Create each due historical copy queued independently of an entry only for
a currently eligible recipient, using its retained sealed bytes (§11).
An occupied own copy requires complete byte equality (§10); absence checks come
from the catalog, not one GET per possible recipient. New rotations and
entry prerequisites are published by the ordered work in step 5.

Read each peer's positions only for new or changed listed identity, or a
missing local checked record. Verify once; use the decoded value for
agreement, retention and reports. One completed positions observation
atomically replaces received reports; a failed scan keeps the previous
reports and records the failure. Do not rewrite unchanged report rows;
persist any changed peer identity or positions with the observation.
Recompare cached fingerprints when this device's own data, schema, positions
or usable fingerprint key changes.

### 5. Resume due operations and required reloads

Only due operations whose prerequisites changed or whose retry delay
elapsed run. All their catalog reads use steps 2–4. They issue their
remaining requests in §18.1 order: prerequisite sealed-key creates or
snapshot uploads, then the referencing entry create, then provider access
changes or deletions. Never reserve a replacement entry without §9's
online catch-up. An obsolete provider grant/revoke completes before its
opposite is issued, as §4 requires. Count provider permission lookups and
asynchronous-job polling as operation requests, subject to backoff.
First settle any reserved store-log entry and its remaining prerequisite
copies before reserving another entry. This includes queued entries whose
initiating operation was discarded; discarding it cannot leave a log gap.

While this device has open invites and a request check is due, list
**`<store>/join-requests/` once**, serving all its invites. Read only newly
present requests or those lacking retained checked bytes. An already
decoded request waiting for the person's decision needs no more GETs.
Approval creates key copies and the membership entry before deleting the
request; decline or expiry deletes the request and settles access under
§12.2. No open invite means no join-request scan.

A required reload first settles attempted uploads as §18.1 requires, then
reads its selected snapshots and necessary intervening writes. Use the
catalog and cached inputs, fetching each needed uncached body once; stage
validated data before the atomic reload. The records checked by the reader
are the records applied; do not decrypt and parse the body again. No entry
operation runs while a reload is active. Missing history covered by a
usable snapshot uses this same reload path, not another discovery pass.

Every create uses fixed attempted bytes and the collision rule of §10;
sealed keys use distinct writer paths under §11. For a lost reply or occupied
path, reuse cached stored bytes when they establish the comparison, or
make the one needed comparison read. Metadata cannot replace that byte comparison. Record returned metadata;
where a provider omits it, use the single-object status call, counted here.
Never invent a publication time from the device clock.

### 6. Send waiting writes, then receive writes

Create queued writes in device-number order, with the key and format rules
of §6. Settle a tried write with its original bytes. An untried write uses
the newest usable key under the received store log. Each success retires
its queue row atomically; it also updates this pass's catalog. Resume any
operation whose own-upload prerequisite this satisfies, using step 5's
request order, without beginning a second full pass.

For downloads, select paths not yet consumed for the audiences this device
must load. A listed path with no local checked input is a read trigger;
an already cached wait is not. Use the header to order causes and identify
parts. Fetch known needed objects ahead in parallel within the configured
transfer limit, retaining their results; reception order never bypasses
causal or per-log application gates. Stream each uncached object once and
check chunks as below. Apply each
write atomically only after all checks, with its header facts and file
references. Skipped audiences, resets and schema exclusions keep their
existing rules. A missing uncovered path is a blocker; do not repeatedly
probe it when a complete listing already established its absence.

### 7. Transfer files

Run due file uploads and newly needed eager downloads. Each upload uses
its captured reference and source checks (§16.5). Use one create below
the provider's single-request limit; crossing a 64-KiB encryption chunk
boundary alone does not start a resumable session. Above the provider's
limit, begin or resume its recorded session, check confirmed progress,
send remaining parts and finish. Completion changes no app row.

Eager work comes from changed file references, previously failed downloads
now due, or an explicit cache request. Query missing cached ranges with
indexes; do not enumerate every eager app row on each pass or requeue
bytes solely because budget eviction discarded them. Share an in-flight
fetch with another reader of the same range. Files retain §16.3's bounded
ranges and per-chunk authentication; these are explicit exceptions to a
single whole-object request. Account for their header and every range.

The source's required whole-file check before an upload (§16.5), snapshot
writer verification (§15), and cache publication (§16.6) remain local IO;
none may hold the database writer across disk reading or cryptography.
This pass does not replace those requirements with an unchecked shortcut.

### 8. Write snapshots and retain history

Use cached signed prefixes and recorded write-part lengths to decide
growth. Create an operation only if an audience actually needs a snapshot.
Capture it, verify its sealed bytes locally as §15 requires, upload, and
check the stored checksum, or fetch the stored bytes once if the provider
has no complete-object checksum. That confirmation is shared with any
occupied-path comparison. No snapshot task is created and deleted merely
to record an idle check.

From the same write, snapshot and positions catalogs, delete only objects
whose coverage, finality, age/reader positions and ownership satisfy §15.
A header missing from the local facts requires one authenticated read,
retained across passes; existing facts require no remote reads. Each
successful deletion updates the local catalog and reference index with
the operation's progress. Failure retains protection and its blocker.

Consider files when a row/loss/protected-input change, history deletion,
finality change, upload completion, ownership change or newly discovered
file can affect their retention. On every pass, list
**`<store>/files/<device>/`** for this device and each removed device it is
authorized to delete for. Reuse that complete observation for retention.
Even with no changed references, it must discover a delayed upload. Count
every page; §4.1 leaves next-number discovery out because deletion can hide
later objects.

Use the durable reference index, checking local protection first. Read a
retained object's body only if reference completeness is missing and the
body can be checked. Record “cannot prove absence” for an unreadable or
refused object; do not fetch it each pass. A changed prerequisite, update
per §19.1, or confirmed object deletion can change that result. Delete a
file only after complete authenticated reference evidence and all finality
gates permit it. A failure never turns an unknown reference set into empty.

### 9. Post changed positions and finish

Derive the last publishable causally closed positions, fingerprints for
exactly that state, and this device's publishable blocked records (D8).
Compare the complete canonical plaintext with the last confirmed post.
Replace **`<store>/positions/<this device>` at most once**, and only on
change; initial publication or observed disappearance also needs a post.
A retained peer report is not this device's report and cannot cause an
echo. A lost replacement reply keeps confirmation pending; a due retry
uses the same attempted bytes or confirms the stored value before retiring
that work. Failed publication retains the last confirmed post.

No heartbeat, idle timestamp, unchanged refusal or time observation causes
a replacement. Positions advance only over committed, fully realized work;
blocked changes can travel with the preceding positions (§6).

Publish the completion status in memory. Upload marking, queue removal,
cache publication, received reports and this pass's own operation commits
do not schedule an immediate full pass. If an app write or explicit command
arrived after the pass's captured work and was not serviced, retain that
wake and run once more. Otherwise wait for the idle schedule, a genuine
external change, a new app command or the earliest due retry. Do not lose a
write racing pass completion.

## One stream, checked as it arrives

A complete-object read is one streaming body request, not a length GET
and data GET for each chunk. Read framing, prefix and header from that
stream, bound lengths before allocating, authenticate each readable chunk,
and incrementally hash every signed byte, including skipped encrypted
parts. Check the final signature and complete framing before applying a
write or snapshot. A snapshot's separately signed prefix can be used only
after its own check. Readers share retained bytes and checked records.
For any object naming a missing key, reuse the pass's acquired copies or
request that exact member copy on its due retry, as in step 2. A later
consumer never unlocks custody or downloads the same copy again.

If an earlier chunk fails, retain the failure and drain the same stream if
a complete download is needed to establish permanent refusal (§19.1).
Never fetch the complete object again to justify the verdict. If transport
fails before completion, it remains a failed read, not a permanent refusal.
An update that permits another check uses the retained bytes first.

A consumer needing only a write header or signed snapshot prefix starts
with a bounded range, capped by object size. If that range does not contain
the whole header, one second range extends the contiguous retained prefix.
When the first range cannot reveal the exact end, extend to the maximum
header end permitted by D1/D2, capped by object size; never assume the next
length field fits the first buffer. Parse and bound every count before use.
This permits at most two header requests, even for the largest position vectors.
Retain any body bytes returned with that range too.

When a consumer later needs the body, one streaming range reads from the
retained prefix's end to EOF. Hash the cached prefix and the arriving suffix
as one signed object. A direct full read needs no preliminary header request.
The retained-input budget may evict completed inputs no pending apply or
reversible effect needs; record eviction and charge any later fetch to it.
A disk failure fails the requesting work and records its reason. Never drop
pending bytes or advance positions while reporting success.

CloudKit streams bounded asset parts through E1's bridge. Each native asset
request is counted as a transfer part; one logical stream is not a claim of
one native request for an object held in several assets.

**Carol's import.** Retention has already checked Ana's write header.
Carol later applies the 50,000-note import: she hashes that retained header
and reads only the remaining bytes through one body stream. Its chunks are
checked as they arrive. Its complete signature must
verify before the atomic apply; neither retention nor a later agreement
check downloads that write again.

## Counting a pass

Let `L_p(x)` be all native requests needed for one complete scoped listing
of `x` on provider `p`, including pages and required folder traversal.
Let `U` contain this device and every removed device whose files it may
delete. For a warm idle pass with usable credentials:

```
D_p = L_p(store-log/) + L_p(devices/) + L_p(snapshots/)
    + L_p(positions/) + L_p(keys/) + sum(d in U, L_p(files/d/))
R_idle,p = D_p
```

This formula applies to S3, Google Drive, Dropbox, OneDrive and CloudKit.
S3 counts prefix pages. Drive and OneDrive also count every folder query
needed for their scoped traversal. Dropbox counts recursive folder pages;
CloudKit counts native query pages. No provider has a history-independent
constant under complete discovery. There are zero idle body reads or writes.

**Ben receives one note.** Each base prefix and his own file prefix fits
one S3 page; he owns no removed-device cleanup. The idle pass is **six LIST
requests**. One new readable note, with no extra maintenance, adds one body
GET and one changed-positions PUT: **eight requests**. A second files page
makes those seven and nine. Other providers use their measured `D_p`, plus
their native read and publication overhead.

A due clock observation adds a replacement and a status call, including
any native overhead. Open invites add their due request checks. A new file
or snapshot, retention deletion, permission call, lookup, redirect or retry
is charged to its separate §3.1 term; none is hidden behind 'one arrival'.
## Waiting without repeated work

The operation worker wakes for a command, a committed prerequisite change,
pass completion that supplied new evidence, or its next due timer. There
is no unconditional one-second database/operation scan. A wake reevaluates
only affected work and never resets an unchanged wait's backoff (§3.1).
Key waits share the keys observation, invite waits share the join-request
observation, and agreement/retention share positions. Unchanged app-action
and after-update blockers issue no automatic requests.

The backoff applies to operations, missing-object probes, invites, join
outcome polling, file retries, and provider job-status checks. A failed
connection also delays new automatic passes; local writes and `sync_now`
coalesce while it cannot be retried. SDK retries consume the same allowance,
not a hidden second loop. Honor `Retry-After` for its provider/account scope
across every worker. Delay-seconds use the monotonic timer; an HTTP-date
must be interpreted against the provider response's clock before converting
to a monotonic deadline, so a skewed device clock cannot shorten it. A
missing usable time basis is reported, not treated as permission to retry
immediately. An app retry can override ordinary backoff, never this cooldown.

A joiner keeps its encrypted request, discovered metadata, downloaded
entries and negative observations across polling iterations. It does not
run a complete store sync on every wait. The invite supplies the initial
key and inviting writer, so each due approval probe uses status on one exact
key-copy path. Read a present copy once and use cached bytes thereafter.
Also use status for the request's continued presence: key absence alone
cannot report a decline. An absent-key polling attempt costs two logical
status calls, one for the copy and one for the request, plus provider lookups. Observing
request deletion requires §12.2's fresh membership/key check before deciding
the outcome. A keys-only result cannot admit a member without replay.

**Ana's open invite.** While Carol has not sent her request, Ana's device
backs off the shared request check. Once it arrives, the checked request
stays local while Ana decides. Passes do not keep reading it, and waiting
for approval does not replay the store log each second.

**Ben's throttled laptop.** Storage says to retry after 600 seconds. The
ordinary backoff cap is 300, but Ben's sync, upload and invite workers all
wait at least 600 for that provider scope. App writes remain local and
queued. The database keeps the provider/account cooldown's T and W.
Restart arms `max(0, min(T - now, W))`, never an unconditional immediate
retry. W remains 600 even though ordinary backoff caps at 300 (§16.5).
