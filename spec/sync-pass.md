# One sync pass

This is the request order for [§6](coven.md#6-syncing-writes) and
[E5](api.md#e5-storage-and-sync). Its bounds are requirements in
[§3.1](coven.md#31-io-bounds). Discovery reads each writer's next number;
no pass lists pages of log history. A request below is made only when its
stated condition holds.

## What survives a pass

A read or listing supplies paths, encrypted sizes and storage publication
times. The device keeps a local catalog of these observations. Within a pass,
every consumer shares each completed observation; no consumer repeats it
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
  schema, fingerprints and pending reports, before sealing another post.
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
- **Log download positions:** one contiguous received position per writer
  for entries, writes and key copies. Keep received bytes and checked facts
  before advancing; an applied snapshot can supply covered write positions.
  These are separate from applied positions: a retained object waiting on a
  key is not downloaded again. A miss is remembered for this pass, then
  retried on a later due observation; it is not a permanent end marker.
- **Sealed-key presence:** retain each whole copy and its clear audience,
  key id and recipient by writer and number. Copies are never deleted.
  Observed presence and a successfully opened member copy are different
  facts; secrets stay in custody. Key selection combines this permanent
  evidence with each writer's newly read copies and current replay.
- **Completed catch-up:** keep its qualifying storage time with its applied
  positions (§15). A pass with unresolved history cannot refresh it. It
  determines when return requires snapshot discovery; an unchanged idle
  checkpoint causes no durable write.
- **Removed-device file scans:** after confirmed storage cut-off, keep the
  complete file-folder observation and its completion with the retention
  candidates. A failed or partial scan records no completion. Reopening
  retries unfinished work; a completed scan is not repeated on idle passes.

The sync owner also holds the **decoded store log between passes**, matching
the committed database revision. It decodes the saved log once on startup
through a read connection, then extends or replaces that value when a ready
batch commits. All entry publishers use this same serialized owner. Prepare
replay outside the writer; at commit, validate the revision and atomically
install the entries, author checks, replay, affected data and operation
progress. A failed transaction retains the previous decoded and durable
state. No independently writable decoded copy is maintained by a worker.

Separately, the owner keeps the monotonic start of the last completed
membership and key-copy catch-up for §10's send gate. This session-only
evidence expires five minutes after that start, counting sleep, and is
invalidated on reopen, reconnect or reset. It is not §15's durable storage-time
checkpoint, which also requires data application and confirmed positions.

**Ben's missing key.** His laptop downloads Ana's write 12, but lacks its
Gifts key. The next pass uses the cached bytes and reports the same wait.
When the copy arrives, Ben opens those bytes and applies them; he does not
download write 12 again.

## Requests, in order

Every pass has three phases. A failed prerequisite remains visible while
independent work proceeds; data first attempts require a usable key selected
from this pass's completed membership and copy observations.

## Phase 1: catch up on membership

### 1. Prepare local work and storage time

Serialize this pass with the store's other sync work. Capture the newest
committed local write and commands it will service. Use the owner's decoded
log and indexed pending work; create no snapshot or retention operation
merely to find out that nothing needs doing.
Record this first phase's monotonic start before its discovery requests.

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
does not establish it. Include §15's return interval: a new device has no
checkpoint; a returning device compares a fresh clock-object storage time
with the last completed catch-up's saved storage time. A resumed session
refreshes time, and a running session's monotonic timer schedules its next
check before the known interval reaches 30 days. No due time-dependent
work means no time probe.

If a monotonic timer says a threshold may be reached and the known `T`
cannot decide it, replace `<store>/clock/<this device>` and read its status.
These are two logical requests; provider overhead is counted separately.
A device clock only schedules a check; storage time decides it.
A sample still before the threshold schedules another check with backoff.
A probe that writes anything must first satisfy §10 with the catalog and
identity steps below, using read-only snapshot discovery conservatively
when absence duration is not yet known. If no fresh completed first phase
exists, complete that read-only phase before the clock replacement. Then
list device folders and read the next entry numbers after the time
observation. Count these extra
requests; a probe cannot bypass identity. A snapshot observation already
made for this return serves recovery too, without another listing.
No unchanged positions post supplies this observation.

### 2. Discover and read the store log

List the immediate **`<store>/store-log/` device folders**, following every
folder page without entering their history. Union them with known writers
and devices named by received registrations, including removed or replaced
devices. §4 gives each provider's scoped operation, including CloudKit's
query for permanent entry 1 records. Unknown writers start at entry 1.
Include writers learned from arriving registrations before completing this
observation; each needs its entry and copy reads too.

For every known writer, GET its next **store-log entry** and **key copy**.
On a hit, fetch ahead in parallel up to the shared transfer limit; stop
scheduling at the first miss. Retain completed reads, including requests
already in flight, but never advance the contiguous download position past
a miss or failed read. Charge every actual request. Do not reread decoded
entries or copies. Store-log entries and key copies are never deleted, so
a miss is an exact end observation at that instant.

Each key-copy GET reads the whole small object. Its clear prefix supplies
audience, key id and recipient; index these facts for key selection, sharing
and exposure. Open only this member's boxes, reusing their retained bytes.
An invalid opened key is ignored without hiding subsequent copies: retain
its verdict and continue discovery (§11).
Read an entry as one stream; its clear prefix names the store key. A copy
may be needed before its introduction can be decrypted. Opening a candidate
is tentative until §11's introduction checks, never authority to seal data.
If it has not arrived, retain the entry and wait on writer-copy discovery
under shared backoff. No keys-folder listing or separate per-recipient
status probe is required.

For finality, the folder listing starts after observing `T`, and each known
writer's entry discovery reaches its next-number miss after that listing.
Together with retained entries, this supplies every entry through `T`.
Discovery after `T` can include later entries too. A writer first publishing
after the folder observation cannot have an entry stored before `T`.
Judge landing times against this complete received history, then check
author views and apply causally ready entries in batches. One batch needs
one replay of the received set and one atomic commit, not one full replay and
transaction per received entry. Author-view checks still use exactly each
entry's recorded past. Entries waiting for a key or cause stay cached.
Independent ready entries can proceed, but a gap or unreadable entry
through `T` prevents finality and any cleanup depending on it.
Do not read at or past a log's permanent refusal (§19.1). An incomplete
catch-up also prevents new administrative work, key sharing and first
attempts requiring a current membership view; it is not permission to seal
against an older replay. Fixed attempted uploads retain their bytes, but
every retry still needs §10's fresh completed catch-up.

Advance the single finality horizon only from all entries through `T`
and the exact §9 window. Queries derive finality by comparing stored time
strictly with that horizon; an unchanged horizon causes no update.
A storage time learned during discovery or any later request can be used by
a later observation, never retroactively as the time before this one.
Reading this device's removal or replacement stops sends as §10 requires.

### 3. Observe positions, return coverage and identity

List **`<store>/positions/` once**, including this device's post. Agreement,
retention and reports share it. Positions never supply log discovery bounds:
uploading and posting are separate requests, so a crash between them must
not hide an uploaded object.

For a new device or one away at least 30 storage days, list each readable
**`<store>/snapshots/<audience>/`** folder. Check previously unchecked signed
prefixes by range to select the newest usable snapshot. Load it only if it
covers past that audience's local position on some writer; otherwise keep
the existing state and use ordinary next-number write reads. Apply §15's
common-point loading rules, not each audience independently. Snapshots are
never found by next number: retention can remove 2 while 3 remains.

On reopening or reconnecting, these read-only observations also supply own
counter evidence for §10, including snapshots in any audience needed to
check this id. Read this device's positions only if their observed identity
is new or their checked local record is missing. A matching confirmed own
post needs no GET. Reuse all authored or previously verified evidence.
An uninterrupted warm pass has no snapshot listing for identity.

Read this device's next write number as part of its identity check; retain
any hit for step 6. Combine it with the received store log, key-copy numbers,
checked own positions/snapshot evidence and device-only custody under §10.

This is the check for every send in this pass. A failed check sends nothing,
including keys, files and positions. An out-of-loop sender serializes with
this work and uses a current completed check plus the occupied-path rule;
it cannot reuse evidence from before reconnect, reset or intervening work
that invalidates it. Its required refreshes count as that call's requests.
Every sender also checks the first phase's age immediately before each
request (§10). A long-running pass or upload waits for the next pass once
five minutes have elapsed from that catch-up's start.

### 4. Observe keys and peers

Use step 2's new-copy observations on every pass, even when all entries
are final and every current member already has a copy. A delayed publication
can expose a key without another entry arriving. No stale catalog substitutes
for this pass's observation; a provider cooldown delays the pass itself.

Use the retained clear prefixes and newly acquired copies for sharing and
exposure; opening this member's copies uses bytes already read in step 2.
This device's successful creates also establish presence. An expected copy
not yet observed stays a wait; a later due read can supply it, without a
separate probe for every possible recipient. Decide recipients and usable
keys from the current received log before first attempts. Own publication
updates the catalog without restarting discovery after each entry.

Read each peer's positions only for new or changed listed identity, or a
missing local checked record. Verify once; use the decoded value for
agreement, retention and reports. One completed positions observation
atomically replaces received reports; a failed scan keeps the previous
reports and records the failure. Do not rewrite unchanged report rows;
persist any changed peer identity or positions with the observation.
Recompare cached fingerprints when this device's own data, schema, positions
or usable fingerprint key changes.

Only a complete membership and key-copy observation with committed replay
and a successful identity check supplies the completed first phase's start
to senders. Completion never substitutes its own time for that start. A
phase taking five minutes or more supplies no permission to send.

## Phase 2: make keys and access match

For each audience, retire keys whose observed copies include someone now
excluded. Use an existing usable replacement; if none is held, acquire one
or have a current member's device rotate. Share historical keys with current
members who lack them. Complete a needed rotation's copies and entry before
first sending data with its key. If no usable key can be acquired or made,
that audience's sends wait with their first reason; app writes still commit.

The owner's device runs the access work recorded when entries were applied.
A non-owner exposes the owner wait. Requests serialize and check their current
intention as §4 requires. Pending provider access does not block independent
data protected by usable keys; no removal call runs a second revocation path.

Ana removes Ben. Carol's phase 1 receives the removal and Ben's old key copy.
Her phase 2 publishes a rotation; Ana's device takes back provider access.
Carol's phase 3 seals new data with that rotation. Ben's unresolved S3 key,
if any, remains in the pending list for the admin.

Reserve each due historical copy independently of an entry only for a
currently eligible recipient whose need is not already satisfied (§11).
The number and fixed sealed bytes commit as an irrevocable attempt. Publish
reserved copies in number order even if their initiating need changes;
discarding a numbered row would hide later copies behind a gap.
An occupied own copy requires complete byte equality (§10); absence checks come
from the catalog, not one GET per possible recipient. New rotations and
entry prerequisites are published by the ordered work in step 5.

### 5. Resume due operations and required reloads

Only due operations whose prerequisites changed or whose retry delay
elapsed run. All their catalog reads use steps 2–4. They issue their
remaining requests in §18.1 order: prerequisite sealed-key creates or
snapshot uploads, then the referencing entry create. Applying an entry
records any access change; the owner's serialized access work performs it.
If a local entry changes membership, re-evaluate key eligibility before
continuing to data first attempts, using this pass's copy observations and
its own newly published copies. Never reserve a replacement entry without §9's
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

## Phase 3: sync data

### 6. Send waiting writes, then receive writes

Create queued writes in device-number order, with the key and format rules
of §6. Settle a tried write with its original bytes. An untried write uses
the newest usable key under the received store log. Each success retires
its queue row atomically; it also updates this pass's catalog. Resume any
operation whose own-upload prerequisite this satisfies, using step 5's
request order, without beginning a second full pass.

For downloads, GET each known device's next write number, reusing this
device's identity read. Before 30 storage days since the last completed
catch-up, a miss means no new writes at that observation (§15). A new or
long-absent device first uses step 3's snapshot discovery and reload.
A known required missing write is a prerequisite, not a terminal idle miss.
On a hit, fetch ahead within the configured transfer limit and stop at the
first miss. Retain completed reads; reception order never bypasses causal
or per-log application gates. Use retained headers to order causes and
identify parts. An already cached wait is not a download trigger.
Stream each uncached object once and check chunks as below. Apply each
write atomically only after all checks, with its header facts and file
references. Skipped audiences, resets and schema exclusions keep their
existing rules. Apply §10's retirement landing deadline from retained write
and entry storage times and current replay; consume an excluded write's
position while keeping inputs needed for reversal. Entries retain §9's
permanent landing rule. A missing required uncovered path is a blocker;
retry only when its shared backoff permits, reusing this pass's absence observation.

### 7. Fill the eager file cache

Eager work comes from changed file references, previously failed downloads
now due, or an explicit cache request. Query missing cached ranges with
indexes; do not enumerate every eager app row on each pass or requeue
bytes solely because budget eviction discarded them. Share an in-flight
fetch with another reader of the same range. Files retain §16.3's bounded
ranges and per-chunk authentication; these are explicit exceptions to a
single whole-object request. Account for their header and every range.

Cache publication (§16.6) remains local IO; it never holds the database
writer across disk reading or cryptography.

### 8. Write snapshots and retain history

Use cached signed prefixes and recorded write-part lengths to check
growth. When growth would request a snapshot or a retention candidate is
due, list only its relevant snapshot audience folders, sharing any discovery
already performed for a reload. Read unchecked signed prefixes by range,
then re-evaluate growth or coverage. A wait with unchanged prerequisites
backs off; an idle pass performs no snapshot discovery. Create an operation
only if an audience actually needs a snapshot.
Capture it, verify its sealed bytes locally as §15 requires, upload, and
check the stored checksum, or fetch the stored bytes once if the provider
has no complete-object checksum. That confirmation is shared with any
occupied-path comparison. No snapshot task is created and deleted merely
to record an idle check.

From the same write, snapshot and positions catalogs, delete only objects
whose coverage, finality, age/reader positions and ownership satisfy §15.
A header missing from the local facts requires one authenticated read,
retained across passes; existing facts require no remote reads. Each
successful deletion updates the local catalog and reference index. Retention
has no operation row; eligibility is derived again from the committed facts.
Failure retains protection and its `Retention { path }` record.

Consider files when a row/loss/protected-input change, history deletion,
finality change, upload completion or ownership change can affect retention.
Find files from fixed row references, retained inputs and the upload queue,
using reads or status at exact paths. Never list files on an idle pass.

Once §13's storage cut-off prevents further publication by a removed device,
the device responsible for its cleanup lists **`<store>/files/<device>/`
once**, across all pages, to discover orphans. This includes settling the
ability of in-flight sessions to publish. Persist the paths and completed
scan; a failed scan stays pending and retries. Retained references and
finality still protect every candidate. If replay restores access, a later
cut-off requires a new scan. An unresolved access request is not cut-off.
When reference completeness needs remote snapshot discovery, list those
audience folders for this due retention work and retain the checked facts.

Use the durable reference index, checking local protection first. Read a
retained object's body only if reference completeness is missing and the
body can be checked. Record “cannot prove absence” for an unreadable or
refused object; do not fetch it each pass. A changed prerequisite, update
per §19.1, or confirmed object deletion can change that result. Delete a
file only after complete authenticated reference evidence and all finality
gates permit it. A failure never turns an unknown reference set into empty.

### 9. Post changed positions and finish

Derive the last publishable causally closed positions, fingerprints for
exactly that state, and this device's publishable pending records (D8).
Compare the complete canonical plaintext with the last confirmed post.
Replace **`<store>/positions/<this device>` at most once**, and only on
change; initial publication or observed disappearance also needs a post.
A retained peer report is not this device's report and cannot cause an
echo. A lost replacement reply keeps confirmation pending; a due retry
uses the same attempted bytes or confirms the stored value before retiring
that work. Failed publication retains the last confirmed post.

No heartbeat, idle timestamp, unchanged refusal or time observation causes
a replacement. Positions advance only over committed, fully realized work;
pending changes can travel with the preceding positions (§6).

If every discovered write and required reload is fully realized and its
positions confirmed, retain the qualifying storage time from before this
catch-up as the return checkpoint (§15). Keep the preceding checkpoint if
history is still pending. `Synced` can still describe a finished pass with
pending work; it does not by itself certify this checkpoint. An unchanged
idle pass does not update a database timestamp.

Publish the completion status in memory. Upload marking, queue removal,
cache publication, received reports and this pass's own operation commits
do not schedule an immediate full pass. If an app write or explicit command
arrived after the pass's captured work and was not serviced, retain that
wake and run once more. Otherwise wait for the idle schedule, a genuine
external change, a new app command or the earliest due retry. Do not lose a
write racing pass completion.

## File uploads outside the pass

The file-upload worker runs independently of these steps. A queued file,
changed prerequisite or due retry wakes it; a pass never waits for its
uploads. It shares the identity check (§10), transfer limits, retained
observations and provider cooldown with the pass. Each upload uses its
captured reference, fixed file key and source checks (§16.5), without
selecting an audience key.
Before every request, including a retry, part or session completion, check
§10's catch-up freshness. Expiry waits for the next pass without changing
the fixed reference, key or confirmed session progress.

Use one create within the provider's single-request limit; crossing a
64-KiB encryption chunk boundary alone does not start a resumable session.
Above the provider's limit, begin or resume the recorded session, check
confirmed progress, send remaining parts and finish. The source's whole-file
check never holds the database writer across disk reading or cryptography.
Completion changes no app row; it supplies new retention evidence without
starting a full pass. Stopping sync waits for active transfers too (E5).

Count the worker's requests and bytes separately from pass completion,
using the same §3.1 terms. Moving work outside a pass does not omit its IO.

## One stream, checked as it arrives

A complete-object read is one streaming body request, not a length GET
and data GET for each chunk. Read framing, prefix and header from that
stream, bound lengths before allocating, authenticate each readable chunk,
and incrementally hash every signed byte, including skipped encrypted
parts. Check the final signature and complete framing before applying a
write or snapshot. A snapshot's separately signed prefix can be used only
after its own check. Readers share retained bytes and checked records.
For any object naming a missing key, reuse the pass's acquired copies or
wait for its due new-copy discovery, as in step 2. A later
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

Let `N` count every known device, this one and removed/replaced devices
included. Let `L_p(x)` count the native pages for one scoped listing on
provider `p`. Folder ids and retained observations are already available
in a warm idle pass with usable credentials:

```
D_p = L_p(store-log device folders) + L_p(positions/) + 3 * N
R_idle,p = D_p
```

The three misses per device are its next store-log entry, write and key
copy. A missing folder also establishes the miss; it needs no body request.
Each folder listing enumerates devices, never log contents. Positions hold
one object per device. If each listing fits one page, `R_idle = 2 + 3 * N`
on S3, Google Drive, Dropbox, OneDrive and CloudKit. There are no idle body
bytes, publications, snapshot queries or file queries. Additional pages
scale with devices, not with historical objects or attached files.

An S3 GET, Dropbox download, OneDrive path read or CloudKit record fetch
establishes an absent next object in one request. Drive needs a
[parent/name query](https://developers.google.com/workspace/drive/api/guides/search-files)
when the file id is unknown. An empty complete query is that one miss;
a hit adds a [download by file id](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/get).
Cache the id and charge this hit's lookup to `M`, its body to `O`.
All query continuation pages, folder lookups, redirects and native asset
reads count when required. Initialization and eviction have their own
counts; an incomplete Drive search is a failure, never an idle miss.

**Ben receives one note.** Household has Ana's phone, Ben's laptop and
Carol's tablet. Each discovery listing fits one S3 page. An idle pass uses
**two LISTs and nine GET misses: 11 requests**, regardless of their history.
With a transfer limit of one, one new readable note and no other due work
adds its GET and one changed-positions PUT: **13 requests**. Fetch-ahead
with a higher limit may leave extra probes in flight after the first miss;
charge those requests too. A second positions page makes these 12 and 14.

A due clock observation adds a replacement and status call, plus any
identity refresh and subsequent discovery needed to use that time.
Open invites add their due request checks. Snapshot recovery or due retention
adds only its required audience listings and prefix/body reads. A new file,
snapshot, deletion, permission call, lookup, redirect or retry is charged to
its separate §3.1 term; none is hidden behind 'one arrival'.

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

A joiner keeps its encrypted request, writer-copy download position,
downloaded copies and entries across polling iterations. It does not run a
complete store sync on every wait. The invite names the initial key, inviting
writer and last published key-copy number at invite time. Each due attempt
GETs the next number, reading whole copies and inspecting their clear prefixes
until its own initial-key copy appears or a miss ends the attempt. Hits can
fetch ahead within the transfer limit, without advancing across a gap.
Retain other recipients' routing facts and every completed copy so reopening
does not start again at the invite's number.
After admission, bootstrap obtains every writer's copy history from 1,
reusing this retained tail. The invite's cursor cannot stand in for the
earlier exposure evidence needed for ordinary key selection.

Also use status for the request's continued presence: absence of the key
alone cannot report a decline. A due poll with no new copy costs one logical
next-copy miss and one request-status call; copies that arrived add their
reads, with provider lookup overhead counted separately. After observing
request deletion, refresh the inviting writer's copies through a miss and
check membership as §12.2 requires before deciding the outcome. A copy may
have arrived between the earlier miss and deletion. Key bytes alone cannot
admit a member without replay.

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
